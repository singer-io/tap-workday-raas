import json
from collections import namedtuple
from xml.etree import ElementTree
import singer
import requests

from singer import metadata

from tap_workday_raas.client import download_xsd, stream_report
from tap_workday_raas.schema_utils import infer_schema_from_value

LOGGER = singer.get_logger()


def _sanitize_response_text(text, max_length=500):
    """Sanitize HTTP response text for safe logging and error aggregation."""
    if text is None:
        return ""
    # Replace newlines with spaces to keep logs and aggregated messages readable
    sanitized = " ".join(text.splitlines())
    sanitized = sanitized.strip()
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length] + "... [truncated]"
    return sanitized


XSD_NAMESPACE = "http://www.w3.org/2001/XMLSchema"
NS = {"xsd": XSD_NAMESPACE}
_ELEMENT_TAG = "{%s}element" % XSD_NAMESPACE
_CONTENT_MODEL_TAGS = {
    "{%s}%s" % (XSD_NAMESPACE, tag)
    for tag in ("sequence", "choice", "all", "complexContent", "extension", "restriction")
}

# type_index: name -> top-level simpleType/complexType; xsd_prefixes: prefixes bound to XSD_NAMESPACE
_SchemaContext = namedtuple("_SchemaContext", ["type_index", "xsd_prefixes"])
_DEFAULT_CONTEXT = _SchemaContext({}, frozenset({"xsd", "xs"}))

_DATETIME_TYPES = {"date", "dateTime"}
_INTEGER_TYPES = {
    "integer", "int", "long", "short", "byte",
    "nonNegativeInteger", "nonPositiveInteger", "positiveInteger", "negativeInteger",
    "unsignedLong", "unsignedInt", "unsignedShort", "unsignedByte",
}
_FLOAT_TYPES = {"float", "double"}


def _primitive_to_schema(type_name):
    """Map a built-in XSD datatype to a JSON schema. Non-numeric/boolean/date types are text."""
    if type_name in _DATETIME_TYPES:
        return {"type": ["string"], "format": "date-time"}
    if type_name == "decimal":
        return {"type": ["string"], "format": "singer.decimal"}
    if type_name in _INTEGER_TYPES:
        return {"type": ["integer"]}
    if type_name in _FLOAT_TYPES:
        return {"type": ["number"]}
    if type_name == "boolean":
        return {"type": ["boolean"]}
    return {"type": ["string"]}


def _child_elements(node):
    """Yield element declarations of a content model without descending into nested inline types."""
    for child in node:
        if child.tag == _ELEMENT_TAG:
            if "name" in child.attrib:
                yield child
        elif child.tag in _CONTENT_MODEL_TAGS:
            yield from _child_elements(child)


def _complex_type_to_schema(complex_type, context, seen):
    # simpleContent types (e.g. *_IDType, MoneyType) carry a scalar value of their base type
    content = complex_type.find("./xsd:simpleContent/*[@base]", NS)
    if content is not None:
        return _type_to_schema(content.attrib["base"], context, seen)

    # Workday instance references (*ObjectType) are emitted as their Descriptor text in RaaS JSON
    if complex_type.find("./xsd:attribute[@name='Descriptor']", NS) is not None:
        return {"type": ["string"]}

    properties = {}
    # complexContent/extension inherits the base type's fields; restriction redeclares them itself
    extension = complex_type.find("./xsd:complexContent/xsd:extension[@base]", NS)
    if extension is not None:
        base_schema = _type_to_schema(extension.attrib["base"], context, seen)
        properties.update(base_schema.get("properties", {}))

    for child in _child_elements(complex_type):
        properties[child.attrib["name"]] = _element_to_schema(child, context, seen)
    return {"type": ["null", "object"], "properties": properties}


def _simple_type_to_schema(simple_type, context, seen):
    restriction = simple_type.find("./xsd:restriction[@base]", NS)
    if restriction is None:
        # xsd:list / xsd:union simple types are rendered as text
        return {"type": ["string"]}
    return _type_to_schema(restriction.attrib["base"], context, seen)


def _type_to_schema(qualified_type, context, seen):
    prefix, _, type_name = qualified_type.rpartition(":")
    if prefix in context.xsd_prefixes:
        return _primitive_to_schema(type_name)

    if type_name in seen:
        LOGGER.warning('Recursive XSD type "%s" detected; defaulting to string.', qualified_type)
        return {"type": ["string"]}

    definition = context.type_index.get(type_name)
    if definition is None:
        LOGGER.warning('XSD type "%s" is not defined in the schema; defaulting to string.', qualified_type)
        return {"type": ["string"]}

    seen = seen | {type_name}
    if definition.tag == "{%s}simpleType" % XSD_NAMESPACE:
        return _simple_type_to_schema(definition, context, seen)
    return _complex_type_to_schema(definition, context, seen)


def _element_to_schema(element, context=_DEFAULT_CONTEXT, seen=frozenset()):
    is_nullable = element.attrib.get("minOccurs") == "0"

    max_occurs = element.attrib.get("maxOccurs")
    is_list = max_occurs == "unbounded" or (max_occurs is not None and max_occurs.isdigit() and int(max_occurs) > 1)

    if "type" in element.attrib:
        schema = _type_to_schema(element.attrib["type"], context, seen)
    else:
        inline_complex = element.find("./xsd:complexType", NS)
        inline_simple = element.find("./xsd:simpleType", NS)
        if inline_complex is not None:
            schema = _complex_type_to_schema(inline_complex, context, seen)
        elif inline_simple is not None:
            schema = _simple_type_to_schema(inline_simple, context, seen)
        else:
            schema = {"type": ["string"]}

    if is_nullable and "null" not in schema["type"]:
        schema["type"].append("null")

    if is_list:
        if is_nullable:
            schema = {"type": ["null", "array"], "items": schema}
        else:
            schema = {"type": "array", "items": schema}

    return schema


def _xsd_namespace_prefixes(xsd):
    parser = ElementTree.XMLPullParser(events=("start-ns",))
    parser.feed(xsd)
    parser.close()
    return frozenset(prefix for _, (prefix, uri) in parser.read_events() if uri == XSD_NAMESPACE)


def generate_schema_for_report(xsd):
    xsd_schema_et = ElementTree.fromstring(xsd)

    type_index = {
        definition.attrib["name"]: definition
        for definition in list(xsd_schema_et.findall("./xsd:simpleType[@name]", NS))
        + list(xsd_schema_et.findall("./xsd:complexType[@name]", NS))
    }
    context = _SchemaContext(type_index, _xsd_namespace_prefixes(xsd))

    schema = {"type": "object", "properties": {}}

    # Only types reachable from Report_EntryType are resolved, so unrelated definitions
    # (e.g. Execute_ReportType) cannot break discovery.
    entry_type = type_index.get("Report_EntryType")
    if entry_type is not None:
        entry_schema = _complex_type_to_schema(entry_type, context, frozenset({"Report_EntryType"}))
        schema["properties"].update(entry_schema.get("properties", {}))
    return schema


def enrich_schema_from_data(schema, report_url, auth_client, sample_size=100):
    """Enrich schema with column definitions found in actual report data but missing from XSD.

    Workday's XSD endpoint may omit columns where all rows contain null values.
    This function samples actual JSON data and adds any additional columns found
    to the schema, defaulting to their inferred type (or nullable string for None).
    """
    if sample_size <= 0:
        return schema

    try:
        all_columns = {}  # column_name -> first non-None sample value
        for i, record in enumerate(stream_report(report_url, auth_client)):
            for key, value in record.items():
                if key not in all_columns or all_columns[key] is None:
                    all_columns[key] = value
            if i >= sample_size - 1:
                break

        for col, sample_value in all_columns.items():
            if col not in schema["properties"]:
                inferred_schema = infer_schema_from_value(sample_value)
                LOGGER.info(
                    'Found column "%s" in data not in XSD schema. '
                    'Adding with inferred type: %s',
                    col, inferred_schema
                )
                schema["properties"][col] = inferred_schema
    except requests.exceptions.HTTPError:
        # HTTP errors (e.g. 403, 500) are fatal for this report – re-raise so
        # discover_streams can catch them per-report and surface a clear message.
        raise
    except Exception as e:
        LOGGER.warning(
            "Could not enrich schema from data sampling: %s. "
            "Continuing with schema from XSD only.",
            str(e)
        )

    return schema


def discover_streams(config, auth_client):
    streams = []

    reports = json.loads(config["reports"])

    report_names = [report["report_name"] for report in reports]
    seen = set()
    duplicates = {name for name in report_names if name in seen or seen.add(name)}
    if duplicates:
        raise ValueError(
            "Duplicate report name(s) found in config: {}. "
            "Each report_name must be unique as it is used as the stream name.".format(
                ", ".join(sorted(duplicates))
            )
        )

    failed_reports = []

    for report in reports:
        report_name = report["report_name"]
        report_url = report["report_url"]

        LOGGER.info('Downloading XSD to determine table schema "%s"', report_name)

        try:
            xsd = download_xsd(report_url, auth_client)
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else "unknown"
            response_text = _sanitize_response_text(e.response.text if e.response is not None else None)
            LOGGER.error(
                'Failed to download XSD for report "%s" (url: %s). '
                'HTTP status: %s. Server message: %s',
                report_name, report_url, status_code, response_text or "(no response body)"
            )
            failed_reports.append(
                'Report "{name}" (url: {url}) - [schema download] HTTP {status}: {msg}'.format(
                    name=report_name,
                    url=report_url,
                    status=status_code,
                    msg=response_text or "(no response body)",
                )
            )
            continue
        except Exception as e:
            LOGGER.error(
                'Unexpected error downloading XSD for report "%s" (url: %s): %s',
                report_name, report_url, str(e)
            )
            failed_reports.append(
                'Report "{name}" (url: {url}) - [schema download] {err}'.format(
                    name=report_name, url=report_url, err=str(e)
                )
            )
            continue

        try:
            schema = generate_schema_for_report(xsd)
        except Exception as e:
            LOGGER.error(
                'Failed to generate schema for report "%s" (url: %s): %s',
                report_name, report_url, str(e)
            )
            failed_reports.append(
                'Report "{name}" (url: {url}) - schema generation error: {err}'.format(
                    name=report_name, url=report_url, err=str(e)
                )
            )
            continue

        LOGGER.info('Enriching schema with columns from data for "%s".', report_name)
        try:
            schema = enrich_schema_from_data(schema, report_url, auth_client)
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else "unknown"
            response_text = _sanitize_response_text(e.response.text if e.response is not None else None)
            LOGGER.error(
                'Failed to fetch report data for "%s" (url: %s). '
                'HTTP status: %s. Server message: %s',
                report_name, report_url, status_code, response_text or "(no response body)"
            )
            failed_reports.append(
                'Report "{name}" (url: {url}) - [data fetch] HTTP {status}: {msg}'.format(
                    name=report_name,
                    url=report_url,
                    status=status_code,
                    msg=response_text or "(no response body)",
                )
            )
            continue

        stream_md = metadata.get_standard_metadata(schema,
                                                   key_properties=report.get("key_properties"),
                                                   replication_method="FULL_TABLE")
        streams.append(
            {
                "stream": report_name,
                "tap_stream_id": report_name,
                "schema": schema,
                "metadata": stream_md
            }
        )

    if failed_reports:
        raise Exception(
            "Discovery failed for {} report(s):\n{}".format(
                len(failed_reports),
                "\n".join("  - {}".format(r) for r in failed_reports)
            )
        )

    return streams
