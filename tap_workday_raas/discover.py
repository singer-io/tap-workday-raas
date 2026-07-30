import json
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


def _element_to_schema(element):
    elem_type = element.attrib["type"].split(":")[1]
    is_nullable = element.attrib.get("minOccurs") == "0"

    max_occurs = element.attrib.get("maxOccurs")
    if max_occurs not in (None, "unbounded"):
        raise Exception("Found unexpected value for maxOccurs attribute: '{}'".format(max_occurs))

    is_list = max_occurs == "unbounded"

    schema = {}

    if elem_type in ("date", "dateTime"):
        schema = {"type": ["string"], "format": "date-time"}
    elif elem_type == "decimal":
        # TODO Update to the singer.decimal format when that is available
        schema = {"type": ["number"],}
    else:
        schema = {"type": [elem_type]}

    if is_nullable:
        schema["type"].append("null")

    if is_list:
        if is_nullable:
            schema = {"type": ["null", "array"], "items": schema}
        else:
            schema = {"type": "array", "items": schema}

    return schema


def parse_complex_type(complex_type_selectors, xsd_schema_et, ns):
    complex_type_mapping = {}
    for selector in complex_type_selectors:
        complex_type = xsd_schema_et.find(selector, ns)
        name = complex_type.attrib["name"]
        complex_type_mapping[name] = {"type": ["null", "object"], "properties": {}}
        for element in complex_type.findall(".//xsd:element", ns):
            elem_name = element.attrib["name"]
            schema_type = _element_to_schema(element)
            complex_type_mapping[name]["properties"][elem_name] = {**schema_type}

    return complex_type_mapping


def generate_schema_for_report(xsd):
    xsd_schema_et = ElementTree.fromstring(xsd)
    ns = {"xsd": "http://www.w3.org/2001/XMLSchema"}

    schema = {"type": "object", "properties": {}}

    # The report structure is defined by two complexType elements
    report_structure_elem_names = {"Report_EntryType", "Report_DataType"}
    all_complex_type_names = {e.attrib["name"] for e in xsd_schema_et.findall("./xsd:complexType", ns)}

    # The set difference results in complexType elements that are used in Report_EntryType to define nested objects
    complex_types = all_complex_type_names - report_structure_elem_names

    # Compute JSON Schemas for other complexType elements which will become nested objects
    complex_type_mapping = parse_complex_type(["./xsd:complexType[@name='{}']".format(i) for i in complex_types],xsd_schema_et,ns,)

    # Iterate the 'element' elements nested under the sequence element of the Report definition's complexType
    for elem in xsd_schema_et.findall("./xsd:complexType[@name='Report_EntryType']/xsd:sequence/xsd:element", ns):
        elem_type = elem.attrib["type"].split(":")[1]
        elem_name = elem.attrib["name"]

        # When elem's type attribute is a type defined as its own complexType - a nested object
        if elem_type in complex_type_mapping:

            max_occurs = elem.attrib.get("maxOccurs")
            if max_occurs not in (None, "unbounded"):
                raise Exception("Found unexpected value for maxOccurs attribute: '{}'".format(max_occurs))

            is_list = max_occurs == "unbounded"
            is_nullable = elem.attrib.get("minOccurs") == "0"

            if is_list:
                if is_nullable:
                    elem_schema = {"type": ["null", "array"], "items": complex_type_mapping[elem_type]}
                else:
                    elem_schema = {"type": "array", "items": complex_type_mapping[elem_type]}
            else:
                elem_schema = complex_type_mapping[elem_type]
            schema["properties"][elem_name] = elem_schema
        else:
            schema_type = _element_to_schema(elem)

            schema["properties"][elem_name] = {**schema_type}
    return schema


def enrich_schema_from_data(schema, report_url, username, password, sample_size=100):
    """Enrich schema with column definitions found in actual report data but missing from XSD.

    Workday's XSD endpoint may omit columns where all rows contain null values.
    This function samples actual JSON data and adds any additional columns found
    to the schema, defaulting to their inferred type (or nullable string for None).
    """
    if sample_size <= 0:
        return schema

    try:
        all_columns = {}  # column_name -> first non-None sample value
        for i, record in enumerate(stream_report(report_url, username, password)):
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


def discover_streams(config):
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

    username = config["username"]
    password = config["password"]

    failed_reports = []

    for report in reports:
        report_name = report["report_name"]
        report_url = report["report_url"]

        LOGGER.info('Downloading XSD to determine table schema "%s"', report_name)

        try:
            xsd = download_xsd(report_url, username, password)
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
            schema = enrich_schema_from_data(schema, report_url, username, password)
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
