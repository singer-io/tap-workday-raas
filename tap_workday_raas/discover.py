import json
from xml.etree import ElementTree
import singer

from singer import metadata

from tap_workday_raas.client import download_xsd, stream_report

LOGGER = singer.get_logger()


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
        schema = {"type": "array", "items": schema}

    return schema


def parse_complex_type(complex_type_selectors, xsd_schema_et, ns):
    complex_type_mapping = {}
    for selector in complex_type_selectors:
        complex_type = xsd_schema_et.find(selector, ns)
        name = complex_type.attrib["name"]
        complex_type_mapping[name] = {"type": "object", "properties": {}}
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

            if is_list:
                elem_schema = {"type": "array", "items": complex_type_mapping[elem_type]}
            else:
                elem_schema = complex_type_mapping[elem_type]
            schema["properties"][elem_name] = elem_schema
        else:
            schema_type = _element_to_schema(elem)

            schema["properties"][elem_name] = {**schema_type}
    return schema


def _collect_flat_properties(properties, flat_props, prefix):
    """Recursively collect properties, flattening nested objects and arrays of objects."""
    for key, prop_schema in properties.items():
        full_key = "{}_{}".format(prefix, key) if prefix else key
        prop_type = prop_schema.get("type")

        if prop_type == "object" and "properties" in prop_schema:
            # Nested object – flatten its children into the parent
            _collect_flat_properties(prop_schema["properties"], flat_props, full_key)
        elif prop_type == "array" and "items" in prop_schema:
            items = prop_schema["items"]
            if items.get("type") == "object" and "properties" in items:
                # Array of objects – flatten the object's children into the parent
                _collect_flat_properties(items["properties"], flat_props, full_key)
            else:
                # Array of primitives – keep as-is
                flat_props[full_key] = prop_schema
        else:
            flat_props[full_key] = prop_schema


def flatten_schema(schema):
    """Flatten a schema so that nested object / array-of-object properties
    are promoted to the top level with underscore-joined names.

    A single Workday report should produce a single output dataset.  The XSD
    often defines complex-type sub-groups that cause targets (e.g. BigQuery)
    to split the data into parent/child tables.  Flattening the schema
    prevents that split.
    """
    if schema.get("type") != "object" or "properties" not in schema:
        return schema

    flat_props = {}
    _collect_flat_properties(schema["properties"], flat_props, prefix="")
    return {"type": "object", "properties": flat_props}


def _infer_schema_from_value(value):
    """Infer JSON schema type from a sample Python value.

    Used when a column is found in the data but not in the XSD schema.
    Defaults to nullable string for None values.
    """
    if value is None:
        return {"type": ["string", "null"]}
    elif isinstance(value, bool):
        return {"type": ["boolean", "null"]}
    elif isinstance(value, (int, float)):
        return {"type": ["number", "null"]}
    elif isinstance(value, dict):
        properties = {}
        for k, v in value.items():
            properties[k] = _infer_schema_from_value(v)
        return {"type": "object", "properties": properties}
    elif isinstance(value, list):
        if value:
            items_schema = _infer_schema_from_value(value[0])
        else:
            items_schema = {"type": ["string", "null"]}
        return {"type": "array", "items": items_schema}
    else:
        return {"type": ["string", "null"]}


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
                inferred_schema = _infer_schema_from_value(sample_value)
                LOGGER.info(
                    'Found column "%s" in data not in XSD schema. '
                    'Adding with inferred type: %s',
                    col, inferred_schema
                )
                schema["properties"][col] = inferred_schema
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

    username = config["username"]
    password = config["password"]
    include_all_columns = config.get("include_all_columns", True)

    for report in reports:
        LOGGER.info('Downloading XSD to determine table schema "%s".', report["report_name"])

        xsd = download_xsd(report["report_url"], username, password)
        schema = generate_schema_for_report(xsd)

        if include_all_columns:
            LOGGER.info('Enriching schema with columns from data for "%s".', report["report_name"])
            schema = enrich_schema_from_data(schema, report["report_url"], username, password)

        # Flatten nested objects / arrays-of-objects so a single Workday
        # report always produces a single output dataset (no parent/child split).
        schema = flatten_schema(schema)

        stream_md = metadata.get_standard_metadata(schema,
                                                   key_properties=report.get("key_properties"),
                                                   replication_method="FULL_TABLE")
        streams.append(
            {
                "stream": report["report_name"],
                "tap_stream_id": report["report_name"],
                "schema": schema,
                "metadata": stream_md
            }
        )

    return streams
