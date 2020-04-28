import json
from xml.etree import ElementTree
import singer

from singer import metadata

from tap_workday_raas.client import download_xsd

LOGGER = singer.get_logger()

def _type_to_schema(elem_type):
    if elem_type in ('date', 'dateTime'):
        return {
            'type': ['string', 'null'],
            'format': 'date-time'
        }
    elif elem_type == 'decimal':
        #TODO What else do we need for decimal types?
        return {
            'type': ['number', 'null']
        }
    else:
        return {'type': [elem_type, 'null']}

# TODO: Test this logic
def parse_complex_type(complex_type_selectors, xsd_schema_et, ns):
    complex_type_mapping = {}
    for selector in complex_type_selectors:
        complex_type = xsd_schema_et.find(selector, ns)
        name = complex_type.attrib['name']
        complex_type_mapping[name] = {'type': 'object', 'properties': {}}
        for element in complex_type.findall('.//xsd:element', ns):
            elem_name = element.attrib['name']
            elem_type = element.attrib['type'].split(':')[1]
            schema_type = _type_to_schema(elem_type)
            complex_type_mapping[name]['properties'][elem_name] = {
                **schema_type
            }

    return complex_type_mapping

# TODO: refactor this so we can test it.
def get_schema_for_report(report, username, password):
    xsd_schema = download_xsd(report['report_url'], username, password)
    xsd_schema_et = ElementTree.fromstring(xsd_schema)
    ns = {'xsd': 'http://www.w3.org/2001/XMLSchema'}

    schema = {'type': 'object', 'properties': {}}

    # The report structure is defined by two complexType elements
    report_structure_elem_names = {'Report_EntryType', 'Report_DataType'}
    all_complex_type_names = {e.attrib['name'] for e in xsd_schema_et.findall("./xsd:complexType", ns)}

    # The set difference results in complexType elements that are used in Report_EntryType to define nested objects
    complex_types = all_complex_type_names - report_structure_elem_names

    # Compute JSON Schemas for other complexType elements which will become nested objects
    complex_type_mapping = parse_complex_type(["./xsd:complexType[@name='{}']".format(i) for i in complex_types], xsd_schema_et, ns)

    # Iterate the 'element' elements nested under the sequence element of the Report definition's complexType
    for elem in xsd_schema_et.findall("./xsd:complexType[@name='Report_EntryType']/xsd:sequence/xsd:element", ns):
        elem_type = elem.attrib['type'].split(':')[1]
        elem_name = elem.attrib['name']

        # When elem's type attribute is a type defined as its own complexType - a nested object
        if elem_type in complex_type_mapping:
            schema['properties'][elem_name] = complex_type_mapping[elem_type]
            continue

        schema_type = _type_to_schema(elem_type)

        schema['properties'][elem_name] = {
            **schema_type
        }
    return schema

def discover_streams(config):
    streams = []

    reports = json.loads(config['reports'])

    username = config['username']
    password = config['password']

    for report in reports:
        LOGGER.info('Downloading XSD to determine table schema "%s".', report['report_name'])
        schema = get_schema_for_report(report, username, password)

        stream_md = metadata.get_standard_metadata(schema,
                                                   key_properties=report.get('key_properties'),
                                                   replication_method='FULL_TABLE')
        streams.append(
            {
                'stream': report['report_name'],
                'tap_stream_id': report['report_name'],
                'schema': schema,
                'metadata': stream_md
            }
        )

    return streams
