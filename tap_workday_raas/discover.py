import json
from xml.etree import ElementTree
import singer

from singer import metadata

from tap_workday_raas.client import download_xsd

LOGGER = singer.get_logger()

def _element_to_schema(element):
    elem_type = element.attrib['type'].split(':')[1]
    is_nullable = element.attrib.get('minOccurs') == '0'

    max_occurs = element.attrib.get("maxOccurs")
    if max_occurs not in (None, "unbounded"):
        raise Exception("Found unexpected value for maxOccurs attribute: '{}'".format(max_occurs))

    is_list = max_occurs == 'unbounded'

    schema = {}

    if elem_type in ('date', 'dateTime'):
        schema = {
            'type': ['string'],
            'format': 'date-time'
        }
    elif elem_type == 'decimal':
        # TODO Update to the singer.decimal format when that is available
        schema = {
            'type': ['number'],
        }
    else:
        schema = {'type': [elem_type]}

    if is_nullable:
        schema['type'].append('null')

    if is_list:
        schema = {
            'type': 'array',
            'items': schema
        }

    return schema

def parse_complex_type(complex_type_selectors, xsd_schema_et, ns):
    complex_type_mapping = {}
    for selector in complex_type_selectors:
        complex_type = xsd_schema_et.find(selector, ns)
        name = complex_type.attrib['name']
        complex_type_mapping[name] = {'type': 'object', 'properties': {}}
        for element in complex_type.findall('.//xsd:element', ns):
            elem_name = element.attrib['name']
            schema_type = _element_to_schema(element)
            complex_type_mapping[name]['properties'][elem_name] = {
                **schema_type
            }

    return complex_type_mapping

def generate_schema_for_report(xsd):
    xsd_schema_et = ElementTree.fromstring(xsd)
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

            max_occurs = elem.attrib.get("maxOccurs")
            if max_occurs not in (None, "unbounded"):
                raise Exception("Found unexpected value for maxOccurs attribute: '{}'".format(max_occurs))

            is_list = max_occurs == 'unbounded'

            if is_list:
                elem_schema = {'type': 'array',
                               'items': complex_type_mapping[elem_type]}
            else:
                elem_schema = complex_type_mapping[elem_type]
            schema['properties'][elem_name] = elem_schema
        else:
            schema_type = _element_to_schema(elem)

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

        xsd = download_xsd(report['report_url'], username, password)
        schema = generate_schema_for_report(xsd)

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
