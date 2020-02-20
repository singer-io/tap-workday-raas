import json
import singer
from xml.etree import ElementTree

from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from singer import metadata

from client import download_xsd

LOGGER = singer.get_logger()

def _type_to_schema(elem_type):
    if elem_type == 'date' or elem_type == 'dateTime':
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

def get_schema_for_report(report, username, password):
    xsd_schema = download_xsd(report['report_url'], username, password)
    xsd_schema_et = ElementTree.fromstring(xsd_schema)

    schema = {'type': 'object', 'properties': {}}


    for base_elem in xsd_schema_et.getchildren():
        if base_elem.attrib['name'] == 'Report_EntryType':
            for sequence in base_elem.getchildren():
                for elem in sequence.getchildren():
                    elem_type = elem.attrib['type'].split(':')[1]
                    elem_name = elem.attrib['name']

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
        LOGGER.info('Downloading XSD to determine table schema "%s".', report['table_name']) # TODO get table name
        schema = get_schema_for_report(report, username, password)

        stream_md = metadata.get_standard_metadata(schema,
                                                   key_properties=report.get('key_properties'),
                                                   replication_method='FULL_TABLE')
        streams.append(
            {
                'stream': report['table_name'],
                'tap_stream_id': report['table_name'],
                'schema': schema,
                'metadata': stream_md
            }
        )

    return streams

def test_schema():
    report_url = 'INSERT REPORT URL HERE'
    username = 'INSERT USERNAME HERE'
    password = 'INSERT PASSWORD HERE'

    report = {'report_url': report_url}

    config = {
        'username': username,
        'password': password,
        'reports': '''[
            {
                "report_url": "INSERT REPORT URL HERE",
                "table_name": "THIS IS MY TABLE"
            }
        ]'''
    }

    return discover_streams(config)
