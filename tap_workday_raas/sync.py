import time
import singer
from singer import metadata, utils, Transformer
from tap_workday_raas.client import stream_report

LOGGER = singer.get_logger()

def sync_report(report, stream, config):
    report_url = report['report_url']
    username = config['username']
    password = config['password']

    LOGGER.info('Syncing report "%s".', report_url)

    record_count = 0

    record = {}

    stream_version = int(time.time() * 1000)
    extraction_time = utils.now().isoformat()

    singer.write_version(stream.tap_stream_id, stream_version)

    with Transformer() as transformer:
        for event, elem in stream_report(report_url, username, password):
            elem_name = elem.tag.split('}')[1]
            if elem_name == 'Report_Data':
                continue
            if elem_name == 'Report_Entry':
                if event == 'end':
                    to_write = transformer.transform(record, stream.schema.to_dict(), metadata.to_map(stream.metadata))
                    to_write['_sdc_extracted_at'] = extraction_time
                    record_message = singer.RecordMessage(stream.tap_stream_id,
                                                          to_write,
                                                          version=stream_version)
                    singer.write_message(record_message)
                    record_count += 1
                    record = {}
            elif event == 'start':
                #TODO Check if there are multiple of the element. If yes then parse them as an array

                # If the streaming element has children, its a complexType
                if elem.getchildren():
                    record[elem_name] = {}
                    for child in elem.getchildren():
                        record[elem_name][child.tag.split('}')[1]] = child.text
                else:
                    record[elem_name] = elem.text

    return record_count
