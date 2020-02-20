import json
import singer
from singer import metadata, utils, Transformer
from tap_workday_raas.client import stream_report

LOGGER = singer.get_logger()

def sync_report(report, stream, state, config):
    report_url = report['report_url']
    username = config['username']
    password = config['password']

    LOGGER.info('Syncing report "%s".', report_url)

    record_count = 0

    record = {}

    version = utils.now()

    with Transformer() as transformer:
        for event, elem in stream_report(report_url, username, password):
            elem_name = elem.tag.split('}')[1]
            if elem_name == 'Report_Data':
                continue
            elif elem_name == 'Report_Entry':
                if event == 'end':
                    LOGGER.info("WRITE RECORD")
                    to_write = transformer.transform(record, stream.schema.to_dict(), metadata.to_map(stream.metadata))
                    singer.write_record(stream.tap_stream_id, to_write)
                    record_count += 1
                    record = {'_sdc_extracted_at': utils.now()}
            elif event == 'start':
                record[elem_name] = elem.text


    return record_count


