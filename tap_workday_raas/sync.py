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
        for record in stream_report(report_url, username, password):
            to_write = transformer.transform(record, stream.schema.to_dict(), metadata.to_map(stream.metadata))
            to_write['_sdc_extracted_at'] = extraction_time
            record_message = singer.RecordMessage(stream.tap_stream_id,
                                                  to_write,
                                                  version=stream_version)
            singer.write_message(record_message)
            record_count += 1

    return record_count
