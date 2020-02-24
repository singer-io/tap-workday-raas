import json
import sys
import singer

from singer import metadata
from singer import utils
from tap_workday_raas.discover import discover_streams
from tap_workday_raas.sync import sync_report

REQUIRED_CONFIG_KEYS = ["username", "password", "reports"]
LOGGER = singer.get_logger()

def do_discover(config):
    LOGGER.info("Starting discover")
    streams = discover_streams(config)
    if not streams:
        raise Exception("No streams found")
    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")

def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)

def do_sync(config, catalog, state):
    LOGGER.info('Starting sync.')

    reports = { report['report_name']: report for report in json.loads(config['reports']) }

    for stream in catalog.get_selected_streams(state):
        stream_name = stream.tap_stream_id
        mdata = metadata.to_map(stream.metadata)
        report = reports[stream.tap_stream_id]

        state = singer.set_currently_syncing(state, stream_name)
        singer.write_state(state)
        key_properties = metadata.get(mdata, (), "table-key-properties") or []
        singer.write_schema(stream_name, stream.schema.to_dict(), key_properties)

        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_report(report, stream, config)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
    LOGGER.info('Done syncing.')

@singer.utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        do_discover(args.config)
    elif args.catalog or args.properties:
        do_sync(args.config, args.catalog, args.state)

if __name__ == '__main__':
    main()
