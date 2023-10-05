import json
import sys
import singer
import traceback

from singer import metadata
from singer import utils
from tap_workday_raas.discover import discover_streams
from tap_workday_raas.symon_exception import SymonException
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
    return mdata.get((), {}).get("selected", False)


def do_sync(config, catalog, state):
    LOGGER.info("Starting sync.")

    reports = {report["report_name"]: report for report in config["reports"]}

    for stream in catalog.get_selected_streams(state):
        stream_name = stream.tap_stream_id
        mdata = metadata.to_map(stream.metadata)
        report = reports[stream.tap_stream_id]

        state = singer.set_currently_syncing(state, stream_name)
        singer.write_state(state)
        key_properties = metadata.get(mdata, (), "table-key-properties") or []
        singer.write_schema(
            stream_name, stream.schema.to_dict(), key_properties)

        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_report(report, stream, config)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
    LOGGER.info("Done syncing.")


@singer.utils.handle_top_exception(LOGGER)
def main():
    try:
        # used for storing error info to write if error occurs
        error_info = None

        args = utils.parse_args(REQUIRED_CONFIG_KEYS)

        if args.discover:
            do_discover(args.config)
        elif args.catalog or args.properties:
            do_sync(args.config, args.catalog, args.state)
    except SymonException as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error_info = {
            'message': traceback.format_exception_only(exc_type, exc_value)[-1],
            'code': e.code,
            'traceback': "".join(traceback.format_tb(exc_traceback))
        }

        if e.details is not None:
            error_info['details'] = e.details
        raise
    except BaseException as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error_info = {
            'message': traceback.format_exception_only(exc_type, exc_value)[-1],
            'code': e.code,
            'traceback': "".join(traceback.format_tb(exc_traceback))
        }
        raise
    finally:
        if error_info is not None:
            error_file_path = args.config.get('error_file_path', None)
            if error_file_path is not None:
                try:
                    with open(error_file_path, 'w', encoding='utf-8') as fp:
                        json.dump(error_info, fp)
                except:
                    pass
            # log error info as well in case file is corrupted
            error_info_json = json.dumps(error_info)
            error_start_marker = args.config.get(
                'error_start_marker', '[tap_error_start]')
            error_end_marker = args.config.get(
                'error_end_marker', '[tap_error_end]')
            LOGGER.info(
                f'{error_start_marker}{error_info_json}{error_end_marker}')


if __name__ == "__main__":
    main()
