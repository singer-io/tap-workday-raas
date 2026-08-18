import json
import sys
import singer

from singer import metadata
from singer import utils
from tap_workday_raas.client import create_auth_client
from tap_workday_raas.discover import discover_streams
from tap_workday_raas.exceptions import WorkdayRaasAuthenticationError
from tap_workday_raas.sync import sync_report

# Only `reports` is required for both auth modes.
# OAuth mode: hostname, tenant, client_id, client_secret, refresh_token
# Basic auth mode: username, password
REQUIRED_CONFIG_KEYS = ["reports"]
LOGGER = singer.get_logger()

_OAUTH_KEYS = {"hostname", "tenant", "client_id", "client_secret", "refresh_token"}
_BASIC_AUTH_KEYS = {"username", "password"}


def _validate_auth_config(config):
    """Raise a clear error if neither a complete OAuth nor basic-auth config is present."""
    has_oauth = all(config.get(k) for k in _OAUTH_KEYS)
    has_basic = all(config.get(k) for k in _BASIC_AUTH_KEYS)
    if not has_oauth and not has_basic:
        missing_oauth = sorted(k for k in _OAUTH_KEYS if not config.get(k))
        missing_basic = sorted(k for k in _BASIC_AUTH_KEYS if not config.get(k))
        raise WorkdayRaasAuthenticationError(
            "Config must contain either OAuth keys ({}) or basic auth keys ({}). "
            "Missing OAuth keys: {}. Missing basic auth keys: {}.".format(
                ", ".join(sorted(_OAUTH_KEYS)),
                ", ".join(sorted(_BASIC_AUTH_KEYS)),
                missing_oauth,
                missing_basic,
            )
        )


def do_discover(config, auth_client):
    LOGGER.info("Starting discover")
    streams = discover_streams(config, auth_client)
    if not streams:
        raise Exception("No streams found")
    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def do_sync(config, catalog, state, auth_client):
    LOGGER.info("Starting sync.")

    reports = {report["report_name"]: report for report in json.loads(config["reports"])}

    for stream in catalog.get_selected_streams(state):
        stream_name = stream.tap_stream_id
        mdata = metadata.to_map(stream.metadata)
        report = reports[stream.tap_stream_id]

        state = singer.set_currently_syncing(state, stream_name)
        singer.write_state(state)
        key_properties = metadata.get(mdata, (), "table-key-properties") or []
        singer.write_schema(stream_name, stream.schema.to_dict(), key_properties)

        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_report(report, stream, auth_client)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
    LOGGER.info("Done syncing.")


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    _validate_auth_config(args.config)

    config_path = getattr(args, "config_path", None)
    with create_auth_client(args.config, config_path) as auth_client:
        if args.discover:
            do_discover(args.config, auth_client)
        elif args.catalog or args.properties:
            do_sync(args.config, args.catalog, args.state, auth_client)


if __name__ == "__main__":
    main()
