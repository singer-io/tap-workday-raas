import time
import backoff
import requests
import singer
from singer import metadata, utils
from urllib3.exceptions import ProtocolError
from .transform import WorkdayTransformer as Transformer
from .client import stream_report
from .schema_utils import infer_schema_from_value

LOGGER = singer.get_logger()

_MAX_SYNC_ATTEMPTS = 5
_RETRYABLE_SYNC_ERRORS = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    ProtocolError,
    ConnectionResetError,
)


def _log_resync(details):
    LOGGER.warning(
        "Report sync interrupted (%s). Re-syncing the full report under a new "
        "version in %.1f s (attempt %d of %d).",
        details.get("exception"),
        details["wait"],
        details["tries"] + 1,
        _MAX_SYNC_ATTEMPTS,
    )


def sync_report(report, stream, auth_client):
    report_url = report["report_url"]

    LOGGER.info('Syncing report "%s".', report_url)

    schema = stream.schema.to_dict()
    if "properties" not in schema:
        schema["properties"] = {}
    mdata = metadata.to_map(stream.metadata)
    key_properties = metadata.get(mdata, (), "table-key-properties") or []

    record_count = _sync_report_version(report_url, stream, auth_client, schema, mdata, key_properties, [])

    if record_count == 0:
        LOGGER.info('Report "%s" returned 0 rows. Completing successfully with empty result set.', report_url)

    return record_count


@backoff.on_exception(
    backoff.expo,
    _RETRYABLE_SYNC_ERRORS,
    max_tries=_MAX_SYNC_ATTEMPTS,
    factor=2,
    on_backoff=_log_resync,
)
def _sync_report_version(report_url, stream, auth_client, schema, mdata, key_properties, used_versions):
    """Sync the full report under a newly activated version.

    A dropped connection cannot be resumed mid-report, so each retry re-runs
    the whole report under a new version, exactly like a fresh sync. Records
    already emitted by the interrupted attempt belong to the superseded
    version and are discarded downstream when the new version is activated.
    """
    record_count = 0

    stream_version = int(time.time() * 1000)
    if used_versions and stream_version <= used_versions[-1]:
        stream_version = used_versions[-1] + 1
    used_versions.append(stream_version)
    extraction_time = utils.now().isoformat()

    schema_properties = schema["properties"]

    singer.write_version(stream.tap_stream_id, stream_version)

    with Transformer() as transformer:
        for record in stream_report(report_url, auth_client):
            # Detect columns in the record that are not yet in the schema
            new_columns = set(record.keys()) - set(schema_properties.keys())
            if new_columns:
                for col in new_columns:
                    inferred = infer_schema_from_value(record[col])
                    schema_properties[col] = inferred
                    LOGGER.info(
                        'Found new column "%s" in data not in schema. '
                        'Adding with inferred type: %s',
                        col, inferred
                    )
                # Re-emit the schema so the target knows about the new columns
                singer.write_schema(stream.tap_stream_id, schema, key_properties)

            to_write = transformer.transform(record, schema, mdata)

            # Ensure every schema property appears in the output record;
            # columns missing from this particular record are emitted as null
            for prop in schema_properties:
                if prop not in to_write:
                    to_write[prop] = None

            to_write["_sdc_extracted_at"] = extraction_time
            record_message = singer.RecordMessage(stream.tap_stream_id, to_write, version=stream_version)
            singer.write_message(record_message)
            record_count += 1

    return record_count
