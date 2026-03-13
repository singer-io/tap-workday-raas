import time
import singer
from singer import metadata, utils
from .transform import WorkdayTransformer as Transformer
from .client import stream_report
from .schema_utils import infer_schema_from_value

LOGGER = singer.get_logger()


def flatten_record(record, parent_key='', sep='_'):
    """Flatten nested dicts and lists-of-dicts into a single-level dict.

    Keys are built by joining the nesting path with *sep*.  For example::

        {"group": [{"col": "val"}]}  ->  {"group_col": "val"}

    This ensures every Workday report row is emitted as a single flat record
    so targets do not split it into parent/child tables.
    """
    items = {}
    for key, value in record.items():
        new_key = "{}{}{}".format(parent_key, sep, key) if parent_key else key
        if isinstance(value, dict):
            items.update(flatten_record(value, new_key, sep))
        elif isinstance(value, list):
            if not value:
                # Preserve existing behavior: ignore empty lists.
                continue
            has_dicts = any(isinstance(item, dict) for item in value)
            has_non_dicts = any(not isinstance(item, dict) for item in value)
            if has_dicts and not has_non_dicts:
                # List of dicts – flatten each dict into the parent.
                for item in value:
                    items.update(flatten_record(item, new_key, sep))
            elif not has_dicts:
                # Non-empty list of primitives – keep as-is.
                items[new_key] = value
            else:
                # Mixed list of dicts and primitives – preserve list to avoid data loss.
                LOGGER.warning(
                    "Mixed list of dicts and primitives encountered at key '%s'; "
                    "preserving original list instead of flattening.",
                    new_key,
                )
                items[new_key] = value
        else:
            items[new_key] = value
    return items


def sync_report(report, stream, config):
    report_url = report["report_url"]
    username = config["username"]
    password = config["password"]

    LOGGER.info('Syncing report "%s".', report_url)

    record_count = 0

    stream_version = int(time.time() * 1000)
    extraction_time = utils.now().isoformat()

    schema = stream.schema.to_dict()
    if "properties" not in schema:
        schema["properties"] = {}
    schema_properties = schema["properties"]
    mdata = metadata.to_map(stream.metadata)
    key_properties = metadata.get(mdata, (), "table-key-properties") or []

    singer.write_version(stream.tap_stream_id, stream_version)

    with Transformer() as transformer:
        for record in stream_report(report_url, username, password):
            # Flatten nested structures so a single report produces a single
            # flat dataset (no parent/child table splitting).
            record = flatten_record(record)

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
