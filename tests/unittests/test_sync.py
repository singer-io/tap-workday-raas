import unittest
from unittest.mock import patch, MagicMock

from tap_workday_raas.sync import sync_report
from tap_workday_raas.schema_utils import infer_schema_from_value


# ---------------------------------------------------------------------------
# Tests for infer_schema_from_value
# ---------------------------------------------------------------------------

class TestInferSchemaType(unittest.TestCase):
    """Verify type inference used when expanding the schema during sync."""

    def test_none_returns_nullable_string(self):
        self.assertEqual(infer_schema_from_value(None), {"type": ["string", "null"]})

    def test_string_returns_nullable_string(self):
        self.assertEqual(infer_schema_from_value("hello"), {"type": ["string", "null"]})

    def test_bool_returns_nullable_boolean(self):
        self.assertEqual(infer_schema_from_value(True), {"type": ["boolean", "null"]})
        self.assertEqual(infer_schema_from_value(False), {"type": ["boolean", "null"]})

    def test_int_returns_nullable_number(self):
        self.assertEqual(infer_schema_from_value(42), {"type": ["number", "null"]})

    def test_float_returns_nullable_number(self):
        self.assertEqual(infer_schema_from_value(3.14), {"type": ["number", "null"]})

    def test_dict_returns_object(self):
        result = infer_schema_from_value({"k": "v"})
        self.assertEqual(result, {
            "type": "object",
            "properties": {"k": {"type": ["string", "null"]}}
        })

    def test_list_with_values_returns_array(self):
        result = infer_schema_from_value(["a"])
        self.assertEqual(result, {
            "type": "array",
            "items": {"type": ["string", "null"]}
        })

    def test_empty_list_returns_array_with_nullable_string_items(self):
        result = infer_schema_from_value([])
        self.assertEqual(result, {
            "type": "array",
            "items": {"type": ["string", "null"]}
        })


# ---------------------------------------------------------------------------
# Helper to build a fake Singer stream object
# ---------------------------------------------------------------------------

def _make_stream(tap_stream_id, schema, metadata_list=None):
    """Return a mock stream object that behaves like singer.catalog.CatalogEntry."""
    stream = MagicMock()
    stream.tap_stream_id = tap_stream_id

    schema_obj = MagicMock()
    schema_obj.to_dict.return_value = schema
    stream.schema = schema_obj

    if metadata_list is None:
        metadata_list = [{"breadcrumb": [], "metadata": {}}]
    stream.metadata = metadata_list
    return stream


# ---------------------------------------------------------------------------
# Tests for sync_report – schema evolution & null-filling
# ---------------------------------------------------------------------------

@patch("tap_workday_raas.sync.singer")
@patch("tap_workday_raas.sync.stream_report")
class TestSyncReportSchemaEvolution(unittest.TestCase):
    """Ensure sync_report dynamically expands the schema for new columns
    and fills null for missing columns."""

    def _default_config(self):
        return {"username": "u", "password": "p"}

    def _default_report(self):
        return {"report_url": "http://fake", "report_name": "test_report"}

    # -----------------------------------------------------------------------
    # AC-3: Data in a previously-empty column flows through without error
    # -----------------------------------------------------------------------

    def test_new_column_in_record_triggers_schema_re_emit(self, mock_stream_report, mock_singer):
        """When a record contains a column not in the original schema,
        write_schema should be called again with the expanded schema."""
        schema = {
            "type": "object",
            "properties": {
                "col_a": {"type": ["string", "null"]},
            }
        }
        stream = _make_stream("test", schema)
        mock_stream_report.return_value = iter([
            {"col_a": "v1", "col_new": "surprise"},
        ])

        sync_report(self._default_report(), stream, self._default_config())

        # write_schema must be called with the expanded schema
        schema_calls = [c for c in mock_singer.write_schema.call_args_list]
        self.assertTrue(len(schema_calls) >= 1, "write_schema should be called for new columns")

        # The last schema emitted should include both col_a and col_new
        last_schema = schema_calls[-1][0][1]  # positional arg: schema dict
        self.assertIn("col_a", last_schema["properties"])
        self.assertIn("col_new", last_schema["properties"])

    def test_new_column_included_in_output_record(self, mock_stream_report, mock_singer):
        """The new column's value must appear in the emitted record."""
        schema = {
            "type": "object",
            "properties": {
                "col_a": {"type": ["string", "null"]},
            }
        }
        stream = _make_stream("test", schema)
        mock_stream_report.return_value = iter([
            {"col_a": "v1", "col_new": "surprise"},
        ])

        sync_report(self._default_report(), stream, self._default_config())

        # RecordMessage is called with (tap_stream_id, record_dict, version=...)
        rm_calls = mock_singer.RecordMessage.call_args_list
        self.assertEqual(len(rm_calls), 1)
        record = rm_calls[0][0][1]  # second positional arg = record dict
        self.assertEqual(record["col_new"], "surprise")

    # -----------------------------------------------------------------------
    # AC-2: Columns with all-null values are included in output
    # -----------------------------------------------------------------------

    def test_missing_schema_columns_filled_with_null(self, mock_stream_report, mock_singer):
        """If a record is missing a column defined in the schema, the output
        record should have that column set to None."""
        schema = {
            "type": "object",
            "properties": {
                "col_a": {"type": ["string", "null"]},
                "col_b": {"type": ["string", "null"]},
                "col_c": {"type": ["number", "null"]},
            }
        }
        stream = _make_stream("test", schema)
        mock_stream_report.return_value = iter([
            {"col_a": "only_a"},  # col_b and col_c are missing
        ])

        sync_report(self._default_report(), stream, self._default_config())

        rm_calls = mock_singer.RecordMessage.call_args_list
        self.assertEqual(len(rm_calls), 1)
        record = rm_calls[0][0][1]  # second positional arg = record dict
        self.assertIn("col_b", record)
        self.assertIsNone(record["col_b"])
        self.assertIn("col_c", record)
        self.assertIsNone(record["col_c"])

    # -----------------------------------------------------------------------
    # Schema is NOT re-emitted when no new columns appear
    # -----------------------------------------------------------------------

    def test_no_schema_re_emit_when_no_new_columns(self, mock_stream_report, mock_singer):
        """write_schema should not be called during sync if all record
        columns are already in the schema."""
        schema = {
            "type": "object",
            "properties": {
                "col_a": {"type": ["string", "null"]},
                "col_b": {"type": ["number", "null"]},
            }
        }
        stream = _make_stream("test", schema)
        mock_stream_report.return_value = iter([
            {"col_a": "v1", "col_b": 42},
        ])

        sync_report(self._default_report(), stream, self._default_config())

        mock_singer.write_schema.assert_not_called()

    # -----------------------------------------------------------------------
    # Multiple records – new column appears mid-stream
    # -----------------------------------------------------------------------

    def test_new_column_mid_stream(self, mock_stream_report, mock_singer):
        """A new column appearing in the second record triggers a schema
        re-emit. Already-emitted records cannot be backfilled."""
        schema = {
            "type": "object",
            "properties": {
                "col_a": {"type": ["string", "null"]},
            }
        }
        stream = _make_stream("test", schema)
        mock_stream_report.return_value = iter([
            {"col_a": "v1"},
            {"col_a": "v2", "col_late": "appeared"},
        ])

        sync_report(self._default_report(), stream, self._default_config())

        # write_schema should have been called once (when col_late first appeared)
        self.assertEqual(mock_singer.write_schema.call_count, 1)

        # Both records should be written
        rm_calls = mock_singer.RecordMessage.call_args_list
        self.assertEqual(len(rm_calls), 2)

        # First record: col_a should be there
        first_record = rm_calls[0][0][1]
        self.assertIn("col_a", first_record)

        # Second record: col_late should have its value, col_a present too
        second_record = rm_calls[1][0][1]
        self.assertEqual(second_record["col_late"], "appeared")
        self.assertIn("col_a", second_record)

    # -----------------------------------------------------------------------
    # Multiple new columns at once
    # -----------------------------------------------------------------------

    def test_multiple_new_columns_in_one_record(self, mock_stream_report, mock_singer):
        """Multiple new columns in a single record are all captured."""
        schema = {
            "type": "object",
            "properties": {
                "col_a": {"type": ["string", "null"]},
            }
        }
        stream = _make_stream("test", schema)
        mock_stream_report.return_value = iter([
            {"col_a": "v1", "new1": "x", "new2": 99, "new3": True},
        ])

        sync_report(self._default_report(), stream, self._default_config())

        rm_calls = mock_singer.RecordMessage.call_args_list
        record = rm_calls[0][0][1]
        self.assertEqual(record["new1"], "x")
        self.assertEqual(record["new2"], 99)
        # bool True should pass through
        self.assertIn("new3", record)

    # -----------------------------------------------------------------------
    # Return value = record count
    # -----------------------------------------------------------------------

    def test_returns_record_count(self, mock_stream_report, mock_singer):
        """sync_report should return the total number of records processed."""
        schema = {
            "type": "object",
            "properties": {"col_a": {"type": ["string", "null"]}}
        }
        stream = _make_stream("test", schema)
        mock_stream_report.return_value = iter([
            {"col_a": "v1"},
            {"col_a": "v2"},
            {"col_a": "v3"},
        ])

        count = sync_report(self._default_report(), stream, self._default_config())
        self.assertEqual(count, 3)

    # -----------------------------------------------------------------------
    # _sdc_extracted_at is always present
    # -----------------------------------------------------------------------

    def test_sdc_extracted_at_present(self, mock_stream_report, mock_singer):
        """Every output record should include _sdc_extracted_at."""
        schema = {
            "type": "object",
            "properties": {"col_a": {"type": ["string", "null"]}}
        }
        stream = _make_stream("test", schema)
        mock_stream_report.return_value = iter([{"col_a": "v1"}])

        sync_report(self._default_report(), stream, self._default_config())

        rm_calls = mock_singer.RecordMessage.call_args_list
        record = rm_calls[0][0][1]
        self.assertIn("_sdc_extracted_at", record)

    # -----------------------------------------------------------------------
    # Inferred type for new columns is correct
    # -----------------------------------------------------------------------

    def test_inferred_type_for_new_string_column(self, mock_stream_report, mock_singer):
        """A new string column should be inferred as nullable string."""
        schema = {"type": "object", "properties": {}}
        stream = _make_stream("test", schema)
        mock_stream_report.return_value = iter([{"new_str": "hello"}])

        sync_report(self._default_report(), stream, self._default_config())

        schema_call = mock_singer.write_schema.call_args_list[-1]
        emitted_schema = schema_call[0][1]
        self.assertEqual(
            emitted_schema["properties"]["new_str"],
            {"type": ["string", "null"]}
        )

    def test_inferred_type_for_new_numeric_column(self, mock_stream_report, mock_singer):
        """A new numeric column should be inferred as nullable number."""
        schema = {"type": "object", "properties": {}}
        stream = _make_stream("test", schema)
        mock_stream_report.return_value = iter([{"new_num": 42}])

        sync_report(self._default_report(), stream, self._default_config())

        schema_call = mock_singer.write_schema.call_args_list[-1]
        emitted_schema = schema_call[0][1]
        self.assertEqual(
            emitted_schema["properties"]["new_num"],
            {"type": ["number", "null"]}
        )

    def test_inferred_type_for_new_none_column(self, mock_stream_report, mock_singer):
        """A new column whose first value is None should default to nullable string."""
        schema = {"type": "object", "properties": {}}
        stream = _make_stream("test", schema)
        mock_stream_report.return_value = iter([{"new_null": None}])

        sync_report(self._default_report(), stream, self._default_config())

        schema_call = mock_singer.write_schema.call_args_list[-1]
        emitted_schema = schema_call[0][1]
        self.assertEqual(
            emitted_schema["properties"]["new_null"],
            {"type": ["string", "null"]}
        )
