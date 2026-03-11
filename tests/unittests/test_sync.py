import unittest
from unittest.mock import patch, MagicMock

from tap_workday_raas.sync import sync_report, _infer_schema_type, flatten_record


# ---------------------------------------------------------------------------
# Tests for flatten_record
# ---------------------------------------------------------------------------

class TestFlattenRecord(unittest.TestCase):
    """Verify that flatten_record converts nested dicts / lists-of-dicts
    into a single flat dict with underscore-joined keys."""

    def test_already_flat_record_unchanged(self):
        record = {"a": 1, "b": "hello"}
        self.assertEqual(flatten_record(record), record)

    def test_nested_dict_flattened(self):
        record = {"a": 1, "group": {"child1": "x", "child2": 2}}
        result = flatten_record(record)
        self.assertEqual(result, {"a": 1, "group_child1": "x", "group_child2": 2})

    def test_list_of_single_dict_flattened(self):
        record = {"a": 1, "group": [{"child1": "x", "child2": 2}]}
        result = flatten_record(record)
        self.assertEqual(result, {"a": 1, "group_child1": "x", "group_child2": 2})

    def test_list_of_multiple_dicts_flattened(self):
        """Multiple array items are flattened; later values overwrite earlier ones."""
        record = {"a": 1, "group": [{"child": "first"}, {"child": "second"}]}
        result = flatten_record(record)
        self.assertEqual(result, {"a": 1, "group_child": "second"})

    def test_empty_list_skipped(self):
        record = {"a": 1, "group": []}
        result = flatten_record(record)
        self.assertEqual(result, {"a": 1})

    def test_list_of_primitives_kept(self):
        record = {"a": 1, "tags": ["x", "y"]}
        result = flatten_record(record)
        self.assertEqual(result, {"a": 1, "tags": ["x", "y"]})

    def test_deeply_nested(self):
        record = {"outer": {"mid": {"inner": "val"}}}
        result = flatten_record(record)
        self.assertEqual(result, {"outer_mid_inner": "val"})

    def test_none_values_preserved(self):
        record = {"a": None, "group": {"child": None}}
        result = flatten_record(record)
        self.assertEqual(result, {"a": None, "group_child": None})

    def test_workday_style_record(self):
        """Simulate a typical Workday record with a sub-group array."""
        record = {
            "Employee_ID": "123",
            "Name": "John",
            "Compensation_group": [
                {"Pay_Rate": 50000, "Currency": "USD"}
            ]
        }
        result = flatten_record(record)
        self.assertEqual(result, {
            "Employee_ID": "123",
            "Name": "John",
            "Compensation_group_Pay_Rate": 50000,
            "Compensation_group_Currency": "USD",
        })


# ---------------------------------------------------------------------------
# Tests for _infer_schema_type
# ---------------------------------------------------------------------------

class TestInferSchemaType(unittest.TestCase):
    """Verify type inference used when expanding the schema during sync."""

    def test_none_returns_nullable_string(self):
        self.assertEqual(_infer_schema_type(None), {"type": ["string", "null"]})

    def test_string_returns_nullable_string(self):
        self.assertEqual(_infer_schema_type("hello"), {"type": ["string", "null"]})

    def test_bool_returns_nullable_boolean(self):
        self.assertEqual(_infer_schema_type(True), {"type": ["boolean", "null"]})
        self.assertEqual(_infer_schema_type(False), {"type": ["boolean", "null"]})

    def test_int_returns_nullable_number(self):
        self.assertEqual(_infer_schema_type(42), {"type": ["number", "null"]})

    def test_float_returns_nullable_number(self):
        self.assertEqual(_infer_schema_type(3.14), {"type": ["number", "null"]})

    def test_dict_returns_object(self):
        result = _infer_schema_type({"k": "v"})
        self.assertEqual(result, {
            "type": "object",
            "properties": {"k": {"type": ["string", "null"]}}
        })

    def test_list_with_values_returns_array(self):
        result = _infer_schema_type(["a"])
        self.assertEqual(result, {
            "type": "array",
            "items": {"type": ["string", "null"]}
        })

    def test_empty_list_returns_array_with_nullable_string_items(self):
        result = _infer_schema_type([])
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


# ---------------------------------------------------------------------------
# Tests for sync_report – nested record flattening
# ---------------------------------------------------------------------------

@patch("tap_workday_raas.sync.singer")
@patch("tap_workday_raas.sync.stream_report")
class TestSyncReportFlattensNestedRecords(unittest.TestCase):
    """Ensure sync_report flattens nested dicts/arrays in records so that
    a single Workday report always produces a single flat output dataset."""

    def _default_config(self):
        return {"username": "u", "password": "p"}

    def _default_report(self):
        return {"report_url": "http://fake", "report_name": "test_report"}

    def test_nested_dict_flattened_in_output(self, mock_stream_report, mock_singer):
        """A nested dict in a record should appear as flattened columns."""
        schema = {
            "type": "object",
            "properties": {
                "col_a": {"type": ["string", "null"]},
                "group_child1": {"type": ["string", "null"]},
                "group_child2": {"type": ["number", "null"]},
            }
        }
        stream = _make_stream("test", schema)
        mock_stream_report.return_value = iter([
            {"col_a": "v1", "group": {"child1": "x", "child2": 2}},
        ])

        sync_report(self._default_report(), stream, self._default_config())

        rm_calls = mock_singer.RecordMessage.call_args_list
        self.assertEqual(len(rm_calls), 1)
        record = rm_calls[0][0][1]
        self.assertEqual(record["col_a"], "v1")
        self.assertEqual(record["group_child1"], "x")
        self.assertEqual(record["group_child2"], 2)
        # The nested key should NOT appear
        self.assertNotIn("group", record)

    def test_array_of_single_object_flattened(self, mock_stream_report, mock_singer):
        """A list containing one dict should be flattened like a plain dict."""
        schema = {
            "type": "object",
            "properties": {
                "col_a": {"type": ["string", "null"]},
                "group_child1": {"type": ["string", "null"]},
            }
        }
        stream = _make_stream("test", schema)
        mock_stream_report.return_value = iter([
            {"col_a": "v1", "group": [{"child1": "x"}]},
        ])

        sync_report(self._default_report(), stream, self._default_config())

        rm_calls = mock_singer.RecordMessage.call_args_list
        record = rm_calls[0][0][1]
        self.assertEqual(record["group_child1"], "x")

    def test_row_count_matches_source(self, mock_stream_report, mock_singer):
        """Flattening should not create extra rows – one input record = one output record."""
        schema = {
            "type": "object",
            "properties": {
                "id": {"type": ["string", "null"]},
                "grp_val": {"type": ["string", "null"]},
            }
        }
        stream = _make_stream("test", schema)
        mock_stream_report.return_value = iter([
            {"id": "1", "grp": [{"val": "a"}]},
            {"id": "2", "grp": [{"val": "b"}]},
            {"id": "3", "grp": [{"val": "c"}]},
        ])

        count = sync_report(self._default_report(), stream, self._default_config())
        self.assertEqual(count, 3)
        self.assertEqual(len(mock_singer.RecordMessage.call_args_list), 3)

    def test_missing_nested_group_filled_with_null(self, mock_stream_report, mock_singer):
        """When a record is missing a nested group, its flattened columns
        should be filled with None."""
        schema = {
            "type": "object",
            "properties": {
                "col_a": {"type": ["string", "null"]},
                "group_child1": {"type": ["string", "null"]},
            }
        }
        stream = _make_stream("test", schema)
        # This record has no "group" at all
        mock_stream_report.return_value = iter([
            {"col_a": "v1"},
        ])

        sync_report(self._default_report(), stream, self._default_config())

        rm_calls = mock_singer.RecordMessage.call_args_list
        record = rm_calls[0][0][1]
        self.assertEqual(record["col_a"], "v1")
        # group_child1 should be None (null-filled)
        self.assertIsNone(record["group_child1"])
