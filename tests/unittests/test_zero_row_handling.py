"""
Unit tests for zero-row report handling.

When a Workday report returns zero rows (e.g. a "yesterday's data" incremental
report on a day with no changes), the connector must:

    1. Complete successfully – no error raised.
    2. Preserve the target table schema (write_schema still emitted).
    3. Log "0 rows processed" clearly, not an error message.

All API calls are mocked – no real credentials needed.
"""

import json
import unittest
from unittest.mock import patch, MagicMock

from tap_workday_raas import discover
from tap_workday_raas.client import stream_report
from tap_workday_raas.sync import sync_report


# ---------------------------------------------------------------------------
# XSD fixtures
# ---------------------------------------------------------------------------

XSD_FLAT = """\
<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:wd="urn:com.workday.report/Daily_Updates"
            elementFormDefault="qualified" attributeFormDefault="qualified"
            targetNamespace="urn:com.workday.report/Daily_Updates">
    <xsd:element name="Report_Data" type="wd:Report_DataType"/>
    <xsd:complexType name="Report_EntryType">
        <xsd:sequence>
            <xsd:element name="Employee_ID" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Change_Date" type="xsd:date"   minOccurs="0"/>
            <xsd:element name="Description" type="xsd:string" minOccurs="0"/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="Report_DataType">
        <xsd:sequence>
            <xsd:element name="Report_Entry" type="wd:Report_EntryType"
                         minOccurs="0" maxOccurs="unbounded"/>
        </xsd:sequence>
    </xsd:complexType>
</xsd:schema>
"""

XSD_WITH_SUBGROUP = """\
<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:wd="urn:com.workday.report/Comp_Report"
            elementFormDefault="qualified" attributeFormDefault="qualified"
            targetNamespace="urn:com.workday.report/Comp_Report">
    <xsd:element name="Report_Data" type="wd:Report_DataType"/>
    <xsd:complexType name="Comp_groupType">
        <xsd:sequence>
            <xsd:element name="Pay_Rate" type="xsd:decimal" minOccurs="0"/>
            <xsd:element name="Currency" type="xsd:string"  minOccurs="0"/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="Report_EntryType">
        <xsd:sequence>
            <xsd:element name="Employee_ID" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Name"        type="xsd:string" minOccurs="0"/>
            <xsd:element name="Comp_group"  type="wd:Comp_groupType"
                         minOccurs="0" maxOccurs="unbounded"/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="Report_DataType">
        <xsd:sequence>
            <xsd:element name="Report_Entry" type="wd:Report_EntryType"
                         minOccurs="0" maxOccurs="unbounded"/>
        </xsd:sequence>
    </xsd:complexType>
</xsd:schema>
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_stream(tap_stream_id, schema, metadata_list=None):
    """Build a mock Singer CatalogEntry."""
    stream = MagicMock()
    stream.tap_stream_id = tap_stream_id
    schema_obj = MagicMock()
    schema_obj.to_dict.return_value = schema
    stream.schema = schema_obj
    if metadata_list is None:
        metadata_list = [{"breadcrumb": [], "metadata": {}}]
    stream.metadata = metadata_list
    return stream


# ===================================================================
# AC-1: stream_report (client) – zero rows must NOT raise
# ===================================================================

class TestStreamReportZeroRows(unittest.TestCase):
    """Verify that stream_report completes without error when the
    Workday API returns a response with no Report_Entry key
    (the standard zero-row response: {"Report_Data": {}})."""

    @patch("tap_workday_raas.client.requests.get")
    def test_empty_report_data_no_report_entry_key(self, mock_get):
        """Workday returns {"Report_Data": {}} → 0 records, no exception."""
        body = json.dumps({"Report_Data": {}}).encode("utf-8")
        mock_resp = MagicMock()
        mock_resp.iter_content.return_value = [body]
        mock_resp.raise_for_status = MagicMock()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_get.return_value = mock_resp

        records = list(stream_report("http://fake?format=json", "u", "p"))
        self.assertEqual(records, [])

    @patch("tap_workday_raas.client.requests.get")
    def test_report_entry_present_but_empty_array(self, mock_get):
        """Workday returns {"Report_Data": {"Report_Entry": []}} → 0 records."""
        body = json.dumps({"Report_Data": {"Report_Entry": []}}).encode("utf-8")
        mock_resp = MagicMock()
        mock_resp.iter_content.return_value = [body]
        mock_resp.raise_for_status = MagicMock()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_get.return_value = mock_resp

        records = list(stream_report("http://fake", "u", "p"))
        self.assertEqual(records, [])

    @patch("tap_workday_raas.client.requests.get")
    def test_report_entry_key_present_sets_found_key(self, mock_get):
        """When the response contains Report_Entry, the found_key check
        passes (no warning logged). We verify by checking that the warning
        about missing Report_Entry is NOT emitted."""
        body = json.dumps({
            "Report_Data": {
                "Report_Entry": [
                    {"Employee_ID": "E001", "Name": "Alice"},
                ]
            }
        }).encode("utf-8")
        mock_resp = MagicMock()
        # Use a single chunk large enough for the key detection
        mock_resp.iter_content.return_value = [body]
        mock_resp.raise_for_status = MagicMock()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_get.return_value = mock_resp

        with patch("tap_workday_raas.client.LOGGER") as mock_logger:
            # Consume the generator fully
            list(stream_report("http://fake", "u", "p"))
            # The warning about missing Report_Entry should NOT fire
            mock_logger.warning.assert_not_called()

    @patch("tap_workday_raas.client.LOGGER")
    @patch("tap_workday_raas.client.requests.get")
    def test_empty_report_logs_warning(self, mock_get, mock_logger):
        """When Report_Entry is missing, a warning is logged (not an error)."""
        body = json.dumps({"Report_Data": {}}).encode("utf-8")
        mock_resp = MagicMock()
        mock_resp.iter_content.return_value = [body]
        mock_resp.raise_for_status = MagicMock()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_get.return_value = mock_resp

        list(stream_report("http://fake", "u", "p"))

        # Should have called LOGGER.warning, not raised
        mock_logger.warning.assert_called_once()
        # The warning uses %-style formatting: first positional is the template,
        # second is the key name passed via %s
        call_args = mock_logger.warning.call_args
        full_message = call_args[0][0] % call_args[0][1:]
        self.assertIn("Report_Entry", full_message)
        self.assertIn("0 rows", full_message)

    @patch("tap_workday_raas.client.requests.get")
    def test_unexpected_payload_raises_exception(self, mock_get):
        """When the response has neither Report_Data nor Report_Entry
        (unexpected schema change), an exception should be raised."""
        body = json.dumps({"Unexpected_Key": {}}).encode("utf-8")
        mock_resp = MagicMock()
        mock_resp.iter_content.return_value = [body]
        mock_resp.raise_for_status = MagicMock()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_get.return_value = mock_resp

        with self.assertRaises(Exception) as ctx:
            list(stream_report("http://fake", "u", "p"))
        self.assertIn("Report_Data", str(ctx.exception))
        self.assertIn("Report_Entry", str(ctx.exception))

    @patch("tap_workday_raas.client.requests.get")
    def test_report_entry_key_split_across_chunks(self, mock_get):
        """When the Report_Entry key is split across chunk boundaries,
        the parser-based key detection should still find it (no false
        'missing key' warning).  The old raw-byte-scan approach would
        have missed the key when it straddled two chunks."""
        body = json.dumps({
            "Report_Data": {
                "Report_Entry": [
                    {"Employee_ID": "E001"}
                ]
            }
        }).encode("utf-8")
        # Split the body so that "Report_Entry" is spread across two
        # chunks – the first chunk ends in the middle of the key.
        key_pos = body.index(b"Report_Entry")
        split_point = key_pos + 5  # mid-key: "Repor" | "t_Entry..."
        chunks = [body[:split_point], body[split_point:]]

        # Verify the key IS actually split (sanity check for the test)
        self.assertNotIn(b"Report_Entry", chunks[0])
        self.assertNotIn(b"Report_Entry", chunks[1])

        mock_resp = MagicMock()
        mock_resp.iter_content.return_value = chunks
        mock_resp.raise_for_status = MagicMock()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_get.return_value = mock_resp

        with patch("tap_workday_raas.client.LOGGER") as mock_logger:
            # Consume the generator – the key detection must succeed
            list(stream_report("http://fake", "u", "p"))
            # Parser-based detection should find the key despite the split;
            # no "missing key" warning should be emitted.
            mock_logger.warning.assert_not_called()


# ===================================================================
# AC-1 & AC-2: sync_report – zero rows completes successfully and
#              schema is still emitted by the caller (do_sync)
# ===================================================================

@patch("tap_workday_raas.sync.singer")
@patch("tap_workday_raas.sync.stream_report")
class TestSyncReportZeroRows(unittest.TestCase):
    """Verify sync_report returns 0, emits no records, and does not raise
    when the API yields no rows."""

    def _config(self):
        return {"username": "u", "password": "p"}

    def _report(self):
        return {"report_url": "http://fake", "report_name": "daily_updates"}

    def test_zero_rows_returns_zero(self, mock_stream_report, mock_singer):
        schema = discover.flatten_schema(
            discover.generate_schema_for_report(XSD_FLAT))
        stream = _make_stream("daily_updates", schema)
        mock_stream_report.return_value = iter([])

        count = sync_report(self._report(), stream, self._config())

        self.assertEqual(count, 0)

    def test_zero_rows_no_record_messages(self, mock_stream_report, mock_singer):
        schema = discover.flatten_schema(
            discover.generate_schema_for_report(XSD_FLAT))
        stream = _make_stream("daily_updates", schema)
        mock_stream_report.return_value = iter([])

        sync_report(self._report(), stream, self._config())

        mock_singer.RecordMessage.assert_not_called()

    def test_zero_rows_write_version_still_called(self, mock_stream_report, mock_singer):
        """write_version should still be called so the target knows the
        sync completed (even with 0 rows)."""
        schema = discover.flatten_schema(
            discover.generate_schema_for_report(XSD_FLAT))
        stream = _make_stream("daily_updates", schema)
        mock_stream_report.return_value = iter([])

        sync_report(self._report(), stream, self._config())

        mock_singer.write_version.assert_called_once()

    def test_zero_rows_no_exception(self, mock_stream_report, mock_singer):
        """Explicitly verify that no exception propagates."""
        schema = discover.flatten_schema(
            discover.generate_schema_for_report(XSD_FLAT))
        stream = _make_stream("daily_updates", schema)
        mock_stream_report.return_value = iter([])

        try:
            sync_report(self._report(), stream, self._config())
        except Exception as exc:
            self.fail("sync_report raised an exception on zero rows: {}".format(exc))

    def test_zero_rows_with_subgroup_schema(self, mock_stream_report, mock_singer):
        """Zero rows on a report that has sub-groups – still no error."""
        schema = discover.flatten_schema(
            discover.generate_schema_for_report(XSD_WITH_SUBGROUP))
        stream = _make_stream("comp_report", schema)
        mock_stream_report.return_value = iter([])

        count = sync_report(self._report(), stream, self._config())

        self.assertEqual(count, 0)
        mock_singer.RecordMessage.assert_not_called()


# ===================================================================
# AC-2: Target table schema preserved when 0 rows returned
# ===================================================================

class TestSchemaPreservedOnZeroRows(unittest.TestCase):
    """The do_sync function writes the schema BEFORE calling sync_report.
    Verify that the schema is emitted even when the report has 0 rows,
    so the target table structure is preserved."""

    @patch("tap_workday_raas.sync.stream_report")
    @patch("tap_workday_raas.singer")
    def test_do_sync_writes_schema_before_sync(self, mock_singer, mock_stream_report):
        """Simulate do_sync flow: write_schema is called for the stream
        regardless of sync_report returning 0 rows."""
        from tap_workday_raas import do_sync

        mock_stream_report.return_value = iter([])

        schema_dict = discover.flatten_schema(
            discover.generate_schema_for_report(XSD_FLAT))

        # Build a mock catalog
        stream_obj = MagicMock()
        stream_obj.tap_stream_id = "daily_updates"
        stream_obj.schema = MagicMock()
        stream_obj.schema.to_dict.return_value = schema_dict
        stream_obj.metadata = [{"breadcrumb": [], "metadata": {}}]

        catalog = MagicMock()
        catalog.get_selected_streams.return_value = [stream_obj]

        config = {
            "username": "u",
            "password": "p",
            "reports": json.dumps([{
                "report_url": "http://fake",
                "report_name": "daily_updates",
            }]),
        }

        do_sync(config, catalog, {})

        # write_schema must have been called with our schema
        schema_calls = [
            c for c in mock_singer.write_schema.call_args_list
        ]
        self.assertGreaterEqual(len(schema_calls), 1,
                                "write_schema should be called even for 0 rows")
        emitted_schema = schema_calls[0][0][1]  # 2nd positional arg
        self.assertEqual(set(emitted_schema["properties"].keys()),
                         {"Employee_ID", "Change_Date", "Description"})

    @patch("tap_workday_raas.discover.enrich_schema_from_data")
    @patch("tap_workday_raas.discover.download_xsd")
    def test_discover_schema_unchanged_by_empty_data(self, mock_xsd, mock_enrich):
        """Discovery produces the full schema even when the data-enrichment
        step sees zero records (the XSD alone defines the structure)."""
        mock_xsd.return_value = XSD_WITH_SUBGROUP
        mock_enrich.side_effect = lambda s, *a, **kw: s

        streams = discover.discover_streams({
            "username": "u", "password": "p",
            "reports": '[{"report_url": "http://fake", "report_name": "rpt"}]',
        })

        schema = streams[0]["schema"]
        # All flattened columns must be present
        self.assertIn("Employee_ID", schema["properties"])
        self.assertIn("Name", schema["properties"])
        self.assertIn("Comp_group_Pay_Rate", schema["properties"])
        self.assertIn("Comp_group_Currency", schema["properties"])


# ===================================================================
# AC-3: Logs clearly indicate "0 rows processed"
# ===================================================================

@patch("tap_workday_raas.sync.singer")
@patch("tap_workday_raas.sync.stream_report")
class TestZeroRowLogging(unittest.TestCase):
    """Verify that zero-row syncs produce clear "0 rows" log messages
    rather than error-level output."""

    def _config(self):
        return {"username": "u", "password": "p"}

    def _report(self):
        return {"report_url": "http://fake", "report_name": "daily_updates"}

    @patch("tap_workday_raas.sync.LOGGER")
    def test_sync_report_logs_zero_rows_info(self, mock_logger,
                                              mock_stream_report, mock_singer):
        """sync_report should log an INFO message about 0 rows."""
        schema = discover.flatten_schema(
            discover.generate_schema_for_report(XSD_FLAT))
        stream = _make_stream("daily_updates", schema)
        mock_stream_report.return_value = iter([])

        sync_report(self._report(), stream, self._config())

        # Find the zero-row info log
        info_calls = mock_logger.info.call_args_list
        zero_row_msgs = [
            c for c in info_calls
            if "0 rows" in str(c) or "0 row" in str(c)
        ]
        self.assertGreaterEqual(len(zero_row_msgs), 1,
                                "Expected an INFO log about 0 rows, got: {}".format(
                                    info_calls))

    @patch("tap_workday_raas.sync.LOGGER")
    def test_sync_report_does_not_log_error_on_zero_rows(self, mock_logger,
                                                          mock_stream_report,
                                                          mock_singer):
        """No ERROR or CRITICAL log should be emitted for zero rows."""
        schema = discover.flatten_schema(
            discover.generate_schema_for_report(XSD_FLAT))
        stream = _make_stream("daily_updates", schema)
        mock_stream_report.return_value = iter([])

        sync_report(self._report(), stream, self._config())

        mock_logger.error.assert_not_called()
        mock_logger.critical.assert_not_called()

    @patch("tap_workday_raas.sync.LOGGER")
    def test_nonzero_rows_does_not_log_zero_message(self, mock_logger,
                                                     mock_stream_report,
                                                     mock_singer):
        """When rows ARE present, the zero-row message should NOT appear."""
        schema = discover.flatten_schema(
            discover.generate_schema_for_report(XSD_FLAT))
        stream = _make_stream("daily_updates", schema)
        mock_stream_report.return_value = iter([
            {"Employee_ID": "E1", "Change_Date": "2024-01-01",
             "Description": "Update"},
        ])

        sync_report(self._report(), stream, self._config())

        info_calls = mock_logger.info.call_args_list
        zero_row_msgs = [
            c for c in info_calls
            if "0 rows" in str(c)
        ]
        self.assertEqual(len(zero_row_msgs), 0,
                         "Zero-row log should not appear when rows exist")


# ===================================================================
# End-to-end: XSD → discover → sync with zero rows
# ===================================================================

@patch("tap_workday_raas.sync.singer")
@patch("tap_workday_raas.sync.stream_report")
class TestEndToEndZeroRows(unittest.TestCase):
    """Full pipeline tests: discover schema from XSD, then sync with an
    empty result set — mimics the customer's incremental scenario on a
    day with no updates."""

    def _config(self):
        return {"username": "u", "password": "p"}

    def _report(self):
        return {"report_url": "http://fake", "report_name": "rpt"}

    def test_flat_report_zero_rows_end_to_end(self, mock_stream_report, mock_singer):
        schema = discover.flatten_schema(
            discover.generate_schema_for_report(XSD_FLAT))
        stream = _make_stream("rpt", schema)
        mock_stream_report.return_value = iter([])

        count = sync_report(self._report(), stream, self._config())

        self.assertEqual(count, 0)
        mock_singer.write_version.assert_called_once()
        mock_singer.RecordMessage.assert_not_called()

    def test_subgroup_report_zero_rows_end_to_end(self, mock_stream_report, mock_singer):
        schema = discover.flatten_schema(
            discover.generate_schema_for_report(XSD_WITH_SUBGROUP))
        stream = _make_stream("rpt", schema)
        mock_stream_report.return_value = iter([])

        count = sync_report(self._report(), stream, self._config())

        self.assertEqual(count, 0)
        mock_singer.write_version.assert_called_once()
        mock_singer.RecordMessage.assert_not_called()

    def test_zero_then_nonzero_rows_sequential(self, mock_stream_report, mock_singer):
        """Simulate two sequential syncs: first returns 0 rows, second
        returns data. Both must succeed."""
        schema = discover.flatten_schema(
            discover.generate_schema_for_report(XSD_FLAT))

        # First sync – zero rows
        stream1 = _make_stream("rpt", schema)
        mock_stream_report.return_value = iter([])
        count1 = sync_report(self._report(), stream1, self._config())
        self.assertEqual(count1, 0)

        # Reset mocks
        mock_singer.reset_mock()
        mock_stream_report.reset_mock()

        # Second sync – has rows
        stream2 = _make_stream("rpt", schema)
        mock_stream_report.return_value = iter([
            {"Employee_ID": "E1", "Change_Date": "2024-06-01",
             "Description": "Promotion"},
        ])
        count2 = sync_report(self._report(), stream2, self._config())
        self.assertEqual(count2, 1)
        self.assertEqual(len(mock_singer.RecordMessage.call_args_list), 1)

    def test_schema_columns_match_xsd_on_zero_rows(self, mock_stream_report, mock_singer):
        """Even with 0 rows, the schema used by sync_report should still
        contain all columns from the XSD."""
        schema = discover.flatten_schema(
            discover.generate_schema_for_report(XSD_WITH_SUBGROUP))
        stream = _make_stream("rpt", schema)
        mock_stream_report.return_value = iter([])

        sync_report(self._report(), stream, self._config())

        # The schema that was built should have all 4 columns
        self.assertIn("Employee_ID", schema["properties"])
        self.assertIn("Name", schema["properties"])
        self.assertIn("Comp_group_Pay_Rate", schema["properties"])
        self.assertIn("Comp_group_Currency", schema["properties"])
