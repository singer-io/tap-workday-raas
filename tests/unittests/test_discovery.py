import unittest
from unittest.mock import patch, MagicMock
import requests
from tap_workday_raas import discover


xsd = """<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:wd="urn:com.workday.report/Stitch_Testing_2" xmlns:nyw="urn:com.netyourwork/aod" elementFormDefault="qualified" attributeFormDefault="qualified" targetNamespace="urn:com.workday.report/Stitch_Testing_2">
    <xsd:element name="Report_Data" type="wd:Report_DataType"/>
    <xsd:simpleType name="RichText">
        <xsd:restriction base="xsd:string"/>
    </xsd:simpleType>
    <xsd:complexType name="Candidate_Details_groupType">
        <xsd:sequence>
            <xsd:element name="Employee" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Willing_To_Travel" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Potential" type="xsd:string" minOccurs="0"/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="Report_EntryType">
        <xsd:sequence>
            <xsd:element name="Default_Job_Title" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Average_Pay_-_Amount" type="xsd:decimal" minOccurs="0"/>
            <xsd:element name="job_profile_id" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Languages" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Default_Assessment_Tests" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Business_Unit_or_Business_Unit_Hierarchy_Container" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Candidate_Details_group" type="wd:Candidate_Details_groupType" minOccurs="0" maxOccurs="unbounded"/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="Report_DataType">
        <xsd:sequence>
            <xsd:element name="Report_Entry" type="wd:Report_EntryType" minOccurs="0" maxOccurs="unbounded"/>
        </xsd:sequence>
    </xsd:complexType>
</xsd:schema>
"""

class DiscoveryTest(unittest.TestCase):


    def test_generate_schema_for_report(self):

        expected = {'properties':
                    {'Average_Pay_-_Amount': {
                                              'type': ['number', 'null']},
                     'Business_Unit_or_Business_Unit_Hierarchy_Container': {'type': ['string', 'null']},
                     'Candidate_Details_group': {'items':
                                                 {'properties': {'Employee': {'type': ['string', 'null']},
                                                                 'Potential': {'type': ['string', 'null']},
                                                                 'Willing_To_Travel': {'type': ['string', 'null']}},
                                                  'type': 'object'},
                                                 'type': ['array', 'null']},
                     'Default_Assessment_Tests': {'type': ['string', 'null']},
                     'Default_Job_Title': {'type': ['string', 'null']},
                     'Languages': {'type': ['string', 'null']},
                     'job_profile_id': {'type': ['string', 'null']}},
                    'type': 'object'}

        actual = discover.generate_schema_for_report(xsd)
        self.assertEqual(expected, actual)


class TestInferSchemaFromValue(unittest.TestCase):
    """Test infer_schema_from_value type inference for various Python values."""

    def test_none_returns_nullable_string(self):
        result = discover.infer_schema_from_value(None)
        self.assertEqual(result, {"type": ["string", "null"]})

    def test_string_returns_nullable_string(self):
        result = discover.infer_schema_from_value("hello")
        self.assertEqual(result, {"type": ["string", "null"]})

    def test_bool_returns_nullable_boolean(self):
        result = discover.infer_schema_from_value(True)
        self.assertEqual(result, {"type": ["boolean", "null"]})

    def test_int_returns_nullable_number(self):
        result = discover.infer_schema_from_value(42)
        self.assertEqual(result, {"type": ["number", "null"]})

    def test_float_returns_nullable_number(self):
        result = discover.infer_schema_from_value(3.14)
        self.assertEqual(result, {"type": ["number", "null"]})

    def test_dict_returns_object_with_properties(self):
        result = discover.infer_schema_from_value({"name": "Alice", "age": 30})
        self.assertEqual(result, {
            "type": "object",
            "properties": {
                "name": {"type": ["string", "null"]},
                "age": {"type": ["number", "null"]},
            }
        })

    def test_list_returns_array_with_item_type(self):
        result = discover.infer_schema_from_value(["a", "b"])
        self.assertEqual(result, {
            "type": "array",
            "items": {"type": ["string", "null"]}
        })

    def test_empty_list_returns_array_with_nullable_string_items(self):
        result = discover.infer_schema_from_value([])
        self.assertEqual(result, {
            "type": "array",
            "items": {"type": ["string", "null"]}
        })


class TestEnrichSchemaFromData(unittest.TestCase):
    """Test enrich_schema_from_data merging of data-discovered columns into schema."""

    def _base_schema(self):
        """Return a minimal schema with two known columns."""
        return {
            "type": "object",
            "properties": {
                "col_a": {"type": ["string", "null"]},
                "col_b": {"type": ["number", "null"]},
            }
        }

    @patch("tap_workday_raas.discover.stream_report")
    def test_adds_columns_found_in_data_but_not_in_xsd(self, mock_stream):
        """Columns present in JSON records but missing from XSD are added to schema."""
        mock_stream.return_value = iter([
            {"col_a": "v1", "col_b": 1, "col_c": "extra", "col_d": 99},
        ])
        schema = self._base_schema()
        result = discover.enrich_schema_from_data(schema, "http://fake", "u", "p")

        self.assertIn("col_c", result["properties"])
        self.assertEqual(result["properties"]["col_c"], {"type": ["string", "null"]})
        self.assertIn("col_d", result["properties"])
        self.assertEqual(result["properties"]["col_d"], {"type": ["number", "null"]})

    @patch("tap_workday_raas.discover.stream_report")
    def test_does_not_overwrite_existing_xsd_columns(self, mock_stream):
        """Columns already in the XSD schema retain their original type definition."""
        mock_stream.return_value = iter([
            {"col_a": 12345, "col_b": "string_value"},
        ])
        schema = self._base_schema()
        result = discover.enrich_schema_from_data(schema, "http://fake", "u", "p")

        # Original types should be preserved, not overwritten by inferred types
        self.assertEqual(result["properties"]["col_a"], {"type": ["string", "null"]})
        self.assertEqual(result["properties"]["col_b"], {"type": ["number", "null"]})

    @patch("tap_workday_raas.discover.stream_report")
    def test_no_extra_columns_leaves_schema_unchanged(self, mock_stream):
        """When data has exactly the same columns as XSD, schema is unchanged."""
        mock_stream.return_value = iter([
            {"col_a": "v1", "col_b": 42},
        ])
        schema = self._base_schema()
        original_props = dict(schema["properties"])
        result = discover.enrich_schema_from_data(schema, "http://fake", "u", "p")

        self.assertEqual(result["properties"], original_props)

    @patch("tap_workday_raas.discover.stream_report")
    def test_handles_stream_report_error_gracefully(self, mock_stream):
        """If stream_report raises an exception, the original schema is returned."""
        mock_stream.side_effect = Exception("connection refused")
        schema = self._base_schema()
        original_props = dict(schema["properties"])
        result = discover.enrich_schema_from_data(schema, "http://fake", "u", "p")

        self.assertEqual(result["properties"], original_props)

    @patch("tap_workday_raas.discover.stream_report")
    def test_handles_empty_data(self, mock_stream):
        """If the report returns zero records, schema is unchanged."""
        mock_stream.return_value = iter([])
        schema = self._base_schema()
        original_props = dict(schema["properties"])
        result = discover.enrich_schema_from_data(schema, "http://fake", "u", "p")

        self.assertEqual(result["properties"], original_props)

    @patch("tap_workday_raas.discover.stream_report")
    def test_collects_columns_across_multiple_records(self, mock_stream):
        """Columns that appear in different records are all captured."""
        mock_stream.return_value = iter([
            {"col_a": "v1"},
            {"col_a": "v2", "col_c": "extra1"},
            {"col_a": "v3", "col_d": 100},
        ])
        schema = self._base_schema()
        result = discover.enrich_schema_from_data(schema, "http://fake", "u", "p")

        self.assertIn("col_c", result["properties"])
        self.assertIn("col_d", result["properties"])

    @patch("tap_workday_raas.discover.stream_report")
    def test_prefers_non_none_value_for_type_inference(self, mock_stream):
        """When a column is None in early records, a later non-None value is used for inference."""
        mock_stream.return_value = iter([
            {"col_a": "v1", "col_new": None},
            {"col_a": "v2", "col_new": 42},
        ])
        schema = self._base_schema()
        result = discover.enrich_schema_from_data(schema, "http://fake", "u", "p")

        self.assertIn("col_new", result["properties"])
        self.assertEqual(result["properties"]["col_new"], {"type": ["number", "null"]})

    @patch("tap_workday_raas.discover.stream_report")
    def test_sample_size_limits_records_read(self, mock_stream):
        """Only sample_size records should be consumed from the stream."""
        consumed = []
        def counting_iter():
            for i in range(200):
                consumed.append(i)
                yield {"col_a": f"v{i}"}

        mock_stream.return_value = counting_iter()
        schema = self._base_schema()

        discover.enrich_schema_from_data(schema, "http://fake", "u", "p", sample_size=5)

        self.assertEqual(len(consumed), 5, "Should consume exactly sample_size records")

    @patch("tap_workday_raas.discover.stream_report")
    def test_sample_size_zero_skips_data_fetch(self, mock_stream):
        """When sample_size is 0, no records should be fetched and schema is unchanged."""
        schema = self._base_schema()
        original_props = dict(schema["properties"])
        result = discover.enrich_schema_from_data(schema, "http://fake", "u", "p", sample_size=0)

        mock_stream.assert_not_called()
        self.assertEqual(result["properties"], original_props)


class TestDiscoverStreamsEnrichment(unittest.TestCase):
    """Test discover_streams always enriches schema from data."""

    @patch("tap_workday_raas.discover.enrich_schema_from_data")
    @patch("tap_workday_raas.discover.download_xsd")
    def test_enrich_schema_always_called(self, mock_xsd, mock_enrich):
        """enrich_schema_from_data is always called during discovery."""
        mock_xsd.return_value = xsd
        mock_enrich.side_effect = lambda schema, *a, **kw: schema

        config = {
            "username": "user",
            "password": "pass",
            "reports": '[{"report_url": "http://fake", "report_name": "test_report"}]',
        }
        discover.discover_streams(config)
        mock_enrich.assert_called_once()


def _make_http_error(status_code, response_text="Server Error"):
    """Helper to construct a requests.exceptions.HTTPError with a mock response."""
    mock_response = MagicMock()
    mock_response.status_code = status_code
    mock_response.text = response_text
    error = requests.exceptions.HTTPError(response=mock_response)
    return error


class TestDiscoverStreamsDuplicateReportNames(unittest.TestCase):
    """Test that duplicate report names are rejected during discovery."""

    def _config(self, reports):
        import json
        return {
            "username": "user",
            "password": "pass",
            "reports": json.dumps(reports),
        }

    def test_duplicate_report_names_raises_value_error(self):
        """discover_streams raises ValueError when two reports share the same name."""
        config = self._config([
            {"report_url": "http://fake/r1", "report_name": "my_report"},
            {"report_url": "http://fake/r2", "report_name": "my_report"},
        ])
        with self.assertRaises(ValueError) as ctx:
            discover.discover_streams(config)

        self.assertIn("my_report", str(ctx.exception))

    def test_multiple_duplicate_report_names_all_listed(self):
        """All duplicate names are included in the error message."""
        config = self._config([
            {"report_url": "http://fake/r1", "report_name": "report_a"},
            {"report_url": "http://fake/r2", "report_name": "report_b"},
            {"report_url": "http://fake/r3", "report_name": "report_a"},
            {"report_url": "http://fake/r4", "report_name": "report_b"},
        ])
        with self.assertRaises(ValueError) as ctx:
            discover.discover_streams(config)

        error_msg = str(ctx.exception)
        self.assertIn("report_a", error_msg)
        self.assertIn("report_b", error_msg)

    def test_unique_report_names_does_not_raise(self):
        """No error is raised when all report names are unique."""
        from unittest.mock import patch
        with patch("tap_workday_raas.discover.download_xsd") as mock_xsd, \
             patch("tap_workday_raas.discover.enrich_schema_from_data") as mock_enrich:
            import xml.etree.ElementTree as ET
            mock_xsd.return_value = xsd
            mock_enrich.side_effect = lambda schema, *a, **kw: schema

            config = self._config([
                {"report_url": "http://fake/r1", "report_name": "report_x"},
                {"report_url": "http://fake/r2", "report_name": "report_y"},
            ])
            # Should not raise
            streams = discover.discover_streams(config)
            stream_names = [s["tap_stream_id"] for s in streams]
            self.assertIn("report_x", stream_names)
            self.assertIn("report_y", stream_names)


class TestDiscoverStreamsErrorHandling(unittest.TestCase):
    """Test per-report error handling in discover_streams."""

    def _config(self, reports):
        import json
        return {
            "username": "user",
            "password": "pass",
            "reports": json.dumps(reports),
        }

    @patch("tap_workday_raas.discover.enrich_schema_from_data")
    @patch("tap_workday_raas.discover.download_xsd")
    def test_all_reports_succeed_returns_streams(self, mock_xsd, mock_enrich):
        """When all reports succeed, discover_streams returns all streams without raising."""
        mock_xsd.return_value = xsd
        mock_enrich.side_effect = lambda schema, *a, **kw: schema

        config = self._config([
            {"report_url": "http://fake/r1", "report_name": "report_1"},
            {"report_url": "http://fake/r2", "report_name": "report_2"},
        ])
        streams = discover.discover_streams(config)

        self.assertEqual(len(streams), 2)
        stream_ids = [s["tap_stream_id"] for s in streams]
        self.assertIn("report_1", stream_ids)
        self.assertIn("report_2", stream_ids)

    @patch("tap_workday_raas.discover.enrich_schema_from_data")
    @patch("tap_workday_raas.discover.download_xsd")
    def test_single_http_error_raises_with_status_code(self, mock_xsd, mock_enrich):
        """An HTTP error on download_xsd raises an exception containing the status code."""
        mock_xsd.side_effect = _make_http_error(500, "Internal Server Error")

        config = self._config([
            {"report_url": "http://fake/r1", "report_name": "report_1"},
        ])
        with self.assertRaises(Exception) as ctx:
            discover.discover_streams(config)

        self.assertIn("500", str(ctx.exception))

    @patch("tap_workday_raas.discover.enrich_schema_from_data")
    @patch("tap_workday_raas.discover.download_xsd")
    def test_http_error_message_contains_report_name_and_url(self, mock_xsd, mock_enrich):
        """The raised exception message includes the report name and URL."""
        mock_xsd.side_effect = _make_http_error(403, "Forbidden")

        config = self._config([
            {"report_url": "http://fake/secret_report", "report_name": "secret_report"},
        ])
        with self.assertRaises(Exception) as ctx:
            discover.discover_streams(config)

        error_msg = str(ctx.exception)
        self.assertIn("secret_report", error_msg)
        self.assertIn("http://fake/secret_report", error_msg)

    @patch("tap_workday_raas.discover.enrich_schema_from_data")
    @patch("tap_workday_raas.discover.download_xsd")
    def test_http_error_message_contains_server_response_text(self, mock_xsd, mock_enrich):
        """The raised exception includes the server's response body text."""
        mock_xsd.side_effect = _make_http_error(500, "The report does not exist.")

        config = self._config([
            {"report_url": "http://fake/r1", "report_name": "report_1"},
        ])
        with self.assertRaises(Exception) as ctx:
            discover.discover_streams(config)

        self.assertIn("The report does not exist.", str(ctx.exception))

    @patch("tap_workday_raas.discover.enrich_schema_from_data")
    @patch("tap_workday_raas.discover.download_xsd")
    def test_one_failing_report_does_not_prevent_others(self, mock_xsd, mock_enrich):
        """A failing report is skipped and remaining reports are still discovered."""
        mock_xsd.side_effect = [
            _make_http_error(500, "Error"),   # report_1 fails
            xsd,                               # report_2 succeeds
        ]
        mock_enrich.side_effect = lambda schema, *a, **kw: schema

        config = self._config([
            {"report_url": "http://fake/r1", "report_name": "report_1"},
            {"report_url": "http://fake/r2", "report_name": "report_2"},
        ])
        with self.assertRaises(Exception) as ctx:
            discover.discover_streams(config)

        # The exception should only mention the failed report
        error_msg = str(ctx.exception)
        self.assertIn("report_1", error_msg)
        self.assertNotIn("report_2", error_msg)

    @patch("tap_workday_raas.discover.enrich_schema_from_data")
    @patch("tap_workday_raas.discover.download_xsd")
    def test_one_failing_report_still_discovers_others(self, mock_xsd, mock_enrich):
        """Streams from successful reports are still discoverable even when another fails."""
        mock_xsd.side_effect = [
            xsd,                               # report_1 succeeds
            _make_http_error(404, "Not Found"), # report_2 fails
        ]
        mock_enrich.side_effect = lambda schema, *a, **kw: schema

        config = self._config([
            {"report_url": "http://fake/r1", "report_name": "report_1"},
            {"report_url": "http://fake/r2", "report_name": "report_2"},
        ])

        try:
            discover.discover_streams(config)
        except Exception:
            pass  # expected – but we can't check streams since it raises not returns

        # Verify that download_xsd was called for both reports (loop continued)
        self.assertEqual(mock_xsd.call_count, 2)

    @patch("tap_workday_raas.discover.enrich_schema_from_data")
    @patch("tap_workday_raas.discover.download_xsd")
    def test_multiple_failing_reports_all_listed_in_exception(self, mock_xsd, mock_enrich):
        """When multiple reports fail, all of them are listed in a single exception."""
        mock_xsd.side_effect = [
            _make_http_error(500, "Error A"),
            _make_http_error(403, "Error B"),
        ]

        config = self._config([
            {"report_url": "http://fake/r1", "report_name": "report_1"},
            {"report_url": "http://fake/r2", "report_name": "report_2"},
        ])
        with self.assertRaises(Exception) as ctx:
            discover.discover_streams(config)

        error_msg = str(ctx.exception)
        self.assertIn("report_1", error_msg)
        self.assertIn("report_2", error_msg)
        self.assertIn("2", error_msg)  # "Discovery failed for 2 report(s)"

    @patch("tap_workday_raas.discover.enrich_schema_from_data")
    @patch("tap_workday_raas.discover.download_xsd")
    def test_non_http_error_on_download_xsd_is_caught(self, mock_xsd, mock_enrich):
        """Non-HTTP exceptions (e.g. connection error) from download_xsd are also handled."""
        mock_xsd.side_effect = ConnectionError("DNS resolution failed")

        config = self._config([
            {"report_url": "http://fake/r1", "report_name": "report_1"},
        ])
        with self.assertRaises(Exception) as ctx:
            discover.discover_streams(config)

        error_msg = str(ctx.exception)
        self.assertIn("report_1", error_msg)
        self.assertIn("DNS resolution failed", error_msg)

    @patch("tap_workday_raas.discover.enrich_schema_from_data")
    @patch("tap_workday_raas.discover.download_xsd")
    def test_schema_generation_failure_is_caught(self, mock_xsd, mock_enrich):
        """If generate_schema_for_report raises, the report is skipped and error is raised at end."""
        mock_xsd.return_value = "invalid xsd"  # will cause generate_schema_for_report to fail

        config = self._config([
            {"report_url": "http://fake/r1", "report_name": "report_1"},
        ])
        with self.assertRaises(Exception) as ctx:
            discover.discover_streams(config)

        error_msg = str(ctx.exception)
        self.assertIn("report_1", error_msg)
