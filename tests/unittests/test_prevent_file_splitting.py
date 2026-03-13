"""
End-to-end unit tests for SAC-30059: Prevent file splitting.

These tests use realistic XSD definitions (similar to actual Workday reports)
to verify that:

    1. A single Workday report always produces a single output dataset.
    2. All columns from the source report are present in the output.
    3. Row count matches source (no duplicates introduced by parent/child splitting).

No real Workday credentials are needed – all API calls are mocked.
"""

import unittest
from unittest.mock import patch, MagicMock

from tap_workday_raas import discover
from tap_workday_raas.sync import sync_report, flatten_record


# ---------------------------------------------------------------------------
# XSD fixtures – realistic Workday report schemas
# ---------------------------------------------------------------------------

# Simple 7-column report with ONE sub-group (the existing test XSD).
XSD_SINGLE_SUBGROUP = """\
<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:wd="urn:com.workday.report/Test_Report"
            elementFormDefault="qualified" attributeFormDefault="qualified"
            targetNamespace="urn:com.workday.report/Test_Report">
    <xsd:element name="Report_Data" type="wd:Report_DataType"/>
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
            <xsd:element name="Average_Pay" type="xsd:decimal" minOccurs="0"/>
            <xsd:element name="Job_Profile_ID" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Languages" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Candidate_Details_group" type="wd:Candidate_Details_groupType"
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

# Realistic 20-column report with TWO sub-groups (mirrors customer scenario).
# 15 top-level columns + Compensation_group (3 cols) + Location_group (2 cols) = 20 total.
XSD_MULTI_SUBGROUP_20_COLS = """\
<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:wd="urn:com.workday.report/Multi_Group_Report"
            elementFormDefault="qualified" attributeFormDefault="qualified"
            targetNamespace="urn:com.workday.report/Multi_Group_Report">
    <xsd:element name="Report_Data" type="wd:Report_DataType"/>

    <xsd:complexType name="Compensation_groupType">
        <xsd:sequence>
            <xsd:element name="Pay_Rate" type="xsd:decimal" minOccurs="0"/>
            <xsd:element name="Currency" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Frequency" type="xsd:string" minOccurs="0"/>
        </xsd:sequence>
    </xsd:complexType>

    <xsd:complexType name="Location_groupType">
        <xsd:sequence>
            <xsd:element name="Country" type="xsd:string" minOccurs="0"/>
            <xsd:element name="City" type="xsd:string" minOccurs="0"/>
        </xsd:sequence>
    </xsd:complexType>

    <xsd:complexType name="Report_EntryType">
        <xsd:sequence>
            <xsd:element name="Employee_ID"   type="xsd:string"  minOccurs="0"/>
            <xsd:element name="First_Name"    type="xsd:string"  minOccurs="0"/>
            <xsd:element name="Last_Name"     type="xsd:string"  minOccurs="0"/>
            <xsd:element name="Email"         type="xsd:string"  minOccurs="0"/>
            <xsd:element name="Phone"         type="xsd:string"  minOccurs="0"/>
            <xsd:element name="Hire_Date"     type="xsd:date"    minOccurs="0"/>
            <xsd:element name="Department"    type="xsd:string"  minOccurs="0"/>
            <xsd:element name="Division"      type="xsd:string"  minOccurs="0"/>
            <xsd:element name="Manager_Name"  type="xsd:string"  minOccurs="0"/>
            <xsd:element name="Job_Title"     type="xsd:string"  minOccurs="0"/>
            <xsd:element name="Job_Family"    type="xsd:string"  minOccurs="0"/>
            <xsd:element name="Job_Level"     type="xsd:string"  minOccurs="0"/>
            <xsd:element name="Status"        type="xsd:string"  minOccurs="0"/>
            <xsd:element name="FTE"           type="xsd:decimal" minOccurs="0"/>
            <xsd:element name="Cost_Center"   type="xsd:string"  minOccurs="0"/>
            <xsd:element name="Compensation_group" type="wd:Compensation_groupType"
                         minOccurs="0" maxOccurs="unbounded"/>
            <xsd:element name="Location_group" type="wd:Location_groupType"
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

# XSD with a single, non-list sub-object (maxOccurs absent → not an array).
XSD_SINGLE_OBJECT_SUBGROUP = """\
<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:wd="urn:com.workday.report/Single_Object"
            elementFormDefault="qualified" attributeFormDefault="qualified"
            targetNamespace="urn:com.workday.report/Single_Object">
    <xsd:element name="Report_Data" type="wd:Report_DataType"/>

    <xsd:complexType name="Address_groupType">
        <xsd:sequence>
            <xsd:element name="Street" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Zip" type="xsd:string" minOccurs="0"/>
        </xsd:sequence>
    </xsd:complexType>

    <xsd:complexType name="Report_EntryType">
        <xsd:sequence>
            <xsd:element name="Name" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Address_group" type="wd:Address_groupType" minOccurs="0"/>
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

# XSD with NO sub-groups – purely flat. Should pass through unchanged.
XSD_FLAT_ONLY = """\
<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:wd="urn:com.workday.report/Flat_Report"
            elementFormDefault="qualified" attributeFormDefault="qualified"
            targetNamespace="urn:com.workday.report/Flat_Report">
    <xsd:element name="Report_Data" type="wd:Report_DataType"/>

    <xsd:complexType name="Report_EntryType">
        <xsd:sequence>
            <xsd:element name="Employee_ID" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Full_Name"   type="xsd:string" minOccurs="0"/>
            <xsd:element name="Salary"      type="xsd:decimal" minOccurs="0"/>
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

# XSD with nested sub-group inside sub-group (3 levels deep).
XSD_DEEPLY_NESTED = """\
<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:wd="urn:com.workday.report/Deep_Report"
            elementFormDefault="qualified" attributeFormDefault="qualified"
            targetNamespace="urn:com.workday.report/Deep_Report">
    <xsd:element name="Report_Data" type="wd:Report_DataType"/>

    <xsd:complexType name="Inner_groupType">
        <xsd:sequence>
            <xsd:element name="Deep_Value" type="xsd:string" minOccurs="0"/>
        </xsd:sequence>
    </xsd:complexType>

    <xsd:complexType name="Outer_groupType">
        <xsd:sequence>
            <xsd:element name="Mid_Value" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Inner_group" type="wd:Inner_groupType"
                         minOccurs="0" maxOccurs="unbounded"/>
        </xsd:sequence>
    </xsd:complexType>

    <xsd:complexType name="Report_EntryType">
        <xsd:sequence>
            <xsd:element name="Top_Value" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Outer_group" type="wd:Outer_groupType"
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
    """Build a mock Singer CatalogEntry-like object."""
    stream = MagicMock()
    stream.tap_stream_id = tap_stream_id

    schema_obj = MagicMock()
    schema_obj.to_dict.return_value = schema
    stream.schema = schema_obj

    if metadata_list is None:
        metadata_list = [{"breadcrumb": [], "metadata": {}}]
    stream.metadata = metadata_list
    return stream


def _assert_schema_is_flat(test_case, schema):
    """Assert that *no* property in schema is a nested object or array-of-objects."""
    for name, prop in schema["properties"].items():
        ptype = prop.get("type")
        if ptype == "object" and "properties" in prop:
            test_case.fail(
                "Property '{}' is a nested object – schema is NOT flat".format(name))
        if ptype == "array":
            items = prop.get("items", {})
            if items.get("type") == "object" and "properties" in items:
                test_case.fail(
                    "Property '{}' is an array of objects – schema is NOT flat".format(name))


# ===================================================================
# AC-1: Single Workday report produces single output dataset
# ===================================================================

class TestSingleReportSingleDataset(unittest.TestCase):
    """Verify that discover_streams returns exactly ONE stream per report
    and that the schema is completely flat (no nested objects that a target
    would interpret as a child table)."""

    @patch("tap_workday_raas.discover.enrich_schema_from_data")
    @patch("tap_workday_raas.discover.download_xsd")
    def test_single_subgroup_xsd_produces_one_flat_stream(self, mock_xsd, mock_enrich):
        mock_xsd.return_value = XSD_SINGLE_SUBGROUP
        mock_enrich.side_effect = lambda s, *a, **kw: s

        streams = discover.discover_streams({
            "username": "u", "password": "p",
            "reports": '[{"report_url": "http://fake", "report_name": "rpt"}]',
        })
        self.assertEqual(len(streams), 1, "Only one stream for one report")
        _assert_schema_is_flat(self, streams[0]["schema"])

    @patch("tap_workday_raas.discover.enrich_schema_from_data")
    @patch("tap_workday_raas.discover.download_xsd")
    def test_multi_subgroup_20col_xsd_produces_one_flat_stream(self, mock_xsd, mock_enrich):
        """The customer's exact scenario: 20-column report with 2 sub-groups."""
        mock_xsd.return_value = XSD_MULTI_SUBGROUP_20_COLS
        mock_enrich.side_effect = lambda s, *a, **kw: s

        streams = discover.discover_streams({
            "username": "u", "password": "p",
            "reports": '[{"report_url": "http://fake", "report_name": "big_rpt"}]',
        })
        self.assertEqual(len(streams), 1)
        _assert_schema_is_flat(self, streams[0]["schema"])

    @patch("tap_workday_raas.discover.enrich_schema_from_data")
    @patch("tap_workday_raas.discover.download_xsd")
    def test_single_object_subgroup_xsd_produces_one_flat_stream(self, mock_xsd, mock_enrich):
        mock_xsd.return_value = XSD_SINGLE_OBJECT_SUBGROUP
        mock_enrich.side_effect = lambda s, *a, **kw: s

        streams = discover.discover_streams({
            "username": "u", "password": "p",
            "reports": '[{"report_url": "http://fake", "report_name": "obj_rpt"}]',
        })
        self.assertEqual(len(streams), 1)
        _assert_schema_is_flat(self, streams[0]["schema"])

    @patch("tap_workday_raas.discover.enrich_schema_from_data")
    @patch("tap_workday_raas.discover.download_xsd")
    def test_flat_only_xsd_still_one_flat_stream(self, mock_xsd, mock_enrich):
        """A report with no sub-groups should still work fine."""
        mock_xsd.return_value = XSD_FLAT_ONLY
        mock_enrich.side_effect = lambda s, *a, **kw: s

        streams = discover.discover_streams({
            "username": "u", "password": "p",
            "reports": '[{"report_url": "http://fake", "report_name": "flat_rpt"}]',
        })
        self.assertEqual(len(streams), 1)
        _assert_schema_is_flat(self, streams[0]["schema"])

    @patch("tap_workday_raas.discover.enrich_schema_from_data")
    @patch("tap_workday_raas.discover.download_xsd")
    def test_deeply_nested_xsd_produces_one_flat_stream(self, mock_xsd, mock_enrich):
        mock_xsd.return_value = XSD_DEEPLY_NESTED
        mock_enrich.side_effect = lambda s, *a, **kw: s

        streams = discover.discover_streams({
            "username": "u", "password": "p",
            "reports": '[{"report_url": "http://fake", "report_name": "deep_rpt"}]',
        })
        self.assertEqual(len(streams), 1)
        _assert_schema_is_flat(self, streams[0]["schema"])


# ===================================================================
# AC-2: All columns from source report present in output
# ===================================================================

class TestAllColumnsPresent(unittest.TestCase):
    """Verify that every column defined in the XSD (including sub-group
    children) is present in the flattened schema."""

    def test_single_subgroup_all_7_columns(self):
        raw = discover.generate_schema_for_report(XSD_SINGLE_SUBGROUP)
        flat = discover.flatten_schema(raw)

        expected_cols = {
            "Default_Job_Title",
            "Average_Pay",
            "Job_Profile_ID",
            "Languages",
            # Flattened children
            "Candidate_Details_group_Employee",
            "Candidate_Details_group_Willing_To_Travel",
            "Candidate_Details_group_Potential",
        }
        self.assertEqual(set(flat["properties"].keys()), expected_cols)

    def test_multi_subgroup_all_20_columns(self):
        """Customer scenario: 15 top-level + 3 comp + 2 location = 20 columns."""
        raw = discover.generate_schema_for_report(XSD_MULTI_SUBGROUP_20_COLS)
        flat = discover.flatten_schema(raw)

        expected_cols = {
            # 15 top-level columns
            "Employee_ID", "First_Name", "Last_Name", "Email", "Phone",
            "Hire_Date", "Department", "Division", "Manager_Name",
            "Job_Title", "Job_Family", "Job_Level", "Status", "FTE",
            "Cost_Center",
            # Compensation_group children (3)
            "Compensation_group_Pay_Rate",
            "Compensation_group_Currency",
            "Compensation_group_Frequency",
            # Location_group children (2)
            "Location_group_Country",
            "Location_group_City",
        }
        self.assertEqual(len(flat["properties"]), 20,
                         "Should have exactly 20 columns, got: {}".format(
                             sorted(flat["properties"].keys())))
        self.assertEqual(set(flat["properties"].keys()), expected_cols)

    def test_single_object_subgroup_all_columns(self):
        raw = discover.generate_schema_for_report(XSD_SINGLE_OBJECT_SUBGROUP)
        flat = discover.flatten_schema(raw)

        expected_cols = {"Name", "Address_group_Street", "Address_group_Zip"}
        self.assertEqual(set(flat["properties"].keys()), expected_cols)

    def test_flat_xsd_columns_unchanged(self):
        raw = discover.generate_schema_for_report(XSD_FLAT_ONLY)
        flat = discover.flatten_schema(raw)

        expected_cols = {"Employee_ID", "Full_Name", "Salary"}
        self.assertEqual(set(flat["properties"].keys()), expected_cols)

    def test_deeply_nested_all_columns_promoted(self):
        """Note: generate_schema_for_report only resolves one level of
        complex-type nesting.  The Inner_group reference inside
        Outer_groupType is treated as an opaque array type (not expanded
        into a nested object), so flatten_schema keeps it as a single
        column.  The sync layer's flatten_record still flattens the actual
        JSON data correctly; the dynamic schema expansion at sync time
        picks up the deeper columns."""
        raw = discover.generate_schema_for_report(XSD_DEEPLY_NESTED)
        flat = discover.flatten_schema(raw)

        # Outer_group children are promoted; Inner_group remains opaque
        expected_cols = {
            "Top_Value",
            "Outer_group_Mid_Value",
            "Outer_group_Inner_group",
        }
        self.assertEqual(set(flat["properties"].keys()), expected_cols)

    def test_column_types_preserved_after_flattening(self):
        """Flattening must not lose type information."""
        raw = discover.generate_schema_for_report(XSD_MULTI_SUBGROUP_20_COLS)
        flat = discover.flatten_schema(raw)

        self.assertEqual(flat["properties"]["Employee_ID"]["type"], ["string", "null"])
        self.assertEqual(flat["properties"]["FTE"]["type"], ["number", "null"])
        self.assertIn("format", flat["properties"]["Hire_Date"])
        self.assertEqual(flat["properties"]["Compensation_group_Pay_Rate"]["type"],
                         ["number", "null"])
        self.assertEqual(flat["properties"]["Location_group_City"]["type"],
                         ["string", "null"])


# ===================================================================
# AC-3: Row count matches source (no duplicates from splitting)
# ===================================================================

@patch("tap_workday_raas.sync.singer")
@patch("tap_workday_raas.sync.stream_report")
class TestRowCountMatchesSource(unittest.TestCase):
    """Verify that sync_report emits exactly one output record per source
    record – no duplication from parent/child splitting."""

    def _config(self):
        return {"username": "u", "password": "p"}

    def _report(self):
        return {"report_url": "http://fake", "report_name": "rpt"}

    def test_single_subgroup_three_rows(self, mock_stream_report, mock_singer):
        """3 Workday rows with sub-group → exactly 3 output records."""
        schema = discover.flatten_schema(
            discover.generate_schema_for_report(XSD_SINGLE_SUBGROUP))
        stream = _make_stream("rpt", schema)

        mock_stream_report.return_value = iter([
            {"Default_Job_Title": "Eng", "Average_Pay": 100,
             "Job_Profile_ID": "JP1", "Languages": "EN",
             "Candidate_Details_group": [
                 {"Employee": "Alice", "Willing_To_Travel": "1", "Potential": "High"}]},
            {"Default_Job_Title": "PM", "Average_Pay": 110,
             "Job_Profile_ID": "JP2", "Languages": "FR",
             "Candidate_Details_group": [
                 {"Employee": "Bob", "Willing_To_Travel": "0", "Potential": "Med"}]},
            {"Default_Job_Title": "DS", "Average_Pay": 120,
             "Job_Profile_ID": "JP3", "Languages": "DE",
             "Candidate_Details_group": [
                 {"Employee": "Carol", "Willing_To_Travel": "1", "Potential": "Low"}]},
        ])

        count = sync_report(self._report(), stream, self._config())

        self.assertEqual(count, 3)
        self.assertEqual(len(mock_singer.RecordMessage.call_args_list), 3)

    def test_multi_subgroup_20col_five_rows(self, mock_stream_report, mock_singer):
        """5 rows of the 20-column report → exactly 5 output records."""
        schema = discover.flatten_schema(
            discover.generate_schema_for_report(XSD_MULTI_SUBGROUP_20_COLS))
        stream = _make_stream("big_rpt", schema)

        rows = []
        for i in range(5):
            rows.append({
                "Employee_ID": str(i), "First_Name": "F{}".format(i),
                "Last_Name": "L{}".format(i), "Email": "e{}@x.com".format(i),
                "Phone": "555-000{}".format(i), "Hire_Date": "2024-01-0{}".format(i + 1),
                "Department": "Dept", "Division": "Div", "Manager_Name": "Mgr",
                "Job_Title": "JT", "Job_Family": "JF", "Job_Level": "JL",
                "Status": "Active", "FTE": 1.0, "Cost_Center": "CC{}".format(i),
                "Compensation_group": [
                    {"Pay_Rate": 50000 + i, "Currency": "USD", "Frequency": "Annual"}],
                "Location_group": [
                    {"Country": "US", "City": "NYC"}],
            })
        mock_stream_report.return_value = iter(rows)

        count = sync_report(self._report(), stream, self._config())

        self.assertEqual(count, 5, "5 source rows must produce exactly 5 output records")
        self.assertEqual(len(mock_singer.RecordMessage.call_args_list), 5)

    def test_flat_report_row_count_unchanged(self, mock_stream_report, mock_singer):
        """Flat report (no sub-groups) also produces correct row count."""
        schema = discover.flatten_schema(
            discover.generate_schema_for_report(XSD_FLAT_ONLY))
        stream = _make_stream("flat_rpt", schema)

        mock_stream_report.return_value = iter([
            {"Employee_ID": "1", "Full_Name": "A", "Salary": 100},
            {"Employee_ID": "2", "Full_Name": "B", "Salary": 200},
        ])

        count = sync_report(self._report(), stream, self._config())
        self.assertEqual(count, 2)

    def test_row_with_empty_subgroup_still_one_record(self, mock_stream_report, mock_singer):
        """A row where the sub-group array is empty → still one output record."""
        schema = discover.flatten_schema(
            discover.generate_schema_for_report(XSD_SINGLE_SUBGROUP))
        stream = _make_stream("rpt", schema)

        mock_stream_report.return_value = iter([
            {"Default_Job_Title": "Eng", "Average_Pay": 100,
             "Job_Profile_ID": "JP1", "Languages": "EN",
             "Candidate_Details_group": []},
        ])

        count = sync_report(self._report(), stream, self._config())
        self.assertEqual(count, 1)

    def test_row_with_missing_subgroup_still_one_record(self, mock_stream_report, mock_singer):
        """A row where the sub-group key is absent → still one output record."""
        schema = discover.flatten_schema(
            discover.generate_schema_for_report(XSD_SINGLE_SUBGROUP))
        stream = _make_stream("rpt", schema)

        mock_stream_report.return_value = iter([
            {"Default_Job_Title": "Eng", "Average_Pay": 100,
             "Job_Profile_ID": "JP1", "Languages": "EN"},
        ])

        count = sync_report(self._report(), stream, self._config())
        self.assertEqual(count, 1)


# ===================================================================
# Full pipeline: XSD → discover schema → sync records → verify output
# ===================================================================

@patch("tap_workday_raas.sync.singer")
@patch("tap_workday_raas.sync.stream_report")
class TestEndToEndPipeline(unittest.TestCase):
    """Simulate the full tap pipeline (discovery + sync) and verify
    every output record is flat with all columns present."""

    def _config(self):
        return {"username": "u", "password": "p"}

    def _report(self):
        return {"report_url": "http://fake", "report_name": "rpt"}

    def test_20col_report_output_has_all_columns_flat(self, mock_stream_report, mock_singer):
        """The customer's 20-column report: all columns must appear in
        every output record, with none nested."""
        schema = discover.flatten_schema(
            discover.generate_schema_for_report(XSD_MULTI_SUBGROUP_20_COLS))
        stream = _make_stream("big_rpt", schema)

        mock_stream_report.return_value = iter([
            {
                "Employee_ID": "E001", "First_Name": "Jane", "Last_Name": "Doe",
                "Email": "j@x.com", "Phone": "555", "Hire_Date": "2020-01-15",
                "Department": "Eng", "Division": "Tech", "Manager_Name": "Boss",
                "Job_Title": "SWE", "Job_Family": "Engineering",
                "Job_Level": "IC3", "Status": "Active",
                "FTE": 1.0, "Cost_Center": "CC100",
                "Compensation_group": [
                    {"Pay_Rate": 120000, "Currency": "USD", "Frequency": "Annual"}],
                "Location_group": [
                    {"Country": "US", "City": "San Francisco"}],
            },
        ])

        sync_report(self._report(), stream, self._config())

        rm_calls = mock_singer.RecordMessage.call_args_list
        self.assertEqual(len(rm_calls), 1)
        record = rm_calls[0][0][1]

        # Verify all 20 columns present
        expected_cols = {
            "Employee_ID", "First_Name", "Last_Name", "Email", "Phone",
            "Hire_Date", "Department", "Division", "Manager_Name",
            "Job_Title", "Job_Family", "Job_Level", "Status", "FTE",
            "Cost_Center",
            "Compensation_group_Pay_Rate", "Compensation_group_Currency",
            "Compensation_group_Frequency",
            "Location_group_Country", "Location_group_City",
        }
        for col in expected_cols:
            self.assertIn(col, record,
                          "Column '{}' missing from output record".format(col))

        # Verify values are correct
        self.assertEqual(record["Employee_ID"], "E001")
        self.assertEqual(record["Compensation_group_Pay_Rate"], 120000)
        self.assertEqual(record["Location_group_City"], "San Francisco")

        # Verify NO nested structures remain
        for key, val in record.items():
            if key == "_sdc_extracted_at":
                continue
            self.assertNotIsInstance(val, dict,
                                    "Record key '{}' should not be a dict".format(key))
            self.assertNotIsInstance(val, list,
                                    "Record key '{}' should not be a list".format(key))

    def test_subgroup_absent_columns_null_filled(self, mock_stream_report, mock_singer):
        """When a row has no sub-group data, flattened child columns
        must still be present as None."""
        schema = discover.flatten_schema(
            discover.generate_schema_for_report(XSD_SINGLE_SUBGROUP))
        stream = _make_stream("rpt", schema)

        mock_stream_report.return_value = iter([
            {"Default_Job_Title": "PM", "Average_Pay": 90,
             "Job_Profile_ID": "JP9", "Languages": "ES"},
            # No Candidate_Details_group at all
        ])

        sync_report(self._report(), stream, self._config())

        rm_calls = mock_singer.RecordMessage.call_args_list
        record = rm_calls[0][0][1]

        # The child columns should be None
        self.assertIn("Candidate_Details_group_Employee", record)
        self.assertIsNone(record["Candidate_Details_group_Employee"])
        self.assertIn("Candidate_Details_group_Willing_To_Travel", record)
        self.assertIsNone(record["Candidate_Details_group_Willing_To_Travel"])
        self.assertIn("Candidate_Details_group_Potential", record)
        self.assertIsNone(record["Candidate_Details_group_Potential"])

    def test_mixed_rows_with_and_without_subgroup(self, mock_stream_report, mock_singer):
        """Mix of rows: some with sub-group data, some without. All must
        produce one record each, all having the same set of columns."""
        schema = discover.flatten_schema(
            discover.generate_schema_for_report(XSD_SINGLE_SUBGROUP))
        stream = _make_stream("rpt", schema)

        mock_stream_report.return_value = iter([
            # Row with sub-group
            {"Default_Job_Title": "Eng", "Average_Pay": 100,
             "Job_Profile_ID": "JP1", "Languages": "EN",
             "Candidate_Details_group": [
                 {"Employee": "Alice", "Willing_To_Travel": "1", "Potential": "High"}]},
            # Row WITHOUT sub-group
            {"Default_Job_Title": "PM", "Average_Pay": 90,
             "Job_Profile_ID": "JP2", "Languages": "FR"},
            # Row with empty sub-group
            {"Default_Job_Title": "DS", "Average_Pay": 120,
             "Job_Profile_ID": "JP3", "Languages": "DE",
             "Candidate_Details_group": []},
        ])

        count = sync_report(self._report(), stream, self._config())

        # All 3 rows emitted
        self.assertEqual(count, 3)

        rm_calls = mock_singer.RecordMessage.call_args_list
        self.assertEqual(len(rm_calls), 3)

        # Collect column sets from each record (excluding _sdc_extracted_at)
        column_sets = []
        for call in rm_calls:
            rec = call[0][1]
            cols = {k for k in rec.keys() if not k.startswith("_sdc_")}
            column_sets.append(cols)

        # All records must have the same column set
        self.assertEqual(column_sets[0], column_sets[1])
        self.assertEqual(column_sets[1], column_sets[2])

        # First record has sub-group values
        self.assertEqual(rm_calls[0][0][1]["Candidate_Details_group_Employee"], "Alice")
        # Second and third records have null-filled sub-group columns
        self.assertIsNone(rm_calls[1][0][1]["Candidate_Details_group_Employee"])
        self.assertIsNone(rm_calls[2][0][1]["Candidate_Details_group_Employee"])

    def test_deeply_nested_xsd_end_to_end(self, mock_stream_report, mock_singer):
        """Even 3-level-deep nesting is flattened correctly end-to-end."""
        schema = discover.flatten_schema(
            discover.generate_schema_for_report(XSD_DEEPLY_NESTED))
        stream = _make_stream("deep_rpt", schema)

        mock_stream_report.return_value = iter([
            {
                "Top_Value": "tv",
                "Outer_group": [
                    {"Mid_Value": "mv", "Inner_group": [{"Deep_Value": "dv"}]}
                ],
            },
        ])

        sync_report(self._report(), stream, self._config())

        rm_calls = mock_singer.RecordMessage.call_args_list
        self.assertEqual(len(rm_calls), 1)
        record = rm_calls[0][0][1]

        self.assertEqual(record["Top_Value"], "tv")
        self.assertEqual(record["Outer_group_Mid_Value"], "mv")
        self.assertEqual(record["Outer_group_Inner_group_Deep_Value"], "dv")

        # No nested structures
        for key, val in record.items():
            if key == "_sdc_extracted_at":
                continue
            self.assertNotIsInstance(val, (dict, list),
                                    "Key '{}' should not be nested".format(key))


# ===================================================================
# Regression: flatten_record handles Workday JSON edge-cases
# ===================================================================

class TestFlattenRecordWorkdayEdgeCases(unittest.TestCase):
    """Test flatten_record with data shapes that Workday APIs actually return."""

    def test_subgroup_with_multiple_items_last_wins(self):
        """When the API returns multiple items in a sub-group array, the
        last item's values are used (since flattening merges into one row)."""
        record = {
            "Employee_ID": "E001",
            "Compensation_group": [
                {"Pay_Rate": 100, "Currency": "USD"},
                {"Pay_Rate": 200, "Currency": "EUR"},
            ],
        }
        flat = flatten_record(record)
        # Last item wins
        self.assertEqual(flat["Compensation_group_Pay_Rate"], 200)
        self.assertEqual(flat["Compensation_group_Currency"], "EUR")

    def test_null_in_subgroup_child(self):
        record = {
            "Name": "Test",
            "group": [{"child_a": "val", "child_b": None}],
        }
        flat = flatten_record(record)
        self.assertEqual(flat["group_child_a"], "val")
        self.assertIsNone(flat["group_child_b"])

    def test_numeric_values_in_subgroup_preserved(self):
        record = {
            "Employee_ID": "E001",
            "group": [{"amount": 1234.56, "count": 3}],
        }
        flat = flatten_record(record)
        self.assertEqual(flat["group_amount"], 1234.56)
        self.assertEqual(flat["group_count"], 3)

    def test_boolean_values_in_subgroup_preserved(self):
        record = {
            "ID": "1",
            "group": [{"active": True, "deleted": False}],
        }
        flat = flatten_record(record)
        self.assertTrue(flat["group_active"])
        self.assertFalse(flat["group_deleted"])

    def test_empty_string_in_subgroup_preserved(self):
        record = {
            "ID": "1",
            "group": [{"val": ""}],
        }
        flat = flatten_record(record)
        self.assertEqual(flat["group_val"], "")

