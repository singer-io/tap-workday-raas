import unittest
from unittest.mock import patch
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


# ---------------------------------------------------------------------------
# Tests for flatten_schema
# ---------------------------------------------------------------------------

class TestFlattenSchema(unittest.TestCase):
    """Verify that flatten_schema promotes nested object/array-of-object
    properties to the top level, preventing parent/child splitting."""

    def test_already_flat_schema_unchanged(self):
        schema = {
            "type": "object",
            "properties": {
                "col_a": {"type": ["string", "null"]},
                "col_b": {"type": ["number", "null"]},
            }
        }
        result = discover.flatten_schema(schema)
        self.assertEqual(result, schema)

    def test_nested_object_flattened(self):
        schema = {
            "type": "object",
            "properties": {
                "col_a": {"type": ["string", "null"]},
                "group": {
                    "type": "object",
                    "properties": {
                        "child1": {"type": ["string", "null"]},
                        "child2": {"type": ["number", "null"]},
                    }
                },
            }
        }
        result = discover.flatten_schema(schema)
        self.assertNotIn("group", result["properties"])
        self.assertEqual(result["properties"]["col_a"], {"type": ["string", "null"]})
        self.assertEqual(result["properties"]["group_child1"], {"type": ["string", "null"]})
        self.assertEqual(result["properties"]["group_child2"], {"type": ["number", "null"]})

    def test_array_of_objects_flattened(self):
        schema = {
            "type": "object",
            "properties": {
                "col_a": {"type": ["string", "null"]},
                "group": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "child1": {"type": ["string", "null"]},
                            "child2": {"type": ["number", "null"]},
                        }
                    }
                },
            }
        }
        result = discover.flatten_schema(schema)
        self.assertNotIn("group", result["properties"])
        self.assertEqual(result["properties"]["group_child1"], {"type": ["string", "null"]})
        self.assertEqual(result["properties"]["group_child2"], {"type": ["number", "null"]})

    def test_array_of_primitives_kept(self):
        schema = {
            "type": "object",
            "properties": {
                "tags": {"type": "array", "items": {"type": ["string", "null"]}},
            }
        }
        result = discover.flatten_schema(schema)
        self.assertEqual(result["properties"]["tags"],
                         {"type": "array", "items": {"type": ["string", "null"]}})

    def test_deeply_nested_flattened(self):
        schema = {
            "type": "object",
            "properties": {
                "level1": {
                    "type": "object",
                    "properties": {
                        "level2": {
                            "type": "object",
                            "properties": {
                                "value": {"type": ["string", "null"]},
                            }
                        }
                    }
                }
            }
        }
        result = discover.flatten_schema(schema)
        self.assertEqual(list(result["properties"].keys()), ["level1_level2_value"])
        self.assertEqual(result["properties"]["level1_level2_value"],
                         {"type": ["string", "null"]})

    def test_xsd_schema_flattened(self):
        """The test XSD's Candidate_Details_group array should be flattened."""
        nested_schema = discover.generate_schema_for_report(xsd)
        result = discover.flatten_schema(nested_schema)

        # The nested array property should be gone
        self.assertNotIn("Candidate_Details_group", result["properties"])

        # Its child properties should be promoted
        self.assertIn("Candidate_Details_group_Employee", result["properties"])
        self.assertIn("Candidate_Details_group_Willing_To_Travel", result["properties"])
        self.assertIn("Candidate_Details_group_Potential", result["properties"])

        # Top-level properties should still be present
        self.assertIn("Default_Job_Title", result["properties"])
        self.assertIn("Average_Pay_-_Amount", result["properties"])
        self.assertIn("job_profile_id", result["properties"])
        self.assertIn("Languages", result["properties"])
        self.assertIn("Default_Assessment_Tests", result["properties"])
        self.assertIn("Business_Unit_or_Business_Unit_Hierarchy_Container",
                       result["properties"])

        # Total: 6 top-level + 3 promoted = 9
        self.assertEqual(len(result["properties"]), 9)

    def test_non_object_schema_returned_as_is(self):
        schema = {"type": "string"}
        self.assertEqual(discover.flatten_schema(schema), schema)

    def test_schema_without_properties_returned_as_is(self):
        schema = {"type": "object"}
        self.assertEqual(discover.flatten_schema(schema), schema)


class TestDiscoverStreamsProducesFlatSchema(unittest.TestCase):
    """Verify that discover_streams returns a flat schema (no nesting)."""

    @patch("tap_workday_raas.discover.enrich_schema_from_data")
    @patch("tap_workday_raas.discover.download_xsd")
    def test_discover_streams_returns_flat_schema(self, mock_xsd, mock_enrich):
        mock_xsd.return_value = xsd
        mock_enrich.side_effect = lambda schema, *a, **kw: schema

        config = {
            "username": "user",
            "password": "pass",
            "reports": '[{"report_url": "http://fake", "report_name": "test_report"}]',
        }
        streams = discover.discover_streams(config)
        schema = streams[0]["schema"]

        # Should be flat – no nested object or array-of-object properties
        for prop_name, prop_schema in schema["properties"].items():
            prop_type = prop_schema.get("type")
            if prop_type == "object":
                self.fail("Property '{}' is a nested object; schema should be flat".format(prop_name))
            if prop_type == "array" and prop_schema.get("items", {}).get("type") == "object":
                self.fail("Property '{}' is an array of objects; schema should be flat".format(prop_name))

        # The Candidate_Details_group children should be promoted
        self.assertIn("Candidate_Details_group_Employee", schema["properties"])
        self.assertIn("Candidate_Details_group_Willing_To_Travel", schema["properties"])
        self.assertIn("Candidate_Details_group_Potential", schema["properties"])