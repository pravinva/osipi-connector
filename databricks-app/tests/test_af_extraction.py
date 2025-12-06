"""
Comprehensive test suite for AFHierarchyExtractor

Tests cover:
- Listing AF databases
- Simple hierarchy extraction (2-3 levels)
- Recursive traversal logic
- Max depth limit (prevents infinite loops)
- Element attribute extraction
- Path construction (/Enterprise/Site1/Unit2)
- Empty children handling

Follows TESTER.md specifications exactly.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, MagicMock, patch, call
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.extractors.af_extractor import AFHierarchyExtractor
from tests.fixtures.sample_responses import (
    SAMPLE_AF_ELEMENT,
    SAMPLE_AF_ELEMENT_WITH_CHILDREN,
    SAMPLE_AF_ELEMENTS_LIST,
    SAMPLE_ATTRIBUTES_LIST,
    SAMPLE_DEEP_HIERARCHY,
    SAMPLE_EMPTY_RESPONSE
)


class TestAFHierarchyExtractor:
    """
    Test PI Asset Framework hierarchy extraction
    Success Criteria: 5/5 tests pass per TESTER.md
    """

    @pytest.fixture
    def mock_client(self):
        """Create mock PI Web API client"""
        client = Mock()
        client.get = Mock()
        return client

    @pytest.fixture
    def extractor(self, mock_client):
        """Create AFHierarchyExtractor instance"""
        return AFHierarchyExtractor(mock_client)

    def test_get_asset_databases(self, extractor, mock_client):
        """
        Test listing AF databases
        Validates: Can discover available AF databases
        """
        # Mock databases response
        mock_response = Mock()
        mock_response.json.return_value = {
            "Items": [
                {"WebId": "F1DP-DB1", "Name": "ProductionDB"},
                {"WebId": "F1DP-DB2", "Name": "UtilitiesDB"}
            ]
        }
        mock_client.get.return_value = mock_response

        databases = extractor.get_asset_databases()

        # Assertions
        assert len(databases) == 2
        assert databases[0]["Name"] == "ProductionDB"
        assert databases[1]["Name"] == "UtilitiesDB"

        # Verify correct endpoint called
        mock_client.get.assert_called_once_with("/piwebapi/assetdatabases")

        print("✓ AF database listing works")

    def test_extract_simple_hierarchy(self, extractor, mock_client):
        """
        Test extracting 2-level hierarchy (Site -> Units)
        Validates: Recursive traversal, path construction
        """
        # Setup mock responses for hierarchy traversal
        def get_side_effect(endpoint, **kwargs):
            mock_response = Mock()

            # Root elements request
            if "assetdatabases" in endpoint and endpoint.endswith("/elements"):
                mock_response.json.return_value = {
                    "Items": [
                        {"WebId": "F1DP-Site1", "Name": "Sydney_Plant"}
                    ]
                }

            # Site element details
            elif "elements/F1DP-Site1" in endpoint and not endpoint.endswith("/elements"):
                mock_response.json.return_value = {
                    "WebId": "F1DP-Site1",
                    "Name": "Sydney_Plant",
                    "TemplateName": "PlantTemplate",
                    "Type": "Element",
                    "Description": "Main production facility",
                    "CategoryNames": ["Production Site"]
                }

            # Site children
            elif "elements/F1DP-Site1/elements" in endpoint:
                mock_response.json.return_value = {
                    "Items": [
                        {"WebId": "F1DP-Unit1", "Name": "Unit_1"},
                        {"WebId": "F1DP-Unit2", "Name": "Unit_2"}
                    ]
                }

            # Unit 1 details
            elif "elements/F1DP-Unit1" in endpoint and not endpoint.endswith("/elements"):
                mock_response.json.return_value = {
                    "WebId": "F1DP-Unit1",
                    "Name": "Unit_1",
                    "TemplateName": "UnitTemplate",
                    "Type": "Element",
                    "Description": "Production Unit 1",
                    "CategoryNames": ["Manufacturing"]
                }

            # Unit 2 details
            elif "elements/F1DP-Unit2" in endpoint and not endpoint.endswith("/elements"):
                mock_response.json.return_value = {
                    "WebId": "F1DP-Unit2",
                    "Name": "Unit_2",
                    "TemplateName": "UnitTemplate",
                    "Type": "Element",
                    "Description": "Production Unit 2",
                    "CategoryNames": ["Manufacturing"]
                }

            # Unit children (empty - leaf nodes)
            elif endpoint.endswith("/elements"):
                mock_response.json.return_value = {"Items": []}

            else:
                mock_response.json.return_value = {"Items": []}

            return mock_response

        mock_client.get.side_effect = get_side_effect

        # Extract hierarchy
        df = extractor.extract_hierarchy(database_webid="F1DP-DB1")

        # Assertions
        assert len(df) == 3, f"Expected 3 elements (Site + 2 Units), got {len(df)}"
        assert set(df['element_name']) == {"Sydney_Plant", "Unit_1", "Unit_2"}

        # Check paths are correctly constructed
        site_row = df[df['element_name'] == 'Sydney_Plant'].iloc[0]
        assert site_row['element_path'] == '/Sydney_Plant'
        assert site_row['depth'] == 0

        unit1_row = df[df['element_name'] == 'Unit_1'].iloc[0]
        assert unit1_row['element_path'] == '/Sydney_Plant/Unit_1'
        assert unit1_row['depth'] == 1

        unit2_row = df[df['element_name'] == 'Unit_2'].iloc[0]
        assert unit2_row['element_path'] == '/Sydney_Plant/Unit_2'
        assert unit2_row['depth'] == 1

        # Verify template names
        assert site_row['template_name'] == 'PlantTemplate'
        assert unit1_row['template_name'] == 'UnitTemplate'

        print("✓ Simple hierarchy extraction works")
        print(f"✓ Extracted {len(df)} elements with correct paths")

    def test_max_depth_limit(self, extractor, mock_client):
        """
        Test max depth prevents infinite recursion
        Validates: Safety mechanism against circular references
        Critical for production robustness
        """
        # Mock circular/deep reference scenario
        call_count = 0

        def get_circular(endpoint, **kwargs):
            nonlocal call_count
            call_count += 1

            mock_response = Mock()

            # Database root
            if "assetdatabases" in endpoint and endpoint.endswith("/elements"):
                mock_response.json.return_value = {
                    "Items": [{"WebId": "F1DP-Root", "Name": "Root"}]
                }

            # Element details
            elif not endpoint.endswith("/elements"):
                mock_response.json.return_value = {
                    "WebId": "F1DP-Root",
                    "Name": f"Element_Depth_{call_count}",
                    "TemplateName": "DeepTemplate",
                    "Type": "Element",
                    "Description": "Deep element",
                    "CategoryNames": []
                }

            # Children (always returns more children - infinite)
            else:
                mock_response.json.return_value = {
                    "Items": [{"WebId": f"F1DP-Child{call_count}", "Name": f"Child{call_count}"}]
                }

            return mock_response

        mock_client.get.side_effect = get_circular

        # Extract with max_depth=3
        df = extractor.extract_hierarchy(
            database_webid="F1DP-DB1",
            max_depth=3
        )

        # Should stop at max_depth, not infinite loop
        assert len(df) <= 10, f"Max depth limit failed, got {len(df)} elements"
        assert df['depth'].max() <= 3, "Depth should not exceed max_depth"

        print(f"✓ Max depth limit works: stopped at depth {df['depth'].max()}")

    def test_extract_element_attributes(self, extractor, mock_client):
        """
        Test attribute extraction for an element
        Validates: Can get metadata about available data points
        """
        # Mock attributes response
        mock_response = Mock()
        mock_response.json.return_value = {
            "Items": [
                {
                    "WebId": "F1DP-Attr1",
                    "Name": "Temperature",
                    "DataReferencePlugIn": "PI Point",
                    "DefaultUnitsName": "degC",
                    "Type": "Double",
                    "IsConfigurationItem": False
                },
                {
                    "WebId": "F1DP-Attr2",
                    "Name": "Pressure",
                    "DataReferencePlugIn": "PI Point",
                    "DefaultUnitsName": "bar",
                    "Type": "Double",
                    "IsConfigurationItem": False
                },
                {
                    "WebId": "F1DP-Attr3",
                    "Name": "FlowRate",
                    "DataReferencePlugIn": "PI Point",
                    "DefaultUnitsName": "m3/h",
                    "Type": "Double",
                    "IsConfigurationItem": False
                }
            ]
        }
        mock_client.get.return_value = mock_response

        # Extract attributes
        df = extractor.extract_element_attributes("F1DP-Element1")

        # Assertions
        assert len(df) == 3
        assert "Temperature" in df['attribute_name'].values
        assert "Pressure" in df['attribute_name'].values
        assert "FlowRate" in df['attribute_name'].values

        # Check units
        temp_attr = df[df['attribute_name'] == 'Temperature'].iloc[0]
        assert temp_attr['default_uom'] == 'degC'

        press_attr = df[df['attribute_name'] == 'Pressure'].iloc[0]
        assert press_attr['default_uom'] == 'bar'

        # Verify correct endpoint called
        mock_client.get.assert_called_once()
        call_args = mock_client.get.call_args[0][0]
        assert "attributes" in call_args

        print("✓ Attribute extraction works")

    def test_empty_hierarchy_handling(self, extractor, mock_client):
        """
        Test handling of empty hierarchy (no elements in database)
        Validates: Graceful handling of edge case
        """
        # Mock empty response
        mock_response = Mock()
        mock_response.json.return_value = {"Items": []}
        mock_client.get.return_value = mock_response

        df = extractor.extract_hierarchy(database_webid="F1DP-EmptyDB")

        # Should return empty DataFrame, not error
        assert isinstance(df, pd.DataFrame)
        assert df.empty

        print("✓ Empty hierarchy handled gracefully")


class TestAFHierarchyExtractorEdgeCases:
    """Additional edge case tests for robustness"""

    @pytest.fixture
    def mock_client(self):
        client = Mock()
        client.get = Mock()
        return client

    @pytest.fixture
    def extractor(self, mock_client):
        return AFHierarchyExtractor(mock_client)

    def test_deep_hierarchy_3_levels(self, extractor, mock_client):
        """
        Test 3-level hierarchy: Site -> Unit -> Equipment
        Validates: Deeper hierarchies work correctly
        """
        call_count = 0

        def get_side_effect(endpoint, **kwargs):
            nonlocal call_count
            mock_response = Mock()

            # Root
            if "assetdatabases" in endpoint and endpoint.endswith("/elements"):
                mock_response.json.return_value = {
                    "Items": [{"WebId": "F1DP-Site", "Name": "Site"}]
                }

            # Site details
            elif "F1DP-Site" in endpoint and not endpoint.endswith("/elements"):
                mock_response.json.return_value = {
                    "WebId": "F1DP-Site",
                    "Name": "Site",
                    "TemplateName": "SiteTemplate",
                    "Type": "Element",
                    "CategoryNames": []
                }

            # Site children
            elif "F1DP-Site/elements" in endpoint:
                mock_response.json.return_value = {
                    "Items": [{"WebId": "F1DP-Unit", "Name": "Unit"}]
                }

            # Unit details
            elif "F1DP-Unit" in endpoint and not endpoint.endswith("/elements"):
                mock_response.json.return_value = {
                    "WebId": "F1DP-Unit",
                    "Name": "Unit",
                    "TemplateName": "UnitTemplate",
                    "Type": "Element",
                    "CategoryNames": []
                }

            # Unit children
            elif "F1DP-Unit/elements" in endpoint:
                mock_response.json.return_value = {
                    "Items": [{"WebId": "F1DP-Pump", "Name": "Pump"}]
                }

            # Pump details
            elif "F1DP-Pump" in endpoint and not endpoint.endswith("/elements"):
                mock_response.json.return_value = {
                    "WebId": "F1DP-Pump",
                    "Name": "Pump",
                    "TemplateName": "PumpTemplate",
                    "Type": "Element",
                    "CategoryNames": []
                }

            # Pump children (leaf)
            elif "F1DP-Pump/elements" in endpoint:
                mock_response.json.return_value = {"Items": []}

            else:
                mock_response.json.return_value = {"Items": []}

            return mock_response

        mock_client.get.side_effect = get_side_effect

        df = extractor.extract_hierarchy(database_webid="F1DP-DB1")

        # Should have 3 elements at depths 0, 1, 2
        assert len(df) == 3
        assert df['depth'].min() == 0
        assert df['depth'].max() == 2

        # Check paths
        pump_row = df[df['element_name'] == 'Pump'].iloc[0]
        assert pump_row['element_path'] == '/Site/Unit/Pump'
        assert pump_row['depth'] == 2

        print("✓ Deep hierarchy (3 levels) works")

    def test_multiple_children_per_level(self, extractor, mock_client):
        """
        Test element with multiple children (breadth)
        Validates: Handles wide hierarchies
        """
        def get_side_effect(endpoint, **kwargs):
            mock_response = Mock()

            # Root
            if "assetdatabases" in endpoint and endpoint.endswith("/elements"):
                mock_response.json.return_value = {
                    "Items": [{"WebId": "F1DP-Root", "Name": "Root"}]
                }

            # Root details
            elif "F1DP-Root" in endpoint and not endpoint.endswith("/elements"):
                mock_response.json.return_value = {
                    "WebId": "F1DP-Root",
                    "Name": "Root",
                    "TemplateName": "RootTemplate",
                    "Type": "Element",
                    "CategoryNames": []
                }

            # Root children (5 children)
            elif "F1DP-Root/elements" in endpoint:
                mock_response.json.return_value = {
                    "Items": [
                        {"WebId": f"F1DP-Child{i}", "Name": f"Child{i}"}
                        for i in range(5)
                    ]
                }

            # Child details
            elif "F1DP-Child" in endpoint and not endpoint.endswith("/elements"):
                child_id = endpoint.split("F1DP-Child")[1].split("/")[0]
                mock_response.json.return_value = {
                    "WebId": f"F1DP-Child{child_id}",
                    "Name": f"Child{child_id}",
                    "TemplateName": "ChildTemplate",
                    "Type": "Element",
                    "CategoryNames": []
                }

            # Children of children (empty)
            else:
                mock_response.json.return_value = {"Items": []}

            return mock_response

        mock_client.get.side_effect = get_side_effect

        df = extractor.extract_hierarchy(database_webid="F1DP-DB1")

        # Should have 1 root + 5 children = 6 elements
        assert len(df) == 6

        # All children should be at depth 1
        children = df[df['depth'] == 1]
        assert len(children) == 5

        print("✓ Multiple children per level works")

    def test_element_with_categories(self, extractor, mock_client):
        """Test that category names are correctly extracted"""
        def get_side_effect(endpoint, **kwargs):
            mock_response = Mock()

            if "assetdatabases" in endpoint and endpoint.endswith("/elements"):
                mock_response.json.return_value = {
                    "Items": [{"WebId": "F1DP-Pump", "Name": "Pump"}]
                }

            elif "F1DP-Pump" in endpoint and not endpoint.endswith("/elements"):
                mock_response.json.return_value = {
                    "WebId": "F1DP-Pump",
                    "Name": "Pump",
                    "TemplateName": "PumpTemplate",
                    "Type": "Element",
                    "Description": "Critical pump",
                    "CategoryNames": ["Rotating Equipment", "Critical", "Production"]
                }

            else:
                mock_response.json.return_value = {"Items": []}

            return mock_response

        mock_client.get.side_effect = get_side_effect

        df = extractor.extract_hierarchy(database_webid="F1DP-DB1")

        # Check categories
        pump_row = df[df['element_name'] == 'Pump'].iloc[0]
        assert pump_row['categories'] == ["Rotating Equipment", "Critical", "Production"]
        assert pump_row['description'] == "Critical pump"

        print("✓ Categories and descriptions extracted")

    def test_error_handling_on_failed_element(self, extractor, mock_client):
        """
        Test graceful handling when one element fails to load
        Validates: Partial failures don't crash entire extraction
        """
        call_count = 0

        def get_side_effect(endpoint, **kwargs):
            nonlocal call_count
            call_count += 1

            mock_response = Mock()

            # Root elements
            if "assetdatabases" in endpoint and endpoint.endswith("/elements"):
                mock_response.json.return_value = {
                    "Items": [
                        {"WebId": "F1DP-Good", "Name": "Good"},
                        {"WebId": "F1DP-Bad", "Name": "Bad"}
                    ]
                }

            # Good element
            elif "F1DP-Good" in endpoint and not endpoint.endswith("/elements"):
                mock_response.json.return_value = {
                    "WebId": "F1DP-Good",
                    "Name": "Good",
                    "TemplateName": "GoodTemplate",
                    "Type": "Element",
                    "CategoryNames": []
                }

            # Bad element (simulates error)
            elif "F1DP-Bad" in endpoint and not endpoint.endswith("/elements"):
                raise Exception("Failed to fetch element")

            # Children
            else:
                mock_response.json.return_value = {"Items": []}

            return mock_response

        mock_client.get.side_effect = get_side_effect

        # Should continue despite error
        df = extractor.extract_hierarchy(database_webid="F1DP-DB1")

        # Should have at least the good element
        assert len(df) >= 1
        assert "Good" in df['element_name'].values

        print("✓ Error handling graceful (continues on failure)")


class TestAFHierarchyExtractorIntegration:
    """Integration-style tests with realistic scenarios"""

    @pytest.fixture
    def mock_client(self):
        client = Mock()
        client.get = Mock()
        return client

    @pytest.fixture
    def extractor(self, mock_client):
        return AFHierarchyExtractor(mock_client)

    def test_alinta_hierarchy_scenario(self, extractor, mock_client):
        """
        Test realistic Alinta Energy hierarchy structure
        Scenario: Plant -> Units -> Equipment
        """
        def get_side_effect(endpoint, **kwargs):
            mock_response = Mock()

            # Root: Plant
            if "assetdatabases/F1DP-AlintaDB/elements" in endpoint:
                mock_response.json.return_value = {
                    "Items": [{"WebId": "F1DP-Plant", "Name": "Alinta_PowerPlant"}]
                }

            # Plant details
            elif "elements/F1DP-Plant" in endpoint and not endpoint.endswith("/elements"):
                mock_response.json.return_value = {
                    "WebId": "F1DP-Plant",
                    "Name": "Alinta_PowerPlant",
                    "TemplateName": "PowerPlantTemplate",
                    "Type": "Element",
                    "Description": "Main power generation facility",
                    "CategoryNames": ["Power Generation"]
                }

            # Plant children: Units
            elif "elements/F1DP-Plant/elements" in endpoint:
                mock_response.json.return_value = {
                    "Items": [
                        {"WebId": "F1DP-Unit1", "Name": "Turbine_Unit_1"},
                        {"WebId": "F1DP-Unit2", "Name": "Turbine_Unit_2"}
                    ]
                }

            # Unit details
            elif "F1DP-Unit" in endpoint and not endpoint.endswith("/elements"):
                unit_num = endpoint.split("F1DP-Unit")[1][0]
                mock_response.json.return_value = {
                    "WebId": f"F1DP-Unit{unit_num}",
                    "Name": f"Turbine_Unit_{unit_num}",
                    "TemplateName": "TurbineUnitTemplate",
                    "Type": "Element",
                    "CategoryNames": ["Generation Equipment"]
                }

            # No children for units
            else:
                mock_response.json.return_value = {"Items": []}

            return mock_response

        mock_client.get.side_effect = get_side_effect

        df = extractor.extract_hierarchy(database_webid="F1DP-AlintaDB")

        # Verify structure
        assert len(df) == 3  # Plant + 2 Units
        assert "Alinta_PowerPlant" in df['element_name'].values
        assert "Turbine_Unit_1" in df['element_name'].values
        assert "Turbine_Unit_2" in df['element_name'].values

        # Verify paths
        plant = df[df['element_name'] == 'Alinta_PowerPlant'].iloc[0]
        assert plant['element_path'] == '/Alinta_PowerPlant'

        unit1 = df[df['element_name'] == 'Turbine_Unit_1'].iloc[0]
        assert unit1['element_path'] == '/Alinta_PowerPlant/Turbine_Unit_1'

        print("✓ Alinta hierarchy scenario works")


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "-s"])
