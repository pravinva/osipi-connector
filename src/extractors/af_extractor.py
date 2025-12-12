from typing import Dict, List, Optional
import pandas as pd
import logging

class AFHierarchyExtractor:
    """
    Extracts PI Asset Framework hierarchy
    Addresses: Alinta April 2024 request for AF connectivity
    """

    def __init__(self, client):
        self.client = client
        self.logger = logging.getLogger(__name__)

    def get_asset_databases(self) -> List[Dict]:
        """List available AF databases"""
        # Use POST endpoint (required for Databricks App authentication)
        response = self.client.post("/piwebapi/assetdatabases/list", json={})
        return response.json().get("Items", [])

    def extract_hierarchy(
        self,
        database_webid: str,
        root_element_webid: Optional[str] = None,
        max_depth: int = 10
    ) -> pd.DataFrame:
        """
        Recursively extract AF element hierarchy

        Args:
            database_webid: AF database WebId
            root_element_webid: Starting element (None = database root)
            max_depth: Maximum recursion depth (safety limit)

        Returns:
            DataFrame with: element_id, name, path, parent_id, template_name, type
        """

        hierarchy_data = []

        def walk_elements(element_webid: str, parent_path: str = "", depth: int = 0):
            """Recursive hierarchy traversal"""
            if depth > max_depth:
                self.logger.warning(f"Max depth {max_depth} reached")
                return

            # Get element details (use POST endpoint for Databricks App compatibility)
            try:
                element_response = self.client.post(
                    "/piwebapi/elements/get",
                    json={"element_webid": element_webid}
                )
                element = element_response.json()
            except Exception as e:
                self.logger.error(f"Failed to get element {element_webid}: {e}")
                return

            # Build element path (like /Enterprise/Site1/Unit2/Pump-101)
            element_path = f"{parent_path}/{element['Name']}"

            # Extract element data
            hierarchy_data.append({
                "element_id": element["WebId"],
                "element_name": element["Name"],
                "element_path": element_path,
                "parent_id": parent_path if parent_path else None,
                "template_name": element.get("TemplateName"),
                "element_type": element.get("Type", "Element"),
                "description": element.get("Description", ""),
                "categories": element.get("CategoryNames", []),
                "depth": depth
            })

            # Get child elements (use POST endpoint for Databricks App compatibility)
            try:
                children_response = self.client.post(
                    "/piwebapi/elements/children",
                    json={"element_webid": element_webid}
                )
                children = children_response.json().get("Items", [])

                # Recurse to children
                for child in children:
                    walk_elements(child["WebId"], element_path, depth + 1)

            except Exception as e:
                self.logger.warning(f"No children for {element['Name']}: {e}")

        # Start traversal
        if root_element_webid:
            walk_elements(root_element_webid)
        else:
            # Start from database root elements
            # Use POST endpoint (required for Databricks App authentication)
            db_elements_response = self.client.post(
                "/piwebapi/assetdatabases/elements",
                json={"db_webid": database_webid, "maxCount": 10000}
            )
            root_elements = db_elements_response.json().get("Items", [])

            for root in root_elements:
                walk_elements(root["WebId"])

        return pd.DataFrame(hierarchy_data)

    def extract_element_attributes(self, element_webid: str) -> pd.DataFrame:
        """
        Extract attributes for an element
        Provides metadata about what data is available
        """
        response = self.client.get(f"/piwebapi/elements/{element_webid}/attributes")
        attributes = response.json().get("Items", [])

        attr_data = []
        for attr in attributes:
            attr_data.append({
                "attribute_id": attr["WebId"],
                "element_id": element_webid,
                "attribute_name": attr["Name"],
                "data_reference": attr.get("DataReferencePlugIn"),
                "default_uom": attr.get("DefaultUnitsName", ""),
                "type": attr.get("Type"),
                "is_configuration": attr.get("IsConfigurationItem", False)
            })

        return pd.DataFrame(attr_data)
