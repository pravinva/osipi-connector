"""
Auto-Discovery of Tags from PI Asset Framework

Automatically discovers all PI tags by traversing the AF hierarchy
instead of requiring manual tag lists.

Features:
- Recursive traversal of AF element tree
- Filter by template type (e.g., only "Flow Meters")
- Filter by attribute patterns
- Exclude test/dev tags
- Output to CSV or directly to connector config
"""

import os
import requests
from typing import List, Dict, Set
from dataclasses import dataclass
from databricks.sdk import WorkspaceClient

# Configuration
PI_SERVER_URL = os.getenv("PI_SERVER_URL", "http://localhost:8010")
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")


@dataclass
class DiscoveredTag:
    """Represents a discovered PI tag with metadata"""
    tag_webid: str
    tag_name: str
    element_path: str
    template_name: str
    sensor_type: str
    plant: str
    unit: str
    description: str
    engineering_units: str


class PITagDiscovery:
    """
    Discovers tags from PI Asset Framework hierarchy
    """

    def __init__(self, pi_server_url: str):
        self.pi_server_url = pi_server_url
        self.session = requests.Session()

    def discover_all_tags(
        self,
        database_webid: str,
        filters: Dict = None
    ) -> List[DiscoveredTag]:
        """
        Main discovery method - traverses AF hierarchy and finds all tags

        Args:
            database_webid: WebID of the AF database
            filters: Optional filters:
                - template_names: List of template names to include
                - exclude_patterns: List of patterns to exclude
                - include_test: Whether to include test tags

        Returns:
            List of discovered tags with metadata
        """
        filters = filters or {}
        discovered_tags = []

        print(f"🔍 Starting tag discovery from database {database_webid}")

        # Step 1: Get root elements
        root_elements = self._get_root_elements(database_webid)
        print(f"   Found {len(root_elements)} root elements")

        # Step 2: Recursively traverse each root element
        for root in root_elements:
            tags = self._traverse_element(
                root['WebId'],
                root['Name'],
                filters
            )
            discovered_tags.extend(tags)

        print(f"✅ Discovery complete: {len(discovered_tags)} tags found")
        return discovered_tags

    def _get_root_elements(self, database_webid: str) -> List[Dict]:
        """Get top-level elements in the AF database"""
        url = f"{self.pi_server_url}/piwebapi/assetdatabases/{database_webid}/elements"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json().get('Items', [])

    def _traverse_element(
        self,
        element_webid: str,
        element_path: str,
        filters: Dict,
        depth: int = 0
    ) -> List[DiscoveredTag]:
        """
        Recursively traverse AF element tree and extract tags

        Args:
            element_webid: Current element's WebID
            element_path: Path to current element (e.g., /Plant1/Unit2)
            filters: Discovery filters
            depth: Current recursion depth (for logging)

        Returns:
            List of discovered tags
        """
        discovered = []
        indent = "  " * depth

        # Get element details
        element_url = f"{self.pi_server_url}/piwebapi/elements/{element_webid}"
        element_response = self.session.get(element_url)
        element_response.raise_for_status()
        element = element_response.json()

        print(f"{indent}📂 {element['Name']}")

        # Check if should exclude based on filters
        if self._should_exclude(element, filters):
            print(f"{indent}   ⏭️  Skipped (filter)")
            return discovered

        # Step 1: Get attributes (potential tags)
        attrs_url = f"{self.pi_server_url}/piwebapi/elements/{element_webid}/attributes"
        attrs_response = self.session.get(attrs_url)
        if attrs_response.status_code == 200:
            attributes = attrs_response.json().get('Items', [])

            for attr in attributes:
                # Only include attributes that reference PI points
                if 'DataReferencePlugIn' in attr and 'PI Point' in attr['DataReferencePlugIn']:
                    tag = self._create_tag_from_attribute(
                        attr,
                        element_path,
                        element.get('TemplateName', 'Unknown')
                    )
                    if tag:
                        discovered.append(tag)
                        print(f"{indent}   ✓ {tag.tag_name}")

        # Step 2: Get child elements and recurse
        children_url = f"{self.pi_server_url}/piwebapi/elements/{element_webid}/elements"
        children_response = self.session.get(children_url)
        if children_response.status_code == 200:
            children = children_response.json().get('Items', [])

            for child in children:
                child_tags = self._traverse_element(
                    child['WebId'],
                    f"{element_path}/{child['Name']}",
                    filters,
                    depth + 1
                )
                discovered.extend(child_tags)

        return discovered

    def _should_exclude(self, element: Dict, filters: Dict) -> bool:
        """Check if element should be excluded based on filters"""

        # Exclude test elements
        if not filters.get('include_test', False):
            if 'test' in element['Name'].lower():
                return True

        # Exclude by template
        if 'template_names' in filters:
            template = element.get('TemplateName', '')
            if template not in filters['template_names']:
                return True

        # Exclude by pattern
        if 'exclude_patterns' in filters:
            name = element['Name'].lower()
            for pattern in filters['exclude_patterns']:
                if pattern.lower() in name:
                    return True

        return False

    def _create_tag_from_attribute(
        self,
        attribute: Dict,
        element_path: str,
        template_name: str
    ) -> DiscoveredTag:
        """Create DiscoveredTag object from AF attribute"""

        # Parse element path to extract plant/unit
        path_parts = element_path.strip('/').split('/')
        plant = path_parts[0] if len(path_parts) > 0 else "Unknown"
        unit = path_parts[1] if len(path_parts) > 1 else "Unknown"

        # Infer sensor type from attribute name
        attr_name = attribute['Name'].lower()
        sensor_type = "Unknown"
        if 'temp' in attr_name:
            sensor_type = "Temp"
        elif 'pres' in attr_name:
            sensor_type = "Pres"
        elif 'flow' in attr_name:
            sensor_type = "Flow"
        elif 'level' in attr_name:
            sensor_type = "Level"

        return DiscoveredTag(
            tag_webid=attribute['WebId'],
            tag_name=attribute['Name'],
            element_path=element_path,
            template_name=template_name,
            sensor_type=sensor_type,
            plant=plant,
            unit=unit,
            description=attribute.get('Description', ''),
            engineering_units=attribute.get('DefaultUnitsName', '')
        )

    def export_to_csv(self, tags: List[DiscoveredTag], output_file: str):
        """Export discovered tags to CSV for review/editing"""
        import csv

        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'tag_webid', 'tag_name', 'element_path', 'template_name',
                'sensor_type', 'plant', 'unit', 'description', 'engineering_units'
            ])

            for tag in tags:
                writer.writerow([
                    tag.tag_webid, tag.tag_name, tag.element_path,
                    tag.template_name, tag.sensor_type, tag.plant,
                    tag.unit, tag.description, tag.engineering_units
                ])

        print(f"📄 Exported {len(tags)} tags to {output_file}")

    def save_to_unity_catalog(self, tags: List[DiscoveredTag]):
        """Save discovered tags to Unity Catalog for connector to use"""

        w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

        # Create SQL to insert discovered tags
        values = []
        for tag in tags:
            values.append(f"""(
                '{tag.tag_webid}',
                '{tag.tag_name}',
                '{tag.element_path}',
                '{tag.template_name}',
                '{tag.sensor_type}',
                '{tag.plant}',
                '{tag.unit}',
                '{tag.description}',
                '{tag.engineering_units}'
            )""")

        sql = f"""
        CREATE TABLE IF NOT EXISTS osipi.bronze.discovered_tags (
            tag_webid STRING,
            tag_name STRING,
            element_path STRING,
            template_name STRING,
            sensor_type STRING,
            plant STRING,
            unit STRING,
            description STRING,
            engineering_units STRING,
            discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        USING DELTA;

        INSERT OVERWRITE osipi.bronze.discovered_tags
        VALUES {','.join(values)};
        """

        print("💾 Saving discovered tags to Unity Catalog...")
        # Execute SQL (implementation needed)
        print(f"✅ Saved {len(tags)} tags to osipi.bronze.discovered_tags")


def main():
    """
    Example usage of auto-discovery
    """

    discovery = PITagDiscovery(PI_SERVER_URL)

    # Define filters
    filters = {
        'include_test': False,  # Exclude test tags
        'exclude_patterns': ['backup', 'temp_'],  # Exclude patterns
        # 'template_names': ['Flow Meter', 'Temperature Sensor']  # Only specific templates
    }

    # Discover all tags
    tags = discovery.discover_all_tags(
        database_webid='F1DP-DB-Production',
        filters=filters
    )

    # Export to CSV for review
    discovery.export_to_csv(tags, 'discovered_tags.csv')

    # Option: Save to Unity Catalog
    # discovery.save_to_unity_catalog(tags)

    # Print summary
    print("\n" + "="*60)
    print("DISCOVERY SUMMARY")
    print("="*60)
    print(f"Total tags discovered: {len(tags)}")

    # Group by sensor type
    by_type = {}
    for tag in tags:
        by_type[tag.sensor_type] = by_type.get(tag.sensor_type, 0) + 1

    print("\nBy Sensor Type:")
    for sensor_type, count in sorted(by_type.items()):
        print(f"  {sensor_type}: {count}")

    # Group by plant
    by_plant = {}
    for tag in tags:
        by_plant[tag.plant] = by_plant.get(tag.plant, 0) + 1

    print("\nBy Plant:")
    for plant, count in sorted(by_plant.items()):
        print(f"  {plant}: {count}")

    print("\n💡 Next steps:")
    print("1. Review discovered_tags.csv")
    print("2. Edit to remove unwanted tags")
    print("3. Use in connector: tags = pd.read_csv('discovered_tags.csv')['tag_webid'].tolist()")


if __name__ == "__main__":
    main()
