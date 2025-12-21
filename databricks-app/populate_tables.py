"""
Populate AF Hierarchy and Event Frames tables from the Mock PI Web API
Run this from within the Databricks App to populate the bronze tables
"""

import os
import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql
from datetime import datetime
import json

# Configuration from environment
PI_WEB_API_URL = os.getenv("PI_WEB_API_URL", "http://localhost:8000/piwebapi")
UC_CATALOG = os.getenv("UC_CATALOG", "osipi")
UC_SCHEMA = os.getenv("UC_SCHEMA", "bronze")
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")

print(f"📊 Populating tables from: {PI_WEB_API_URL}")
print(f"📦 Target: {UC_CATALOG}.{UC_SCHEMA}")
print(f"⚙️  Warehouse: {DATABRICKS_WAREHOUSE_ID}")
print()

# Initialize Databricks client
w = WorkspaceClient()

def execute_sql_direct(query: str):
    """Execute SQL using Databricks SDK"""
    try:
        response = w.statement_execution.execute_statement(
            statement=query,
            warehouse_id=DATABRICKS_WAREHOUSE_ID,
            catalog=UC_CATALOG,
            schema=UC_SCHEMA,
            wait_timeout="2m"
        )
        return response.status.state == sql.StatementState.SUCCEEDED
    except Exception as e:
        print(f"❌ SQL Error: {e}")
        return False

def create_tables():
    """Create tables if they don't exist"""
    print("1️⃣  Creating tables...")

    # AF Hierarchy table
    af_sql = f"""
    CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.pi_af_hierarchy (
        webid STRING NOT NULL,
        name STRING,
        template_name STRING,
        description STRING,
        path STRING,
        parent_webid STRING,
        plant STRING,
        unit INT,
        equipment_type STRING,
        ingestion_timestamp TIMESTAMP NOT NULL
    )
    USING DELTA
    """

    # Event Frames table
    ef_sql = f"""
    CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.pi_event_frames (
        webid STRING NOT NULL,
        name STRING,
        template_name STRING,
        start_time TIMESTAMP NOT NULL,
        end_time TIMESTAMP,
        primary_element_webid STRING,
        description STRING,
        attributes STRING,
        ingestion_timestamp TIMESTAMP NOT NULL
    )
    USING DELTA
    PARTITIONED BY (DATE(start_time))
    """

    if execute_sql_direct(af_sql):
        print("   ✓ pi_af_hierarchy table ready")
    if execute_sql_direct(ef_sql):
        print("   ✓ pi_event_frames table ready")
    print()

def fetch_af_hierarchy():
    """Fetch AF hierarchy from mock API"""
    print("2️⃣  Fetching AF Hierarchy...")

    try:
        # Get asset database
        resp = requests.get(f"{PI_WEB_API_URL}/assetdatabases", timeout=10)
        resp.raise_for_status()
        databases = resp.json().get("Items", [])

        if not databases:
            print("   ⚠️  No asset databases found")
            return []

        db = databases[0]
        db_webid = db.get("WebId")
        print(f"   Found database: {db.get('Name')}")

        # Get all elements
        resp = requests.get(
            f"{PI_WEB_API_URL}/assetdatabases/{db_webid}/elements",
            params={"maxCount": 10000},
            timeout=30
        )
        resp.raise_for_status()
        elements = resp.json().get("Items", [])

        print(f"   ✓ Fetched {len(elements)} AF elements")
        return elements

    except Exception as e:
        print(f"   ❌ Error fetching AF hierarchy: {e}")
        return []

def insert_af_hierarchy(elements):
    """Insert AF hierarchy into Delta table"""
    print("3️⃣  Inserting AF Hierarchy...")

    if not elements:
        print("   ⚠️  No elements to insert")
        return

    # Create VALUES clause
    values = []
    for elem in elements:
        webid = elem.get("WebId", "").replace("'", "''")
        name = elem.get("Name", "").replace("'", "''")
        template = elem.get("TemplateName", "").replace("'", "''")
        description = elem.get("Description", "").replace("'", "''")
        path = elem.get("Path", "").replace("'", "''")
        parent = elem.get("ParentWebId", "").replace("'", "''")

        # Extract plant/unit from name (e.g., "Loy_Yang_A-U001-Reactor")
        parts = name.split("-")
        plant = parts[0] if len(parts) > 0 else ""
        unit_str = parts[1] if len(parts) > 1 else "0"
        unit = int(unit_str.replace("U", "")) if "U" in unit_str else 0
        equipment = parts[2] if len(parts) > 2 else ""

        values.append(f"""(
            '{webid}', '{name}', '{template}', '{description}', '{path}',
            '{parent}', '{plant}', {unit}, '{equipment}', current_timestamp()
        )""")

    # Insert in batches of 1000
    batch_size = 1000
    for i in range(0, len(values), batch_size):
        batch = values[i:i+batch_size]

        insert_sql = f"""
        INSERT INTO {UC_CATALOG}.{UC_SCHEMA}.pi_af_hierarchy
        VALUES {','.join(batch)}
        """

        if execute_sql_direct(insert_sql):
            print(f"   ✓ Inserted batch {i//batch_size + 1} ({len(batch)} rows)")

    print(f"   ✓ Total: {len(elements)} elements inserted")
    print()

def fetch_event_frames():
    """Fetch event frames from mock API"""
    print("4️⃣  Fetching Event Frames...")

    try:
        # Get asset database
        resp = requests.get(f"{PI_WEB_API_URL}/assetdatabases", timeout=10)
        resp.raise_for_status()
        databases = resp.json().get("Items", [])

        if not databases:
            print("   ⚠️  No asset databases found")
            return []

        db = databases[0]
        db_webid = db.get("WebId")

        # Get event frames
        resp = requests.get(
            f"{PI_WEB_API_URL}/assetdatabases/{db_webid}/eventframes",
            params={
                "maxCount": 1000,
                "startTime": "*-30d",  # Last 30 days
                "endTime": "*"
            },
            timeout=30
        )
        resp.raise_for_status()
        events = resp.json().get("Items", [])

        print(f"   ✓ Fetched {len(events)} event frames")
        return events

    except Exception as e:
        print(f"   ❌ Error fetching event frames: {e}")
        return []

def insert_event_frames(events):
    """Insert event frames into Delta table"""
    print("5️⃣  Inserting Event Frames...")

    if not events:
        print("   ⚠️  No events to insert")
        return

    values = []
    for event in events:
        webid = event.get("WebId", "").replace("'", "''")
        name = event.get("Name", "").replace("'", "''")
        template = event.get("TemplateName", "").replace("'", "''")
        start_time = event.get("StartTime", "")
        end_time = event.get("EndTime", "")
        primary_elem = event.get("PrimaryReferencedElementWebId", "").replace("'", "''")
        description = event.get("Description", "").replace("'", "''")

        # Convert attributes to JSON string
        attributes = json.dumps(event.get("Attributes", {})).replace("'", "''")

        values.append(f"""(
            '{webid}', '{name}', '{template}',
            timestamp'{start_time}',
            {f"timestamp'{end_time}'" if end_time else "NULL"},
            '{primary_elem}', '{description}', '{attributes}', current_timestamp()
        )""")

    # Insert in batches
    batch_size = 500
    for i in range(0, len(values), batch_size):
        batch = values[i:i+batch_size]

        insert_sql = f"""
        INSERT INTO {UC_CATALOG}.{UC_SCHEMA}.pi_event_frames
        VALUES {','.join(batch)}
        """

        if execute_sql_direct(insert_sql):
            print(f"   ✓ Inserted batch {i//batch_size + 1} ({len(batch)} rows)")

    print(f"   ✓ Total: {len(events)} events inserted")
    print()

def main():
    """Main execution"""
    print("=" * 60)
    print("POPULATING AF HIERARCHY & EVENT FRAMES")
    print("=" * 60)
    print()

    # Step 1: Create tables
    create_tables()

    # Step 2: Fetch and insert AF hierarchy
    elements = fetch_af_hierarchy()
    if elements:
        insert_af_hierarchy(elements)

    # Step 3: Fetch and insert event frames
    events = fetch_event_frames()
    if events:
        insert_event_frames(events)

    print("=" * 60)
    print("✓ COMPLETED")
    print("=" * 60)
    print()
    print("Now refresh your dashboard at:")
    print(f"  http://localhost:8002/ingestion")
    print(f"  http://localhost:8002/data/af-hierarchy")
    print(f"  http://localhost:8002/data/events")

if __name__ == "__main__":
    main()
