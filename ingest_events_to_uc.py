"""Ingest PI Event Frames from mock server to Unity Catalog"""
import requests
from databricks.sdk import WorkspaceClient
from datetime import datetime
import json

# Fetch events from mock server
print("Fetching event frames from mock server...")
response = requests.get("http://localhost:8010/piwebapi/assetdatabases/F1DP-DB-Production/eventframes?startTime=2025-11-01T00:00:00Z&endTime=2025-12-07T23:59:59Z")
data = response.json()
events = data.get('Items', [])

print(f"Found {len(events)} event frames")

if len(events) == 0:
    print("No events to ingest")
    exit(0)

# Connect to Databricks
w = WorkspaceClient()

# Prepare INSERT statements
print("\nIngesting events to osipi.bronze.pi_event_frames...")

for event in events:
    # Convert attributes dict to SQL map format
    attributes = event.get('Attributes', {})
    attr_pairs = [f"'{k}', '{v}'" for k, v in attributes.items()]
    attr_map = f"map({', '.join(attr_pairs)})" if attr_pairs else "map()"

    # Convert category names to SQL array format
    categories = event.get('CategoryNames', [])
    cat_list = ', '.join([f"'{c}'" for c in categories])
    cat_array = f"array({cat_list})" if categories else "array()"

    # Extract date from start_time for partitioning
    start_time = event['StartTime']
    partition_date = start_time.split('T')[0]

    insert_sql = f"""
    INSERT INTO osipi.bronze.pi_event_frames VALUES (
        '{event['WebId']}',
        '{event['Name']}',
        '{event['TemplateName']}',
        TIMESTAMP'{start_time.replace('Z', '')}',
        TIMESTAMP'{event['EndTime'].replace('Z', '')}',
        '{event['PrimaryReferencedElementWebId']}',
        '{event['Description']}',
        {cat_array},
        {attr_map},
        CURRENT_TIMESTAMP(),
        DATE'{partition_date}'
    )
    """

    try:
        stmt = w.statement_execution.execute_statement(
            statement=insert_sql,
            warehouse_id="4b9b953939869799",
            wait_timeout="30s"
        )
        print(f"✅ Inserted {event['Name']} ({event['TemplateName']})")
    except Exception as e:
        print(f"❌ Failed to insert {event['Name']}: {e}")

# Verify count
print("\nVerifying ingestion...")
stmt = w.statement_execution.execute_statement(
    statement="SELECT COUNT(*) FROM osipi.bronze.pi_event_frames",
    warehouse_id="4b9b953939869799",
    wait_timeout="30s"
)

if stmt.result and stmt.result.data_array:
    count = stmt.result.data_array[0][0]
    print(f"✅ Total events in Unity Catalog: {count}")

print("\n✅ Event ingestion complete!")
print("Events can now be queried from osipi.bronze.pi_event_frames")
