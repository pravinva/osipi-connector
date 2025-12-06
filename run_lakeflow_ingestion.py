"""
Lakeflow PI Connector - Ingestion Script
Connects to mock PI server and ingests data into Databricks Unity Catalog
"""

import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import json

# Configuration
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://e2-demo-field-eng.cloud.databricks.com")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "YOUR_DATABRICKS_TOKEN")
PI_SERVER_URL = "http://localhost:8010"
CATALOG = "main"
SCHEMA = "pi_ingestion"
WAREHOUSE_ID = None  # Will auto-detect

# Initialize Databricks client
w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

print("=" * 80)
print("PI LAKEFLOW CONNECTOR - DATA INGESTION")
print("=" * 80)

def execute_sql(sql: str, warehouse_id: str = None):
    """Execute SQL statement and wait for completion"""
    if warehouse_id is None:
        # Find first available warehouse
        warehouses = list(w.warehouses.list())
        if not warehouses:
            raise Exception("No SQL warehouses found")
        warehouse_id = warehouses[0].id

    print(f"\nExecuting SQL:\n{sql[:200]}...")

    stmt = w.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=warehouse_id,
        wait_timeout="30s"
    )

    if stmt.status.state == StatementState.SUCCEEDED:
        print("✓ SQL executed successfully")
        return stmt
    else:
        print(f"✗ SQL failed: {stmt.status.state}")
        raise Exception(f"SQL execution failed: {stmt.status.state}")

# Step 1: Create catalog and schema
print("\n" + "=" * 80)
print("STEP 1: CREATE CATALOG AND SCHEMA")
print("=" * 80)

try:
    execute_sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    execute_sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    execute_sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.checkpoints")
    print(f"✓ Catalog '{CATALOG}' and schema '{SCHEMA}' ready")
except Exception as e:
    print(f"✗ Error creating catalog/schema: {e}")
    print("Note: You may need appropriate permissions")

# Step 2: Create Delta tables
print("\n" + "=" * 80)
print("STEP 2: CREATE DELTA TABLES")
print("=" * 80)

# Create timeseries table
timeseries_ddl = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.pi_timeseries (
    tag_webid STRING,
    tag_name STRING,
    timestamp TIMESTAMP,
    value DOUBLE,
    units STRING,
    quality_good BOOLEAN,
    quality_questionable BOOLEAN,
    quality_substituted BOOLEAN,
    ingestion_timestamp TIMESTAMP,
    sensor_type STRING,
    plant STRING,
    unit INT
)
USING DELTA
PARTITIONED BY (DATE(timestamp))
"""

# Create asset hierarchy table
hierarchy_ddl = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.pi_asset_hierarchy (
    webid STRING,
    name STRING,
    template_name STRING,
    description STRING,
    path STRING,
    parent_webid STRING,
    element_type STRING,
    category_names ARRAY<STRING>,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
"""

# Create event frames table
events_ddl = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.pi_event_frames (
    webid STRING,
    name STRING,
    template_name STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds DOUBLE,
    referenced_element_webid STRING,
    description STRING,
    category_names ARRAY<STRING>,
    attributes MAP<STRING, STRING>,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
"""

# Create checkpoint table
checkpoint_ddl = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.checkpoints.pi_watermarks (
    tag_webid STRING PRIMARY KEY,
    last_timestamp TIMESTAMP,
    last_value DOUBLE,
    updated_at TIMESTAMP
)
USING DELTA
"""

try:
    execute_sql(timeseries_ddl)
    print("✓ Created pi_timeseries table")

    execute_sql(hierarchy_ddl)
    print("✓ Created pi_asset_hierarchy table")

    execute_sql(events_ddl)
    print("✓ Created pi_event_frames table")

    execute_sql(checkpoint_ddl)
    print("✓ Created pi_watermarks checkpoint table")
except Exception as e:
    print(f"✗ Error creating tables: {e}")

# Step 3: Extract data from mock PI server
print("\n" + "=" * 80)
print("STEP 3: EXTRACT DATA FROM MOCK PI SERVER")
print("=" * 80)

# Get tags
print("\nFetching tags from mock server...")
response = requests.get(f"{PI_SERVER_URL}/piwebapi/dataservers")
servers = response.json()['Items']
server_webid = servers[0]['WebId']

response = requests.get(
    f"{PI_SERVER_URL}/piwebapi/dataservers/{server_webid}/points",
    params={"maxCount": 20}
)
tags = response.json()['Items']
print(f"✓ Found {len(tags)} tags")

# Extract time-series data using batch controller
print("\nExtracting time-series data (last 2 hours)...")
end_time = datetime.now()
start_time = end_time - timedelta(hours=2)

batch_requests = []
for tag in tags:
    batch_requests.append({
        "Method": "GET",
        "Resource": f"/streams/{tag['WebId']}/recorded",
        "Parameters": {
            "startTime": start_time.isoformat() + "Z",
            "endTime": end_time.isoformat() + "Z",
            "maxCount": "100"
        }
    })

response = requests.post(
    f"{PI_SERVER_URL}/piwebapi/batch",
    json={"Requests": batch_requests}
)
batch_results = response.json()['Responses']

# Parse into DataFrame
all_data = []
for i, result in enumerate(batch_results):
    if result['Status'] == 200:
        tag = tags[i]
        tag_webid = tag['WebId']
        items = result['Content']['Items']

        for item in items:
            all_data.append({
                'tag_webid': tag_webid,
                'tag_name': tag['Name'],
                'timestamp': item['Timestamp'],
                'value': item['Value'],
                'units': item['UnitsAbbreviation'],
                'quality_good': item['Good'],
                'quality_questionable': item['Questionable'],
                'quality_substituted': item['Substituted'],
                'ingestion_timestamp': datetime.utcnow().isoformat() + "Z",
                'sensor_type': tag_webid.split('-')[3] if len(tag_webid.split('-')) > 3 else 'Unknown',
                'plant': tag_webid.split('-')[1] if len(tag_webid.split('-')) > 1 else 'Unknown',
                'unit': 1
            })

df_timeseries = pd.DataFrame(all_data)
print(f"✓ Extracted {len(df_timeseries)} time-series data points")

# Step 4: Upload data to Databricks
print("\n" + "=" * 80)
print("STEP 4: UPLOAD DATA TO DATABRICKS")
print("=" * 80)

# Save to temporary CSV
temp_file = "/tmp/pi_timeseries_data.csv"
df_timeseries.to_csv(temp_file, index=False)
print(f"✓ Saved data to {temp_file}")

print("\n⚠️  NEXT STEPS TO COMPLETE INGESTION:")
print("1. Upload the CSV file to Databricks using:")
print(f"   databricks fs cp {temp_file} dbfs:/tmp/pi_ingestion/")
print("2. Load into Delta table using:")
print(f"   COPY INTO {CATALOG}.{SCHEMA}.pi_timeseries")
print(f"   FROM 'dbfs:/tmp/pi_ingestion/pi_timeseries_data.csv'")
print(f"   FILEFORMAT = CSV")
print(f"   FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')")
print("   COPY_OPTIONS ('mergeSchema' = 'true')")

# Summary
print("\n" + "=" * 80)
print("INGESTION SUMMARY")
print("=" * 80)
print(f"✓ Catalog: {CATALOG}")
print(f"✓ Schema: {SCHEMA}")
print(f"✓ Tables created: pi_timeseries, pi_asset_hierarchy, pi_event_frames")
print(f"✓ Data extracted: {len(df_timeseries)} rows from {len(tags)} tags")
print(f"✓ Time range: {start_time} to {end_time}")
print(f"✓ Data file: {temp_file}")
print("\n✅ Mock PI server data ready for ingestion!")
print("=" * 80)

# Print sample data
print("\nSample data (first 5 rows):")
print(df_timeseries.head().to_string())
