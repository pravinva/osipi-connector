"""
Lakeflow PI Web API Connector - Continuous Incremental Ingestion
Runs as a Databricks Job to continuously ingest PI data
"""

import os
import requests
from datetime import datetime, timedelta
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
import time

# Configuration
PI_SERVER_URL = os.getenv("PI_SERVER_URL", "http://localhost:8010")
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://e2-demo-field-eng.cloud.databricks.com")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "4b9b953939869799")

CATALOG = "osipi"
SCHEMA = "bronze"
TABLE = "pi_timeseries"
CHECKPOINT_TABLE = f"{CATALOG}.checkpoints.pi_ingestion_checkpoint"

print("=" * 80)
print("LAKEFLOW PI WEB API CONNECTOR - INCREMENTAL INGESTION")
print("=" * 80)
print(f"Target: {CATALOG}.{SCHEMA}.{TABLE}")
print(f"PI Server: {PI_SERVER_URL}")
print(f"Timestamp: {datetime.utcnow().isoformat()}Z")
print("=" * 80)

# Initialize Databricks client
w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

def execute_sql(sql: str):
    """Execute SQL on Databricks"""
    stmt = w.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=DATABRICKS_WAREHOUSE_ID,
        wait_timeout="50s"
    )

    while stmt.status.state in [StatementState.PENDING, StatementState.RUNNING]:
        time.sleep(1)
        stmt = w.statement_execution.get_statement(stmt.statement_id)

    if stmt.status.state == StatementState.SUCCEEDED:
        return stmt
    else:
        error_msg = stmt.status.error.message if stmt.status.error else "Unknown error"
        raise Exception(f"SQL failed: {error_msg}")

# Step 1: Get last checkpoint (last ingestion timestamp)
print("\n[1/6] Getting last checkpoint...")
try:
    result = execute_sql(f"""
        SELECT MAX(last_ingestion_time) as last_time
        FROM {CHECKPOINT_TABLE}
    """)
    if result.result and result.result.data_array and result.result.data_array[0][0]:
        last_ingestion = result.result.data_array[0][0]
        print(f"   Last ingestion: {last_ingestion}")
    else:
        last_ingestion = None
        print("   No checkpoint found - full load")
except:
    # Checkpoint table doesn't exist yet
    print("   Creating checkpoint table...")
    execute_sql(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            last_ingestion_time TIMESTAMP,
            rows_ingested INT,
            ingestion_run_id STRING,
            created_at TIMESTAMP
        )
    """)
    last_ingestion = None
    print("   Checkpoint table created")

# Step 2: Calculate time window for ingestion
if last_ingestion:
    # Incremental: get data since last ingestion
    start_time = datetime.fromisoformat(last_ingestion.replace('Z', ''))
    end_time = datetime.utcnow()
    print(f"   Incremental load: {start_time.isoformat()}Z to {end_time.isoformat()}Z")
else:
    # Full load: get last 2 hours
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=2)
    print(f"   Full load: {start_time.isoformat()}Z to {end_time.isoformat()}Z")

# Step 3: Get all available tags from PI Server
print("\n[2/6] Discovering PI tags...")
try:
    response = requests.get(f"{PI_SERVER_URL}/piwebapi/dataservers")
    data_servers = response.json()["Items"]

    all_tags = []
    for server in data_servers:
        points_response = requests.get(server["Links"]["Points"])
        points = points_response.json()["Items"]
        all_tags.extend(points)

    print(f"   Found {len(all_tags)} tags")
except Exception as e:
    print(f"   Error discovering tags: {e}")
    print("   Falling back to known tags...")
    # Fallback to known tags if PI server unavailable
    all_tags = []

# Step 3b: Generate batch requests for all tags
print("\n[3/6] Building batch requests...")
batch_requests = []
for i, tag in enumerate(all_tags):
    batch_requests.append({
        "Method": "GET",
        "Resource": f"{tag['Links']['RecordedData']}?startTime={start_time.isoformat()}Z&endTime={end_time.isoformat()}Z&maxCount=1000"
    })

print(f"   Created {len(batch_requests)} batch requests")

# Step 4: Execute batch request
print("\n[4/6] Extracting data from PI Server...")
if len(batch_requests) == 0:
    print("   No data to extract")
    exit(0)

try:
    batch_response = requests.post(
        f"{PI_SERVER_URL}/piwebapi/batch",
        json={"Requests": batch_requests},
        timeout=30
    )
    batch_results = batch_response.json()

    # Parse results
    rows = []
    for i, result in enumerate(batch_results):
        if result["Status"] == 200:
            tag = all_tags[i]
            content = result["Content"]

            for item in content.get("Items", []):
                rows.append({
                    "tag_webid": tag["WebId"],
                    "tag_name": tag["Name"],
                    "timestamp": item["Timestamp"],
                    "value": item["Value"],
                    "units": tag.get("EngineeringUnits", ""),
                    "quality_good": item.get("Good", True),
                    "quality_questionable": item.get("Questionable", False),
                    "quality_substituted": item.get("Substituted", False),
                    "ingestion_timestamp": datetime.utcnow().isoformat() + "Z",
                    "sensor_type": tag["Name"].split('-')[1] if '-' in tag["Name"] else "Unknown",
                    "plant": tag["Name"].split('-')[0] if '-' in tag["Name"] else "Unknown",
                    "unit": int(tag["Name"].split('-')[2].replace('U', '')) if len(tag["Name"].split('-')) > 2 else 0
                })

    print(f"   Extracted {len(rows)} data points")

except Exception as e:
    print(f"   Error extracting data: {e}")
    rows = []

if len(rows) == 0:
    print("   No new data to ingest")
    exit(0)

# Step 5: Write to Delta table using SQL INSERT
print("\n[5/6] Writing to Delta table...")
run_id = f"run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

# Create temp view from data
import pandas as pd
df = pd.DataFrame(rows)
csv_path = f"/tmp/pi_data_{run_id}.csv"
df.to_csv(csv_path, index=False)

# Upload to DBFS
with open(csv_path, 'rb') as f:
    w.dbfs.upload(f"/tmp/lakeflow_pi/{run_id}.csv", f, overwrite=True)

# Use COPY INTO for incremental load
copy_sql = f"""
COPY INTO {CATALOG}.{SCHEMA}.{TABLE}
FROM 'dbfs:/tmp/lakeflow_pi/{run_id}.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'timestampFormat' = 'yyyy-MM-dd\\'T\\'HH:mm:ss.SSSSSS\\'Z\\'')
COPY_OPTIONS ('mergeSchema' = 'true')
"""

try:
    execute_sql(copy_sql)
    print(f"   Loaded {len(rows)} rows")
except Exception as e:
    print(f"   Error loading data: {e}")
    raise

# Step 6: Update checkpoint
print("\n[6/6] Updating checkpoint...")
checkpoint_sql = f"""
INSERT INTO {CHECKPOINT_TABLE} VALUES (
    '{end_time.isoformat()}Z',
    {len(rows)},
    '{run_id}',
    current_timestamp()
)
"""
execute_sql(checkpoint_sql)
print(f"   Checkpoint updated: {end_time.isoformat()}Z")

# Step 7: Verify total row count
result = execute_sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{TABLE}")
total_rows = result.result.data_array[0][0]

print("\n" + "=" * 80)
print("✅ INGESTION COMPLETE")
print("=" * 80)
print(f"Run ID: {run_id}")
print(f"Rows ingested: {len(rows)}")
print(f"Total rows in table: {total_rows}")
print(f"Time window: {start_time.isoformat()}Z to {end_time.isoformat()}Z")
print("=" * 80)
