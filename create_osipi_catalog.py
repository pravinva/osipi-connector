"""
Create OSIPI catalog and load PI data
"""

import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
import time

# Configuration
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://e2-demo-field-eng.cloud.databricks.com")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "YOUR_DATABRICKS_TOKEN")
CATALOG = "osipi"
SCHEMA = "bronze"

# Initialize Databricks client
w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

# Get first SQL warehouse
warehouses = list(w.warehouses.list())
if not warehouses:
    raise Exception("No SQL warehouses found")
warehouse_id = warehouses[0].id

print(f"Using SQL Warehouse: {warehouses[0].name} ({warehouse_id})")

def execute_sql(sql: str):
    """Execute SQL and wait for result"""
    print(f"\n{sql}\n")

    stmt = w.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=warehouse_id,
        wait_timeout="50s"
    )

    # Poll for result
    while stmt.status.state in [StatementState.PENDING, StatementState.RUNNING]:
        time.sleep(1)
        stmt = w.statement_execution.get_statement(stmt.statement_id)

    if stmt.status.state == StatementState.SUCCEEDED:
        print("✓ Success")
        return stmt
    else:
        error_msg = stmt.status.error.message if stmt.status.error else "Unknown error"
        print(f"✗ Failed: {error_msg}")
        if "already exists" not in error_msg.lower():
            raise Exception(f"SQL failed: {error_msg}")
        return None

print("=" * 80)
print("CREATING OSIPI CATALOG AND LOADING DATA")
print("=" * 80)

# Step 1: Create catalog
print("\n--- STEP 1: CREATE CATALOG ---")
execute_sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")

# Step 2: Create schemas
print("\n--- STEP 2: CREATE SCHEMAS ---")
execute_sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
execute_sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
execute_sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")
execute_sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.checkpoints")

# Step 2b: Drop old tables in main catalog
print("\n--- STEP 2B: DROP OLD TABLES IN MAIN ---")
try:
    execute_sql("DROP TABLE IF EXISTS main.pi_ingestion.pi_timeseries")
    execute_sql("DROP SCHEMA IF EXISTS main.pi_ingestion CASCADE")
except:
    print("(No old tables to drop)")

# Step 3: Create table
print("\n--- STEP 3: CREATE DELTA TABLE ---")
create_table_sql = f"""
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
  unit INT,
  date DATE GENERATED ALWAYS AS (CAST(timestamp AS DATE))
)
USING DELTA
PARTITIONED BY (date)
"""
execute_sql(create_table_sql)

# Step 4: Load data
print("\n--- STEP 4: LOAD DATA FROM CSV ---")
copy_into_sql = f"""
COPY INTO {CATALOG}.{SCHEMA}.pi_timeseries
FROM 'dbfs:/tmp/pi_ingestion/pi_timeseries_data.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'timestampFormat' = 'yyyy-MM-dd\\'T\\'HH:mm:ss.SSSSSS\\'Z\\'')
COPY_OPTIONS ('mergeSchema' = 'true')
"""
execute_sql(copy_into_sql)

# Step 5: Verify data
print("\n--- STEP 5: VERIFY DATA ---")
count_result = execute_sql(f"SELECT COUNT(*) as row_count FROM {CATALOG}.{SCHEMA}.pi_timeseries")

# Step 6: Show summary
print("\n--- STEP 6: DATA SUMMARY ---")
summary_sql = f"""
SELECT
  plant,
  COUNT(*) as row_count,
  COUNT(DISTINCT tag_webid) as unique_tags,
  MIN(timestamp) as min_time,
  MAX(timestamp) as max_time
FROM {CATALOG}.{SCHEMA}.pi_timeseries
GROUP BY plant
ORDER BY plant
"""
execute_sql(summary_sql)

# Step 7: Create aggregated gold table
print("\n--- STEP 7: CREATE GOLD LAYER (AGGREGATED METRICS) ---")
gold_table_sql = f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.pi_metrics_hourly AS
SELECT
  plant,
  sensor_type,
  DATE_TRUNC('hour', timestamp) as hour,
  COUNT(*) as data_points,
  COUNT(DISTINCT tag_webid) as unique_tags,
  AVG(value) as avg_value,
  MIN(value) as min_value,
  MAX(value) as max_value,
  SUM(CASE WHEN quality_good THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as quality_pct
FROM {CATALOG}.{SCHEMA}.pi_timeseries
GROUP BY plant, sensor_type, DATE_TRUNC('hour', timestamp)
ORDER BY hour DESC, plant, sensor_type
"""
execute_sql(gold_table_sql)

print("\n" + "=" * 80)
print("✅ OSIPI CATALOG READY!")
print("=" * 80)
print(f"Catalog: {CATALOG}")
print(f"Bronze: {CATALOG}.{SCHEMA}.pi_timeseries")
print(f"Gold: {CATALOG}.gold.pi_metrics_hourly")
print("=" * 80)
