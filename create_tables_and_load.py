"""
Create Delta tables and load real PI data from DBFS
"""

import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
import time

# Configuration
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://e2-demo-field-eng.cloud.databricks.com")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "YOUR_DATABRICKS_TOKEN")
CATALOG = "main"
SCHEMA = "pi_ingestion"

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
    print(f"\nExecuting:\n{sql}\n")

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
        if stmt.result and stmt.result.data_array:
            print(f"  Rows affected: {len(stmt.result.data_array)}")
        return stmt
    else:
        error_msg = stmt.status.error.message if stmt.status.error else "Unknown error"
        print(f"✗ Failed: {error_msg}")
        raise Exception(f"SQL failed: {error_msg}")

# Step 1: Create table
print("=" * 80)
print("STEP 1: CREATE DELTA TABLE")
print("=" * 80)

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
  unit INT
)
USING DELTA
"""

execute_sql(create_table_sql)

# Step 1b: Load data using COPY INTO
print("\n" + "=" * 80)
print("STEP 1B: LOAD DATA FROM CSV")
print("=" * 80)

copy_into_sql = f"""
COPY INTO {CATALOG}.{SCHEMA}.pi_timeseries
FROM 'dbfs:/tmp/pi_ingestion/pi_timeseries_data.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'timestampFormat' = 'yyyy-MM-dd\\'T\\'HH:mm:ss.SSSSSS\\'Z\\'')
COPY_OPTIONS ('mergeSchema' = 'true')
"""

execute_sql(copy_into_sql)

# Step 2: Query the data
print("\n" + "=" * 80)
print("STEP 2: VERIFY DATA LOADED")
print("=" * 80)

count_sql = f"SELECT COUNT(*) as row_count FROM {CATALOG}.{SCHEMA}.pi_timeseries"
result = execute_sql(count_sql)

# Step 3: Show sample data
print("\n" + "=" * 80)
print("STEP 3: SAMPLE DATA")
print("=" * 80)

sample_sql = f"""
SELECT
  tag_name,
  timestamp,
  value,
  units,
  plant
FROM {CATALOG}.{SCHEMA}.pi_timeseries
LIMIT 10
"""

result = execute_sql(sample_sql)

# Step 4: Show aggregates
print("\n" + "=" * 80)
print("STEP 4: DATA SUMMARY")
print("=" * 80)

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

result = execute_sql(summary_sql)

print("\n" + "=" * 80)
print("✅ DATA INGESTION COMPLETE!")
print("=" * 80)
print(f"Table: {CATALOG}.{SCHEMA}.pi_timeseries")
print(f"Data ready for dashboard queries!")
print("=" * 80)
