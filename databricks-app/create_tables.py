#!/usr/bin/env python3
"""
Create bronze tables in osipi catalog using Databricks SDK
"""

from databricks.sdk import WorkspaceClient
import os

# Use credentials from environment or ~/.databrickscfg
# Set these before running:
#   export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
#   export DATABRICKS_TOKEN="your-token"
#   export DATABRICKS_WAREHOUSE_ID="your-warehouse-id"

WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
if not WAREHOUSE_ID:
    raise ValueError("DATABRICKS_WAREHOUSE_ID environment variable not set")

w = WorkspaceClient()

# Read SQL file
with open("databricks-app/setup_tables.sql", "r") as f:
    sql_content = f.read()

# Split into individual statements
statements = [s.strip() for s in sql_content.split(";") if s.strip() and not s.strip().startswith("--")]

print(f"Executing {len(statements)} SQL statements...")

for i, statement in enumerate(statements, 1):
    if not statement:
        continue

    print(f"\n[{i}/{len(statements)}] Executing...")
    print(f"  {statement[:80]}{'...' if len(statement) > 80 else ''}")

    try:
        response = w.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=WAREHOUSE_ID,
            catalog="osipi",
            schema="bronze",
            wait_timeout="30s"
        )

        if response.status.state == "SUCCEEDED":
            print(f"  ✓ Success")
        else:
            print(f"  ⚠️  Status: {response.status.state}")

    except Exception as e:
        print(f"  ✗ Error: {e}")
        # Continue with next statement

print("\n✓ Table setup complete!")
print("\nVerifying tables...")

# List tables
try:
    tables_response = w.statement_execution.execute_statement(
        statement="SHOW TABLES IN osipi.bronze",
        warehouse_id=WAREHOUSE_ID,
        catalog="osipi",
        schema="bronze",
        wait_timeout="30s"
    )

    if tables_response.result and tables_response.result.data_array:
        print("\nCreated tables:")
        for row in tables_response.result.data_array:
            table_name = row[1] if len(row) > 1 else row[0]
            print(f"  - osipi.bronze.{table_name}")
    else:
        print("No tables found (this might be okay if schema was just created)")

except Exception as e:
    print(f"Could not list tables: {e}")
