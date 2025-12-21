#!/usr/bin/env python3
"""Check Unity Catalog tables for AF hierarchy data."""

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Check AF hierarchy table
print("Checking osipi.bronze.pi_af_hierarchy...")
try:
    result = w.statement_execution.execute_statement(
        statement="SELECT COUNT(*) as count FROM osipi.bronze.pi_af_hierarchy",
        warehouse_id="4b9b953939869799",
        catalog="osipi",
        schema="bronze",
        wait_timeout="30s"
    )

    if result.result and result.result.data_array:
        count = result.result.data_array[0][0]
        print(f"✓ AF Hierarchy table exists with {count} rows")

        if count == 0:
            print("⚠️  Table is empty - no data has been ingested yet")
        else:
            # Show sample data
            result2 = w.statement_execution.execute_statement(
                statement="SELECT name, equipment_type, path FROM osipi.bronze.pi_af_hierarchy LIMIT 5",
                warehouse_id="4b9b953939869799",
                catalog="osipi",
                schema="bronze",
                wait_timeout="30s"
            )
            print("\nSample rows:")
            if result2.result and result2.result.data_array:
                for row in result2.result.data_array:
                    print(f"  {row}")
    else:
        print("✗ No data returned")

except Exception as e:
    print(f"✗ Error: {e}")
    print("\nPossible causes:")
    print("1. Table doesn't exist yet - run databricks-app/create_tables.py first")
    print("2. No ingestion has run yet - tables are empty")
    print("3. Warehouse ID is incorrect")

# Check event frames table
print("\nChecking osipi.bronze.pi_event_frames...")
try:
    result = w.statement_execution.execute_statement(
        statement="SELECT COUNT(*) as count FROM osipi.bronze.pi_event_frames",
        warehouse_id="4b9b953939869799",
        catalog="osipi",
        schema="bronze",
        wait_timeout="30s"
    )

    if result.result and result.result.data_array:
        count = result.result.data_array[0][0]
        print(f"✓ Event frames table exists with {count} rows")
    else:
        print("✗ No data returned")

except Exception as e:
    print(f"✗ Error: {e}")

# Check timeseries table
print("\nChecking osipi.bronze.pi_timeseries...")
try:
    result = w.statement_execution.execute_statement(
        statement="SELECT COUNT(*) as count FROM osipi.bronze.pi_timeseries",
        warehouse_id="4b9b953939869799",
        catalog="osipi",
        schema="bronze",
        wait_timeout="30s"
    )

    if result.result and result.result.data_array:
        count = result.result.data_array[0][0]
        print(f"✓ Timeseries table exists with {count} rows")
    else:
        print("✗ No data returned")

except Exception as e:
    print(f"✗ Error: {e}")
