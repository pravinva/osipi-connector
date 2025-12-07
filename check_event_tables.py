"""Check for event-related tables in Unity Catalog"""
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# List all tables in osipi schema
try:
    # Check osipi schema
    stmt = w.statement_execution.execute_statement(
        statement="SHOW TABLES IN osipi.bronze",
        warehouse_id="4b9b953939869799",
        wait_timeout="30s"
    )

    print("Tables in osipi.bronze:")
    if stmt.result and stmt.result.data_array:
        for row in stmt.result.data_array:
            print(f"  - {row[1]}")  # table name is in second column

    # Check if there's an events table
    stmt2 = w.statement_execution.execute_statement(
        statement="SHOW TABLES IN osipi.bronze LIKE '*event*'",
        warehouse_id="4b9b953939869799",
        wait_timeout="30s"
    )

    print("\nEvent-related tables:")
    if stmt2.result and stmt2.result.data_array:
        for row in stmt2.result.data_array:
            print(f"  - {row[1]}")
    else:
        print("  (No event tables found)")

except Exception as e:
    print(f"Error: {e}")
