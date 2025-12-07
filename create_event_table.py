"""Create PI Event Frames table in Unity Catalog"""
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Create event frames table
create_table_sql = """
CREATE TABLE IF NOT EXISTS osipi.bronze.pi_event_frames (
    webid STRING,
    name STRING,
    template_name STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    primary_referenced_element_webid STRING,
    description STRING,
    category_names ARRAY<STRING>,
    attributes MAP<STRING, STRING>,
    ingestion_timestamp TIMESTAMP,
    partition_date DATE
)
USING DELTA
PARTITIONED BY (partition_date)
COMMENT 'PI Asset Framework Event Frames (Batch Runs, Alarms, Maintenance, Downtime)'
"""

print("Creating osipi.bronze.pi_event_frames table...")

try:
    stmt = w.statement_execution.execute_statement(
        statement=create_table_sql,
        warehouse_id="4b9b953939869799",
        wait_timeout="30s"
    )
    print("✅ Table created successfully!")

    # Check if table is empty and insert mock data
    check_sql = "SELECT COUNT(*) FROM osipi.bronze.pi_event_frames"
    stmt = w.statement_execution.execute_statement(
        statement=check_sql,
        warehouse_id="4b9b953939869799",
        wait_timeout="30s"
    )

    if stmt.result and stmt.result.data_array:
        count = stmt.result.data_array[0][0]
        print(f"Current row count: {count}")

        if count == 0:
            print("\nInserting sample event frames...")
            # We'll use the mock server to generate events and insert them
            print("Run: python3 ingest_events_to_uc.py")

except Exception as e:
    print(f"❌ Error: {e}")
