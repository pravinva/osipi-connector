"""
Check checkpoint table status
"""
import os
from databricks.sdk import WorkspaceClient

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://e2-demo-field-eng.cloud.databricks.com")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "YOUR_DATABRICKS_TOKEN")
WAREHOUSE_ID = "4b9b953939869799"

w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

# Check if checkpoint table exists
print("Checking checkpoint table...")
try:
    result = w.statement_execution.execute_statement(
        statement="SELECT * FROM osipi.checkpoints.pi_ingestion_checkpoint ORDER BY created_at DESC LIMIT 10",
        warehouse_id=WAREHOUSE_ID,
        wait_timeout="50s"
    )

    if result.result and result.result.data_array:
        print(f"\nFound {len(result.result.data_array)} checkpoint records:")
        for row in result.result.data_array:
            print(f"  {row}")
    else:
        print("\n⚠️  Checkpoint table exists but is EMPTY")
        print("\nReason: The connector notebook has never been run!")
        print("\nTo populate checkpoints:")
        print("  1. Open the notebook in Databricks workspace")
        print("  2. Run all cells")
        print("  3. Checkpoint will be created after successful ingestion")

except Exception as e:
    if "Table or view not found" in str(e):
        print("\n⚠️  Checkpoint table does NOT exist yet")
        print("\nReason: The notebook creates it on first run")
        print("\nTo create the checkpoint table:")
        print("  1. Open notebook: /Users/pravin.varma@databricks.com/OSIPI_Lakeflow_Connector")
        print("  2. Run all cells")
        print("  3. Table will be created automatically")
    else:
        print(f"\n✗ Error: {e}")
