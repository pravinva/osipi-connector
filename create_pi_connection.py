"""
Create a Lakeflow Connection for PI Web API
"""

import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ConnectionType

# Configuration
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://e2-demo-field-eng.cloud.databricks.com")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "YOUR_DATABRICKS_TOKEN")

# Initialize client
w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

# Connection details
CONNECTION_NAME = "osipi_webapi_connector"
PI_SERVER_URL = "http://localhost:8010"  # Or the Databricks Apps URL once deployed

print("=" * 80)
print("CREATING PI WEB API LAKEFLOW CONNECTION")
print("=" * 80)

try:
    # Check if connection already exists
    try:
        existing = w.connections.get(name=CONNECTION_NAME)
        print(f"\n✓ Connection '{CONNECTION_NAME}' already exists")
        print(f"  Connection ID: {existing.connection_id}")
        print(f"  Type: {existing.connection_type}")
        print(f"  Status: {existing.provisioning_info.state if existing.provisioning_info else 'N/A'}")
    except Exception:
        # Connection doesn't exist, create it
        print(f"\nCreating new connection: {CONNECTION_NAME}")

        # For a REST API like PI Web API, we'll use HTTP connection type
        connection = w.connections.create(
            name=CONNECTION_NAME,
            connection_type=ConnectionType.HTTP,
            comment="PI Web API Lakeflow Connector for OSIPI data ingestion",
            options={
                "host": "localhost:8010",  # Will be updated to Databricks Apps URL
                "bearer_token": "{{secrets/demo/pi_api_token}}",  # Reference to secret (can be demo value)
                "base_path": "/piwebapi"
            }
        )

        print(f"✓ Connection created successfully!")
        print(f"  Connection ID: {connection.connection_id}")
        print(f"  Name: {connection.name}")
        print(f"  Type: {connection.connection_type}")

    print("\n" + "=" * 80)
    print("VIEW IN WORKSPACE")
    print("=" * 80)
    print(f"Navigate to: Data > Connections")
    print(f"Or: Catalog > External Data > Connections")
    print(f"Connection name: {CONNECTION_NAME}")
    print("=" * 80)

except Exception as e:
    print(f"\n✗ Error creating connection: {e}")
    print("\nNote: HTTP connection type may not be available in all workspaces.")
    print("Alternative: Show the ingestion pipeline as the 'connector' implementation")
