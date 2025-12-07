"""
OSI PI Lakeflow Connector using Databricks Zerobus Ingest SDK

This is the official Lakeflow Connect implementation for ingesting real-time
data from OSI PI Web API into Databricks Unity Catalog.

Architecture:
- WebSocket stream from PI Web API → Zerobus SDK → Unity Catalog table
- Direct ingestion using gRPC protocol (no staging files needed)
- Automatic schema validation and type checking
- Built-in backpressure management and recovery

Usage:
    python lakeflow_osipi_connector.py
"""

import asyncio
import websockets
import json
import os
from datetime import datetime
from databricks_zerobus_ingest_sdk import ZerobusSdk, TableProperties, StreamConfigurationOptions
from databricks.sdk import WorkspaceClient

# Configuration
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
CATALOG = os.getenv("UC_CATALOG", "osipi")
SCHEMA = os.getenv("UC_SCHEMA", "bronze")
TABLE = os.getenv("UC_TABLE", "pi_realtime")
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE}"

# PI Web API Configuration
PI_SERVER_URL = os.getenv("PI_SERVER_URL", "ws://localhost:8010")
PI_WEBSOCKET_ENDPOINT = f"{PI_SERVER_URL}/piwebapi/streams/channel"

# Tags to subscribe to
SUBSCRIBED_TAGS = [
    "F1DP-Eraring-U001-Temp-000001",
    "F1DP-Eraring-U001-Pres-000002",
    "F1DP-Eraring-U001-Flow-000003",
    "F1DP-Eraring-U002-Temp-000009",
]


class OSIPILakeflowConnector:
    """
    Lakeflow connector for OSI PI Web API using Zerobus Ingest SDK.

    This connector:
    1. Connects to PI Web API via WebSocket
    2. Subscribes to real-time sensor streams
    3. Ingests data directly into Unity Catalog using Zerobus SDK
    4. Handles automatic recovery and backpressure
    """

    def __init__(self):
        """Initialize the Lakeflow connector."""
        print("🚀 Initializing OSI PI Lakeflow Connector...")

        # Initialize Databricks client
        if DATABRICKS_HOST and DATABRICKS_TOKEN:
            self.workspace_client = WorkspaceClient(
                host=DATABRICKS_HOST,
                token=DATABRICKS_TOKEN
            )
        else:
            # Use Databricks CLI config
            self.workspace_client = WorkspaceClient()

        # Ensure table exists
        self._ensure_table_exists()

        # Initialize Zerobus SDK
        self.zerobus_sdk = ZerobusSdk(
            server_endpoint=f"{DATABRICKS_HOST}/api/2.0/zerobus/ingest",
            workspace_url=DATABRICKS_HOST
        )

        # Table properties for Zerobus
        self.table_props = TableProperties(
            catalog_name=CATALOG,
            schema_name=SCHEMA,
            table_name=TABLE
        )

        # Stream configuration
        self.stream_config = StreamConfigurationOptions(
            max_inflight_records=100,
            enable_recovery=True
        )

        print(f"✅ Connector initialized for table: {FULL_TABLE_NAME}")

    def _ensure_table_exists(self):
        """Create the Unity Catalog table if it doesn't exist."""
        try:
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} (
                tag_webid STRING NOT NULL,
                tag_name STRING,
                timestamp TIMESTAMP NOT NULL,
                value DOUBLE,
                units STRING,
                quality_good BOOLEAN,
                quality_questionable BOOLEAN,
                quality_substituted BOOLEAN,
                ingestion_timestamp TIMESTAMP,
                sensor_type STRING,
                plant STRING,
                unit INT,
                CONSTRAINT pk PRIMARY KEY (tag_webid, timestamp)
            )
            USING DELTA
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.columnMapping.mode' = 'name',
                'description' = 'Real-time OSI PI sensor data via Lakeflow Connect'
            )
            """

            self.workspace_client.statement_execution.execute_statement(
                statement=create_table_sql,
                warehouse_id=os.getenv("DATABRICKS_WAREHOUSE_ID", "4b9b953939869799"),
                wait_timeout="30s"
            )

            print(f"✅ Table {FULL_TABLE_NAME} ready")

        except Exception as e:
            print(f"⚠️  Error ensuring table exists: {e}")

    async def ingest_from_websocket(self):
        """
        Connect to PI Web API WebSocket and ingest data using Zerobus SDK.

        This is the main ingestion loop that:
        1. Connects to WebSocket
        2. Subscribes to sensor tags
        3. Receives real-time messages
        4. Ingests to Unity Catalog via Zerobus SDK
        """
        print(f"🔌 Connecting to PI WebSocket: {PI_WEBSOCKET_ENDPOINT}")

        try:
            # Create Zerobus stream
            stream = self.zerobus_sdk.create_stream(
                table_properties=self.table_props,
                options=self.stream_config
            )

            print("✅ Zerobus stream created")

            async with websockets.connect(PI_WEBSOCKET_ENDPOINT) as websocket:
                print("✅ WebSocket connected")

                # Subscribe to all configured tags
                for tag_webid in SUBSCRIBED_TAGS:
                    subscribe_msg = {
                        "Action": "Subscribe",
                        "Resource": f"streams/{tag_webid}/value",
                        "Parameters": {"updateRate": 1000}
                    }
                    await websocket.send(json.dumps(subscribe_msg))
                    print(f"📡 Subscribed to {tag_webid}")

                # Ingestion loop
                message_count = 0
                while True:
                    try:
                        # Receive message from WebSocket
                        message = await websocket.recv()
                        data = json.loads(message)

                        # Handle subscription confirmation
                        if data.get("Action") == "Subscribed":
                            print(f"✅ Subscription confirmed: {data.get('Resource')}")
                            continue

                        # Process data messages
                        if "Items" in data and len(data["Items"]) > 0:
                            item = data["Items"][0]

                            # Extract tag info from resource
                            resource = data.get("Resource", "")
                            tag_webid = resource.split("streams/")[1].split("/")[0] if "streams/" in resource else "Unknown"

                            # Parse tag name for metadata
                            # Format: PlantName_UnitXXX_SensorType_PV
                            tag_parts = tag_webid.split('-')
                            plant = tag_parts[1] if len(tag_parts) > 1 else "Unknown"
                            unit_str = tag_parts[2] if len(tag_parts) > 2 else "U000"
                            unit_num = int(unit_str.replace('U', '').lstrip('0') or 0)
                            sensor_type = tag_parts[3] if len(tag_parts) > 3 else "Unknown"

                            # Create record for Zerobus
                            record = {
                                "tag_webid": tag_webid,
                                "tag_name": f"{plant}_Unit{unit_num:03d}_{sensor_type}_PV",
                                "timestamp": item.get("Timestamp"),
                                "value": float(item.get("Value", 0)),
                                "units": item.get("UnitsAbbreviation", ""),
                                "quality_good": item.get("Good", True),
                                "quality_questionable": item.get("Questionable", False),
                                "quality_substituted": item.get("Substituted", False),
                                "ingestion_timestamp": datetime.utcnow().isoformat() + "Z",
                                "sensor_type": sensor_type,
                                "plant": plant,
                                "unit": unit_num
                            }

                            # Ingest via Zerobus SDK
                            ack_future = stream.ingest_record(record)

                            # Optionally await acknowledgment
                            # await ack_future

                            message_count += 1
                            if message_count % 10 == 0:
                                print(f"📊 Ingested {message_count} records to {FULL_TABLE_NAME}")

                    except websockets.exceptions.ConnectionClosed:
                        print("⚠️  WebSocket connection closed, reconnecting...")
                        break
                    except Exception as e:
                        print(f"❌ Error processing message: {e}")
                        continue

        except Exception as e:
            print(f"❌ Connector error: {e}")
            import traceback
            traceback.print_exc()

        finally:
            print(f"🏁 Connector stopped. Total messages ingested: {message_count}")

    async def run(self):
        """Run the connector with automatic reconnection."""
        while True:
            try:
                await self.ingest_from_websocket()
            except Exception as e:
                print(f"❌ Connection lost: {e}")
                print("🔄 Reconnecting in 5 seconds...")
                await asyncio.sleep(5)


async def main():
    """Main entry point for the Lakeflow connector."""
    print("""
╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║        OSI PI Lakeflow Connector (Zerobus Ingest SDK)               ║
║                                                                      ║
║   Real-time ingestion from PI Web API to Databricks Unity Catalog   ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
    """)

    print(f"Configuration:")
    print(f"  Databricks Host: {DATABRICKS_HOST}")
    print(f"  Target Table: {FULL_TABLE_NAME}")
    print(f"  PI Server: {PI_SERVER_URL}")
    print(f"  Subscribed Tags: {len(SUBSCRIBED_TAGS)}")
    print()

    # Create and run connector
    connector = OSIPILakeflowConnector()
    await connector.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n👋 Connector stopped by user")
