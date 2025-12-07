"""
Ingest PI Event Frames to Unity Catalog using Lakeflow Connector
Uses existing connector infrastructure - no SQL writing
"""
from src.connector.pi_lakeflow_connector import PILakeflowConnector
from src.extractors.event_frame_extractor import EventFrameExtractor
from src.writers.delta_writer import DeltaLakeWriter
from databricks.sdk import WorkspaceClient
from datetime import datetime, timedelta
import pandas as pd

# Initialize connector
config = {
    "pi_web_api_url": "http://localhost:8010/piwebapi",
    "pi_server_name": "F1DP-Archive",
    "pi_af_database": "F1DP-DB-Production",
    "databricks_catalog": "osipi",
    "databricks_schema": "bronze",
    "databricks_warehouse_id": "4b9b953939869799",
    "auth_mode": "basic",  # Mock server doesn't need auth
    "username": "admin",
    "password": "admin"
}

print("Initializing PI Lakeflow Connector...")
connector = PILakeflowConnector(config)

# Initialize Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("PI Event Frame Ingestion") \
    .config("spark.databricks.service.server.enabled", "true") \
    .getOrCreate()

# Initialize Databricks workspace client
w = WorkspaceClient()

# Initialize Delta Writer for event frames
event_writer = DeltaLakeWriter(
    spark=spark,
    workspace_client=w,
    catalog="osipi",
    schema="bronze"
)

# Extract events from last 30 days
print("\nExtracting event frames...")
end_time = datetime.now()
start_time = end_time - timedelta(days=30)

event_extractor = EventFrameExtractor(connector.client)
events_df = event_extractor.extract_event_frames(
    database_webid="F1DP-DB-Production",
    start_time=start_time,
    end_time=end_time
)

print(f"Extracted {len(events_df)} event frames")

# Transform data for Delta Lake
if len(events_df) > 0:
    # Flatten attributes into columns
    events_df['attributes'] = events_df['event_attributes'].apply(lambda x: x if x else {})

    # Add ingestion metadata
    events_df['ingestion_timestamp'] = pd.Timestamp.now()
    events_df['partition_date'] = events_df['start_time'].dt.date

    # Rename columns to match Delta table schema
    events_df_renamed = events_df.rename(columns={
        'event_frame_id': 'webid',
        'event_name': 'name',
        'template_name': 'template_name',
        'start_time': 'start_time',
        'end_time': 'end_time',
        'primary_element_id': 'primary_referenced_element_webid',
        'description': 'description',
        'category': 'category_names',
        'attributes': 'attributes'
    })

    # Select only required columns
    final_df = events_df_renamed[[
        'webid', 'name', 'template_name', 'start_time', 'end_time',
        'primary_referenced_element_webid', 'description', 'category_names',
        'attributes', 'ingestion_timestamp', 'partition_date'
    ]]

    print("\nWriting events to Unity Catalog...")
    # Write to Delta Lake using DeltaWriter
    event_writer.write_event_frames(final_df)

    print(f"✅ Successfully ingested {len(final_df)} event frames to osipi.bronze.pi_event_frames")

    # Show sample
    print("\nSample events ingested:")
    print(final_df[['name', 'template_name', 'start_time']].head())
else:
    print("No events found to ingest")

print("\n✅ Event ingestion complete!")
