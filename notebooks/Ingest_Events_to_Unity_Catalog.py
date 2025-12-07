# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest PI Event Frames to Unity Catalog
# MAGIC
# MAGIC This notebook ingests event frames from PI Web API to Unity Catalog using the Lakeflow connector.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Mock PI server running on port 8010
# MAGIC - Databricks SQL Warehouse: 4b9b953939869799
# MAGIC
# MAGIC **Output Table:** `osipi.bronze.pi_event_frames`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install Dependencies and Import Libraries

# COMMAND ----------

# Import libraries
import sys
sys.path.append("/Workspace/Users/pravin.varma@databricks.com/osipi-connector")

from src.connector.pi_lakeflow_connector import PILakeflowConnector
from src.extractors.event_frame_extractor import EventFrameExtractor
from src.writers.delta_writer import DeltaLakeWriter
from databricks.sdk import WorkspaceClient
from datetime import datetime, timedelta
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Configure Connector

# COMMAND ----------

config = {
    "pi_web_api_url": "http://localhost:8010/piwebapi",
    "pi_server_name": "F1DP-Archive",
    "pi_af_database": "F1DP-DB-Production",
    "databricks_catalog": "osipi",
    "databricks_schema": "bronze",
    "databricks_warehouse_id": "4b9b953939869799",
    "auth_mode": "basic",
    "username": "admin",
    "password": "admin"
}

print("Initializing PI Lakeflow Connector...")
connector = PILakeflowConnector(config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Initialize Databricks Components

# COMMAND ----------

# Spark session is already available in Databricks
# Just initialize workspace client and Delta writer

w = WorkspaceClient()

event_writer = DeltaLakeWriter(
    spark=spark,  # spark is predefined in Databricks
    workspace_client=w,
    catalog="osipi",
    schema="bronze"
)

print("✅ Databricks components initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Extract Event Frames from PI

# COMMAND ----------

print("Extracting event frames from PI Web API...")
end_time = datetime.now()
start_time = end_time - timedelta(days=30)

event_extractor = EventFrameExtractor(connector.client)
events_df = event_extractor.extract_event_frames(
    database_webid="F1DP-DB-Production",
    start_time=start_time,
    end_time=end_time
)

print(f"✅ Extracted {len(events_df)} event frames")
display(events_df.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Transform and Load to Unity Catalog

# COMMAND ----------

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

    print("Writing events to Unity Catalog...")
    event_writer.write_event_frames(final_df)

    print(f"✅ Successfully ingested {len(final_df)} event frames to osipi.bronze.pi_event_frames")

    # Show sample
    print("\nSample events ingested:")
    display(final_df[['name', 'template_name', 'start_time']].head(10))
else:
    print("❌ No events found to ingest")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Ingestion

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) as total_events,
# MAGIC   COUNT(DISTINCT template_name) as event_types,
# MAGIC   MIN(start_time) as earliest_event,
# MAGIC   MAX(start_time) as latest_event
# MAGIC FROM osipi.bronze.pi_event_frames

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show sample events by type
# MAGIC SELECT
# MAGIC   template_name,
# MAGIC   COUNT(*) as event_count
# MAGIC FROM osipi.bronze.pi_event_frames
# MAGIC GROUP BY template_name
# MAGIC ORDER BY event_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show recent events
# MAGIC SELECT *
# MAGIC FROM osipi.bronze.pi_event_frames
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 10

# COMMAND ----------

print("✅ Event ingestion complete!")
print("Events are now available in Unity Catalog: osipi.bronze.pi_event_frames")
