# Databricks notebook source
# MAGIC %md
# MAGIC # Extract PI Event Frames
# MAGIC
# MAGIC Extracts event frames (batch runs, downtimes, alarms) from PI Server.

# COMMAND ----------

from datetime import datetime, timedelta
import sys
import os

# Add src to path
sys.path.append("/Workspace" + os.path.dirname(os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())))

from src.connector.pi_lakeflow_connector import PILakeflowConnector

# COMMAND ----------

# Get parameters
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
pi_web_api_url = dbutils.widgets.get("pi_web_api_url")

# COMMAND ----------

# Get authentication
pi_username = dbutils.secrets.get(scope="pi-connector", key="username")
pi_password = dbutils.secrets.get(scope="pi-connector", key="password")

# Configure connector
config = {
    'pi_web_api_url': pi_web_api_url,
    'auth': {
        'type': 'basic',
        'username': pi_username,
        'password': pi_password
    },
    'catalog': catalog,
    'schema': schema
}

connector = PILakeflowConnector(config)

# COMMAND ----------

# Get AF database WebID (same as AF hierarchy extraction)
import requests

response = requests.get(
    f"{pi_web_api_url}/piwebapi/assetservers",
    auth=(pi_username, pi_password),
    timeout=30
)
asset_servers = response.json()["Items"]
server_webid = asset_servers[0]["WebId"]

response = requests.get(
    f"{pi_web_api_url}/piwebapi/assetservers/{server_webid}/assetdatabases",
    auth=(pi_username, pi_password),
    timeout=30
)
databases = response.json()["Items"]
database_webid = databases[0]["WebId"]

print(f"✓ AF Database: {databases[0]['Name']}")

# COMMAND ----------

# Get checkpoint time
checkpoint_table = f"{catalog}.checkpoints.pi_event_frames_watermark"

try:
    last_checkpoint = spark.sql(f"""
        SELECT MAX(last_timestamp) as max_time
        FROM {checkpoint_table}
    """).collect()[0].max_time
    
    start_time = last_checkpoint if last_checkpoint else datetime.now() - timedelta(days=7)
except:
    start_time = datetime.now() - timedelta(days=7)

end_time = datetime.now()

print(f"✓ Time range: {start_time} to {end_time}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract Event Frames

# COMMAND ----------

print("🔄 Extracting event frames...")

df_events = connector.extract_event_frames(
    database_webid=database_webid,
    start_time=start_time,
    end_time=end_time
)

event_count = df_events.count()
print(f"✅ Extracted {event_count} event frames")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Lake

# COMMAND ----------

# Write events (append mode)
events_table = f"{catalog}.{schema}.pi_event_frames"
df_events.write.format("delta").mode("append").saveAsTable(events_table)

print(f"✅ Written to {events_table}")

# COMMAND ----------

# Update checkpoint
checkpoint_data = [{
    'last_timestamp': end_time,
    'events_ingested': event_count,
    'updated_at': datetime.now()
}]

df_checkpoint = spark.createDataFrame(checkpoint_data)
df_checkpoint.write.format("delta").mode("append").saveAsTable(checkpoint_table)

print(f"✅ Checkpoint updated")

# COMMAND ----------

# Display sample
if event_count > 0:
    print("\n📊 Event Frames Sample:")
    df_events.limit(10).display()

# COMMAND ----------

# Summary
print(f"""
╔══════════════════════════════════════════════════════════════╗
║           Event Frames Extraction Complete                   ║
╚══════════════════════════════════════════════════════════════╝

Event Frames: {event_count}
Time Range: {start_time} to {end_time}
Table: {events_table}

✅ Event frames extraction complete!
""")

