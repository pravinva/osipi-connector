# Databricks notebook source
# MAGIC %md
# MAGIC # PI Web API Mock Server → osipi.bronze Ingestion
# MAGIC
# MAGIC This notebook ingests data from the mock PI Web API server (Databricks App) into Unity Catalog bronze tables.
# MAGIC
# MAGIC **Architecture:**
# MAGIC ```
# MAGIC Mock PI Web API (Databricks App)
# MAGIC   ↓ HTTP REST API calls
# MAGIC   ↓ Extract: /piwebapi/dataservers/{server}/points
# MAGIC   ↓         /piwebapi/streams/{webid}/recorded
# MAGIC   ↓         /piwebapi/assetdatabases/{db}/elements
# MAGIC   ↓         /piwebapi/assetdatabases/{db}/eventframes
# MAGIC   ↓
# MAGIC Transform & Load (this notebook)
# MAGIC   ↓
# MAGIC osipi.bronze.pi_timeseries
# MAGIC osipi.bronze.pi_af_hierarchy
# MAGIC osipi.bronze.pi_event_frames
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, explode
from pyspark.sql.types import *
from datetime import datetime, timedelta
import json

# Configuration
MOCK_API_URL = "https://osipi-webserver-1444828305810485.aws.databricksapps.com"
UC_CATALOG = "osipi"
UC_SCHEMA = "bronze"

# API endpoints
DATASERVERS_ENDPOINT = f"{MOCK_API_URL}/piwebapi/dataservers"
ASSETDATABASES_ENDPOINT = f"{MOCK_API_URL}/piwebapi/assetdatabases"

print(f"Mock API URL: {MOCK_API_URL}")
print(f"Target: {UC_CATALOG}.{UC_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingest PI Points (Tags) Metadata

# COMMAND ----------

# Get data server
response = requests.get(DATASERVERS_ENDPOINT)
dataservers = response.json()["Items"]
server_webid = dataservers[0]["WebId"]

print(f"Data Server: {server_webid}")

# Get all PI points
points_url = f"{MOCK_API_URL}/piwebapi/dataservers/{server_webid}/points?maxCount=10000"
response = requests.get(points_url)
points = response.json()["Items"]

print(f"Found {len(points)} PI points")

# Convert to Spark DataFrame
points_data = []
for point in points:
    points_data.append({
        "webid": point["WebId"],
        "name": point["Name"],
        "units": point.get("EngineeringUnits", ""),
        "descriptor": point.get("Descriptor", ""),
        "path": point.get("Path", ""),
        "ingestion_timestamp": datetime.utcnow()
    })

points_df = spark.createDataFrame(points_data)
display(points_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest Timeseries Data

# COMMAND ----------

# For each tag, get last 24 hours of data
end_time = datetime.utcnow()
start_time = end_time - timedelta(hours=24)

start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

print(f"Fetching data from {start_time_str} to {end_time_str}")

timeseries_data = []

# Sample first 100 tags for demo (adjust as needed)
sample_points = points[:100]

for i, point in enumerate(sample_points):
    if i % 10 == 0:
        print(f"  Progress: {i}/{len(sample_points)} tags...")

    webid = point["WebId"]
    tag_name = point["Name"]

    # Extract metadata from tag name (format: Plant_UnitXXX_SensorType_PV)
    parts = tag_name.split("_")
    plant = parts[0] if len(parts) > 0 else "Unknown"
    unit_str = parts[1] if len(parts) > 1 else "Unit000"
    unit_num = int(unit_str.replace("Unit", "")) if "Unit" in unit_str else 0
    sensor_type = parts[2] if len(parts) > 2 else "Unknown"

    try:
        # Get recorded values
        recorded_url = f"{MOCK_API_URL}/piwebapi/streams/{webid}/recorded"
        params = {"startTime": start_time_str, "endTime": end_time_str, "maxCount": 1000}
        response = requests.get(recorded_url, params=params)

        if response.status_code == 200:
            data = response.json()
            items = data.get("Items", [])

            for item in items:
                timeseries_data.append({
                    "tag_webid": webid,
                    "tag_name": tag_name,
                    "timestamp": item["Timestamp"],
                    "value": float(item["Value"]),
                    "units": point.get("EngineeringUnits", ""),
                    "quality": item.get("Good", True),
                    "plant": plant,
                    "unit": unit_num,
                    "sensor_type": sensor_type,
                    "ingestion_timestamp": datetime.utcnow()
                })

    except Exception as e:
        print(f"    Error fetching {tag_name}: {e}")

print(f"✓ Fetched {len(timeseries_data)} timeseries records")

# COMMAND ----------

# Convert to Spark DataFrame and write to Delta
if timeseries_data:
    timeseries_df = spark.createDataFrame(timeseries_data)

    # Cast quality to string
    timeseries_df = timeseries_df.withColumn("quality", col("quality").cast("string"))

    display(timeseries_df.limit(10))

    # Write to Delta table
    timeseries_df.write.mode("append").saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.pi_timeseries")

    print(f"✓ Wrote {timeseries_df.count()} records to {UC_CATALOG}.{UC_SCHEMA}.pi_timeseries")
else:
    print("⚠️ No timeseries data to write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingest AF Hierarchy

# COMMAND ----------

# Get asset databases
response = requests.get(ASSETDATABASES_ENDPOINT)
databases = response.json()["Items"]

print(f"Found {len(databases)} asset databases")

af_hierarchy_data = []

for db in databases:
    db_webid = db["WebId"]
    db_name = db["Name"]

    # Get root elements
    elements_url = f"{MOCK_API_URL}/piwebapi/assetdatabases/{db_webid}/elements"
    response = requests.get(elements_url)
    elements = response.json().get("Items", [])

    print(f"Database {db_name}: {len(elements)} root elements")

    # Recursively extract hierarchy
    def extract_elements(elements_list, parent_webid=None):
        for elem in elements_list:
            # Extract metadata
            parts = elem["Name"].split("_")
            plant = parts[0] if len(parts) > 0 else "Unknown"

            af_hierarchy_data.append({
                "webid": elem["WebId"],
                "name": elem["Name"],
                "template_name": elem.get("TemplateName", ""),
                "description": elem.get("Description", ""),
                "path": elem.get("Path", ""),
                "parent_webid": parent_webid,
                "plant": plant,
                "unit": None,  # Could extract from name if needed
                "equipment_type": elem.get("TemplateName", "").replace("Template", ""),
                "ingestion_timestamp": datetime.utcnow()
            })

            # Recursively process child elements
            if "Elements" in elem and elem["Elements"]:
                extract_elements(elem["Elements"], elem["WebId"])

    extract_elements(elements)

print(f"✓ Extracted {len(af_hierarchy_data)} AF elements")

# COMMAND ----------

# Write AF hierarchy to Delta
if af_hierarchy_data:
    af_df = spark.createDataFrame(af_hierarchy_data)

    display(af_df.limit(10))

    af_df.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.pi_af_hierarchy")

    print(f"✓ Wrote {af_df.count()} records to {UC_CATALOG}.{UC_SCHEMA}.pi_af_hierarchy")
else:
    print("⚠️ No AF hierarchy data to write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ingest Event Frames

# COMMAND ----------

# Get event frames for last 30 days
end_time = datetime.utcnow()
start_time = end_time - timedelta(days=30)

start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

event_frames_data = []

for db in databases:
    db_webid = db["WebId"]

    # Get event frames
    ef_url = f"{MOCK_API_URL}/piwebapi/assetdatabases/{db_webid}/eventframes"
    params = {"startTime": start_time_str, "endTime": end_time_str}
    response = requests.get(ef_url, params=params)

    if response.status_code == 200:
        event_frames = response.json().get("Items", [])

        print(f"Database {db['Name']}: {len(event_frames)} event frames")

        for ef in event_frames:
            event_frames_data.append({
                "webid": ef["WebId"],
                "name": ef["Name"],
                "template_name": ef.get("TemplateName", ""),
                "start_time": ef["StartTime"],
                "end_time": ef.get("EndTime"),
                "primary_element_webid": ef.get("PrimaryReferencedElementWebId"),
                "description": ef.get("Description", ""),
                "attributes": json.dumps(ef.get("Attributes", {})),  # Store as JSON string
                "ingestion_timestamp": datetime.utcnow()
            })

print(f"✓ Extracted {len(event_frames_data)} event frames")

# COMMAND ----------

# Write event frames to Delta
if event_frames_data:
    ef_df = spark.createDataFrame(event_frames_data)

    display(ef_df.limit(10))

    ef_df.write.mode("append").saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.pi_event_frames")

    print(f"✓ Wrote {ef_df.count()} records to {UC_CATALOG}.{UC_SCHEMA}.pi_event_frames")
else:
    print("⚠️ No event frames data to write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify Ingestion

# COMMAND ----------

# Check record counts
print("Ingestion Summary:")
print("=" * 80)

tables = ["pi_timeseries", "pi_af_hierarchy", "pi_event_frames"]

for table in tables:
    count_df = spark.sql(f"SELECT COUNT(*) as count FROM {UC_CATALOG}.{UC_SCHEMA}.{table}")
    count = count_df.collect()[0]["count"]
    print(f"{table:30} {count:>10,} records")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. **Schedule this notebook** to run periodically (e.g., every 15 minutes)
# MAGIC 2. **Add checkpoint logic** to only fetch new data since last run
# MAGIC 3. **Monitor the dashboard** at the Databricks App URL
# MAGIC 4. **Optimize queries** with Z-ordering on frequently queried columns
# MAGIC
# MAGIC ```sql
# MAGIC OPTIMIZE osipi.bronze.pi_timeseries ZORDER BY (tag_webid, timestamp);
# MAGIC OPTIMIZE osipi.bronze.pi_af_hierarchy ZORDER BY (webid, plant);
# MAGIC OPTIMIZE osipi.bronze.pi_event_frames ZORDER BY (webid, start_time);
# MAGIC ```
