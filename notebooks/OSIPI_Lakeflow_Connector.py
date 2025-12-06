# Databricks notebook source
# MAGIC %md
# MAGIC # OSIPI Lakeflow Connector
# MAGIC
# MAGIC **Purpose:** Continuous ingestion from PI Web API to Delta Lake
# MAGIC **Data Source:** AVEVA PI Web API (Industrial IoT time-series data)
# MAGIC **Target:** `osipi.bronze.pi_timeseries` Delta table
# MAGIC **Architecture:** Incremental ingestion with checkpointing

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

# Configuration
PI_SERVER_URL = "http://localhost:8010"  # Update to Databricks Apps URL when deployed
CATALOG = "osipi"
SCHEMA = "bronze"
TABLE = "pi_timeseries"
CHECKPOINT_TABLE = f"{CATALOG}.checkpoints.pi_ingestion_checkpoint"

print(f"""
Lakeflow Connector Configuration
================================
PI Server: {PI_SERVER_URL}
Target Table: {CATALOG}.{SCHEMA}.{TABLE}
Checkpoint: {CHECKPOINT_TABLE}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Get Last Checkpoint

# COMMAND ----------

# Get last ingestion timestamp from checkpoint
try:
    last_checkpoint_df = spark.sql(f"""
        SELECT MAX(last_ingestion_time) as last_time
        FROM {CHECKPOINT_TABLE}
    """)
    last_ingestion = last_checkpoint_df.first()["last_time"]

    if last_ingestion:
        print(f"✓ Last ingestion: {last_ingestion}")
    else:
        last_ingestion = None
        print("ℹ No previous checkpoint found - performing full load")

except Exception as e:
    # Checkpoint table doesn't exist yet
    print("ℹ Creating checkpoint table...")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            last_ingestion_time TIMESTAMP,
            rows_ingested INT,
            ingestion_run_id STRING,
            created_at TIMESTAMP
        )
    """)
    last_ingestion = None
    print("✓ Checkpoint table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Discover Available Tags

# COMMAND ----------

import requests
from datetime import datetime, timedelta

# Calculate time window
if last_ingestion:
    start_time = last_ingestion
    end_time = datetime.utcnow()
    print(f"📊 Incremental load: {start_time} to {end_time}")
else:
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=2)
    print(f"📊 Full load: {start_time} to {end_time}")

# Discover all tags from PI Server
print(f"\n🔍 Discovering PI tags from {PI_SERVER_URL}...")

try:
    response = requests.get(f"{PI_SERVER_URL}/piwebapi/dataservers", timeout=10)
    data_servers = response.json()["Items"]

    all_tags = []
    for server in data_servers:
        points_response = requests.get(server["Links"]["Points"])
        points = points_response.json()["Items"]
        all_tags.extend(points)

    print(f"✓ Found {len(all_tags)} tags")

    # Show sample tags
    print("\nSample tags:")
    for tag in all_tags[:5]:
        print(f"  - {tag['Name']} ({tag.get('EngineeringUnits', 'N/A')})")

except Exception as e:
    print(f"✗ Error connecting to PI Server: {e}")
    print(f"⚠️  Make sure PI Server is running at {PI_SERVER_URL}")
    dbutils.notebook.exit("Failed to connect to PI Server")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Extract Data via Batch API

# COMMAND ----------

# Build batch requests for all tags
batch_requests = []
for tag in all_tags:
    batch_requests.append({
        "Method": "GET",
        "Resource": f"{tag['Links']['RecordedData']}?startTime={start_time.isoformat()}Z&endTime={end_time.isoformat()}Z&maxCount=1000"
    })

print(f"📦 Executing batch request for {len(batch_requests)} tags...")

# Execute batch request
batch_response = requests.post(
    f"{PI_SERVER_URL}/piwebapi/batch",
    json={"Requests": batch_requests},
    timeout=60
)
batch_results = batch_response.json()

# Parse results into rows
rows = []
for i, result in enumerate(batch_results):
    if result["Status"] == 200:
        tag = all_tags[i]
        content = result["Content"]

        for item in content.get("Items", []):
            rows.append({
                "tag_webid": tag["WebId"],
                "tag_name": tag["Name"],
                "timestamp": item["Timestamp"],
                "value": item["Value"],
                "units": tag.get("EngineeringUnits", ""),
                "quality_good": item.get("Good", True),
                "quality_questionable": item.get("Questionable", False),
                "quality_substituted": item.get("Substituted", False),
                "ingestion_timestamp": datetime.utcnow().isoformat() + "Z",
                "sensor_type": tag["Name"].split('-')[1] if '-' in tag["Name"] else "Unknown",
                "plant": tag["Name"].split('-')[0] if '-' in tag["Name"] else "Unknown",
                "unit": int(tag["Name"].split('-')[2].replace('U', '')) if len(tag["Name"].split('-')) > 2 else 0
            })

print(f"✓ Extracted {len(rows)} data points")

if len(rows) == 0:
    print("ℹ No new data to ingest")
    dbutils.notebook.exit("No new data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Load to Delta Table

# COMMAND ----------

from pyspark.sql.types import *

# Define schema
schema = StructType([
    StructField("tag_webid", StringType(), True),
    StructField("tag_name", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("units", StringType(), True),
    StructField("quality_good", BooleanType(), True),
    StructField("quality_questionable", BooleanType(), True),
    StructField("quality_substituted", BooleanType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("sensor_type", StringType(), True),
    StructField("plant", StringType(), True),
    StructField("unit", IntegerType(), True)
])

# Create DataFrame
df = spark.createDataFrame(rows, schema)

# Convert timestamp strings to proper timestamps
df = df.withColumn("timestamp", df["timestamp"].cast(TimestampType())) \
       .withColumn("ingestion_timestamp", df["ingestion_timestamp"].cast(TimestampType()))

print(f"📊 DataFrame created with {df.count()} rows")
print("\nSample data:")
display(df.limit(10))

# COMMAND ----------

# Append to Delta table
print(f"💾 Writing to {CATALOG}.{SCHEMA}.{TABLE}...")

df.write \
  .format("delta") \
  .mode("append") \
  .saveAsTable(f"{CATALOG}.{SCHEMA}.{TABLE}")

print(f"✓ Successfully loaded {len(rows)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Update Checkpoint

# COMMAND ----------

# Record checkpoint
run_id = f"run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

checkpoint_data = [(
    end_time,
    len(rows),
    run_id,
    datetime.utcnow()
)]

checkpoint_df = spark.createDataFrame(
    checkpoint_data,
    ["last_ingestion_time", "rows_ingested", "ingestion_run_id", "created_at"]
)

checkpoint_df.write.format("delta").mode("append").saveAsTable(CHECKPOINT_TABLE)

print(f"✓ Checkpoint updated: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Ingestion Summary

# COMMAND ----------

# Get total row count
total_rows = spark.sql(f"SELECT COUNT(*) as count FROM {CATALOG}.{SCHEMA}.{TABLE}").first()["count"]

# Get ingestion history
history_df = spark.sql(f"""
    SELECT
        ingestion_run_id,
        last_ingestion_time,
        rows_ingested,
        created_at
    FROM {CHECKPOINT_TABLE}
    ORDER BY created_at DESC
    LIMIT 10
""")

print("=" * 80)
print("✅ INGESTION COMPLETE")
print("=" * 80)
print(f"Run ID: {run_id}")
print(f"Rows ingested this run: {len(rows)}")
print(f"Total rows in table: {total_rows:,}")
print(f"Time window: {start_time} to {end_time}")
print("=" * 80)

print("\n📋 Recent ingestion history:")
display(history_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Data Quality Check

# COMMAND ----------

# Query recent data
recent_data = spark.sql(f"""
    SELECT
        plant,
        sensor_type,
        COUNT(*) as data_points,
        COUNT(DISTINCT tag_webid) as unique_tags,
        MIN(timestamp) as earliest,
        MAX(timestamp) as latest,
        SUM(CASE WHEN quality_good THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as quality_pct
    FROM {CATALOG}.{SCHEMA}.{TABLE}
    GROUP BY plant, sensor_type
    ORDER BY plant, sensor_type
""")

print("📊 Data Quality Summary:")
display(recent_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. **Schedule this notebook** as a Databricks Workflow to run every 15 minutes
# MAGIC 2. **Monitor ingestion** via the checkpoint table
# MAGIC 3. **View data** in the osipi catalog
# MAGIC 4. **Query metrics** from the gold layer (osipi.gold.pi_metrics_hourly)
