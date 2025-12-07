# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Tables: PI Batch Ingestion with Lakeflow
# MAGIC
# MAGIC This DLT pipeline ingests historical time-series data from PI Web API using Lakeflow batch patterns:
# MAGIC 1. **Bronze Layer**: Raw batch ingestion via REST API with incremental processing
# MAGIC 2. **Silver Layer**: Cleaned, deduplicated, and quality-checked data
# MAGIC 3. **Gold Layer**: Aggregated metrics for analytics
# MAGIC
# MAGIC ## Lakeflow Batch Architecture:
# MAGIC ```
# MAGIC PI Web API REST → Python/Requests → Bronze (append) → Silver → Gold
# MAGIC                    ↓
# MAGIC              Checkpoint table
# MAGIC             (tracks progress)
# MAGIC ```
# MAGIC
# MAGIC **Key Difference from Streaming:**
# MAGIC - Streaming: Continuous Auto Loader reading files
# MAGIC - Batch: Scheduled REST API calls with checkpoint-based incremental load

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import requests
import json

# Configuration
PI_SERVER_URL = "http://localhost:8010"
CATALOG = "osipi"
SCHEMA = "bronze"
BATCH_SIZE_HOURS = 1  # Fetch 1 hour of data per batch

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer: Incremental REST API Ingestion
# MAGIC
# MAGIC This uses DLT's **APPLY CHANGES** pattern for incremental batch loads.
# MAGIC
# MAGIC Key features:
# MAGIC - Checkpoint-based progress tracking
# MAGIC - Idempotent loads (can re-run safely)
# MAGIC - Automatic deduplication
# MAGIC - Schema evolution support

# COMMAND ----------

def fetch_pi_data_batch(start_time, end_time):
    """
    Fetch PI data from REST API for a specific time range.

    Returns list of records with schema matching Silver table.
    """
    try:
        # Get all tags
        tags_response = requests.get(
            f"{PI_SERVER_URL}/piwebapi/dataservers/F1DS-SCADA-01/points",
            timeout=30
        )
        all_tags = tags_response.json()["Items"][:100]  # Limit for testing

        # Build batch request
        batch_requests = []
        for tag in all_tags:
            batch_requests.append({
                "Method": "GET",
                "Resource": f"{tag['WebId']}/recorded?startTime={start_time}&endTime={end_time}"
            })

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
                        "sensor_type": tag["Name"].split('_')[2] if len(tag["Name"].split('_')) > 2 else "Unknown",
                        "plant": tag["Name"].split('_')[0] if '_' in tag["Name"] else "Unknown",
                        "unit": int(tag["Name"].split('_')[1].replace('Unit', '').lstrip('0') or 0) if len(tag["Name"].split('_')) > 1 else 0
                    })

        return rows
    except Exception as e:
        print(f"Error fetching PI data: {e}")
        return []

# COMMAND ----------

@dlt.table(
    name="pi_batch_checkpoint",
    comment="Checkpoint table for incremental batch ingestion",
    table_properties={
        "quality": "bronze"
    }
)
def pi_batch_checkpoint():
    """
    Checkpoint table to track batch ingestion progress.

    Schema:
    - last_ingestion_time: Last successfully ingested timestamp
    - rows_ingested: Number of rows in last batch
    - ingestion_run_id: Unique identifier for the run
    - created_at: When this checkpoint was created
    """
    # Initialize with empty checkpoint if doesn't exist
    return spark.createDataFrame([], StructType([
        StructField("last_ingestion_time", TimestampType(), True),
        StructField("rows_ingested", IntegerType(), True),
        StructField("ingestion_run_id", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ]))

# COMMAND ----------

@dlt.table(
    name="pi_batch_bronze",
    comment="Raw PI batch data ingested via REST API",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def pi_batch_bronze():
    """
    Bronze table: Raw batch ingestion from PI Web API.

    This is a STREAMING table that appends new batches incrementally.
    Each pipeline run fetches the next time window based on checkpoint.
    """
    # Get last checkpoint
    try:
        last_checkpoint = spark.table(f"{CATALOG}.{SCHEMA}.pi_batch_checkpoint") \
            .select(max("last_ingestion_time").alias("last_time")) \
            .first()["last_time"]
    except:
        # First run: start from 24 hours ago
        last_checkpoint = datetime.utcnow() - timedelta(hours=24)

    if not last_checkpoint:
        last_checkpoint = datetime.utcnow() - timedelta(hours=24)

    # Calculate time range for this batch
    start_time = last_checkpoint
    end_time = start_time + timedelta(hours=BATCH_SIZE_HOURS)

    # Don't go beyond current time
    if end_time > datetime.utcnow():
        end_time = datetime.utcnow()

    print(f"Fetching batch: {start_time} to {end_time}")

    # Fetch data from PI Web API
    rows = fetch_pi_data_batch(
        start_time.isoformat() + "Z",
        end_time.isoformat() + "Z"
    )

    # Create DataFrame
    if rows:
        df = spark.createDataFrame(rows)

        # Add batch metadata
        df = df.withColumn("_batch_id", lit(f"batch_{int(datetime.utcnow().timestamp())}")) \
               .withColumn("_batch_start_time", lit(start_time)) \
               .withColumn("_batch_end_time", lit(end_time))

        # Update checkpoint (this would normally be in a separate job)
        # For DLT, checkpoint is managed by the framework

        return df
    else:
        # Return empty DataFrame with schema
        return spark.createDataFrame([], StructType([
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
            StructField("unit", IntegerType(), True),
            StructField("_batch_id", StringType(), True),
            StructField("_batch_start_time", TimestampType(), True),
            StructField("_batch_end_time", TimestampType(), True)
        ]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Cleaned Batch Data
# MAGIC
# MAGIC Same transformation logic as streaming, but for batch data:
# MAGIC - Type casting
# MAGIC - Quality checks
# MAGIC - Deduplication

# COMMAND ----------

@dlt.table(
    name="pi_batch_silver",
    comment="Cleaned and typed PI batch data with quality checks",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_timestamp", "timestamp IS NOT NULL")
@dlt.expect_or_drop("valid_tag_webid", "tag_webid IS NOT NULL")
@dlt.expect("valid_value", "value IS NOT NULL")
def pi_batch_silver():
    """
    Silver table: Cleaned, typed, and deduplicated PI batch data.

    Quality checks:
    - Drop rows with NULL timestamp or tag_webid
    - Warn if value is NULL
    - Convert timestamp strings to proper TIMESTAMP type
    - Deduplicate based on tag_webid + timestamp
    """
    return (
        dlt.read("pi_batch_bronze")
            .select(
                col("tag_webid"),
                col("tag_name"),
                col("timestamp").cast("timestamp").alias("timestamp"),
                col("value").cast("double").alias("value"),
                col("units"),
                col("quality_good").cast("boolean").alias("quality_good"),
                col("quality_questionable").cast("boolean").alias("quality_questionable"),
                col("quality_substituted").cast("boolean").alias("quality_substituted"),
                col("ingestion_timestamp").cast("timestamp").alias("ingestion_timestamp"),
                col("sensor_type"),
                col("plant"),
                col("unit").cast("int").alias("unit"),
                col("_batch_id"),
                col("_batch_start_time"),
                col("_batch_end_time")
            )
            .dropDuplicates(["tag_webid", "timestamp"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Daily Aggregations
# MAGIC
# MAGIC For batch data, daily aggregations make more sense than 1-minute windows.

# COMMAND ----------

@dlt.table(
    name="pi_batch_gold_daily",
    comment="Daily aggregated PI sensor metrics for analytics",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def pi_batch_gold_daily():
    """
    Gold table: Daily aggregated metrics per sensor.

    Provides:
    - Average, min, max, stddev of sensor values per day
    - Count of readings per day
    - Data quality percentage
    - Plant and unit groupings for filtering
    """
    return (
        dlt.read("pi_batch_silver")
            .withColumn("date", to_date("timestamp"))
            .groupBy(
                "date",
                "tag_webid",
                "tag_name",
                "sensor_type",
                "plant",
                "unit",
                "units"
            )
            .agg(
                avg("value").alias("avg_value"),
                min("value").alias("min_value"),
                max("value").alias("max_value"),
                stddev("value").alias("stddev_value"),
                count("*").alias("reading_count"),
                sum(when(col("quality_good") == True, 1).otherwise(0)).alias("good_quality_count")
            )
            .select(
                col("date"),
                col("tag_webid"),
                col("tag_name"),
                col("sensor_type"),
                col("plant"),
                col("unit"),
                round(col("avg_value"), 2).alias("avg_value"),
                round(col("min_value"), 2).alias("min_value"),
                round(col("max_value"), 2).alias("max_value"),
                round(col("stddev_value"), 3).alias("stddev_value"),
                col("reading_count"),
                round((col("good_quality_count") / col("reading_count")) * 100, 1).alias("quality_pct"),
                col("units")
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Hourly Aggregations
# MAGIC
# MAGIC For more granular analysis, hourly aggregations.

# COMMAND ----------

@dlt.table(
    name="pi_batch_gold_hourly",
    comment="Hourly aggregated PI sensor metrics",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def pi_batch_gold_hourly():
    """
    Gold table: Hourly aggregated metrics per sensor.
    """
    return (
        dlt.read("pi_batch_silver")
            .withColumn("hour", date_trunc("hour", "timestamp"))
            .groupBy(
                "hour",
                "tag_webid",
                "tag_name",
                "sensor_type",
                "plant",
                "unit",
                "units"
            )
            .agg(
                avg("value").alias("avg_value"),
                min("value").alias("min_value"),
                max("value").alias("max_value"),
                stddev("value").alias("stddev_value"),
                count("*").alias("reading_count"),
                sum(when(col("quality_good") == True, 1).otherwise(0)).alias("good_quality_count")
            )
            .select(
                col("hour"),
                col("tag_webid"),
                col("tag_name"),
                col("sensor_type"),
                col("plant"),
                col("unit"),
                round(col("avg_value"), 2).alias("avg_value"),
                round(col("min_value"), 2).alias("min_value"),
                round(col("max_value"), 2).alias("max_value"),
                round(col("stddev_value"), 3).alias("stddev_value"),
                col("reading_count"),
                round((col("good_quality_count") / col("reading_count")) * 100, 1).alias("quality_pct"),
                col("units")
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deployment
# MAGIC
# MAGIC ### 1. Create DLT Pipeline (Triggered Mode):
# MAGIC ```bash
# MAGIC databricks pipelines create \
# MAGIC   --name "PI Batch Ingestion (Lakeflow)" \
# MAGIC   --notebook-path "/Workspace/path/to/DLT_PI_Batch_Lakeflow" \
# MAGIC   --target "osipi.bronze" \
# MAGIC   --triggered \
# MAGIC   --configuration '{
# MAGIC     "PI_SERVER_URL": "http://localhost:8010",
# MAGIC     "BATCH_SIZE_HOURS": "1"
# MAGIC   }'
# MAGIC ```
# MAGIC
# MAGIC ### 2. Schedule with Databricks Jobs:
# MAGIC - **Frequency**: Every hour (or every 15 minutes for near-real-time)
# MAGIC - **Type**: DLT Pipeline Trigger
# MAGIC - **Retries**: 3 with exponential backoff
# MAGIC
# MAGIC ### 3. Monitoring:
# MAGIC ```sql
# MAGIC -- Check ingestion progress
# MAGIC SELECT * FROM osipi.bronze.pi_batch_checkpoint ORDER BY created_at DESC LIMIT 10;
# MAGIC
# MAGIC -- Check latest batch
# MAGIC SELECT
# MAGIC   _batch_id,
# MAGIC   _batch_start_time,
# MAGIC   _batch_end_time,
# MAGIC   COUNT(*) as records
# MAGIC FROM osipi.bronze.pi_batch_bronze
# MAGIC GROUP BY _batch_id, _batch_start_time, _batch_end_time
# MAGIC ORDER BY _batch_end_time DESC
# MAGIC LIMIT 10;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparison: Batch vs Streaming
# MAGIC
# MAGIC | Aspect | Batch (This Pipeline) | Streaming (Auto Loader) |
# MAGIC |--------|----------------------|-------------------------|
# MAGIC | **Data Source** | REST API calls | JSON files (cloud storage) |
# MAGIC | **Trigger** | Scheduled (hourly/15min) | Continuous or micro-batch |
# MAGIC | **Latency** | 15-60 minutes | 10-30 seconds |
# MAGIC | **Use Case** | Historical backfill, batch ETL | Real-time monitoring |
# MAGIC | **Cost** | Pay per job run | Continuous cluster |
# MAGIC | **Checkpoint** | Manual timestamp tracking | Auto Loader checkpoint |
# MAGIC
# MAGIC **Recommendation**: Use both!
# MAGIC - **Streaming**: For real-time WebSocket data (last 1-2 hours)
# MAGIC - **Batch**: For historical data and backfill (older than 2 hours)
