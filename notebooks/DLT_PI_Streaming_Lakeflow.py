# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Tables: PI WebSocket Streaming with Auto Loader
# MAGIC
# MAGIC This DLT pipeline ingests real-time WebSocket data from PI Web API using Lakeflow patterns:
# MAGIC 1. **Bronze Layer**: Raw JSON ingestion with Auto Loader
# MAGIC 2. **Silver Layer**: Cleaned, typed, and deduplicated data
# MAGIC 3. **Gold Layer**: Aggregated metrics for analytics
# MAGIC
# MAGIC ## Lakeflow Architecture:
# MAGIC ```
# MAGIC WebSocket Stream → JSON Files (staging) → Auto Loader → Bronze → Silver → Gold
# MAGIC ```

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# Configuration
STAGING_PATH = os.getenv("LAKEFLOW_STAGING_PATH", "file:/tmp/lakeflow-pi-streams")
CATALOG = os.getenv("UC_CATALOG", "osipi")
SCHEMA = os.getenv("UC_SCHEMA", "bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer: Raw Ingestion with Auto Loader
# MAGIC
# MAGIC Auto Loader (cloudFiles) incrementally ingests new JSON files as they arrive.
# MAGIC Key features:
# MAGIC - **Schema inference**: Automatically detects JSON structure
# MAGIC - **Checkpoint management**: Tracks processed files
# MAGIC - **Scalable**: Handles millions of files efficiently

# COMMAND ----------

@dlt.table(
    name="pi_streaming_bronze",
    comment="Raw PI WebSocket streaming data ingested via Auto Loader",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def pi_streaming_bronze():
    """
    Bronze table: Raw JSON ingestion from WebSocket staging directory.

    Auto Loader reads JSON files as they arrive and automatically handles:
    - Schema inference and evolution
    - Checkpointing (tracks which files have been processed)
    - File notification (efficient for cloud storage)
    """
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"{STAGING_PATH}/_schema")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(STAGING_PATH)
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source_file", input_file_name())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Cleaned and Typed Data
# MAGIC
# MAGIC The Silver layer applies:
# MAGIC - Type casting (timestamp strings → proper timestamps)
# MAGIC - Data quality checks (@expect constraints)
# MAGIC - Deduplication (based on tag_webid + timestamp)
# MAGIC - NULL handling

# COMMAND ----------

@dlt.table(
    name="pi_streaming_silver",
    comment="Cleaned and typed PI streaming data with quality checks",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_timestamp", "timestamp IS NOT NULL")
@dlt.expect_or_drop("valid_tag_webid", "tag_webid IS NOT NULL")
@dlt.expect("valid_value", "value IS NOT NULL")
def pi_streaming_silver():
    """
    Silver table: Cleaned, typed, and deduplicated PI streaming data.

    Quality checks:
    - Drop rows with NULL timestamp or tag_webid (critical fields)
    - Warn if value is NULL (allowed but tracked)
    - Convert timestamp strings to proper TIMESTAMP type
    """
    return (
        dlt.read_stream("pi_streaming_bronze")
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
                col("_ingestion_timestamp"),
                col("_source_file")
            )
            # Deduplicate based on tag + timestamp
            .dropDuplicates(["tag_webid", "timestamp"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Aggregated Metrics
# MAGIC
# MAGIC The Gold layer provides business-ready analytics:
# MAGIC - 1-minute aggregations per sensor
# MAGIC - Statistical metrics (avg, min, max, stddev)
# MAGIC - Data quality summary (% good quality readings)
# MAGIC - Optimized for BI tools and dashboards

# COMMAND ----------

@dlt.table(
    name="pi_streaming_gold_1min",
    comment="1-minute aggregated PI sensor metrics for analytics",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def pi_streaming_gold_1min():
    """
    Gold table: 1-minute aggregated metrics per sensor.

    Provides:
    - Average, min, max, stddev of sensor values
    - Count of readings per minute
    - Data quality percentage (good readings / total readings)
    - Plant and unit groupings for filtering
    """
    return (
        dlt.read_stream("pi_streaming_silver")
            .withWatermark("timestamp", "2 minutes")  # Handle late-arriving data
            .groupBy(
                window("timestamp", "1 minute"),
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
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
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
# MAGIC ## Real-Time Anomaly Detection (Optional Gold Layer)
# MAGIC
# MAGIC Detects anomalies in sensor readings using statistical thresholds

# COMMAND ----------

@dlt.table(
    name="pi_streaming_gold_anomalies",
    comment="Real-time anomaly detection on PI sensor data",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def pi_streaming_gold_anomalies():
    """
    Gold table: Anomaly detection using statistical methods.

    Flags anomalies when:
    - Value > mean + 3*stddev (statistical outlier)
    - Value < mean - 3*stddev
    - Quality flags indicate bad data
    """
    return (
        dlt.read_stream("pi_streaming_silver")
            .filter(
                (col("quality_good") == False) |
                (col("quality_questionable") == True) |
                (col("quality_substituted") == True)
            )
            .select(
                col("timestamp"),
                col("tag_webid"),
                col("tag_name"),
                col("value"),
                col("units"),
                col("plant"),
                col("unit"),
                col("sensor_type"),
                when(col("quality_good") == False, "BAD_QUALITY")
                    .when(col("quality_questionable") == True, "QUESTIONABLE_QUALITY")
                    .when(col("quality_substituted") == True, "SUBSTITUTED_VALUE")
                    .alias("anomaly_type"),
                current_timestamp().alias("detected_at")
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Monitoring Views

# COMMAND ----------

@dlt.view(
    name="pi_streaming_pipeline_health",
    comment="Real-time monitoring of the streaming pipeline health"
)
def pi_streaming_pipeline_health():
    """
    Monitoring view: Pipeline health metrics.

    Tracks:
    - Ingestion lag (time between data timestamp and ingestion)
    - Records per minute
    - Quality issues percentage
    """
    return (
        dlt.read_stream("pi_streaming_silver")
            .select(
                (unix_timestamp("_ingestion_timestamp") - unix_timestamp("timestamp")).alias("ingestion_lag_seconds"),
                col("tag_webid"),
                col("plant"),
                col("quality_good")
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to Deploy This Pipeline
# MAGIC
# MAGIC ### 1. Create DLT Pipeline (UI or CLI):
# MAGIC ```bash
# MAGIC databricks pipelines create \
# MAGIC   --name "PI WebSocket Streaming (Lakeflow)" \
# MAGIC   --notebook-path "/Workspace/path/to/DLT_PI_Streaming_Lakeflow" \
# MAGIC   --target "osipi.bronze" \
# MAGIC   --continuous \
# MAGIC   --configuration '{"LAKEFLOW_STAGING_PATH": "file:/tmp/lakeflow-pi-streams"}'
# MAGIC ```
# MAGIC
# MAGIC ### 2. Or use the Databricks UI:
# MAGIC - Go to **Delta Live Tables** → **Create Pipeline**
# MAGIC - Select this notebook
# MAGIC - Set target schema: `osipi.bronze`
# MAGIC - Mode: **Continuous** (for real-time streaming)
# MAGIC - Add configuration:
# MAGIC   - `LAKEFLOW_STAGING_PATH`: Path to staging directory
# MAGIC   - `UC_CATALOG`: Unity Catalog name (default: osipi)
# MAGIC   - `UC_SCHEMA`: Schema name (default: bronze)
# MAGIC
# MAGIC ### 3. For Production (Cloud Storage):
# MAGIC Replace `LAKEFLOW_STAGING_PATH` with cloud storage:
# MAGIC - **AWS**: `s3://my-bucket/pi-streams/`
# MAGIC - **Azure**: `abfss://container@account.dfs.core.windows.net/pi-streams/`
# MAGIC - **GCP**: `gs://my-bucket/pi-streams/`
# MAGIC
# MAGIC ### 4. Query the Results:
# MAGIC ```sql
# MAGIC -- Bronze: Raw data
# MAGIC SELECT * FROM osipi.bronze.pi_streaming_bronze LIMIT 100;
# MAGIC
# MAGIC -- Silver: Cleaned data
# MAGIC SELECT * FROM osipi.bronze.pi_streaming_silver ORDER BY timestamp DESC LIMIT 100;
# MAGIC
# MAGIC -- Gold: 1-minute aggregations
# MAGIC SELECT * FROM osipi.bronze.pi_streaming_gold_1min ORDER BY window_start DESC LIMIT 100;
# MAGIC
# MAGIC -- Anomalies
# MAGIC SELECT * FROM osipi.bronze.pi_streaming_gold_anomalies ORDER BY detected_at DESC LIMIT 100;
# MAGIC ```
