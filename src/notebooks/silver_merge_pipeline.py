# Databricks notebook source
"""
Silver Layer Merge DLT Pipeline

This notebook merges data from multiple bronze pipeline tables into unified silver tables.
Each DLT pipeline writes to its own bronze table (e.g., pi_timeseries_pipeline1, pi_timeseries_pipeline2).
This silver pipeline merges and deduplicates data from all pipeline-specific tables.

Architecture:
- Bronze: Multiple pipelines write to separate tables (no ownership conflicts)
- Silver: Single pipeline reads from all bronze tables and merges (deduplication, data quality)
"""

import dlt
from pyspark.sql.functions import col, current_timestamp, row_number, max as spark_max
from pyspark.sql.window import Window

# COMMAND ----------
# Configuration

# Get list of bronze table patterns to merge
catalog = spark.conf.get('pi.target.catalog', 'osipi')
bronze_schema = spark.conf.get('pi.bronze.schema', 'bronze')
silver_schema = spark.conf.get('pi.silver.schema', 'silver')

# COMMAND ----------
# Helper function to get all bronze tables matching a pattern

def get_bronze_tables(table_pattern: str) -> list:
    """
    Get all bronze tables matching the pattern (e.g., pi_timeseries_pipeline*)
    Returns list of full table names
    """
    try:
        tables = spark.sql(f"SHOW TABLES IN {catalog}.{bronze_schema}").collect()
        matching_tables = [
            f"{catalog}.{bronze_schema}.{row.tableName}"
            for row in tables
            if row.tableName.startswith(table_pattern)
        ]
        return matching_tables
    except Exception as e:
        print(f"Warning: Could not list bronze tables: {e}")
        return []

# COMMAND ----------
# Silver Table 1: pi_timeseries (merged and deduplicated)

@dlt.table(
    name="pi_timeseries",
    comment="Silver: Merged and deduplicated PI time-series data from all pipelines",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    },
    partition_cols=["partition_date"]
)
def pi_timeseries_silver():
    """
    Merge all bronze pi_timeseries_pipeline* tables into unified silver table.
    Deduplicates by (tag_webid, timestamp), keeping most recent ingestion.
    """

    # Get all bronze timeseries tables
    bronze_tables = get_bronze_tables("pi_timeseries_pipeline")

    if not bronze_tables:
        # Return empty DataFrame with correct schema if no bronze tables exist yet
        return spark.createDataFrame([], schema="""
            tag_webid STRING,
            timestamp TIMESTAMP,
            value DOUBLE,
            quality_good BOOLEAN,
            quality_questionable BOOLEAN,
            units STRING,
            partition_date DATE,
            ingestion_timestamp TIMESTAMP,
            source_pipeline STRING
        """)

    # Union all bronze tables
    all_data = None
    for table_name in bronze_tables:
        df = spark.read.table(table_name).withColumn("source_pipeline", col(table_name))
        if all_data is None:
            all_data = df
        else:
            all_data = all_data.union(df)

    # Deduplicate: Keep most recent ingestion for each (tag_webid, timestamp)
    window_spec = Window.partitionBy("tag_webid", "timestamp").orderBy(col("ingestion_timestamp").desc())

    return (all_data
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
        .withColumn("silver_timestamp", current_timestamp())
    )

# COMMAND ----------
# Silver Table 2: pi_af_hierarchy (merged and deduplicated)

@dlt.table(
    name="pi_af_hierarchy",
    comment="Silver: Merged and deduplicated AF hierarchy from all pipelines",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    }
)
def pi_af_hierarchy_silver():
    """
    Merge all bronze pi_af_hierarchy_pipeline* tables into unified silver table.
    Deduplicates by element_id, keeping most recent ingestion.
    """

    # Get all bronze AF hierarchy tables
    bronze_tables = get_bronze_tables("pi_af_hierarchy_pipeline")

    if not bronze_tables:
        # Return empty DataFrame with correct schema
        return spark.createDataFrame([], schema="""
            element_id STRING,
            element_name STRING,
            element_path STRING,
            parent_id STRING,
            template_name STRING,
            element_type STRING,
            description STRING,
            categories STRING,
            depth INT,
            database_webid STRING,
            database_name STRING,
            ingestion_timestamp TIMESTAMP,
            source_pipeline STRING
        """)

    # Union all bronze tables
    all_data = None
    for table_name in bronze_tables:
        df = spark.read.table(table_name).withColumn("source_pipeline", col(table_name))
        if all_data is None:
            all_data = df
        else:
            all_data = all_data.union(df)

    # Deduplicate: Keep most recent ingestion for each element_id
    window_spec = Window.partitionBy("element_id").orderBy(col("ingestion_timestamp").desc())

    return (all_data
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
        .withColumn("silver_timestamp", current_timestamp())
    )

# COMMAND ----------
# Silver Table 3: pi_event_frames (merged and deduplicated)

@dlt.table(
    name="pi_event_frames",
    comment="Silver: Merged and deduplicated event frames from all pipelines",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    },
    partition_cols=["partition_date"]
)
def pi_event_frames_silver():
    """
    Merge all bronze pi_event_frames_pipeline* tables into unified silver table.
    Deduplicates by (event_frame_id, start_time), keeping most recent ingestion.
    """

    # Get all bronze event frames tables
    bronze_tables = get_bronze_tables("pi_event_frames_pipeline")

    if not bronze_tables:
        # Return empty DataFrame with correct schema
        return spark.createDataFrame([], schema="""
            event_frame_id STRING,
            event_name STRING,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            template_name STRING,
            categories STRING,
            severity STRING,
            acknowledged BOOLEAN,
            ack_time TIMESTAMP,
            ack_by STRING,
            partition_date DATE,
            ingestion_timestamp TIMESTAMP,
            source_pipeline STRING
        """)

    # Union all bronze tables
    all_data = None
    for table_name in bronze_tables:
        df = spark.read.table(table_name).withColumn("source_pipeline", col(table_name))
        if all_data is None:
            all_data = df
        else:
            all_data = all_data.union(df)

    # Deduplicate: Keep most recent ingestion for each (event_frame_id, start_time)
    window_spec = Window.partitionBy("event_frame_id", "start_time").orderBy(col("ingestion_timestamp").desc())

    return (all_data
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
        .withColumn("silver_timestamp", current_timestamp())
    )

# COMMAND ----------
# Data Quality View: Check for duplicates and late arrivals

@dlt.view(
    name="data_quality_metrics"
)
def data_quality_metrics():
    """
    Calculate data quality metrics across all bronze tables
    - Duplicate detection
    - Late data detection (data older than 7 days arriving now)
    - Pipeline contribution analysis
    """
    from pyspark.sql.functions import count, countDistinct, sum as spark_sum, datediff, lit
    from datetime import datetime, timedelta

    # Get all bronze timeseries tables
    bronze_tables = get_bronze_tables("pi_timeseries_pipeline")

    if not bronze_tables:
        return spark.createDataFrame([], schema="""
            metric_name STRING,
            metric_value DOUBLE,
            source_pipeline STRING,
            calculated_at TIMESTAMP
        """)

    metrics = []
    for table_name in bronze_tables:
        df = spark.read.table(table_name)

        # Calculate metrics
        total_records = df.count()
        distinct_records = df.select("tag_webid", "timestamp").distinct().count()
        duplicates = total_records - distinct_records

        # Late data: timestamp > 7 days old but ingestion_timestamp is recent
        late_data = df.filter(
            datediff(col("ingestion_timestamp"), col("timestamp")) > 7
        ).count()

        metrics.extend([
            (table_name, "total_records", float(total_records)),
            (table_name, "distinct_records", float(distinct_records)),
            (table_name, "duplicates", float(duplicates)),
            (table_name, "late_arrivals", float(late_data))
        ])

    return spark.createDataFrame(
        [(m[0], m[1], m[2], datetime.now()) for m in metrics],
        schema="""
            source_pipeline STRING,
            metric_name STRING,
            metric_value DOUBLE,
            calculated_at TIMESTAMP
        """
    )
