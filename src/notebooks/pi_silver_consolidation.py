# Databricks notebook source
"""
PI Silver Consolidation Pipeline

This notebook consolidates data from multiple bronze_raw pipeline groups into
clean, deduplicated silver tables.

Architecture:
- Reads from all bronze_raw tables (pi_timeseries_raw_group1, group2, etc.)
- Merges and deduplicates based on business keys
- Writes to final silver tables (pi_timeseries, pi_af_hierarchy, pi_event_frames)
- Single pipeline ownership - no conflicts!
"""

import dlt
from pyspark.sql.functions import col, current_timestamp, row_number
from pyspark.sql.window import Window

# COMMAND ----------
# Get configuration from DLT pipeline parameters

target_catalog = spark.conf.get('pi.target.catalog', 'osipi')
bronze_raw_schema = spark.conf.get('pi.bronze_raw.schema', 'bronze_raw')
silver_schema = spark.conf.get('pi.silver.schema', 'bronze')
num_pipeline_groups = int(spark.conf.get('pi.num_pipeline_groups', '5'))

# COMMAND ----------
# Silver Table: pi_timeseries (Consolidated & Deduplicated)

@dlt.table(
    name="pi_timeseries",
    comment="Consolidated and deduplicated PI time-series data from all pipeline groups",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    },
    partition_cols=["partition_date"]
)
def pi_timeseries():
    """
    Silver table: Consolidates all bronze_raw_group* tables.

    Deduplication logic:
    - Primary key: (tag_webid, timestamp)
    - If duplicates exist across groups, keep the most recent ingestion
    """
    from pyspark.sql.functions import col, row_number
    from pyspark.sql.window import Window

    # Union all bronze raw tables
    dfs = []
    for group_id in range(1, num_pipeline_groups + 1):
        try:
            df = dlt.read(f"pi_timeseries_raw_group{group_id}")
            dfs.append(df)
        except Exception as e:
            # Group table doesn't exist yet - skip
            print(f"Skipping group {group_id}: {e}")
            continue

    if not dfs:
        # Return empty dataframe with correct schema
        return spark.createDataFrame([], schema="tag_webid STRING, timestamp TIMESTAMP, value DOUBLE, units STRING, quality_good BOOLEAN, quality_questionable BOOLEAN, partition_date DATE, ingestion_timestamp TIMESTAMP, pipeline_group STRING")

    # Union all groups
    union_df = dfs[0]
    for df in dfs[1:]:
        union_df = union_df.union(df)

    # Deduplicate: Keep most recent ingestion per (tag_webid, timestamp)
    window_spec = Window.partitionBy("tag_webid", "timestamp").orderBy(col("ingestion_timestamp").desc())

    dedup_df = (union_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num", "pipeline_group")  # Remove internal columns
    )

    return dedup_df

# COMMAND ----------
# Silver Table: pi_af_hierarchy (Consolidated & Deduplicated)

@dlt.table(
    name="pi_af_hierarchy",
    comment="Consolidated AF hierarchy from all pipeline groups",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
def pi_af_hierarchy():
    """
    Silver table: Consolidates all AF hierarchy data.

    Deduplication logic:
    - Primary key: element_id
    - Keep most recent ingestion if duplicates exist
    """
    from pyspark.sql.functions import col, row_number
    from pyspark.sql.window import Window

    # Union all bronze raw tables
    dfs = []
    for group_id in range(1, num_pipeline_groups + 1):
        try:
            df = dlt.read(f"pi_af_hierarchy_raw_group{group_id}")
            dfs.append(df)
        except Exception as e:
            print(f"Skipping group {group_id}: {e}")
            continue

    if not dfs:
        # Return empty dataframe with correct schema
        return spark.createDataFrame([], schema="element_id STRING, element_name STRING, element_path STRING, parent_id STRING, template_name STRING, element_type STRING, description STRING, categories ARRAY<STRING>, depth INT, database_webid STRING, database_name STRING, ingestion_timestamp TIMESTAMP, pipeline_group STRING")

    # Union all groups
    union_df = dfs[0]
    for df in dfs[1:]:
        union_df = union_df.union(df)

    # Deduplicate: Keep most recent ingestion per element_id
    window_spec = Window.partitionBy("element_id").orderBy(col("ingestion_timestamp").desc())

    dedup_df = (union_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num", "pipeline_group")
    )

    return dedup_df

# COMMAND ----------
# Silver Table: pi_event_frames (Consolidated & Deduplicated)

@dlt.table(
    name="pi_event_frames",
    comment="Consolidated event frames from all pipeline groups",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    },
    partition_cols=["partition_date"]
)
def pi_event_frames():
    """
    Silver table: Consolidates all event frame data.

    Deduplication logic:
    - Primary key: (event_frame_id, start_time)
    - Keep most recent ingestion if duplicates exist
    """
    from pyspark.sql.functions import col, row_number
    from pyspark.sql.window import Window

    # Union all bronze raw tables
    dfs = []
    for group_id in range(1, num_pipeline_groups + 1):
        try:
            df = dlt.read(f"pi_event_frames_raw_group{group_id}")
            dfs.append(df)
        except Exception as e:
            print(f"Skipping group {group_id}: {e}")
            continue

    if not dfs:
        # Return empty dataframe with correct schema
        return spark.createDataFrame([], schema="event_frame_id STRING, event_name STRING, template_name STRING, start_time TIMESTAMP, end_time TIMESTAMP, primary_element_id STRING, category ARRAY<STRING>, description STRING, duration_minutes DOUBLE, event_attributes MAP<STRING,STRING>, ingestion_timestamp TIMESTAMP, partition_date DATE, pipeline_group STRING")

    # Union all groups
    union_df = dfs[0]
    for df in dfs[1:]:
        union_df = union_df.union(df)

    # Deduplicate: Keep most recent ingestion per (event_frame_id, start_time)
    window_spec = Window.partitionBy("event_frame_id", "start_time").orderBy(col("ingestion_timestamp").desc())

    dedup_df = (union_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num", "pipeline_group")
    )

    return dedup_df
