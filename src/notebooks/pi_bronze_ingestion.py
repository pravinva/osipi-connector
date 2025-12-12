# Databricks notebook source
"""
PI Bronze Ingestion Pipeline (Append-Only)

This notebook ingests PI data for a specific pipeline group and appends to bronze_raw tables.
Multiple pipelines can run in parallel without conflicts.

Architecture:
- Each pipeline writes to its own bronze_raw table (e.g., pi_timeseries_raw_group1)
- Append-only mode (no ownership conflicts)
- Silver pipeline later consolidates all groups
"""

import dlt
from pyspark.sql.functions import col, current_timestamp, lit
from datetime import datetime, timedelta
import os
import sys

# Add src to path for imports
sys.path.append(os.path.abspath('../../'))

from src.connector.pi_lakeflow_connector import PILakeflowConnector

# COMMAND ----------
# Get configuration from DLT pipeline parameters

pipeline_group = spark.conf.get('pi.pipeline.group')  # NEW: Group identifier
tags = spark.conf.get('pi.tags').split(',')
pi_server_url = spark.conf.get('pi.server.url')
connection_name = spark.conf.get('pi.connection.name')
target_catalog = spark.conf.get('pi.target.catalog')
target_schema = spark.conf.get('pi.target.schema', 'bronze_raw')  # Use bronze_raw schema
start_time_offset_days = int(spark.conf.get('pi.start_time_offset_days', '30'))

# Detect authentication type based on connection name
auth_type = spark.conf.get('pi.auth.type', 'basic')

if connection_name == 'mock_pi_connection' or 'databricksapps.com' in pi_server_url:
    # Use OAuth M2M for Databricks App (mock API)
    from databricks.sdk import WorkspaceClient
    import requests

    # Get service principal credentials from sp-osipi scope
    CLIENT_ID = dbutils.secrets.get(scope="sp-osipi", key="sp-client-id")
    CLIENT_SECRET = dbutils.secrets.get(scope="sp-osipi", key="sp-client-secret")

    # Get workspace URL for token endpoint
    workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()

    # Initialize WorkspaceClient with service principal
    wc = WorkspaceClient(
        host=workspace_url,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )

    # Get OAuth headers
    auth_headers = wc.config.authenticate()

    config = {
        'pi_web_api_url': pi_server_url,
        'auth': {
            'type': 'oauth',
            'headers': auth_headers
        },
        'catalog': target_catalog,
        'schema': target_schema,
        'tags': tags,
        'start_time': datetime.now() - timedelta(days=start_time_offset_days),
        'end_time': datetime.now(),
        'dlt_mode': True
    }
else:
    # Use basic auth for real PI servers
    username = dbutils.secrets.get(scope=connection_name, key='username')
    password = dbutils.secrets.get(scope=connection_name, key='password')

    config = {
        'pi_web_api_url': pi_server_url,
        'auth': {
            'type': auth_type,
            'username': username,
            'password': password
        },
        'catalog': target_catalog,
        'schema': target_schema,
        'tags': tags,
        'start_time': datetime.now() - timedelta(days=start_time_offset_days),
        'end_time': datetime.now(),
        'dlt_mode': True
    }

# COMMAND ----------
# Define Bronze Raw Tables (Append-Only, Per Group)

@dlt.table(
    name=f"pi_timeseries_raw_group{pipeline_group}",
    comment=f"Raw PI time-series data for pipeline group {pipeline_group} (append-only)",
    table_properties={
        "quality": "bronze_raw",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    },
    partition_cols=["partition_date"]
)
def pi_timeseries_raw():
    """
    Bronze raw table for time-series data (append-only).
    No deduplication - just append new data.
    Silver pipeline will handle deduplication.
    """
    from datetime import datetime, timedelta
    from pyspark.sql.functions import col, current_timestamp, lit

    # Fetch last 7 days (overlapping for safety)
    start_time = datetime.now() - timedelta(days=7)
    end_time = datetime.now()

    # Update config
    extraction_config = config.copy()
    extraction_config['start_time'] = start_time
    extraction_config['end_time'] = end_time

    # Extract data
    connector = PILakeflowConnector(extraction_config)
    df = connector.extract_timeseries_to_df()

    # Add metadata
    df = df.withColumn("partition_date", col("timestamp").cast("date"))
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    df = df.withColumn("pipeline_group", lit(pipeline_group))  # Track source pipeline

    return df

# COMMAND ----------
# AF Hierarchy (Append-Only)

@dlt.table(
    name=f"pi_af_hierarchy_raw_group{pipeline_group}",
    comment=f"Raw AF hierarchy for pipeline group {pipeline_group} (append-only)",
    table_properties={
        "quality": "bronze_raw",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
def pi_af_hierarchy_raw():
    """
    Bronze raw table for AF hierarchy (append-only).
    Full refresh on each run - AF structure doesn't change frequently.
    """
    from pyspark.sql.functions import current_timestamp, lit

    # Use connector to extract AF hierarchy
    connector = PILakeflowConnector(config)
    df = connector.extract_af_hierarchy_to_df()

    # Add metadata
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    df = df.withColumn("pipeline_group", lit(pipeline_group))

    return df

# COMMAND ----------
# Event Frames (Append-Only)

@dlt.table(
    name=f"pi_event_frames_raw_group{pipeline_group}",
    comment=f"Raw event frames for pipeline group {pipeline_group} (append-only)",
    table_properties={
        "quality": "bronze_raw",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    },
    partition_cols=["partition_date"]
)
def pi_event_frames_raw():
    """
    Bronze raw table for event frames (append-only).
    No deduplication - silver pipeline handles it.
    """
    from pyspark.sql.functions import col, current_timestamp, lit
    from datetime import datetime, timedelta

    # Set time range (last 30 days)
    event_config = config.copy()
    event_config['start_time'] = datetime.now() - timedelta(days=30)
    event_config['end_time'] = datetime.now()

    # Extract event frames
    connector = PILakeflowConnector(event_config)
    df = connector.extract_event_frames_to_df()

    # Add metadata
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    df = df.withColumn("partition_date", col("start_time").cast("date"))
    df = df.withColumn("pipeline_group", lit(pipeline_group))

    return df
