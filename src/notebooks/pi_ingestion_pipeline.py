# Databricks notebook source
"""
PI Web API Ingestion DLT Pipeline

This notebook is executed by DLT pipelines to ingest PI data.
Configuration is passed via pipeline configuration parameters.
"""

import dlt
from pyspark.sql.functions import col, current_timestamp
from datetime import datetime, timedelta
import os
import sys

# Add src to path for imports
sys.path.append(os.path.abspath('../../'))

from src.connector.pi_lakeflow_connector import PILakeflowConnector

# COMMAND ----------
# Get configuration from DLT pipeline parameters

tags = spark.conf.get('pi.tags').split(',')
pi_server_url = spark.conf.get('pi.server.url')
connection_name = spark.conf.get('pi.connection.name')
target_catalog = spark.conf.get('pi.target.catalog')
target_schema = spark.conf.get('pi.target.schema')
start_time_offset_days = int(spark.conf.get('pi.start_time_offset_days', '30'))

# NEW: Get pipeline identifier for multi-pipeline architecture
# Each pipeline writes to its own tables (e.g., pi_timeseries_pipeline1, pi_timeseries_pipeline2)
# Silver layer merges and deduplicates data from all pipeline-specific bronze tables
pipeline_id = spark.conf.get('pi.pipeline.id', '1')

# Detect authentication type based on connection name
# For mock_pi_connection (Databricks App), use OAuth M2M
# For real PI servers, use basic auth from connection-specific scope
auth_type = spark.conf.get('pi.auth.type', 'basic')

if connection_name == 'mock_pi_connection' or 'databricksapps.com' in pi_server_url:
    # Databricks Apps authenticate using the App's OAuth client integration (see redirect client_id).
    # When the pipeline runs as a user, the most reliable option is to use the runtime WorkspaceClient()
    # (which uses the run-as identity) rather than a workspace PAT or a generic M2M token.
    from databricks.sdk import WorkspaceClient

    wc = WorkspaceClient()
    auth_headers = wc.config.authenticate()

    # Preflight: quickly verify the App accepts the token (avoids failing deep inside extraction)
    try:
        import requests
        auth_val = auth_headers.get('Authorization', '')
        token = auth_val[7:] if auth_val.startswith('Bearer ') else ''
        token_kind = 'jwt' if token.startswith('ey') or token.count('.') == 2 else ('pat' if token.startswith('dapi') else 'opaque')
        print(f"[auth] App token kind: {token_kind}")

        r = requests.post(
            f"{pi_server_url.rstrip('/')}/piwebapi/batch",
            headers=auth_headers,
            json={"Requests": []},
            timeout=30,
            allow_redirects=False,
        )
        print(f"[auth] POST /piwebapi/batch -> {r.status_code}")
        if r.is_redirect:
            print(f"[auth] redirect Location: {r.headers.get('Location','')}")
        if r.status_code == 401:
            raise RuntimeError('Databricks App rejected auth token with 401')
    except Exception as e:
        raise

    config = {
        'pi_web_api_url': pi_server_url,
        'auth': {
            'type': 'oauth',
            'headers': auth_headers  # Pass headers directly
        },
        'catalog': target_catalog,
        'schema': target_schema,
        'tags': tags,
        'start_time': datetime.now() - timedelta(days=start_time_offset_days),
        'end_time': datetime.now(),
        'dlt_mode': True  # Skip table creation in DLT
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
        'dlt_mode': True  # Skip table creation in DLT
    }

# COMMAND ----------
# Note: PILakeflowConnector needs to be updated to handle OAuth headers
# if auth type is 'oauth', use the provided headers instead of basic auth

# COMMAND ----------
# Define DLT tables for PI Web API data
# Multiple pipelines can append to the same tables concurrently

@dlt.table(
    name=f"pi_timeseries_pipeline{pipeline_id}",
    comment=f"PI Web API time-series data from pipeline {pipeline_id} - per-pipeline bronze table",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true"
    },
    partition_cols=["partition_date"]
)
def pi_timeseries():
    """
    Bronze table for raw PI time-series data.
    Multiple pipelines append concurrently without ownership conflicts.
    Deduplication happens via MERGE at read time in silver layer.
    """
    from datetime import datetime, timedelta
    from pyspark.sql.functions import col, current_timestamp

    # Always fetch last 7 days (overlapping window for safety)
    start_time = datetime.now() - timedelta(days=7)
    end_time = datetime.now()

    # Update config
    extraction_config = config.copy()
    extraction_config['start_time'] = start_time
    extraction_config['end_time'] = end_time

    # Extract data
    connector = PILakeflowConnector(extraction_config)
    df = connector.extract_timeseries_to_df()

    # Add metadata and return
    return (df
        .withColumn("partition_date", col("timestamp").cast("date"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .dropDuplicates(["tag_webid", "timestamp"])  # Dedupe within batch
    )

# COMMAND ----------
# AF Hierarchy Table

@dlt.table(
    name=f"pi_af_hierarchy_pipeline{pipeline_id}",
    comment=f"PI Asset Framework hierarchy from pipeline {pipeline_id} - per-pipeline bronze table",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true"
    }
)
def pi_af_hierarchy():
    """
    Bronze table for AF (Asset Framework) hierarchy.
    Multiple pipelines append concurrently without ownership conflicts.
    Each pipeline contributes hierarchy for its plant's tags.
    """
    from pyspark.sql.functions import current_timestamp

    # Use connector to extract AF hierarchy
    connector = PILakeflowConnector(config)
    df = connector.extract_af_hierarchy_to_df()

    # Add ingestion timestamp and return
    return (df
        .withColumn("ingestion_timestamp", current_timestamp())
        .dropDuplicates(["element_id"])  # Dedupe within batch
    )

# COMMAND ----------
# Event Frames Table

@dlt.table(
    name=f"pi_event_frames_pipeline{pipeline_id}",
    comment=f"PI Event Frames from pipeline {pipeline_id} - per-pipeline bronze table",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true"
    },
    partition_cols=["partition_date"]
)
def pi_event_frames():
    """
    Bronze table for Event Frames (alarms, events, batch runs).
    Multiple pipelines append concurrently without ownership conflicts.
    Each pipeline contributes events for its plant's tags.
    """
    from pyspark.sql.functions import col, current_timestamp
    from datetime import datetime, timedelta

    # Set time range for connector (last 30 days)
    event_config = config.copy()
    event_config['start_time'] = datetime.now() - timedelta(days=30)
    event_config['end_time'] = datetime.now()

    # Use connector to extract event frames
    connector = PILakeflowConnector(event_config)
    df = connector.extract_event_frames_to_df()

    # Add metadata and return
    return (df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("partition_date", col("start_time").cast("date"))
        .dropDuplicates(["event_frame_id", "start_time"])  # Dedupe within batch
    )
