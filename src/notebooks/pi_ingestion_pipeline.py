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

# Detect authentication type based on connection name
# For mock_pi_connection (Databricks App), use OAuth M2M
# For real PI servers, use basic auth from connection-specific scope
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
# Define DLT streaming tables for PI Web API data
# Using append mode to allow multiple pipelines to write to the same tables

@dlt.view(
    name="pi_timeseries_raw"
)
def pi_timeseries_raw():
    """
    Raw streaming view for PI time-series data.
    Multiple pipelines can read from this view and append to the bronze table.
    """
    from datetime import datetime, timedelta
    from pyspark.sql.functions import col, current_timestamp

    # Always fetch last 7 days (overlapping for safety)
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

    return df

@dlt.table(
    name="pi_timeseries",
    comment="PI Web API time-series data - multiple pipelines append concurrently",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true"
    },
    partition_cols=["partition_date"]
)
def pi_timeseries():
    """
    Bronze table for raw PI time-series data.

    Uses streaming append mode to allow multiple pipelines to write concurrently.
    Deduplication happens via merge keys at read time in silver layer.
    """
    return (
        dlt.read_stream("pi_timeseries_raw")
        .dropDuplicates(["tag_webid", "timestamp"])  # Dedupe within each batch
    )

# COMMAND ----------
# AF Hierarchy Table

@dlt.view(
    name="pi_af_hierarchy_raw"
)
def pi_af_hierarchy_raw():
    """
    Raw view for AF (Asset Framework) hierarchy.
    Multiple pipelines can read from this and append to bronze table.
    """
    from pyspark.sql.functions import current_timestamp

    # Use connector to extract AF hierarchy
    connector = PILakeflowConnector(config)
    df = connector.extract_af_hierarchy_to_df()

    # Add ingestion timestamp
    df = df.withColumn("ingestion_timestamp", current_timestamp())

    return df

@dlt.table(
    name="pi_af_hierarchy",
    comment="PI Asset Framework hierarchy - multiple pipelines append concurrently",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true"
    }
)
def pi_af_hierarchy():
    """
    Bronze table for AF (Asset Framework) hierarchy.

    Uses streaming append mode to allow multiple pipelines to write concurrently.
    Each pipeline contributes hierarchy for its plant's tags.
    """
    return (
        dlt.read_stream("pi_af_hierarchy_raw")
        .dropDuplicates(["element_webid"])  # Dedupe within each batch
    )

# COMMAND ----------
# Event Frames Table

@dlt.view(
    name="pi_event_frames_raw"
)
def pi_event_frames_raw():
    """
    Raw view for Event Frames (alarms, events, batch runs).
    Multiple pipelines can read from this and append to bronze table.
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

    # Add ingestion timestamp and partition date
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    df = df.withColumn("partition_date", col("start_time").cast("date"))

    return df

@dlt.table(
    name="pi_event_frames",
    comment="PI Event Frames - multiple pipelines append concurrently",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true"
    },
    partition_cols=["partition_date"]
)
def pi_event_frames():
    """
    Bronze table for Event Frames (alarms, events, batch runs).

    Uses streaming append mode to allow multiple pipelines to write concurrently.
    Each pipeline contributes events for its plant's tags.
    """
    return (
        dlt.read_stream("pi_event_frames_raw")
        .dropDuplicates(["webid", "start_time"])  # Dedupe within each batch
    )
