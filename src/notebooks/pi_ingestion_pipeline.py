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
# Define DLT table for PI time-series data using simple overlapping windows

@dlt.view(name="pi_timeseries_raw")
def pi_timeseries_raw():
    """
    Raw extraction view - fetches last 7 days of data on each run.
    Overlapping windows ensure no data is missed between runs.
    """
    from datetime import datetime, timedelta

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

# Bronze table with MERGE to handle duplicates
dlt.create_streaming_table(
    name="pi_timeseries",
    comment="PI Web API time-series data with automatic deduplication",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    },
    partition_cols=["partition_date"]
)

dlt.apply_changes(
    target="pi_timeseries",
    source="pi_timeseries_raw",
    keys=["tag_webid", "timestamp"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=1
)
