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

# Get auth credentials from Databricks secrets
# Assumes secrets are stored in scope matching connection name
auth_type = spark.conf.get('pi.auth.type', 'basic')
username = dbutils.secrets.get(scope=connection_name, key='username')
password = dbutils.secrets.get(scope=connection_name, key='password')

# COMMAND ----------
# Configure PI connector

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
    'end_time': datetime.now()
}

# COMMAND ----------
# Define DLT table for PI time-series data

@dlt.table(
    name="pi_timeseries",
    comment="PI Web API time-series data ingested via Lakeflow connector",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=["partition_date"]
)
def pi_timeseries_bronze():
    """
    Bronze table for raw PI time-series data.

    Runs incrementally using checkpoints to track last ingestion timestamp per tag.
    """
    # Initialize connector
    connector = PILakeflowConnector(config)

    # Run extraction (returns DataFrame)
    df = connector.extract_timeseries_to_df()

    # Add partition column
    df = df.withColumn("partition_date", col("timestamp").cast("date"))
    df = df.withColumn("ingestion_timestamp", current_timestamp())

    return df

# COMMAND ----------
# Define DLT table for checkpoints

@dlt.table(
    name="pi_watermarks",
    comment="Checkpoint watermarks for incremental ingestion"
)
def pi_watermarks():
    """
    Checkpoint table tracking last successful ingestion timestamp per tag.
    """
    # This table is managed by the checkpoint manager in the connector
    # DLT will create and maintain it based on connector writes
    pass
