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

# Ensure URL ends with /piwebapi
if not pi_server_url.endswith('/piwebapi'):
    pi_server_url = f"{pi_server_url}/piwebapi"

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
# Define DLT tables for PI Web API data

@dlt.table(
    name="pi_timeseries",
    comment="PI Web API time-series data with overlapping window deduplication",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true",
        "pipelines.merge.keys": "tag_webid,timestamp"
    },
    partition_cols=["partition_date"]
)
def pi_timeseries():
    """
    Bronze table for raw PI time-series data.

    Fetches last 7 days on each run (overlapping windows).
    Uses MERGE on write to deduplicate based on (tag_webid, timestamp).
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

# COMMAND ----------
# AF Hierarchy Table

@dlt.table(
    name="pi_af_hierarchy",
    comment="PI Asset Framework hierarchy from PI Web API",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
def pi_af_hierarchy():
    """
    Bronze table for AF (Asset Framework) hierarchy.

    Full refresh on each run - AF structure doesn't change frequently.
    """
    import requests
    from pyspark.sql.functions import col, current_timestamp
    from datetime import datetime

    # Get asset databases
    assetdatabases_url = f"{pi_server_url}/assetdatabases"

    if connection_name == 'mock_pi_connection' or 'databricksapps.com' in pi_server_url:
        response = requests.get(assetdatabases_url, headers=auth_headers)
    else:
        username = dbutils.secrets.get(scope=connection_name, key='username')
        password = dbutils.secrets.get(scope=connection_name, key='password')
        response = requests.get(assetdatabases_url, auth=(username, password))

    databases = response.json().get("Items", [])

    af_hierarchy_data = []

    for db in databases:
        db_webid = db["WebId"]

        # Get root elements
        elements_url = f"{pi_server_url}/assetdatabases/{db_webid}/elements"

        if connection_name == 'mock_pi_connection' or 'databricksapps.com' in pi_server_url:
            response = requests.get(elements_url, headers=auth_headers)
        else:
            response = requests.get(elements_url, auth=(username, password))

        elements = response.json().get("Items", [])

        # Recursively extract hierarchy
        def extract_elements(elements_list, parent_webid=None):
            for elem in elements_list:
                # Extract metadata from name
                parts = elem["Name"].split("_")
                plant = parts[0] if len(parts) > 0 else "Unknown"

                af_hierarchy_data.append({
                    "webid": elem["WebId"],
                    "name": elem["Name"],
                    "template_name": elem.get("TemplateName", ""),
                    "description": elem.get("Description", ""),
                    "path": elem.get("Path", ""),
                    "parent_webid": parent_webid if parent_webid else "",
                    "plant": plant,
                    "unit": 0,
                    "equipment_type": elem.get("TemplateName", "").replace("Template", ""),
                    "ingestion_timestamp": datetime.now()
                })

                # Recursively process child elements
                if "Elements" in elem and elem["Elements"]:
                    extract_elements(elem["Elements"], elem["WebId"])

        extract_elements(elements)

    # Create DataFrame
    if af_hierarchy_data:
        df = spark.createDataFrame(af_hierarchy_data)
        df = df.withColumn("ingestion_timestamp", current_timestamp())
        return df
    else:
        # Return empty DataFrame with schema
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
        schema = StructType([
            StructField("webid", StringType(), True),
            StructField("name", StringType(), True),
            StructField("template_name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("path", StringType(), True),
            StructField("parent_webid", StringType(), True),
            StructField("plant", StringType(), True),
            StructField("unit", IntegerType(), True),
            StructField("equipment_type", StringType(), True),
            StructField("ingestion_timestamp", TimestampType(), True)
        ])
        return spark.createDataFrame([], schema)

# COMMAND ----------
# Event Frames Table

@dlt.table(
    name="pi_event_frames",
    comment="PI Event Frames (batch runs, alarms, maintenance) from PI Web API",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true",
        "pipelines.merge.keys": "webid,start_time"
    },
    partition_cols=["partition_date"]
)
def pi_event_frames():
    """
    Bronze table for Event Frames (alarms, events, batch runs).

    Fetches last 30 days on each run (overlapping windows).
    Uses MERGE on write to deduplicate based on (webid, start_time).
    """
    import requests
    from pyspark.sql.functions import col, current_timestamp
    from datetime import datetime, timedelta

    # Fetch last 30 days
    end_time = datetime.now()
    start_time = end_time - timedelta(days=30)

    start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Get asset databases
    assetdatabases_url = f"{pi_server_url}/assetdatabases"

    if connection_name == 'mock_pi_connection' or 'databricksapps.com' in pi_server_url:
        response = requests.get(assetdatabases_url, headers=auth_headers)
    else:
        username = dbutils.secrets.get(scope=connection_name, key='username')
        password = dbutils.secrets.get(scope=connection_name, key='password')
        response = requests.get(assetdatabases_url, auth=(username, password))

    databases = response.json().get("Items", [])

    event_frames_data = []

    for db in databases:
        db_webid = db["WebId"]

        # Get event frames
        ef_url = f"{pi_server_url}/assetdatabases/{db_webid}/eventframes"
        params = {"startTime": start_time_str, "endTime": end_time_str}

        if connection_name == 'mock_pi_connection' or 'databricksapps.com' in pi_server_url:
            response = requests.get(ef_url, params=params, headers=auth_headers)
        else:
            response = requests.get(ef_url, params=params, auth=(username, password))

        if response.status_code == 200:
            event_frames = response.json().get("Items", [])

            for ef in event_frames:
                # Convert attributes to dict
                raw_attrs = ef.get("Attributes", {})
                attrs_map = {k: str(v) if v is not None else "" for k, v in raw_attrs.items()} if raw_attrs else {}

                # Category names
                category_names = ef.get("CategoryNames", []) if ef.get("CategoryNames") else []

                event_frames_data.append({
                    "webid": ef["WebId"],
                    "name": ef["Name"],
                    "template_name": ef.get("TemplateName", ""),
                    "start_time": ef["StartTime"],
                    "end_time": ef.get("EndTime"),
                    "primary_referenced_element_webid": ef.get("PrimaryReferencedElementWebId"),
                    "description": ef.get("Description", ""),
                    "category_names": category_names,
                    "attributes": attrs_map,
                    "ingestion_timestamp": datetime.now()
                })

    # Create DataFrame
    if event_frames_data:
        df = spark.createDataFrame(event_frames_data)

        # Convert timestamps
        df = df.withColumn("start_time", col("start_time").cast("timestamp"))
        df = df.withColumn("end_time", col("end_time").cast("timestamp"))
        df = df.withColumn("ingestion_timestamp", current_timestamp())
        df = df.withColumn("partition_date", col("start_time").cast("date"))

        return df
    else:
        # Return empty DataFrame with schema
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType, MapType, DateType
        schema = StructType([
            StructField("webid", StringType(), True),
            StructField("name", StringType(), True),
            StructField("template_name", StringType(), True),
            StructField("start_time", TimestampType(), True),
            StructField("end_time", TimestampType(), True),
            StructField("primary_referenced_element_webid", StringType(), True),
            StructField("description", StringType(), True),
            StructField("category_names", ArrayType(StringType()), True),
            StructField("attributes", MapType(StringType(), StringType()), True),
            StructField("ingestion_timestamp", TimestampType(), True),
            StructField("partition_date", DateType(), True)
        ])
        return spark.createDataFrame([], schema)
