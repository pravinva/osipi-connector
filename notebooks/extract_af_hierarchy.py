# Databricks notebook source
# MAGIC %md
# MAGIC # Extract PI Asset Framework Hierarchy
# MAGIC
# MAGIC Extracts the complete AF hierarchy from PI Server.
# MAGIC Runs once per pipeline execution.

# COMMAND ----------

from datetime import datetime
import sys
import os

# Add src to path
sys.path.append("/Workspace" + os.path.dirname(os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())))

from src.connector.pi_lakeflow_connector import PILakeflowConnector

# COMMAND ----------

# Get parameters
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
pi_web_api_url = dbutils.widgets.get("pi_web_api_url")

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"PI Web API: {pi_web_api_url}")

# COMMAND ----------

# Get authentication
pi_username = dbutils.secrets.get(scope="pi-connector", key="username")
pi_password = dbutils.secrets.get(scope="pi-connector", key="password")

# Configure connector
config = {
    'pi_web_api_url': pi_web_api_url,
    'auth': {
        'type': 'basic',
        'username': pi_username,
        'password': pi_password
    },
    'catalog': catalog,
    'schema': schema
}

connector = PILakeflowConnector(config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract AF Database ID

# COMMAND ----------

import requests

# Get first AF database
response = requests.get(
    f"{pi_web_api_url}/piwebapi/assetservers",
    auth=(pi_username, pi_password),
    timeout=30
)
response.raise_for_status()

asset_servers = response.json()["Items"]
if not asset_servers:
    raise Exception("No PI Asset Servers found")

server_webid = asset_servers[0]["WebId"]

# Get databases
response = requests.get(
    f"{pi_web_api_url}/piwebapi/assetservers/{server_webid}/assetdatabases",
    auth=(pi_username, pi_password),
    timeout=30
)
response.raise_for_status()

databases = response.json()["Items"]
if not databases:
    raise Exception("No AF Databases found")

database_webid = databases[0]["WebId"]
print(f"✓ AF Database: {databases[0]['Name']}")
print(f"✓ Database WebID: {database_webid}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract Hierarchy

# COMMAND ----------

print("🔄 Extracting AF hierarchy...")

df_hierarchy = connector.extract_af_hierarchy(database_webid)

element_count = df_hierarchy.count()
print(f"✅ Extracted {element_count} AF elements")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Lake

# COMMAND ----------

# Write AF hierarchy (overwrite - full refresh)
hierarchy_table = f"{catalog}.{schema}.pi_asset_hierarchy"
df_hierarchy.write.format("delta").mode("overwrite").saveAsTable(hierarchy_table)

print(f"✅ Written to {hierarchy_table}")

# COMMAND ----------

# Display sample
print("\n📊 AF Hierarchy Sample:")
df_hierarchy.limit(10).display()

# COMMAND ----------

# Summary
print(f"""
╔══════════════════════════════════════════════════════════════╗
║           AF Hierarchy Extraction Complete                   ║
╚══════════════════════════════════════════════════════════════╝

AF Elements: {element_count}
Table: {hierarchy_table}

✅ Hierarchy extraction complete!
""")



