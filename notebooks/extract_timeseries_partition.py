# Databricks notebook source
# MAGIC %md
# MAGIC # Extract Time-Series Data for Partition
# MAGIC
# MAGIC This notebook extracts time-series data for a specific partition of tags.
# MAGIC Multiple instances run in parallel (one per partition).

# COMMAND ----------

from pyspark.sql import SparkSession
from datetime import datetime, timedelta
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
partition_id = int(dbutils.widgets.get("partition_id"))

print(f"Partition ID: {partition_id}")
print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"PI Web API: {pi_web_api_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Tags for This Partition

# COMMAND ----------

# Load tags assigned to this partition
partition_table = f"{catalog}.{schema}.tag_partitions"
df_partition_tags = spark.sql(f"""
    SELECT tag_webid, tag_name
    FROM {partition_table}
    WHERE partition_id = {partition_id}
""")

partition_tags = [row.tag_webid for row in df_partition_tags.collect()]

print(f"\n📊 Partition {partition_id} Summary:")
print(f"   Tags to process: {len(partition_tags)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Get Checkpoint Time

# COMMAND ----------

# Get last checkpoint for this partition
checkpoint_table = f"{catalog}.checkpoints.pi_watermarks"

try:
    last_checkpoint = spark.sql(f"""
        SELECT MAX(last_timestamp) as max_time
        FROM {checkpoint_table}
        WHERE partition_id = {partition_id}
    """).collect()[0].max_time
    
    if last_checkpoint:
        start_time = last_checkpoint
        print(f"✓ Resuming from checkpoint: {start_time}")
    else:
        start_time = datetime.now() - timedelta(hours=1)
        print(f"✓ No checkpoint found, starting from: {start_time}")
        
except:
    # First run
    start_time = datetime.now() - timedelta(hours=1)
    print(f"✓ First run, starting from: {start_time}")

end_time = datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Extract Time-Series Data

# COMMAND ----------

# Get authentication from secrets
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
    'schema': schema,
    'tags': partition_tags
}

# Initialize connector
connector = PILakeflowConnector(config)

print(f"\n🔄 Extracting data for partition {partition_id}...")
print(f"   Time range: {start_time} to {end_time}")
print(f"   Tags: {len(partition_tags)}")

# COMMAND ----------

# Extract time-series data
df_timeseries = connector.extract_timeseries(
    tag_webids=partition_tags,
    start_time=start_time,
    end_time=end_time,
    batch_size=100
)

print(f"\n✅ Extracted {df_timeseries.count()} data points")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write to Delta Lake

# COMMAND ----------

# Add partition metadata
from pyspark.sql.functions import lit

df_timeseries_with_partition = df_timeseries.withColumn("partition_id", lit(partition_id))

# Write to Delta table (append mode)
timeseries_table = f"{catalog}.{schema}.pi_timeseries"
df_timeseries_with_partition.write.format("delta").mode("append").saveAsTable(timeseries_table)

print(f"✅ Written to {timeseries_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Update Checkpoint

# COMMAND ----------

# Update checkpoint for this partition
checkpoint_data = [{
    'partition_id': partition_id,
    'last_timestamp': end_time,
    'tags_processed': len(partition_tags),
    'points_ingested': df_timeseries.count(),
    'updated_at': datetime.now()
}]

df_checkpoint = spark.createDataFrame(checkpoint_data)
df_checkpoint.write.format("delta").mode("append").saveAsTable(checkpoint_table)

print(f"✅ Checkpoint updated for partition {partition_id}")

# COMMAND ----------

# Summary
print(f"""
╔══════════════════════════════════════════════════════════════╗
║         Partition {partition_id} Extraction Complete                  ║
╚══════════════════════════════════════════════════════════════╝

Tags Processed: {len(partition_tags)}
Data Points Ingested: {df_timeseries.count()}
Time Range: {start_time} to {end_time}

✅ Partition {partition_id} complete!
""")

