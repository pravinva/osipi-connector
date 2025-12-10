# Databricks notebook source
# MAGIC %md
# MAGIC # Orchestrator: Discover and Partition Tags
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Discovers all tags from PI Web API
# MAGIC 2. Partitions them into N groups for parallel processing
# MAGIC 3. Saves partitions to Delta table for downstream tasks

# COMMAND ----------

import requests
from pyspark.sql import SparkSession
from datetime import datetime
from typing import List, Dict
import math

# COMMAND ----------

# Get parameters
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
pi_web_api_url = dbutils.widgets.get("pi_web_api_url")
total_partitions = int(dbutils.widgets.get("total_partitions"))

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"PI Web API: {pi_web_api_url}")
print(f"Total Partitions: {total_partitions}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Discover All Tags from PI Web API

# COMMAND ----------

def discover_all_tags(pi_url: str) -> List[Dict]:
    """
    Discover all tags from PI Web API.
    
    Returns list of tag metadata dictionaries.
    """
    # Get data servers
    response = requests.get(f"{pi_url}/piwebapi/dataservers", timeout=30)
    response.raise_for_status()
    servers = response.json()["Items"]
    
    if not servers:
        raise Exception("No PI Data Servers found")
    
    server_webid = servers[0]["WebId"]
    print(f"✓ Found PI Data Server: {servers[0]['Name']}")
    
    # Get all points (tags) from first server
    response = requests.get(
        f"{pi_url}/piwebapi/dataservers/{server_webid}/points",
        params={"maxCount": 100000},  # Increase if you have more tags
        timeout=60
    )
    response.raise_for_status()
    
    tags = response.json()["Items"]
    print(f"✓ Discovered {len(tags)} tags")
    
    return tags

# COMMAND ----------

# Discover tags
all_tags = discover_all_tags(pi_web_api_url)

print(f"\n📊 Tag Discovery Summary:")
print(f"   Total tags: {len(all_tags)}")
print(f"   First tag: {all_tags[0]['Name'] if all_tags else 'None'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Partitions

# COMMAND ----------

def create_partitions(tags: List[Dict], num_partitions: int) -> List[List[Dict]]:
    """
    Partition tags into N roughly equal groups.
    """
    tags_per_partition = math.ceil(len(tags) / num_partitions)
    
    partitions = []
    for i in range(num_partitions):
        start_idx = i * tags_per_partition
        end_idx = min((i + 1) * tags_per_partition, len(tags))
        partition_tags = tags[start_idx:end_idx]
        partitions.append(partition_tags)
    
    return partitions

# COMMAND ----------

# Create partitions
partitions = create_partitions(all_tags, total_partitions)

print(f"\n📦 Partition Summary:")
for i, partition in enumerate(partitions):
    print(f"   Partition {i}: {len(partition)} tags")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Save Partitions to Delta Table

# COMMAND ----------

# Flatten partitions into rows
partition_rows = []
for partition_id, partition_tags in enumerate(partitions):
    for tag in partition_tags:
        partition_rows.append({
            "partition_id": partition_id,
            "tag_webid": tag["WebId"],
            "tag_name": tag["Name"],
            "tag_path": tag.get("Path", ""),
            "point_type": tag.get("PointType", ""),
            "engineering_units": tag.get("EngineeringUnits", ""),
            "created_at": datetime.utcnow()
        })

# Create DataFrame
df_partitions = spark.createDataFrame(partition_rows)

# Save to Delta table (overwrite for idempotency)
partition_table = f"{catalog}.{schema}.tag_partitions"
df_partitions.write.format("delta").mode("overwrite").saveAsTable(partition_table)

print(f"\n✅ Saved {len(partition_rows)} tag assignments to {partition_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# Verify partitions table
spark.sql(f"""
    SELECT 
        partition_id,
        COUNT(*) as tag_count,
        COUNT(DISTINCT tag_webid) as unique_tags
    FROM {catalog}.{schema}.tag_partitions
    GROUP BY partition_id
    ORDER BY partition_id
""").display()

# COMMAND ----------

# Summary
print(f"""
╔══════════════════════════════════════════════════════════════╗
║           Tag Discovery & Partitioning Complete              ║
╚══════════════════════════════════════════════════════════════╝

Total Tags Discovered: {len(all_tags)}
Number of Partitions: {total_partitions}
Average Tags per Partition: {len(all_tags) // total_partitions}

Partition Assignments Table: {partition_table}

✅ Ready for parallel extraction!
""")



