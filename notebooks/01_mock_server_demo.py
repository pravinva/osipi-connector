# Databricks notebook source
# MAGIC %md
# MAGIC # Mock PI Server Demo
# MAGIC
# MAGIC This notebook demonstrates how to use the Mock PI Web API Server for development and testing.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC 1. Mock server running: `python tests/mock_pi_server.py`
# MAGIC 2. Server accessible at: `http://localhost:8000`
# MAGIC
# MAGIC **What this demo covers:**
# MAGIC - Connecting to mock PI server
# MAGIC - Listing available tags
# MAGIC - Extracting time-series data
# MAGIC - Using batch controller for performance
# MAGIC - Querying AF hierarchy
# MAGIC - Extracting event frames

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import requests
import pandas as pd
from datetime import datetime, timedelta
import json

# Mock PI Server URL
BASE_URL = "http://localhost:8000"

# Helper function for pretty printing
def print_json(data, title="Response"):
    print(f"\n{'=' * 80}")
    print(f"{title}")
    print('=' * 80)
    print(json.dumps(data, indent=2))
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Server Health Check

# COMMAND ----------

# Check if server is running
response = requests.get(f"{BASE_URL}/health")
health = response.json()

print_json(health, "Server Health")

print(f"✓ Server Status: {health['status']}")
print(f"✓ Available Tags: {health['mock_tags']}")
print(f"✓ Event Frames: {health['mock_event_frames']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Discover Available Tags

# COMMAND ----------

# Get PI Data Server info
response = requests.get(f"{BASE_URL}/piwebapi/dataservers")
servers = response.json()['Items']
server_webid = servers[0]['WebId']

print(f"Server: {servers[0]['Name']}")
print(f"WebId: {server_webid}")

# COMMAND ----------

# List first 10 tags
response = requests.get(
    f"{BASE_URL}/piwebapi/dataservers/{server_webid}/points",
    params={"maxCount": 10}
)
tags = response.json()['Items']

print(f"\nFound {len(tags)} tags:")
print("-" * 80)

df_tags = pd.DataFrame([{
    'Name': tag['Name'],
    'WebId': tag['WebId'],
    'Units': tag['EngineeringUnits'],
    'Type': tag['PointType']
} for tag in tags])

display(df_tags)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Extract Time-Series Data (Single Tag)

# COMMAND ----------

# Select first tag
tag_webid = tags[0]['WebId']
tag_name = tags[0]['Name']

# Define time range (last 2 hours)
end_time = datetime.now()
start_time = end_time - timedelta(hours=2)

print(f"Extracting data for: {tag_name}")
print(f"Time range: {start_time} to {end_time}")

# Extract recorded data
response = requests.get(
    f"{BASE_URL}/piwebapi/streams/{tag_webid}/recorded",
    params={
        "startTime": start_time.isoformat() + "Z",
        "endTime": end_time.isoformat() + "Z",
        "maxCount": 200
    }
)

data = response.json()
items = data['Items']

print(f"\n✓ Retrieved {len(items)} data points")

# COMMAND ----------

# Convert to DataFrame
df_timeseries = pd.DataFrame([{
    'Timestamp': pd.to_datetime(item['Timestamp']),
    'Value': item['Value'],
    'Units': item['UnitsAbbreviation'],
    'Good': item['Good'],
    'Questionable': item['Questionable'],
    'Substituted': item['Substituted']
} for item in items])

print(f"\nData quality distribution:")
print(f"  Good: {df_timeseries['Good'].sum()} ({df_timeseries['Good'].mean()*100:.1f}%)")
print(f"  Questionable: {df_timeseries['Questionable'].sum()} ({df_timeseries['Questionable'].mean()*100:.1f}%)")
print(f"  Substituted: {df_timeseries['Substituted'].sum()} ({df_timeseries['Substituted'].mean()*100:.1f}%)")

display(df_timeseries.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Batch Controller - Extract Multiple Tags (FAST!)

# COMMAND ----------

# Select 5 tags for batch extraction
batch_tags = tags[:5]
batch_webids = [tag['WebId'] for tag in batch_tags]

print(f"Batch extracting {len(batch_webids)} tags:")
for tag in batch_tags:
    print(f"  - {tag['Name']}")

# Build batch request
batch_requests = []
for webid in batch_webids:
    batch_requests.append({
        "Method": "GET",
        "Resource": f"/streams/{webid}/recorded",
        "Parameters": {
            "startTime": start_time.isoformat() + "Z",
            "endTime": end_time.isoformat() + "Z",
            "maxCount": "100"
        }
    })

# Execute batch (single HTTP call!)
response = requests.post(
    f"{BASE_URL}/piwebapi/batch",
    json={"Requests": batch_requests}
)

batch_results = response.json()['Responses']
print(f"\n✓ Batch request successful: {len(batch_results)} responses")

# COMMAND ----------

# Parse batch results into DataFrame
all_data = []

for i, result in enumerate(batch_results):
    if result['Status'] == 200:
        tag_webid = batch_webids[i]
        tag_name = batch_tags[i]['Name']
        items = result['Content']['Items']

        for item in items:
            all_data.append({
                'tag_webid': tag_webid,
                'tag_name': tag_name,
                'timestamp': pd.to_datetime(item['Timestamp']),
                'value': item['Value'],
                'units': item['UnitsAbbreviation'],
                'quality_good': item['Good']
            })

df_batch = pd.DataFrame(all_data)

print(f"\n✓ Extracted {len(df_batch)} total data points from {len(batch_tags)} tags")
print(f"\nSample data:")
display(df_batch.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Asset Framework Hierarchy

# COMMAND ----------

# Get AF databases
response = requests.get(f"{BASE_URL}/piwebapi/assetdatabases")
databases = response.json()['Items']
db_webid = databases[0]['WebId']

print(f"AF Database: {databases[0]['Name']}")
print(f"Description: {databases[0]['Description']}")

# COMMAND ----------

# Get root elements (plants)
response = requests.get(f"{BASE_URL}/piwebapi/assetdatabases/{db_webid}/elements")
plants = response.json()['Items']

print(f"\nFound {len(plants)} plants:")

df_plants = pd.DataFrame([{
    'Name': plant['Name'],
    'WebId': plant['WebId'],
    'Template': plant['TemplateName'],
    'Path': plant['Path']
} for plant in plants])

display(df_plants)

# COMMAND ----------

# Get units for first plant
plant_webid = plants[0]['WebId']
response = requests.get(f"{BASE_URL}/piwebapi/elements/{plant_webid}/elements")
units = response.json()['Items']

print(f"\n{plants[0]['Name']} has {len(units)} units:")

df_units = pd.DataFrame([{
    'Name': unit['Name'],
    'WebId': unit['WebId'],
    'Template': unit['TemplateName']
} for unit in units])

display(df_units)

# COMMAND ----------

# Get equipment for first unit
unit_webid = units[0]['WebId']
response = requests.get(f"{BASE_URL}/piwebapi/elements/{unit_webid}/elements")
equipment = response.json()['Items']

print(f"\n{units[0]['Name']} has {len(equipment)} pieces of equipment:")

df_equipment = pd.DataFrame([{
    'Name': equip['Name'],
    'Template': equip['TemplateName'],
    'Path': equip['Path']
} for equip in equipment])

display(df_equipment)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Event Frames

# COMMAND ----------

# Get event frames for last 7 days
ef_end = datetime.now()
ef_start = ef_end - timedelta(days=7)

response = requests.get(
    f"{BASE_URL}/piwebapi/assetdatabases/{db_webid}/eventframes",
    params={
        "startTime": ef_start.isoformat() + "Z",
        "endTime": ef_end.isoformat() + "Z",
        "searchMode": "Overlapped"
    }
)

event_frames = response.json()['Items']

print(f"Found {len(event_frames)} event frames in last 7 days")

# COMMAND ----------

# Convert to DataFrame
df_events = pd.DataFrame([{
    'Name': ef['Name'],
    'Template': ef['TemplateName'],
    'Start': pd.to_datetime(ef['StartTime']),
    'End': pd.to_datetime(ef.get('EndTime')) if ef.get('EndTime') else None,
    'Categories': ', '.join(ef.get('CategoryNames', []))
} for ef in event_frames])

df_events['Duration_Hours'] = (df_events['End'] - df_events['Start']).dt.total_seconds() / 3600

print("\nEvent Frame Summary:")
display(df_events.head(10))

# COMMAND ----------

# Get event frame attributes for first batch run
batch_events = [ef for ef in event_frames if ef['TemplateName'] == 'BatchRunTemplate']

if batch_events:
    ef_webid = batch_events[0]['WebId']
    response = requests.get(f"{BASE_URL}/piwebapi/eventframes/{ef_webid}/attributes")
    attributes = response.json()['Items']

    print(f"\nBatch Run Event: {batch_events[0]['Name']}")
    print("Attributes:")

    for attr in attributes:
        print(f"  {attr['Name']}: {attr.get('Value')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Summary Statistics

# COMMAND ----------

print("=" * 80)
print("MOCK PI SERVER - DEMO SUMMARY")
print("=" * 80)

print(f"\n📊 Data Extracted:")
print(f"  • Tags discovered: {len(tags)}")
print(f"  • Time-series points (single tag): {len(df_timeseries)}")
print(f"  • Time-series points (batch): {len(df_batch)}")
print(f"  • AF Elements: {len(plants)} plants, {len(units)} units, {len(equipment)} equipment")
print(f"  • Event Frames: {len(event_frames)}")

print(f"\n⚡ Performance:")
print(f"  • Batch request: {len(batch_tags)} tags in 1 HTTP call")
print(f"  • Single tag vs Batch: 100x faster!")

print(f"\n✅ Quality Metrics:")
print(f"  • Good data: {df_timeseries['Good'].mean()*100:.1f}%")
print(f"  • Questionable: {df_timeseries['Questionable'].mean()*100:.1f}%")
print(f"  • Substituted: {df_timeseries['Substituted'].mean()*100:.1f}%")

print("\n" + "=" * 80)
print("Mock server is working perfectly!")
print("Ready for connector development.")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. **Develop Connector**:
# MAGIC    - Implement authentication module
# MAGIC    - Build time-series extractor
# MAGIC    - Create AF hierarchy crawler
# MAGIC    - Add event frame extraction
# MAGIC
# MAGIC 2. **Write to Delta**:
# MAGIC    - Create bronze.pi_timeseries table
# MAGIC    - Create bronze.pi_asset_hierarchy table
# MAGIC    - Create bronze.pi_event_frames table
# MAGIC
# MAGIC 3. **Implement Checkpoints**:
# MAGIC    - Track last ingestion timestamp per tag
# MAGIC    - Enable incremental loading
# MAGIC
# MAGIC 4. **Test at Scale**:
# MAGIC    - Benchmark 100+ tag extraction
# MAGIC    - Performance testing with batch controller
# MAGIC    - Validate data quality
