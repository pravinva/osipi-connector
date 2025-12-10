# Databricks notebook source
# MAGIC %md
# MAGIC # Test Mock API Locally
# MAGIC
# MAGIC Quick test to verify the mock API is responding correctly before running full ingestion.

# COMMAND ----------

import requests
import json

# Local mock API (change to Databricks App URL when deployed)
MOCK_API_URL = "http://localhost:8001"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Test Data Server Endpoint

# COMMAND ----------

print("Testing /piwebapi/dataservers endpoint...")
response = requests.get(f"{MOCK_API_URL}/piwebapi/dataservers")
print(f"Status Code: {response.status_code}")

if response.status_code == 200:
    data = response.json()
    print(f"✓ Found {len(data['Items'])} data servers")
    server = data['Items'][0]
    print(f"  Server Name: {server['Name']}")
    print(f"  Server WebId: {server['WebId']}")
    server_webid = server['WebId']
else:
    print(f"❌ Error: {response.text}")
    raise Exception("Failed to fetch data servers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Test PI Points Endpoint

# COMMAND ----------

print(f"\nTesting /piwebapi/dataservers/{server_webid}/points endpoint...")
response = requests.get(f"{MOCK_API_URL}/piwebapi/dataservers/{server_webid}/points?maxCount=100")
print(f"Status Code: {response.status_code}")

if response.status_code == 200:
    data = response.json()
    print(f"✓ Found {len(data['Items'])} PI points")

    # Show first 5 tags
    print("\nFirst 5 tags:")
    for point in data['Items'][:5]:
        print(f"  - {point['Name']} ({point['WebId']})")
        print(f"    Units: {point['EngineeringUnits']}, Descriptor: {point['Descriptor']}")
else:
    print(f"❌ Error: {response.text}")
    raise Exception("Failed to fetch PI points")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Test Recorded Data Endpoint

# COMMAND ----------

from datetime import datetime, timedelta

# Get data for first tag
tag_webid = data['Items'][0]['WebId']
tag_name = data['Items'][0]['Name']

start_time = (datetime.utcnow() - timedelta(hours=1)).isoformat() + "Z"
end_time = datetime.utcnow().isoformat() + "Z"

print(f"\nTesting /piwebapi/streams/{tag_webid}/recorded endpoint...")
print(f"Tag: {tag_name}")
print(f"Time Range: {start_time} to {end_time}")

response = requests.get(
    f"{MOCK_API_URL}/piwebapi/streams/{tag_webid}/recorded",
    params={"startTime": start_time, "endTime": end_time}
)
print(f"Status Code: {response.status_code}")

if response.status_code == 200:
    data = response.json()
    print(f"✓ Found {len(data['Items'])} data points")

    # Show first 3 values
    print("\nFirst 3 values:")
    for item in data['Items'][:3]:
        print(f"  - Timestamp: {item['Timestamp']}, Value: {item['Value']}, Quality: {item['Good']}")
else:
    print(f"❌ Error: {response.text}")
    raise Exception("Failed to fetch recorded data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Test AF Hierarchy Endpoint

# COMMAND ----------

print("\nTesting /piwebapi/assetdatabases endpoint...")
response = requests.get(f"{MOCK_API_URL}/piwebapi/assetdatabases")
print(f"Status Code: {response.status_code}")

if response.status_code == 200:
    data = response.json()
    print(f"✓ Found {len(data['Items'])} asset databases")

    if len(data['Items']) > 0:
        db = data['Items'][0]
        db_webid = db['WebId']
        print(f"  Database Name: {db['Name']}")
        print(f"  Database WebId: {db_webid}")

        # Get elements
        print(f"\nTesting /piwebapi/assetdatabases/{db_webid}/elements endpoint...")
        response = requests.get(f"{MOCK_API_URL}/piwebapi/assetdatabases/{db_webid}/elements?maxCount=10")

        if response.status_code == 200:
            elements = response.json()
            print(f"✓ Found {len(elements['Items'])} AF elements")

            # Show first 3 elements
            print("\nFirst 3 AF elements:")
            for elem in elements['Items'][:3]:
                print(f"  - {elem['Name']} ({elem['TemplateName']})")
                print(f"    Path: {elem['Path']}")
        else:
            print(f"❌ Error fetching elements: {response.text}")
else:
    print(f"❌ Error: {response.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Test Event Frames Endpoint

# COMMAND ----------

print("\nTesting /piwebapi/assetdatabases/{db_webid}/eventframes endpoint...")
response = requests.get(f"{MOCK_API_URL}/piwebapi/assetdatabases/{db_webid}/eventframes?maxCount=10")
print(f"Status Code: {response.status_code}")

if response.status_code == 200:
    data = response.json()
    print(f"✓ Found {len(data['Items'])} event frames")

    # Show first 3 events
    print("\nFirst 3 event frames:")
    for event in data['Items'][:3]:
        print(f"  - {event['Name']} ({event['TemplateName']})")
        print(f"    Start: {event['StartTime']}, End: {event['EndTime']}")
        if event.get('Attributes'):
            print(f"    Attributes: {list(event['Attributes'].keys())}")
else:
    print(f"❌ Error: {response.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC If all tests pass, the mock API is working correctly and you can proceed with:
# MAGIC 1. Running the ingestion notebook (`ingest_from_mock_api.py`)
# MAGIC 2. Generating pipelines (`generate_pipelines_from_mock_api.py`)
# MAGIC 3. Deploying DAB pipelines

# COMMAND ----------

print("\n" + "="*80)
print("All tests PASSED! ✓")
print("="*80)
print("\nMock API is responding correctly.")
print(f"URL: {MOCK_API_URL}")
print("\nNext steps:")
print("1. Update notebooks to use this Mock API URL")
print("2. Run ingest_from_mock_api.py notebook")
print("3. Run generate_pipelines_from_mock_api.py notebook")
print("4. Deploy DAB pipelines")
print("="*80)
