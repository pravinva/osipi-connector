# Testing Guide - Mock Server, Dashboard, WebSocket & AF Hierarchy

**Quick Reference Guide for Testing All Features**

---

## 🎯 Quick Start - Run Everything

### Step 1: Start Mock Server with 10,000 Tags

```bash
cd /Users/pravin.varma/Documents/Demo/osipi-connector

# Stop any existing server
lsof -i :8010 | grep LISTEN | awk '{print $2}' | xargs kill

# Start with 10,000 tags
MOCK_PI_TAG_COUNT=10000 python3 app/main.py
```

**Expected Output:**
```
Generated 10000 mock PI tags (target: 10000)
Configuration: 25 plants × 50 units × 8 sensor types
Generated 50 mock event frames
INFO:     Started server process [12345]
INFO:     Uvicorn running on http://0.0.0.0:8010
```

**What's Running:**
- ✅ Mock PI Web API: `http://localhost:8010/piwebapi`
- ✅ Dashboard: `http://localhost:8010/ingestion`
- ✅ WebSocket endpoint: `ws://localhost:8010/piwebapi/streams/channel`

---

## 📊 Dashboard Testing

### Available Dashboards

#### 1. **Ingestion Dashboard**
```
http://localhost:8010/ingestion
```

**Shows:**
- Total rows ingested
- Tags monitored
- Data quality score
- Time-series charts
- Tag distribution by sensor type
- Pipeline health

#### 2. **PI Web API Home**
```
http://localhost:8010/
```

**Shows:**
- AVEVA-style landing page
- API version
- Available endpoints

#### 3. **API Endpoints (JSON)**
```bash
# Status
curl http://localhost:8010/api/ingestion/status | python3 -m json.tool

# Timeseries metrics
curl http://localhost:8010/api/ingestion/timeseries | python3 -m json.tool

# Tag distribution
curl http://localhost:8010/api/ingestion/tags | python3 -m json.tool

# Pipeline health
curl http://localhost:8010/api/ingestion/pipeline_health | python3 -m json.tool
```

---

## 🌳 AF Hierarchy Visualization

### Method 1: Python Visualizer (Graphviz)

```bash
# Install graphviz if needed
brew install graphviz  # macOS
# or: sudo apt-get install graphviz  # Linux

# Run AF hierarchy visualizer
python3 visualizations/af_hierarchy_visualizer.py
```

**Output:**
- Generates `af_hierarchy_tree.png`
- Shows plant → unit → equipment hierarchy
- Color-coded by level

**Expected Structure:**
```
ProductionDB
├── Eraring_Plant
│   ├── Unit_1
│   │   ├── Pump_101
│   │   ├── Compressor_101
│   │   ├── HeatExchanger_101
│   │   └── Reactor_101
│   ├── Unit_2
│   └── ...
├── Bayswater_Plant
└── ...
```

### Method 2: Query AF Hierarchy via API

```bash
# Get AF databases
curl http://localhost:8010/piwebapi/assetdatabases | python3 -m json.tool

# Get elements in database
curl "http://localhost:8010/piwebapi/assetdatabases/F1DP-DB-Production/elements" | python3 -m json.tool

# Get specific plant
curl "http://localhost:8010/piwebapi/elements/F1DP-Site-Eraring/elements" | python3 -m json.tool
```

### Method 3: View in Notebook

```python
# In Databricks notebook or Jupyter
import requests
import json

# Get AF hierarchy
response = requests.get('http://localhost:8010/piwebapi/assetdatabases/F1DP-DB-Production/elements')
hierarchy = response.json()

# Pretty print
print(json.dumps(hierarchy, indent=2))

# Or load into DataFrame
import pandas as pd
elements = hierarchy['Items']
df = pd.DataFrame(elements)
display(df[['Name', 'Path', 'TemplateName']])
```

---

## 🔌 WebSocket Real-Time Testing

### Method 1: Python WebSocket Client (Recommended)

Create `test_websocket.py`:

```python
import asyncio
import websockets
import json
from datetime import datetime

async def test_websocket():
    uri = "ws://localhost:8010/piwebapi/streams/channel"

    async with websockets.connect(uri) as websocket:
        print("✅ Connected to WebSocket")

        # Subscribe to a tag
        subscribe_msg = {
            'Action': 'Subscribe',
            'Resource': 'streams/F1DP-Eraring-U001-Temp-000001/value',
            'Parameters': {
                'updateRate': 1000  # 1 second updates
            }
        }

        await websocket.send(json.dumps(subscribe_msg))
        print(f"📡 Subscribed to tag")

        # Listen for updates
        print("👂 Listening for real-time updates... (Ctrl+C to stop)")
        try:
            async for message in websocket:
                data = json.loads(message)
                if 'Items' in data and data['Items']:
                    value = data['Items'][0]
                    timestamp = value.get('Timestamp', 'N/A')
                    val = value.get('Value', 'N/A')
                    uom = value.get('UnitsAbbreviation', '')
                    print(f"🔔 {datetime.now().strftime('%H:%M:%S')} - Value: {val} {uom} at {timestamp}")
        except KeyboardInterrupt:
            print("\n👋 Disconnected")

# Run
asyncio.run(test_websocket())
```

Run it:
```bash
python3 test_websocket.py
```

**Expected Output:**
```
✅ Connected to WebSocket
📡 Subscribed to tag
👂 Listening for real-time updates... (Ctrl+C to stop)
🔔 10:30:01 - Value: 42.5 degC at 2024-12-07T10:30:01Z
🔔 10:30:02 - Value: 42.7 degC at 2024-12-07T10:30:02Z
🔔 10:30:03 - Value: 42.3 degC at 2024-12-07T10:30:03Z
```

### Method 2: WebSocket + Delta Lake Integration

Use the full streaming connector:

```python
# test_streaming_to_delta.py
import asyncio
from src.connectors.pi_streaming_connector import PIStreamingConnector

config = {
    'pi_web_api_url': 'http://localhost:8010/piwebapi',
    'auth': {
        'type': 'basic',
        'username': 'test',
        'password': 'test'
    },
    'catalog': 'main',
    'schema': 'bronze',
    'table_name': 'pi_streaming_timeseries',
    'tags': [
        'F1DP-Eraring-U001-Temp-000001',
        'F1DP-Eraring-U001-Pres-000002',
        'F1DP-Eraring-U001-Flow-000003'
    ],
    'flush_interval_seconds': 10,  # Flush every 10 seconds for testing
    'max_buffer_size': 100,
    'warehouse_id': '4b9b953939869799'
}

async def main():
    connector = PIStreamingConnector(config)
    await connector.run()

asyncio.run(main())
```

Run it:
```bash
python3 test_streaming_to_delta.py
```

**Expected Output:**
```
======================================================================
🚀 Starting PI WebSocket Streaming Connector
======================================================================
✅ Spark session initialized (Warehouse: 4b9b953939869799)
✅ Streaming connector initialized
   Target table: main.bronze.pi_streaming_timeseries
   Tags to monitor: 3
   Flush interval: 10s
   Buffer size: 100 records
======================================================================
📡 Connecting to PI Web API WebSocket...
✅ WebSocket connection established
📋 Subscribing to 3 tags...
✅ Tag subscriptions active

🎯 Streaming to: main.bronze.pi_streaming_timeseries
⚙️  Databricks Warehouse: 4b9b953939869799

📊 Monitoring real-time PI data... (Press Ctrl+C to stop)
----------------------------------------------------------------------
✅ Flushed 30 records to Delta Lake (buffer: 0 records)
✅ Flushed 31 records to Delta Lake (buffer: 0 records)
```

### Method 3: Browser WebSocket (wscat)

```bash
# Install wscat
npm install -g wscat

# Connect
wscat -c ws://localhost:8010/piwebapi/streams/channel

# Send subscription
> {"Action":"Subscribe","Resource":"streams/F1DP-Eraring-U001-Temp-000001/value","Parameters":{"updateRate":1000}}

# Watch real-time updates
< {"Resource":"streams/F1DP-Eraring-U001-Temp-000001/value","Items":[{"Value":42.5,...}]}
```

---

## 🧪 Verify Mock Server Scale

### Check Tag Count

```bash
# Get all data servers
curl http://localhost:8010/piwebapi/dataservers | python3 -m json.tool

# Get points (should show 10,000 tags)
curl "http://localhost:8010/piwebapi/dataservers/F1DP-Archive/points?maxCount=10" | python3 -m json.tool
```

### Query Sample Tags

```python
import requests

# Get first 10 tags
response = requests.get('http://localhost:8010/piwebapi/dataservers/F1DP-Archive/points?maxCount=10')
tags = response.json()

print(f"Total tags available: {len(tags.get('Items', []))}")
for tag in tags.get('Items', [])[:10]:
    print(f"  - {tag['Name']}: {tag['Descriptor']}")
```

---

## 📈 Performance Testing

### Batch Performance Test (100 tags)

```python
import requests
import time

# Build batch request for 100 tags
batch_requests = []
for i in range(1, 101):
    batch_requests.append({
        'Method': 'GET',
        'Resource': f'https://localhost:8010/piwebapi/streams/F1DP-Eraring-U001-Temp-{i:06d}/recorded?startTime=*-1d&endTime=*'
    })

batch_payload = {'Requests': batch_requests}

# Time the batch request
start = time.time()
response = requests.post('http://localhost:8010/piwebapi/batch', json=batch_payload)
duration = time.time() - start

print(f"✅ Batch request: 100 tags in {duration:.2f} seconds")
print(f"   Avg: {duration*10:.0f}ms per tag")
```

**Expected:** <5 seconds for 100 tags

---

## 🎯 Complete Test Checklist

### ✅ Mock Server (10K Tags)

- [ ] Server starts with `MOCK_PI_TAG_COUNT=10000`
- [ ] Shows "Generated 10000 mock PI tags"
- [ ] Running on `http://localhost:8010`

### ✅ Dashboard

- [ ] Ingestion dashboard loads: `http://localhost:8010/ingestion`
- [ ] Shows real data from Unity Catalog
- [ ] Charts render correctly
- [ ] No "Mock Data" warning (unless table empty)

### ✅ AF Hierarchy

- [ ] API returns hierarchy: `/piwebapi/assetdatabases/F1DP-DB-Production/elements`
- [ ] Visualizer generates PNG: `python3 visualizations/af_hierarchy_visualizer.py`
- [ ] Shows 10+ plants with units and equipment

### ✅ WebSocket Streaming

- [ ] Can connect: `ws://localhost:8010/piwebapi/streams/channel`
- [ ] Subscription works
- [ ] Receives real-time updates (1 per second)
- [ ] Data has realistic patterns (daily cycles, noise)

### ✅ Delta Lake Integration

- [ ] Streaming connector starts
- [ ] Connects to Databricks warehouse `4b9b953939869799`
- [ ] Creates table: `main.bronze.pi_streaming_timeseries`
- [ ] Flushes data every 10-60 seconds
- [ ] Can query data in Unity Catalog

---

## 🔧 Troubleshooting

### Issue: Port 8010 already in use

```bash
lsof -i :8010 | grep LISTEN | awk '{print $2}' | xargs kill
```

### Issue: WebSocket connection refused

- Verify server running: `curl http://localhost:8010/piwebapi`
- Check logs: `tail -f /tmp/mock_server_10k.log`

### Issue: Mock server only shows 128 tags (not 10K)

- Verify env var: `echo $MOCK_PI_TAG_COUNT`
- Restart with explicit var: `MOCK_PI_TAG_COUNT=10000 python3 app/main.py`

### Issue: Dashboard shows "Mock Data" warning

- Check Databricks CLI: `databricks configure`
- Verify table exists: `databricks sql query "SELECT COUNT(*) FROM osipi.bronze.pi_timeseries"`

---

## 📚 Quick Reference

| Feature | URL/Command |
|---------|-------------|
| **Dashboard** | http://localhost:8010/ingestion |
| **PI Web API** | http://localhost:8010/piwebapi |
| **WebSocket** | ws://localhost:8010/piwebapi/streams/channel |
| **AF Hierarchy** | http://localhost:8010/piwebapi/assetdatabases/F1DP-DB-Production/elements |
| **Status API** | http://localhost:8010/api/ingestion/status |
| **Start Server (10K)** | `MOCK_PI_TAG_COUNT=10000 python3 app/main.py` |
| **Test WebSocket** | `python3 test_websocket.py` |
| **Stream to Delta** | `python3 test_streaming_to_delta.py` |
| **Visualize AF** | `python3 visualizations/af_hierarchy_visualizer.py` |

---

**Last Updated:** December 7, 2025
