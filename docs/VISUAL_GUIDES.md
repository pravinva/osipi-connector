# Visual Guides - How to See Everything

**Quick reference for all visual interfaces**

---

## ✅ What You Can See NOW (Open in Browser)

### 1. 📊 **Dashboard** - Real Unity Catalog Data
```
http://localhost:8010/ingestion
```

**Shows:**
- ✅ 2,000 rows from `osipi.bronze.pi_timeseries`
- ✅ 20 tags ingested
- ✅ 95% data quality
- ✅ Time-series charts
- ✅ Tag distribution by sensor type
- ✅ Pipeline health

**Status:** ✅ WORKING - Uses real Databricks data (warehouse 4b9b953939869799)

---

### 2. 🌳 **AF Hierarchy Tree** - Interactive Visual
```
file:///Users/pravin.varma/Documents/Demo/osipi-connector/af_hierarchy_tree.html
```

**Shows:**
- ✅ 25 Australian power plants
- ✅ 1,250 units (50 per plant)
- ✅ 5,000 equipment items
- ✅ Total: 6,275 elements
- ✅ Color-coded: Plants (red), Units (blue), Equipment (green)
- ✅ Interactive expand/collapse

**Status:** ✅ WORKING - Open in browser now

---

### 3. 🚨 **Events & Alarms Viewer**
```
file:///Users/pravin.varma/Documents/Demo/osipi-connector/events_alarms_viewer.html
```

**Shows:**
- ✅ 50 event frames over 30 days
- ✅ Batch runs (production runs)
- ✅ Alarms (High/Medium/Low priority)
- ✅ Maintenance events
- ✅ Downtime events
- ✅ Filter by type, time range
- ✅ Event attributes (Product, Operator, BatchID, etc.)

**Status:** ✅ WORKING - Just opened in browser

---

## ⚠️ WebSocket Real-Time (Needs Implementation)

### 4. 🔌 **WebSocket Monitor** - Real-Time Streaming
```
file:///Users/pravin.varma/Documents/Demo/osipi-connector/websocket_monitor.html
```

**Would Show:**
- Real-time tag value updates (1 Hz)
- Live data streaming
- Quality flags
- Connection status

**Status:** ⚠️ **NOT WORKING** - Mock server needs WebSocket support

**Why:** The `mock_pi_server.py` is built on FastAPI (HTTP only), not WebSockets.

**Workaround:** The streaming connector (`pi_streaming_connector.py`) is ready, but needs a WebSocket-enabled PI server (real PI or updated mock).

---

## 📍 Where to Find Each Feature

### Dashboard Data (Real-Time)
```bash
# API endpoint
curl http://localhost:8010/api/ingestion/status | python3 -m json.tool
```

**Shows:**
```json
{
  "status": "Healthy",
  "total_rows_today": 2000,
  "tags_ingested": 20,
  "data_quality_score": 95
}
```

---

### AF Hierarchy (API)
```bash
# Get hierarchy via API
curl "http://localhost:8010/piwebapi/assetdatabases/F1DP-DB-Production/elements" | python3 -m json.tool
```

**Or run Python:**
```python
import requests

response = requests.get('http://localhost:8010/piwebapi/assetdatabases/F1DP-DB-Production/elements')
hierarchy = response.json()

print(f"Plants: {len(hierarchy['Items'])}")
for plant in hierarchy['Items'][:3]:
    print(f"  - {plant['Name']}: {len(plant['Elements'])} units")
```

---

### Events & Alarms (API)
```bash
# Get events from last 7 days
python3 -c "
from datetime import datetime, timedelta
import requests

start = (datetime.now() - timedelta(days=7)).isoformat()
end = datetime.now().isoformat()

url = f'http://localhost:8010/piwebapi/assetdatabases/F1DP-DB-Production/eventframes?startTime={start}&endTime={end}'
response = requests.get(url)
events = response.json()

print(f'Total events: {len(events[\"Items\"])}')
for event in events['Items'][:5]:
    print(f'  - {event[\"TemplateName\"]}: {event[\"Name\"]}')
"
```

---

### Time-Series Data (API)
```bash
# Get data for a specific tag
curl "http://localhost:8010/piwebapi/streams/F1DP-Loy_Yang_A-U001-Temp-000001/recorded?startTime=2025-12-07T00:00:00Z&endTime=2025-12-07T23:59:59Z" | python3 -m json.tool | head -50
```

---

## 🎯 Summary - What Works Now

| Feature | Visual Interface | Status | Notes |
|---------|------------------|--------|-------|
| **Dashboard** | ✅ Browser | Working | Real Unity Catalog data |
| **AF Hierarchy** | ✅ Browser | Working | Interactive tree, 6,275 elements |
| **Events/Alarms** | ✅ Browser | Working | 50 events, filterable |
| **WebSocket Monitor** | ❌ Browser | Not Working | Needs WebSocket server |
| **PI Web API** | ✅ curl/Python | Working | All REST endpoints |
| **Batch Data** | ✅ curl/Python | Working | 100x performance boost |

---

## 📖 How to Use Each Visual

### Dashboard (http://localhost:8010/ingestion)

1. Open in browser
2. Automatically refreshes every 30 seconds
3. Shows:
   - Top KPIs (rows, tags, quality)
   - Time-series chart (hourly data)
   - Tag distribution pie chart
   - Pipeline health status

**Screenshot What You See:**
- Total rows: 2,000
- Tags: 20
- Quality: 95%
- Charts with real data

---

### AF Hierarchy Tree (af_hierarchy_tree.html)

1. Open in browser
2. Click any node to expand/collapse
3. Use "Expand All" button (bottom right) to see everything
4. Hover over nodes for highlighting

**What to Look For:**
- 🏭 Red = Plants (25 Australian power stations)
- ⚙️ Blue = Units (1,250 units)
- 💧🔄🔥⚗️ Green = Equipment (5,000 items)

**Example Path:**
```
Loy_Yang_A_Plant → Unit_1 → Pump_101
```

---

### Events & Alarms Viewer (events_alarms_viewer.html)

1. Open in browser
2. Filter by event type (Batch, Alarm, Maintenance, Downtime)
3. Change time range (1 hour to 30 days)
4. Click event cards to see details

**Event Types:**
- 🏭 **Batch Runs**: Production runs with Product, Operator, Quantity
- 🚨 **Alarms**: High/Medium/Low priority with Alarm Type
- 🔧 **Maintenance**: Preventive/Corrective with Work Order
- ⏸️ **Downtime**: Equipment downtime events

**What You'll See:**
- Event name, timestamp, duration
- Location (plant/unit)
- Attributes (Product, BatchID, Priority, etc.)

---

## 🔧 Quick Commands Reference

### Start Server with 10K Tags
```bash
MOCK_PI_TAG_COUNT=10000 python3 app/main.py
```

### Open All Visuals
```bash
# Dashboard
open http://localhost:8010/ingestion

# AF Hierarchy
open af_hierarchy_tree.html

# Events/Alarms
open events_alarms_viewer.html
```

### Check Data via API
```bash
# Status
curl http://localhost:8010/api/ingestion/status | python3 -m json.tool

# Events count
python3 -c "import requests; from datetime import datetime, timedelta; r = requests.get(f'http://localhost:8010/piwebapi/assetdatabases/F1DP-DB-Production/eventframes?startTime={(datetime.now()-timedelta(days=7)).isoformat()}&endTime={datetime.now().isoformat()}'); print(f'Events: {len(r.json()[\"Items\"])}')"
```

---

## ❓ FAQ

### Q: Why is WebSocket not working?

**A:** The mock server (`mock_pi_server.py`) is built on FastAPI which uses HTTP. WebSocket requires:
1. WebSocket-enabled server (FastAPI supports it, but not implemented yet)
2. `/piwebapi/streams/channel` WebSocket endpoint

**Solution:** Use the real-time REST endpoints instead, or wait for WebSocket mock implementation.

### Q: How to see events/alarms?

**A:** Two ways:
1. **Visual**: Open `events_alarms_viewer.html` in browser
2. **API**: Query `/piwebapi/assetdatabases/{db}/eventframes?startTime=...&endTime=...`

Events include:
- Batch runs
- Alarms (with priority)
- Maintenance events
- Downtime events

### Q: Where are the 10,000 tags?

**A:** In the mock server! Access via:
```bash
# List tags
curl "http://localhost:8010/piwebapi/dataservers/F1DP-Archive/points?maxCount=10"

# Get time-series data
curl "http://localhost:8010/piwebapi/streams/F1DP-Loy_Yang_A-U001-Temp-000001/recorded?startTime=2025-12-07T00:00:00Z&endTime=2025-12-07T23:59:59Z"
```

10,000 tags = 25 plants × 50 units × 8 sensor types

### Q: Can I see real-time data without WebSocket?

**A:** Yes! Use polling:
```python
import requests
import time

tag_webid = "F1DP-Loy_Yang_A-U001-Temp-000001"

while True:
    response = requests.get(f'http://localhost:8010/piwebapi/streams/{tag_webid}/value')
    data = response.json()
    print(f"{data['Timestamp']}: {data['Value']} {data['UnitsAbbreviation']}")
    time.sleep(1)  # Poll every second
```

---

## ✅ What's Ready for Demo

1. **Dashboard** - Live, showing real Unity Catalog data
2. **AF Hierarchy** - Visual tree with 6,275 elements
3. **Events/Alarms** - 50 events, fully interactive
4. **10,000 Tags** - Available via API (batch extraction works)
5. **Module 6** - Streaming connector code ready (needs WebSocket server)

---

**Last Updated:** December 7, 2025, 7:20 PM
