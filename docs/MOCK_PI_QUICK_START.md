# Mock PI Server - Quick Start Guide

## Start the Server (30 seconds)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start the server
python tests/mock_pi_server.py

# 3. Verify it's running
curl http://localhost:8000/piwebapi
```

**Expected Output**:
```
================================================================================
Mock PI Web API Server Starting...
================================================================================
📊 Tags available: 96
🏭 AF Elements: 12
📅 Event Frames: 50
================================================================================
🚀 Server running at: http://localhost:8000
📖 API docs at: http://localhost:8000/docs
================================================================================
```

---

## Quick API Tests

### 1. Get Server Info
```bash
curl http://localhost:8000/piwebapi
```

### 2. List Data Servers
```bash
curl http://localhost:8000/piwebapi/dataservers
```

### 3. List Tags
```bash
curl "http://localhost:8000/piwebapi/dataservers/F1DP-Server-Primary/points?maxCount=5"
```

### 4. Get Time-Series Data (Last 1 Hour)
```bash
# Get the timestamp range
START=$(date -u -d "1 hour ago" +"%Y-%m-%dT%H:%M:%SZ")
END=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Retrieve data
curl "http://localhost:8000/piwebapi/streams/F1DP-Sydney-U1-Temp-0001/recorded?startTime=${START}&endTime=${END}&maxCount=60"
```

### 5. Batch Request (5 Tags)
```bash
curl -X POST http://localhost:8000/piwebapi/batch \
  -H "Content-Type: application/json" \
  -d '{
    "Requests": [
      {
        "Method": "GET",
        "Resource": "/streams/F1DP-Sydney-U1-Temp-0001/recorded",
        "Parameters": {
          "startTime": "2025-01-08T09:00:00Z",
          "endTime": "2025-01-08T10:00:00Z",
          "maxCount": "50"
        }
      },
      {
        "Method": "GET",
        "Resource": "/streams/F1DP-Sydney-U1-Pres-0002/recorded",
        "Parameters": {
          "startTime": "2025-01-08T09:00:00Z",
          "endTime": "2025-01-08T10:00:00Z",
          "maxCount": "50"
        }
      }
    ]
  }'
```

### 6. List AF Databases
```bash
curl http://localhost:8000/piwebapi/assetdatabases
```

### 7. Get AF Hierarchy
```bash
curl http://localhost:8000/piwebapi/assetdatabases/F1DP-DB-Production/elements
```

### 8. List Event Frames (Last 7 Days)
```bash
START=$(date -u -d "7 days ago" +"%Y-%m-%dT%H:%M:%SZ")
END=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

curl "http://localhost:8000/piwebapi/assetdatabases/F1DP-DB-Production/eventframes?startTime=${START}&endTime=${END}&searchMode=Overlapped"
```

### 9. Health Check
```bash
curl http://localhost:8000/health
```

---

## Run Full Test Suite

```bash
# Terminal 1: Start server
python tests/mock_pi_server.py

# Terminal 2: Run tests
python tests/test_mock_server.py
```

**Expected Output**:
```
================================================================================
Mock PI Web API Server - Test Suite
================================================================================
Testing /piwebapi endpoint...
✓ Version: Mock 1.0
Testing /piwebapi/dataservers...
✓ Found 1 data server(s)
...
✅ ALL TESTS PASSED!
================================================================================
```

---

## Use in Your Code

### Python Example
```python
import requests
from datetime import datetime, timedelta

BASE_URL = "http://localhost:8000"

# Get a single tag's data
tag_webid = "F1DP-Sydney-U1-Temp-0001"
start_time = (datetime.now() - timedelta(hours=1)).isoformat() + "Z"
end_time = datetime.now().isoformat() + "Z"

response = requests.get(
    f"{BASE_URL}/piwebapi/streams/{tag_webid}/recorded",
    params={
        "startTime": start_time,
        "endTime": end_time,
        "maxCount": 100
    }
)

data = response.json()
for item in data["Items"]:
    print(f"{item['Timestamp']}: {item['Value']} {item['UnitsAbbreviation']}")
```

### Batch Request Example
```python
# Get 100 tags efficiently in ONE request
tags = [
    "F1DP-Sydney-U1-Temp-0001",
    "F1DP-Sydney-U1-Pres-0002",
    "F1DP-Sydney-U1-Flow-0003",
    # ... more tags
]

requests_list = [
    {
        "Method": "GET",
        "Resource": f"/streams/{tag}/recorded",
        "Parameters": {
            "startTime": start_time,
            "endTime": end_time,
            "maxCount": "100"
        }
    }
    for tag in tags
]

response = requests.post(
    f"{BASE_URL}/piwebapi/batch",
    json={"Requests": requests_list}
)

responses = response.json()["Responses"]
for i, resp in enumerate(responses):
    if resp["Status"] == 200:
        print(f"Tag {i}: {len(resp['Content']['Items'])} data points")
```

---

## Available Mock Data

### Tags (96 Total)
```
Format: F1DP-{Plant}-U{Unit}-{Type}-{ID}
Example: F1DP-Sydney-U1-Temp-0001

Plants: Sydney, Melbourne, Brisbane
Units: 1-4 per plant
Types: Temp, Pres, Flow, Leve, Powe, Sped, Volt, Curr
ID: 0001-0096
```

**Sample Tags**:
```
F1DP-Sydney-U1-Temp-0001     -> Sydney_Unit1_Temperature_PV (degC)
F1DP-Sydney-U1-Pres-0002     -> Sydney_Unit1_Pressure_PV (bar)
F1DP-Melbourne-U2-Flow-0011  -> Melbourne_Unit2_Flow_PV (m3/h)
F1DP-Brisbane-U4-Power-0032  -> Brisbane_Unit4_Power_PV (kW)
```

### AF Elements
```
Database: F1DP-DB-Production (ProductionDB)
├── F1DP-Site-Sydney (Sydney_Plant)
│   ├── F1DP-Unit-Sydney-1 (Unit_1)
│   ├── F1DP-Unit-Sydney-2 (Unit_2)
│   ├── F1DP-Unit-Sydney-3 (Unit_3)
│   └── F1DP-Unit-Sydney-4 (Unit_4)
├── F1DP-Site-Melbourne (Melbourne_Plant)
│   └── [4 Units]
└── F1DP-Site-Brisbane (Brisbane_Plant)
    └── [4 Units]
```

### Event Frame Templates
```
1. BatchRunTemplate
   - Product, BatchID, Operator, TargetQuantity, ActualQuantity

2. MaintenanceTemplate
   - MaintenanceType, Technician, WorkOrder

3. AlarmTemplate
   - Priority (High/Medium/Low), AlarmType, AcknowledgedBy

4. DowntimeTemplate
   - [Custom attributes]
```

**Sample Event Frames**:
```
F1DP-EF-0001: BatchRun_20250108_1000 (BatchRunTemplate)
              2025-01-08 10:00-12:30 on Sydney Unit 1

F1DP-EF-0015: Maintenance_20250103_0830 (MaintenanceTemplate)
              2025-01-03 08:30-10:45 on Melbourne Unit 2
```

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| "Connection refused" | Start server: `python tests/mock_pi_server.py` |
| Port 8000 already in use | Use different port or kill process: `lsof -i :8000` |
| "Tag not found" 404 | Use valid tag: check `curl .../points` for available tags |
| "Invalid datetime" 400 | Use ISO 8601 format: "2025-01-08T10:00:00Z" |
| No time-series data | Check time range is recent (mock data is daily updated) |
| Batch returns error | Verify Resource path format: "/streams/{webid}/recorded" |

---

## API Documentation

### Interactive Docs (Swagger UI)
```
http://localhost:8000/docs
```

### ReDoc Documentation
```
http://localhost:8000/redoc
```

---

## Performance Tips

### 1. Use Batch for Multiple Tags
**❌ Slow** (10 sequential requests = ~500ms):
```python
for tag in tags[:10]:
    response = requests.get(f".../streams/{tag}/recorded?...")
```

**✓ Fast** (1 batch request = ~50ms):
```python
batch_requests = [...]
response = requests.post(".../batch", json={"Requests": batch_requests})
```

### 2. Limit Time Range
```python
# ✓ Good - 1 hour of data
startTime="2025-01-08T09:00:00Z"
endTime="2025-01-08T10:00:00Z"

# ❌ Slow - 1 year of data (generates 525,600 points!)
startTime="2024-01-08T00:00:00Z"
endTime="2025-01-08T00:00:00Z"
```

### 3. Use maxCount Parameter
```python
# ✓ Limits results to first 1000 points
params={"maxCount": 1000}
```

---

## Example Workflows

### Extract Training Data for ML
```python
import requests
import pandas as pd

BASE_URL = "http://localhost:8000"

# Get all available tags
response = requests.get(
    f"{BASE_URL}/piwebapi/dataservers/F1DP-Server-Primary/points?maxCount=100"
)
tags = response.json()["Items"]

# Extract data for each tag
data_dict = {}
for tag in tags:
    response = requests.get(
        f"{BASE_URL}/piwebapi/streams/{tag['WebId']}/recorded",
        params={
            "startTime": "2024-01-01T00:00:00Z",
            "endTime": "2024-12-31T23:59:59Z"
        }
    )
    data_dict[tag['Name']] = response.json()["Items"]

# Convert to DataFrame
df = pd.DataFrame([
    {
        "timestamp": item["Timestamp"],
        "tag": tag_name,
        "value": item["Value"]
    }
    for tag_name, items in data_dict.items()
    for item in items
])

print(df.head())
```

### Monitor Asset Hierarchy
```python
# Get complete AF hierarchy
def print_hierarchy(element, level=0):
    print("  " * level + f"├─ {element['Name']}")
    for child in element.get("Elements", []):
        print_hierarchy(child, level + 1)

# Get root elements
response = requests.get(
    f"{BASE_URL}/piwebapi/assetdatabases/F1DP-DB-Production/elements"
)
for element in response.json()["Items"]:
    print_hierarchy(element)
```

### Track Event Frames (Last 30 Days)
```python
from datetime import datetime, timedelta

start = (datetime.now() - timedelta(days=30)).isoformat() + "Z"
end = datetime.now().isoformat() + "Z"

response = requests.get(
    f"{BASE_URL}/piwebapi/assetdatabases/F1DP-DB-Production/eventframes",
    params={
        "startTime": start,
        "endTime": end,
        "searchMode": "Overlapped"
    }
)

events = response.json()["Items"]
for event in events:
    print(f"{event['Name']}")
    print(f"  Duration: {event['StartTime']} to {event['EndTime']}")
    print(f"  Template: {event['TemplateName']}")
    print()
```

---

## Next Steps

1. **Review Full Documentation**: `/MOCK_PI_SERVER_DOCUMENTATION.md`
2. **Explore API**: http://localhost:8000/docs
3. **Integration Testing**: Run `tests/test_mock_server.py`
4. **Build on It**: Customize mock data as needed
5. **Deploy**: Use in CI/CD pipelines

---

## Support

For issues or questions:
1. Check Troubleshooting section above
2. Review `/MOCK_PI_SERVER_DOCUMENTATION.md` for detailed API reference
3. Check `/tests/test_mock_server.py` for example usage
4. Review source: `/tests/mock_pi_server.py`

---

**Status**: ✓ Production Ready
**Version**: 1.0
**Last Updated**: 2025-01-08
