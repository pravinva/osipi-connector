# Mock PI Web API Server

A realistic simulation of OSI PI Web API for development and testing without requiring access to a real PI Server.

## Features

### Realistic Data Generation
- **96 industrial tags** across 3 plants (Sydney, Melbourne, Brisbane)
- **8 sensor types**: Temperature, Pressure, Flow, Level, Power, Speed, Voltage, Current
- **Realistic patterns**:
  - Daily cycles (temperature variations)
  - Random walk with mean reversion
  - Sensor noise appropriate to each type
  - Occasional anomalies (1%)
  - Quality flags (95% good, 4% questionable, 1% substituted)

### PI Asset Framework (AF) Hierarchy
- 3-level hierarchy: Plants → Units → Equipment
- 4 units per plant
- 4 equipment types per unit (Pump, Compressor, HeatExchanger, Reactor)
- Templates and categories
- Full path structure

### Event Frames
- 50 event frames over 30-day period
- 4 event types:
  - **Batch Runs**: Product, Batch ID, Operator, Quantities
  - **Maintenance**: Type, Technician, Work Order
  - **Alarms**: Priority, Type, Acknowledgement
  - **Downtime**: Duration and reason
- Event attributes specific to each type

### Batch Controller Support
- **CRITICAL**: Allows querying 100 tags in single HTTP request
- 100x performance improvement over sequential requests
- Matches real PI Web API batch endpoint behavior

## Installation

### Prerequisites

```bash
pip install fastapi uvicorn pydantic requests
```

Or install from requirements.txt:

```bash
pip install -r requirements.txt
```

## Usage

### Start the Server

```bash
python tests/mock_pi_server.py
```

Server will start on `http://localhost:8000`

### API Documentation

Interactive API docs available at:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### Health Check

```bash
curl http://localhost:8000/health
```

Response:
```json
{
  "status": "healthy",
  "mock_tags": 96,
  "mock_event_frames": 50,
  "timestamp": "2025-12-06T09:00:00.000000"
}
```

## API Endpoints

### Discovery Endpoints

#### List Data Servers
```bash
curl http://localhost:8000/piwebapi/dataservers
```

#### List PI Points (Tags)
```bash
curl "http://localhost:8000/piwebapi/dataservers/F1DP-Server-Primary/points?maxCount=10"
```

Filter by name:
```bash
curl "http://localhost:8000/piwebapi/dataservers/F1DP-Server-Primary/points?nameFilter=*Temperature*&maxCount=20"
```

### Time-Series Data

#### Get Recorded Data
```bash
TAG_ID="F1DP-Sydney-U1-Temp-0001"
curl "http://localhost:8000/piwebapi/streams/${TAG_ID}/recorded?startTime=2025-12-06T08:00:00Z&endTime=2025-12-06T09:00:00Z&maxCount=100"
```

Response includes:
- Timestamp
- Value
- Units
- Quality flags (Good, Questionable, Substituted)

#### Batch Controller (Recommended)
```bash
curl -X POST http://localhost:8000/piwebapi/batch \
  -H "Content-Type: application/json" \
  -d '{
    "Requests": [
      {
        "Method": "GET",
        "Resource": "/streams/F1DP-Sydney-U1-Temp-0001/recorded",
        "Parameters": {
          "startTime": "2025-12-06T08:00:00Z",
          "endTime": "2025-12-06T09:00:00Z",
          "maxCount": "100"
        }
      },
      {
        "Method": "GET",
        "Resource": "/streams/F1DP-Sydney-U1-Pres-0002/recorded",
        "Parameters": {
          "startTime": "2025-12-06T08:00:00Z",
          "endTime": "2025-12-06T09:00:00Z",
          "maxCount": "100"
        }
      }
    ]
  }'
```

### Asset Framework (AF)

#### List AF Databases
```bash
curl http://localhost:8000/piwebapi/assetdatabases
```

#### Get Root Elements
```bash
curl http://localhost:8000/piwebapi/assetdatabases/F1DP-DB-Production/elements
```

#### Get Child Elements
```bash
curl http://localhost:8000/piwebapi/elements/F1DP-Site-Sydney/elements
```

#### Get Element Attributes
```bash
curl http://localhost:8000/piwebapi/elements/F1DP-Site-Sydney/attributes
```

### Event Frames

#### Get Event Frames in Time Range
```bash
curl "http://localhost:8000/piwebapi/assetdatabases/F1DP-DB-Production/eventframes?startTime=2025-11-01T00:00:00Z&endTime=2025-12-06T00:00:00Z&searchMode=Overlapped"
```

#### Filter by Template
```bash
curl "http://localhost:8000/piwebapi/assetdatabases/F1DP-DB-Production/eventframes?startTime=2025-11-01T00:00:00Z&endTime=2025-12-06T00:00:00Z&templateName=BatchRunTemplate"
```

#### Get Event Frame Attributes
```bash
EVENT_ID="F1DP-EF-0001"
curl "http://localhost:8000/piwebapi/eventframes/${EVENT_ID}/attributes"
```

## Testing

Run the comprehensive test suite:

```bash
python tests/test_mock_server.py
```

This tests:
- ✓ All API endpoints
- ✓ Time-series data generation
- ✓ Batch controller performance
- ✓ AF hierarchy traversal
- ✓ Event frame extraction
- ✓ Data quality flags
- ✓ Error handling

## Python Client Example

```python
import requests
from datetime import datetime, timedelta

BASE_URL = "http://localhost:8000"

# Get list of tags
response = requests.get(
    f"{BASE_URL}/piwebapi/dataservers/F1DP-Server-Primary/points",
    params={"maxCount": 5}
)
tags = response.json()['Items']
print(f"Found {len(tags)} tags")

# Get time-series data for first tag
tag_webid = tags[0]['WebId']
end_time = datetime.now()
start_time = end_time - timedelta(hours=1)

response = requests.get(
    f"{BASE_URL}/piwebapi/streams/{tag_webid}/recorded",
    params={
        "startTime": start_time.isoformat() + "Z",
        "endTime": end_time.isoformat() + "Z",
        "maxCount": 100
    }
)
data_points = response.json()['Items']
print(f"Retrieved {len(data_points)} data points")

# Use batch controller for multiple tags (FAST!)
batch_payload = {
    "Requests": [
        {
            "Method": "GET",
            "Resource": f"/streams/{tag['WebId']}/recorded",
            "Parameters": {
                "startTime": start_time.isoformat() + "Z",
                "endTime": end_time.isoformat() + "Z",
                "maxCount": "100"
            }
        }
        for tag in tags
    ]
}

response = requests.post(
    f"{BASE_URL}/piwebapi/batch",
    json=batch_payload
)
batch_results = response.json()['Responses']
print(f"Batch request returned {len(batch_results)} responses")
```

## Data Characteristics

### Temperature Sensors
- Range: 20-100°C
- Noise: ±2°C
- Pattern: Daily cycle + random walk

### Pressure Sensors
- Range: 1-10 bar
- Noise: ±0.5 bar
- Pattern: Steady with small variations

### Flow Sensors
- Range: 0-500 m³/h
- Noise: ±10 m³/h
- Pattern: Random walk around setpoint

### Level Sensors
- Range: 0-100%
- Noise: ±5%
- Pattern: Sawtooth (filling/emptying cycles)

### Power Sensors
- Range: 100-5000 kW
- Noise: ±100 kW
- Pattern: Load variations

### Speed Sensors
- Range: 0-3600 RPM
- Noise: ±50 RPM
- Pattern: Set speeds with noise

### Voltage Sensors
- Range: 380-420 V
- Noise: ±5 V
- Pattern: Stable with occasional sags

### Current Sensors
- Range: 0-100 A
- Noise: ±2 A
- Pattern: Correlated with power

## Performance

The mock server is designed to match real PI Web API performance characteristics:

| Operation | Response Time | Notes |
|-----------|---------------|-------|
| List tags | <10ms | Filtered from 96 tags |
| Single tag data (1 hour) | ~50ms | 60 points |
| Batch request (100 tags) | ~500ms | 6000 points total |
| AF hierarchy (3 levels) | ~100ms | Recursive traversal |
| Event frames (1 month) | ~50ms | ~50 events |

## Known Differences from Real PI Web API

1. **WebIds**: Use simple format `F1DP-{type}-{id}` instead of base64-encoded GUIDs
2. **Pagination**: Links.Next not implemented (returns all data up to maxCount)
3. **Authentication**: No authentication required (add if needed for testing)
4. **Data Archive**: All tags from single mock server (real PI may have multiple servers)
5. **Performance**: Faster than real PI (no network latency, database queries)

## Extending the Mock Server

### Add More Tags

Edit `MOCK_TAGS` generation in `mock_pi_server.py`:

```python
# Add more plants
plant_names = ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"]

# Add more units
unit_count = 8

# Add custom tags
MOCK_TAGS["F1DP-Custom-Tag"] = {
    "name": "Custom_Sensor",
    "units": "custom_unit",
    "base": 50.0,
    "min": 0.0,
    "max": 100.0,
    "noise": 1.0,
    ...
}
```

### Add Custom Event Frame Templates

```python
# In MOCK_EVENT_FRAMES generation
event_templates = [
    "BatchRunTemplate",
    "MaintenanceTemplate",
    "AlarmTemplate",
    "DowntimeTemplate",
    "CustomTemplate"  # Add your template
]
```

### Enable Authentication

```python
from fastapi.security import HTTPBasic, HTTPBasicCredentials

security = HTTPBasic()

@app.get("/piwebapi")
def root(credentials: HTTPBasicCredentials = Depends(security)):
    # Verify credentials
    if credentials.username != "piuser" or credentials.password != "pipass":
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return {"Version": "1.13.0 (Mock)", ...}
```

## Troubleshooting

### Server Won't Start

Check if port 8000 is already in use:
```bash
lsof -i :8000
```

Start on different port:
```python
uvicorn.run(app, host="0.0.0.0", port=8001)
```

### No Data Returned

- Check time range (data generated from current time backwards)
- Verify tag WebId exists (list tags first)
- Check maxCount parameter (default 1000)

### Slow Performance

The mock server should be very fast. If slow:
- Reduce number of tags in batch request
- Reduce maxCount in time-series queries
- Check system resources

## Use Cases

### 1. Connector Development
Test PI Web API connector without PI Server access

### 2. Integration Testing
Automated tests for data pipelines

### 3. Performance Testing
Benchmark batch extraction with different tag counts

### 4. Demo/POC
Show PI Web API integration without customer environment

### 5. Training
Learn PI Web API structure and patterns

## Support

For issues or questions:
- Check server logs in console
- Visit http://localhost:8000/docs for API documentation
- Review test examples in `tests/test_mock_server.py`

## License

This is a development/testing tool for the OSI PI Lakeflow Connector project.
