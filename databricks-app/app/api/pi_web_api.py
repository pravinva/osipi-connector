"""
Mock PI Web API Server for Development and Testing

Simulates OSI PI Web API endpoints with realistic data generation.
Run with: python tests/mock_pi_server.py
Access at: http://localhost:8000
"""

from fastapi import FastAPI, Query, HTTPException, Header, Body
from fastapi.responses import JSONResponse
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
import random
import math
import re
import uvicorn
from pydantic import BaseModel


def _iso_z(dt: datetime) -> str:
    # Ensure timestamps are in PI-like Zulu format (e.g., 2025-01-01T00:00:00Z)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

_REL_TIME_RE = re.compile(r"^\*-(\d+)([mhd])$")

def _parse_pi_time(value: str | None, now: datetime | None = None) -> datetime:
    """
    Parse PI Web API time expressions.

    Supports:
    - "*" (now)
    - "*-10m", "*-2h", "*-7d" (relative to now)
    - ISO timestamps with or without Z suffix
    """
    now_dt = now or datetime.now(timezone.utc)
    if value is None or value == "" or value == "*":
        return now_dt

    m = _REL_TIME_RE.match(value)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        if unit == "m":
            return now_dt - timedelta(minutes=n)
        if unit == "h":
            return now_dt - timedelta(hours=n)
        if unit == "d":
            return now_dt - timedelta(days=n)

    v = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


app = FastAPI(
    title="Mock PI Web API Server",
    description="Simulated PI Web API for development/testing",
    version="1.0"
)

# ============================================================================
# MOCK DATA STRUCTURES
# ============================================================================

# Generate 100 realistic industrial tags
MOCK_TAGS = {}
tag_types = [
    ("Temperature", "degC", 20.0, 100.0, 2.0),
    ("Pressure", "bar", 1.0, 10.0, 0.5),
    ("Flow", "m3/h", 0.0, 500.0, 10.0),
    ("Level", "%", 0.0, 100.0, 5.0),
    ("Power", "kW", 100.0, 5000.0, 100.0),
    ("Speed", "RPM", 0.0, 3600.0, 50.0),
    ("Voltage", "V", 380.0, 420.0, 5.0),
    ("Current", "A", 0.0, 100.0, 2.0),
]

# Multi-plant architecture: 10 plants with 1,000 tags each = 10,000 total tags
# Flexible: Support variable number of pipelines by plant or by tag ranges
# Examples:
# - 5 pipelines: Use 5 plants (Sydney, Melbourne, Brisbane, Perth, Adelaide)
# - 10 pipelines: Use all 10 plants (one per plant)
# - 15 pipelines: Use 5 plants with 3 pipelines each (split by tag ranges)
plant_names = ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide",
               "Darwin", "Hobart", "Canberra", "Newcastle", "Wollongong"]
units_per_plant = 125  # 125 units per plant (125 × 8 sensors = 1,000 tags per plant)

tag_id = 1
for plant in plant_names:
    for unit in range(1, units_per_plant + 1):
        for sensor_type, units, min_val, max_val, noise in tag_types:
            tag_webid = f"F1DP-{plant}-U{unit:03d}-{sensor_type[:4]}-{tag_id:05d}"
            base_value = random.uniform(min_val, max_val)

            MOCK_TAGS[tag_webid] = {
                "name": f"{plant}_Unit{unit:03d}_{sensor_type}_PV",
                "units": units,
                "base": base_value,
                "min": min_val,
                "max": max_val,
                "noise": noise,
                "sensor_type": sensor_type,
                "plant": plant,
                "unit": unit,
                "descriptor": f"{sensor_type} sensor at {plant} Plant Unit {unit}",
                "path": f"\\\\{plant}_Plant\\Unit_{unit:03d}\\{sensor_type}"
            }
            tag_id += 1

print(f"Generated {len(MOCK_TAGS)} mock PI tags")

# Mock AF Hierarchy - Realistic industrial structure
MOCK_AF_HIERARCHY = {
    "F1DP-DB-Production": {
        "Name": "ProductionDB",
        "WebId": "F1DP-DB-Production",
        "Description": "Production Asset Database",
        "Elements": []
    }
}

# Build hierarchical structure (5 plants, 50 units per plant, 4 equipment per unit)
# Limit to first 10 units per plant for AF hierarchy (to keep it manageable)
for plant in plant_names:
    plant_element = {
        "WebId": f"F1DP-Site-{plant}",
        "Name": f"{plant}_Plant",
        "TemplateName": "PlantTemplate",
        "Description": f"Main production facility in {plant}",
        "Path": f"\\\\{plant}_Plant",
        "CategoryNames": ["Production", "Primary"],
        "Elements": []
    }

    # Only create AF hierarchy for first 10 units (keeps hierarchy size reasonable)
    for unit in range(1, 11):
        unit_element = {
            "WebId": f"F1DP-Unit-{plant}-{unit:03d}",
            "Name": f"Unit_{unit:03d}",
            "TemplateName": "ProcessUnitTemplate",
            "Description": f"Processing unit {unit}",
            "Path": f"\\\\{plant}_Plant\\Unit_{unit:03d}",
            "CategoryNames": ["ProcessUnit"],
            "Elements": []
        }

        # Add equipment to each unit
        equipment_types = ["Pump", "Compressor", "HeatExchanger", "Reactor"]
        for equip_type in equipment_types:
            equipment = {
                "WebId": f"F1DP-Equip-{plant}-U{unit:03d}-{equip_type}",
                "Name": f"{equip_type}_101",
                "TemplateName": f"{equip_type}Template",
                "Description": f"{equip_type} equipment",
                "Path": f"\\\\{plant}_Plant\\Unit_{unit:03d}\\{equip_type}_101",
                "CategoryNames": ["Equipment"],
                "Elements": []
            }
            unit_element["Elements"].append(equipment)

        plant_element["Elements"].append(unit_element)

    MOCK_AF_HIERARCHY["F1DP-DB-Production"]["Elements"].append(plant_element)

# Mock Event Frames - Batch runs, maintenance, alarms
MOCK_EVENT_FRAMES = []
event_templates = [
    "BatchRunTemplate",
    "MaintenanceTemplate",
    "AlarmTemplate",
    "DowntimeTemplate"
]

# Generate 250 event frames over the past month (50 per plant)
base_time = datetime.now() - timedelta(days=30)
event_id = 1
for plant in plant_names:
    for i in range(50):  # 50 events per plant
        template = random.choice(event_templates)
        start = base_time + timedelta(hours=random.randint(0, 720))
        duration = timedelta(minutes=random.randint(30, 240))

        unit = random.randint(1, 10)  # Reference first 10 units with AF hierarchy

        event = {
            "WebId": f"F1DP-EF-{plant}-{event_id:05d}",
            "Name": f"{plant}_{template.replace('Template', '')}_{start.strftime('%Y%m%d_%H%M')}",
            "TemplateName": template,
            "StartTime": _iso_z(start),
            "EndTime": _iso_z(start + duration),
            "PrimaryReferencedElementWebId": f"F1DP-Unit-{plant}-{unit:03d}",
            "Description": f"Event on {plant} Unit {unit:03d}",
            "CategoryNames": [template.replace("Template", "")],
            "Attributes": {}
        }

        # Add event-specific attributes
        if template == "BatchRunTemplate":
            event["Attributes"] = {
                "Product": random.choice(["ProductA", "ProductB", "ProductC"]),
                "BatchID": f"BATCH-{plant}-{event_id:05d}",
                "Operator": random.choice(["Operator1", "Operator2", "Operator3"]),
                "TargetQuantity": random.randint(1000, 5000),
                "ActualQuantity": random.randint(950, 5000)
            }
        elif template == "AlarmTemplate":
            event["Attributes"] = {
                "Priority": random.choice(["High", "Medium", "Low"]),
                "AlarmType": random.choice(["High Temperature", "Low Pressure", "Equipment Fault"]),
                "AcknowledgedBy": random.choice(["Operator1", "Operator2", "System"])
            }
        elif template == "MaintenanceTemplate":
            event["Attributes"] = {
                "MaintenanceType": random.choice(["Preventive", "Corrective", "Inspection"]),
                "Technician": random.choice(["Tech1", "Tech2", "Tech3"]),
                "WorkOrder": f"WO-{plant}-{event_id:05d}"
            }

        MOCK_EVENT_FRAMES.append(event)
        event_id += 1

print(f"Generated {len(MOCK_EVENT_FRAMES)} mock event frames")

# ============================================================================
# REQUEST MODELS
# ============================================================================

class BatchRequest(BaseModel):
    Method: str
    Resource: str
    Parameters: Optional[Dict] = None
    Content: Optional[Dict] = None

class BatchPayload(BaseModel):
    Requests: List[BatchRequest]


def _iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def generate_realistic_timeseries(
    tag_info: Dict,
    start: datetime,
    end: datetime,
    interval_seconds: int = 60,
    max_count: int = 10000
) -> List[Dict]:
    """
    Generate realistic time-series data with patterns:
    - Daily cycles (temperature variations)
    - Random walk with mean reversion
    - Occasional anomalies
    - Realistic sensor noise
    """
    items = []
    current = start
    interval = timedelta(seconds=interval_seconds)

    base_value = tag_info["base"]
    noise_level = tag_info["noise"]
    min_val = tag_info["min"]
    max_val = tag_info["max"]

    # Initialize random walk
    current_value = base_value

    while current <= end and len(items) < max_count:
        # Daily cycle (24-hour period)
        hour_of_day = current.hour + current.minute / 60.0
        daily_variation = math.sin(2 * math.pi * hour_of_day / 24.0) * (noise_level * 2)

        # Random walk with mean reversion
        drift = (base_value - current_value) * 0.1  # Mean reversion
        random_change = random.gauss(0, noise_level)
        current_value = current_value + drift + random_change + daily_variation

        # Clamp to realistic bounds
        current_value = max(min_val, min(max_val, current_value))

        # Occasionally inject anomalies (1% chance)
        is_anomaly = random.random() < 0.01
        if is_anomaly:
            current_value = random.uniform(min_val, max_val)

        # Determine data quality (95% good, 4% questionable, 1% bad)
        quality_rand = random.random()
        good = quality_rand < 0.95
        questionable = 0.95 <= quality_rand < 0.99
        substituted = quality_rand >= 0.99

        items.append({
            "Timestamp": _iso_z(current),
            "Value": round(current_value, 3),
            "UnitsAbbreviation": tag_info["units"],
            "Good": good,
            "Questionable": questionable,
            "Substituted": substituted,
            "Annotated": False
        })

        current += interval

    return items

def find_element_by_webid(webid: str, elements: List[Dict]) -> Optional[Dict]:
    """Recursively search for element by WebId"""
    for elem in elements:
        if elem["WebId"] == webid:
            return elem
        if "Elements" in elem:
            result = find_element_by_webid(webid, elem["Elements"])
            if result:
                return result
    return None

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/piwebapi")
def root():
    """PI Web API root endpoint"""
    return {
        "Version": "1.13.0 (Mock)",
        "Links": {
            "AssetDatabases": "https://localhost:8000/piwebapi/assetdatabases",
            "DataServers": "https://localhost:8000/piwebapi/dataservers",
            "Self": "https://localhost:8000/piwebapi"
        }
    }

@app.get("/piwebapi/dataservers")
def list_dataservers():
    """List available PI Data Archives"""
    return {
        "Items": [
            {
                "WebId": "F1DP-Server-Primary",
                "Name": "MockPIServer",
                "Description": "Mock PI Data Archive for testing",
                "IsConnected": True,
                "ServerVersion": "2018 SP3 (Mock)",
                "Links": {
                    "Points": "https://localhost:8000/piwebapi/dataservers/F1DP-Server-Primary/points"
                }
            }
        ]
    }

@app.get("/piwebapi/dataservers/{server_webid}/points")
def list_points(
    server_webid: str,
    nameFilter: Optional[str] = "*",
    maxCount: int = 1000,
    startIndex: int = 0
):
    """List PI Points (tags) with optional name filter"""
    # Simple wildcard matching
    filtered_tags = []
    filter_pattern = nameFilter.replace("*", "").lower()

    for webid, info in sorted(MOCK_TAGS.items(), key=lambda kv: kv[1]["name"]):
        if filter_pattern in info["name"].lower() or nameFilter == "*":
            filtered_tags.append({
                "WebId": webid,
                "Name": info["name"],
                "Path": info["path"],
                "Descriptor": info["descriptor"],
                "PointType": "Float32",
                "EngineeringUnits": info["units"],
                "Span": info["max"] - info["min"],
                "Zero": info["min"]
            })


    page = filtered_tags[startIndex : startIndex + maxCount]
    return {"Items": page}

@app.get("/piwebapi/points/{point_webid}/attributes")
def get_point_attributes(point_webid: str, selectedFields: str = None):
    """Point GetAttributes (mock): returns a small set of attributes for a point.

    This is used by the Lakeflow OSIPI connector table: pi_point_attributes.
    """
    if point_webid not in MOCK_TAGS:
        raise HTTPException(status_code=404, detail=f"Point {point_webid} not found")

    info = MOCK_TAGS[point_webid]
    items = [
        {"Name": "Name", "Value": info.get("name"), "Type": "String"},
        {"Name": "Path", "Value": info.get("path"), "Type": "String"},
        {"Name": "EngineeringUnits", "Value": info.get("units"), "Type": "String"},
        {"Name": "Descriptor", "Value": info.get("descriptor"), "Type": "String"},
        {"Name": "SensorType", "Value": info.get("sensor_type"), "Type": "String"},
    ]

    # Optional naive selectedFields support (very lightweight): if provided, keep only matching attribute names.
    if selectedFields:
        sf = str(selectedFields)
        # Expect patterns like 'Items.Name;Items.Value' etc. Keep everything by default.
        # If user provides a comma-separated list of attribute names, filter by that.
        if ',' in sf and 'Items.' not in sf:
            allow = {s.strip() for s in sf.split(',') if s.strip()}
            if allow:
                items = [i for i in items if i.get('Name') in allow]

    return {"Items": items}

@app.get("/piwebapi/streams/{webid}/recorded")
def get_recorded_data(
    webid: str,
    startTime: str,
    endTime: str,
    maxCount: int = 1000,
    boundaryType: str = "Inside"
):
    """
    Get recorded (historical) data for a PI Point

    Simulates realistic sensor data with:
    - Daily cycles
    - Random noise
    - Occasional anomalies
    - Quality flags
    """
    if webid not in MOCK_TAGS:
        raise HTTPException(status_code=404, detail=f"Tag {webid} not found")

    try:
        now = datetime.now(timezone.utc)
        start = _parse_pi_time(startTime, now=now)
        end = _parse_pi_time(endTime, now=now)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid datetime format")

    tag_info = MOCK_TAGS[webid]

    # Generate realistic time-series
    # Changed from 60 to 10 seconds for pagination testing (360 events/hour instead of 60)
    items = generate_realistic_timeseries(tag_info, start, end, interval_seconds=10, max_count=maxCount)

    return {
        "Items": items,
        "UnitsAbbreviation": tag_info["units"]
    }





@app.get("/piwebapi/streams/{webid}/calculated")
def get_calculated(
    webid: str,
    startTime: str,
    endTime: str,
    interval: str = "1m",
    calculationType: str = "Average",
):
    """Stream GetCalculated (mock): returns points at a fixed interval with a simple transform."""
    if webid not in MOCK_TAGS:
        raise HTTPException(status_code=404, detail=f"Tag {webid} not found")

    try:
        now = datetime.now(timezone.utc)
        start = _parse_pi_time(startTime, now=now)
        end = _parse_pi_time(endTime, now=now)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid datetime format")

    m = re.match(r"^(\d+)([smhd])$", str(interval).strip())
    sec = 60
    if m:
        n = int(m.group(1)); u = m.group(2)
        sec = n if u=='s' else n*60 if u=='m' else n*3600 if u=='h' else n*86400

    tag_info = MOCK_TAGS[webid]
    items = generate_realistic_timeseries(tag_info, start, end, interval_seconds=max(1, sec), max_count=2000)

    # Apply a simple transform based on calculationType
    c = str(calculationType).lower()
    out = []
    running = 0.0
    count = 0
    for it in items:
        v = it.get('Value')
        if v is None:
            out.append(it)
            continue
        try:
            fv = float(v)
        except Exception:
            out.append(it)
            continue
        if c == 'total':
            running += fv
            it = dict(it)
            it['Value'] = running
        elif c == 'average':
            running += fv
            count += 1
            it = dict(it)
            it['Value'] = (running / max(1, count))
        # else leave as-is
        out.append(it)

    return {"Items": out, "UnitsAbbreviation": tag_info["units"], "Links": {}}
@app.get("/piwebapi/streams/{webid}/interpolated")
def get_interpolated_data(
    webid: str,
    startTime: str,
    endTime: str,
    interval: str = "1m",
    maxCount: int = 1000,
):
    """Stream GetInterpolated (mock): interpolated values at a fixed interval."""
    if webid not in MOCK_TAGS:
        raise HTTPException(status_code=404, detail=f"Tag {webid} not found")

    try:
        now = datetime.now(timezone.utc)
        start = _parse_pi_time(startTime, now=now)
        end = _parse_pi_time(endTime, now=now)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid datetime format")

    # Parse interval like '30s', '1m', '5m', '1h'
    m = re.match(r"^(\d+)([smhd])$", str(interval).strip())
    sec = 60
    if m:
        n = int(m.group(1))
        u = m.group(2)
        if u == 's':
            sec = n
        elif u == 'm':
            sec = n * 60
        elif u == 'h':
            sec = n * 3600
        elif u == 'd':
            sec = n * 86400

    tag_info = MOCK_TAGS[webid]
    items = generate_realistic_timeseries(tag_info, start, end, interval_seconds=max(1, sec), max_count=maxCount)

    return {
        "Items": items,
        "UnitsAbbreviation": tag_info["units"],
    }


@app.get("/piwebapi/streams/{webid}/plot")
def get_plot_data(
    webid: str,
    startTime: str,
    endTime: str,
    intervals: int = 300,
):
    """Stream GetPlot (mock): downsampled points for visualization."""
    if webid not in MOCK_TAGS:
        raise HTTPException(status_code=404, detail=f"Tag {webid} not found")

    try:
        now = datetime.now(timezone.utc)
        start = _parse_pi_time(startTime, now=now)
        end = _parse_pi_time(endTime, now=now)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid datetime format")

    total = max(1.0, (end - start).total_seconds())
    intervals = int(intervals) if intervals and int(intervals) > 0 else 300
    step = max(1, int(total / intervals))

    tag_info = MOCK_TAGS[webid]
    items = generate_realistic_timeseries(tag_info, start, end, interval_seconds=step, max_count=intervals)

    return {
        "Items": items,
        "UnitsAbbreviation": tag_info["units"],
    }




@app.get("/piwebapi/streams/{webid}/recordedattime")
def get_recorded_at_time(webid: str, time: str = "*"):
    """Stream GetRecordedAtTime (mock)."""
    if webid not in MOCK_TAGS:
        raise HTTPException(status_code=404, detail=f"Tag {webid} not found")

    try:
        ts = _parse_pi_time(time, now=datetime.now(timezone.utc))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid datetime format")

    tag_info = MOCK_TAGS[webid]
    # Generate a small neighborhood and pick the closest timestamp.
    start = ts - timedelta(minutes=5)
    end = ts + timedelta(minutes=5)
    items = generate_realistic_timeseries(tag_info, start, end, interval_seconds=60, max_count=30)
    if not items:
        return {"Timestamp": _iso_z(ts), "UnitsAbbreviation": tag_info["units"], "Good": True, "Value": None}

    # Find closest
    def parse_item_ts(it):
        try:
            return datetime.fromisoformat(it.get('Timestamp').replace('Z','+00:00'))
        except Exception:
            return ts

    best = min(items, key=lambda it: abs((parse_item_ts(it) - ts).total_seconds()))
    return best
@app.get("/piwebapi/streams/{webid}/end")
def get_stream_end(webid: str):
    """Stream GetEnd (mock): last archived value."""
    if webid not in MOCK_TAGS:
        raise HTTPException(status_code=404, detail=f"Tag {webid} not found")

    # Use last point from a short generated window.
    ts = datetime.now(timezone.utc)
    tag_info = MOCK_TAGS[webid]
    items = generate_realistic_timeseries(tag_info, ts - timedelta(minutes=10), ts, interval_seconds=60, max_count=20)
    return items[-1] if items else {
        "Timestamp": _iso_z(ts),
        "UnitsAbbreviation": tag_info["units"],
        "Good": True,
        "Questionable": False,
        "Substituted": False,
        "Annotated": False,
        "Value": None,
    }
@app.get("/piwebapi/streams/{webid}/value")
def get_stream_value(webid: str, time: Optional[str] = None):
    """Stream GetValue (mock): returns current value."""
    if webid not in MOCK_TAGS:
        raise HTTPException(status_code=404, detail=f"Tag {webid} not found")

    try:
        ts = _parse_pi_time(time, now=datetime.now(timezone.utc))
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid datetime format")

    tag_info = MOCK_TAGS[webid]
    start = ts - timedelta(minutes=5)
    end = ts
    items = generate_realistic_timeseries(tag_info, start, end, interval_seconds=60, max_count=10)
    return items[-1] if items else {
        "Timestamp": _iso_z(ts),
        "UnitsAbbreviation": tag_info["units"],
        "Good": True,
        "Questionable": False,
        "Substituted": False,
        "Annotated": False,
        "Value": None,
    }


@app.get("/piwebapi/streams/{webid}/summary")
def get_stream_summary(
    webid: str,
    startTime: Optional[str] = None,
    endTime: Optional[str] = None,
    summaryType: Optional[List[str]] = Query(None),
):
    """Stream GetSummary (mock): supports Total and Count."""
    if webid not in MOCK_TAGS:
        raise HTTPException(status_code=404, detail=f"Tag {webid} not found")

    try:
        end = datetime.now(timezone.utc) if not endTime or endTime == '*' else datetime.fromisoformat(endTime.replace('Z', ''))
        if not startTime or startTime.startswith('*-'):
            # support '*-Nd'
            days = 1
            if startTime and startTime.endswith('d'):
                days = int(startTime.replace('*-', '').replace('d', ''))
            start = end - timedelta(days=days)
        else:
            start = datetime.fromisoformat(startTime.replace('Z', ''))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid datetime format")

    tag_info = MOCK_TAGS[webid]
    items = generate_realistic_timeseries(tag_info, start, end, interval_seconds=60, max_count=2000)
    values = [i.get('Value') for i in items if i.get('Good', True) and i.get('Value') is not None]

    types = summaryType or ["Total"]
    out = []
    for t in types:
        t_norm = str(t)
        if t_norm.lower() == 'count':
            val = float(len(values))
        else:
            val = float(sum(values)) if values else 0.0
        out.append({
            "Type": t_norm,
            "Value": {
                "Timestamp": _iso_z(end),
                "UnitsAbbreviation": tag_info["units"],
                "Good": True,
                "Questionable": False,
                "Substituted": False,
                "Annotated": False,
                "Value": val,
            },
        })

    return {"Items": out, "Links": {}}


@app.get("/piwebapi/streamsets/recorded")
def get_streamsets_recorded(
    webId: List[str] = Query(...),
    startTime: str = Query(...),
    endTime: str = Query(...),
    maxCount: int = 1000,
):
    """StreamSet GetRecordedAdHoc (mock): recorded values for multiple streams."""
    streams = []
    for wid in webId:
        if wid not in MOCK_TAGS:
            continue
        rec = get_recorded_data(wid, startTime=startTime, endTime=endTime, maxCount=maxCount)
        streams.append({
            "WebId": wid,
            "Name": MOCK_TAGS[wid]["name"],
            "Path": MOCK_TAGS[wid]["path"],
            "Items": rec.get("Items", []),
            "UnitsAbbreviation": MOCK_TAGS[wid]["units"],
            "Links": {},
        })

    return {"Items": streams, "Links": {}}




@app.get("/piwebapi/streamsets/interpolated")
def get_streamsets_interpolated(
    webId: List[str] = Query(...),
    startTime: str = Query(...),
    endTime: str = Query(...),
    interval: str = "1m",
    maxCount: int = 1000,
):
    """StreamSet GetInterpolatedAdHoc (mock): interpolated values for multiple streams."""
    streams = []
    for wid in webId:
        if wid not in MOCK_TAGS:
            continue
        rec = get_interpolated_data(wid, startTime=startTime, endTime=endTime, interval=interval, maxCount=maxCount)
        streams.append({
            "WebId": wid,
            "Name": MOCK_TAGS[wid]["name"],
            "Path": MOCK_TAGS[wid]["path"],
            "Items": rec.get("Items", []),
            "UnitsAbbreviation": MOCK_TAGS[wid]["units"],
            "Links": {},
        })
    return {"Items": streams, "Links": {}}


@app.get("/piwebapi/streamsets/summary")
def get_streamsets_summary(
    webId: List[str] = Query(...),
    startTime: Optional[str] = None,
    endTime: Optional[str] = None,
    summaryType: Optional[List[str]] = Query(None),
    calculationBasis: Optional[str] = None,
    summaryDuration: Optional[str] = None,
):
    """StreamSet GetSummaryAdHoc (mock): summary values for multiple streams."""
    streams = []
    for wid in webId:
        if wid not in MOCK_TAGS:
            continue
        summ = get_stream_summary(wid, startTime=startTime, endTime=endTime, summaryType=summaryType)
        streams.append({
            "WebId": wid,
            "Name": MOCK_TAGS[wid]["name"],
            "Path": MOCK_TAGS[wid]["path"],
            "Items": summ.get("Items", []),
            "Links": {},
        })
    return {"Items": streams, "Links": {}}


@app.get("/piwebapi/streamsets/plot")
def get_streamsets_plot(
    webId: List[str] = Query(...),
    startTime: str = Query(...),
    endTime: str = Query(...),
    intervals: int = 300,
):
    """StreamSet GetPlotAdHoc (mock): plot values for multiple streams."""
    streams = []
    for wid in webId:
        if wid not in MOCK_TAGS:
            continue
        rec = get_plot_data(wid, startTime=startTime, endTime=endTime, intervals=intervals)
        streams.append({
            "WebId": wid,
            "Name": MOCK_TAGS[wid]["name"],
            "Path": MOCK_TAGS[wid]["path"],
            "Items": rec.get("Items", []),
            "UnitsAbbreviation": MOCK_TAGS[wid]["units"],
            "Links": {},
        })
    return {"Items": streams, "Links": {}}


@app.get("/piwebapi/streamsets/end")
def get_streamsets_end(webId: List[str] = Query(...)):
    """StreamSet GetEndAdHoc (mock): last archived value for multiple streams."""
    items = []
    for wid in webId:
        if wid not in MOCK_TAGS:
            continue
        v = get_stream_end(wid)
        items.append({
            "WebId": wid,
            "Name": MOCK_TAGS[wid]["name"],
            "Path": MOCK_TAGS[wid]["path"],
            "Value": v,
            "Links": {},
        })
    return {"Items": items, "Links": {}}
@app.post("/piwebapi/batch")
def batch_execute(payload: Any = Body(...)):
    """
    Batch controller - Execute multiple requests in single HTTP call

    This is CRITICAL for performance:
    - 100 tags = 1 batch request instead of 100 individual requests
    - 100x performance improvement
    """
    responses_list = []
    responses_dict: Dict[str, dict] = {}

    # Accept official PI Web API batch format (dict keyed by request id) and legacy mock format.
    requests_by_id: Dict[str, dict] = {}
    if isinstance(payload, dict):
        if "Requests" in payload and isinstance(payload.get("Requests"), list):
            for i, req in enumerate(payload.get("Requests") or []):
                if isinstance(req, dict):
                    requests_by_id[str(i + 1)] = req
        else:
            for k, v in payload.items():
                if isinstance(v, dict) and "Method" in v and "Resource" in v:
                    requests_by_id[str(k)] = v
    else:
        try:
            reqs = payload.Requests  # type: ignore[attr-defined]
            for i, req in enumerate(reqs):
                try:
                    requests_by_id[str(i + 1)] = req.dict()
                except Exception:
                    requests_by_id[str(i + 1)] = dict(req)
        except Exception:
            requests_by_id = {}

    for req_id, req in requests_by_id.items():
        try:
            resource = req.get("Resource")
            params = req.get("Parameters") or {}
            if not resource:
                resp_obj = {"Status": 400, "Content": {"Message": "Missing Resource"}}
                responses_list.append(resp_obj)
                responses_dict[req_id] = resp_obj
                continue

            if "/streams/" in resource and "/recorded" in resource:
                # Resource can be like /piwebapi/streams/{webid}/recorded or /streams/{webid}/recorded
                try:
                    webid = resource.split("/streams/", 1)[1].split("/", 1)[0]
                except Exception:
                    webid = None
                if not webid or webid not in MOCK_TAGS:
                    resp_obj = {"Status": 404, "Content": {"Message": f"Tag {webid} not found"}}
                    responses_list.append(resp_obj)
                    responses_dict[req_id] = resp_obj
                    continue
                start_time = params.get("startTime", _iso_z(datetime.now(timezone.utc)))
                end_time = params.get("endTime", _iso_z(datetime.now(timezone.utc)))
                max_count = int(params.get("maxCount", 1000))
                content = get_recorded_data(webid, startTime=start_time, endTime=end_time, maxCount=max_count)
                resp_obj = {"Status": 200, "Headers": {"Content-Type": "application/json"}, "Content": content}
                responses_list.append(resp_obj)
                responses_dict[req_id] = resp_obj



            elif "/streams/" in resource and resource.endswith("/interpolated"):
                try:
                    webid = resource.split("/streams/", 1)[1].split("/", 1)[0]
                except Exception:
                    webid = None
                if not webid or webid not in MOCK_TAGS:
                    resp_obj = {"Status": 404, "Content": {"Message": f"Tag {webid} not found"}}
                    responses_list.append(resp_obj)
                    responses_dict[req_id] = resp_obj
                    continue
                start_time = params.get("startTime", _iso_z(datetime.now(timezone.utc)))
                end_time = params.get("endTime", _iso_z(datetime.now(timezone.utc)))
                interval = params.get("interval", "1m")
                max_count = int(params.get("maxCount", 1000))
                content = get_interpolated_data(webid, startTime=start_time, endTime=end_time, interval=interval, maxCount=max_count)
                resp_obj = {"Status": 200, "Headers": {"Content-Type": "application/json"}, "Content": content}
                responses_list.append(resp_obj)
                responses_dict[req_id] = resp_obj


            elif "/streams/" in resource and resource.endswith("/end"):
                try:
                    webid = resource.split("/streams/", 1)[1].split("/", 1)[0]
                except Exception:
                    webid = None
                if not webid or webid not in MOCK_TAGS:
                    resp_obj = {"Status": 404, "Content": {"Message": f"Tag {webid} not found"}}
                    responses_list.append(resp_obj)
                    responses_dict[req_id] = resp_obj
                    continue
                content = get_stream_end(webid)
                resp_obj = {"Status": 200, "Headers": {"Content-Type": "application/json"}, "Content": content}
                responses_list.append(resp_obj)
                responses_dict[req_id] = resp_obj
            elif "/streams/" in resource and resource.endswith("/value"):
                # Resource can be like /piwebapi/streams/{webid}/recorded or /streams/{webid}/recorded
                try:
                    webid = resource.split("/streams/", 1)[1].split("/", 1)[0]
                except Exception:
                    webid = None
                if not webid or webid not in MOCK_TAGS:
                    resp_obj = {"Status": 404, "Content": {"Message": f"Tag {webid} not found"}}
                    responses_list.append(resp_obj)
                    responses_dict[req_id] = resp_obj
                    continue
                val = get_stream_value(webid, time=params.get("time"))
                resp_obj = {"Status": 200, "Headers": {"Content-Type": "application/json"}, "Content": val}
                responses_list.append(resp_obj)
                responses_dict[req_id] = resp_obj

            elif "/streams/" in resource and resource.endswith("/summary"):
                # Resource can be like /piwebapi/streams/{webid}/recorded or /streams/{webid}/recorded
                try:
                    webid = resource.split("/streams/", 1)[1].split("/", 1)[0]
                except Exception:
                    webid = None
                if not webid or webid not in MOCK_TAGS:
                    resp_obj = {"Status": 404, "Content": {"Message": f"Tag {webid} not found"}}
                    responses_list.append(resp_obj)
                    responses_dict[req_id] = resp_obj
                    continue
                st = params.get("summaryType")
                if isinstance(st, str):
                    st_list = [s for s in st.split(",") if s]
                elif isinstance(st, list):
                    st_list = st
                else:
                    st_list = ["Total"]
                summ = get_stream_summary(webid, startTime=params.get("startTime"), endTime=params.get("endTime"), summaryType=st_list)
                resp_obj = {"Status": 200, "Headers": {"Content-Type": "application/json"}, "Content": summ}
                responses_list.append(resp_obj)
                responses_dict[req_id] = resp_obj

            else:
                resp_obj = {"Status": 501, "Content": {"Message": f"Batch resource not implemented in mock: {resource}"}}
                responses_list.append(resp_obj)
                responses_dict[req_id] = resp_obj

        except Exception as e:
            resp_obj = {"Status": 500, "Content": {"Message": f"Internal error: {str(e)}"}}
            responses_list.append(resp_obj)
            responses_dict[req_id] = resp_obj

    out = dict(responses_dict)
    out["Responses"] = responses_list
    return out



@app.get("/piwebapi/uoms")
def list_uoms():
    """Units of measure (mock): derived from tag population."""
    units = sorted({info.get('units') for info in MOCK_TAGS.values() if info.get('units')})
    items = []
    for u in units:
        items.append({
            "WebId": f"UOM-{u}",
            "Name": u,
            "Abbreviation": u,
            "QuantityType": "Unknown",
        })
    return {"Items": items, "Links": {}}
@app.get("/piwebapi/assetservers")
def list_asset_servers():
    """List available AF servers (PI AF)."""
    return {
        "Items": [
            {
                "WebId": "F1AF-Server-Primary",
                "Name": "MockPIAF",
                "Description": "Mock PI Asset Framework server for testing",
                "IsConnected": True,
                "Links": {
                    "AssetDatabases": "https://localhost:8000/piwebapi/assetservers/F1AF-Server-Primary/assetdatabases"
                },
            }
        ]
    }


@app.get("/piwebapi/assetservers/{server_webid}/assetdatabases")
def list_asset_databases_for_server(server_webid: str):
    """List AF databases under an AF server."""
    if server_webid != "F1AF-Server-Primary":
        raise HTTPException(status_code=404, detail=f"AssetServer {server_webid} not found")
    return {
        "Items": [
            {
                "WebId": "F1DP-DB-Production",
                "Name": "ProductionDB",
                "Description": "Production Asset Database",
                "Path": "\\\\MockPIAF\\\\ProductionDB",
            }
        ]
    }


@app.get("/piwebapi/assetdatabases")
def list_asset_databases():
    """List AF databases"""
    return {
        "Items": [
            {
                "WebId": "F1DP-DB-Production",
                "Name": "ProductionDB",
                "Description": "Production Asset Database",
                "Path": "\\\\MockPIAF\\ProductionDB"
            }
        ]
    }



@app.get("/piwebapi/assetdatabases/{db_webid}/categories")
def get_categories(db_webid: str):
    """AssetDatabase GetCategories (mock)."""
    if db_webid not in MOCK_AF_HIERARCHY:
        raise HTTPException(status_code=404, detail=f"Database {db_webid} not found")

    items = [
        {"WebId": f"{db_webid}-Cat-Production", "Name": "Production", "Description": "Production category", "CategoryType": "Element"},
        {"WebId": f"{db_webid}-Cat-Equipment", "Name": "Equipment", "Description": "Equipment category", "CategoryType": "Element"},
        {"WebId": f"{db_webid}-Cat-Alarm", "Name": "Alarm", "Description": "Alarm category", "CategoryType": "EventFrame"},
    ]
    return {"Items": items, "Links": {}}


@app.get("/piwebapi/assetdatabases/{db_webid}/analyses")
def get_analyses(db_webid: str):
    """AssetDatabase GetAnalyses (mock)."""
    if db_webid not in MOCK_AF_HIERARCHY:
        raise HTTPException(status_code=404, detail=f"Database {db_webid} not found")

    # Provide a small inventory of analyses tied to a couple of elements.
    target_elements = [
        f"F1DP-Unit-Sydney-001",
        f"F1DP-Unit-Melbourne-001",
    ]
    items = []
    for i, te in enumerate(target_elements, start=1):
        items.append({
            "WebId": f"{db_webid}-An-{i:03d}",
            "Name": f"OEE_Calc_{i}",
            "Description": "Mock analysis",
            "Path": f"\\MockPIAF\\ProductionDB\\Analyses\\OEE_Calc_{i}",
            "AnalysisTemplateName": "OEE_Template",
            "TargetElementWebId": te,
        })
    return {"Items": items, "Links": {}}


@app.get("/piwebapi/assetdatabases/{db_webid}/analysistemplates")
def get_analysis_templates(db_webid: str):
    """AssetDatabase GetAnalysisTemplates (mock)."""
    if db_webid not in MOCK_AF_HIERARCHY:
        raise HTTPException(status_code=404, detail=f"Database {db_webid} not found")

    items = [
        {
            "WebId": f"{db_webid}-ATpl-OEE",
            "Name": "OEE_Template",
            "Description": "OEE calculation template",
            "Path": "\\MockPIAF\\ProductionDB\\AnalysisTemplates\\OEE_Template",
        },
        {
            "WebId": f"{db_webid}-ATpl-Energy",
            "Name": "Energy_Template",
            "Description": "Energy KPI template",
            "Path": "\\MockPIAF\\ProductionDB\\AnalysisTemplates\\Energy_Template",
        },
    ]
    return {"Items": items, "Links": {}}




@app.get("/piwebapi/tables/{table_webid}/rows")
def get_table_rows(table_webid: str, startIndex: int = 0, maxCount: int = 100):
    """AF Table rows (mock)."""
    # Provide deterministic fake rows for known mock tables.
    rows = []

    if table_webid.endswith('Tbl-ShiftCalendar'):
        base = [
            {"Shift": "A", "Start": "06:00", "End": "14:00"},
            {"Shift": "B", "Start": "14:00", "End": "22:00"},
            {"Shift": "C", "Start": "22:00", "End": "06:00"},
        ]
        for i, cols in enumerate(base):
            rows.append({"Index": i, "Columns": cols})

    elif table_webid.endswith('Tbl-Products'):
        base = [
            {"Product": "ProductA", "Line": "Line1", "TargetRate": 1000},
            {"Product": "ProductB", "Line": "Line2", "TargetRate": 850},
            {"Product": "ProductC", "Line": "Line3", "TargetRate": 1200},
        ]
        for i, cols in enumerate(base):
            rows.append({"Index": i, "Columns": cols})

    else:
        # Unknown table id: return empty (real PI would 404, but empty is OK for demo)
        rows = []

    page = rows[startIndex : startIndex + maxCount]
    return {"Items": page, "Links": {}}
@app.get("/piwebapi/assetdatabases/{db_webid}/tables")
def get_af_tables(db_webid: str):
    """AssetDatabase GetTables (mock): AF Tables inventory."""
    if db_webid not in MOCK_AF_HIERARCHY:
        raise HTTPException(status_code=404, detail=f"Database {db_webid} not found")

    items = [
        {
            "WebId": f"{db_webid}-Tbl-ShiftCalendar",
            "Name": "ShiftCalendar",
            "Description": "Shift calendar reference table",
            "Path": "\\MockPIAF\\ProductionDB\\Tables\\ShiftCalendar",
        },
        {
            "WebId": f"{db_webid}-Tbl-Products",
            "Name": "Products",
            "Description": "Product master data",
            "Path": "\\MockPIAF\\ProductionDB\\Tables\\Products",
        },
    ]
    return {"Items": items, "Links": {}}
@app.get("/piwebapi/assetdatabases/{db_webid}/elements")
def get_database_elements(db_webid: str):
    """Get root elements of AF database"""
    if db_webid not in MOCK_AF_HIERARCHY:
        raise HTTPException(status_code=404, detail=f"Database {db_webid} not found")

    db = MOCK_AF_HIERARCHY[db_webid]
    return {"Items": db["Elements"]}





@app.get("/piwebapi/elementtemplates/{template_webid}/attributetemplates")
def get_attribute_templates(template_webid: str):
    """ElementTemplate GetAttributeTemplates (mock)."""
    # Basic attribute templates
    items = [
        {
            "WebId": f"{template_webid}-AT-Status",
            "Name": "Status",
            "Description": "Equipment status",
            "Type": "String",
            "DefaultUnitsName": "",
            "DataReferencePlugin": "None",
            "IsConfigurationItem": True,
            "Path": f"\\MockPIAF\\Templates\\{template_webid}\\Status",
        },
        {
            "WebId": f"{template_webid}-AT-Temp",
            "Name": "Temperature",
            "Description": "Temperature attribute",
            "Type": "Double",
            "DefaultUnitsName": "degC",
            "DataReferencePlugin": "PI Point",
            "IsConfigurationItem": False,
            "Path": f"\\MockPIAF\\Templates\\{template_webid}\\Temperature",
        },
    ]
    return {"Items": items, "Links": {}}
@app.get("/piwebapi/assetdatabases/{db_webid}/elementtemplates")
def get_element_templates(db_webid: str):
    """AssetDatabase GetElementTemplates (mock)."""
    if db_webid not in MOCK_AF_HIERARCHY:
        raise HTTPException(status_code=404, detail=f"Database {db_webid} not found")

    templates = [
        {
            "WebId": f"{db_webid}-Tpl-Plant",
            "Name": "PlantTemplate",
            "Description": "Template for plants",
            "Path": f"\\MockPIAF\\{MOCK_AF_HIERARCHY[db_webid]['Name']}\\Templates\\PlantTemplate",
        },
        {
            "WebId": f"{db_webid}-Tpl-Unit",
            "Name": "ProcessUnitTemplate",
            "Description": "Template for process units",
            "Path": f"\\MockPIAF\\{MOCK_AF_HIERARCHY[db_webid]['Name']}\\Templates\\ProcessUnitTemplate",
        },
    ]

    equipment_types = ["Pump", "Compressor", "HeatExchanger", "Reactor"]
    for et in equipment_types:
        templates.append({
            "WebId": f"{db_webid}-Tpl-{et}",
            "Name": f"{et}Template",
            "Description": f"Template for {et}",
            "Path": f"\\MockPIAF\\{MOCK_AF_HIERARCHY[db_webid]['Name']}\\Templates\\{et}Template",
        })

    return {"Items": templates, "Links": {}}
@app.get("/piwebapi/elements/{element_webid}")
def get_element_details(element_webid: str):
    """Get details of a specific AF element"""
    # Search all hierarchy
    for db in MOCK_AF_HIERARCHY.values():
        element = find_element_by_webid(element_webid, db["Elements"])
        if element:
            return element

    raise HTTPException(status_code=404, detail=f"Element {element_webid} not found")

@app.get("/piwebapi/elements/{element_webid}/elements")
def get_child_elements(element_webid: str):
    """Get child elements of an AF element"""
    for db in MOCK_AF_HIERARCHY.values():
        element = find_element_by_webid(element_webid, db["Elements"])
        if element:
            return {"Items": element.get("Elements", [])}

    raise HTTPException(status_code=404, detail=f"Element {element_webid} not found")

@app.get("/piwebapi/elements/{element_webid}/attributes")
def get_element_attributes(element_webid: str):
    """Get attributes of an AF element"""
    # Return mock attributes for any element
    return {
        "Items": [
            {
                "WebId": f"{element_webid}-Attr-Status",
                "Name": "Status",
                "Type": "String",
                "DataReferencePlugIn": "PI Point",
                "DefaultUnitsName": "",
                "IsConfigurationItem": False
            },
            {
                "WebId": f"{element_webid}-Attr-Capacity",
                "Name": "Capacity",
                "Type": "Double",
                "DataReferencePlugIn": "Table Lookup",
                "DefaultUnitsName": "m3/h",
                "IsConfigurationItem": True
            }
        ]
    }



@app.get("/piwebapi/assetdatabases/{db_webid}/eventframetemplates")
def get_eventframe_templates(db_webid: str):
    """AssetDatabase GetEventFrameTemplates (mock)."""
    if db_webid not in MOCK_AF_HIERARCHY:
        raise HTTPException(status_code=404, detail=f"Database {db_webid} not found")

    # Reuse the template names already used in MOCK_EVENT_FRAMES generation.
    items = []
    for t in event_templates:
        items.append({
            "WebId": f"{db_webid}-EFT-{t}",
            "Name": t,
            "Description": f"Template {t}",
            "Path": f"\\MockPIAF\\ProductionDB\\EventFrameTemplates\\{t}",
        })
    return {"Items": items, "Links": {}}


@app.get("/piwebapi/eventframetemplates/{template_webid}/attributetemplates")
def get_eventframe_template_attribute_templates(template_webid: str):
    """EventFrameTemplate GetAttributeTemplates (mock)."""
    items = [
        {
            "WebId": f"{template_webid}-AT-Product",
            "Name": "Product",
            "Description": "Product name",
            "Type": "String",
            "Path": f"\\MockPIAF\\EventFrameTemplates\\{template_webid}\\Product",
        },
        {
            "WebId": f"{template_webid}-AT-BatchID",
            "Name": "BatchID",
            "Description": "Batch identifier",
            "Type": "String",
            "Path": f"\\MockPIAF\\EventFrameTemplates\\{template_webid}\\BatchID",
        },
    ]
    return {"Items": items, "Links": {}}
@app.get("/piwebapi/assetdatabases/{db_webid}/eventframes")
def get_event_frames(
    db_webid: str,
    startTime: str,
    endTime: str,
    searchMode: str = "Overlapped",
    templateName: Optional[str] = None,
    startIndex: int = 0,
    maxCount: int = 1000
):
    """
    Get Event Frames in time range

    SearchMode:
    - Overlapped: Any overlap with time range
    - Inclusive: Completely within time range
    - Exact: Starts exactly at startTime
    """
    try:
        now = datetime.now(timezone.utc)
        start = _parse_pi_time(startTime, now=now)
        end = _parse_pi_time(endTime, now=now)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid datetime format")

    filtered_events = []

    for event in MOCK_EVENT_FRAMES:
        # Ensure event timestamps are timezone-aware (avoid naive vs aware comparison errors)
        try:
            event_start = _parse_pi_time(event.get("StartTime"), now=now)
            event_end = _parse_pi_time(event.get("EndTime") or "*", now=now)
        except Exception:
            continue

        # Apply time filter based on search mode
        include_event = False
        if searchMode == "Overlapped":
            include_event = event_start <= end and event_end >= start
        elif searchMode == "Inclusive":
            include_event = event_start >= start and event_end <= end
        elif searchMode == "Exact":
            include_event = event_start == start

        # Apply template filter if specified
        if templateName and event["TemplateName"] != templateName:
            include_event = False

        if include_event:
            filtered_events.append(event)

    return {"Items": filtered_events[startIndex : startIndex + maxCount]}







@app.get("/piwebapi/eventframes/{ef_webid}/annotations")
def get_eventframe_annotations(ef_webid: str):
    """EventFrame annotations (mock)."""
    ef = next((e for e in MOCK_EVENT_FRAMES if e.get('WebId') == ef_webid), None)
    if not ef:
        raise HTTPException(status_code=404, detail=f"EventFrame {ef_webid} not found")

    now = datetime.now(timezone.utc)
    items = [
        {"Id": f"{ef_webid}-note-1", "Timestamp": _iso_z(now - timedelta(minutes=30)), "User": "Operator2", "Text": "Reviewed during shift handoff"}
    ]
    return {"Items": items, "Links": {}}
@app.get("/piwebapi/eventframes/{ef_webid}/acknowledgements")
def get_eventframe_acknowledgements(ef_webid: str):
    """EventFrame acknowledgements (mock)."""
    ef = next((e for e in MOCK_EVENT_FRAMES if e.get('WebId') == ef_webid), None)
    if not ef:
        raise HTTPException(status_code=404, detail=f"EventFrame {ef_webid} not found")

    now = datetime.now(timezone.utc)
    items = [
        {"Id": f"{ef_webid}-ack-1", "Timestamp": _iso_z(now - timedelta(hours=1)), "User": "Operator1", "Comment": "Acknowledged"}
    ]
    return {"Items": items, "Links": {}}
@app.get("/piwebapi/eventframes/{ef_webid}/referencedelements")
def get_eventframe_referenced_elements(ef_webid: str):
    """EventFrame GetReferencedElements (mock)."""
    ef = next((e for e in MOCK_EVENT_FRAMES if e.get('WebId') == ef_webid), None)
    if not ef:
        raise HTTPException(status_code=404, detail=f"EventFrame {ef_webid} not found")

    primary = ef.get('PrimaryReferencedElementWebId')
    items = []
    if primary:
        items.append({"WebId": primary, "Name": "PrimaryElement"})

    # Add 1-2 additional referenced elements deterministically based on plant.
    try:
        plant = ef_webid.split('-')[2]
    except Exception:
        plant = 'Sydney'
    items.append({"WebId": f"F1DP-Unit-{plant}-001", "Name": f"Unit_001"})
    items.append({"WebId": f"F1DP-Equip-{plant}-U001-Pump", "Name": "Pump_101"})

    # de-dup
    seen=set(); out=[]
    for it in items:
        w=it.get('WebId')
        if w and w not in seen:
            seen.add(w)
            out.append(it)

    return {"Items": out, "Links": {}}
@app.get("/piwebapi/eventframes/{ef_webid}/attributes")
def get_event_frame_attributes(ef_webid: str):
    """Get attributes of an event frame"""
    # Find event frame
    event = next((ef for ef in MOCK_EVENT_FRAMES if ef["WebId"] == ef_webid), None)

    if not event:
        raise HTTPException(status_code=404, detail=f"Event Frame {ef_webid} not found")

    # Convert attributes dict to PI Web API format
    items = []
    for attr_name, attr_value in event.get("Attributes", {}).items():
        items.append({
            "WebId": f"{ef_webid}-Attr-{attr_name}",
            "Name": attr_name,
            "Value": attr_value,
            "Type": type(attr_value).__name__
        })

    return {"Items": items}

@app.get("/piwebapi/streams/{attr_webid}/value")
def get_attribute_value(attr_webid: str):
    """Get current value of an attribute"""
    # Extract event frame ID from attribute WebId
    ef_webid = attr_webid.split("-Attr-")[0] if "-Attr-" in attr_webid else None
    attr_name = attr_webid.split("-Attr-")[1] if "-Attr-" in attr_webid else None

    if ef_webid and attr_name:
        event = next((ef for ef in MOCK_EVENT_FRAMES if ef["WebId"] == ef_webid), None)
        if event and attr_name in event.get("Attributes", {}):
            return {
                "Value": event["Attributes"][attr_name],
                "Timestamp": event["StartTime"]
            }

    raise HTTPException(status_code=404, detail="Attribute value not found")

# ============================================================================
# POST ENDPOINTS - Alternative to GET for Databricks App authentication
# ============================================================================

@app.post("/piwebapi/assetdatabases/list")
def list_asset_databases_post():
    """
    List AF databases (POST alternative for Databricks App)
    Works around authentication issues with GET endpoints in Databricks Apps
    """
    return {
        "Items": [
            {
                "WebId": "F1DP-DB-Production",
                "Name": "ProductionDB",
                "Description": "Production Asset Database",
                "Path": "\\\\MockPIAF\\ProductionDB"
            }
        ]
    }

class AFElementsRequest(BaseModel):
    db_webid: str
    maxCount: Optional[int] = 10000

@app.post("/piwebapi/assetdatabases/elements")
def get_database_elements_post(request: AFElementsRequest):
    """
    Get root elements of AF database (POST alternative for Databricks App)
    Works around authentication issues with GET endpoints in Databricks Apps
    """
    if request.db_webid not in MOCK_AF_HIERARCHY:
        raise HTTPException(status_code=404, detail=f"Database {request.db_webid} not found")

    db = MOCK_AF_HIERARCHY[request.db_webid]
    return {"Items": db["Elements"]}

class EventFramesRequest(BaseModel):
    db_webid: str
    startTime: str
    endTime: str
    searchMode: Optional[str] = "Overlapped"
    maxCount: Optional[int] = 1000

@app.post("/piwebapi/assetdatabases/eventframes")
def get_event_frames_post(request: EventFramesRequest):
    """
    Get event frames from AF database (POST alternative for Databricks App)
    Works around authentication issues with GET endpoints in Databricks Apps
    """
    # Parse time strings
    try:
        if request.startTime.startswith('*'):
            # Handle relative time (e.g., "*-30d")
            days = int(request.startTime.replace('*-', '').replace('d', ''))
            start_dt = datetime.now() - timedelta(days=days)
        else:
            start_dt = datetime.fromisoformat(request.startTime.replace('Z', ''))

        if request.endTime == '*':
            end_dt = datetime.now()
        else:
            end_dt = datetime.fromisoformat(request.endTime.replace('Z', ''))
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid datetime format")

    filtered_events = []

    for event in MOCK_EVENT_FRAMES:
        # Ensure event timestamps are timezone-aware (avoid naive vs aware comparison errors)
        try:
            event_start = _parse_pi_time(event.get("StartTime"), now=now)
            event_end = _parse_pi_time(event.get("EndTime") or "*", now=now)
        except Exception:
            continue

        # Apply time filter based on search mode
        include_event = False

        if request.searchMode == "Overlapped":
            # Include if any part of event overlaps with query time range
            include_event = event_start <= end_dt and event_end >= start_dt
        elif request.searchMode == "StartInclusive":
            # Include if event started within time range
            include_event = start_dt <= event_start <= end_dt

        if include_event:
            filtered_events.append(event)

        if len(filtered_events) >= request.maxCount:
            break

    return {"Items": filtered_events[startIndex : startIndex + maxCount]}

# New POST endpoints for AF element traversal (fix for Databricks App auth)
class ElementRequest(BaseModel):
    element_webid: str

@app.post("/piwebapi/elements/get")
def get_element_post(request: ElementRequest):
    """
    Get element details by WebId (POST alternative for Databricks App)
    Works around authentication issues with GET endpoints in Databricks Apps
    """
    element_webid = request.element_webid

    # Search in AF hierarchy
    def find_element(webid: str, hierarchy: Dict) -> Optional[Dict]:
        """Recursively search for element by WebId"""
        for db_webid, db_data in hierarchy.items():
            if db_data.get("WebId") == webid:
                return db_data

            # Search in elements
            for plant in db_data.get("Elements", []):
                if plant.get("WebId") == webid:
                    return plant

                for unit in plant.get("Elements", []):
                    if unit.get("WebId") == webid:
                        return unit

                    for equipment in unit.get("Elements", []):
                        if equipment.get("WebId") == webid:
                            return equipment

        return None

    element = find_element(element_webid, MOCK_AF_HIERARCHY)

    if not element:
        raise HTTPException(status_code=404, detail=f"Element {element_webid} not found")

    return element

@app.post("/piwebapi/elements/children")
def get_element_children_post(request: ElementRequest):
    """
    Get child elements (POST alternative for Databricks App)
    Works around authentication issues with GET endpoints in Databricks Apps
    """
    element_webid = request.element_webid

    # Search in AF hierarchy
    def find_element(webid: str, hierarchy: Dict) -> Optional[Dict]:
        """Recursively search for element by WebId"""
        for db_webid, db_data in hierarchy.items():
            if db_data.get("WebId") == webid:
                return db_data

            # Search in elements
            for plant in db_data.get("Elements", []):
                if plant.get("WebId") == webid:
                    return plant

                for unit in plant.get("Elements", []):
                    if unit.get("WebId") == webid:
                        return unit

                    for equipment in unit.get("Elements", []):
                        if equipment.get("WebId") == webid:
                            return equipment

        return None

    element = find_element(element_webid, MOCK_AF_HIERARCHY)

    if not element:
        raise HTTPException(status_code=404, detail=f"Element {element_webid} not found")

    # Return child elements
    children = element.get("Elements", [])
    return {"Items": children}

@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "mock_tags": len(MOCK_TAGS),
        "mock_event_frames": len(MOCK_EVENT_FRAMES),
        "timestamp": datetime.now().isoformat()
    }

# ============================================================================
# SERVER STARTUP
# ============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("Mock PI Web API Server Starting...")
    print("=" * 80)
    print(f"📊 Tags available: {len(MOCK_TAGS)}")
    print(f"🏭 AF Elements: {sum(len(db['Elements']) for db in MOCK_AF_HIERARCHY.values())}")
    print(f"📅 Event Frames: {len(MOCK_EVENT_FRAMES)}")
    print("=" * 80)
    print("🚀 Server running at: http://localhost:8000")
    print("📖 API docs at: http://localhost:8000/docs")
    print("=" * 80)

    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
