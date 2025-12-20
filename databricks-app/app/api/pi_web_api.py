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
import uvicorn
from pydantic import BaseModel


def _iso_z(dt: datetime) -> str:
    # Ensure timestamps are in PI-like Zulu format (e.g., 2025-01-01T00:00:00Z)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


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
        start = datetime.fromisoformat(startTime.replace('Z', ''))
        end = datetime.fromisoformat(endTime.replace('Z', ''))
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid datetime format")

    tag_info = MOCK_TAGS[webid]

    # Generate realistic time-series
    items = generate_realistic_timeseries(tag_info, start, end, interval_seconds=60, max_count=maxCount)

    return {
        "Items": items,
        "UnitsAbbreviation": tag_info["units"]
    }

@app.get("/piwebapi/streams/{webid}/value")
def get_stream_value(webid: str, time: Optional[str] = None):
    """Stream GetValue (mock): returns current value."""
    if webid not in MOCK_TAGS:
        raise HTTPException(status_code=404, detail=f"Tag {webid} not found")

    try:
        ts = datetime.now(timezone.utc) if not time else datetime.fromisoformat(time.replace('Z', ''))
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

@app.get("/piwebapi/assetdatabases/{db_webid}/elements")
def get_database_elements(db_webid: str):
    """Get root elements of AF database"""
    if db_webid not in MOCK_AF_HIERARCHY:
        raise HTTPException(status_code=404, detail=f"Database {db_webid} not found")

    db = MOCK_AF_HIERARCHY[db_webid]
    return {"Items": db["Elements"]}

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
        start = datetime.fromisoformat(startTime.replace('Z', ''))
        end = datetime.fromisoformat(endTime.replace('Z', ''))
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid datetime format")

    filtered_events = []

    for event in MOCK_EVENT_FRAMES:
        event_start = datetime.fromisoformat(event["StartTime"].replace('Z', ''))
        event_end = datetime.fromisoformat(event["EndTime"].replace('Z', ''))

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
        event_start = datetime.fromisoformat(event["StartTime"].replace('Z', ''))
        event_end = datetime.fromisoformat(event["EndTime"].replace('Z', ''))

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
