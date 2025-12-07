"""
Mock PI Web API Server for Development and Testing

Simulates OSI PI Web API endpoints with realistic data generation.
Run with: python tests/mock_pi_server.py
Access at: http://localhost:8000
"""

from fastapi import FastAPI, Query, HTTPException, Header, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import random
import math
import uvicorn
from pydantic import BaseModel
import asyncio
import json
import os
from databricks.sdk import WorkspaceClient
from collections import deque
import threading

app = FastAPI(
    title="Mock PI Web API Server",
    description="Simulated PI Web API for development/testing",
    version="1.0"
)

# Add CORS middleware for browser access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# UNITY CATALOG STREAMING WRITER
# ============================================================================

# Lakeflow Streaming Configuration
UC_CATALOG = os.getenv("UC_CATALOG", "osipi")
UC_SCHEMA = os.getenv("UC_SCHEMA", "bronze")
UC_TABLE = os.getenv("UC_TABLE", "pi_streaming_raw")
UC_FULL_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.{UC_TABLE}"

# Databricks connection
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "4b9b953939869799")

# Lakeflow staging directory (simulating cloud storage for local dev)
# In production, this would be s3://bucket/pi-streams/ or abfss://container@account.dfs.core.windows.net/pi-streams/
LAKEFLOW_STAGING_PATH = os.getenv("LAKEFLOW_STAGING_PATH", "/tmp/lakeflow-pi-streams")

# Streaming buffer configuration
STREAM_BUFFER = deque(maxlen=1000)  # Buffer up to 1000 messages
BUFFER_FLUSH_INTERVAL = 5  # Flush every 5 seconds (faster for testing)
BUFFER_FLUSH_SIZE = 10  # Or when we hit 10 messages (smaller batches for Auto Loader)

# Flag to enable/disable Lakeflow streaming
LAKEFLOW_ENABLED = os.getenv("LAKEFLOW_ENABLED", "true").lower() == "true"

def get_databricks_client():
    """Get Databricks WorkspaceClient using env vars or CLI config."""
    try:
        if DATABRICKS_HOST and DATABRICKS_TOKEN:
            return WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
        else:
            # Use Databricks CLI config (~/.databrickscfg)
            return WorkspaceClient()
    except Exception as e:
        print(f"⚠️  Failed to initialize Databricks client: {e}")
        return None

def flush_buffer_to_lakeflow():
    """Flush buffered WebSocket messages to Lakeflow staging directory as JSON files."""
    if not LAKEFLOW_ENABLED:
        return

    if len(STREAM_BUFFER) == 0:
        return

    try:
        # Extract all messages from buffer
        messages = []
        while STREAM_BUFFER:
            messages.append(STREAM_BUFFER.popleft())

        if not messages:
            return

        print(f"💾 Flushing {len(messages)} WebSocket messages to Lakeflow staging...")

        # Ensure staging directory exists
        os.makedirs(LAKEFLOW_STAGING_PATH, exist_ok=True)

        # Write messages as newline-delimited JSON file
        # Auto Loader can efficiently process JSONL files
        timestamp_ms = int(datetime.now().timestamp() * 1000)
        batch_id = f"{timestamp_ms}_{len(messages)}"
        output_file = f"{LAKEFLOW_STAGING_PATH}/pi_stream_{batch_id}.json"

        with open(output_file, 'w') as f:
            for msg in messages:
                # Write each message as a JSON line (JSONL format)
                json.dump(msg, f)
                f.write('\n')

        print(f"✅ Wrote {len(messages)} messages to {output_file}")
        print(f"📂 Lakeflow staging: {LAKEFLOW_STAGING_PATH}")

    except Exception as e:
        print(f"❌ Error flushing to Lakeflow staging: {e}")
        import traceback
        traceback.print_exc()

async def buffer_flusher():
    """Background task to periodically flush buffer to Lakeflow staging."""
    while True:
        await asyncio.sleep(BUFFER_FLUSH_INTERVAL)
        if len(STREAM_BUFFER) >= BUFFER_FLUSH_SIZE or len(STREAM_BUFFER) > 0:
            # Run flush in thread pool to avoid blocking event loop
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, flush_buffer_to_lakeflow)

# Start buffer flusher on app startup
@app.on_event("startup")
async def startup_event():
    if LAKEFLOW_ENABLED:
        print(f"🚀 Starting Lakeflow streaming pipeline")
        print(f"   Staging path: {LAKEFLOW_STAGING_PATH}")
        print(f"   Target table: {UC_FULL_TABLE}")
        print(f"   Flush interval: {BUFFER_FLUSH_INTERVAL}s or {BUFFER_FLUSH_SIZE} messages")
        asyncio.create_task(buffer_flusher())
    else:
        print("ℹ️  Lakeflow streaming disabled (set LAKEFLOW_ENABLED=true to enable)")

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

# Generate tags for 3 plants, 4 units each, multiple sensors
# Configurable scale: Set TARGET_TAG_COUNT for load-balanced pipeline testing
import os
TARGET_TAG_COUNT = int(os.getenv("MOCK_PI_TAG_COUNT", "128"))  # Default 128, set to 30000 for scale test

# Australian Energy Facility Names (Real Australian power plants, mines, and facilities)
AUSTRALIAN_ENERGY_FACILITIES = [
    # Major Power Stations (Coal, Gas, Hydro, Solar)
    "Loy_Yang_A", "Loy_Yang_B", "Yallourn", "Eraring", "Bayswater",
    "Mount_Piper", "Vales_Point", "Stanwell", "Kogan_Creek", "Callide",
    "Gladstone", "Tarong", "Millmerran", "Torrens_Island", "Pelican_Point",
    "Osborne", "Quarantine", "Hazelwood", "Macquarie", "Uranquinty",

    # Renewable Energy
    "Hornsdale_Wind", "Capital_Wind", "Snowtown_Wind", "Lake_Bonney_Wind",
    "Broken_Hill_Solar", "Nyngan_Solar", "Moree_Solar", "Darling_Downs_Solar",
    "Bungala_Solar", "Tailem_Bend_Solar", "Kennedy_Energy_Park",

    # Hydro Power
    "Snowy_Hydro", "Tumut_1", "Tumut_2", "Tumut_3", "Murray_1", "Murray_2",
    "Blowering", "Gordon", "Poatina", "Tarraleah", "Trevallyn", "Cethana",

    # Mining and Resources
    "Olympic_Dam", "Mount_Isa_Copper", "Bowen_Basin", "Hunter_Valley",
    "Pilbara_Iron_Ore", "Kalgoorlie_Gold", "Roy_Hill", "Port_Hedland",
    "Gladstone_Refinery", "Kwinana_Refinery", "Bell_Bay_Aluminium",

    # Water Treatment
    "Kurnell_Desalination", "Perth_Desalination", "Adelaide_Desalination",
    "Gold_Coast_Desalination", "Sydney_Water_Treatment", "Melbourne_Water",

    # Additional facilities for scale
    "Collie", "Muja", "Bluewaters", "Cockburn", "Alcoa_Pinjarra", "Alcoa_Wagerup"
]

# Calculate required plants/units to reach target
# Formula: plants × units × 8 sensor types = target tags
if TARGET_TAG_COUNT <= 200:
    # Small scale (demo): 4 plants × 4 units × 8 sensors = 128 tags
    plant_names = ["Eraring", "Bayswater", "Loy_Yang_A", "Stanwell"]
    unit_count = 4
elif TARGET_TAG_COUNT <= 1000:
    # Medium scale: 10 plants × 13 units × 8 sensors = 1,040 tags
    plant_names = AUSTRALIAN_ENERGY_FACILITIES[:10]
    unit_count = 13
elif TARGET_TAG_COUNT <= 10000:
    # Large scale: 25 plants × 50 units × 8 sensors = 10,000 tags
    plant_names = AUSTRALIAN_ENERGY_FACILITIES[:25]
    unit_count = 50
else:
    # Massive scale: Use all facilities + numbered expansions
    # 60 facilities needed for 30K tags
    plant_names = AUSTRALIAN_ENERGY_FACILITIES.copy()
    # Add numbered facilities if we need more than available real names
    while len(plant_names) < 60:
        plant_names.append(f"Processing_Plant_{len(plant_names) - len(AUSTRALIAN_ENERGY_FACILITIES) + 1:02d}")
    plant_names = plant_names[:60]
    unit_count = 63

tag_id = 1
for plant in plant_names:
    for unit in range(1, unit_count + 1):
        for sensor_type, units, min_val, max_val, noise in tag_types:
            tag_webid = f"F1DP-{plant}-U{unit:03d}-{sensor_type[:4]}-{tag_id:06d}"
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
                "path": f"\\\\{plant}\\Unit{unit:03d}\\{sensor_type}"
            }
            tag_id += 1

            # Stop if we've hit target (for precise control)
            if len(MOCK_TAGS) >= TARGET_TAG_COUNT:
                break
        if len(MOCK_TAGS) >= TARGET_TAG_COUNT:
            break
    if len(MOCK_TAGS) >= TARGET_TAG_COUNT:
        break

print(f"Generated {len(MOCK_TAGS)} mock PI tags (target: {TARGET_TAG_COUNT})")
print(f"Configuration: {len(plant_names)} plants × {unit_count} units × 8 sensor types")

# Mock AF Hierarchy - Realistic industrial structure
MOCK_AF_HIERARCHY = {
    "F1DP-DB-Production": {
        "Name": "ProductionDB",
        "WebId": "F1DP-DB-Production",
        "Description": "Production Asset Database",
        "Elements": []
    }
}

# Build hierarchical structure (scales with tag count)
# For massive scale, only create AF hierarchy for subset (performance)
af_plant_limit = min(len(plant_names), 10) if TARGET_TAG_COUNT > 10000 else len(plant_names)
af_unit_limit = min(unit_count, 10) if TARGET_TAG_COUNT > 10000 else unit_count

for plant in plant_names[:af_plant_limit]:
    plant_element = {
        "WebId": f"F1DP-Site-{plant}",
        "Name": f"{plant}_Plant",
        "TemplateName": "PlantTemplate",
        "Description": f"Main production facility in {plant}",
        "Path": f"\\\\{plant}_Plant",
        "CategoryNames": ["Production", "Primary"],
        "Elements": []
    }

    for unit in range(1, min(af_unit_limit + 1, unit_count + 1)):
        unit_element = {
            "WebId": f"F1DP-Unit-{plant}-{unit}",
            "Name": f"Unit_{unit}",
            "TemplateName": "ProcessUnitTemplate",
            "Description": f"Processing unit {unit}",
            "Path": f"\\\\{plant}_Plant\\Unit_{unit}",
            "CategoryNames": ["ProcessUnit"],
            "Elements": []
        }

        # Add equipment to each unit
        equipment_types = ["Pump", "Compressor", "HeatExchanger", "Reactor"]
        for equip_type in equipment_types:
            equipment = {
                "WebId": f"F1DP-Equip-{plant}-U{unit}-{equip_type}",
                "Name": f"{equip_type}_101",
                "TemplateName": f"{equip_type}Template",
                "Description": f"{equip_type} equipment",
                "Path": f"\\\\{plant}_Plant\\Unit_{unit}\\{equip_type}_101",
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

# Generate 50 event frames over the past month
base_time = datetime.now() - timedelta(days=30)
for i in range(50):
    template = random.choice(event_templates)
    start = base_time + timedelta(hours=random.randint(0, 720))
    duration = timedelta(minutes=random.randint(30, 240))

    plant = random.choice(plant_names)
    unit = random.randint(1, unit_count)

    event = {
        "WebId": f"F1DP-EF-{i:04d}",
        "Name": f"{template.replace('Template', '')}_{start.strftime('%Y%m%d_%H%M')}",
        "TemplateName": template,
        "StartTime": start.isoformat() + "Z",
        "EndTime": (start + duration).isoformat() + "Z",
        "PrimaryReferencedElementWebId": f"F1DP-Unit-{plant}-{unit}",
        "Description": f"Event on {plant} Unit {unit}",
        "CategoryNames": [template.replace("Template", "")],
        "Attributes": {}
    }

    # Add event-specific attributes
    if template == "BatchRunTemplate":
        event["Attributes"] = {
            "Product": random.choice(["ProductA", "ProductB", "ProductC"]),
            "BatchID": f"BATCH-{i:05d}",
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
            "WorkOrder": f"WO-{i:05d}"
        }

    MOCK_EVENT_FRAMES.append(event)

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
            "Timestamp": current.isoformat() + "Z",
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
    maxCount: int = 1000
):
    """List PI Points (tags) with optional name filter"""
    # Simple wildcard matching
    filtered_tags = []
    filter_pattern = nameFilter.replace("*", "").lower()

    for webid, info in MOCK_TAGS.items():
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

            if len(filtered_tags) >= maxCount:
                break

    return {"Items": filtered_tags}

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

@app.post("/piwebapi/batch")
def batch_execute(payload: BatchPayload):
    """
    Batch controller - Execute multiple requests in single HTTP call

    This is CRITICAL for performance:
    - 100 tags = 1 batch request instead of 100 individual requests
    - 100x performance improvement
    """
    responses = []

    for req in payload.Requests:
        try:
            # Parse resource path
            if "/streams/" in req.Resource and "/recorded" in req.Resource:
                # Extract WebId from path like "/streams/{webid}/recorded"
                parts = req.Resource.split("/")
                webid = parts[2] if len(parts) > 2 else None

                if webid and webid in MOCK_TAGS:
                    params = req.Parameters or {}
                    start_time = params.get("startTime", datetime.now().isoformat() + "Z")
                    end_time = params.get("endTime", datetime.now().isoformat() + "Z")
                    max_count = int(params.get("maxCount", 1000))

                    # Generate data
                    tag_info = MOCK_TAGS[webid]
                    start = datetime.fromisoformat(start_time.replace('Z', ''))
                    end = datetime.fromisoformat(end_time.replace('Z', ''))
                    items = generate_realistic_timeseries(tag_info, start, end, max_count=max_count)

                    responses.append({
                        "Status": 200,
                        "Headers": {"Content-Type": "application/json"},
                        "Content": {
                            "Items": items,
                            "UnitsAbbreviation": tag_info["units"]
                        }
                    })
                else:
                    responses.append({
                        "Status": 404,
                        "Content": {"Message": f"Tag {webid} not found"}
                    })
            else:
                # Unsupported batch resource
                responses.append({
                    "Status": 501,
                    "Content": {"Message": "Batch resource not implemented in mock"}
                })

        except Exception as e:
            responses.append({
                "Status": 500,
                "Content": {"Message": f"Internal error: {str(e)}"}
            })

    return {"Responses": responses}

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
    templateName: Optional[str] = None
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

    return {"Items": filtered_events}

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
# WEBSOCKET REAL-TIME STREAMING
# ============================================================================

# Active WebSocket subscriptions
active_subscriptions = {}

@app.websocket("/piwebapi/streams/channel")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time PI data streaming.

    Protocol:
    1. Client sends: {"Action": "Subscribe", "Resource": "streams/{webid}/value", "Parameters": {"updateRate": 1000}}
    2. Server sends: {"Resource": "...", "Items": [{"Value": ..., "Timestamp": ..., "Good": true}]}
    """
    await websocket.accept()
    client_id = id(websocket)
    subscriptions = []
    running = True

    print(f"✅ WebSocket client {client_id} connected")

    async def send_updates():
        """Background task to send real-time updates"""
        print(f"🚀 send_updates task started for client {client_id}")
        while running:
            try:
                if len(subscriptions) > 0:
                    print(f"🔄 Loop iteration, {len(subscriptions)} subscriptions")
                    for subscription in subscriptions:
                        tag_webid = subscription['tag_webid']

                        # Get current value from tag
                        if tag_webid in MOCK_TAGS:
                            print(f"🔔 Sending update for {tag_webid}")
                            tag = MOCK_TAGS[tag_webid]

                            # Get or initialize current value
                            if 'current_value' not in tag:
                                tag['current_value'] = tag['base']

                            current_value = tag['current_value']

                            # Generate realistic fluctuation
                            min_val = tag['min']
                            max_val = tag['max']
                            noise = tag['noise']
                            change = random.gauss(0, noise / 5)
                            new_value = max(min_val, min(max_val, current_value + change))
                            tag['current_value'] = new_value

                            # Quality flags (95% good, 3% questionable, 2% substituted)
                            quality_roll = random.random()
                            good = quality_roll < 0.95
                            questionable = 0.95 <= quality_roll < 0.98
                            substituted = quality_roll >= 0.98

                            # Send update
                            response = {
                                "Resource": f"streams/{tag_webid}/value",
                                "Items": [{
                                    "Value": round(new_value, 2),
                                    "UnitsAbbreviation": tag['units'],
                                    "Timestamp": datetime.now().isoformat() + "Z",
                                    "Good": good,
                                    "Questionable": questionable,
                                    "Substituted": substituted
                                }]
                            }

                            try:
                                await websocket.send_json(response)
                                print(f"✅ Successfully sent update to client {client_id}")

                                # Buffer message for Unity Catalog streaming
                                if LAKEFLOW_ENABLED:
                                    buffer_msg = {
                                        'tag_webid': tag_webid,
                                        'tag_name': tag['name'],
                                        'timestamp': response['Items'][0]['Timestamp'],
                                        'value': new_value,
                                        'units': tag['units'],
                                        'quality_good': good,
                                        'quality_questionable': questionable,
                                        'quality_substituted': substituted,
                                        'ingestion_timestamp': datetime.now().isoformat() + "Z",
                                        'sensor_type': tag['sensor_type'],
                                        'plant': tag['plant'],
                                        'unit': tag['unit']
                                    }
                                    STREAM_BUFFER.append(buffer_msg)
                                    print(f"📦 Buffered message ({len(STREAM_BUFFER)} in buffer)")

                            except Exception as send_error:
                                print(f"❌ Failed to send: {send_error}")
                                return  # Exit if WebSocket is broken
                        else:
                            print(f"⚠️  Tag {tag_webid} not found in MOCK_TAGS")
            except Exception as e:
                print(f"❌ Exception in send_updates loop: {type(e).__name__}: {e}")
                import traceback
                traceback.print_exc()

            # Wait 1 second before next update
            await asyncio.sleep(1.0)
            print(f"💤 Slept 1 second, looping back...")

    async def receive_messages():
        """Handle incoming subscription messages"""
        nonlocal running
        try:
            while running:
                data = await websocket.receive_text()
                message = json.loads(data)

                action = message.get('Action')

                if action == 'Subscribe':
                    resource = message.get('Resource', '')
                    parameters = message.get('Parameters', {})
                    update_rate = parameters.get('updateRate', 1000)

                    # Extract tag WebID from resource (e.g., "streams/F1DP-Loy_Yang_A-U001-Temp-000001/value")
                    if 'streams/' in resource:
                        tag_webid = resource.split('streams/')[1].split('/')[0]

                        subscriptions.append({
                            'tag_webid': tag_webid,
                            'update_rate': update_rate
                        })

                        print(f"📡 Client {client_id} subscribed to {tag_webid} ({update_rate}ms)")

                        # Send confirmation
                        await websocket.send_json({
                            "Action": "Subscribed",
                            "Resource": resource,
                            "Status": "Success"
                        })

                elif action == 'Unsubscribe':
                    resource = message.get('Resource', '')
                    if 'streams/' in resource:
                        tag_webid = resource.split('streams/')[1].split('/')[0]
                        subscriptions[:] = [s for s in subscriptions if s['tag_webid'] != tag_webid]
                        print(f"🔌 Client {client_id} unsubscribed from {tag_webid}")

        except WebSocketDisconnect:
            print(f"🔌 WebSocket client {client_id} disconnected")
            running = False
        except Exception as e:
            print(f"❌ WebSocket error for client {client_id}: {e}")
            running = False

    # Run both tasks concurrently
    try:
        sender = asyncio.create_task(send_updates())
        receiver = asyncio.create_task(receive_messages())

        # Wait for either task to complete (which means error or disconnect)
        done, pending = await asyncio.wait(
            [sender, receiver],
            return_when=asyncio.FIRST_COMPLETED
        )

        # Cancel remaining tasks
        for task in pending:
            task.cancel()

    except Exception as e:
        print(f"❌ WebSocket handler error: {e}")
    finally:
        running = False
        print(f"🏁 WebSocket handler for client {client_id} finished")

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
