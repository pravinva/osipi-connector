"""
Databricks App - PI Web API Mock Server + Lakehouse Dashboard

A production-ready app that:
1. Mimics PI Web API endpoints with AVEVA-style branding
2. Provides live Lakehouse ingestion monitoring dashboard
3. Serves both JSON API and HTML/JS frontend
"""

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import random
import os

# Import the existing mock PI server app
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tests import mock_pi_server
from tests.mock_pi_server import app as pi_app

# Databricks SDK for querying Unity Catalog tables
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

# Use the existing PI app and add UI routes
app = pi_app
app.title = "PI Web API - Lakeflow Connector"
app.description = "Mock PI Web API Server with Lakehouse Integration Dashboard"

# Mount static files and templates
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

# Databricks configuration
UC_CATALOG = os.getenv("UC_CATALOG", "osipi")
UC_SCHEMA = os.getenv("UC_SCHEMA", "bronze")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")

# API Access Configuration
# Set ALLOW_UNAUTHENTICATED_API=true to allow notebooks to call PI Web API endpoints
ALLOW_UNAUTHENTICATED_API = os.getenv("ALLOW_UNAUTHENTICATED_API", "false").lower() == "true"

if ALLOW_UNAUTHENTICATED_API:
    print("⚠️  ALLOW_UNAUTHENTICATED_API=true: PI Web API endpoints are publicly accessible")
    print("   This allows notebooks to ingest data without authentication")

# Initialize Databricks client
try:
    w = WorkspaceClient()
    print(f"✓ Connected to Databricks workspace")
except Exception as e:
    print(f"⚠️  Failed to connect to Databricks: {e}")
    w = None


def execute_sql(query: str) -> List[Dict]:
    """Execute SQL query and return results as list of dicts."""
    if not w or not WAREHOUSE_ID:
        return []

    try:
        response = w.statement_execution.execute_statement(
            statement=query,
            warehouse_id=WAREHOUSE_ID,
            catalog=UC_CATALOG,
            schema=UC_SCHEMA,
            wait_timeout="30s"
        )

        if not response.result or not response.result.data_array:
            return []

        # Convert to list of dicts
        columns = [col.name for col in response.manifest.schema.columns]
        results = []
        for row in response.result.data_array:
            results.append(dict(zip(columns, row)))
        return results

    except Exception as e:
        print(f"SQL execution error: {e}")
        return []


# ============================================================================
# UI ROUTES - Landing Page and Dashboard
# ============================================================================

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def home(request: Request):
    """PI Web API landing page with AVEVA-style branding."""
    return templates.TemplateResponse(
        "pi_home.html",
        {
            "request": request,
            "version": "2019 R2 (Mock)",
            "server_name": "PI-DATABRICKS-DEMO",
            "api_base": "/piwebapi"
        }
    )


@app.get("/ingestion", response_class=HTMLResponse, include_in_schema=False)
async def ingestion_dashboard(request: Request):
    """Lakehouse ingestion status dashboard."""
    return templates.TemplateResponse(
        "ingestion.html",
        {
            "request": request,
            "title": "PI Lakehouse Ingestion Dashboard"
        }
    )


@app.get("/config", response_class=HTMLResponse, include_in_schema=False)
async def config_page(request: Request):
    """Mock server configuration page."""
    # Calculate current stats from mock server
    current_tags = len(mock_pi_server.MOCK_TAGS)

    # Count AF elements (flatten hierarchy)
    current_af = 0
    if "F1DP-DB-Production" in mock_pi_server.MOCK_AF_HIERARCHY:
        for plant in mock_pi_server.MOCK_AF_HIERARCHY["F1DP-DB-Production"].get("Elements", []):
            current_af += 1  # Plant
            for unit in plant.get("Elements", []):
                current_af += 1  # Unit
                current_af += len(unit.get("Elements", []))  # Equipment

    current_events = len(mock_pi_server.MOCK_EVENT_FRAMES)

    # Count unique plants
    current_plants = len(set(tag["plant"] for tag in mock_pi_server.MOCK_TAGS.values()))

    return templates.TemplateResponse(
        "config.html",
        {
            "request": request,
            "current_tags": current_tags,
            "current_af": current_af,
            "current_events": current_events,
            "current_plants": current_plants
        }
    )


# ============================================================================
# DASHBOARD API ENDPOINTS
# ============================================================================

@app.get("/api/ingestion/status")
async def get_ingestion_status() -> Dict[str, Any]:
    """Get current ingestion status and KPIs from osipi.bronze tables."""

    # Query actual data from Unity Catalog
    # Changed from CURRENT_DATE() to last 7 days to show data regardless of ingestion date
    timeseries_stats = execute_sql(f"""
        SELECT
            COUNT(*) as total_rows_today,
            COUNT(DISTINCT tag_webid) as tags_ingested,
            MAX(ingestion_timestamp) as last_run
        FROM {UC_CATALOG}.{UC_SCHEMA}.pi_timeseries
        WHERE ingestion_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
    """)

    event_frames_count = execute_sql(f"""
        SELECT COUNT(*) as count
        FROM {UC_CATALOG}.{UC_SCHEMA}.pi_event_frames
        WHERE start_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
    """)

    af_elements_count = execute_sql(f"""
        SELECT COUNT(DISTINCT webid) as count
        FROM {UC_CATALOG}.{UC_SCHEMA}.pi_af_hierarchy
    """)

    # Extract values with fallback to 0
    stats = timeseries_stats[0] if timeseries_stats else {}
    total_rows = int(stats.get('total_rows_today', 0))
    tags = int(stats.get('tags_ingested', 0))
    last_run = stats.get('last_run') if stats.get('last_run') else datetime.utcnow().isoformat() + "Z"

    event_frames = int(event_frames_count[0].get('count', 0)) if event_frames_count else 0
    af_elements = int(af_elements_count[0].get('count', 0)) if af_elements_count else 0

    return {
        "status": "Healthy" if total_rows > 0 else "No Data",
        "last_run": last_run,
        "next_run": (datetime.utcnow() + timedelta(minutes=15)).isoformat() + "Z",
        "rows_loaded_last_hour": total_rows,  # Simplified: showing today's data
        "total_rows_today": total_rows,
        "tags_ingested": tags,
        "event_frames_ingested": event_frames,
        "af_elements_indexed": af_elements,
        "data_quality_score": 100 if total_rows > 0 else 0,
        "avg_latency_seconds": 0.0,
        "pipeline_groups": 1,
        "active_pipelines": 1 if total_rows > 0 else 0
    }


@app.get("/api/ingestion/timeseries")
async def get_ingestion_timeseries() -> Dict[str, List]:
    """Get time-series metrics for charts from osipi.bronze tables."""

    # Query actual ingestion rate over last 24 hours (changed from 1 hour to show more data)
    results = execute_sql(f"""
        SELECT
            DATE_TRUNC('hour', ingestion_timestamp) as ts,
            COUNT(*) as row_count
        FROM {UC_CATALOG}.{UC_SCHEMA}.pi_timeseries
        WHERE ingestion_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
        GROUP BY DATE_TRUNC('hour', ingestion_timestamp)
        ORDER BY ts
    """)

    timestamps = []
    rows_per_minute = []

    if results:
        for row in results:
            ts = row.get('ts')
            if ts:
                timestamps.append(ts.strftime("%H:%M") if hasattr(ts, 'strftime') else str(ts)[-5:])
                rows_per_minute.append(int(row.get('row_count', 0)))
    else:
        # No data yet - return empty arrays
        pass

    return {
        "timestamps": timestamps,
        "rows_per_minute": rows_per_minute,
        "errors_per_minute": [0] * len(timestamps)  # Error tracking not implemented yet
    }


@app.get("/api/ingestion/tags")
async def get_tags_by_plant() -> Dict[str, List]:
    """Get tag distribution by plant/site from osipi.bronze tables."""

    results = execute_sql(f"""
        SELECT
            plant,
            COUNT(DISTINCT webid) as tag_count
        FROM {UC_CATALOG}.{UC_SCHEMA}.pi_af_hierarchy
        WHERE plant IS NOT NULL
        GROUP BY plant
        ORDER BY tag_count DESC
        LIMIT 10
    """)

    plants = []
    tag_counts = []
    colors = ["#FF3621", "#00A8E1", "#1B3139", "#44AF69", "#F7B32B", "#9F5F80", "#2E8BC0", "#4CAF50"]

    for row in results:
        plants.append(row.get('plant', 'Unknown'))
        tag_counts.append(int(row.get('tag_count', 0)))

    return {
        "plants": plants,
        "tag_counts": tag_counts,
        "colors": colors[:len(plants)]
    }


@app.get("/api/ingestion/pipeline_health")
async def get_pipeline_health() -> List[Dict[str, Any]]:
    """Get health status of ingestion pipeline from osipi.bronze tables."""

    # Check last ingestion time and row count
    result = execute_sql(f"""
        SELECT
            MAX(ingestion_timestamp) as last_run,
            COUNT(*) as row_count,
            COUNT(DISTINCT tag_webid) as tag_count
        FROM {UC_CATALOG}.{UC_SCHEMA}.pi_timeseries
    """)

    if result and result[0]:
        stats = result[0]
        last_run = stats.get('last_run')
        row_count = int(stats.get('row_count', 0))
        tag_count = int(stats.get('tag_count', 0))

        return [{
            "pipeline_id": 1,
            "name": "PI Timeseries Ingestion",
            "status": "Healthy" if row_count > 0 else "No Data",
            "last_run": last_run if last_run else datetime.utcnow().isoformat() + "Z",
            "tags": tag_count,
            "avg_duration_seconds": 0.0,
            "success_rate": 100.0 if row_count > 0 else 0.0
        }]

    return []


@app.get("/api/ingestion/recent_events")
async def get_recent_events() -> List[Dict[str, Any]]:
    """Get recent ingestion events from osipi.bronze tables."""

    # Query recent ingestion activity
    results = execute_sql(f"""
        SELECT
            ingestion_timestamp,
            COUNT(*) as record_count
        FROM {UC_CATALOG}.{UC_SCHEMA}.pi_timeseries
        WHERE ingestion_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
        GROUP BY ingestion_timestamp
        ORDER BY ingestion_timestamp DESC
        LIMIT 5
    """)

    events = []
    now = datetime.utcnow()

    for row in results:
        ts = row.get('ingestion_timestamp')
        count = int(row.get('record_count', 0))

        # Calculate time ago
        if ts:
            if hasattr(ts, 'timestamp'):
                delta = now - ts
                minutes_ago = int(delta.total_seconds() / 60)
                time_str = f"{minutes_ago} min ago" if minutes_ago > 0 else "just now"
            else:
                time_str = "recently"

            events.append({
                "time": time_str,
                "type": "success",
                "message": f"Ingested {count:,} records"
            })

    if not events:
        events.append({
            "time": "N/A",
            "type": "info",
            "message": "No recent ingestion activity. Waiting for connector job to run."
        })

    return events


@app.post("/api/config/update")
async def update_config(config: Dict[str, int]) -> Dict[str, Any]:
    """Update mock server configuration and regenerate data."""
    try:
        tag_count = config.get("tag_count", 128)
        event_count = config.get("event_count", 50)
        event_days = config.get("event_days", 30)

        # Validate inputs
        if not (10 <= tag_count <= 100000):
            return {"success": False, "error": "tag_count must be between 10 and 100,000"}
        if not (10 <= event_count <= 10000):
            return {"success": False, "error": "event_count must be between 10 and 10,000"}
        if not (1 <= event_days <= 365):
            return {"success": False, "error": "event_days must be between 1 and 365"}

        # Regenerate mock data
        print(f"🔄 Regenerating mock data: {tag_count} tags, {event_count} events, {event_days} days history")

        # Import regeneration logic
        import random
        from datetime import datetime, timedelta

        # Clear existing data
        mock_pi_server.MOCK_TAGS.clear()
        mock_pi_server.MOCK_EVENT_FRAMES.clear()

        # Regenerate tags using same logic as mock_pi_server.py
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

        AUSTRALIAN_ENERGY_FACILITIES = [
            "Loy_Yang_A", "Loy_Yang_B", "Yallourn", "Eraring", "Bayswater",
            "Mount_Piper", "Vales_Point", "Stanwell", "Kogan_Creek", "Callide",
            "Gladstone", "Tarong", "Millmerran", "Torrens_Island", "Pelican_Point",
            "Osborne", "Quarantine", "Hazelwood", "Macquarie", "Uranquinty",
            "Hornsdale_Wind", "Capital_Wind", "Snowtown_Wind", "Lake_Bonney_Wind",
            "Broken_Hill_Solar", "Nyngan_Solar", "Moree_Solar", "Darling_Downs_Solar",
            "Bungala_Solar", "Tailem_Bend_Solar", "Kennedy_Energy_Park",
            "Snowy_Hydro", "Tumut_1", "Tumut_2", "Tumut_3", "Murray_1", "Murray_2",
            "Blowering", "Gordon", "Poatina", "Tarraleah", "Trevallyn", "Cethana",
            "Olympic_Dam", "Mount_Isa_Copper", "Bowen_Basin", "Hunter_Valley",
            "Pilbara_Iron_Ore", "Kalgoorlie_Gold", "Roy_Hill", "Port_Hedland",
            "Gladstone_Refinery", "Kwinana_Refinery", "Bell_Bay_Aluminium",
            "Kurnell_Desalination", "Perth_Desalination", "Adelaide_Desalination",
            "Gold_Coast_Desalination", "Sydney_Water_Treatment", "Melbourne_Water",
            "Collie", "Muja", "Bluewaters", "Cockburn", "Alcoa_Pinjarra", "Alcoa_Wagerup"
        ]

        # Calculate required plants/units
        if tag_count <= 200:
            plant_names = ["Eraring", "Bayswater", "Loy_Yang_A", "Stanwell"]
            unit_count = 4
        elif tag_count <= 1000:
            plant_names = AUSTRALIAN_ENERGY_FACILITIES[:10]
            unit_count = 13
        elif tag_count <= 10000:
            plant_names = AUSTRALIAN_ENERGY_FACILITIES[:25]
            unit_count = 50
        else:
            plant_names = AUSTRALIAN_ENERGY_FACILITIES.copy()
            while len(plant_names) < 60:
                plant_names.append(f"Processing_Plant_{len(plant_names) - len(AUSTRALIAN_ENERGY_FACILITIES) + 1:02d}")
            plant_names = plant_names[:60]
            unit_count = 63

        # Generate tags
        tag_id = 1
        for plant in plant_names:
            for unit in range(1, unit_count + 1):
                for sensor_type, units, min_val, max_val, noise in tag_types:
                    tag_webid = f"F1DP-{plant}-U{unit:03d}-{sensor_type[:4]}-{tag_id:06d}"
                    base_value = random.uniform(min_val, max_val)

                    mock_pi_server.MOCK_TAGS[tag_webid] = {
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

                    if len(mock_pi_server.MOCK_TAGS) >= tag_count:
                        break
                if len(mock_pi_server.MOCK_TAGS) >= tag_count:
                    break
            if len(mock_pi_server.MOCK_TAGS) >= tag_count:
                break

        # Regenerate AF Hierarchy
        af_plant_limit = min(len(plant_names), 10) if tag_count > 10000 else len(plant_names)
        af_unit_limit = min(unit_count, 10) if tag_count > 10000 else unit_count

        mock_pi_server.MOCK_AF_HIERARCHY["F1DP-DB-Production"]["Elements"].clear()

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

            mock_pi_server.MOCK_AF_HIERARCHY["F1DP-DB-Production"]["Elements"].append(plant_element)

        # Regenerate event frames
        event_templates = [
            "BatchRunTemplate",
            "MaintenanceTemplate",
            "AlarmTemplate",
            "DowntimeTemplate"
        ]

        base_time = datetime.now() - timedelta(days=event_days)
        for i in range(event_count):
            template = random.choice(event_templates)
            start = base_time + timedelta(hours=random.randint(0, event_days * 24))
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

            if template == "BatchRunTemplate":
                event["Attributes"] = {
                    "Product": random.choice(["ProductA", "ProductB", "ProductC"]),
                    "BatchID": f"BATCH-{i:05d}",
                    "Operator": random.choice(["Operator1", "Operator2", "Operator3"]),
                    "TargetQuantity": random.randint(1000, 5000),
                    "ActualQuantity": random.randint(950, 5000)
                }
            elif template == "MaintenanceTemplate":
                event["Attributes"] = {
                    "MaintenanceType": random.choice(["Scheduled", "Unscheduled", "Preventive"]),
                    "Technician": random.choice(["Tech1", "Tech2", "Tech3"]),
                    "WorkOrderID": f"WO-{i:05d}"
                }
            elif template == "AlarmTemplate":
                event["Attributes"] = {
                    "Severity": random.choice(["Low", "Medium", "High", "Critical"]),
                    "AlarmType": random.choice(["ProcessAlarm", "EquipmentAlarm", "SafetyAlarm"]),
                    "Acknowledged": random.choice([True, False])
                }

            mock_pi_server.MOCK_EVENT_FRAMES.append(event)

        # Calculate AF elements
        af_elements = 0
        if "F1DP-DB-Production" in mock_pi_server.MOCK_AF_HIERARCHY:
            for plant in mock_pi_server.MOCK_AF_HIERARCHY["F1DP-DB-Production"].get("Elements", []):
                af_elements += 1
                for unit in plant.get("Elements", []):
                    af_elements += 1
                    af_elements += len(unit.get("Elements", []))

        print(f"✓ Regenerated: {len(mock_pi_server.MOCK_TAGS)} tags, {af_elements} AF elements, {len(mock_pi_server.MOCK_EVENT_FRAMES)} events")

        return {
            "success": True,
            "tags": len(mock_pi_server.MOCK_TAGS),
            "af_elements": af_elements,
            "events": len(mock_pi_server.MOCK_EVENT_FRAMES),
            "plants": len(plant_names)
        }

    except Exception as e:
        print(f"❌ Configuration update failed: {e}")
        return {
            "success": False,
            "error": str(e)
        }


# ============================================================================
# PI WEB API ROUTES (already defined in mock_pi_server.py)
# ============================================================================
# The routes are already included from the imported pi_app:
# - /piwebapi
# - /piwebapi/assetdatabases
# - /piwebapi/dataservers
# - /piwebapi/assetdatabases/{database_id}/elements
# - /piwebapi/elements/{element_id}/attributes
# - /piwebapi/streams/{webid}/recorded
# - /piwebapi/streams/{webid}/interpolated
# - /piwebapi/assetdatabases/{database_id}/eventframes
# - /piwebapi/batch


# ============================================================================
# HEALTH CHECK
# ============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint for Databricks Apps."""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "version": "2019 R2 (Mock)",
        "components": {
            "pi_web_api": "operational",
            "dashboard": "operational",
            "lakehouse": "connected"
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
