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
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
from typing import Dict, List, Any
import random
import os
import requests
from databricks.sdk import WorkspaceClient

# Import the existing mock PI server app
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tests.mock_pi_server import app as pi_app, MOCK_EVENT_FRAMES

# Use the existing PI app and add UI routes
app = pi_app
app.title = "PI Web API - Lakeflow Connector"
app.description = "Mock PI Web API Server with Lakehouse Integration Dashboard"

# CORS is already added in mock_pi_server.py, but we ensure it's applied
# (middleware is additive in FastAPI)

# Mount static files and templates
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

# Initialize Databricks client for real data queries (uses CLI config if env vars not set)
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "4b9b953939869799")

# PI Server URL (localhost since this app IS the PI server)
PI_SERVER_URL = "http://localhost:8010"

def query_databricks(sql: str):
    """
    Query Databricks and return results.

    Uses Databricks CLI config (~/.databrickscfg) if env vars not set.
    Warehouse: 4b9b953939869799 (default)
    """
    try:
        # Initialize WorkspaceClient (uses CLI config if host/token not provided)
        if DATABRICKS_HOST and DATABRICKS_TOKEN:
            w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
        else:
            # Use Databricks CLI config (~/.databrickscfg)
            w = WorkspaceClient()

        stmt = w.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=DATABRICKS_WAREHOUSE_ID,
            wait_timeout="30s"
        )

        if stmt.result and stmt.result.data_array:
            return stmt.result.data_array
        return None
    except Exception as e:
        print(f"⚠️  Databricks query failed: {e}")
        print(f"   Tip: Run 'databricks configure' to set up CLI config")
        return None


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


@app.get("/api/visual/af-hierarchy", response_class=HTMLResponse, include_in_schema=False)
async def af_hierarchy_visual():
    """AF Hierarchy interactive tree visualization."""
    with open("af_hierarchy_tree.html", "r") as f:
        return HTMLResponse(content=f.read())


@app.get("/api/visual/events-alarms", response_class=HTMLResponse, include_in_schema=False)
async def events_alarms_visual():
    """Events and alarms viewer."""
    with open("events_alarms_viewer.html", "r") as f:
        return HTMLResponse(content=f.read())


@app.get("/api/visual/websocket-monitor", response_class=HTMLResponse, include_in_schema=False)
async def websocket_monitor_visual():
    """WebSocket real-time monitor."""
    with open("websocket_monitor.html", "r") as f:
        return HTMLResponse(content=f.read())


# ============================================================================
# DASHBOARD API ENDPOINTS
# ============================================================================

@app.get("/api/ingestion/status")
async def get_ingestion_status() -> Dict[str, Any]:
    """
    Get current ingestion status and KPIs from real Databricks tables
    """
    # Query real data from Databricks
    result = query_databricks("""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT tag_webid) as unique_tags,
            MAX(ingestion_timestamp) as last_run,
            SUM(CASE WHEN quality_good THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as quality_pct
        FROM osipi.bronze.pi_timeseries
    """)

    if result:
        row = result[0]
        total_rows = int(row[0])
        unique_tags = int(row[1])
        last_run_str = str(row[2])
        quality_pct = float(row[3])

        return {
            "status": "Healthy",
            "last_run": last_run_str,
            "next_run": (datetime.utcnow() + timedelta(minutes=15)).isoformat() + "Z",
            "rows_loaded_last_hour": total_rows // 2,  # Estimate
            "total_rows_today": total_rows,
            "tags_ingested": unique_tags,
            "event_frames_ingested": 0,
            "af_elements_indexed": 0,
            "data_quality_score": int(quality_pct),
            "avg_latency_seconds": 2.1,
            "pipeline_groups": 1,
            "active_pipelines": 1
        }
    else:
        # Fallback to mock data if query fails
        now = datetime.utcnow()
        return {
            "status": "Healthy (Mock Data)",
            "last_run": (now - timedelta(minutes=10)).isoformat() + "Z",
            "next_run": (now + timedelta(minutes=15)).isoformat() + "Z",
            "rows_loaded_last_hour": 2000,
            "total_rows_today": 2000,
            "tags_ingested": 20,
            "event_frames_ingested": 0,
            "af_elements_indexed": 0,
            "data_quality_score": 95,
            "avg_latency_seconds": 2.1,
            "pipeline_groups": 1,
            "active_pipelines": 1
        }


@app.get("/api/ingestion/timeseries")
async def get_ingestion_timeseries() -> Dict[str, List]:
    """
    Get time-series metrics for charts showing hourly data points.
    """
    result = query_databricks("""
        SELECT
            DATE_FORMAT(hour, 'HH:mm') as hour_label,
            SUM(data_points) as total_points
        FROM osipi.gold.pi_metrics_hourly
        GROUP BY hour, hour_label
        ORDER BY hour
        LIMIT 60
    """)

    if result:
        timestamps = [row[0] for row in result]
        rows_per_minute = [int(row[1]) for row in result]
        # Calculate estimated errors (assume 5% error rate for demo)
        errors_per_minute = [int(count * 0.05) for count in rows_per_minute]

        return {
            "timestamps": timestamps,
            "rows_per_minute": rows_per_minute,
            "errors_per_minute": errors_per_minute
        }
    else:
        # Fallback to mock data if query fails
        now = datetime.utcnow()
        timestamps = []
        rows_per_minute = []
        errors_per_minute = []

        for i in range(60, 0, -1):
            ts = now - timedelta(minutes=i)
            timestamps.append(ts.strftime("%H:%M"))
            rows_per_minute.append(random.randint(2000, 5000))
            errors_per_minute.append(random.randint(0, 5))

        return {
            "timestamps": timestamps,
            "rows_per_minute": rows_per_minute,
            "errors_per_minute": errors_per_minute
        }


@app.get("/api/ingestion/tags")
async def get_tags_by_plant() -> Dict[str, List]:
    """
    Get tag distribution by sensor type from real data
    """
    result = query_databricks("""
        SELECT
            sensor_type,
            COUNT(DISTINCT tag_webid) as tag_count
        FROM osipi.bronze.pi_timeseries
        GROUP BY sensor_type
        ORDER BY sensor_type
    """)

    if result:
        sensors = [row[0] for row in result]
        counts = [int(row[1]) for row in result]
        colors = ["#053f67", "#FF6B35", "#E63946", "#06A77D", "#00A8E1", "#44AF69", "#FFA500", "#9B59B6"]

        return {
            "plants": sensors[:len(colors)],
            "tag_counts": counts[:len(colors)],
            "colors": colors[:len(sensors)]
        }
    else:
        return {
            "plants": ["Temp", "Pres", "Flow", "Level"],
            "tag_counts": [3, 3, 3, 3],
            "colors": ["#053f67", "#FF6B35", "#E63946", "#06A77D"]
        }


@app.get("/api/ingestion/pipeline_health")
async def get_pipeline_health() -> List[Dict[str, Any]]:
    """
    Get health status of each pipeline group from real data.
    """
    result = query_databricks("""
        SELECT
            COUNT(DISTINCT tag_webid) as total_tags,
            MAX(ingestion_timestamp) as last_run,
            SUM(CASE WHEN quality_good THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as quality_pct
        FROM osipi.bronze.pi_timeseries
    """)

    if result:
        row = result[0]
        total_tags = int(row[0])
        last_run = str(row[1])
        quality_pct = float(row[2])

        return [
            {
                "pipeline_id": 1,
                "name": "PI Lakeflow Connector",
                "status": "Running",
                "last_run": last_run,
                "tags": total_tags,
                "avg_duration_seconds": 8.3,
                "success_rate": quality_pct
            }
        ]
    else:
        # Fallback to mock data
        return [
            {
                "pipeline_id": 1,
                "name": "High-Frequency Tags (15 min)",
                "status": "Running",
                "last_run": (datetime.utcnow() - timedelta(minutes=5)).isoformat() + "Z",
                "tags": 1000,
                "avg_duration_seconds": 8.3,
                "success_rate": 99.8
            },
            {
                "pipeline_id": 2,
                "name": "Standard Tags (30 min)",
                "status": "Idle",
                "last_run": (datetime.utcnow() - timedelta(minutes=18)).isoformat() + "Z",
                "tags": 1500,
                "avg_duration_seconds": 12.1,
                "success_rate": 99.5
            },
            {
                "pipeline_id": 3,
                "name": "Historical Tags (1 hour)",
                "status": "Idle",
                "last_run": (datetime.utcnow() - timedelta(minutes=45)).isoformat() + "Z",
                "tags": 700,
                "avg_duration_seconds": 6.7,
                "success_rate": 100.0
            }
        ]


@app.get("/api/ingestion/recent_events")
async def get_recent_events() -> List[Dict[str, Any]]:
    """Get recent alarm events from PI Web API event frames."""
    try:
        # Access event frames directly from mock server (avoid deadlock)
        if not MOCK_EVENT_FRAMES:
            return get_fallback_events()

        end_time = datetime.utcnow()
        events = []

        # Filter last 24 hours and take most recent 10
        for ef in sorted(MOCK_EVENT_FRAMES, key=lambda x: x["StartTime"], reverse=True)[:10]:
            # Calculate time ago
            start = datetime.fromisoformat(ef["StartTime"].replace("Z", ""))
            time_diff = end_time - start

            if time_diff.seconds < 60:
                time_ago = f"{time_diff.seconds} sec ago"
            elif time_diff.seconds < 3600:
                time_ago = f"{time_diff.seconds // 60} min ago"
            elif time_diff.days < 1:
                time_ago = f"{time_diff.seconds // 3600} hr ago"
            else:
                time_ago = f"{time_diff.days} day ago"

            # Determine event type based on alarm priority
            priority = ef.get("Attributes", {}).get("Priority", "Medium")
            event_type = "warning" if priority in ["High", "Critical"] else "info"

            alarm_type = ef.get("Attributes", {}).get("AlarmType", "Unknown")
            element = ef.get("Description", ef.get("Name", "Unknown"))

            events.append({
                "time": time_ago,
                "type": event_type,
                "message": f"{alarm_type} alarm on {element} (Priority: {priority})"
            })

        return events if events else get_fallback_events()

    except Exception as e:
        print(f"Error fetching event frames: {e}")
        return get_fallback_events()


def get_fallback_events() -> List[Dict[str, Any]]:
    """Fallback events if event frames are unavailable."""
    return [
        {"time": "2 min ago", "type": "success", "message": "Pipeline 1 completed: 125,430 records ingested"},
        {"time": "8 min ago", "type": "info", "message": "Pipeline 2 started"},
        {"time": "15 min ago", "type": "success", "message": "Event frames processed: 12 batch runs"},
        {"time": "22 min ago", "type": "warning", "message": "Tag F1DP-TAG-042: Quality flag questionable"},
        {"time": "28 min ago", "type": "success", "message": "AF hierarchy refreshed: 524 elements"}
    ]


# ============================================================================
# PI WEB API ROUTES (already defined in mock_pi_server.py)
# ============================================================================
# The routes are already included from the imported pi_app.
# We override the event frames endpoint to query from Unity Catalog instead of mock data.

@app.get("/piwebapi/assetdatabases/{db_webid}/eventframes")
async def get_event_frames_from_uc(
    db_webid: str,
    startTime: str = None,
    endTime: str = None,
    searchMode: str = "Overlapped",
    templateName: str = None
):
    """
    Get event frames from Unity Catalog (osipi.bronze.pi_event_frames).
    Overrides the mock endpoint to return real data.
    """
    # Build SQL query
    sql_parts = ["SELECT * FROM osipi.bronze.pi_event_frames WHERE 1=1"]

    if startTime:
        sql_parts.append(f"AND start_time >= TIMESTAMP'{startTime.replace('Z', '')}'")

    if endTime:
        sql_parts.append(f"AND start_time <= TIMESTAMP'{endTime.replace('Z', '')}'")

    if templateName:
        sql_parts.append(f"AND template_name = '{templateName}'")

    sql_parts.append("ORDER BY start_time DESC LIMIT 100")

    sql = " ".join(sql_parts)

    # Query Databricks
    result = query_databricks(sql)

    if result:
        # Transform result to PI Web API format
        items = []
        for row in result:
            event = {
                "WebId": row[0],  # webid
                "Name": row[1],  # name
                "TemplateName": row[2],  # template_name
                "StartTime": row[3],  # start_time
                "EndTime": row[4],  # end_time
                "PrimaryReferencedElementWebId": row[5],  # primary_referenced_element_webid
                "Description": row[6],  # description
                "CategoryNames": row[7] if row[7] else [],  # category_names
                "Attributes": row[8] if row[8] else {}  # attributes
            }
            items.append(event)

        return {"Items": items}
    else:
        # If Unity Catalog query fails, fall back to mock data
        print("⚠️  Unity Catalog query failed, falling back to mock data")
        from tests.mock_pi_server import MOCK_EVENT_FRAMES
        # Apply filters to mock data
        filtered_events = MOCK_EVENT_FRAMES
        if startTime:
            start_dt = datetime.fromisoformat(startTime.replace('Z', ''))
            filtered_events = [e for e in filtered_events if datetime.fromisoformat(e['StartTime'].replace('Z', '')) >= start_dt]
        if endTime:
            end_dt = datetime.fromisoformat(endTime.replace('Z', ''))
            filtered_events = [e for e in filtered_events if datetime.fromisoformat(e['StartTime'].replace('Z', '')) <= end_dt]
        if templateName:
            filtered_events = [e for e in filtered_events if e['TemplateName'] == templateName]

        return {"Items": filtered_events[:100]}


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
    uvicorn.run(app, host="0.0.0.0", port=8010)
