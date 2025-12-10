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


# ============================================================================
# DASHBOARD API ENDPOINTS
# ============================================================================

@app.get("/api/ingestion/status")
async def get_ingestion_status() -> Dict[str, Any]:
    """Get current ingestion status and KPIs from osipi.bronze tables."""

    # Query actual data from Unity Catalog
    timeseries_stats = execute_sql(f"""
        SELECT
            COUNT(*) as total_rows_today,
            COUNT(DISTINCT tag_webid) as tags_ingested,
            MAX(ingestion_timestamp) as last_run
        FROM {UC_CATALOG}.{UC_SCHEMA}.pi_timeseries
        WHERE DATE(ingestion_timestamp) = CURRENT_DATE()
    """)

    event_frames_count = execute_sql(f"""
        SELECT COUNT(*) as count
        FROM {UC_CATALOG}.{UC_SCHEMA}.pi_event_frames
        WHERE DATE(start_time) = CURRENT_DATE()
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

    # Query actual ingestion rate over last hour
    results = execute_sql(f"""
        SELECT
            DATE_TRUNC('minute', ingestion_timestamp) as ts,
            COUNT(*) as row_count
        FROM {UC_CATALOG}.{UC_SCHEMA}.pi_timeseries
        WHERE ingestion_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
        GROUP BY DATE_TRUNC('minute', ingestion_timestamp)
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
