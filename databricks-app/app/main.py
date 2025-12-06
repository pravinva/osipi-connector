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
from typing import Dict, List, Any
import random

# Import the existing mock PI server app
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tests.mock_pi_server import app as pi_app

# Use the existing PI app and add UI routes
app = pi_app
app.title = "PI Web API - Lakeflow Connector"
app.description = "Mock PI Web API Server with Lakehouse Integration Dashboard"

# Mount static files and templates
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")


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
    """
    Get current ingestion status and KPIs.

    In production, this would query Delta tables:
    - SELECT COUNT(*) FROM main.bronze.pi_timeseries WHERE date = current_date()
    - SELECT COUNT(DISTINCT tag_webid) FROM main.bronze.pi_timeseries
    - SELECT MAX(ingestion_timestamp) FROM checkpoints.pi_watermarks
    """
    # Mock data for demo
    now = datetime.utcnow()
    last_run = now - timedelta(minutes=random.randint(5, 30))

    return {
        "status": "Healthy",
        "last_run": last_run.isoformat() + "Z",
        "next_run": (now + timedelta(minutes=15)).isoformat() + "Z",
        "rows_loaded_last_hour": random.randint(100000, 500000),
        "total_rows_today": random.randint(2000000, 5000000),
        "tags_ingested": 3200,
        "event_frames_ingested": 48,
        "af_elements_indexed": 524,
        "data_quality_score": random.randint(95, 100),
        "avg_latency_seconds": round(random.uniform(8.0, 12.0), 2),
        "pipeline_groups": 3,
        "active_pipelines": 2
    }


@app.get("/api/ingestion/timeseries")
async def get_ingestion_timeseries() -> Dict[str, List]:
    """
    Get time-series metrics for charts.

    In production:
    SELECT
        date_trunc('minute', ingestion_timestamp) as ts,
        COUNT(*) as row_count
    FROM main.bronze.pi_timeseries
    WHERE ingestion_timestamp >= current_timestamp - INTERVAL 1 HOUR
    GROUP BY ts
    ORDER BY ts
    """
    # Generate mock data for last 60 minutes
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
    Get tag distribution by plant/site.

    In production:
    SELECT
        SPLIT(element_path, '/')[1] as plant,
        COUNT(DISTINCT tag_webid) as tag_count
    FROM main.bronze.pi_asset_hierarchy
    GROUP BY plant
    """
    return {
        "plants": ["Plant 1", "Plant 2", "Plant 3", "Plant 4"],
        "tag_counts": [850, 920, 780, 650],
        "colors": ["#FF3621", "#00A8E1", "#1B3139", "#44AF69"]
    }


@app.get("/api/ingestion/pipeline_health")
async def get_pipeline_health() -> List[Dict[str, Any]]:
    """
    Get health status of each pipeline group.

    In production:
    - Query job run history from Databricks Jobs API
    - Check last run status, duration, record counts
    """
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
    """Get recent ingestion events/logs."""
    events = [
        {"time": "2 min ago", "type": "success", "message": "Pipeline 1 completed: 125,430 records ingested"},
        {"time": "8 min ago", "type": "info", "message": "Pipeline 2 started"},
        {"time": "15 min ago", "type": "success", "message": "Event frames processed: 12 batch runs"},
        {"time": "22 min ago", "type": "warning", "message": "Tag F1DP-TAG-042: Quality flag questionable"},
        {"time": "28 min ago", "type": "success", "message": "AF hierarchy refreshed: 524 elements"}
    ]
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
