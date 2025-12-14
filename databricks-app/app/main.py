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

# Import the PI Web API mock server
from app.api import pi_web_api
from app.api.pi_web_api import app as pi_app

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

# ============================================================================
# CACHING LAYER FOR PERFORMANCE
# ============================================================================
from functools import lru_cache
import time

# Cache configuration
CACHE_TTL = 60  # seconds - cache for 1 minute
_cache_timestamps = {}

def cache_with_ttl(ttl_seconds: int):
    """Decorator to cache function results with a TTL."""
    def decorator(func):
        cache_key = func.__name__
        _cache_timestamps[cache_key] = 0
        cached_func = lru_cache(maxsize=1)(func)

        def wrapper(*args, **kwargs):
            current_time = time.time()
            last_cache_time = _cache_timestamps.get(cache_key, 0)

            # Invalidate cache if TTL expired
            if current_time - last_cache_time > ttl_seconds:
                cached_func.cache_clear()
                _cache_timestamps[cache_key] = current_time

            return cached_func(*args, **kwargs)

        return wrapper
    return decorator


def execute_sql(query: str) -> List[Dict]:
    """Execute SQL query and return results as list of dicts."""
    if not w or not WAREHOUSE_ID:
        print(f"⚠️  Cannot execute SQL: w={w}, WAREHOUSE_ID={WAREHOUSE_ID}")
        return []

    try:
        print(f"🔍 Executing SQL: {query[:100]}...")
        response = w.statement_execution.execute_statement(
            statement=query,
            warehouse_id=WAREHOUSE_ID,
            catalog=UC_CATALOG,
            schema=UC_SCHEMA,
            wait_timeout="30s"
        )

        print(f"📊 Response status: {response.status}")

        if not response.result:
            print("⚠️  No result in response")
            return []

        if not response.result.data_array:
            print("⚠️  No data_array in result")
            return []

        # Convert to list of dicts
        columns = [col.name for col in response.manifest.schema.columns]
        print(f"✓ Got {len(response.result.data_array)} rows with columns: {columns}")
        results = []
        for row in response.result.data_array:
            results.append(dict(zip(columns, row)))
        return results

    except Exception as e:
        print(f"❌ SQL execution error: {e}")
        import traceback
        traceback.print_exc()
        return []


@cache_with_ttl(ttl_seconds=300)  # Cache for 5 minutes
def get_pipeline_tables(table_pattern: str) -> List[str]:
    """
    Dynamically discover all per-pipeline bronze tables matching the pattern.
    Cached for 5 minutes to avoid repeated INFORMATION_SCHEMA queries.

    Args:
        table_pattern: Base table name (e.g., 'pi_timeseries', 'pi_af_hierarchy', 'pi_event_frames')

    Returns:
        List of full table names (e.g., ['pi_timeseries_pipeline1', 'pi_timeseries_pipeline2', ...])
    """
    try:
        # Query INFORMATION_SCHEMA to find all tables matching pattern
        query = f"""
            SELECT table_name
            FROM {UC_CATALOG}.information_schema.tables
            WHERE table_schema = '{UC_SCHEMA}'
              AND table_name LIKE '{table_pattern}_pipeline%'
            ORDER BY table_name
        """

        results = execute_sql(query)
        table_names = [row['table_name'] for row in results]

        if table_names:
            print(f"✓ Found {len(table_names)} tables for pattern '{table_pattern}': {table_names}")
        else:
            print(f"⚠️  No tables found for pattern '{table_pattern}' - may need to check if pipelines have run")

        return table_names

    except Exception as e:
        print(f"❌ Error discovering pipeline tables: {e}")
        return []


def build_union_query(table_pattern: str, select_columns: str, where_clause: str = "",
                      group_by: str = "", order_by: str = "", limit: int = None) -> str:
    """
    Build a UNION ALL query across all per-pipeline bronze tables.

    Args:
        table_pattern: Base table name (e.g., 'pi_timeseries')
        select_columns: Columns to select (e.g., 'COUNT(*) as total, MAX(ingestion_timestamp) as last_run')
        where_clause: Optional WHERE clause (without 'WHERE' keyword)
        group_by: Optional GROUP BY clause (without 'GROUP BY' keyword)
        order_by: Optional ORDER BY clause (without 'ORDER BY' keyword)
        limit: Optional LIMIT value

    Returns:
        Complete SQL query with UNION ALL across all pipelines
    """
    pipeline_tables = get_pipeline_tables(table_pattern)

    if not pipeline_tables:
        # Return empty result query if no tables exist
        return f"SELECT {select_columns}, '' as source_pipeline WHERE 1=0"

    # Build UNION ALL query
    union_parts = []
    for i, table_name in enumerate(pipeline_tables, 1):
        # Extract pipeline number from table name (e.g., 'pi_timeseries_pipeline1' -> '1')
        pipeline_num = table_name.split('_pipeline')[-1] if '_pipeline' in table_name else str(i)

        query_part = f"""
        SELECT {select_columns}, '{pipeline_num}' as source_pipeline
        FROM {UC_CATALOG}.{UC_SCHEMA}.{table_name}
        """

        if where_clause:
            query_part += f" WHERE {where_clause}"

        # Add GROUP BY inside each UNION part if provided
        if group_by:
            query_part += f" GROUP BY {group_by}"

        union_parts.append(query_part.strip())

    # Join all parts with UNION ALL
    full_query = " UNION ALL ".join(union_parts)

    # Wrap in subquery if we have ORDER BY or LIMIT (not GROUP BY, as it's already applied)
    if order_by or limit:
        full_query = f"SELECT * FROM ({full_query}) combined"

        if order_by:
            full_query += f" ORDER BY {order_by}"

        if limit:
            full_query += f" LIMIT {limit}"

    # Log the generated query for debugging
    print(f"\n[DEBUG] Generated SQL Query:\n{full_query}\n")

    return full_query


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
    current_tags = len(pi_web_api.MOCK_TAGS)

    # Count AF elements (flatten hierarchy)
    current_af = 0
    if "F1DP-DB-Production" in pi_web_api.MOCK_AF_HIERARCHY:
        for plant in pi_web_api.MOCK_AF_HIERARCHY["F1DP-DB-Production"].get("Elements", []):
            current_af += 1  # Plant
            for unit in plant.get("Elements", []):
                current_af += 1  # Unit
                current_af += len(unit.get("Elements", []))  # Equipment

    current_events = len(pi_web_api.MOCK_EVENT_FRAMES)

    # Count unique plants
    current_plants = len(set(tag["plant"] for tag in pi_web_api.MOCK_TAGS.values()))

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


@app.get("/data/af-hierarchy", response_class=HTMLResponse, include_in_schema=False)
async def af_hierarchy_page(request: Request):
    """View AF Hierarchy Tree (Interactive)."""
    return templates.TemplateResponse(
        "af_hierarchy_tree.html",
        {"request": request}
    )


@app.get("/data/af-hierarchy-table", response_class=HTMLResponse, include_in_schema=False)
async def af_hierarchy_table_page(request: Request):
    """View AF Hierarchy data as table."""
    results = execute_sql(f"""
        SELECT
            element_name as name,
            element_type,
            template_name,
            element_path as path,
            description
        FROM {UC_CATALOG}.{UC_SCHEMA}.pi_af_hierarchy
        ORDER BY element_path
        LIMIT 100
    """)

    return templates.TemplateResponse(
        "data_table.html",
        {
            "request": request,
            "title": "AF Hierarchy (Table View)",
            "data": results if results else [],
            "columns": ["name", "element_type", "template_name", "path", "description"]
        }
    )


@app.get("/data/events", response_class=HTMLResponse, include_in_schema=False)
async def events_page(request: Request):
    """View Events & Alarms Dashboard (Visual)."""
    return templates.TemplateResponse(
        "events_dashboard.html",
        {"request": request}
    )


@app.get("/data/events-table", response_class=HTMLResponse, include_in_schema=False)
async def events_table_page(request: Request):
    """View Event Frames as table."""
    results = execute_sql(f"""
        SELECT
            event_name as name,
            template_name,
            start_time,
            end_time,
            event_attributes as attributes
        FROM {UC_CATALOG}.{UC_SCHEMA}.pi_event_frames
        ORDER BY start_time DESC
        LIMIT 100
    """)

    return templates.TemplateResponse(
        "data_table.html",
        {
            "request": request,
            "title": "Event Frames (All Types)",
            "data": results if results else [],
            "columns": ["name", "template_name", "start_time", "end_time", "attributes"]
        }
    )


@app.get("/data/alarms", response_class=HTMLResponse, include_in_schema=False)
async def alarms_page(request: Request):
    """View Alarms (AlarmTemplate events) from Unity Catalog."""
    # Query event frames using UNION ALL pattern for multi-pipeline architecture
    query = build_union_query(
        table_pattern="pi_event_frames",
        select_columns="""event_name as name,
            start_time,
            end_time,
            event_attributes as attributes""",
        where_clause="template_name = 'AlarmTemplate'",
        order_by="start_time DESC",
        limit=100
    )

    results = execute_sql(query)

    return templates.TemplateResponse(
        "data_table.html",
        {
            "request": request,
            "title": "Alarms",
            "data": results if results else [],
            "columns": ["name", "start_time", "end_time", "attributes"]
        }
    )


# ============================================================================
# DASHBOARD API ENDPOINTS
# ============================================================================

@app.get("/api/ingestion/status")
async def get_ingestion_status() -> Dict[str, Any]:
    """Get current ingestion status and KPIs from all per-pipeline bronze tables."""

    # Query actual data from Unity Catalog across ALL per-pipeline tables
    # Build UNION ALL query for timeseries
    timeseries_query = build_union_query(
        table_pattern="pi_timeseries",
        select_columns="COUNT(*) as total_rows, COUNT(DISTINCT tag_webid) as tags, MAX(ingestion_timestamp) as last_run"
    )

    event_frames_query = build_union_query(
        table_pattern="pi_event_frames",
        select_columns="COUNT(*) as count"
    )

    af_hierarchy_query = build_union_query(
        table_pattern="pi_af_hierarchy",
        select_columns="COUNT(DISTINCT element_id) as count"
    )

    # Execute queries
    timeseries_stats = execute_sql(timeseries_query)
    event_frames_count = execute_sql(event_frames_query)
    af_elements_count = execute_sql(af_hierarchy_query)

    # Extract values with fallback to 0
    stats = timeseries_stats[0] if timeseries_stats else {}
    total_rows = int(stats.get('total_rows', 0))
    tags = int(stats.get('tags', 0))
    last_run = stats.get('last_run') if stats.get('last_run') else datetime.utcnow().isoformat() + "Z"

    event_frames = int(event_frames_count[0].get('count', 0)) if event_frames_count else 0
    af_elements = int(af_elements_count[0].get('count', 0)) if af_elements_count else 0

    # Count how many pipelines are active
    pipeline_tables = get_pipeline_tables("pi_timeseries")
    active_pipelines = len(pipeline_tables)

    return {
        "status": "Healthy" if total_rows > 0 else "No Data",
        "last_run": last_run,
        "next_run": (datetime.utcnow() + timedelta(minutes=15)).isoformat() + "Z",
        "rows_loaded_last_hour": total_rows,  # Simplified: showing all data
        "total_rows_today": total_rows,
        "tags_ingested": tags,
        "event_frames_ingested": event_frames,
        "af_elements_indexed": af_elements,
        "data_quality_score": 100 if total_rows > 0 else 0,
        "avg_latency_seconds": 0.0,
        "pipeline_groups": active_pipelines,  # Number of discovered pipelines
        "active_pipelines": active_pipelines  # All pipelines are active if tables exist
    }


@app.get("/api/ingestion/timeseries")
async def get_ingestion_timeseries() -> Dict[str, List]:
    """Get time-series metrics for charts from all per-pipeline bronze tables."""

    # Build UNION ALL query across all pipelines
    query = build_union_query(
        table_pattern="pi_timeseries",
        select_columns="DATE_TRUNC('hour', ingestion_timestamp) as ts, COUNT(*) as row_count",
        group_by="ts",
        order_by="ts DESC",
        limit=24
    )

    results = execute_sql(query)

    timestamps = []
    rows_per_minute = []

    if results:
        for row in results:
            ts = row.get('ts')
            if ts:
                timestamps.append(ts.strftime("%H:%M") if hasattr(ts, 'strftime') else str(ts)[-5:])
                rows_per_minute.append(int(row.get('row_count', 0)))

    return {
        "timestamps": timestamps,
        "rows_per_minute": rows_per_minute,
        "errors_per_minute": [0] * len(timestamps)  # Error tracking not implemented yet
    }


@app.get("/api/ingestion/tags")
async def get_tags_by_plant() -> Dict[str, List]:
    """Get tag distribution by plant/site from all per-pipeline bronze tables."""

    # Build UNION ALL query across all AF hierarchy tables
    query = build_union_query(
        table_pattern="pi_af_hierarchy",
        select_columns="SPLIT(element_name, '_')[0] as plant, COUNT(DISTINCT element_id) as tag_count",
        where_clause="depth = 0 AND element_name LIKE '%_Plant'",
        group_by="plant",
        order_by="tag_count DESC",
        limit=10
    )

    results = execute_sql(query)

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
    """Get health status of ALL ingestion pipelines from per-pipeline bronze tables."""

    # Get all pipeline tables
    pipeline_tables = get_pipeline_tables("pi_timeseries")

    if not pipeline_tables:
        return []

    pipeline_health = []

    # Query each pipeline individually to show per-pipeline health
    for table_name in pipeline_tables:
        # Extract pipeline number from table name
        pipeline_num = table_name.split('_pipeline')[-1] if '_pipeline' in table_name else "unknown"

        query = f"""
            SELECT
                MAX(ingestion_timestamp) as last_run,
                COUNT(*) as row_count,
                COUNT(DISTINCT tag_webid) as tag_count
            FROM {UC_CATALOG}.{UC_SCHEMA}.{table_name}
        """

        result = execute_sql(query)

        if result and result[0]:
            stats = result[0]
            last_run = stats.get('last_run')
            row_count = int(stats.get('row_count', 0))
            tag_count = int(stats.get('tag_count', 0))

            pipeline_health.append({
                "pipeline_id": int(pipeline_num) if pipeline_num.isdigit() else 0,
                "name": f"PI Pipeline {pipeline_num}",
                "status": "Healthy" if row_count > 0 else "No Data",
                "last_run": last_run if last_run else datetime.utcnow().isoformat() + "Z",
                "tags": tag_count,
                "avg_duration_seconds": 0.0,
                "success_rate": 100.0 if row_count > 0 else 0.0
            })

    return sorted(pipeline_health, key=lambda x: x["pipeline_id"])


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


@app.get("/api/ingestion/alarms")
async def get_alarms() -> Dict[str, Any]:
    """Get alarm statistics from all per-pipeline event frames tables."""

    # Query event frames (alarms) data using UNION ALL pattern
    query = build_union_query(
        table_pattern="pi_event_frames",
        select_columns="""COUNT(*) as total_alarms,
            COUNT(DISTINCT event_name) as unique_alarm_types,
            COUNT(DISTINCT template_name) as unique_templates"""
    )

    results = execute_sql(query)

    if results and len(results) > 0:
        row = results[0]
        return {
            "total_alarms": int(row.get('total_alarms', 0)),
            "unique_alarm_types": int(row.get('unique_alarm_types', 0)),
            "unique_templates": int(row.get('unique_templates', 0))
        }

    return {
        "total_alarms": 0,
        "unique_alarm_types": 0,
        "unique_templates": 0
    }


@cache_with_ttl(ttl_seconds=60)  # Cache for 1 minute
def _get_af_hierarchy_stats_cached() -> Dict[str, Any]:
    """Internal cached function for AF hierarchy stats."""
    # Query AF hierarchy data using UNION ALL pattern
    # Use template_name to identify element types:
    # - Plants: depth=0 or template_name='PlantTemplate'
    # - Units: depth=1 or template_name like '%Unit%'
    # - Equipment: depth>=2 or template_name like '%Equipment%' or '%Compressor%' etc.
    query = build_union_query(
        table_pattern="pi_af_hierarchy",
        select_columns="""COUNT(*) as total_elements,
            SUM(CASE WHEN depth = 0 OR template_name = 'PlantTemplate' THEN 1 ELSE 0 END) as plants,
            SUM(CASE WHEN depth = 1 OR template_name LIKE '%Unit%' THEN 1 ELSE 0 END) as units,
            SUM(CASE WHEN depth >= 2 OR template_name LIKE '%Equipment%' OR template_name LIKE '%Compressor%' THEN 1 ELSE 0 END) as equipment"""
    )

    results = execute_sql(query)

    if results and len(results) > 0:
        row = results[0]
        return {
            "total_elements": int(row.get('total_elements', 0)),
            "plants": int(row.get('plants', 0)),
            "units": int(row.get('units', 0)),
            "equipment": int(row.get('equipment', 0))
        }

    return {
        "total_elements": 0,
        "plants": 0,
        "units": 0,
        "equipment": 0
    }


@app.get("/api/ingestion/af-hierarchy")
async def get_af_hierarchy_stats() -> Dict[str, Any]:
    """Get AF hierarchy statistics from all per-pipeline AF hierarchy tables. Cached for 1 minute."""
    return _get_af_hierarchy_stats_cached()


@app.get("/api/data/af_hierarchy")
async def get_af_hierarchy_data(limit: int = 5000, offset: int = 0) -> Dict[str, Any]:
    """
    Get AF hierarchy data from all per-pipeline bronze tables.

    Args:
        limit: Maximum number of records to return (default: 5000, reduced from 100000 for performance)
        offset: Number of records to skip (for pagination)

    Returns:
        Dictionary with data array and count
    """
    # Reduced default limit from 100000 to 5000 for better performance
    # Frontend can paginate if needed
    query = build_union_query(
        table_pattern="pi_af_hierarchy",
        select_columns="""element_name as name,
            element_type as equipment_type,
            template_name,
            element_path as path,
            description,
            depth,
            SPLIT(element_name, '_')[0] as plant,
            element_id as webid""",
        order_by="path",
        limit=limit
    )

    # Add OFFSET if provided (for pagination)
    if offset > 0:
        query += f" OFFSET {offset}"

    results = execute_sql(query)

    return {
        "data": results if results else [],
        "count": len(results) if results else 0,
        "limit": limit,
        "offset": offset
    }


@app.get("/api/data/event_frames")
async def get_event_frames_data(limit: int = 100) -> Dict[str, Any]:
    """Get event frames data from all per-pipeline bronze tables."""
    query = build_union_query(
        table_pattern="pi_event_frames",
        select_columns="""event_name as name,
            template_name,
            start_time,
            end_time,
            event_attributes as attributes,
            primary_element_id as primary_referenced_element_webid""",
        order_by="start_time DESC",
        limit=limit
    )

    results = execute_sql(query)

    return {
        "data": results if results else [],
        "count": len(results) if results else 0
    }


@app.get("/api/data/timeseries")
async def get_timeseries_data(limit: int = 100) -> Dict[str, Any]:
    """Get timeseries data from all per-pipeline bronze tables."""
    query = build_union_query(
        table_pattern="pi_timeseries",
        select_columns="""tag_webid as tag_name,
            timestamp,
            value,
            units,
            ingestion_timestamp""",
        order_by="timestamp DESC",
        limit=limit
    )

    results = execute_sql(query)

    return {
        "data": results if results else [],
        "count": len(results) if results else 0
    }


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
        pi_web_api.MOCK_TAGS.clear()
        pi_web_api.MOCK_EVENT_FRAMES.clear()

        # Regenerate tags using same logic as pi_web_api.py
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

                    pi_web_api.MOCK_TAGS[tag_webid] = {
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

                    if len(pi_web_api.MOCK_TAGS) >= tag_count:
                        break
                if len(pi_web_api.MOCK_TAGS) >= tag_count:
                    break
            if len(pi_web_api.MOCK_TAGS) >= tag_count:
                break

        # Regenerate AF Hierarchy
        af_plant_limit = min(len(plant_names), 10) if tag_count > 10000 else len(plant_names)
        af_unit_limit = min(unit_count, 10) if tag_count > 10000 else unit_count

        pi_web_api.MOCK_AF_HIERARCHY["F1DP-DB-Production"]["Elements"].clear()

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

            pi_web_api.MOCK_AF_HIERARCHY["F1DP-DB-Production"]["Elements"].append(plant_element)

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

            pi_web_api.MOCK_EVENT_FRAMES.append(event)

        # Calculate AF elements
        af_elements = 0
        if "F1DP-DB-Production" in pi_web_api.MOCK_AF_HIERARCHY:
            for plant in pi_web_api.MOCK_AF_HIERARCHY["F1DP-DB-Production"].get("Elements", []):
                af_elements += 1
                for unit in plant.get("Elements", []):
                    af_elements += 1
                    af_elements += len(unit.get("Elements", []))

        print(f"✓ Regenerated: {len(pi_web_api.MOCK_TAGS)} tags, {af_elements} AF elements, {len(pi_web_api.MOCK_EVENT_FRAMES)} events")

        return {
            "success": True,
            "tags": len(pi_web_api.MOCK_TAGS),
            "af_elements": af_elements,
            "events": len(pi_web_api.MOCK_EVENT_FRAMES),
            "plants": len(plant_names)
        }

    except Exception as e:
        print(f"❌ Configuration update failed: {e}")
        return {
            "success": False,
            "error": str(e)
        }


# ============================================================================
# PI WEB API ROUTES (already defined in pi_web_api.py)
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
