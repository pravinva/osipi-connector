# Data Sources: Real vs Mock

This document shows which endpoints query real Unity Catalog data vs mock/hardcoded data.

## ✅ REAL DATA - Querying Unity Catalog

These endpoints query actual Unity Catalog tables created by the Lakeflow connector:

| Endpoint | Data Source | SQL Query |
|----------|-------------|-----------|
| `/api/ingestion/status` | osipi.bronze.pi_timeseries | `SELECT COUNT(*), MAX(ingestion_timestamp), quality stats` |
| `/api/ingestion/tags` | osipi.bronze.pi_timeseries | `SELECT sensor_type, COUNT(DISTINCT tag_webid)` |
| `/api/ingestion/timeseries` | osipi.gold.pi_metrics_hourly | `SELECT hour, AVG(value) FROM gold table` |
| `/api/ingestion/pipeline_health` | osipi.bronze.pi_timeseries | `SELECT COUNT(tags), MAX(timestamp), quality %` |
| `/ingestion` (Dashboard) | Displays data from above 4 endpoints | Renders HTML with Chart.js visualizations |

## ✅ REAL DATA - From Mock PI Server

These endpoints query real alarm event frames from the PI Web API mock server:

| Endpoint | Data Source | Details |
|----------|-------------|---------|
| `/api/ingestion/recent_events` | MOCK_EVENT_FRAMES (50 events) | Alarm events with Priority, AlarmType, timestamps |

## ❌ MOCK DATA - Hardcoded/Static

These endpoints return hardcoded data or serve as mock PI Web API endpoints:

| Endpoint | Data Type | Purpose |
|----------|-----------|---------|
| `/piwebapi` | Mock JSON | PI Web API root endpoint (data source, not destination) |
| `/piwebapi/assetdatabases` | Mock JSON | Lists 2 mock asset databases |
| `/piwebapi/dataservers` | Mock JSON | Lists 1 mock data server |
| `/piwebapi/assetdatabases/{db_webid}/points` | Mock JSON | 10,000 mock PI tags (25 plants × 50 units × 8 sensors) |
| `/piwebapi/streams/{webid}/recorded` | Mock JSON | Time-series data for a specific tag |
| `/piwebapi/assetdatabases/{db_webid}/eventframes` | Mock JSON | 50 alarm event frames |
| `/health` | Status check | Simple operational status |

## ⚪ NOT CONNECTED YET - But Available

These components exist but are not scheduled/running:

| Component | Status | How to Connect |
|-----------|--------|----------------|
| Databricks Workflow Job | Not scheduled | Run `python create_lakeflow_job.py` to schedule |
| Lakeflow Connection UI | Custom connector (won't show) | Custom connectors don't appear in Connections UI |
| Checkpoint table | Empty | Will populate when notebook runs in Databricks |

## Summary

**5 endpoints** query real Unity Catalog data (bronze + gold tables)
**1 endpoint** queries real alarm event frames from PI server
**7 endpoints** serve as mock PI Web API (data source)
**3 components** exist but need to be scheduled/triggered

## How to Verify

Run this script to test all endpoints:
```bash
bash /tmp/check_endpoints.sh
```

Or test individually:
```bash
# Real UC data
curl http://localhost:8010/api/ingestion/status | jq
curl http://localhost:8010/api/ingestion/tags | jq
curl http://localhost:8010/api/ingestion/timeseries | jq
curl http://localhost:8010/api/ingestion/pipeline_health | jq

# Real alarm data
curl http://localhost:8010/api/ingestion/recent_events | jq

# Mock PI server
curl http://localhost:8010/piwebapi | jq
curl http://localhost:8010/piwebapi/assetdatabases | jq
```
