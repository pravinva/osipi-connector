# Mock PI Web API Server

A lightweight FastAPI server that **mimics a subset of AVEVA/OSI PI Web API** for development, demos, and pagination/load testing.

This repo is **only** the mock server (no Databricks Apps, no Lakeflow connector code).

## Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

uvicorn mock_piwebapi.main:app --host 0.0.0.0 --port 8000
```

Test:

```bash
curl -H "Authorization: Bearer test-token" http://localhost:8000/health
curl -H "Authorization: Bearer test-token" http://localhost:8000/piwebapi
curl -H "Authorization: Bearer test-token" http://localhost:8000/piwebapi/assetservers
```

## Deploy to Google Cloud Run

See `GCP_DEPLOYMENT.md`.

The service is deployed from source using:

```bash
gcloud run deploy mock-piwebapi --source . --region us-central1 --project <project-id> --allow-unauthenticated
```

## Authentication

All endpoints require `Authorization: Bearer <token>`.

- If `EXPECTED_BEARER_TOKEN` is set (recommended in Cloud Run), the token must match exactly.
- If not set, any **non-empty** bearer token is accepted.

## Code layout

```text
mock_piwebapi/
  main.py        # auth wrapper + Cloud Run entrypoint
  pi_web_api.py  # PI Web API mock endpoints
Dockerfile
requirements.txt
GCP_DEPLOYMENT.md
```

## What’s mocked (data model)

The server generates synthetic PI-like objects **in memory** and serves them via PI Web API-style JSON.

- **PI Points (tags)**: 10 plants × 125 units/plant × 8 sensor types = **10,000 tags**
- **AF hierarchy**: `ProductionDB` → Plants → Units → Equipment
- **Event frames**: **250** event frames (50/plant) over ~30 days with templates:
  - `BatchRunTemplate`, `MaintenanceTemplate`, `AlarmTemplate`, `DowntimeTemplate`

The server is **stateless**: time-series values are generated on-demand for the requested time window.

## Time & paging semantics (important)

- **Time strings**:
  - `"*"` = now
  - `"*-10m"`, `"*-2h"`, `"*-7d"` = relative to now
  - ISO timestamps (with or without `Z`)
- **Paging**:
  - `GET /piwebapi/dataservers/{server_webid}/points` supports `startIndex` + `maxCount`
  - Stream endpoints support `maxCount` as a hard cap on returned items

## Implemented API surface (42 routes)

All routes are implemented in `mock_piwebapi/pi_web_api.py`.

### Health
- **GET** `/health`

### Root + inventory
- **GET** `/piwebapi`
- **GET** `/piwebapi/dataservers`
- **GET** `/piwebapi/uoms`

### Points (tags)
- **GET** `/piwebapi/dataservers/{server_webid}/points` (query: `nameFilter`, `startIndex`, `maxCount`)
- **GET** `/piwebapi/points/{point_webid}/attributes`

### Streams (single tag)
- **GET** `/piwebapi/streams/{webid}/recorded` (query: `startTime`, `endTime`, `maxCount`)
- **GET** `/piwebapi/streams/{webid}/interpolated` (query: `startTime`, `endTime`, `interval`, `maxCount`)
- **GET** `/piwebapi/streams/{webid}/calculated` (query: `startTime`, `endTime`, `interval`, `calculationType`)
- **GET** `/piwebapi/streams/{webid}/plot` (query: `startTime`, `endTime`, `intervals`)
- **GET** `/piwebapi/streams/{webid}/recordedattime` (query: `time`)
- **GET** `/piwebapi/streams/{webid}/end`
- **GET** `/piwebapi/streams/{webid}/value` (query: `time`)
- **GET** `/piwebapi/streams/{webid}/summary` (query: `startTime`, `endTime`, `summaryType=Total|Count`)

### Streamsets (multi-tag fanout)
- **GET** `/piwebapi/streamsets/recorded` (query: `webId=...` repeated, plus `startTime`, `endTime`, `maxCount`)
- **GET** `/piwebapi/streamsets/interpolated` (query: `webId=...` repeated, plus `startTime`, `endTime`, `interval`, `maxCount`)
- **GET** `/piwebapi/streamsets/summary` (query: `webId=...` repeated, plus `startTime`, `endTime`, `summaryType`)
- **GET** `/piwebapi/streamsets/plot` (query: `webId=...` repeated, plus `startTime`, `endTime`, `intervals`)
- **GET** `/piwebapi/streamsets/end` (query: `webId=...` repeated)

### Batch (limited subset)
- **POST** `/piwebapi/batch`
  - Supports a subset of stream resources (recorded/interpolated/end/value/summary).

### AF (Asset Framework)
- **GET** `/piwebapi/assetservers`
- **GET** `/piwebapi/assetservers/{server_webid}/assetdatabases`
- **GET** `/piwebapi/assetdatabases`
- **GET** `/piwebapi/assetdatabases/{db_webid}/categories`
- **GET** `/piwebapi/assetdatabases/{db_webid}/tables`
- **GET** `/piwebapi/tables/{table_webid}/rows`
- **GET** `/piwebapi/assetdatabases/{db_webid}/elements`
- **GET** `/piwebapi/assetdatabases/{db_webid}/elementtemplates`
- **GET** `/piwebapi/elementtemplates/{template_webid}/attributetemplates`
- **GET** `/piwebapi/elements/{element_webid}`
- **GET** `/piwebapi/elements/{element_webid}/elements`
- **GET** `/piwebapi/elements/{element_webid}/attributes`
- **GET** `/piwebapi/assetdatabases/{db_webid}/analyses`
- **GET** `/piwebapi/assetdatabases/{db_webid}/analysistemplates`

### Event Frames
- **GET** `/piwebapi/assetdatabases/{db_webid}/eventframetemplates`
- **GET** `/piwebapi/eventframetemplates/{template_webid}/attributetemplates`
- **GET** `/piwebapi/assetdatabases/{db_webid}/eventframes` (query: `startTime`, `endTime`, `searchMode`, `maxCount`)
- **GET** `/piwebapi/eventframes/{ef_webid}/annotations`
- **GET** `/piwebapi/eventframes/{ef_webid}/acknowledgements`
- **GET** `/piwebapi/eventframes/{ef_webid}/referencedelements`
- **GET** `/piwebapi/eventframes/{ef_webid}/attributes`
- **GET** `/piwebapi/attributes/{attr_webid}/value` (for attribute WebIds returned above)

## Design notes

- The mock server is intentionally **not** a full PI Web API implementation; it provides the endpoints needed for testing client code, pagination, and time-window behavior.
- Responses include PI-like shapes (`Items`, `WebId`, `Links`) but `Links` may contain placeholder hosts.

## License

Copyright 2025 Databricks. All rights reserved.
