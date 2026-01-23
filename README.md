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
- ✅ **Load-balanced pipelines**: Auto-distributes tags across multiple DLT pipelines
- ✅ **Serverless compute**: All pipelines use serverless (no cluster management)
- ✅ **Batch & streaming**: Switch modes with one config change
- ✅ **Incremental ingestion**: Only fetches new data since last checkpoint
- ✅ **Real-time dashboard**: Monitor ingestion metrics live
- ✅ **Data viewers**: UI pages for AF hierarchy, events, alarms
- ✅ **Unity Catalog**: All data stored in governed Delta tables

## Unity Catalog Tables

### osipi.bronze.pi_timeseries
```sql
SELECT * FROM osipi.bronze.pi_timeseries LIMIT 100;
```
Columns: tag_name, tag_webid, timestamp, value, units, quality_*, ingestion_timestamp

### osipi.bronze.pi_af_hierarchy
```sql
SELECT * FROM osipi.bronze.pi_af_hierarchy;
```
Columns: name, element_type, template_name, path, description, parent_element_webid

### osipi.bronze.pi_event_frames
```sql
-- All events
SELECT * FROM osipi.bronze.pi_event_frames;

-- Alarms only
SELECT * FROM osipi.bronze.pi_event_frames
WHERE template_name = 'AlarmTemplate';
```
Columns: name, template_name, start_time, end_time, attributes, primary_referenced_element_webid

## Configuration

All operator configuration is in `notebooks/generate_pipelines_from_mock_api.py`:

```python
# Ingestion mode
INGESTION_MODE = "batch"  # or "streaming"

# Load balancing
TAGS_PER_PIPELINE = 100  # Tags per pipeline

# Batch schedule (only for batch mode)
DEFAULT_BATCH_SCHEDULE = "0 */30 * * * ?"  # Every 30 minutes

# Target location
TARGET_CATALOG = "osipi"
TARGET_SCHEMA = "bronze"

# API endpoint
MOCK_API_URL = "https://osipi-webserver-xxx.aws.databricksapps.com"
```

## Dashboard URLs

**Deployed Databricks App:**
- Home: https://osipi-webserver-1444828305810485.aws.databricksapps.com/
- Dashboard: https://osipi-webserver-1444828305810485.aws.databricksapps.com/ingestion
- AF Hierarchy: https://osipi-webserver-1444828305810485.aws.databricksapps.com/data/af-hierarchy
- All Events: https://osipi-webserver-1444828305810485.aws.databricksapps.com/data/events
- Alarms: https://osipi-webserver-1444828305810485.aws.databricksapps.com/data/alarms

## Troubleshooting

### Common Issues

1. **Empty AF Hierarchy or Event Frames tables**
   - Check that DLT pipeline ran successfully
   - Verify OAuth authentication is working
   - See [docs/troubleshooting/](docs/troubleshooting/) for detailed fixes

2. **Hierarchy tree not expanding**
   - Clear browser cache and reload
   - Verify Databricks App has been redeployed with latest code

3. **Pipeline generation fails**
   - Ensure `pyyaml` is installed: `%pip install pyyaml`
   - Restart Python kernel: `dbutils.library.restartPython()`

4. **Column name errors in queries**
   - Check that table schemas match expected columns
   - See [docs/CHANGELOG.md](docs/CHANGELOG.md) for recent fixes

### Documentation

- **Operator Guide**: [OPERATOR_GUIDE.md](OPERATOR_GUIDE.md)
- **Changelog**: [docs/CHANGELOG.md](docs/CHANGELOG.md)
- **Troubleshooting Guides**: [docs/troubleshooting/](docs/troubleshooting/)

## Current Status

Production-ready with all features working:
- ✅ Timeseries ingestion: 1.28M rows (128 tags)
- ✅ AF Hierarchy: 84 elements
- ✅ Event Frames: 50 events
- ✅ Last ingestion: 2025-12-12 (data is fresh)
- ✅ Dashboard UI: Fully functional with hierarchy tree expansion

## Support

- **Issues**: Create a GitHub issue
- **Questions**: Contact the development team

## License

Copyright 2025 Databricks. All rights reserved.
