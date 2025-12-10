# OSI PI Lakeflow Connector

Production-ready Databricks solution for OSI PI data ingestion with load-balanced DLT pipelines and real-time monitoring.

## Quick Start

### For Operators

1. **Configure pipelines**: Open `notebooks/generate_pipelines_from_mock_api.py`
   - Set `INGESTION_MODE` (batch or streaming)
   - Set `TAGS_PER_PIPELINE` for load balancing
   - Run notebook to generate configurations

2. **Deploy pipelines**: Open `notebooks/deploy_pipelines.py`
   - Run notebook to validate and deploy
   - Pipelines will be created in Databricks workspace

3. **Monitor ingestion**: Visit the dashboard
   - Dashboard: https://osipi-webserver-xxx.aws.databricksapps.com/ingestion
   - View AF hierarchy, events, alarms via UI links

See [OPERATOR_GUIDE.md](OPERATOR_GUIDE.md) for complete instructions.

### For Developers

```bash
# Install dependencies
pip install -r requirements.txt

# Start mock PI server + dashboard
cd databricks-app
MOCK_PI_TAG_COUNT=1040 python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## What It Does

### Data Ingestion
- **Timeseries data**: Sensor readings with timestamps
- **AF Hierarchy**: Asset Framework structure
- **Event Frames**: Process events (including alarms)

### Load Balancing
- Automatically distributes tags across multiple DLT pipelines
- Configurable tags per pipeline
- Supports batch (scheduled) or streaming (continuous) modes

### Monitoring
- Real-time dashboard showing ingestion stats
- UI pages for viewing AF hierarchy, events, and alarms
- Metrics from Unity Catalog Delta tables

## Architecture

```
Mock PI Web API (Databricks App)
  ↓ HTTP REST API
  ↓
DLT Pipelines (auto-generated, load-balanced)
  ↓
Unity Catalog Delta Tables
  ├── osipi.bronze.pi_timeseries
  ├── osipi.bronze.pi_af_hierarchy
  └── osipi.bronze.pi_event_frames
```

## Project Structure

```
osipi-connector/
│
├── 📋 Operator Workflow
│   ├── notebooks/
│   │   ├── generate_pipelines_from_mock_api.py  ⭐ Configure & generate pipelines
│   │   ├── deploy_pipelines.py                  ⭐ Deploy via DAB
│   │   └── ingest_from_mock_api.py             (Optional) Manual test ingestion
│   │
│   └── OPERATOR_GUIDE.md                         ⭐ Step-by-step guide
│
├── 🔧 DLT Pipeline Code
│   └── src/notebooks/
│       └── pi_ingestion_pipeline.py              ⭐ DLT pipeline (executed by pipelines)
│
├── 🏗️ Core Modules (used by DLT pipeline)
│   ├── src/connector/
│   │   └── pi_lakeflow_connector.py             Main connector orchestration
│   ├── src/auth/
│   │   └── pi_auth_manager.py                   Authentication
│   ├── src/client/
│   │   └── pi_web_api_client.py                 HTTP client
│   ├── src/extractors/
│   │   ├── timeseries_extractor.py              Timeseries extraction
│   │   ├── af_extractor.py                      AF hierarchy extraction
│   │   └── event_frame_extractor.py             Event frames extraction
│   ├── src/checkpoints/
│   │   └── checkpoint_manager.py                Incremental state tracking
│   └── src/writers/
│       └── delta_writer.py                      Delta Lake writer
│
├── 🖥️ Mock PI Server & Dashboard
│   └── databricks-app/
│       ├── app/
│       │   ├── main.py                          ⭐ FastAPI server + dashboard
│       │   └── templates/
│       │       ├── pi_home.html                 API home page
│       │       ├── ingestion.html               Ingestion metrics dashboard
│       │       └── data_table.html              Data viewer pages
│       │
│       ├── app.yaml                             Databricks App config
│       └── create_tables.py                     Table creation script
│
├── 🚀 DAB Configuration
│   ├── databricks.yml                           ⭐ Main DAB configuration
│   └── deployment/resources/
│       ├── pipelines.yml                        (Auto-generated) DLT pipelines
│       └── jobs.yml                             (Auto-generated) Scheduled jobs
│
├── 🧪 Tests (for development)
│   └── tests/
│       ├── test_*.py                            Unit tests
│       └── mock_pi_server.py                    Standalone mock server
│
├── 📚 Documentation
│   ├── README.md                                ⭐ This file
│   └── OPERATOR_GUIDE.md                        ⭐ Operator instructions
│
└── 📦 Configuration
    ├── requirements.txt                         ⭐ Python dependencies
    └── .gitignore                               Git ignore patterns
```

## Data Flow

1. **Tag Discovery**
   - Pipeline generator notebook queries mock PI Web API
   - Discovers all tags (e.g., 1,040 tags)

2. **Load Balancing**
   - Distributes tags into pipeline groups (e.g., 100 tags per pipeline = 11 pipelines)
   - Generates YAML configurations for DAB

3. **Deployment**
   - DAB creates DLT pipelines in Databricks
   - For batch: Creates scheduled jobs
   - For streaming: Pipelines run continuously

4. **Ingestion**
   - Each pipeline ingests its assigned tags
   - Data written to Unity Catalog Delta tables
   - Checkpoints track incremental progress

5. **Monitoring**
   - Dashboard queries Unity Catalog tables
   - Shows real-time ingestion metrics
   - UI pages display AF hierarchy, events, alarms

## Ingestion Modes

### Batch Mode (Scheduled)
- Runs on schedule (e.g., every 30 minutes)
- Cost-effective (pay only when running)
- Good for: Periodic data refresh

**Configuration:**
```python
INGESTION_MODE = "batch"
DEFAULT_BATCH_SCHEDULE = "0 */30 * * * ?"  # Every 30 minutes
```

### Streaming Mode (Continuous)
- Runs 24/7 continuously
- Real-time data ingestion
- Good for: Low-latency requirements

**Configuration:**
```python
INGESTION_MODE = "streaming"
```

## Key Features

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

## Support

- **Operator Guide**: See [OPERATOR_GUIDE.md](OPERATOR_GUIDE.md)
- **Issues**: Create a GitHub issue
- **Questions**: Contact the development team

## License

Copyright 2025 Databricks. All rights reserved.
