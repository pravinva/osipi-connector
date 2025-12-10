# Databricks App Authentication Limitation

## The Problem

**Databricks Apps use browser-based OAuth authentication which CANNOT be called from notebooks.**

When a notebook tries to call a Databricks App URL:
```python
response = requests.get("https://osipi-webserver-...databricksapps.com/piwebapi/dataservers")
# Returns: HTML login page (status 200)
# Expected: JSON data
# Error: JSONDecodeError: Expecting value: line 1 column 1 (char 0)
```

## Why WorkspaceClient Doesn't Work

The documentation says to use:
```python
from databricks.sdk import WorkspaceClient
wc = WorkspaceClient()
headers = wc.config.authenticate()
```

**But this only works when:**
- ✅ One Databricks App calls another Databricks App
- ✗ Regular notebook calls a Databricks App

Regular notebooks run on clusters, not as Databricks Apps, so they don't have the service principal context required.

## Solutions

### Solution 1: Direct Database Writes (RECOMMENDED)

Instead of HTTP API → Notebook → Database, do:
**Mock Server → Directly Write to Unity Catalog**

Modify the Databricks App to write data directly:

```python
# In databricks-app/app/main.py
from databricks.sdk import WorkspaceClient

@app.post("/api/populate_bronze")
async def populate_bronze_tables():
    """Directly populate osipi.bronze tables from mock data"""
    w = WorkspaceClient()

    # Generate data
    timeseries_data = []
    for tag in mock_pi_server.MOCK_TAGS.values():
        # ... generate data ...
        timeseries_data.append({...})

    # Write directly to Unity Catalog via SQL
    insert_query = f"""
    INSERT INTO osipi.bronze.pi_timeseries
    VALUES {','.join(timeseries_data)}
    """

    w.statement_execution.execute_statement(
        statement=insert_query,
        warehouse_id=WAREHOUSE_ID
    )

    return {"success": True, "records": len(timeseries_data)}
```

Then call from browser or notebook:
```python
# Notebook can call this once to populate data
import requests
requests.post("https://osipi-webserver-.../api/populate_bronze")
```

### Solution 2: Scheduled Job in App

Add a scheduled task within the Databricks App itself:

```python
# databricks-app/app/main.py
from apscheduler.schedulers.asyncio import AsyncIOScheduler

scheduler = AsyncIOScheduler()

@scheduler.scheduled_job('interval', minutes=15)
async def auto_populate_bronze():
    """Auto-populate bronze tables every 15 minutes"""
    # Generate and insert data directly
    pass

scheduler.start()
```

### Solution 3: Public API Endpoints (NOT RECOMMENDED for production)

You cannot disable Databricks Apps OAuth from within your code. However, for demos, you could:

1. **Deploy without Databricks Apps** - Use a regular Databricks job/cluster
2. **Use ngrok** - Tunnel local server to a public URL
3. **Separate deployment** - Host mock API on external service (AWS Lambda, etc.)

### Solution 4: Notebook Runs Locally

The simplest for development:

```python
# Run notebook on your laptop (not in Databricks workspace)
MOCK_API_URL = "http://localhost:8001"  # Your local server

# Use databricks-connect to write to Unity Catalog
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.remote(
    host="https://e2-demo-field-eng.cloud.databricks.com",
    token=dbutils.secrets.get("scope", "token")
).getOrCreate()

# Now can read from localhost AND write to remote Unity Catalog
df.write.saveAsTable("osipi.bronze.pi_timeseries")
```

## Recommended Architecture

**For Demo/POC:**
```
Databricks App (Mock PI Server)
     ↓
  Built-in scheduled task (every 15 min)
     ↓
  Directly writes to Unity Catalog via Databricks SDK
     ↓
osipi.bronze.* tables
     ↓
Dashboard auto-refreshes
```

**For Production:**
```
Real PI Web API Server
     ↓
Databricks Job (scheduled notebook)
     ↓
osipi.bronze.* tables
     ↓
Delta Live Tables pipelines (bronze → silver → gold)
```

## Immediate Action Items

1. **Add `/api/populate_bronze` endpoint** to Databricks App
   - Generates mock data
   - Writes directly to Unity Catalog using WorkspaceClient
   - Returns success/failure

2. **Update notebook** to call this endpoint once
   - Or add a button in the dashboard UI

3. **Add auto-refresh schedule** (optional)
   - Every 15 minutes, app regenerates and writes fresh data
   - Simulates real-time ingestion

## Why This Is Better

✅ No authentication issues
✅ Databricks App has full Unity Catalog access via its service principal
✅ Simpler architecture
✅ More realistic (production systems often push data, not pull)
✅ Dashboard updates automatically

## Code Changes Needed

See: `SOLUTION_DIRECT_WRITES.md` for implementation guide
