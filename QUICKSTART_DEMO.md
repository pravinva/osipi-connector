# OSI PI Lakeflow Connector - Quick Start Demo

**Target**: Deploy to field-eng Databricks workspace for demo
**Time**: 15 minutes
**Prerequisites**: Databricks CLI configured with field-eng profile

---

## Step 1: Verify Databricks CLI Configuration

```bash
# Check current Databricks configuration
databricks auth profiles

# Should show field-eng workspace
# Expected output:
# Name: field-eng
# Host: https://field-eng.cloud.databricks.com
# Token: dapi...
```

If not configured, set it up:
```bash
databricks auth login --host https://field-eng.cloud.databricks.com
```

---

## Step 2: Create Workspace Directory

```bash
# Create workspace directory for connector
databricks workspace mkdirs /Workspace/Shared/osipi-connector
```

---

## Step 3: Upload Source Code

```bash
# Navigate to connector directory
cd /Users/pravin.varma/Documents/Demo/osipi-connector

# Upload source files
databricks workspace import-dir src /Workspace/Shared/osipi-connector/src --overwrite

# Upload config
databricks workspace import-dir config /Workspace/Shared/osipi-connector/config --overwrite

# Upload mock server (for demo)
databricks workspace import tests/mock_pi_server.py /Workspace/Shared/osipi-connector/tests/mock_pi_server.py --overwrite
```

---

## Step 4: Create Demo Notebook

The demo notebook is already created at `notebooks/03_connector_demo_performance.py`.

Upload it:
```bash
databricks workspace import notebooks/03_connector_demo_performance.py \
  /Workspace/Shared/osipi-connector/demos/03_connector_demo_performance.py \
  --language PYTHON \
  --overwrite
```

---

## Step 5: Install Dependencies on Cluster

### Option A: Create New Cluster with Libraries

Create a cluster with these libraries pre-installed:

**Via UI**:
1. Go to Compute → Create Compute
2. Cluster name: `osipi-connector-demo`
3. Runtime: `14.3 LTS (Scala 2.12, Spark 3.5.0)`
4. Node type: `Standard_DS3_v2` (Azure) or `i3.xlarge` (AWS)
5. Workers: 2
6. Click "Libraries" → Install New
7. Add these PyPI packages:
   - `databricks-sdk>=0.30.0`
   - `requests>=2.31.0`
   - `fastapi>=0.104.0`
   - `uvicorn>=0.24.0`

**Via CLI**:
```bash
# Create cluster with init script
cat > /tmp/install-deps.sh << 'EOF'
#!/bin/bash
pip install databricks-sdk requests fastapi uvicorn pandas matplotlib seaborn
EOF

# Upload init script to DBFS
databricks fs cp /tmp/install-deps.sh dbfs:/init-scripts/osipi-connector-install.sh --overwrite

# Create cluster (save as cluster.json)
cat > /tmp/cluster.json << 'EOF'
{
  "cluster_name": "osipi-connector-demo",
  "spark_version": "14.3.x-scala2.12",
  "node_type_id": "i3.xlarge",
  "num_workers": 2,
  "autotermination_minutes": 120,
  "init_scripts": [
    {
      "dbfs": {
        "destination": "dbfs:/init-scripts/osipi-connector-install.sh"
      }
    }
  ]
}
EOF

databricks clusters create --json-file /tmp/cluster.json
```

### Option B: Use Existing Cluster

If you have an existing cluster, install libraries:

```bash
# Get cluster ID
databricks clusters list

# Install libraries (replace <cluster-id>)
databricks libraries install --cluster-id <cluster-id> --pypi-package databricks-sdk
databricks libraries install --cluster-id <cluster-id> --pypi-package requests
databricks libraries install --cluster-id <cluster-id> --pypi-package fastapi
databricks libraries install --cluster-id <cluster-id> --pypi-package uvicorn
databricks libraries install --cluster-id <cluster-id> --pypi-package pandas
databricks libraries install --cluster-id <cluster-id> --pypi-package matplotlib
databricks libraries install --cluster-id <cluster-id> --pypi-package seaborn

# Restart cluster for libraries to take effect
databricks clusters restart --cluster-id <cluster-id>
```

---

## Step 6: Create Unity Catalog Resources (Optional)

If you want to test the full write functionality:

```sql
-- Create catalog and schema
CREATE CATALOG IF NOT EXISTS demo_osipi;
CREATE SCHEMA IF NOT EXISTS demo_osipi.bronze;
CREATE SCHEMA IF NOT EXISTS demo_osipi.checkpoints;

-- Grant permissions (if needed)
GRANT ALL PRIVILEGES ON CATALOG demo_osipi TO `your-service-principal`;
GRANT ALL PRIVILEGES ON SCHEMA demo_osipi.bronze TO `your-service-principal`;
GRANT ALL PRIVILEGES ON SCHEMA demo_osipi.checkpoints TO `your-service-principal`;
```

Run this SQL in a SQL Warehouse or notebook.

---

## Step 7: Run Demo Notebook

### Quick Demo (Mock Server)

1. **Start Mock PI Server** (in one notebook cell):
```python
# This runs a mock PI server on port 8000
%pip install fastapi uvicorn

import subprocess
import sys

# Start mock server in background
proc = subprocess.Popen([
    sys.executable,
    "/Workspace/Shared/osipi-connector/tests/mock_pi_server.py"
], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

print("Mock PI server starting on http://localhost:8000")
```

2. **Open the Demo Notebook**:
   - Navigate to: `/Workspace/Shared/osipi-connector/demos/03_connector_demo_performance.py`
   - Attach to your cluster
   - Click "Run All"

3. **View Results**:
   - Performance benchmark charts
   - 30K tag extrapolation
   - Data granularity analysis
   - AF hierarchy visualization
   - Event frame analytics

**Expected Runtime**: 2-3 minutes

---

## Step 8: Show the Full Connector (Optional)

To demonstrate the complete connector with Unity Catalog:

Create a new notebook with:

```python
# Install dependencies
%pip install databricks-sdk requests

# Import connector
import sys
sys.path.append('/Workspace/Shared/osipi-connector')

from src.connector.pi_lakeflow_connector import PILakeflowConnector

# Configure for demo
config = {
    'pi_web_api_url': 'http://localhost:8000',  # Mock server
    'auth': {
        'type': 'basic',
        'username': 'demo',
        'password': 'demo'
    },
    'catalog': 'demo_osipi',
    'schema': 'bronze',
    'tags': ['all'],  # Extract all tags from mock server
    'af_database_id': None,  # Optional
    'include_event_frames': False  # Optional
}

# Run connector
connector = PILakeflowConnector(config)
connector.run()

# Verify data written
display(spark.sql("SELECT * FROM demo_osipi.bronze.pi_timeseries LIMIT 10"))
```

---

## What to Show in Demo

### Slide 1: Problem Statement (30 sec)
- "Industrial customers have 30K+ PI tags"
- "Current solutions: 2K limit, >5min granularity"
- "Need: Raw data at scale"

### Slide 2: Our Solution (30 sec)
- "Batch controller: 100 tags per request"
- "100x performance improvement"
- "30K tags in 25 minutes"

### Slide 3: Live Demo (2 min)
- Run performance benchmark notebook
- Show charts (4 visualizations)
- Highlight key metrics

### Slide 4: Impact (30 sec)
- "15,000 hours saved annually"
- "15x scale increase"
- "5x better resolution"

---

## Troubleshooting

### Issue: Module not found
**Solution**: Add to notebook first cell:
```python
import sys
sys.path.append('/Workspace/Shared/osipi-connector')
```

### Issue: Mock server not responding
**Solution**: Check if port 8000 is available:
```python
import socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
result = sock.connect_ex(('localhost', 8000))
if result == 0:
    print("Port 8000 is in use")
else:
    print("Port 8000 is available")
sock.close()
```

### Issue: Libraries not installed
**Solution**: Install in notebook:
```python
%pip install databricks-sdk requests fastapi uvicorn
dbutils.library.restartPython()
```

### Issue: Unity Catalog permissions
**Solution**: Use workspace catalog instead:
```python
config['catalog'] = 'hive_metastore'  # Default catalog
config['schema'] = 'default'
```

---

## Alternative: Use Databricks Asset Bundle

If you want to deploy via Asset Bundle:

```bash
# Update databricks.yml with field-eng workspace
cat > databricks.yml << 'EOF'
bundle:
  name: osipi-lakeflow-connector

targets:
  demo:
    mode: development
    workspace:
      host: https://field-eng.cloud.databricks.com
      root_path: /Workspace/Shared/osipi-connector

resources:
  jobs:
    osipi_demo_job:
      name: "OSI PI Connector Demo"
      tasks:
        - task_key: run_demo
          notebook_task:
            notebook_path: ./notebooks/03_connector_demo_performance.py
          existing_cluster_id: <your-cluster-id>
EOF

# Deploy
databricks bundle deploy --target demo

# Run
databricks bundle run osipi_demo_job --target demo
```

---

## Clean Up (After Demo)

```bash
# Remove workspace files
databricks workspace delete /Workspace/Shared/osipi-connector --recursive

# Delete cluster (if created for demo)
databricks clusters delete --cluster-id <cluster-id>

# Drop Unity Catalog resources (if created)
# Run in SQL Warehouse:
# DROP CATALOG demo_osipi CASCADE;
```

---

## Demo Checklist

Before your demo:
- [ ] Databricks CLI configured with field-eng
- [ ] Cluster created and running
- [ ] Dependencies installed
- [ ] Source code uploaded
- [ ] Demo notebook uploaded
- [ ] Mock server tested
- [ ] Unity Catalog setup (if showing full connector)

During demo:
- [ ] Start mock PI server
- [ ] Run demo notebook
- [ ] Show performance charts
- [ ] Highlight key metrics
- [ ] Show code architecture (optional)
- [ ] Answer questions

---

## Quick Commands Summary

```bash
# 1. Upload code
databricks workspace import-dir src /Workspace/Shared/osipi-connector/src --overwrite

# 2. Upload notebook
databricks workspace import notebooks/03_connector_demo_performance.py \
  /Workspace/Shared/osipi-connector/demos/demo.py --language PYTHON --overwrite

# 3. List clusters
databricks clusters list

# 4. Install libraries on cluster
databricks libraries install --cluster-id <id> --pypi-package databricks-sdk
databricks libraries install --cluster-id <id> --pypi-package requests

# 5. Run notebook
databricks runs submit --json '{
  "run_name": "OSI PI Demo",
  "existing_cluster_id": "<cluster-id>",
  "notebook_task": {
    "notebook_path": "/Workspace/Shared/osipi-connector/demos/demo.py"
  }
}'
```

---

**You're ready to demo!** 🚀

The quickest path:
1. Upload source files (2 min)
2. Install deps on cluster (5 min)
3. Run demo notebook (3 min)
4. Show results (5 min)

**Total**: 15 minutes setup + demo
