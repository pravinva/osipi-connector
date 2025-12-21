# OSI PI Lakeflow Connector - Deployment Guide

## Overview

This guide covers deploying the OSI PI Lakeflow Connector to Databricks using Databricks Asset Bundles (DABs).

## Prerequisites

### 1. Databricks Workspace Requirements

- ✅ Databricks workspace with Unity Catalog enabled
- ✅ Cluster with Spark 14.3.x or later
- ✅ Access to create catalogs, schemas, and tables
- ✅ Databricks CLI installed (`databricks --version`)
- ✅ Databricks SDK for Python (`pip install databricks-sdk`)

### 2. PI Web API Requirements

- ✅ PI Web API endpoint URL (e.g., `https://pi-server.company.com/piwebapi`)
- ✅ Valid credentials:
  - Basic: Username + Password
  - Kerberos: Cluster with Kerberos config
  - OAuth: Bearer token
- ✅ Network connectivity from Databricks to PI Web API
- ✅ PI Web API version 2019 or later (batch controller support)

### 3. Development Environment (for testing)

```bash
# Install Databricks CLI
pip install databricks-cli

# Install Databricks SDK
pip install databricks-sdk

# Install connector dependencies
pip install -r requirements.txt
```

## Installation Methods

### Method 1: Databricks Asset Bundle (Recommended for Production)

**Best for**: Production deployments, CI/CD integration, multi-environment management

#### Step 1: Configure Workspace

```bash
# Set Databricks profile (one-time setup)
databricks configure --token

# Provide:
# - Workspace URL: https://your-workspace.cloud.databricks.com
# - Token: dapi...
```

#### Step 2: Configure Bundle

Edit `databricks.yml` to customize for your environment:

```yaml
targets:
  dev:
    workspace:
      host: https://your-dev-workspace.cloud.databricks.com
    variables:
      catalog: main
      schema: bronze_dev

  prod:
    workspace:
      host: https://your-prod-workspace.cloud.databricks.com
      root_path: /Workspace/production/osipi-connector
    variables:
      catalog: main
      schema: bronze
      pi_web_api_url: https://pi-server.company.com/piwebapi
```

#### Step 3: Setup Secrets

```bash
# Create secret scope
databricks secrets create-scope pi-connector

# Add PI credentials
databricks secrets put-secret pi-connector pi_username
databricks secrets put-secret pi-connector pi_password

# For OAuth
databricks secrets put-secret pi-connector oauth_token
```

#### Step 4: Validate and Deploy

```bash
# Validate bundle configuration
databricks bundle validate --target prod

# Deploy to production
databricks bundle deploy --target prod

# Check deployment status
databricks bundle summary --target prod
```

#### Step 5: Run Initial Load

```bash
# Trigger initial connector run
databricks bundle run osipi_connector_job --target prod

# Monitor job execution
databricks jobs runs list --job-id <job_id>
```

#### Step 6: Verify Data

```sql
-- Check time-series data
SELECT COUNT(*) FROM main.bronze.pi_timeseries;

-- Check AF hierarchy
SELECT element_name, element_path FROM main.bronze.pi_asset_hierarchy LIMIT 10;

-- Check event frames
SELECT event_name, start_time FROM main.bronze.pi_event_frames LIMIT 10;

-- Verify checkpoint
SELECT * FROM main.checkpoints.pi_watermarks;
```

### Method 2: Manual Notebook Deployment (Quick Testing)

**Best for**: Quick testing, development, proof of concept

#### Step 1: Upload Files to Workspace

```bash
# Using Databricks CLI
databricks workspace import_dir src/ /Workspace/Shared/osipi-connector/src/
databricks workspace import_dir config/ /Workspace/Shared/osipi-connector/config/
```

Or upload via Databricks UI:
1. Navigate to Workspace
2. Create folder: `/Shared/osipi-connector`
3. Upload `src/` and `config/` folders

#### Step 2: Create Job Notebook

Create a notebook in Databricks with:

```python
# Install dependencies (if not in cluster libraries)
%pip install databricks-sdk requests requests-kerberos pyyaml

# Import connector
import sys
sys.path.append('/Workspace/Shared/osipi-connector')

from src.connector.lakeflow_connector import PILakeflowConnector

# Configure
config = {
    'pi_web_api_url': dbutils.secrets.get('pi-connector', 'pi_web_api_url'),
    'pi_auth_type': 'basic',
    'pi_username': dbutils.secrets.get('pi-connector', 'pi_username'),
    'pi_password': dbutils.secrets.get('pi-connector', 'pi_password'),
    'catalog': 'main',
    'schema': 'bronze',
    'tags': dbutils.secrets.get('pi-connector', 'tags').split(','),
    'af_database_id': dbutils.secrets.get('pi-connector', 'af_database_id'),
    'include_event_frames': True
}

# Run connector
connector = PILakeflowConnector(config)
connector.run()
```

#### Step 3: Create and Schedule Job

1. Navigate to Workflows → Jobs
2. Click "Create Job"
3. Configure:
   - **Name**: OSI PI Connector
   - **Task**: Notebook task pointing to your connector notebook
   - **Cluster**: Select existing cluster or create new
   - **Schedule**: Hourly (0 0 * * * ?)
   - **Timeout**: 2 hours
4. Click "Create"

### Method 3: Python Package (Advanced)

**Best for**: Custom integrations, programmatic deployment

#### Step 1: Package Connector

```bash
# Create wheel package
python setup.py bdist_wheel

# Package will be in dist/osipi_connector-1.0.0-py3-none-any.whl
```

#### Step 2: Upload to Databricks

```bash
# Upload to DBFS
databricks fs cp dist/osipi_connector-1.0.0-py3-none-any.whl \
  dbfs:/FileStore/jars/osipi_connector-1.0.0-py3-none-any.whl

# Or upload to Workspace Files
databricks workspace import dist/osipi_connector-1.0.0-py3-none-any.whl \
  /Workspace/Shared/packages/osipi_connector-1.0.0-py3-none-any.whl
```

#### Step 3: Install on Cluster

Add library to cluster:
1. Navigate to Compute → Your Cluster
2. Click "Libraries" tab
3. Click "Install New"
4. Select "Python Whl"
5. Provide path: `dbfs:/FileStore/jars/osipi_connector-1.0.0-py3-none-any.whl`
6. Click "Install"

#### Step 4: Use in Notebook

```python
from osipi_connector import PILakeflowConnector

config = {...}
connector = PILakeflowConnector(config)
connector.run()
```

## Configuration Reference

### Required Configuration

```yaml
# Minimum required configuration
pi_web_api:
  url: "https://pi-server.company.com/piwebapi"
  auth_type: "basic"
  secrets_scope: "pi-connector"

unity_catalog:
  catalog: "main"
  schema: "bronze"

extraction:
  tags:
    - "F1DP-Tag1-WebId"
    - "F1DP-Tag2-WebId"
```

### Full Configuration Example

See `config/connector_config.yaml` for complete configuration options including:
- Authentication (Basic, OAuth, Kerberos)
- Extraction settings (batch size, time ranges)
- Performance tuning (partitioning, optimization)
- Monitoring and alerting
- Retry configuration

## Network Configuration

### Firewall Rules

Ensure Databricks can reach PI Web API:

**From**: Databricks cluster IP ranges
**To**: PI Web API endpoint (port 443)
**Protocol**: HTTPS (TLS 1.2+)

Get Databricks IP ranges:
```bash
# For AWS
https://docs.databricks.com/administration-guide/cloud-configurations/aws/customer-managed-vpc.html

# For Azure
https://docs.databricks.com/administration-guide/cloud-configurations/azure/vnet-inject.html

# For GCP
https://docs.databricks.com/administration-guide/cloud-configurations/gcp/customer-managed-vpc.html
```

### Private Link / VPC Peering

For secure connectivity:

1. **AWS**: Setup PrivateLink between Databricks VPC and PI VPC
2. **Azure**: Setup Private Endpoint
3. **GCP**: Setup Private Service Connect

## Authentication Setup

### Basic Authentication

```bash
# Store credentials in Databricks Secrets
databricks secrets put-secret pi-connector pi_username
databricks secrets put-secret pi-connector pi_password
```

In config:
```yaml
pi_web_api:
  auth_type: "basic"
  secrets_scope: "pi-connector"
  username_key: "pi_username"
  password_key: "pi_password"
```

### Kerberos Authentication

1. Upload Kerberos keytab to DBFS:
```bash
databricks fs cp krb5.keytab dbfs:/FileStore/kerberos/krb5.keytab
```

2. Configure cluster with Kerberos:
```yaml
spark_conf:
  spark.security.credentials.hiveserver2.enabled: "true"
  spark.security.kerberos.keytab.location: "/dbfs/FileStore/kerberos/krb5.keytab"
  spark.security.kerberos.principal: "your-principal@REALM.COM"
```

3. In config:
```yaml
pi_web_api:
  auth_type: "kerberos"
```

### OAuth Authentication

```bash
# Store OAuth token in Databricks Secrets
databricks secrets put-secret pi-connector oauth_token
```

In config:
```yaml
pi_web_api:
  auth_type: "oauth"
  secrets_scope: "pi-connector"
  oauth_token_key: "oauth_token"
```

## Cluster Configuration

### Recommended Cluster Settings

```yaml
cluster_config:
  spark_version: "14.3.x-scala2.12"
  node_type_id: "i3.xlarge"  # AWS example
  num_workers: 2
  autoscale:
    min_workers: 2
    max_workers: 8

  spark_conf:
    # Delta Lake optimizations
    spark.databricks.delta.preview.enabled: "true"
    spark.databricks.delta.properties.defaults.enableChangeDataFeed: "true"

    # Network settings
    spark.executor.heartbeatInterval: "60s"
    spark.network.timeout: "600s"

    # Memory settings
    spark.executor.memory: "8g"
    spark.driver.memory: "8g"

  # Install connector dependencies
  libraries:
    - pypi:
        package: "databricks-sdk>=0.30.0"
    - pypi:
        package: "requests>=2.31.0"
    - pypi:
        package: "requests-kerberos>=0.14.0"
```

### Cluster Libraries

```python
# Via init script (recommended)
cat > /dbfs/init-scripts/install-connector.sh << 'EOF'
#!/bin/bash
pip install databricks-sdk requests requests-kerberos pyyaml
EOF

# Configure cluster to use init script
# Cluster → Init Scripts → Add: dbfs:/init-scripts/install-connector.sh
```

## Monitoring and Alerting

### Job Monitoring

```python
# Add to connector for metrics tracking
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Log metrics after each run
metrics = {
    'run_id': dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().get(),
    'tags_extracted': len(tag_webids),
    'records_written': df.count(),
    'duration_seconds': elapsed_time,
    'status': 'success'
}

# Write to metrics table
spark.createDataFrame([metrics]).write.mode('append').saveAsTable('main.monitoring.pi_connector_metrics')
```

### Email Alerts on Failure

Configure in Databricks job settings:
1. Navigate to job → Settings
2. Add email alerts:
   - On Failure
   - On Duration Exceeded (2 hours)

### Slack Notifications

```python
# Add to connector error handling
import requests

def send_slack_alert(message):
    webhook_url = dbutils.secrets.get('pi-connector', 'slack_webhook')
    payload = {
        'text': f'PI Connector Alert: {message}'
    }
    requests.post(webhook_url, json=payload)

# Use in exception handler
try:
    connector.run()
except Exception as e:
    send_slack_alert(f'Connector failed: {str(e)}')
    raise
```

## Troubleshooting

### Common Issues

#### 1. Authentication Failed

**Error**: `401 Unauthorized`

**Solutions**:
- Verify credentials in Databricks Secrets
- Check PI Web API endpoint URL (must end with `/piwebapi`)
- Test credentials manually: `curl -u username:password https://pi-server/piwebapi`

#### 2. Network Timeout

**Error**: `ConnectionError: Connection timeout`

**Solutions**:
- Verify firewall rules allow Databricks → PI Web API
- Check if private link is configured correctly
- Increase timeout in config: `timeout: 120`

#### 3. Batch Controller Not Supported

**Error**: `405 Method Not Allowed` on `/batch`

**Solutions**:
- Verify PI Web API version ≥ 2019
- Check if batch controller is enabled on PI server
- Fall back to sequential mode in config

#### 4. Unity Catalog Permission Denied

**Error**: `403 Forbidden: insufficient privileges`

**Solutions**:
- Grant catalog/schema permissions:
```sql
GRANT CREATE, USAGE ON CATALOG main TO `your-service-principal`;
GRANT ALL PRIVILEGES ON SCHEMA main.bronze TO `your-service-principal`;
```

#### 5. Out of Memory

**Error**: `OutOfMemoryError: Java heap space`

**Solutions**:
- Reduce `batch_size` in config
- Reduce `max_count_per_tag` in config
- Increase cluster memory: `spark.executor.memory: "16g"`

### Debug Mode

Enable debug logging in connector:

```python
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

connector = PILakeflowConnector(config)
connector.run()
```

### Testing Connectivity

```python
# Test PI Web API connectivity
import requests
from requests.auth import HTTPBasicAuth

url = "https://pi-server.company.com/piwebapi"
auth = HTTPBasicAuth(username, password)

response = requests.get(url, auth=auth)
print(f"Status: {response.status_code}")
print(f"Response: {response.json()}")

# Should return PI Web API system info
```

## Performance Tuning

### For Large Deployments (30K+ tags)

```yaml
extraction:
  batch_size: 100  # Optimal for PI Web API
  max_count_per_tag: 10000  # PI Web API limit

performance:
  spark_partitions: 200  # Match cluster cores
  optimize_after_write: true
  zorder_columns:
    - tag_webid
    - timestamp
```

### For Low Latency

```yaml
schedule:
  cron_expression: "0 0/5 * * * ?"  # Every 5 minutes

extraction:
  mode: "incremental"
  enable_paging: true

performance:
  optimize_after_write: false  # Skip for frequent writes
```

### For Historical Backfills

```yaml
extraction:
  mode: "full"
  start_date: "2023-01-01T00:00:00Z"

performance:
  spark_partitions: 400  # More parallelism
  optimize_after_write: true
```

## Maintenance

### Regular Tasks

**Daily**:
- Monitor job success rate
- Check data freshness in tables
- Review error logs

**Weekly**:
- Optimize Delta tables:
```sql
OPTIMIZE main.bronze.pi_timeseries;
OPTIMIZE main.bronze.pi_asset_hierarchy;
OPTIMIZE main.bronze.pi_event_frames;
```

**Monthly**:
- Review and update tag list
- Check for new AF databases
- Vacuum old data:
```sql
VACUUM main.bronze.pi_timeseries RETAIN 168 HOURS;  -- 7 days
```

### Upgrades

```bash
# Pull latest connector version
git pull origin main

# Validate changes
databricks bundle validate --target prod

# Deploy to dev first
databricks bundle deploy --target dev

# Test in dev
databricks bundle run osipi_connector_job --target dev

# Deploy to prod
databricks bundle deploy --target prod
```

## Cost Optimization

### Reduce Compute Costs

1. **Use spot instances** (AWS) or low-priority VMs (Azure)
2. **Right-size clusters**: Start with 2 workers, scale as needed
3. **Optimize schedule**: Run during off-peak hours
4. **Use incremental mode**: Only extract new data

### Reduce Storage Costs

1. **Partition by date**: Improves pruning, reduces scans
2. **Z-ORDER by tag**: Reduces file reads
3. **Vacuum old data**: Remove unused versions
4. **Compress data**: Delta handles automatically

### Monitor Costs

```sql
-- Query to track data volume
SELECT
    DATE(partition_date) as date,
    COUNT(*) as records,
    COUNT(DISTINCT tag_webid) as tags,
    SUM(LENGTH(value)) as bytes_approx
FROM main.bronze.pi_timeseries
GROUP BY partition_date
ORDER BY date DESC
LIMIT 30;
```

## Security Best Practices

### 1. Secrets Management

✅ Always use Databricks Secrets (never hardcode credentials)
✅ Use separate scopes for dev/prod
✅ Rotate secrets regularly (every 90 days)
✅ Grant least privilege access to secret scopes

### 2. Network Security

✅ Use Private Link / VPC Peering for PI Web API access
✅ Enable cluster isolation
✅ Restrict outbound traffic to PI Web API only
✅ Use TLS 1.2+ for all connections

### 3. Data Security

✅ Enable Unity Catalog access controls
✅ Use row-level security for sensitive tags
✅ Enable audit logging
✅ Encrypt data at rest (Delta Lake default)

### 4. Identity and Access

✅ Use service principals (not personal accounts) for jobs
✅ Grant minimal permissions (CREATE on catalog/schema)
✅ Enable SSO for Databricks workspace
✅ Use Azure AD / Okta / AWS IAM for authentication

## Support

### Documentation
- **README.md** - Quick start guide
- **HACKATHON_SUBMISSION.md** - Complete overview
- **DEMO_GUIDE.md** - Demo instructions
- **DEVELOPER.md** - Technical specification

### Contact
- **Issues**: Create GitHub issue
- **Email**: pravin.varma@databricks.com
- **Slack**: @pravin.varma

---

**Ready to deploy? Start with Method 1 (Databricks Asset Bundle) for the best experience.**
