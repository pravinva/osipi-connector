# Databricks App Deployment Guide

## Overview

This guide covers deploying the PI Web API mock server as a Databricks App with the new configuration UI.

## Prerequisites

- Databricks CLI installed and configured
- Databricks workspace with Apps enabled
- Python 3.10+
- Git repository access

## Deployment Steps

### 1. Navigate to Databricks App Directory

```bash
cd /Users/pravin.varma/Documents/Demo/osipi-connector/databricks-app
```

### 2. Deploy to Databricks Apps

Using Databricks CLI:

```bash
databricks apps deploy osipi-webserver
```

Or using the UI:
1. Go to Databricks workspace
2. Navigate to **Workspace** → **Apps**
3. Click **Create App**
4. Select **From Git**
5. Enter repository URL: `https://github.com/pravinva/osipi-connector.git`
6. Set working directory: `databricks-app`
7. Click **Deploy**

### 3. Verify Deployment

**App URL**: https://osipi-webserver-1444828305810485.aws.databricksapps.com

Test endpoints:
- `/` - Home page
- `/config` - Configuration UI (**NEW**)
- `/ingestion` - Ingestion dashboard
- `/piwebapi` - PI Web API root
- `/health` - Health check

### 4. Configure Environment Variables (Optional)

Set default configuration via environment variables:

```bash
databricks apps configure osipi-webserver \
  --env MOCK_PI_TAG_COUNT=1040 \
  --env UC_CATALOG=osipi \
  --env UC_SCHEMA=bronze \
  --env DATABRICKS_WAREHOUSE_ID=your-warehouse-id
```

## Configuration UI Usage

### Access Configuration Page

Navigate to: `https://osipi-webserver-1444828305810485.aws.databricksapps.com/config`

### Quick Configuration

Use preset buttons for common configurations:
- **Demo (128 tags)**: Fast demos
- **Small (1K tags)**: Development testing
- **Medium (10K tags)**: Integration testing
- **Large (30K tags)**: Production-scale testing

### Custom Configuration

1. Set **Number of PI Tags** (10-100,000)
2. Set **Number of Event Frames** (10-10,000)
3. Set **Event History Days** (1-365)
4. Click **Apply Configuration**
5. Wait for success message
6. Page reloads with new configuration

### Configuration Takes Effect Immediately

No app restart required. All API endpoints immediately return data based on the new configuration.

## Architecture

```
databricks-app/
├── app/
│   ├── main.py                 # FastAPI app with new config endpoints
│   ├── static/                 # Static assets
│   └── templates/
│       ├── config.html         # Configuration UI (NEW)
│       ├── pi_home.html        # Home page (updated with config link)
│       └── ingestion.html      # Dashboard
├── requirements.txt            # Python dependencies
├── README_CONFIGURATION.md     # Configuration UI documentation
└── DEPLOYMENT.md              # This file
```

## New Endpoints

### GET /config
**Description**: Configuration UI page

**Response**: HTML page with configuration form

### POST /api/config/update
**Description**: Update mock server configuration

**Request Body**:
```json
{
  "tag_count": 1040,
  "event_count": 100,
  "event_days": 30
}
```

**Response**:
```json
{
  "success": true,
  "tags": 1040,
  "af_elements": 650,
  "events": 100,
  "plants": 10
}
```

## Updating the App

### Method 1: Redeploy from Git

```bash
databricks apps deploy osipi-webserver --force
```

### Method 2: Update via UI

1. Go to **Workspace** → **Apps** → **osipi-webserver**
2. Click **Update**
3. Select latest commit from Git
4. Click **Deploy**

## Monitoring

### Check App Status

```bash
databricks apps status osipi-webserver
```

### View Logs

```bash
databricks apps logs osipi-webserver --follow
```

### Test Health Endpoint

```bash
curl https://osipi-webserver-1444828305810485.aws.databricksapps.com/health
```

Expected response:
```json
{
  "status": "healthy",
  "timestamp": "2025-01-10T12:00:00Z",
  "version": "2019 R2 (Mock)",
  "components": {
    "pi_web_api": "operational",
    "dashboard": "operational",
    "lakehouse": "connected"
  }
}
```

## Integration with Pipeline Generator

After updating configuration, regenerate pipelines:

```bash
# Upload notebook to workspace
databricks workspace import notebooks/generate_pipelines_from_mock_api.py \
  --language PYTHON \
  --format SOURCE

# Run notebook
databricks jobs create --json '{
  "name": "Generate PI Pipelines",
  "tasks": [{
    "task_key": "generate",
    "notebook_task": {
      "notebook_path": "/Workspace/Users/pravin.varma@databricks.com/generate_pipelines_from_mock_api"
    },
    "new_cluster": {
      "spark_version": "14.3.x-scala2.12",
      "node_type_id": "i3.xlarge",
      "num_workers": 0
    }
  }]
}'
```

## Troubleshooting

### App Not Responding
**Symptom**: App URL returns 503 or timeout

**Solutions**:
1. Check app status: `databricks apps status osipi-webserver`
2. View logs: `databricks apps logs osipi-webserver`
3. Redeploy: `databricks apps deploy osipi-webserver --force`

### Configuration Not Saving
**Symptom**: Configuration changes don't persist

**Cause**: In-memory storage resets on app restart

**Solution**: Set `MOCK_PI_TAG_COUNT` environment variable for persistent default

### API Returns Old Tag Count
**Symptom**: `/piwebapi/dataservers/{id}/points` returns old tag count

**Solutions**:
1. Hard refresh browser (Ctrl+Shift+R)
2. Wait a few seconds for data regeneration to complete
3. Check logs for errors

### Build Failures
**Symptom**: Deployment fails with dependency errors

**Solutions**:
1. Check `requirements.txt` for conflicting dependencies
2. Remove Kerberos dependencies if present
3. Test locally: `pip install -r requirements.txt`

## Performance Tuning

### Response Time Optimization

| Tag Count | Response Time | Recommendation |
|-----------|---------------|----------------|
| 128-1K | < 500ms | Optimal for demos |
| 10K | ~1s | Good for testing |
| 30K+ | 2-3s | Use caching |

### Memory Usage

- **128 tags**: ~50 MB
- **1K tags**: ~100 MB
- **10K tags**: ~500 MB
- **30K tags**: ~1.5 GB

Databricks Apps have sufficient memory for all configurations.

## Security

### Authentication

Databricks Apps inherit workspace authentication:
- OAuth 2.0 for user access
- Service principals for API access
- No additional authentication required

### Authorization

Configure workspace permissions:
1. Go to **Settings** → **Users and Groups**
2. Set app permissions:
   - **Can View**: Read-only access
   - **Can Run**: Execute configuration changes
   - **Can Manage**: Full admin access

## Cost Optimization

### Compute Usage

Databricks Apps use serverless compute:
- Pay only for active requests
- Automatic scaling
- No idle compute charges

### Estimated Costs

| Configuration | Requests/Day | Est. Cost/Month |
|---------------|--------------|-----------------|
| Demo (128 tags) | 1,000 | $5-10 |
| Small (1K tags) | 10,000 | $20-30 |
| Medium (10K tags) | 50,000 | $50-75 |
| Large (30K tags) | 100,000 | $100-150 |

Costs vary by workspace region and usage patterns.

## Next Steps

After deployment:

1. **Test Configuration UI**: Visit `/config` and try different presets
2. **Run Connector Notebook**: Ingest data from mock API to osipi.bronze
3. **Generate Pipelines**: Run `generate_pipelines_from_mock_api.py`
4. **Deploy DAB**: Deploy generated pipelines using `databricks bundle deploy`
5. **Monitor Dashboard**: Check `/ingestion` for real-time ingestion status

## Support

For issues or questions:
- Check logs: `databricks apps logs osipi-webserver`
- Review documentation: `/databricks-app/README_CONFIGURATION.md`
- Contact Databricks support
