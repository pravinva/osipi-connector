# Deployment Guide: Bearer Token Authentication Fix

## Overview

This guide covers deploying the authentication fix that resolves 401 Unauthorized errors in the PI ingestion pipelines.

## Changes Summary

**File Modified**: `src/notebooks/pi_ingestion_pipeline.py` (lines 40-67)
- **Before**: OAuth M2M authentication using sp-osipi Service Principal credentials
- **After**: Bearer token authentication using user's PAT token
- **Result**: Authentication identity now matches pipeline execution identity (both are the user)

## Prerequisites

✓ User's PAT token already stored in secrets scope: `sp-osipi/databricks-pat-token`
✓ Value: `YOUR_PAT_TOKEN_HERE`
✓ Code changes completed in local repository

## Deployment Steps

### Step 1: Verify Local Changes

```bash
# Check the modified file
cat src/notebooks/pi_ingestion_pipeline.py | grep -A 30 "connection_name == 'mock_pi_connection'"
```

Expected output should show Bearer token authentication code (lines 40-67).

### Step 2: Deploy to Databricks Workspace

You have two deployment options:

#### Option A: Using Databricks CLI (Recommended)

```bash
# Deploy the updated notebook
databricks workspace import \
  src/notebooks/pi_ingestion_pipeline.py \
  /Workspace/Users/pravin.varma@databricks.com/osipi-connector/notebooks/pi_ingestion_pipeline \
  --language PYTHON \
  --format SOURCE \
  --overwrite
```

#### Option B: Using Databricks UI

1. Open Databricks workspace: https://e2-demo-field-eng.cloud.databricks.com
2. Navigate to: `/Workspace/Users/pravin.varma@databricks.com/osipi-connector/notebooks/`
3. Find: `pi_ingestion_pipeline` notebook
4. Click: "File" → "Import" → Select local file: `src/notebooks/pi_ingestion_pipeline.py`
5. Confirm: Overwrite existing notebook

### Step 3: Verify Secrets Configuration

```bash
# Verify the PAT token is stored correctly
databricks secrets get-secret sp-osipi databricks-pat-token
```

Expected output:
```
key: databricks-pat-token
value: [REDACTED]
```

### Step 4: Test Single Pipeline

Test one pipeline before running all scheduled jobs:

```bash
# Trigger pipeline job ID: 1021961729261145 (one of the 7 scheduled jobs)
databricks jobs run-now --job-id 1021961729261145
```

Monitor the run:
```bash
# Get the run ID from the previous command output, then:
databricks runs get --run-id <RUN_ID>
```

### Step 5: Verify Pipeline Success

Check for these success indicators:

1. **No 401 Errors**: Pipeline logs should not contain "401 Unauthorized"
2. **Data Ingestion**: Bronze tables should have new records with recent `ingestion_timestamp`
3. **Pipeline Status**: Run should complete with SUCCESS state

```sql
-- Check latest ingestion in bronze tables
SELECT
  COUNT(*) as record_count,
  MAX(ingestion_timestamp) as latest_ingestion
FROM osipi.bronze.pi_timeseries_pipeline1;
```

### Step 6: Enable All Scheduled Jobs

Once the test pipeline succeeds, the remaining 6 scheduled jobs will run on their 3-hour schedule:

**Scheduled Jobs** (all use 3-hour cron):
- Job ID: 1021961729261145
- Job ID: 578867469558588
- Job ID: 872279485612338
- Job ID: 97914001466943
- Job ID: 1020821715924116
- Job ID: 752380099131891
- Job ID: 148777949316459

These will automatically use the updated authentication code on their next scheduled run.

## Rollback Plan

If the new authentication fails, you can quickly rollback:

### Option 1: Restore OAuth M2M Authentication

```bash
# Restore the previous version from git
git checkout HEAD~1 -- src/notebooks/pi_ingestion_pipeline.py

# Re-deploy the old version
databricks workspace import \
  src/notebooks/pi_ingestion_pipeline.py \
  /Workspace/Users/pravin.varma@databricks.com/osipi-connector/notebooks/pi_ingestion_pipeline \
  --language PYTHON \
  --format SOURCE \
  --overwrite
```

### Option 2: Update PAT Token

If the PAT token is expired or invalid:

```bash
# Generate new PAT token in Databricks UI:
# User Settings → Developer → Access Tokens → Generate New Token

# Store the new token
databricks secrets put-secret sp-osipi databricks-pat-token --string-value "<NEW_TOKEN>"
```

## Troubleshooting

### Issue: Still Getting 401 Errors

**Check 1: Verify notebook was actually updated**
```bash
databricks workspace export \
  /Workspace/Users/pravin.varma@databricks.com/osipi-connector/notebooks/pi_ingestion_pipeline \
  --format SOURCE \
  | grep "databricks-pat-token"
```

You should see the line:
```python
pat_token = dbutils.secrets.get(scope="sp-osipi", key="databricks-pat-token")
```

**Check 2: Verify PAT token is valid**
```bash
curl -H "Authorization: Bearer YOUR_PAT_TOKEN_HERE" \
  https://e2-demo-field-eng.cloud.databricks.com/api/2.0/clusters/list
```

Expected: JSON response with cluster list
If 401: Token is invalid or expired

**Check 3: Test App authentication directly**
```bash
curl -H "Authorization: Bearer YOUR_PAT_TOKEN_HERE" \
  https://osipi-webserver-1444828305810485.aws.databricksapps.com/piwebapi/dataservers
```

Expected: JSON response with PI data servers
If 401: User doesn't have permission to access the App

### Issue: Pipeline Fails with Different Error

Check pipeline logs for the specific error:
```bash
databricks jobs get-run --run-id <RUN_ID> | jq '.state.state_message'
```

Common errors:
- **"No module named 'requests'"**: Missing library in cluster
- **"Secret does not exist"**: Secret key name mismatch
- **"Table not found"**: Bronze tables don't exist yet (expected on first run)

## Verification Checklist

- [ ] Local code changes reviewed (lines 40-67 in `pi_ingestion_pipeline.py`)
- [ ] PAT token stored in secrets scope (`sp-osipi/databricks-pat-token`)
- [ ] Notebook deployed to Databricks workspace
- [ ] Single test pipeline triggered manually
- [ ] Test pipeline completed successfully (no 401 errors)
- [ ] Bronze table has new data with recent `ingestion_timestamp`
- [ ] Scheduled jobs will use updated code on next run

## Post-Deployment

### Monitor Scheduled Runs

Check the scheduled jobs over the next 24 hours:

```bash
# List recent runs for all 7 jobs
for job_id in 1021961729261145 578867469558588 872279485612338 97914001466943 1020821715924116 752380099131891 148777949316459; do
  echo "Job ID: $job_id"
  databricks jobs list-runs --job-id $job_id --limit 1 | jq '.runs[0] | {run_id, state, start_time}'
done
```

### Validate Data Quality

After successful runs, verify data quality:

```sql
-- Check ingestion counts across all pipelines
SELECT
  'pipeline1' as pipeline,
  COUNT(*) as records,
  MAX(ingestion_timestamp) as latest_ingestion
FROM osipi.bronze.pi_timeseries_pipeline1
UNION ALL
SELECT
  'pipeline2' as pipeline,
  COUNT(*) as records,
  MAX(ingestion_timestamp) as latest_ingestion
FROM osipi.bronze.pi_timeseries_pipeline2
-- Repeat for all 7 pipelines
;
```

## Success Criteria

The deployment is successful when:

1. ✓ All 7 scheduled jobs complete without 401 errors
2. ✓ Bronze tables receive new data on every scheduled run (every 3 hours)
3. ✓ Silver merge pipeline successfully merges data from all bronze tables
4. ✓ Dashboard shows updated data from latest ingestion

## Support

If issues persist after following this guide:

1. Check the diagnostic script: `notebooks/test_sp_osipi_api_call.py`
2. Review architecture documentation: `docs/ARCHITECTURE_MULTI_PIPELINE.md`
3. Examine pipeline logs in Databricks UI: Workflows → Jobs → Select Job → Runs Tab
