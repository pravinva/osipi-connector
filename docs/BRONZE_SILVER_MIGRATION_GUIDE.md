# Bronze-Silver Architecture Migration Guide

## Overview

This guide explains how to migrate from single-pipeline architecture to Bronze-Silver medallion architecture to support multiple parallel pipelines writing to the same tables.

## Problem Solved

**Issue**: DLT only allows ONE pipeline to own a table. When you create 5 pipelines all trying to write to `osipi.bronze.pi_timeseries`, only the first one succeeds - others get "table already managed by pipeline X" error.

**Solution**: Use Bronze-Silver architecture:
- **5 Bronze Pipelines** → Write to separate tables (append-only, no conflicts)
- **1 Silver Pipeline** → Consolidate and deduplicate into final tables

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Mock PI Web API (Databricks App)                           │
│  https://osipi-webserver-*.aws.databricksapps.com          │
└─────────────────────────────────────────────────────────────┘
                           ↓
        ┌──────────────────┼──────────────────┐
        ↓                  ↓                  ↓
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ Bronze       │  │ Bronze       │  │ Bronze       │
│ Pipeline 1   │  │ Pipeline 2   │  │ Pipeline 3-5 │
│              │  │              │  │              │
│ Tags: 1-2000 │  │ Tags: 2001-  │  │ ...          │
│              │  │      4000    │  │              │
└──────────────┘  └──────────────┘  └──────────────┘
        ↓                  ↓                  ↓
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ osipi.       │  │ osipi.       │  │ osipi.       │
│ bronze_raw   │  │ bronze_raw   │  │ bronze_raw   │
│              │  │              │  │              │
│ pi_timeseries│  │ pi_timeseries│  │ pi_timeseries│
│ _raw_group1  │  │ _raw_group2  │  │ _raw_group3-5│
└──────────────┘  └──────────────┘  └──────────────┘
        └──────────────────┴───────────────────┘
                           ↓
                  ┌──────────────────┐
                  │ Silver Pipeline  │
                  │ (Consolidation)  │
                  │                  │
                  │ UNION ALL +      │
                  │ Deduplication    │
                  └──────────────────┘
                           ↓
                  ┌──────────────────┐
                  │ osipi.bronze     │
                  │                  │
                  │ pi_timeseries    │
                  │ pi_af_hierarchy  │
                  │ pi_event_frames  │
                  └──────────────────┘
                           ↓
                  ┌──────────────────┐
                  │ Dashboard UI     │
                  └──────────────────┘
```

## Files Created

### 1. `/src/notebooks/pi_bronze_ingestion.py`
**Purpose**: Ingests data for one pipeline group (append-only, no conflicts)

**Key Changes from Original**:
- Writes to `pi_timeseries_raw_group{N}` instead of `pi_timeseries`
- Uses `@dlt.table()` without MERGE (append-only)
- Adds `pipeline_group` column to track source
- No ownership conflicts - multiple pipelines can run in parallel!

### 2. `/src/notebooks/pi_silver_consolidation.py`
**Purpose**: Consolidates all bronze_raw tables into final silver tables

**Logic**:
- Reads from `pi_timeseries_raw_group1`, `pi_timeseries_raw_group2`, etc.
- UNIONs all groups together
- Deduplicates using window functions (keep latest ingestion)
- Writes to final `osipi.bronze.pi_timeseries` table
- Single pipeline ownership - no conflicts!

## Changes Needed in generate_pipelines_from_mock_api.py

### Update Configuration Section (Lines 40-90)

Add these new constants:

```python
# NEW: Bronze-Silver Architecture Settings
USE_BRONZE_SILVER = True  # Toggle for bronze-silver architecture

# Notebook paths
BRONZE_NOTEBOOK_PATH = "/Workspace/Users/pravin.varma@databricks.com/osipi-connector/src/notebooks/pi_bronze_ingestion"
SILVER_NOTEBOOK_PATH = "/Workspace/Users/pravin.varma@databricks.com/osipi-connector/src/notebooks/pi_silver_consolidation"

# Schema names
BRONZE_RAW_SCHEMA = "bronze_raw"  # For raw append-only tables
SILVER_SCHEMA = "bronze"          # For final deduplicated tables
```

### Update create_pipelines_yaml Function (Lines 306-349)

Replace with this updated version:

```python
def create_pipelines_yaml(df, project_name, bronze_notebook_path, silver_notebook_path, ingestion_mode="batch", use_bronze_silver=True):
    """Generate DLT pipelines YAML with Bronze-Silver architecture support."""
    pipelines = {}
    num_groups = df['pipeline_group'].nunique()

    if use_bronze_silver:
        # Create N bronze pipelines (append-only, per group)
        for pipeline_group in sorted(df['pipeline_group'].unique()):
            group_df = df[df['pipeline_group'] == pipeline_group]
            pipeline_name = f"{project_name}_bronze_group_{pipeline_group}"

            first_row = group_df.iloc[0]
            tags = group_df['tag_webid'].tolist()

            # Bronze pipeline configuration (append-only)
            pipeline_config = {
                'name': f"{project_name}_bronze_ingestion_group_{pipeline_group}",
                'catalog': first_row['target_catalog'],
                'target': 'bronze_raw',  # Write to bronze_raw schema
                'libraries': [
                    {'notebook': {'path': bronze_notebook_path}}
                ],
                'configuration': {
                    'pi.pipeline.group': str(pipeline_group),  # NEW: Group ID
                    'pi.tags': ','.join(tags),
                    'pi.server.url': first_row['pi_server_url'],
                    'pi.connection.name': first_row['connection_name'],
                    'pi.target.catalog': first_row['target_catalog'],
                    'pi.target.schema': 'bronze_raw',
                    'pi.start_time_offset_days': str(first_row['start_time_offset_days'])
                }
            }

            # Add mode-specific configuration
            if ingestion_mode == "streaming":
                pipeline_config['continuous'] = True
                pipeline_config['channel'] = 'CURRENT'
                pipeline_config['serverless'] = True
            else:
                pipeline_config['continuous'] = False
                pipeline_config['serverless'] = True

            pipelines[pipeline_name] = pipeline_config

        # Create 1 silver pipeline (consolidation)
        silver_pipeline_name = f"{project_name}_silver_consolidation"
        pipelines[silver_pipeline_name] = {
            'name': f"{project_name}_silver_consolidation",
            'catalog': TARGET_CATALOG,
            'target': TARGET_SCHEMA,  # Write to final bronze schema
            'libraries': [
                {'notebook': {'path': silver_notebook_path}}
            ],
            'configuration': {
                'pi.target.catalog': TARGET_CATALOG,
                'pi.bronze_raw.schema': 'bronze_raw',
                'pi.silver.schema': TARGET_SCHEMA,
                'pi.num_pipeline_groups': str(num_groups)
            },
            'continuous': False,
            'serverless': True
        }

    else:
        # Original single-pipeline architecture (will have conflicts!)
        for pipeline_group in sorted(df['pipeline_group'].unique()):
            # ... original code ...
            pass

    return {'resources': {'pipelines': pipelines}}
```

### Update Jobs YAML Generation (Lines 351-377)

Add silver pipeline dependency:

```python
def create_jobs_yaml(df, project_name, use_bronze_silver=True):
    """Generate scheduled jobs YAML with dependencies."""
    jobs = {}

    if use_bronze_silver:
        # Create jobs for each bronze pipeline
        for pipeline_group in sorted(df['pipeline_group'].unique()):
            group_df = df[df['pipeline_group'] == pipeline_group]
            job_name = f"{project_name}_bronze_job_{pipeline_group}"
            pipeline_ref = f"{project_name}_bronze_group_{pipeline_group}"

            schedule = group_df.iloc[0]['schedule']

            jobs[job_name] = {
                'name': f"{project_name}_bronze_scheduler_group_{pipeline_group}",
                'schedule': {
                    'quartz_cron_expression': schedule,
                    'timezone_id': 'UTC',
                    'pause_status': 'UNPAUSED'
                },
                'tasks': [{
                    'task_key': f'run_bronze_pipeline_{pipeline_group}',
                    'pipeline_task': {
                        'pipeline_id': f'${{resources.pipelines.{pipeline_ref}.id}}'
                    }
                }]
            }

        # Create job for silver pipeline (runs AFTER bronze pipelines)
        silver_job_name = f"{project_name}_silver_job"
        silver_pipeline_ref = f"{project_name}_silver_consolidation"

        # Run silver 30 minutes after bronze pipelines finish
        silver_schedule = "0 30 * * * ?"  # Run at :30 past each hour

        jobs[silver_job_name] = {
            'name': f"{project_name}_silver_scheduler",
            'schedule': {
                'quartz_cron_expression': silver_schedule,
                'timezone_id': 'UTC',
                'pause_status': 'UNPAUSED'
            },
            'tasks': [{
                'task_key': 'run_silver_consolidation',
                'pipeline_task': {
                    'pipeline_id': f'${{resources.pipelines.{silver_pipeline_ref}.id}}'
                }
            }]
        }

    return {'resources': {'jobs': jobs}}
```

### Update YAML Generation Call (Line 384)

```python
# Change from:
pipelines_yaml = create_pipelines_yaml(config_df, PROJECT_NAME, PIPELINE_NOTEBOOK_PATH, INGESTION_MODE)

# To:
pipelines_yaml = create_pipelines_yaml(
    config_df,
    PROJECT_NAME,
    BRONZE_NOTEBOOK_PATH,  # NEW
    SILVER_NOTEBOOK_PATH,  # NEW
    INGESTION_MODE,
    USE_BRONZE_SILVER      # NEW
)

# And:
jobs_yaml = create_jobs_yaml(config_df, PROJECT_NAME, USE_BRONZE_SILVER)
```

## Deployment Steps

1. **Drop old tables** (to start fresh):
   ```sql
   DROP TABLE IF EXISTS osipi.bronze.pi_timeseries;
   DROP TABLE IF EXISTS osipi.bronze.pi_af_hierarchy;
   DROP TABLE IF EXISTS osipi.bronze.pi_event_frames;
   ```

2. **Delete old pipelines** in Databricks UI

3. **Update generate_pipelines notebook** with changes above

4. **Run generate_pipelines notebook**:
   - Will create 5 bronze pipelines + 1 silver pipeline
   - Each bronze writes to `osipi.bronze_raw.pi_timeseries_raw_group{N}`
   - Silver consolidates into `osipi.bronze.pi_timeseries`

5. **Deploy via DAB or UI**

6. **Run pipelines**:
   - Bronze pipelines run in parallel (no conflicts!)
   - Silver pipeline runs after bronze finishes
   - Final tables in `osipi.bronze` schema

## Benefits

✅ **No Ownership Conflicts**: Each bronze pipeline writes to its own table
✅ **Parallel Execution**: All 5 bronze pipelines run simultaneously
✅ **Deduplication**: Silver pipeline handles late-arriving data
✅ **Scalability**: Add more bronze pipelines easily
✅ **Standard Pattern**: Follows Databricks medallion architecture best practices

## Data Flow Example

**Bronze Pipelines (Parallel)**:
- Pipeline 1: Tags 1-2000 → `pi_timeseries_raw_group1`
- Pipeline 2: Tags 2001-4000 → `pi_timeseries_raw_group2`
- Pipeline 3: Tags 4001-6000 → `pi_timeseries_raw_group3`
- Pipeline 4: Tags 6001-8000 → `pi_timeseries_raw_group4`
- Pipeline 5: Tags 8001-10000 → `pi_timeseries_raw_group5`

**Silver Pipeline (Sequential)**:
```sql
SELECT * FROM pi_timeseries_raw_group1
UNION ALL
SELECT * FROM pi_timeseries_raw_group2
UNION ALL
SELECT * FROM pi_timeseries_raw_group3
UNION ALL
SELECT * FROM pi_timeseries_raw_group4
UNION ALL
SELECT * FROM pi_timeseries_raw_group5
-- Then deduplicate on (tag_webid, timestamp)
```

## Late-Arriving Data Handling

**Scenario**: Tag "Temperature-101" value for timestamp "2025-12-12 10:00:00" arrives late in multiple groups.

**What Happens**:
1. Pipeline 2 ingests it → `pi_timeseries_raw_group2`
2. Pipeline 3 also ingests it (overlapping window) → `pi_timeseries_raw_group3`
3. Silver pipeline sees both copies
4. Deduplication keeps the most recent ingestion
5. Final table has only ONE row for that (tag, timestamp)

This is how the bronze-silver pattern handles late data gracefully!
