# Multi-Pipeline DLT Architecture

## Overview

This document describes the multi-pipeline DLT architecture that eliminates table ownership conflicts by using per-pipeline bronze tables and a silver merge layer.

## Architecture Design

### Problem Statement

DLT managed tables (`@dlt.table()`) have single-owner semantics. When multiple pipelines tried to write to the same tables (e.g., `pi_timeseries`, `pi_af_hierarchy`, `pi_event_frames`), we encountered ownership conflicts:

```
Table 'osipi.bronze.pi_timeseries' is already managed by pipeline <id>.
A table can only be owned by one pipeline.
```

### Solution: Per-Pipeline Bronze + Silver Merge

**Bronze Layer (Multiple Pipelines)**:
- Each pipeline writes to its own set of tables
- Table naming: `{base_name}_pipeline{id}` (e.g., `pi_timeseries_pipeline1`, `pi_timeseries_pipeline2`)
- No ownership conflicts since each pipeline owns its tables
- Pipelines can run concurrently without coordination

**Silver Layer (Optional)**:
- This repo currently focuses on the **bronze per-pipeline tables** written by `pi_ingestion_pipeline`.
- A silver merge pipeline is a valid pattern, but the sample silver notebooks were removed to keep the repo aligned with the “pipelines are generated from the mock API” workflow.

## Table Structure

### Bronze Tables (Per-Pipeline)

Each pipeline creates three tables with pipeline suffix:

1. **`pi_timeseries_pipeline{id}`**
   - Partition: `partition_date`
   - Dedup Key: `(tag_webid, timestamp)`
   - Contains: Raw time-series data from PI Web API

2. **`pi_af_hierarchy_pipeline{id}`**
   - No partitions
   - Dedup Key: `element_id`
   - Contains: Asset Framework hierarchy metadata

3. **`pi_event_frames_pipeline{id}`**
   - Partition: `partition_date`
   - Dedup Key: `(event_frame_id, start_time)`
   - Contains: Event frames (alarms, batch runs)

### Silver Tables (Merged)

The silver merge pipeline creates three unified tables:

1. **`pi_timeseries`** (silver)
   - Merged from all `pi_timeseries_pipeline*` tables
   - Deduplication: Window function on `(tag_webid, timestamp)`, keeps most recent `ingestion_timestamp`
   - Additional column: `source_pipeline` (tracks which pipeline contributed the data)

2. **`pi_af_hierarchy`** (silver)
   - Merged from all `pi_af_hierarchy_pipeline*` tables
   - Deduplication: Window function on `element_id`, keeps most recent `ingestion_timestamp`
   - Additional column: `source_pipeline`

3. **`pi_event_frames`** (silver)
   - Merged from all `pi_event_frames_pipeline*` tables
   - Deduplication: Window function on `(event_frame_id, start_time)`, keeps most recent `ingestion_timestamp`
   - Additional column: `source_pipeline`

## Pipeline Configuration

### Bronze Ingestion Pipelines

**Configuration Parameters**:
```yaml
configuration:
  pi.pipeline.id: "1"  # Unique pipeline identifier
  pi.tags: "Sydney_Temp_01,Sydney_Pressure_02,..."  # Tags this pipeline handles
  pi.server.url: "https://osipi-webserver-..."
  pi.connection.name: "mock_pi_connection"
  pi.target.catalog: "osipi"
  pi.target.schema: "bronze"
  pi.start_time_offset_days: "30"
```

**Table Names Generated**:
- `osipi.bronze.pi_timeseries_pipeline1`
- `osipi.bronze.pi_af_hierarchy_pipeline1`
- `osipi.bronze.pi_event_frames_pipeline1`

### Silver Merge Pipeline (Optional)

If you need unified “silver” tables, create a separate pipeline that reads from `pi_*_pipeline*` bronze tables and merges/deduplicates into a silver schema. (Not included in this repo right now.)

## Deduplication Strategy

### Within Bronze (Per-Pipeline)

Each pipeline deduplicates within its own batch:

```python
.dropDuplicates(["tag_webid", "timestamp"])  # For timeseries
.dropDuplicates(["element_id"])  # For AF hierarchy
.dropDuplicates(["event_frame_id", "start_time"])  # For event frames
```

### Across Pipelines (Silver Merge)

Silver merge uses window functions to deduplicate across all pipelines:

```python
window_spec = Window.partitionBy("tag_webid", "timestamp").orderBy(col("ingestion_timestamp").desc())

return (all_data
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
    .withColumn("silver_timestamp", current_timestamp())
)
```

This keeps the most recently ingested record for each unique key combination.

## Data Quality Monitoring

The silver merge pipeline includes a data quality view:

```python
@dlt.view(name="data_quality_metrics")
def data_quality_metrics():
    """
    Calculate data quality metrics across all bronze tables:
    - Total records per pipeline
    - Distinct records per pipeline
    - Duplicate count per pipeline
    - Late arrivals (data >7 days old but recently ingested)
    """
```

## Deployment Workflow

### 1. Generate Pipeline Configurations

Run `notebooks/generate_pipelines_from_mock_api.py`:
- Discovers tags from mock PI API
- Groups tags by plant or custom logic
- Generates YAML configurations with unique `pi.pipeline.id`
- Outputs to `dab-config/pipelines.yml`

### 2. Deploy Bronze Pipelines

Run `notebooks/validate_and_deploy_dab.py`:
- Reads generated YAML configurations
- Creates DLT pipelines using Databricks SDK
- Each pipeline uses `src/notebooks/pi_ingestion_pipeline.py`
- Pipelines can run concurrently

### 3. Deploy Silver Merge Pipeline (Optional)

Create a dedicated merge pipeline if/when you want silver tables. (No merge notebook is shipped in this repo currently.)

### 4. Schedule Coordination

**Bronze Pipelines**:
- Schedule: Hourly or continuous
- Each writes to its own tables

**Silver Merge Pipeline**:
- Schedule: 15 minutes after bronze pipelines complete
- Reads from all bronze tables
- Writes to silver tables

## Benefits

1. **No Ownership Conflicts**: Each pipeline owns its own tables
2. **Concurrent Execution**: Pipelines don't block each other
3. **Scalable**: Add more pipelines without coordination
4. **Flexible Distribution**: Distribute tags by plant, volume, or custom logic
5. **Automatic Discovery**: Silver merge automatically finds new bronze pipelines
6. **Deduplication**: Silver layer handles overlapping data from multiple pipelines
7. **Audit Trail**: `source_pipeline` column tracks data lineage

## Configuration Flexibility

You can distribute tags across pipelines in multiple ways:

### By Plant/Location
```python
pipeline_1_tags = ["Sydney_Temp_01", "Sydney_Pressure_02", ...]
pipeline_2_tags = ["Melbourne_Temp_01", "Melbourne_Pressure_02", ...]
```

### By Tag Count
```python
TAGS_PER_PIPELINE = 50
pipeline_1_tags = all_tags[0:50]
pipeline_2_tags = all_tags[50:100]
```

### By Data Volume
```python
high_volume_tags = ["Tag1", "Tag2", ...]  # High-frequency tags
low_volume_tags = ["Tag3", "Tag4", ...]   # Low-frequency tags
```

### Custom Logic
```python
critical_tags = get_critical_tags()
non_critical_tags = get_non_critical_tags()
```

## File References

- **Bronze Ingestion**: `src/notebooks/pi_ingestion_pipeline.py`
- **Configuration Generator**: `notebooks/generate_pipelines_from_mock_api.py`

## Migration from Old Architecture

### Old Architecture (Shared Tables)
```
Pipeline 1 → pi_timeseries ← Pipeline 2 (CONFLICT!)
```

### New Architecture (Per-Pipeline + Merge)
```
Pipeline 1 → pi_timeseries_pipeline1 ↘
                                       Silver Merge → pi_timeseries
Pipeline 2 → pi_timeseries_pipeline2 ↗
```

### Migration Steps

1. Delete old pipelines (done)
2. Drop old bronze tables (done)
3. Deploy new bronze pipelines with pipeline_id
4. Deploy silver merge pipeline
5. Update downstream consumers to read from silver schema

## Monitoring and Observability

### Bronze Layer
- Each pipeline has independent metrics
- Monitor: Pipeline run status, row counts, errors
- Check: `osipi.bronze.pi_timeseries_pipeline{id}` for data

### Silver Layer
- Unified metrics across all pipelines
- Monitor: Deduplication ratios, late arrivals, pipeline contribution
- Check: `osipi.silver.data_quality_metrics` view

### Dashboard Updates
- Current: Reads from `osipi.bronze`
- Future: Update to read from `osipi.silver` for clean, deduplicated data
