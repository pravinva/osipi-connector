# Pipeline Generator - Configuration Guide

## Overview

The `generate_pipelines_from_mock_api.py` notebook dynamically creates Databricks Asset Bundle (DAB) pipeline configurations based on tags discovered from the mock PI Web API.

## Configurable Parameters

### Tag Limits

```python
MAX_TAGS_TO_FETCH = None  # Set to limit tags for testing
```

| Value | Result | Use Case |
|-------|--------|----------|
| `None` | All tags (~30,000) | Production |
| `300` | First 300 tags | Quick test (3 pipelines) |
| `1000` | First 1000 tags | Medium test (10 pipelines) |
| `5000` | First 5000 tags | Large test (50 pipelines) |

### Pipeline Sizing

```python
TAGS_PER_PIPELINE = 100  # Tags grouped per pipeline
```

| Value | Impact | Example (1000 tags) |
|-------|--------|---------------------|
| `50` | More pipelines, faster per pipeline | 20 pipelines |
| `100` | Balanced (recommended) | 10 pipelines |
| `200` | Fewer pipelines, longer per pipeline | 5 pipelines |
| `500` | Very few pipelines | 2 pipelines |

**Trade-offs:**
- **Smaller groups (50-100 tags):** Faster pipeline execution, more parallelism, but more management overhead
- **Larger groups (200-500 tags):** Fewer pipelines to manage, but longer execution times

### Schedules

```python
SCHEDULE_15MIN = "0 */15 * * * ?"  # High priority
SCHEDULE_30MIN = "0 */30 * * * ?"  # Medium priority
SCHEDULE_HOURLY = "0 0 * * * ?"    # Low priority
```

Automatically assigned based on sensor type:
- **High (15 min):** Temperature, Pressure (safety-critical)
- **Medium (30 min):** Flow, Level (operational)
- **Low (1 hour):** Power, Speed, Voltage (monitoring)

### Deployment

```python
AUTO_DEPLOY_DAB = True  # Automatically deploy after generation
DAB_TARGET = "dev"      # Target environment
```

| Setting | Behavior |
|---------|----------|
| `AUTO_DEPLOY_DAB = True` | Copies YAML to project, shows deployment command |
| `AUTO_DEPLOY_DAB = False` | Only generates YAML, manual deployment |
| `DAB_TARGET = "dev"` | Deploy to development workspace |
| `DAB_TARGET = "prod"` | Deploy to production workspace |

## Example Configurations

### Quick Test (3 pipelines)
```python
MAX_TAGS_TO_FETCH = 300
TAGS_PER_PIPELINE = 100
AUTO_DEPLOY_DAB = True
DAB_TARGET = "dev"
```
**Result:** 3 pipelines, ~30 seconds to generate, fast deployment

### Medium Scale (10 pipelines)
```python
MAX_TAGS_TO_FETCH = 1000
TAGS_PER_PIPELINE = 100
AUTO_DEPLOY_DAB = True
DAB_TARGET = "dev"
```
**Result:** 10 pipelines, balanced testing

### Production Scale (300+ pipelines)
```python
MAX_TAGS_TO_FETCH = None  # All 30,000 tags
TAGS_PER_PIPELINE = 100
AUTO_DEPLOY_DAB = False  # Review before deploying
DAB_TARGET = "prod"
```
**Result:** 300 pipelines, full production setup

## Pipeline Distribution Example

For 1000 tags with `TAGS_PER_PIPELINE = 100`:

```
Priority    Sensor Types          Schedule      Pipelines  Tags
──────────────────────────────────────────────────────────────
High        Temperature,Pressure  Every 15 min  5          500
Medium      Flow,Level            Every 30 min  3          300
Low         Power,Speed,Voltage   Every hour    2          200
──────────────────────────────────────────────────────────────
TOTAL                                           10         1000
```

## Workflow

1. **Configure** parameters at top of notebook
2. **Run** notebook - discovers tags from mock API
3. **Review** generated `pipelines.yml` and `jobs.yml`
4. **Deploy** using one of:
   - Notebook auto-copy (if `AUTO_DEPLOY_DAB = True`)
   - Manual: `./deployment/deploy_dab.sh dev`
   - Terminal: `databricks bundle deploy -t dev`

## Regenerating Pipelines

When to regenerate:
- New tags added to mock API
- Changed priority for sensor types
- Adjusted `TAGS_PER_PIPELINE` for performance tuning
- Updated schedules

**Steps:**
1. Update configuration in notebook
2. Re-run notebook
3. Redeploy DAB

## Performance Tuning

### Too Many Pipelines?
Increase `TAGS_PER_PIPELINE`:
```python
TAGS_PER_PIPELINE = 200  # Half as many pipelines
```

### Pipelines Running Too Long?
Decrease `TAGS_PER_PIPELINE`:
```python
TAGS_PER_PIPELINE = 50   # Double the pipelines, faster execution
```

### Want Different Schedules?
Modify sensor categorization in notebook:
```python
def categorize_tag(tag_name):
    # Customize logic here
    if sensor_type in ['Temperature']:
        return 'critical', SCHEDULE_5MIN  # More frequent
```

## Cost Optimization

| Configuration | Compute Cost | Data Freshness |
|---------------|--------------|----------------|
| 300 pipelines @ 15 min | Higher (more clusters) | Real-time |
| 30 pipelines @ 15 min | Lower | Near real-time |
| 30 pipelines @ 1 hour | Lowest | Hourly batch |

**Recommendation:** Start with `TAGS_PER_PIPELINE = 100` and adjust based on observed performance.
