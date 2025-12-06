# PI Web API Deployment - Load-Balanced Pipeline Generator

This directory contains tooling to generate Databricks Asset Bundle (DAB) configurations for load-balanced PI Web API ingestion pipelines.

## Overview

Instead of a single pipeline handling 30,000+ tags (which could overwhelm resources), this approach:
- **Distributes tags** across multiple DLT pipelines (pipeline groups)
- **Schedules independently** - different groups can run on different schedules
- **Scales horizontally** - add more pipeline groups as tag count grows
- **Prevents overload** - no single pipeline becomes a bottleneck

## Quick Start

### 1. Create Tag Configuration CSV

Create a CSV file with your tags and pipeline assignments:

```csv
tag_name,tag_webid,pi_server_url,connection_name,target_catalog,target_schema,pipeline_group,schedule,start_time_offset_days
Plant1_Temp,F1DP-TAG-001,https://pi.company.com/piwebapi,pi_conn,main,bronze,1,0 */15 * * * ?,30
Plant1_Pressure,F1DP-TAG-002,https://pi.company.com/piwebapi,pi_conn,main,bronze,1,0 */15 * * * ?,30
Plant2_Temp,F1DP-TAG-003,https://pi.company.com/piwebapi,pi_conn,main,bronze,2,0 */30 * * * ?,30
```

**Column Descriptions:**
- `tag_name`: Display name for the tag
- `tag_webid`: PI Web API WebID (e.g., F1DP-TAG-001)
- `pi_server_url`: PI Web API base URL
- `connection_name`: Databricks connection/secret scope name
- `target_catalog`: Unity Catalog catalog name
- `target_schema`: Schema name for Delta tables
- `pipeline_group`: Pipeline group number (1, 2, 3, ...)
- `schedule`: Quartz cron expression for scheduling
- `start_time_offset_days`: Days to lookback for initial historical load

### 2. Generate DAB YAML

Run the generator script:

```bash
python generate_dab_yaml.py examples/pi_tags_config.csv --project my_pi_project
```

**Output:**
- `resources/pipelines.yml` - DLT pipeline configurations
- `resources/jobs.yml` - Scheduled job configurations

### 3. Review Generated YAML

Check the generated YAML files in `resources/`:

```yaml
# resources/pipelines.yml
resources:
  pipelines:
    my_pi_project_pipeline_pi_ingestion_1:
      name: my_pi_project_pi_ingestion_group_1
      catalog: main
      target: bronze
      configuration:
        pi.tags: F1DP-TAG-001,F1DP-TAG-002,F1DP-TAG-003
        pi.server.url: https://pi.company.com/piwebapi
        ...

# resources/jobs.yml
resources:
  jobs:
    my_pi_project_job_pi_scheduler_1:
      name: my_pi_project_pi_scheduler_group_1
      schedule:
        quartz_cron_expression: 0 */15 * * * ?
      ...
```

### 4. Deploy with Databricks Asset Bundles

```bash
# From project root
databricks bundle deploy -t dev

# Or for production
databricks bundle deploy -t prod
```

## Load Balancing Strategy

### Manual Distribution (Current)

Manually assign tags to pipeline groups in the CSV:

```csv
# High-frequency tags (every 15 min) → Group 1
Plant1_Temp,...,1,0 */15 * * * ?,30
Plant1_Pressure,...,1,0 */15 * * * ?,30

# Medium-frequency tags (every 30 min) → Group 2
Plant2_Temp,...,2,0 */30 * * * ?,30
Plant2_Pressure,...,2,0 */30 * * * ?,30

# Low-frequency tags (hourly) → Group 3
Plant3_Temp,...,3,0 0 */1 * * ?,30
```

**Benefits:**
- Simple and predictable
- Group by frequency or business unit
- Full control over distribution

### Example Distributions

**By Frequency:**
- Group 1 (every 15 min): Critical tags, high-frequency monitoring
- Group 2 (every 30 min): Standard operational tags
- Group 3 (hourly): Historical trend analysis tags

**By Volume:**
- Group 1: 100 high-volume tags (10K records/day each)
- Group 2: 200 medium-volume tags (1K records/day each)
- Group 3: 500 low-volume tags (<100 records/day each)

**By Business Unit:**
- Group 1: Plant 1 tags (all frequencies)
- Group 2: Plant 2 tags (all frequencies)
- Group 3: Plant 3 tags (all frequencies)

## CSV Format Reference

### Required Columns

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `tag_name` | string | Display name | `Plant1_Temperature` |
| `tag_webid` | string | PI Web API WebID | `F1DP-TAG-001` |
| `pi_server_url` | string | PI Web API URL | `https://pi.company.com/piwebapi` |
| `connection_name` | string | Databricks connection | `pi_connection_prod` |
| `target_catalog` | string | Unity Catalog catalog | `main` |
| `target_schema` | string | Target schema | `bronze` |
| `pipeline_group` | int | Pipeline group (1, 2, 3, ...) | `1` |
| `schedule` | string | Quartz cron expression | `0 */15 * * * ?` |
| `start_time_offset_days` | int | Historical lookback days | `30` |

### Quartz Cron Expression Examples

| Expression | Description |
|------------|-------------|
| `0 */15 * * * ?` | Every 15 minutes |
| `0 */30 * * * ?` | Every 30 minutes |
| `0 0 */1 * * ?` | Every hour |
| `0 0 */6 * * ?` | Every 6 hours |
| `0 0 0 * * ?` | Daily at midnight |

## Project Structure

```
deployment/
├── README.md                      # This file
├── generate_dab_yaml.py           # YAML generator script
├── examples/
│   └── pi_tags_config.csv         # Example configuration
└── resources/                     # Generated YAML output
    ├── pipelines.yml              # DLT pipeline configs
    └── jobs.yml                   # Scheduled job configs
```

## Example: 30K Tags Across 10 Pipelines

```csv
# Group 1: 3,000 tags
tag_1,F1DP-TAG-0001,...,1,0 */15 * * * ?,30
tag_2,F1DP-TAG-0002,...,1,0 */15 * * * ?,30
...
tag_3000,F1DP-TAG-3000,...,1,0 */15 * * * ?,30

# Group 2: 3,000 tags
tag_3001,F1DP-TAG-3001,...,2,0 */15 * * * ?,30
...

# Group 10: 3,000 tags
tag_27001,F1DP-TAG-27001,...,10,0 */15 * * * ?,30
...
tag_30000,F1DP-TAG-30000,...,10,0 */15 * * * ?,30
```

**Result:**
- 10 DLT pipelines, each handling 3,000 tags
- Each pipeline uses 2 workers (configurable)
- Scheduled independently (can stagger start times)
- Total compute: 20 workers (vs overwhelming a single pipeline)

## Advanced: Staggered Scheduling

To avoid all pipelines starting simultaneously:

```csv
# Group 1 starts at :00
...,1,0 0 * * * ?,30

# Group 2 starts at :15
...,2,0 15 * * * ?,30

# Group 3 starts at :30
...,3,0 30 * * * ?,30

# Group 4 starts at :45
...,4,0 45 * * * ?,30
```

## Troubleshooting

### Pipeline group not created
- Check CSV has tags assigned to that group
- Verify pipeline_group column is numeric (1, 2, 3, ...)

### YAML syntax errors
- Ensure CSV columns match required schema
- Check for special characters in tag names
- Verify Quartz cron expressions are valid

### Tags not appearing in pipeline
- Confirm tag_webid is correct PI Web API format
- Check that tags are assigned to the correct pipeline_group

## Next Steps

1. **Test with small dataset** - Start with 10-20 tags across 2-3 groups
2. **Monitor performance** - Check pipeline run times and resource usage
3. **Scale up** - Add more tags and pipeline groups as needed
4. **Optimize schedules** - Adjust cron expressions based on business needs

## Related Documentation

- [PI Web API Connector README](../README.md)
- [Developer Specification](../docs/pi_connector_dev.md)
- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [DLT Pipeline Documentation](https://docs.databricks.com/delta-live-tables/index.html)
