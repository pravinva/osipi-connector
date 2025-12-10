# Databricks notebook source
# MAGIC %md
# MAGIC # Dynamic Pipeline Generator from Mock PI API
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Discovers all tags from the mock PI Web API
# MAGIC 2. Groups tags into pipelines based on load balancing strategy
# MAGIC 3. Generates CSV config for DAB pipeline generator
# MAGIC 4. Optionally generates and deploys the DAB YAML
# MAGIC
# MAGIC **Run this notebook whenever you want to update pipeline configuration**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import requests
import pandas as pd
from datetime import datetime
import os

# Mock API Configuration
MOCK_API_URL = "https://osipi-webserver-1444828305810485.aws.databricksapps.com"
DATASERVERS_ENDPOINT = f"{MOCK_API_URL}/piwebapi/dataservers"

# Pipeline Configuration
PROJECT_NAME = "osipi_demo"
TARGET_CATALOG = "osipi"
TARGET_SCHEMA = "bronze"
CONNECTION_NAME = "mock_pi_connection"  # Will use mock API, no real auth needed

# Load Balancing Strategy
TAGS_PER_PIPELINE = 100  # Number of tags per pipeline
SCHEDULE_15MIN = "0 */15 * * * ?"  # Every 15 minutes (Quartz cron)
SCHEDULE_30MIN = "0 */30 * * * ?"  # Every 30 minutes
SCHEDULE_HOURLY = "0 0 * * * ?"    # Every hour

# Output paths
OUTPUT_CSV = "/Workspace/Users/pravin.varma@databricks.com/osipi_pipeline_config.csv"
OUTPUT_YAML_DIR = "/Workspace/Users/pravin.varma@databricks.com/dab_resources"

print(f"Mock API: {MOCK_API_URL}")
print(f"Target: {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"Output CSV: {OUTPUT_CSV}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Discover Tags from Mock API

# COMMAND ----------

# Get data server
print("Fetching data server info...")
response = requests.get(DATASERVERS_ENDPOINT)
dataservers = response.json()["Items"]
server_webid = dataservers[0]["WebId"]
server_name = dataservers[0]["Name"]

print(f"✓ Data Server: {server_name} ({server_webid})")

# Get all PI points
print("\nFetching all PI points...")
points_url = f"{MOCK_API_URL}/piwebapi/dataservers/{server_webid}/points?maxCount=100000"
response = requests.get(points_url)
points = response.json()["Items"]

print(f"✓ Found {len(points)} PI points\n")

# Convert to DataFrame
tags_df = pd.DataFrame([{
    'tag_name': p['Name'],
    'tag_webid': p['WebId'],
    'descriptor': p.get('Descriptor', ''),
    'units': p.get('EngineeringUnits', '')
} for p in points])

print("Sample tags:")
print(tags_df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Categorize Tags by Priority

# COMMAND ----------

# Extract metadata from tag names (format: Plant_UnitXXX_SensorType_PV)
def categorize_tag(tag_name):
    """Categorize tag priority based on sensor type."""
    parts = tag_name.split('_')

    if len(parts) >= 3:
        sensor_type = parts[2]

        # High priority: Temperature, Pressure (critical safety)
        if sensor_type in ['Temperature', 'Pressure']:
            return 'high', SCHEDULE_15MIN

        # Medium priority: Flow, Level
        elif sensor_type in ['Flow', 'Level']:
            return 'medium', SCHEDULE_30MIN

        # Low priority: Power, Speed, Voltage
        else:
            return 'low', SCHEDULE_HOURLY

    return 'medium', SCHEDULE_30MIN

tags_df[['priority', 'schedule']] = tags_df['tag_name'].apply(
    lambda x: pd.Series(categorize_tag(x))
)

print("Tags by Priority:")
print(tags_df['priority'].value_counts())
print("\nTags by Schedule:")
print(tags_df['schedule'].value_counts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load Balance into Pipeline Groups

# COMMAND ----------

# Sort by priority (high first) then alphabetically
tags_df = tags_df.sort_values(['priority', 'tag_name'])

# Assign pipeline group (round-robin within each priority)
pipeline_groups = []
current_group = 1
tags_in_current_group = 0

for _, row in tags_df.iterrows():
    pipeline_groups.append(current_group)
    tags_in_current_group += 1

    if tags_in_current_group >= TAGS_PER_PIPELINE:
        current_group += 1
        tags_in_current_group = 0

tags_df['pipeline_group'] = pipeline_groups

print(f"Created {tags_df['pipeline_group'].max()} pipeline groups")
print(f"Average tags per pipeline: {len(tags_df) / tags_df['pipeline_group'].max():.1f}")

# Show distribution
print("\nTags per Pipeline Group:")
pipeline_dist = tags_df.groupby('pipeline_group').agg({
    'tag_webid': 'count',
    'priority': 'first',
    'schedule': 'first'
}).rename(columns={'tag_webid': 'tag_count'})
print(pipeline_dist)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Generate DAB Configuration CSV

# COMMAND ----------

# Create configuration dataframe
config_df = tags_df.copy()
config_df['pi_server_url'] = MOCK_API_URL
config_df['connection_name'] = CONNECTION_NAME
config_df['target_catalog'] = TARGET_CATALOG
config_df['target_schema'] = TARGET_SCHEMA
config_df['start_time_offset_days'] = 7  # 7 days initial backfill

# Reorder columns to match DAB generator expectations
config_df = config_df[[
    'tag_name',
    'tag_webid',
    'pi_server_url',
    'connection_name',
    'target_catalog',
    'target_schema',
    'pipeline_group',
    'schedule',
    'start_time_offset_days'
]]

# Save to Workspace
config_df.to_csv(OUTPUT_CSV.replace('/Workspace', '/dbfs/Workspace'), index=False)

print(f"✓ Saved configuration to: {OUTPUT_CSV}")
print(f"\nTotal records: {len(config_df)}")
print(f"Pipeline groups: {config_df['pipeline_group'].nunique()}")
print(f"\nFirst few rows:")
display(config_df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Generate DAB YAML Files

# COMMAND ----------

# Create output directory for YAML files
os.makedirs(OUTPUT_YAML_DIR.replace('/Workspace', '/dbfs/Workspace'), exist_ok=True)

# Now we'd normally call the generate_dab_yaml.py script
# Since we're in a notebook, let's inline the YAML generation

import yaml
from collections import defaultdict

def create_pipelines_yaml(df, project_name):
    """Generate DLT pipelines YAML."""
    pipelines = {}

    for pipeline_group in sorted(df['pipeline_group'].unique()):
        group_df = df[df['pipeline_group'] == pipeline_group]
        pipeline_name = f"{project_name}_pipeline_{pipeline_group}"

        first_row = group_df.iloc[0]
        tags = group_df['tag_webid'].tolist()

        pipelines[pipeline_name] = {
            'name': f"{project_name}_ingestion_group_{pipeline_group}",
            'catalog': first_row['target_catalog'],
            'target': first_row['target_schema'],
            'clusters': [{
                'label': 'default',
                'node_type_id': 'i3.xlarge',
                'num_workers': 2
            }],
            'libraries': [
                {'notebook': {'path': '../src/notebooks/pi_ingestion_pipeline.py'}}
            ],
            'configuration': {
                'pi.tags': ','.join(tags),
                'pi.server.url': first_row['pi_server_url'],
                'pi.connection.name': first_row['connection_name'],
                'pi.target.catalog': first_row['target_catalog'],
                'pi.target.schema': first_row['target_schema'],
                'pi.start_time_offset_days': str(first_row['start_time_offset_days'])
            }
        }

    return {'resources': {'pipelines': pipelines}}

def create_jobs_yaml(df, project_name):
    """Generate scheduled jobs YAML."""
    jobs = {}

    for pipeline_group in sorted(df['pipeline_group'].unique()):
        group_df = df[df['pipeline_group'] == pipeline_group]
        job_name = f"{project_name}_job_{pipeline_group}"
        pipeline_ref = f"{project_name}_pipeline_{pipeline_group}"

        schedule = group_df.iloc[0]['schedule']

        jobs[job_name] = {
            'name': f"{project_name}_scheduler_group_{pipeline_group}",
            'schedule': {
                'quartz_cron_expression': schedule,
                'timezone_id': 'UTC',
                'pause_status': 'UNPAUSED'
            },
            'tasks': [{
                'task_key': f'run_pipeline_{pipeline_group}',
                'pipeline_task': {
                    'pipeline_id': f'${{resources.pipelines.{pipeline_ref}.id}}'
                }
            }]
        }

    return {'resources': {'jobs': jobs}}

# Generate YAMLs
pipelines_yaml = create_pipelines_yaml(config_df, PROJECT_NAME)
jobs_yaml = create_jobs_yaml(config_df, PROJECT_NAME)

# Write files
pipelines_file = f"{OUTPUT_YAML_DIR}/pipelines.yml"
jobs_file = f"{OUTPUT_YAML_DIR}/jobs.yml"

with open(pipelines_file.replace('/Workspace', '/dbfs/Workspace'), 'w') as f:
    yaml.dump(pipelines_yaml, f, default_flow_style=False, sort_keys=False)

with open(jobs_file.replace('/Workspace', '/dbfs/Workspace'), 'w') as f:
    yaml.dump(jobs_yaml, f, default_flow_style=False, sort_keys=False)

print(f"✓ Generated YAML files:")
print(f"  - {pipelines_file}")
print(f"  - {jobs_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary & Next Steps

# COMMAND ----------

print("=" * 80)
print("Pipeline Generation Complete!")
print("=" * 80)
print(f"\n✓ Discovered {len(tags_df)} tags from mock API")
print(f"✓ Created {config_df['pipeline_group'].nunique()} pipeline groups")
print(f"✓ Generated configuration CSV: {OUTPUT_CSV}")
print(f"✓ Generated DAB YAML files: {OUTPUT_YAML_DIR}")

print("\n" + "=" * 80)
print("Pipeline Distribution:")
print("=" * 80)

for pipeline_group in sorted(config_df['pipeline_group'].unique()):
    group_df = config_df[config_df['pipeline_group'] == pipeline_group]
    tag_count = len(group_df)
    schedule = group_df.iloc[0]['schedule']
    priority = group_df.iloc[0]['priority']

    print(f"Pipeline {pipeline_group:2d}: {tag_count:4d} tags | Priority: {priority:6s} | Schedule: {schedule}")

print("\n" + "=" * 80)
print("Next Steps:")
print("=" * 80)
print("1. Review generated files in your Workspace")
print("2. Update project databricks.yml to include these resources:")
print(f"   include:")
print(f"     - {pipelines_file}")
print(f"     - {jobs_file}")
print("3. Deploy DAB:")
print("   databricks bundle validate -t dev")
print("   databricks bundle deploy -t dev")
print("4. Monitor pipelines in Databricks UI")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: Display Sample YAML

# COMMAND ----------

print("Sample pipelines.yml:")
print("-" * 80)
print(yaml.dump(pipelines_yaml, default_flow_style=False, sort_keys=False)[:1000])
print("\n... (truncated)")
