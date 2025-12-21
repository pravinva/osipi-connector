# Databricks notebook source
# MAGIC %md
# MAGIC # Dynamic Pipeline Generator from Mock PI API
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Discovers all tags from the mock PI Web API
# MAGIC 2. Assigns priorities based on sensor type (configurable via SENSOR_PRIORITY_MAP)
# MAGIC 3. Groups tags into pipelines based on load balancing strategy
# MAGIC 4. Generates DAB YAML files (pipelines.yml, jobs.yml)
# MAGIC
# MAGIC **Run this notebook whenever you want to update pipeline configuration**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install pyyaml

# COMMAND ----------

# Restart Python to pick up new packages
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Re-import after restart
import requests
import pandas as pd
from datetime import datetime
import os

# Mock API Configuration
MOCK_API_URL = "https://osipi-webserver-1444828305810485.aws.databricksapps.com"
DATASERVERS_ENDPOINT = f"{MOCK_API_URL}/piwebapi/dataservers"

# Pipeline Configuration (CONFIGURABLE)
PROJECT_NAME = "osipi_demo"
TARGET_CATALOG = "osipi"
TARGET_SCHEMA = "bronze"
CONNECTION_NAME = "mock_pi_connection"  # Will use mock API, no real auth needed

# Notebook path - MUST be absolute Workspace path WITHOUT .py extension
# For Repos: /Workspace/Repos/<username>/<repo-name>/src/notebooks/pi_ingestion_pipeline_from_lakeflow_source
# For Users: /Workspace/Users/<email>/osipi-connector/src/notebooks/pi_ingestion_pipeline_from_lakeflow_source
#
# This notebook uses the merged Python Data Source artifact (lakeflow_connect) and writes via DLT.
PIPELINE_NOTEBOOK_PATH = "/Workspace/Users/pravin.varma@databricks.com/osipi-connector/src/notebooks/pi_ingestion_pipeline_from_lakeflow_source"

# Ingestion Mode (CONFIGURABLE)
# - "streaming": Continuous ingestion using DLT continuous mode (real-time, auto-scaling)
# - "batch": Scheduled batch ingestion (triggered mode, cost-optimized)
INGESTION_MODE = "batch"  # Options: "streaming" or "batch"

# Load Balancing Strategy (CONFIGURABLE)
# Max tags to fetch from API (None = all tags, or set limit like 1000)
MAX_TAGS_TO_FETCH = None

# IMPORTANT: Controls number of pipelines created!
# Formula: Number of Pipelines = Total Tags / TAGS_PER_PIPELINE
# Examples:
#   - TAGS_PER_PIPELINE = 10000 → 1 pipeline (all tags in one pipeline)
#   - TAGS_PER_PIPELINE = 2000  → 5 pipelines (recommended for demo)
#   - TAGS_PER_PIPELINE = 1000  → 10 pipelines
#   - TAGS_PER_PIPELINE = 100   → 100 pipelines (for production scale)
TAGS_PER_PIPELINE = 2000   # ← CHANGE THIS to control pipeline count

# Batch Mode Schedules (only used when INGESTION_MODE = "batch")
SCHEDULE_15MIN = "0 */15 * * * ?"  # Every 15 minutes (Quartz cron)
SCHEDULE_30MIN = "0 */30 * * * ?"  # Every 30 minutes
SCHEDULE_HOURLY = "0 0 * * * ?"    # Every hour
DEFAULT_BATCH_SCHEDULE = SCHEDULE_30MIN  # Default for batch pipelines

# ============================================================================
# PRIORITY CONFIGURATION - Edit these mappings to change tag priorities
# ============================================================================
SENSOR_PRIORITY_MAP = {
    # Sensor Type: (Priority, Schedule)
    'Temperature': ('high', SCHEDULE_15MIN),
    'Pressure': ('high', SCHEDULE_15MIN),
    'Flow': ('medium', SCHEDULE_30MIN),
    'Level': ('medium', SCHEDULE_30MIN),
    'Power': ('low', SCHEDULE_HOURLY),
    'Speed': ('low', SCHEDULE_HOURLY),
    'Voltage': ('low', SCHEDULE_HOURLY),
    'Current': ('low', SCHEDULE_HOURLY),
}

# Default priority for unrecognized sensor types
DEFAULT_PRIORITY = ('medium', SCHEDULE_30MIN)

# ============================================================================
# PLANT SELECTION - Choose which plants to include in pipelines
# ============================================================================
# Available plants (10 total) in the mock API
AVAILABLE_PLANTS = ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide",
                   "Darwin", "Hobart", "Canberra", "Newcastle", "Wollongong"]

# CONFIGURABLE: Select which plants to use
# Option 1: Specify exact plants you want (recommended)
SELECTED_PLANTS = ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide", "Darwin", "Hobart"]  # 7 plants

# Option 2: Auto-select first N plants (comment out SELECTED_PLANTS above and uncomment below)
# NUM_PIPELINES_DESIRED = 7
# SELECTED_PLANTS = AVAILABLE_PLANTS[:NUM_PIPELINES_DESIRED]

# Option 3: Use all available plants (10 pipelines)
# SELECTED_PLANTS = AVAILABLE_PLANTS

# Each selected plant will create ONE pipeline with ~1000 tags
# Total tags ingested = len(SELECTED_PLANTS) × 1000

# Cluster Configuration
# All pipelines use serverless compute (no need to configure workers/autoscaling)

# Output paths
OUTPUT_YAML_DIR = "/Workspace/Users/pravin.varma@databricks.com/osipi-connector/dab-config"

print(f"Mock API: {MOCK_API_URL}")
print(f"Target: {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"Output YAML: {OUTPUT_YAML_DIR}")
print(f"Selected Plants: {len(SELECTED_PLANTS)} → {', '.join(SELECTED_PLANTS)}")
print(f"Expected Pipelines: {len(SELECTED_PLANTS)} (one per plant)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Authentication Setup

# COMMAND ----------

# NOTE:
# Databricks Apps require OAuth tokens. Use a Service Principal (client credentials)
# that has been granted "Can Use" permission on the App.
print("Authenticating to Databricks App using Service Principal OAuth (OIDC client-credentials)...")
print(f"URL: {MOCK_API_URL}")

CLIENT_ID = dbutils.secrets.get(scope="sp-osipi", key="sp-client-id")
CLIENT_SECRET = dbutils.secrets.get(scope="sp-osipi", key="sp-client-secret")

# IMPORTANT: In serverless / notebooks, context.apiUrl() can be a control-plane URL.
# The OIDC token endpoint must be called on the workspace host.
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
workspace_host = None
try:
    workspace_host = 'https://' + ctx.browserHostName().get()
except Exception:
    workspace_host = None
if not workspace_host:
    try:
        workspace_host = spark.conf.get('spark.databricks.workspaceUrl')
    except Exception:
        workspace_host = None
if not workspace_host:
    workspace_host = ctx.apiUrl().get()
workspace_host = workspace_host.rstrip('/')
if not workspace_host.startswith('http://') and not workspace_host.startswith('https://'):
    workspace_host = 'https://' + workspace_host

token_url = f"{workspace_host}/oidc/v1/token"

token_resp = requests.post(
    token_url,
    data={
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'scope': 'all-apis',
    },
    headers={'Content-Type': 'application/x-www-form-urlencoded'},
    timeout=30,
)

token_resp.raise_for_status()
access_token = token_resp.json().get('access_token')
if not access_token:
    raise RuntimeError('OIDC token endpoint did not return access_token')

headers = {'Authorization': f'Bearer {access_token}'}

print("✓ Authentication configured (SP OAuth)")
print(f"  Client ID: {CLIENT_ID[:8]}...")
print(f"  Headers: {list(headers.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Discover Tags from Mock API

# COMMAND ----------

# Get data server
print("Fetching data server info...")
response = requests.get(DATASERVERS_ENDPOINT, headers=headers)
dataservers = response.json()["Items"]
server_webid = dataservers[0]["WebId"]
server_name = dataservers[0]["Name"]

print(f"✓ Data Server: {server_name} ({server_webid})")

# Get PI points filtered by selected plants
print(f"\nFetching PI points for {len(SELECTED_PLANTS)} selected plants...")
print("=" * 60)

all_points = []
for plant in SELECTED_PLANTS:
    plant_url = f"{MOCK_API_URL}/piwebapi/dataservers/{server_webid}/points"
    params = {"nameFilter": f"{plant}_*", "maxCount": 10000}
    response = requests.get(plant_url, params=params, headers=headers)
    plant_points = response.json()["Items"]
    all_points.extend(plant_points)
    print(f"  ✓ {plant:12s}: {len(plant_points):4d} tags")

points = all_points
print("=" * 60)
print(f"✓ Total fetched: {len(points)} PI points from {len(SELECTED_PLANTS)} plants\n")

# Convert to DataFrame with plant information
tags_df = pd.DataFrame([{
    'tag_name': p['Name'],
    'tag_webid': p['WebId'],
    'plant': p['Name'].split('_')[0],  # Extract plant from tag name (e.g., "Sydney_Unit001_Temperature_PV" → "Sydney")
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
    """Categorize tag priority based on sensor type using SENSOR_PRIORITY_MAP."""
    parts = tag_name.split('_')

    if len(parts) >= 3:
        sensor_type = parts[2]
        # Look up priority and schedule from configuration map
        return SENSOR_PRIORITY_MAP.get(sensor_type, DEFAULT_PRIORITY)

    return DEFAULT_PRIORITY

tags_df[['priority', 'schedule']] = tags_df['tag_name'].apply(
    lambda x: pd.Series(categorize_tag(x))
)

print("Tags by Priority:")
print(tags_df['priority'].value_counts())
print("\nTags by Schedule:")
print(tags_df['schedule'].value_counts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Assign Pipeline Groups by Plant

# COMMAND ----------

# Assign pipeline group based on plant
# Each plant gets its own pipeline group (one-to-one mapping)
plant_to_group = {plant: idx + 1 for idx, plant in enumerate(SELECTED_PLANTS)}
tags_df['pipeline_group'] = tags_df['plant'].map(plant_to_group)

# Sort by plant and priority for better organization
tags_df = tags_df.sort_values(['plant', 'priority', 'tag_name'])

print(f"Created {tags_df['pipeline_group'].nunique()} pipeline groups (one per plant)")
print(f"Average tags per pipeline: {len(tags_df) / tags_df['pipeline_group'].nunique():.1f}")

# Show distribution by plant
print("\nPipeline Distribution (One Pipeline per Plant):")
print("=" * 60)
pipeline_dist = tags_df.groupby(['pipeline_group', 'plant']).agg({
    'tag_webid': 'count',
    'priority': lambda x: x.mode()[0] if len(x.mode()) > 0 else x.iloc[0],  # Most common priority
    'schedule': lambda x: x.mode()[0] if len(x.mode()) > 0 else x.iloc[0]   # Most common schedule
}).rename(columns={'tag_webid': 'tag_count'})

for (group_id, plant), row in pipeline_dist.iterrows():
    print(f"Pipeline {group_id} ({plant:12s}): {row['tag_count']:4d} tags | Priority: {row['priority']:6s} | Schedule: {row['schedule']}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Prepare Configuration for DAB Generation

# COMMAND ----------

# Create configuration dataframe
config_df = tags_df.copy()
config_df['pi_server_url'] = MOCK_API_URL
config_df['connection_name'] = CONNECTION_NAME
config_df['target_catalog'] = TARGET_CATALOG
config_df['target_schema'] = TARGET_SCHEMA
config_df['start_time_offset_days'] = 7  # 7 days initial backfill

# Reorder columns for clarity
config_df = config_df[[
    'tag_name',
    'tag_webid',
    'pi_server_url',
    'connection_name',
    'target_catalog',
    'target_schema',
    'pipeline_group',
    'priority',
    'schedule',
    'start_time_offset_days'
]]

print(f"✓ Configuration prepared")
print(f"\nTotal tags: {len(config_df)}")
print(f"Pipeline groups: {config_df['pipeline_group'].nunique()}")
print(f"\nConfiguration summary:")
display(config_df.groupby(['pipeline_group', 'priority']).size().reset_index(name='tag_count'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Generate DAB YAML Files

# COMMAND ----------

# Create output directory for YAML files using dbutils
try:
    dbutils.fs.mkdirs(OUTPUT_YAML_DIR)
except:
    pass  # Directory may already exist

# Now we'd normally call the generate_dab_yaml.py script
# Since we're in a notebook, let's inline the YAML generation

import yaml
from collections import defaultdict

def create_pipelines_yaml(df, project_name, notebook_path, ingestion_mode="batch"):
    """Generate DLT pipelines YAML with support for streaming and batch modes."""
    pipelines = {}

    for pipeline_group in sorted(df['pipeline_group'].unique()):
        group_df = df[df['pipeline_group'] == pipeline_group]
        pipeline_name = f"{project_name}_pipeline_{pipeline_group}"

        first_row = group_df.iloc[0]
        tags = group_df['tag_webid'].tolist()

        # Base pipeline configuration
        pipeline_config = {
            'name': f"{project_name}_ingestion_group_{pipeline_group}",
            'catalog': first_row['target_catalog'],
            'target': first_row['target_schema'],
            'libraries': [
                {'notebook': {'path': notebook_path}}
            ],
            'configuration': {
                'pi.pipeline.id': str(pipeline_group),  # CRITICAL: Per-pipeline table architecture
                'pi.tags': ','.join(tags),
                'pi.server.url': first_row['pi_server_url'],
                'pi.connection.name': first_row['connection_name'],
                'pi.target.catalog': first_row['target_catalog'],
                'pi.target.schema': first_row['target_schema'],
                'pi.start_time_offset_days': str(first_row['start_time_offset_days'])
            }
        }

        # Add mode-specific configuration
        if ingestion_mode == "streaming":
            # Streaming: continuous mode with serverless
            pipeline_config['continuous'] = True
            pipeline_config['channel'] = 'CURRENT'  # Use CURRENT for streaming
            pipeline_config['serverless'] = True
        else:
            # Batch: triggered mode with serverless
            pipeline_config['continuous'] = False
            pipeline_config['channel'] = None  # No channel for batch (triggered)
            pipeline_config['serverless'] = True

        pipelines[pipeline_name] = pipeline_config

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
print(f"\n{'='*80}")
print(f"Generating pipeline YAMLs in {INGESTION_MODE.upper()} mode")
print(f"{'='*80}\n")

pipelines_yaml = create_pipelines_yaml(config_df, PROJECT_NAME, PIPELINE_NOTEBOOK_PATH, INGESTION_MODE)

# Only generate jobs for batch mode (streaming runs continuously)
if INGESTION_MODE == "batch":
    jobs_yaml = create_jobs_yaml(config_df, PROJECT_NAME)
    print(f"✓ Generated {len(jobs_yaml['resources']['jobs'])} scheduled jobs (batch mode)")
else:
    jobs_yaml = {'resources': {'jobs': {}}}
    print(f"✓ Skipping job generation (streaming mode - pipelines run continuously)")

# Write files using dbutils
pipelines_file = f"{OUTPUT_YAML_DIR}/pipelines.yml"
jobs_file = f"{OUTPUT_YAML_DIR}/jobs.yml"

# Convert YAML to string and write
pipelines_content = yaml.dump(pipelines_yaml, default_flow_style=False, sort_keys=False)
dbutils.fs.put(pipelines_file, pipelines_content, overwrite=True)

jobs_content = yaml.dump(jobs_yaml, default_flow_style=False, sort_keys=False)
dbutils.fs.put(jobs_file, jobs_content, overwrite=True)

print(f"✓ Generated YAML files:")
print(f"  - {pipelines_file}")
print(f"  - {jobs_file}")

# DEBUG: Print pipeline configurations to verify pi.pipeline.id
print("\n" + "=" * 80)
print("DEBUG: Pipeline Configurations (pi.pipeline.id values)")
print("=" * 80)
for pipeline_name, pipeline_spec in sorted(pipelines_yaml['resources']['pipelines'].items()):
    config_params = pipeline_spec.get('configuration', {})
    pipeline_id = config_params.get('pi.pipeline.id', 'MISSING!')
    catalog = pipeline_spec.get('catalog')
    schema = config_params.get('pi.target.schema')

    print(f"\nPipeline: {pipeline_name}")
    print(f"  Display Name: {pipeline_spec['name']}")
    print(f"  pi.pipeline.id: '{pipeline_id}' ← CRITICAL FOR PER-PIPELINE TABLES")
    print(f"  Tables will be created:")
    print(f"    - {catalog}.{schema}.pi_timeseries_pipeline{pipeline_id}")
    print(f"    - {catalog}.{schema}.pi_af_hierarchy_pipeline{pipeline_id}")
    print(f"    - {catalog}.{schema}.pi_event_frames_pipeline{pipeline_id}")

print("\n" + "=" * 80)
print("✓ Each pipeline has UNIQUE pi.pipeline.id → NO TABLE CONFLICTS!")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary & Next Steps

# COMMAND ----------

print("=" * 80)
print("Pipeline Generation Complete!")
print("=" * 80)
print(f"\n✓ Discovered {len(tags_df)} tags from mock API")
print(f"✓ Created {config_df['pipeline_group'].nunique()} pipeline groups")
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC **Generated Files:**
# MAGIC - `pipelines.yml`: DLT pipeline definitions
# MAGIC - `jobs.yml`: Scheduled job definitions (batch mode only)
# MAGIC
# MAGIC **Next Steps:**
# MAGIC 1. Use separate notebook to validate and deploy DAB
# MAGIC 2. Monitor pipeline execution in Databricks UI
# MAGIC
# MAGIC **Configurable Parameters:**
# MAGIC
# MAGIC ```python
# MAGIC MAX_TAGS_TO_FETCH = None        # Limit for testing (None = all tags)
# MAGIC TAGS_PER_PIPELINE = 2000        # Tags per pipeline group
# MAGIC SENSOR_PRIORITY_MAP             # Edit to change sensor priorities
# MAGIC ```
# MAGIC
# MAGIC **Example Configurations:**
# MAGIC
# MAGIC - **Quick Test**: `MAX_TAGS_TO_FETCH = 300, TAGS_PER_PIPELINE = 100` → 3 pipelines
# MAGIC - **Medium Scale**: `MAX_TAGS_TO_FETCH = 1000, TAGS_PER_PIPELINE = 100` → 10 pipelines
# MAGIC - **Full Scale**: `MAX_TAGS_TO_FETCH = None, TAGS_PER_PIPELINE = 100` → 300+ pipelines
