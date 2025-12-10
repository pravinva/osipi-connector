# Databricks notebook source
# MAGIC %md
# MAGIC # Validate and Deploy DAB Using Databricks SDK
# MAGIC
# MAGIC This notebook validates and deploys the generated DLT pipelines using the Databricks SDK.
# MAGIC It reads the generated YAML files and creates resources directly via API.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import pipelines, jobs

# Initialize workspace client
w = WorkspaceClient()

# Configuration
DAB_TARGET = "dev"
YAML_DIR = "/Workspace/Users/pravin.varma@databricks.com/dab_resources"
PIPELINES_YAML = f"{YAML_DIR}/pipelines.yml"
JOBS_YAML = f"{YAML_DIR}/jobs.yml"

print(f"Target Environment: {DAB_TARGET}")
print(f"YAML Directory: {YAML_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load and Validate YAML Files

# COMMAND ----------

print("=" * 80)
print("Loading YAML Configuration Files...")
print("=" * 80)

# Load pipelines YAML
try:
    pipelines_content = dbutils.fs.head(PIPELINES_YAML, 1000000)  # Read up to 1MB
    pipelines_config = yaml.safe_load(pipelines_content)
    print(f"✓ Loaded pipelines.yml")
    print(f"  Found {len(pipelines_config.get('resources', {}).get('pipelines', {}))} pipeline(s)")
except Exception as e:
    print(f"✗ Error loading pipelines.yml: {e}")
    raise

# Load jobs YAML
try:
    jobs_content = dbutils.fs.head(JOBS_YAML, 1000000)
    jobs_config = yaml.safe_load(jobs_content)
    print(f"✓ Loaded jobs.yml")
    print(f"  Found {len(jobs_config.get('resources', {}).get('jobs', {}))} job(s)")
except Exception as e:
    print(f"✗ Error loading jobs.yml: {e}")
    raise

print("\n✓ All YAML files loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Validate Pipeline Configurations

# COMMAND ----------

print("=" * 80)
print("Validating Pipeline Configurations...")
print("=" * 80)

validation_errors = []

# Validate each pipeline configuration
for pipeline_name, pipeline_spec in pipelines_config.get('resources', {}).get('pipelines', {}).items():
    print(f"\nValidating: {pipeline_name}")

    # Check required fields
    required_fields = ['name', 'catalog', 'target', 'libraries']
    missing_fields = [f for f in required_fields if f not in pipeline_spec]

    if missing_fields:
        error = f"  ✗ Missing required fields: {', '.join(missing_fields)}"
        print(error)
        validation_errors.append(error)
    else:
        print(f"  ✓ Has all required fields")
        print(f"    - Name: {pipeline_spec['name']}")
        print(f"    - Catalog: {pipeline_spec['catalog']}")
        print(f"    - Target: {pipeline_spec['target']}")
        print(f"    - Libraries: {len(pipeline_spec['libraries'])} configured")

        # Check if serverless is enabled
        if pipeline_spec.get('serverless'):
            print(f"    - Compute: Serverless")
        else:
            print(f"    - Compute: Classic")

        # Check continuous vs triggered
        if pipeline_spec.get('continuous'):
            print(f"    - Mode: Continuous (streaming)")
        else:
            print(f"    - Mode: Triggered (batch)")

if validation_errors:
    print("\n" + "=" * 80)
    print("✗ VALIDATION FAILED")
    print("=" * 80)
    for error in validation_errors:
        print(error)
    raise Exception("Configuration validation failed")
else:
    print("\n" + "=" * 80)
    print("✓ VALIDATION PASSED")
    print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Check for Existing Resources

# COMMAND ----------

print("=" * 80)
print("Checking for Existing Resources...")
print("=" * 80)

# Get list of pipeline names we want to create
pipeline_names = [spec['name'] for spec in pipelines_config.get('resources', {}).get('pipelines', {}).values()]

# Check for existing pipelines
existing_pipelines = {}
try:
    all_pipelines = list(w.pipelines.list_pipelines())
    for p in all_pipelines:
        if p.name in pipeline_names:
            existing_pipelines[p.name] = p.pipeline_id
            print(f"  ⚠ Pipeline exists: {p.name} (ID: {p.pipeline_id})")
except Exception as e:
    print(f"  ! Error listing pipelines: {e}")

if not existing_pipelines:
    print("  ✓ No existing pipelines found - will create new")
else:
    print(f"\n  Found {len(existing_pipelines)} existing pipeline(s)")
    print("  These will be updated (not recreated)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Deploy Pipelines

# COMMAND ----------

print("=" * 80)
print("Deploying Pipelines...")
print("=" * 80)

deployed_pipelines = {}

for pipeline_key, pipeline_spec in pipelines_config.get('resources', {}).get('pipelines', {}).items():
    pipeline_name = pipeline_spec['name']
    print(f"\n{'=' * 60}")
    print(f"Pipeline: {pipeline_name}")
    print('=' * 60)

    try:
        # Build pipeline configuration
        pipeline_config = {
            "name": pipeline_spec['name'],
            "catalog": pipeline_spec['catalog'],
            "target": pipeline_spec['target'],
            "libraries": pipeline_spec['libraries'],
            "continuous": pipeline_spec.get('continuous', False),
            "serverless": pipeline_spec.get('serverless', True),
        }

        # Add optional fields
        if 'configuration' in pipeline_spec:
            pipeline_config['configuration'] = pipeline_spec['configuration']

        if 'clusters' in pipeline_spec:
            pipeline_config['clusters'] = pipeline_spec['clusters']

        # Check if pipeline exists
        if pipeline_name in existing_pipelines:
            pipeline_id = existing_pipelines[pipeline_name]
            print(f"  Updating existing pipeline: {pipeline_id}")

            # Update pipeline
            w.pipelines.update(
                pipeline_id=pipeline_id,
                name=pipeline_config['name'],
                catalog=pipeline_config['catalog'],
                target=pipeline_config['target'],
                libraries=pipeline_config['libraries'],
                continuous=pipeline_config['continuous'],
                serverless=pipeline_config['serverless'],
                configuration=pipeline_config.get('configuration'),
                clusters=pipeline_config.get('clusters')
            )

            deployed_pipelines[pipeline_name] = pipeline_id
            print(f"  ✓ Updated successfully")
        else:
            print(f"  Creating new pipeline...")

            # Create pipeline
            created = w.pipelines.create(
                name=pipeline_config['name'],
                catalog=pipeline_config['catalog'],
                target=pipeline_config['target'],
                libraries=pipeline_config['libraries'],
                continuous=pipeline_config['continuous'],
                serverless=pipeline_config['serverless'],
                configuration=pipeline_config.get('configuration'),
                clusters=pipeline_config.get('clusters')
            )

            deployed_pipelines[pipeline_name] = created.pipeline_id
            print(f"  ✓ Created successfully")
            print(f"  Pipeline ID: {created.pipeline_id}")

    except Exception as e:
        print(f"  ✗ Error: {e}")
        print(f"  Continuing with next pipeline...")

print("\n" + "=" * 80)
print(f"✓ Deployment Complete: {len(deployed_pipelines)}/{len(pipeline_names)} pipelines")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Deployment Summary

# COMMAND ----------

print("=" * 80)
print("Deployment Summary")
print("=" * 80)

print(f"\nDeployed Pipelines ({len(deployed_pipelines)}):")
for name, pipeline_id in deployed_pipelines.items():
    print(f"  • {name}")
    print(f"    ID: {pipeline_id}")
    print(f"    URL: https://{w.config.host}/pipelines/{pipeline_id}")

print("\n" + "=" * 80)
print("Next Steps:")
print("=" * 80)
print("1. View pipelines in Workflows → Delta Live Tables")
print("2. Start a manual update to test: see cell below")
print("3. For batch mode, create jobs to trigger on schedule")
print("4. Monitor ingested data in Data Explorer → osipi.bronze")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: Start Pipeline Updates

# COMMAND ----------

# Uncomment to trigger all deployed pipelines
"""
import time

print("=" * 80)
print("Starting Pipeline Updates...")
print("=" * 80)

for pipeline_name, pipeline_id in deployed_pipelines.items():
    try:
        print(f"\nStarting: {pipeline_name}")
        update = w.pipelines.start_update(pipeline_id=pipeline_id)
        print(f"  ✓ Update started: {update.update_id}")
        time.sleep(2)  # Rate limiting
    except Exception as e:
        print(f"  ✗ Error: {e}")

print("\n" + "=" * 80)
print("✓ All pipeline updates triggered")
print("Monitor progress in the DLT UI")
print("=" * 80)
"""

print("Uncomment the code in this cell to trigger pipeline updates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Issues:
# MAGIC
# MAGIC **1. "Pipeline creation failed - insufficient permissions"**
# MAGIC - Ensure you have CREATE_PIPELINE permission on the catalog
# MAGIC - Check workspace admin settings for DLT enablement
# MAGIC
# MAGIC **2. "Catalog not found"**
# MAGIC - Verify catalog exists: `spark.sql("SHOW CATALOGS")`
# MAGIC - Create if needed: `spark.sql("CREATE CATALOG IF NOT EXISTS osipi")`
# MAGIC
# MAGIC **3. "Notebook not found in libraries"**
# MAGIC - Ensure notebook paths in YAML are correct
# MAGIC - Check notebook exists in Workspace
# MAGIC
# MAGIC **4. "Serverless not available"**
# MAGIC - Serverless compute may not be enabled in your workspace
# MAGIC - Change `serverless: false` in YAML and add cluster configuration
# MAGIC
# MAGIC **5. "YAML file not found"**
# MAGIC - Run `generate_pipelines_from_mock_api.py` notebook first
# MAGIC - Check YAML_DIR path matches where files were generated
