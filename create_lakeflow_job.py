"""
Create Databricks Workflow Job for PI Lakeflow Connector
"""

import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

# Configuration
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://e2-demo-field-eng.cloud.databricks.com")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "YOUR_DATABRICKS_TOKEN")

w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

JOB_NAME = "OSIPI_Lakeflow_Connector"

print("=" * 80)
print("CREATING LAKEFLOW CONNECTOR JOB")
print("=" * 80)

# First, upload the connector script to workspace
print("\n[1/3] Uploading connector script to workspace...")
with open("lakeflow_pi_connector.py", "r") as f:
    script_content = f.read()

workspace_path = "/Workspace/Users/pravin.varma@databricks.com/osipi/lakeflow_pi_connector"
try:
    w.workspace.mkdirs(os.path.dirname(workspace_path))
    w.workspace.upload(workspace_path + ".py", script_content.encode(), overwrite=True)
    print(f"   ✓ Uploaded to: {workspace_path}.py")
except Exception as e:
    print(f"   Error: {e}")

# Check if job already exists
print("\n[2/3] Checking for existing job...")
existing_jobs = list(w.jobs.list(name=JOB_NAME))
if existing_jobs:
    job_id = existing_jobs[0].job_id
    print(f"   Found existing job: {job_id}")
    print(f"   Deleting old job...")
    w.jobs.delete(job_id)

# Create new job
print("\n[3/3] Creating Databricks Workflow job...")
# Get available clusters
clusters = list(w.clusters.list())
if clusters:
    cluster_id = clusters[0].cluster_id
    print(f"   Using existing cluster: {cluster_id}")
    cluster_config = {
        "existing_cluster_id": cluster_id
    }
else:
    print("   Creating new job cluster...")
    cluster_config = {
        "new_cluster": {
            "num_workers": 2,
            "spark_version": "14.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "spark_env_vars": {
                "PI_SERVER_URL": "http://localhost:8010",
                "DATABRICKS_WAREHOUSE_ID": "4b9b953939869799"
            }
        }
    }

job = w.jobs.create(
    name=JOB_NAME,
    tasks=[
        jobs.Task(
            task_key="ingest_pi_data",
            description="Ingest data from PI Web API to Delta Lake",
            spark_python_task=jobs.SparkPythonTask(
                python_file=f"{workspace_path}.py",
                source=jobs.Source.WORKSPACE
            ),
            **cluster_config,
            timeout_seconds=1200,
            max_retries=2
        )
    ],
    schedule=jobs.CronSchedule(
        quartz_cron_expression="0 */15 * * * ?",  # Every 15 minutes
        timezone_id="UTC"
    ),
    tags={
        "project": "osipi",
        "type": "lakeflow_connector"
    },
    max_concurrent_runs=1
)

print("\n" + "=" * 80)
print("✅ JOB CREATED SUCCESSFULLY")
print("=" * 80)
print(f"Job ID: {job.job_id}")
print(f"Job Name: {JOB_NAME}")
print(f"Schedule: Every 15 minutes")
print(f"Workspace Path: {workspace_path}.py")
print("\n" + "=" * 80)
print("VIEW IN WORKSPACE")
print("=" * 80)
print(f"Navigate to: Workflows > Jobs > {JOB_NAME}")
print(f"Or: {DATABRICKS_HOST}/#job/{job.job_id}")
print("\n" + "=" * 80)
print("MANUAL TRIGGER")
print("=" * 80)
print(f"To trigger manually: databricks jobs run-now {job.job_id}")
print("Or click 'Run now' in the Workflows UI")
print("=" * 80)
