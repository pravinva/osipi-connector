## OSIPI Connector – Hackathon Submission Guide (Merge Branch)

This repository contains **two complementary implementations**:

1) **Lakeflow Python Data Source connector** (submission-critical)
- A single-file merged artifact suitable for SDP / Python Data Source execution where module imports are limited.

2) **Operational DLT ingestion pipelines** (value-add demo)
- A production-style ingestion workflow (scheduling, checkpoints, UC tables, monitoring UI).

This guide focuses on the **submission-critical** deliverables first, then explains the optional DLT add-ons.

---

## Do we need the OSIPI Databricks App running?

**Yes, if your demo uses the mock PI Web API hosted as a Databricks App.**

In the merged connector, `pi_base_url` is the base HTTP endpoint that serves PI Web API routes:

- `GET {pi_base_url}/piwebapi/dataservers`
- `POST {pi_base_url}/piwebapi/batch`
- `POST {pi_base_url}/piwebapi/assetdatabases/list`

For the demo, `pi_base_url` should point to your deployed app, e.g.:

- `https://osipi-webserver-<workspace-id>.<cloud>.databricksapps.com`

If the App is stopped or deleted, the connector cannot read data from it.

---

## Submission-critical deliverable (SDP/Python Data Source workaround)

### What to deliver

The hackathon deliverable for SDP/Python Data Source is the merged file:

- `lakeflow_cc/sources/osipi/_generated_osipi_python_source.py`

This file contains:
- Upstream parsing utilities (`libs/utils.py`)
- Your `LakeflowConnect` implementation (`sources/osipi/osipi.py`)
- The Spark DataSource wrapper/registration (`pipeline/lakeflow_python_source.py`)

### How to regenerate the merged file (required by hackathon rules)

From repo root:

```bash
cd lakeflow_cc
python3 scripts/merge_python_source.py osipi -o sources/osipi/_generated_osipi_python_source.py
python3 -m py_compile sources/osipi/_generated_osipi_python_source.py
```

**Rule of thumb**: never hand-edit the generated file—edit `sources/osipi/osipi.py` and re-run merge.

---

## How to demo the merged Python Data Source

### 1) Upload the merged artifact to DBFS

```bash
databricks fs mkdirs dbfs:/tmp/osipi-merged
databricks fs cp lakeflow_cc/sources/osipi/_generated_osipi_python_source.py   dbfs:/tmp/osipi-merged/_generated_osipi_python_source.py   --overwrite
```

### 2) Notebook demo (read-only “inspection”)

```python
local_path = "/dbfs/tmp/osipi-merged/_generated_osipi_python_source.py"
code = open(local_path, "r").read()
exec(code, globals())
register_lakeflow_source(spark)

pi_base_url = "https://osipi-webserver-1444828305810485.aws.databricksapps.com"
workspace_host = "https://e2-demo-field-eng.cloud.databricks.com"
client_id = dbutils.secrets.get("sp-osipi", "sp-client-id")
client_secret = dbutils.secrets.get("sp-osipi", "sp-client-secret")

# Test: list dataservers
(df := (spark.read.format("lakeflow_connect")
  .option("tableName", "pi_dataservers")
  .option("pi_base_url", pi_base_url)
  .option("workspace_host", workspace_host)
  .option("client_id", client_id)
  .option("client_secret", client_secret)
  .load()))

display(df)
```

### 3) Optional: write results to Delta/Unity Catalog for inspection

This is not required to prove the Data Source works, but it’s a strong demo move because it shows:
- schema correctness
- repeatable reads
- you can persist results to governed storage

Example:

```python
spark.sql("CREATE SCHEMA IF NOT EXISTS osipi_demo")
(df.write.mode("overwrite").saveAsTable("osipi_demo.pi_dataservers"))
```

**What’s needed for this?**
- a schema you can write to
- permissions to create/write tables

---

## Where do DLT pipelines fit? Are they needed?

### Are they required for the hackathon?

**No.** The merge script exists because SDP can’t reliably import multi-file modules for Python Data Source.
The submission-critical item is the merged artifact.

### Are they a value add?

**Yes** if you have time:
- shows operational scheduling/automation
- shows incremental state tracking
- produces stable UC tables your dashboards can query

### Relationship

- **Python Data Source (merged file)**: Spark **pulls** data on demand via `spark.read.format("lakeflow_connect")`.
- **DLT pipelines**: DLT **orchestrates and persists** ingestion on a schedule/continuous mode.

---

## Recommended demo narrative

1) Show the Databricks App (mock PI Web API + dashboard) is running.
2) Show the merged file exists and was generated via the official script.
3) In a notebook:
   - `register_lakeflow_source(spark)`
   - read `pi_dataservers`, `pi_points`, `pi_timeseries`, `pi_af_hierarchy`, `pi_event_frames`
4) (Optional) Persist one or two tables into UC for inspection.
5) (Optional) Show DLT pipelines running as the operational “always-on” mode.
