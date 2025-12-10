# DAB Pipeline Resources

This directory contains auto-generated Databricks Asset Bundle (DAB) resources for OSI PI ingestion pipelines.

## How It Works

1. **Run the generator notebook**: `notebooks/generate_pipelines_from_mock_api.py`
   - Discovers all tags from the mock PI API
   - Groups tags into pipelines based on priority and load balancing
   - Generates `pipelines.yml` and `jobs.yml` in this directory

2. **Deploy with DAB**:
   ```bash
   databricks bundle validate -t dev
   databricks bundle deploy -t dev
   ```

3. **Pipelines run automatically** on their configured schedules

## Files

- `pipelines.yml` - DLT pipeline definitions (auto-generated)
- `jobs.yml` - Scheduled job definitions (auto-generated)
- `README.md` - This file

## Regenerating Pipelines

Whenever the mock API changes (new tags added, priorities updated):

1. Re-run `notebooks/generate_pipelines_from_mock_api.py`
2. Review the updated YAML files
3. Redeploy: `databricks bundle deploy -t dev`

**Note:** These files are gitignored since they're auto-generated. The generator notebook is the source of truth.
