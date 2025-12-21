# Debug Tools

Diagnostic utilities for troubleshooting and validation.

## Available Tools

### check_tables.py

**Purpose:** Verify Unity Catalog tables exist and contain data

**Usage:**
```bash
python debugtools/check_tables.py
```

**What it checks:**
- `osipi.bronze.pi_af_hierarchy` - AF hierarchy elements
- `osipi.bronze.pi_event_frames` - Event frames and alarms
- `osipi.bronze.pi_timeseries` - Time-series sensor data

**Output:**
- Row counts for each table
- Sample data from AF hierarchy
- Error diagnostics if tables are missing

**When to use:**
- After running ingestion to verify data was loaded
- When dashboard shows "No data found" errors
- To validate table schemas and column names
- Before running demo to ensure tables are populated

**Requirements:**
- Databricks workspace authentication
- Access to Unity Catalog `osipi.bronze` schema
- Valid warehouse ID (currently hardcoded: `4b9b953939869799`)
