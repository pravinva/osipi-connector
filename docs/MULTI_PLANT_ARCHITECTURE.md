# Multi-Plant Architecture for OSI PI Connector

## Overview

This document explains the realistic multi-plant load balancing architecture implemented for the OSI PI connector. This approach matches how real-world industrial OT (Operational Technology) systems work.

## Architecture Diagram

```
┌──────────────────────────────────────────────────────────────┐
│  Mock PI Web API (10,000 Tags Total)                         │
│  5 Plants × 2,000 Tags Each                                  │
│                                                                │
│  - Sydney Plant: 2,000 tags                                   │
│  - Melbourne Plant: 2,000 tags                                │
│  - Brisbane Plant: 2,000 tags                                 │
│  - Perth Plant: 2,000 tags                                    │
│  - Adelaide Plant: 2,000 tags                                 │
└──────────────────────────────────────────────────────────────┘
                            ↓
        ┌───────────────────┼────────────────────┬────────────┐
        ↓                   ↓                    ↓            ↓
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ...  ┌──────────────┐
│ DLT Pipeline │  │ DLT Pipeline │  │ DLT Pipeline │        │ DLT Pipeline │
│ Sydney       │  │ Melbourne    │  │ Brisbane     │        │ Adelaide     │
│              │  │              │  │              │        │              │
│ Filter:      │  │ Filter:      │  │ Filter:      │        │ Filter:      │
│ nameFilter=  │  │ nameFilter=  │  │ nameFilter=  │        │ nameFilter=  │
│ "Sydney_*"   │  │ "Melbourne_*"│  │ "Brisbane_*" │        │ "Adelaide_*" │
└──────────────┘  └──────────────┘  └──────────────┘        └──────────────┘
        ↓                   ↓                    ↓                    ↓
┌──────────────────────────────────────────────────────────────────────────┐
│         Unity Catalog Delta Tables (osipi.bronze)                        │
│                                                                            │
│  pi_timeseries    |  tag_webid          |  plant                         │
│  ─────────────────┼─────────────────────┼──────────────────             │
│                   |  F1DP-Sydney-...    |  Sydney                        │
│                   |  F1DP-Melbourne-... |  Melbourne                     │
│                   |  F1DP-Brisbane-...  |  Brisbane                      │
│                   |  F1DP-Perth-...     |  Perth                         │
│                   |  F1DP-Adelaide-...  |  Adelaide                      │
│                                                                            │
│  No ownership conflicts - data naturally partitioned by plant!            │
└──────────────────────────────────────────────────────────────────────────┘
```

## Key Concept

**In real OT environments:**
- Each pipeline connects to a DIFFERENT physical data source (e.g., Sydney PI Server, Melbourne PI Server)
- Data is naturally non-overlapping because each plant has different equipment and sensors
- All pipelines can write to the same Unity Catalog table without conflicts
- No Bronze-Silver medallion pattern needed - data is already partitioned by source

## Implementation Details

### Mock API Server Changes

**File:** `databricks-app/app/api/pi_web_api.py`

**Changes Made:**

1. **Expanded to 5 plants (lines 41-44):**
   ```python
   plant_names = ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"]
   units_per_plant = 250  # 250 units × 8 sensors = 2,000 tags per plant
   ```

2. **Tag naming includes plant prefix (line 54):**
   ```python
   "name": f"{plant}_Unit{unit:03d}_{sensor_type}_PV"
   ```

   Examples:
   - `Sydney_Unit001_Temperature_PV`
   - `Melbourne_Unit050_Pressure_PV`
   - `Adelaide_Unit250_Current_PV`

3. **AF Hierarchy separated by plant (lines 83-121):**
   - Each plant has separate root element: `Sydney_Plant`, `Melbourne_Plant`, etc.
   - Full path: `\\Sydney_Plant\Unit_001\Temperature`

4. **Event Frames scoped to plant equipment (lines 143-178):**
   - Event WebIDs include plant: `F1DP-EF-Sydney-00001`
   - Event names prefixed: `Sydney_BatchRun_20251212_1430`
   - References plant units: `F1DP-Unit-Sydney-001`

### Tag Distribution

**Total: 10,000 tags**

| Plant     | Tags  | Units | Sensor Types | Calculation        |
|-----------|-------|-------|--------------|-------------------|
| Sydney    | 2,000 | 250   | 8            | 250 × 8 = 2,000   |
| Melbourne | 2,000 | 250   | 8            | 250 × 8 = 2,000   |
| Brisbane  | 2,000 | 250   | 8            | 250 × 8 = 2,000   |
| Perth     | 2,000 | 250   | 8            | 250 × 8 = 2,000   |
| Adelaide  | 2,000 | 250   | 8            | 250 × 8 = 2,000   |

**Sensor Types:**
- Temperature (degC)
- Pressure (bar)
- Flow (m3/h)
- Level (%)
- Power (kW)
- Speed (RPM)
- Voltage (V)
- Current (A)

### AF Hierarchy Structure

**Total: 250 elements (50 per plant)**

Each plant has:
- 10 process units represented in AF hierarchy
- 4 equipment types per unit (Pump, Compressor, HeatExchanger, Reactor)
- Total: 5 plants × 10 units × 4 equipment = 200 equipment elements + 50 unit/plant elements

Example hierarchy:
```
ProductionDB/
├── Sydney_Plant/
│   ├── Unit_001/
│   │   ├── Pump_101
│   │   ├── Compressor_101
│   │   ├── HeatExchanger_101
│   │   └── Reactor_101
│   ├── Unit_002/
│   ...
│   └── Unit_010/
├── Melbourne_Plant/
│   ├── Unit_001/
│   ...
...
```

### Event Frames

**Total: 250 events (50 per plant)**

Each plant generates:
- BatchRun events (batch production runs)
- Maintenance events (preventive, corrective, inspection)
- Alarm events (high temp, low pressure, equipment faults)
- Downtime events

## Pipeline Generator Changes Needed

**File:** `notebooks/generate_pipelines_from_mock_api.py`

**Changes Required:**

1. **Query tags by plant (instead of arbitrary grouping):**
   ```python
   # OLD: Group tags arbitrarily into groups of TAGS_PER_PIPELINE
   # NEW: Group tags by plant name

   plants = ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"]

   for plant in plants:
       # Query API for tags matching this plant
       response = requests.get(f"{MOCK_API_URL}/piwebapi/dataservers/F1DP-Server-Primary/points",
                               params={"nameFilter": f"{plant}_*", "maxCount": 10000})
       plant_tags = response.json()['Items']

       # Create one pipeline for this plant
       pipeline_config = create_pipeline_config(plant, plant_tags)
   ```

2. **Pass plant name to DLT pipeline:**
   ```python
   'configuration': {
       'pi.plant.name': plant,  # NEW: Plant filter
       'pi.tags': ','.join([t['WebId'] for t in plant_tags]),
       'pi.server.url': MOCK_API_URL,
       ...
   }
   ```

## DLT Pipeline Changes Needed

**File:** `src/notebooks/pi_ingestion_pipeline.py`

**Changes Required:**

Add plant filter to data extraction:

```python
# Get configuration
plant_name = spark.conf.get('pi.plant.name')  # NEW
tags = spark.conf.get('pi.tags').split(',')

# Filter tags by plant (safety check)
filtered_tags = [tag for tag in tags if plant_name in tag]

config = {
    'pi_web_api_url': pi_server_url,
    'tags': filtered_tags,  # Plant-specific tags
    ...
}
```

## Why This Approach?

### Benefits

✅ **Matches Real OT Architecture:**
   - In reality, each pipeline connects to a different PI server (different plant/site)
   - Data is naturally partitioned by physical location
   - No artificial grouping needed

✅ **No Ownership Conflicts:**
   - All 5 pipelines write different data (different plants)
   - All write to same Unity Catalog table (`osipi.bronze.pi_timeseries`)
   - DLT is happy because data doesn't overlap

✅ **Scalability:**
   - Add new plant? Just add new pipeline
   - No complex Bronze-Silver consolidation needed
   - Simple, maintainable architecture

✅ **Realistic Testing:**
   - Simulates actual industrial deployments
   - Tests cross-plant analytics scenarios
   - Dashboard can filter/aggregate by plant

### Why NOT Bronze-Silver Pattern?

The Bronze-Silver medallion pattern (with append-only bronze tables per pipeline group + consolidation) is:
- **Unnecessary** when data is naturally partitioned by source
- **Adds complexity** with extra pipeline and deduplication logic
- **Doesn't match reality** - real OT pipelines don't need deduplication because they connect to different sources

Use Bronze-Silver when:
- Multiple pipelines fetch the SAME data from the SAME source (for redundancy/reliability)
- Need to handle duplicates or late-arriving data from overlapping time windows

Our use case:
- Each pipeline handles a DIFFERENT plant
- No overlap, no duplicates
- Direct bronze layer is sufficient

## Testing the Implementation

### Verify Tag Distribution

```python
import requests

response = requests.get("http://localhost:8003/piwebapi/dataservers/F1DP-Server-Primary/points",
                        params={"maxCount": 20000})
all_tags = response.json()['Items']

from collections import Counter
plants = [tag['Name'].split('_')[0] for tag in all_tags]
plant_counts = Counter(plants)

print(f"Total tags: {len(all_tags)}")
for plant, count in sorted(plant_counts.items()):
    print(f"  {plant}: {count} tags")
```

**Expected Output:**
```
Total tags: 10000
  Adelaide: 2000 tags
  Brisbane: 2000 tags
  Melbourne: 2000 tags
  Perth: 2000 tags
  Sydney: 2000 tags
```

### Query Tags for Specific Plant

```python
# Get all Sydney tags
response = requests.get("http://localhost:8003/piwebapi/dataservers/F1DP-Server-Primary/points",
                        params={"nameFilter": "Sydney_*", "maxCount": 10000})
sydney_tags = response.json()['Items']
print(f"Sydney tags: {len(sydney_tags)}")  # Should be 2000
```

## Deployment Workflow

1. **Update generate_pipelines notebook** with plant-based grouping logic
2. **Run notebook** to generate 5 pipeline configs (one per plant)
3. **Deploy via DAB** or UI
4. **Run all 5 pipelines in parallel** - no conflicts!
5. **Verify data** in Unity Catalog tables partitioned by plant

## Data Verification Queries

After pipelines run successfully:

```sql
-- Check total rows
SELECT COUNT(*) FROM osipi.bronze.pi_timeseries;
-- Should have data from all 5 plants

-- Check distribution by plant
SELECT
  SUBSTRING_INDEX(tag_name, '_', 1) AS plant,
  COUNT(*) AS row_count,
  COUNT(DISTINCT tag_webid) AS unique_tags
FROM osipi.bronze.pi_timeseries
GROUP BY plant
ORDER BY plant;
-- Should show equal distribution

-- Check AF hierarchy
SELECT
  element_name,
  element_type,
  element_path
FROM osipi.bronze.pi_af_hierarchy
WHERE element_path LIKE '\\\\Sydney_Plant%'
LIMIT 10;
```

## Troubleshooting

### Issue: Pipeline gets "table already managed" error

**Cause:** Old pipeline still owns the table

**Solution:**
1. Delete old pipeline in Databricks UI
2. Drop and recreate tables (or let DLT auto-create)
3. Run new pipelines

### Issue: Only one pipeline's data appears

**Cause:** Plant filter not applied correctly

**Solution:**
1. Verify pipeline configuration has correct `pi.plant.name`
2. Check DLT notebook reads and uses the plant filter
3. Verify mock API returns plant-specific tags

### Issue: Data overlaps between pipelines

**Cause:** Tag assignment logic is wrong

**Solution:**
1. Verify each pipeline queries with correct `nameFilter` parameter
2. Check mock API tag naming includes plant prefix
3. Ensure no duplicate tag WebIDs across plants

## Summary

This multi-plant architecture provides:
- **10,000 tags** across 5 realistic plants
- **Natural data partitioning** by plant location
- **No ownership conflicts** - all pipelines write to same table
- **Realistic OT simulation** matching real-world deployments
- **Simple, maintainable design** without unnecessary complexity

Each DLT pipeline handles ONE complete plant (all tags, AF hierarchy, events for that location), just like connecting to a real PI Server at that plant.
