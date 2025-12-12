# OSIPI Connector - Issue Diagnosis and Fix

## Issue Summary

**Problem**: AF Hierarchy and Event Frames tables have 0 rows, while Timeseries table has 1.28M rows.

**Root Cause**: Databricks Apps have a known authentication limitation where:
- ✅ POST endpoints work with OAuth authentication
- ❌ GET endpoints return 401 Unauthorized with OAuth tokens

This is documented in the mock server code comments (pi_web_api.py:582-590, 609-611, 628-630):
> "POST alternative for Databricks App - Works around authentication issues with GET endpoints in Databricks Apps"

## Testing Results

All `/piwebapi/*` endpoints return **401 Unauthorized** when called with OAuth Bearer tokens via GET:

```
GET /piwebapi/assetdatabases → 401 Unauthorized
GET /piwebapi/dataservers → 401 Unauthorized  
GET /piwebapi/dataservers/F1DP-PI-SRV/points → 401 Unauthorized
```

POST endpoints work correctly:
```
POST /piwebapi/batch → 200 OK (used by timeseries)
POST /piwebapi/assetdatabases/list → Available but not used
POST /piwebapi/assetdatabases/elements → Available but not used
POST /piwebapi/assetdatabases/eventframes → Available but not used
```

## Current Code Status

### What Works ✅
- **Timeseries ingestion** (1.28M rows)
  - Uses: `POST /piwebapi/batch` (src/extractors/timeseries_extractor.py)
  - Status: Working perfectly

- **AF Databases List** (partial)
  - Uses: `POST /piwebapi/assetdatabases/list` (src/extractors/af_extractor.py:18)
  - Status: Already using POST endpoint

### What's Broken ❌
- **AF Hierarchy Traversal** (0 rows)
  - Uses: `GET /piwebapi/elements/{webid}` (src/extractors/af_extractor.py:49)
  - Uses: `GET /piwebapi/elements/{webid}/elements` (src/extractors/af_extractor.py:73)
  - **Fix needed**: Change to POST endpoints

- **Event Frames** (0 rows)
  - Uses: `GET /piwebapi/assetdatabases/{db_webid}/eventframes` (src/extractors/event_frame_extractor.py)
  - **Fix needed**: Change to POST endpoint `/piwebapi/assetdatabases/eventframes`

## Required Code Changes

### 1. Update AF Extractor (src/extractors/af_extractor.py)

**File**: `src/extractors/af_extractor.py`

**Lines to change**:
- Line 18: ✅ Already uses POST
- Line 49: ❌ Change GET to POST (element details endpoint doesn't exist as POST)
- Line 73-74: ❌ Change `GET /piwebapi/elements/{webid}/elements` to `POST /piwebapi/assetdatabases/elements`

**Problem**: The mock server provides:
- `POST /piwebapi/assetdatabases/list` - Get databases ✅ 
- `POST /piwebapi/assetdatabases/elements` - Get root elements ✅
- But NO POST alternative for `/piwebapi/elements/{webid}/elements` (child traversal) ❌

**Solution**: Need to add POST endpoint for element traversal in mock server OR restructure traversal logic.

### 2. Update Event Frame Extractor (src/extractors/event_frame_extractor.py)

**File**: `src/extractors/event_frame_extractor.py`

**Change required**:
```python
# Current (broken):
response = self.client.get(f"/piwebapi/assetdatabases/{db_webid}/eventframes")

# Fix:
response = self.client.post(
    "/piwebapi/assetdatabases/eventframes",
    json={"db_webid": db_webid, "start_time": start_time, "end_time": end_time}
)
```

The POST endpoint already exists (pi_web_api.py:625).

## Tables to Drop (Checkpoints Not Needed for DLT)

Since the DLT pipeline uses **overlapping windows + MERGE** strategy (not checkpoint-based incremental):

**Drop these**:
```sql
-- Checkpoints are not needed (DLT handles deduplication via MERGE)
DROP SCHEMA IF EXISTS osipi.checkpoints CASCADE;
```

**Keep these**:
- ✅ `osipi.bronze.pi_timeseries` - DLT managed, working
- ✅ `osipi.bronze.pi_af_hierarchy` - DLT managed, needs fixing
- ✅ `osipi.bronze.pi_event_frames` - DLT managed, needs fixing
- ✅ `osipi.gold.pi_metrics_hourly` - Downstream aggregation (16 rows)

## Implementation Plan

1. **Update Event Frame Extractor** (simplest fix)
   - Change GET to POST for `/piwebapi/assetdatabases/eventframes`
   - Test: Should immediately start ingesting event frames

2. **Add POST endpoint for AF element traversal in mock server**
   - Add `POST /piwebapi/elements/get` endpoint
   - Takes `{"element_webid": "..."}` in body
   - Returns element details + child elements

3. **Update AF Extractor to use new POST endpoint**
   - Change lines 49, 73-74 to use POST

4. **Drop unused checkpoint schema**
   - Run: `DROP SCHEMA osipi.checkpoints CASCADE`

5. **Test full ingestion**
   - Run DLT pipeline
   - Verify all 3 tables populate

## Why Not Use ALLOW_UNAUTHENTICATED_API=true?

Setting `ALLOW_UNAUTHENTICATED_API=true` would work but:
- ❌ Security risk (anyone can call your API)
- ❌ Not production-ready
- ✅ Better to fix root cause (use POST endpoints)

## Checkpoint vs DLT MERGE Strategy

Your DLT pipeline correctly uses DLT-native deduplication:

```python
@dlt.table(
    name="pi_timeseries",
    table_properties={
        "pipelines.merge.keys": "tag_webid,timestamp"  # ← DLT MERGE
    }
)
def pi_timeseries():
    # Always fetch last 7 days (overlapping windows)
    start_time = datetime.now() - timedelta(days=7)  # ← Intentional overlap
```

**How it works**:
1. Fetch last 7 days of data (overlaps with previous runs)
2. DLT automatically MERGEs based on `(tag_webid, timestamp)`
3. Duplicates are overwritten, new records inserted
4. No manual checkpoint management needed

**Checkpoint Manager** (src/checkpoints/checkpoint_manager.py) is **NOT used** by DLT pipeline and can be ignored.

---

## Next Steps

Would you like me to:
1. ✅ **Update the Event Frame extractor** to use POST endpoints
2. ✅ **Add missing POST endpoint** to mock server for AF element traversal  
3. ✅ **Update AF extractor** to use POST endpoints
4. ❌ Drop the checkpoint schema
5. ✅ **Test the full ingestion** to verify AF and Events populate

This is the **proper fix** that doesn't compromise security.
