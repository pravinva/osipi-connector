# Changelog

All notable changes to the OSI PI Connector project.

## [2025-12-12] - Production Ready

### Fixed
- **Authentication Issue**: Fixed Databricks Apps OAuth authentication by migrating all GET endpoints to POST endpoints
  - Added POST alternatives for `/piwebapi/elements/get` and `/piwebapi/elements/children`
  - Updated AF extractor to use POST endpoints instead of GET
  - Maintained full OAuth security (no workarounds needed)

- **UI Column Name Mismatches**: Fixed SQL queries in dashboard to match actual DLT table schemas
  - Fixed column aliases: `element_name as name`, `element_path as path`, `element_id as webid`
  - Added logic to extract plant names from element names
  - Fixed event frame and alarm queries

- **Hierarchy Tree Expansion**: Fixed JavaScript path separator bug in AF hierarchy tree view
  - Changed path separator from backslash (`\`) to forward slash (`/`)
  - Fixed `buildTree()` and `getChildren()` functions
  - Tree now correctly expands and collapses on click

- **Notebook Dependencies**: Added pip install and Python restart to pipeline generation notebook
  - Added `%pip install pyyaml` before imports
  - Added `dbutils.library.restartPython()` after installation

### Verified
- All three tables now populated with fresh data:
  - **pi_timeseries**: 1,280,000 rows (128 tags)
  - **pi_af_hierarchy**: 84 elements
  - **pi_event_frames**: 50 events
- Last ingestion: 2025-12-12 09:15 AM (data is 1.5 hours old)
- DLT pipeline running successfully with overlapping windows + MERGE strategy

### Technical Details

#### Authentication Fix
**Root Cause**: Databricks Apps have different auth middleware for GET vs POST requests. GET endpoints return 401 with OAuth tokens, POST endpoints work correctly.

**Solution**:
- databricks-app/app/api/pi_web_api.py:671-749 - Added POST endpoint alternatives
- src/extractors/af_extractor.py:18, 51, 78, 97 - Changed to POST calls
- src/extractors/event_frame_extractor.py:53 - Fixed POST syntax

#### UI Column Fix
**Root Cause**: UI queries expected generic column names but DLT tables use specific names from AF extractor.

**Solution**:
- databricks-app/app/main.py:460-515 - Updated all SQL queries with proper aliases

#### Hierarchy Tree Fix
**Root Cause**: JavaScript code split paths on `\` but database uses `/` separator.

**Solution**:
- databricks-app/app/templates/af_hierarchy_tree.html:278, 289, 292-293 - Changed path separator

## [Earlier] - Initial Development

### Added
- Mock PI Web API server with 128 synthetic tags
- DLT pipeline with 3 ingestion notebooks
- AF hierarchy extraction
- Event frame extraction
- Timeseries batch ingestion
- Dashboard with real-time monitoring
- Databricks App deployment

### Architecture
- Mock server: FastAPI (Databricks Apps)
- Ingestion: Delta Live Tables with MERGE strategy
- Storage: Unity Catalog (osipi.bronze schema)
- Dashboard: FastAPI + HTML/JavaScript
- Authentication: OAuth M2M (service principal)
