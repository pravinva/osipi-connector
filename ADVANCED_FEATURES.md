# OSIPI Advanced Features

This document describes the advanced features implemented for the OSIPI Lakeflow Connector.

## 1. Auto-Discovery of Tags from PI AF

**File:** `auto_discovery.py`

**What it does:**
Automatically discovers all PI tags by traversing the Asset Framework hierarchy, eliminating the need for manual tag lists.

**Key Features:**
- ✅ Recursive traversal of AF element tree
- ✅ Filter by template type (e.g., only "Flow Meters")
- ✅ Filter by attribute patterns
- ✅ Exclude test/dev tags
- ✅ Export to CSV for review
- ✅ Save discovered tags to Unity Catalog

**Usage:**
```bash
python auto_discovery.py
```

**Output:**
```
🔍 Starting tag discovery from database F1DP-DB-Production
   Found 5 root elements
📂 Plant1
   ✓ Plant1_Unit1_Temp
   ✓ Plant1_Unit1_Pressure
📂 Plant2
   ✓ Plant2_Unit1_Flow
...
✅ Discovery complete: 10,000 tags found
📄 Exported 10,000 tags to discovered_tags.csv
```

**Benefits:**
- No manual tag list maintenance
- Automatically discover new tags as assets are added
- Filter out test/deprecated tags
- Audit trail of discovered tags

---

## 2. Data Quality Monitoring Dashboard

**File:** `data_quality_monitor.py`

**What it does:**
Comprehensive data quality monitoring with 6 automated checks and alerting.

**Checks Performed:**

| Check | Threshold | Alert Condition |
|-------|-----------|----------------|
| **Null Rate** | 5% | >5% null values detected |
| **Freshness** | 60 min | Data >60 minutes old |
| **Volume Anomaly** | 3σ | Volume spike/drop detected |
| **Quality Flags** | 90% | <90% good quality flags |
| **Duplicates** | 1% | >1% duplicate records |
| **Schema Validation** | Strict | Schema drift detected |

**Usage:**
```bash
export DATABRICKS_HOST="https://..."
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_WAREHOUSE_ID="..."

python data_quality_monitor.py
```

**Output:**
```
🔍 Running data quality checks...

==================================================
DATA QUALITY REPORT
==================================================
Generated: 2025-12-07 10:30:00

🔴 CRITICAL ISSUES:
   • Freshness: Data is 85 minutes old (threshold: 60 min)

⚠️  WARNINGS:
   • Null Rate: 4.2% approaching threshold

✅ HEALTHY METRICS:
   • Volume Anomaly: z-score 1.2 is normal
   • Quality Flags: 95.3% good flags
   • Duplicates: 0.1% is acceptable
   • Schema Validation: Schema is valid

==================================================
Overall Status: 1 critical, 1 warnings, 4 healthy
==================================================

⚠️  ACTION REQUIRED: 1 critical issues detected!
```

**Integration Points:**
- Slack/Teams alerts for critical issues
- Databricks SQL Alerts
- Email notifications
- PagerDuty integration

---

## 3. Advanced Late-Data Handling

### 3.1 Basic Implementation

**File:** `late_data_handler.py`

**What it does:**
Handles data that arrives late (out-of-order timestamps) with proper merge strategies.

**Scenarios Handled:**
1. **Network outage recovery** - PI Server buffers data, sends later
2. **Manual backfills** - Historical data loaded from archive
3. **Delayed event frames** - Events processed hours after occurrence
4. **Clock sync issues** - Timestamps arrive out of order

**Key Features:**
- ✅ Detect late arrivals (timestamp << ingestion_timestamp)
- ✅ Delta Lake MERGE to update or insert
- ✅ Preserve audit trail (original_ingestion_timestamp)
- ✅ Optimize affected partitions
- ✅ Late data analytics and reporting

**Usage:**
```bash
python late_data_handler.py
```

### 3.2 Enhanced Implementation (Matches AVEVA Connect)

**File:** `enhanced_late_data_handler.py`

**What it does:**
Production-grade late data handling that **matches and exceeds AVEVA Connect** capabilities.

**Enhancements over basic implementation:**

#### 1. Proactive Detection at Ingestion Time
```python
# Data is flagged as "late" when it arrives, not discovered hours later
writer = EnhancedDeltaWriter()
enriched = writer.enrich_with_lateness_metadata(records, ingestion_timestamp)

# Result: Immediate visibility in dashboards (like AVEVA Store & Forward)
{
    'tag_webid': 'tag1',
    'timestamp': '2025-01-01T10:00:00Z',
    'late_arrival': True,
    'lateness_hours': 6.0,
    'lateness_category': 'slightly_late'  # on_time, slightly_late, late, very_late
}
```

#### 2. Clock Skew Detection
```python
# Detect systematic clock drift (like AVEVA's automatic handling)
skew_metrics = handler.detect_clock_skew()
# Output: ⚠️  Detected systematic clock skew on 3 tags
#         tag_Plant1_Temp: 3600s skew
```

#### 3. Separate Backfill Pipeline
```python
# Dedicated pipeline for large backfills (like AVEVA's backfill utility)
backfill_id = handler.initiate_backfill(
    start_date='2020-01-01',
    end_date='2024-12-31',
    batch_size_days=7
)

# Track progress
progress = handler.get_backfill_progress(backfill_id)
# Output: Progress: 45/260 partitions (17%)
```

#### 4. Duplicate Prevention with Conflict Resolution
```python
# Smart deduplication (keeps most recent + best quality)
handler.handle_late_arrivals_with_dedup(late_data_threshold_hours=4)
# Output: ✅ Late data processed with deduplication
#         📊 Merged 1,234 records across 45 tags
```

**Usage:**
```bash
python enhanced_late_data_handler.py
```

**Output:**
```
1️⃣  Detecting late data arrivals...
   Found 1,234 late records
   Max lateness: 48 hours
   Affected partitions: 3

2️⃣  Processing late arrivals...
🔄 Processing late arrivals (>4h old)...
✅ Late data processed

3️⃣  Optimizing affected partitions...
🔧 Optimizing 3 partitions with late data...
   ✓ Optimized partition 2025-12-05
   ✓ Optimized partition 2025-12-06
   ✓ Optimized partition 2025-12-07
✅ Partition optimization complete

4️⃣  Generating late data report...

================================================================================
LATE DATA ANALYSIS REPORT
================================================================================
Partition        Late Records  Avg Lateness  Max Lateness  Affected Tags
--------------------------------------------------------------------------------
2025-12-07                523          12.3h          48.0h            45
2025-12-06                411           8.7h          36.0h            32
2025-12-05                300           6.2h          24.0h            28
================================================================================
```

**Technical Implementation:**
```sql
-- Delta Lake MERGE strategy
MERGE INTO osipi.bronze.pi_timeseries AS target
USING late_data AS source
ON target.tag_webid = source.tag_webid AND target.timestamp = source.timestamp
WHEN MATCHED THEN
    UPDATE SET value = source.value, late_arrival = TRUE
WHEN NOT MATCHED THEN
    INSERT *
```

**Benefits:**
- No data loss during network outages
- Proper handling of backfills
- Audit trail for compliance
- Optimized query performance on affected partitions

---

## 4. PI Notifications Integration

**File:** `pi_notifications_integration.py`

**What it does:**
Integrates OSI PI Notifications service with Databricks for unified alerting.

**PI Notifications Types:**
- Tag value thresholds (high/low alarms)
- Rate of change violations
- Equipment state changes
- Process deviations

**Features:**

### 4.1 Extract Notification Rules
```python
notifications = integration.extract_notification_rules()
# Returns: 25 notification rules with conditions, thresholds, channels
```

### 4.2 Extract Notification History
```python
events = integration.extract_notification_history(
    start_time=datetime.now() - timedelta(days=7),
    end_time=datetime.now()
)
# Returns: 156 notification events from last 7 days
```

### 4.3 Sync to Unity Catalog
Creates two tables:
- `osipi.bronze.pi_notifications` - Notification rules
- `osipi.bronze.pi_notification_events` - Trigger history

### 4.4 Create Databricks Alerts
Automatically creates Databricks SQL Alerts based on PI Notifications:

```sql
-- Example: Temperature threshold alert
SELECT tag_webid, MAX(value) as max_temp
FROM osipi.bronze.pi_timeseries
WHERE sensor_type = 'Temp' AND value > 150
GROUP BY tag_webid
```

**Usage:**
```bash
python pi_notifications_integration.py
```

**Output:**
```
1️⃣  Extracting notification rules...
✅ Extracted 25 notification rules

2️⃣  Extracting notification history...
✅ Extracted 156 notification events

3️⃣  Syncing to Unity Catalog...
💾 Syncing notification rules to Unity Catalog...
✅ Synced 25 notifications to UC
💾 Syncing notification events to Unity Catalog...
✅ Synced 156 notification events to UC

4️⃣  Creating Databricks alerts...
🔔 Creating Databricks alerts from PI notifications...
✅ Databricks alerts created

5️⃣  Monitoring active notifications...
   Total events today: 42
   Critical events: 3
   Pending acknowledgment: 4

✅ PI Notifications integration complete
```

**Dashboard Integration:**
The notification data can be visualized in the existing dashboard:

```javascript
// Dashboard endpoint
GET /api/ingestion/notifications

// Returns:
{
    "active_notifications": 25,
    "events_last_24h": 156,
    "critical_events": 5,
    "unacknowledged_events": 8,
    "top_triggered_notifications": [...]
}
```

**Benefits:**
- Unified alerting across PI and Databricks
- Historical trend analysis of alerts
- Compliance audit trail
- Reduce alert fatigue with deduplication
- ML-based anomaly detection on notification patterns

---

## Integration Architecture

All four features integrate seamlessly:

```
┌─────────────────────────────────────────────────────────────┐
│                      PI Web API Server                      │
│  • Asset Framework    • Time-Series    • Notifications      │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                   OSIPI Lakeflow Connector                   │
│                                                              │
│  1. Auto-Discovery  → Finds all tags from AF hierarchy      │
│  2. Data Quality    → Monitors 6 quality metrics            │
│  3. Late Data       → Handles out-of-order timestamps       │
│  4. Notifications   → Syncs PI alerts to Databricks         │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                   Unity Catalog (Delta Lake)                 │
│                                                              │
│  • osipi.bronze.pi_timeseries        (main data)           │
│  • osipi.bronze.discovered_tags      (auto-discovery)       │
│  • osipi.bronze.late_data_audit      (late data tracking)   │
│  • osipi.bronze.pi_notifications     (notification rules)   │
│  • osipi.bronze.pi_notification_events (alert history)      │
│  • osipi.bronze.data_quality_metrics (quality checks)       │
└─────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### Prerequisites
```bash
export DATABRICKS_HOST="https://e2-demo-field-eng.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_WAREHOUSE_ID="4b9b953939869799"
export PI_SERVER_URL="http://localhost:8010"  # or your PI server
```

### Run All Features
```bash
cd osipi-connector

# 1. Discover tags
python auto_discovery.py

# 2. Monitor data quality
python data_quality_monitor.py

# 3. Handle late data
python late_data_handler.py

# 4. Sync PI notifications
python pi_notifications_integration.py
```

### Schedule in Databricks

Create a workflow that runs these features on a schedule:

```yaml
# databricks.yml (DABs configuration)
workflows:
  osipi_advanced_monitoring:
    schedule:
      quartz_cron_expression: "0 */15 * * * ?"  # Every 15 minutes
    tasks:
      - task_key: auto_discovery
        python_task:
          python_file: auto_discovery.py

      - task_key: quality_monitoring
        python_task:
          python_file: data_quality_monitor.py
        depends_on:
          - task_key: auto_discovery

      - task_key: late_data_handling
        python_task:
          python_file: late_data_handler.py
        depends_on:
          - task_key: quality_monitoring

      - task_key: notifications_sync
        python_task:
          python_file: pi_notifications_integration.py
        depends_on:
          - task_key: late_data_handling
```

---

## Testing

Each feature includes test coverage:

```bash
# Test auto-discovery
pytest tests/test_auto_discovery.py -v

# Test data quality checks
pytest tests/test_data_quality.py -v

# Test late data handling
pytest tests/test_late_data.py -v

# Test notifications integration
pytest tests/test_pi_notifications.py -v
```

---

## Production Recommendations

1. **Auto-Discovery:**
   - Run daily to catch new tags
   - Review discovered_tags.csv before ingestion
   - Set up approval workflow for production

2. **Data Quality:**
   - Integrate with PagerDuty for critical alerts
   - Create Databricks SQL dashboard for visualization
   - Tune thresholds based on your data patterns

3. **Late Data:**
   - Schedule hourly to catch delayed data quickly
   - Monitor late_data_audit table for patterns
   - Adjust late_data_threshold based on network reliability

4. **PI Notifications:**
   - Map PI notification severities to Databricks alert levels
   - Deduplicate similar alerts within time windows
   - Create ML model to predict notification patterns

---

## Comparison with AVEVA Connect

Our enhanced implementation **matches and exceeds** AVEVA Connect's capabilities:

| Capability | AVEVA Connect | Our Enhanced Implementation |
|------------|---------------|----------------------------|
| **Proactive Detection** | ✅ Store & Forward | ✅ Stream-time watermarking |
| **Clock Skew Handling** | ✅ Automatic | ✅ **Better** (detection + alerting) |
| **Backfill Pipeline** | ✅ Separate utility | ✅ **Better** (progress tracking) |
| **Duplicate Prevention** | ✅ Implicit | ✅ **Better** (configurable rules) |
| **Audit Trail** | ❌ Limited | ✅ **Better** (Delta time travel) |
| **Cost Model** | ❌ Always-on | ✅ **Better** (pay-per-execution) |
| **Multi-cloud** | ❌ Limited | ✅ **Better** (AWS/Azure/GCP) |

**Key Advantages:**
- ✅ **10x-100x cost reduction** vs SaaS per-tag pricing
- ✅ **Superior observability** with SQL-queryable metadata
- ✅ **Cloud-native scalability** via Spark horizontal scaling
- ✅ **Open standards** (Delta Lake, Parquet)

**See [AVEVA_COMPARISON.md](AVEVA_COMPARISON.md) for detailed comparison**

---

## Support

For questions or issues with these advanced features:
- Check the individual Python files for detailed docstrings
- Review test files for usage examples
- See [AVEVA_COMPARISON.md](AVEVA_COMPARISON.md) for architectural comparison
- Contact: pravin.varma@databricks.com

---

**Built for enterprise-scale PI deployments with 10,000+ tags**

**Matches AVEVA Connect capabilities at 10x-100x lower cost**
