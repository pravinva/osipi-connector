"""
Verify OSIPI data and show statistics
"""

import os
from databricks.sdk import WorkspaceClient

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://e2-demo-field-eng.cloud.databricks.com")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "YOUR_DATABRICKS_TOKEN")

w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

warehouses = list(w.warehouses.list())
warehouse_id = warehouses[0].id

def query(sql):
    """Execute query and return results"""
    stmt = w.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=warehouse_id,
        wait_timeout="50s"
    )
    return stmt

print("=" * 80)
print("OSIPI DATA VERIFICATION")
print("=" * 80)

# Query 1: Row count
print("\n1. TOTAL ROWS:")
result = query("SELECT COUNT(*) as count FROM osipi.bronze.pi_timeseries")
if result.result and result.result.data_array:
    print(f"   {result.result.data_array[0][0]} rows")

# Query 2: Plants and tags
print("\n2. DATA BY PLANT:")
result = query("""
    SELECT
        plant,
        COUNT(*) as rows,
        COUNT(DISTINCT tag_webid) as tags
    FROM osipi.bronze.pi_timeseries
    GROUP BY plant
    ORDER BY plant
""")
if result.result and result.result.data_array:
    print("   Plant      | Rows | Tags")
    print("   " + "-" * 30)
    for row in result.result.data_array:
        print(f"   {row[0]:<10} | {row[1]:>4} | {row[2]:>4}")

# Query 3: Sensor types
print("\n3. DATA BY SENSOR TYPE:")
result = query("""
    SELECT
        sensor_type,
        COUNT(*) as rows,
        COUNT(DISTINCT tag_webid) as tags
    FROM osipi.bronze.pi_timeseries
    GROUP BY sensor_type
    ORDER BY rows DESC
""")
if result.result and result.result.data_array:
    print("   Sensor     | Rows | Tags")
    print("   " + "-" * 30)
    for row in result.result.data_array:
        print(f"   {row[0]:<10} | {row[1]:>4} | {row[2]:>4}")

# Query 4: Time range
print("\n4. TIME RANGE:")
result = query("""
    SELECT
        MIN(timestamp) as min_time,
        MAX(timestamp) as max_time,
        CAST((MAX(timestamp) - MIN(timestamp)) AS STRING) as duration
    FROM osipi.bronze.pi_timeseries
""")
if result.result and result.result.data_array:
    row = result.result.data_array[0]
    print(f"   Start: {row[0]}")
    print(f"   End:   {row[1]}")
    print(f"   Duration: {row[2]}")

# Query 5: Data quality
print("\n5. DATA QUALITY:")
result = query("""
    SELECT
        SUM(CASE WHEN quality_good THEN 1 ELSE 0 END) as good,
        SUM(CASE WHEN quality_questionable THEN 1 ELSE 0 END) as questionable,
        SUM(CASE WHEN quality_substituted THEN 1 ELSE 0 END) as substituted,
        COUNT(*) as total
    FROM osipi.bronze.pi_timeseries
""")
if result.result and result.result.data_array:
    row = result.result.data_array[0]
    good, questionable, substituted, total = int(row[0]), int(row[1]), int(row[2]), int(row[3])
    print(f"   Good:         {good} ({good*100//total}%)")
    print(f"   Questionable: {questionable} ({questionable*100//total}%)")
    print(f"   Substituted:  {substituted} ({substituted*100//total}%)")

# Query 6: Gold table stats
print("\n6. GOLD TABLE (HOURLY AGGREGATES):")
result = query("SELECT COUNT(*) as count FROM osipi.gold.pi_metrics_hourly")
if result.result and result.result.data_array:
    print(f"   {result.result.data_array[0][0]} hourly metric records")

result = query("""
    SELECT plant, sensor_type, hour, data_points, avg_value, quality_pct
    FROM osipi.gold.pi_metrics_hourly
    ORDER BY hour DESC
    LIMIT 5
""")
if result.result and result.result.data_array:
    print("\n   Recent hourly metrics:")
    print("   " + "-" * 70)
    for row in result.result.data_array:
        plant, sensor, hour, pts, avg, qual = row[0], row[1], row[2], int(row[3]), float(row[4]), float(row[5])
        print(f"   {plant:<10} {sensor:<8} {hour} pts:{pts:>3} avg:{avg:>6.2f} q:{qual:>5.1f}%")

print("\n" + "=" * 80)
print("✅ DATA VERIFICATION COMPLETE")
print("=" * 80)
