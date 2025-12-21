"""
Quick script to check the date range of data in pi_timeseries table
"""

from databricks.sdk import WorkspaceClient
from datetime import datetime, timedelta

# Initialize client
w = WorkspaceClient()

# Query the table
query = """
SELECT
    MIN(timestamp) as min_timestamp,
    MAX(timestamp) as max_timestamp,
    COUNT(*) as total_rows,
    COUNT(DISTINCT tag_webid) as unique_tags,
    COUNT(DISTINCT partition_date) as unique_dates
FROM osipi.bronze.pi_timeseries
"""

# Execute query
statement_id = w.statement_execution.execute_statement(
    warehouse_id="4b9b953939869799",
    catalog="osipi",
    schema="bronze",
    statement=query
).statement_id

# Wait for completion and get results
result = w.statement_execution.get_statement(statement_id)
while result.status.state in ['PENDING', 'RUNNING']:
    import time
    time.sleep(1)
    result = w.statement_execution.get_statement(statement_id)

if result.status.state == 'SUCCEEDED':
    # Extract results
    if result.result and result.result.data_array:
        row = result.result.data_array[0]
        min_ts = row[0]
        max_ts = row[1]
        total_rows = row[2]
        unique_tags = row[3]
        unique_dates = row[4]

        print("=" * 80)
        print("OSIPI Data Range Analysis")
        print("=" * 80)
        print(f"Table: osipi.bronze.pi_timeseries")
        print()
        print(f"Date Range:")
        print(f"  Min Timestamp: {min_ts}")
        print(f"  Max Timestamp: {max_ts}")
        print()
        print(f"Data Volume:")
        print(f"  Total Rows:    {total_rows:,}")
        print(f"  Unique Tags:   {unique_tags}")
        print(f"  Unique Dates:  {unique_dates}")
        print()

        # Calculate age
        if max_ts:
            max_dt = datetime.fromisoformat(max_ts.replace('Z', '+00:00'))
            age_days = (datetime.now(max_dt.tzinfo) - max_dt).days
            print(f"Data Age:")
            print(f"  Most recent data is {age_days} days old")
            print()

            # Expected range (last 7 days)
            expected_min = datetime.now() - timedelta(days=7)
            expected_max = datetime.now()
            print(f"Expected Range (last 7 days):")
            print(f"  Should be: {expected_min.isoformat()} to {expected_max.isoformat()}")
            print()

            if age_days > 1:
                print("⚠️  WARNING: Data is older than expected!")
                print("   Expected: Last 7 days")
                print(f"   Actual: Data from {age_days} days ago")
            else:
                print("✓ Data is current (last 7 days)")

        print("=" * 80)
else:
    print(f"Query failed: {result.status.state}")
    if result.status.error:
        print(f"Error: {result.status.error.message}")
