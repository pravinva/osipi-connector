"""
Advanced Late-Data Handling

Handles scenarios where data arrives late (out-of-order):
- Backfill detection: Data arrives for past timestamps
- Merge strategy: Properly merge late data into existing partitions
- Duplicate resolution: Handle same timestamp arriving multiple times
- Audit trail: Track when late data was received and processed

Common scenarios:
1. PI Server buffering during network outage
2. Manual backfills from archive
3. Delayed event frames
4. Clock synchronization issues
"""

import os
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


class LateDataHandler:
    """
    Handles late-arriving data in the PI ingestion pipeline
    """

    def __init__(self, workspace_client: WorkspaceClient, warehouse_id: str):
        self.w = workspace_client
        self.warehouse_id = warehouse_id

    def detect_late_data(self, lookback_days: int = 7) -> Dict:
        """
        Detect if recently ingested data contains late arrivals

        Late data = data where timestamp << ingestion_timestamp

        Returns:
            Dictionary with late data statistics
        """

        sql = f"""
        SELECT
            COUNT(*) as late_records,
            MIN(TIMESTAMPDIFF(HOUR, timestamp, ingestion_timestamp)) as max_lateness_hours,
            COUNT(DISTINCT partition_date) as affected_partitions,
            MIN(timestamp) as oldest_late_timestamp
        FROM osipi.bronze.pi_timeseries
        WHERE
            partition_date >= CURRENT_DATE() - INTERVAL {lookback_days} DAYS
            AND TIMESTAMPDIFF(HOUR, timestamp, ingestion_timestamp) > 4  -- >4 hours late
        """

        result = self._execute_sql(sql)
        row = result[0]

        return {
            'late_records': int(row[0]) if row[0] else 0,
            'max_lateness_hours': int(row[1]) if row[1] else 0,
            'affected_partitions': int(row[2]) if row[2] else 0,
            'oldest_late_timestamp': row[3]
        }

    def handle_late_arrivals(self, late_data_threshold_hours: int = 4):
        """
        Process late-arriving data using Delta Lake MERGE

        Strategy:
        1. Identify late records
        2. Use MERGE to update existing or insert new
        3. Preserve original ingestion_timestamp for audit
        4. Add late_arrival_flag
        """

        print(f"🔄 Processing late arrivals (>{late_data_threshold_hours}h old)...")

        # Step 1: Create temporary view of late data
        create_view_sql = f"""
        CREATE OR REPLACE TEMP VIEW late_data AS
        SELECT *,
            TIMESTAMPDIFF(HOUR, timestamp, ingestion_timestamp) as lateness_hours
        FROM osipi.bronze.pi_timeseries
        WHERE
            partition_date >= CURRENT_DATE() - INTERVAL 30 DAYS
            AND TIMESTAMPDIFF(HOUR, timestamp, ingestion_timestamp) > {late_data_threshold_hours}
        """

        self._execute_sql(create_view_sql)

        # Step 2: Merge late data (update or insert)
        merge_sql = """
        MERGE INTO osipi.bronze.pi_timeseries AS target
        USING late_data AS source
        ON target.tag_webid = source.tag_webid AND target.timestamp = source.timestamp
        WHEN MATCHED THEN
            UPDATE SET
                target.value = source.value,
                target.quality_good = source.quality_good,
                target.quality_questionable = source.quality_questionable,
                target.units = source.units,
                target.ingestion_timestamp = CURRENT_TIMESTAMP(),  -- Update to current
                target.late_arrival = TRUE,
                target.original_ingestion_timestamp = target.ingestion_timestamp  -- Preserve original
        WHEN NOT MATCHED THEN
            INSERT (
                tag_webid, timestamp, value, quality_good, quality_questionable,
                units, ingestion_timestamp, partition_date, late_arrival, lateness_hours
            )
            VALUES (
                source.tag_webid, source.timestamp, source.value, source.quality_good,
                source.quality_questionable, source.units, CURRENT_TIMESTAMP(),
                source.partition_date, TRUE, source.lateness_hours
            )
        """

        result = self._execute_sql(merge_sql)
        print("✅ Late data processed")

        return result

    def create_late_data_audit_table(self):
        """
        Create audit table to track late data arrivals
        """

        sql = """
        CREATE TABLE IF NOT EXISTS osipi.bronze.late_data_audit (
            audit_id STRING DEFAULT uuid(),
            detection_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            late_records_count INT,
            max_lateness_hours INT,
            affected_partitions INT,
            oldest_late_timestamp TIMESTAMP,
            processing_status STRING,  -- 'detected', 'processing', 'completed', 'failed'
            processing_duration_seconds INT,
            notes STRING
        )
        USING DELTA
        """

        self._execute_sql(sql)
        print("✅ Late data audit table created")

    def backfill_historical_data(
        self,
        start_date: str,
        end_date: str,
        tag_list: List[str] = None
    ):
        """
        Backfill historical data for specific date range

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            tag_list: Optional list of specific tags to backfill

        This would integrate with the PI connector to re-extract data
        """

        print(f"📥 Initiating backfill: {start_date} to {end_date}")

        # Log backfill initiation
        log_sql = f"""
        INSERT INTO osipi.bronze.late_data_audit (
            detection_timestamp,
            processing_status,
            notes
        ) VALUES (
            CURRENT_TIMESTAMP(),
            'backfill_initiated',
            'Backfill requested: {start_date} to {end_date}'
        )
        """

        self._execute_sql(log_sql)

        # Actual backfill would call PI connector
        # connector.extract_data(start_time=start_date, end_time=end_date, tags=tag_list)

        print("✅ Backfill initiated (connector execution required)")

    def optimize_partitions_with_late_data(self):
        """
        Optimize partitions that received late data

        Delta Lake OPTIMIZE + ZORDER for affected partitions
        """

        # Find partitions with late data
        sql = """
        SELECT DISTINCT partition_date
        FROM osipi.bronze.pi_timeseries
        WHERE late_arrival = TRUE
        ORDER BY partition_date DESC
        LIMIT 10
        """

        result = self._execute_sql(sql)
        affected_dates = [row[0] for row in result]

        print(f"🔧 Optimizing {len(affected_dates)} partitions with late data...")

        for partition_date in affected_dates:
            optimize_sql = f"""
            OPTIMIZE osipi.bronze.pi_timeseries
            WHERE partition_date = '{partition_date}'
            ZORDER BY (tag_webid, timestamp)
            """

            self._execute_sql(optimize_sql)
            print(f"   ✓ Optimized partition {partition_date}")

        print("✅ Partition optimization complete")

    def generate_late_data_report(self) -> str:
        """
        Generate comprehensive late data report
        """

        sql = """
        SELECT
            partition_date,
            COUNT(*) as total_late_records,
            AVG(lateness_hours) as avg_lateness_hours,
            MAX(lateness_hours) as max_lateness_hours,
            COUNT(DISTINCT tag_webid) as affected_tags
        FROM osipi.bronze.pi_timeseries
        WHERE late_arrival = TRUE
        GROUP BY partition_date
        ORDER BY partition_date DESC
        LIMIT 30
        """

        result = self._execute_sql(sql)

        report = []
        report.append("\n" + "="*80)
        report.append("LATE DATA ANALYSIS REPORT")
        report.append("="*80)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        report.append("Partition        Late Records  Avg Lateness  Max Lateness  Affected Tags")
        report.append("-" * 80)

        for row in result:
            report.append(
                f"{row[0]}        {row[1]:>12}  {row[2]:>12.1f}h  {row[3]:>12.1f}h  {row[4]:>13}"
            )

        report.append("="*80)

        return "\n".join(report)

    def _execute_sql(self, sql: str):
        """Execute SQL statement"""
        stmt = self.w.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=self.warehouse_id,
            wait_timeout="60s"
        )

        if stmt.result and stmt.result.data_array:
            return stmt.result.data_array
        return []


def main():
    """
    Example usage of late data handling
    """

    DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
    DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")

    w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

    handler = LateDataHandler(w, DATABRICKS_WAREHOUSE_ID)

    # 1. Detect late data
    print("1️⃣  Detecting late data arrivals...")
    late_stats = handler.detect_late_data(lookback_days=7)
    print(f"   Found {late_stats['late_records']} late records")
    print(f"   Max lateness: {late_stats['max_lateness_hours']} hours")
    print(f"   Affected partitions: {late_stats['affected_partitions']}")

    # 2. Handle late arrivals
    if late_stats['late_records'] > 0:
        print("\n2️⃣  Processing late arrivals...")
        handler.handle_late_arrivals(late_data_threshold_hours=4)

    # 3. Optimize affected partitions
    print("\n3️⃣  Optimizing affected partitions...")
    handler.optimize_partitions_with_late_data()

    # 4. Generate report
    print("\n4️⃣  Generating late data report...")
    report = handler.generate_late_data_report()
    print(report)


if __name__ == "__main__":
    main()
