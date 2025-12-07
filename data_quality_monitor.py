"""
Data Quality Monitoring Dashboard

Monitors data quality metrics across the PI ingestion pipeline:
- Null rate tracking
- Freshness monitoring (data staleness detection)
- Volume anomalies (unexpected spikes/drops)
- Quality flag distribution
- Duplicate detection
- Schema drift detection

Alerts when quality degrades below thresholds.
"""

import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from dataclasses import dataclass
from databricks.sdk import WorkspaceClient


@dataclass
class QualityMetric:
    """Single data quality metric"""
    metric_name: str
    current_value: float
    threshold: float
    status: str  # 'healthy', 'warning', 'critical'
    message: str
    checked_at: datetime


class DataQualityMonitor:
    """
    Monitors data quality for PI ingestion pipeline
    """

    def __init__(self, workspace_client: WorkspaceClient, warehouse_id: str):
        self.w = workspace_client
        self.warehouse_id = warehouse_id

    def check_all_metrics(self) -> List[QualityMetric]:
        """Run all quality checks and return results"""

        metrics = []

        print("🔍 Running data quality checks...")

        # 1. Null rate check
        metrics.append(self.check_null_rate())

        # 2. Freshness check
        metrics.append(self.check_freshness())

        # 3. Volume check
        metrics.append(self.check_volume_anomaly())

        # 4. Quality flag distribution
        metrics.append(self.check_quality_flags())

        # 5. Duplicate detection
        metrics.append(self.check_duplicates())

        # 6. Schema validation
        metrics.append(self.check_schema())

        return metrics

    def check_null_rate(self) -> QualityMetric:
        """Check percentage of null values in critical columns"""

        sql = """
        SELECT
            COUNT(*) as total_rows,
            SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) as null_values,
            SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as null_pct
        FROM osipi.bronze.pi_timeseries
        WHERE partition_date >= CURRENT_DATE() - INTERVAL 1 DAY
        """

        result = self._execute_sql(sql)
        row = result[0]

        null_pct = float(row[2])
        threshold = 5.0  # Alert if >5% nulls

        if null_pct > threshold:
            status = "critical"
            message = f"Null rate {null_pct:.1f}% exceeds threshold {threshold}%"
        elif null_pct > threshold * 0.8:
            status = "warning"
            message = f"Null rate {null_pct:.1f}% approaching threshold"
        else:
            status = "healthy"
            message = f"Null rate {null_pct:.1f}% is acceptable"

        return QualityMetric(
            metric_name="Null Rate",
            current_value=null_pct,
            threshold=threshold,
            status=status,
            message=message,
            checked_at=datetime.now()
        )

    def check_freshness(self) -> QualityMetric:
        """Check how recent the data is (staleness detection)"""

        sql = """
        SELECT
            MAX(ingestion_timestamp) as last_ingestion,
            TIMESTAMPDIFF(MINUTE, MAX(ingestion_timestamp), CURRENT_TIMESTAMP()) as minutes_old
        FROM osipi.bronze.pi_timeseries
        """

        result = self._execute_sql(sql)
        row = result[0]

        minutes_old = int(row[1]) if row[1] else 999999
        threshold = 60  # Alert if data >60 minutes old

        if minutes_old > threshold:
            status = "critical"
            message = f"Data is {minutes_old} minutes old (threshold: {threshold} min)"
        elif minutes_old > threshold * 0.8:
            status = "warning"
            message = f"Data freshness degrading: {minutes_old} minutes old"
        else:
            status = "healthy"
            message = f"Data is fresh: {minutes_old} minutes old"

        return QualityMetric(
            metric_name="Data Freshness",
            current_value=minutes_old,
            threshold=threshold,
            status=status,
            message=message,
            checked_at=datetime.now()
        )

    def check_volume_anomaly(self) -> QualityMetric:
        """Detect unexpected spikes or drops in data volume"""

        sql = """
        WITH hourly_volumes AS (
            SELECT
                DATE_TRUNC('hour', timestamp) as hour,
                COUNT(*) as row_count
            FROM osipi.bronze.pi_timeseries
            WHERE timestamp >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
            GROUP BY DATE_TRUNC('hour', timestamp)
        ),
        stats AS (
            SELECT
                AVG(row_count) as avg_volume,
                STDDEV(row_count) as stddev_volume
            FROM hourly_volumes
        ),
        latest AS (
            SELECT row_count as latest_volume
            FROM hourly_volumes
            ORDER BY hour DESC
            LIMIT 1
        )
        SELECT
            latest.latest_volume,
            stats.avg_volume,
            stats.stddev_volume,
            (latest.latest_volume - stats.avg_volume) / stats.stddev_volume as z_score
        FROM latest, stats
        """

        result = self._execute_sql(sql)
        if not result or not result[0][0]:
            return QualityMetric(
                metric_name="Volume Anomaly",
                current_value=0,
                threshold=3.0,
                status="warning",
                message="Insufficient data for volume analysis",
                checked_at=datetime.now()
            )

        row = result[0]
        z_score = abs(float(row[3]))
        threshold = 3.0  # Alert if >3 standard deviations

        if z_score > threshold:
            status = "critical"
            message = f"Volume anomaly detected: z-score {z_score:.2f}"
        elif z_score > threshold * 0.8:
            status = "warning"
            message = f"Volume fluctuation: z-score {z_score:.2f}"
        else:
            status = "healthy"
            message = f"Volume is normal: z-score {z_score:.2f}"

        return QualityMetric(
            metric_name="Volume Anomaly",
            current_value=z_score,
            threshold=threshold,
            status=status,
            message=message,
            checked_at=datetime.now()
        )

    def check_quality_flags(self) -> QualityMetric:
        """Check distribution of quality flags (Good/Questionable)"""

        sql = """
        SELECT
            SUM(CASE WHEN quality_good THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as good_pct
        FROM osipi.bronze.pi_timeseries
        WHERE partition_date >= CURRENT_DATE() - INTERVAL 1 DAY
        """

        result = self._execute_sql(sql)
        row = result[0]

        good_pct = float(row[0])
        threshold = 90.0  # Alert if <90% good quality

        if good_pct < threshold:
            status = "critical"
            message = f"Only {good_pct:.1f}% good quality (threshold: {threshold}%)"
        elif good_pct < threshold + 5:
            status = "warning"
            message = f"Quality degrading: {good_pct:.1f}% good"
        else:
            status = "healthy"
            message = f"Quality is good: {good_pct:.1f}% good flags"

        return QualityMetric(
            metric_name="Quality Flags",
            current_value=good_pct,
            threshold=threshold,
            status=status,
            message=message,
            checked_at=datetime.now()
        )

    def check_duplicates(self) -> QualityMetric:
        """Detect duplicate records (same tag + timestamp)"""

        sql = """
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT tag_webid, timestamp) as unique_rows,
            (COUNT(*) - COUNT(DISTINCT tag_webid, timestamp)) * 100.0 / COUNT(*) as dup_pct
        FROM osipi.bronze.pi_timeseries
        WHERE partition_date >= CURRENT_DATE() - INTERVAL 1 DAY
        """

        result = self._execute_sql(sql)
        row = result[0]

        dup_pct = float(row[2])
        threshold = 1.0  # Alert if >1% duplicates

        if dup_pct > threshold:
            status = "critical"
            message = f"Duplicate rate {dup_pct:.1f}% exceeds threshold {threshold}%"
        elif dup_pct > threshold * 0.5:
            status = "warning"
            message = f"Duplicate rate {dup_pct:.1f}% detected"
        else:
            status = "healthy"
            message = f"Duplicate rate {dup_pct:.1f}% is acceptable"

        return QualityMetric(
            metric_name="Duplicates",
            current_value=dup_pct,
            threshold=threshold,
            status=status,
            message=message,
            checked_at=datetime.now()
        )

    def check_schema(self) -> QualityMetric:
        """Validate schema hasn't drifted"""

        sql = """
        DESCRIBE TABLE osipi.bronze.pi_timeseries
        """

        result = self._execute_sql(sql)

        expected_columns = {
            'tag_webid', 'timestamp', 'value', 'quality_good',
            'quality_questionable', 'units', 'ingestion_timestamp', 'partition_date'
        }

        actual_columns = {row[0] for row in result}
        missing = expected_columns - actual_columns
        extra = actual_columns - expected_columns

        if missing or extra:
            status = "critical"
            message = f"Schema drift detected: missing={missing}, extra={extra}"
        else:
            status = "healthy"
            message = "Schema is valid"

        return QualityMetric(
            metric_name="Schema Validation",
            current_value=len(actual_columns),
            threshold=len(expected_columns),
            status=status,
            message=message,
            checked_at=datetime.now()
        )

    def _execute_sql(self, sql: str) -> List[Tuple]:
        """Execute SQL and return results"""
        stmt = self.w.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=self.warehouse_id,
            wait_timeout="30s"
        )

        if stmt.result and stmt.result.data_array:
            return stmt.result.data_array
        return []

    def print_quality_report(self, metrics: List[QualityMetric]):
        """Print formatted quality report"""

        print("\n" + "="*70)
        print("DATA QUALITY REPORT")
        print("="*70)
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()

        # Group by status
        critical = [m for m in metrics if m.status == 'critical']
        warning = [m for m in metrics if m.status == 'warning']
        healthy = [m for m in metrics if m.status == 'healthy']

        if critical:
            print("🔴 CRITICAL ISSUES:")
            for m in critical:
                print(f"   • {m.metric_name}: {m.message}")
            print()

        if warning:
            print("⚠️  WARNINGS:")
            for m in warning:
                print(f"   • {m.metric_name}: {m.message}")
            print()

        print("✅ HEALTHY METRICS:")
        for m in healthy:
            print(f"   • {m.metric_name}: {m.message}")

        print("\n" + "="*70)
        print(f"Overall Status: {len(critical)} critical, {len(warning)} warnings, {len(healthy)} healthy")
        print("="*70)


def main():
    """Run data quality monitoring"""

    DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
    DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")

    w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

    monitor = DataQualityMonitor(w, DATABRICKS_WAREHOUSE_ID)

    # Run all checks
    metrics = monitor.check_all_metrics()

    # Print report
    monitor.print_quality_report(metrics)

    # Optional: Send alerts if critical issues found
    critical_count = len([m for m in metrics if m.status == 'critical'])
    if critical_count > 0:
        print(f"\n⚠️  ACTION REQUIRED: {critical_count} critical issues detected!")
        # send_alert_to_slack(metrics)  # Integration point


if __name__ == "__main__":
    main()
