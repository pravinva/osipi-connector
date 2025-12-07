"""
Enhanced Delta Lake Writer with Proactive Late Data Detection

Adds AVEVA-style proactive detection to the ingestion pipeline:
- Flags late data at write time (not hours later)
- Stream-time watermarking for immediate visibility
- Automatic clock skew detection
- Write-time quality validation
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)


class EnhancedDeltaWriter:
    """
    Enhanced Delta Lake writer with proactive late data detection

    Key improvements:
    1. Detects and flags late arrivals at ingestion time
    2. Adds stream-time watermark for monitoring
    3. Calculates lateness metrics immediately
    4. Enables real-time dashboard updates (no post-processing needed)
    """

    def __init__(self, spark_session=None):
        self.spark = spark_session
        self.late_data_threshold_seconds = 4 * 3600  # 4 hours

    def enrich_with_lateness_metadata(
        self,
        records: List[Dict[str, Any]],
        ingestion_timestamp: datetime
    ) -> List[Dict[str, Any]]:
        """
        Enrich records with lateness metadata (like AVEVA Store & Forward)

        This happens at ingestion time, not post-processing:
        - Immediate visibility in dashboards
        - No need for late-running batch jobs
        - Data is flagged as "late" when it arrives, not discovered later

        Args:
            records: List of data records with 'timestamp' field
            ingestion_timestamp: Current ingestion time

        Returns:
            Records enriched with lateness metadata
        """

        enriched = []

        for record in records:
            # Copy original record
            enriched_record = record.copy()

            # Get data timestamp
            data_timestamp = record.get('timestamp')
            if isinstance(data_timestamp, str):
                data_timestamp = datetime.fromisoformat(data_timestamp.replace('Z', ''))
            elif not isinstance(data_timestamp, datetime):
                # Skip if no valid timestamp
                enriched.append(enriched_record)
                continue

            # Calculate lateness
            lateness_seconds = (ingestion_timestamp - data_timestamp).total_seconds()
            lateness_hours = lateness_seconds / 3600

            # Add lateness metadata
            enriched_record['ingestion_timestamp'] = ingestion_timestamp
            enriched_record['lateness_seconds'] = lateness_seconds
            enriched_record['lateness_hours'] = lateness_hours
            enriched_record['late_arrival'] = lateness_seconds > self.late_data_threshold_seconds

            # Categorize lateness severity
            if lateness_hours > 168:  # >1 week
                enriched_record['lateness_category'] = 'very_late'
            elif lateness_hours > 24:  # >1 day
                enriched_record['lateness_category'] = 'late'
            elif lateness_hours > 4:  # >4 hours
                enriched_record['lateness_category'] = 'slightly_late'
            else:
                enriched_record['lateness_category'] = 'on_time'

            # Flag potential clock skew (negative lateness)
            if lateness_seconds < -300:  # >5 minutes in the future
                enriched_record['potential_clock_skew'] = True
                enriched_record['lateness_category'] = 'future_timestamp'
            else:
                enriched_record['potential_clock_skew'] = False

            enriched.append(enriched_record)

        # Log statistics
        late_count = sum(1 for r in enriched if r.get('late_arrival', False))
        clock_skew_count = sum(1 for r in enriched if r.get('potential_clock_skew', False))

        if late_count > 0:
            logger.warning(f"⚠️  {late_count}/{len(enriched)} records are late (>{self.late_data_threshold_seconds/3600:.0f}h old)")

        if clock_skew_count > 0:
            logger.warning(f"⚠️  {clock_skew_count}/{len(enriched)} records have future timestamps (clock skew suspected)")

        return enriched

    def write_with_late_data_detection(
        self,
        records: List[Dict[str, Any]],
        table_name: str,
        mode: str = "append"
    ):
        """
        Write data with automatic late data detection

        This replaces the basic write() method with enhanced version that:
        1. Detects late arrivals at ingestion time
        2. Flags potential clock skew issues
        3. Adds metadata for monitoring

        Args:
            records: Data records to write
            table_name: Target Delta table
            mode: Write mode (append, overwrite, etc.)
        """

        if not records:
            logger.info("No records to write")
            return

        # Enrich with lateness metadata
        ingestion_timestamp = datetime.utcnow()
        enriched_records = self.enrich_with_lateness_metadata(records, ingestion_timestamp)

        # Convert to Spark DataFrame (if Spark session available)
        if self.spark:
            df = self.spark.createDataFrame(enriched_records)

            # Add partition column
            df = df.withColumn("partition_date", df.timestamp.cast("date"))

            # Write to Delta table
            df.write.format("delta") \
                .mode(mode) \
                .partitionBy("partition_date") \
                .saveAsTable(table_name)

            logger.info(f"✅ Wrote {len(enriched_records)} records to {table_name} with lateness detection")
        else:
            # Fallback: return enriched records for external writing
            logger.info(f"✅ Enriched {len(enriched_records)} records with lateness metadata")
            return enriched_records

    def calculate_write_time_quality_metrics(
        self,
        records: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Calculate quality metrics at write time (not post-processing)

        Returns metrics for immediate dashboard display:
        - Late data percentage
        - Clock skew issues
        - Quality flag distribution
        - Null rates

        This enables AVEVA-style real-time monitoring.
        """

        if not records:
            return {}

        total = len(records)
        late_count = sum(1 for r in records if r.get('late_arrival', False))
        clock_skew_count = sum(1 for r in records if r.get('potential_clock_skew', False))
        null_values = sum(1 for r in records if r.get('value') is None)
        good_quality = sum(1 for r in records if r.get('quality_good', False))

        # Calculate lateness distribution
        lateness_hours = [r.get('lateness_hours', 0) for r in records]
        avg_lateness = sum(lateness_hours) / len(lateness_hours) if lateness_hours else 0
        max_lateness = max(lateness_hours) if lateness_hours else 0

        return {
            'total_records': total,
            'late_records': late_count,
            'late_percentage': (late_count / total * 100) if total > 0 else 0,
            'clock_skew_records': clock_skew_count,
            'null_values': null_values,
            'null_percentage': (null_values / total * 100) if total > 0 else 0,
            'good_quality_records': good_quality,
            'good_quality_percentage': (good_quality / total * 100) if total > 0 else 0,
            'avg_lateness_hours': avg_lateness,
            'max_lateness_hours': max_lateness,
            'ingestion_timestamp': datetime.utcnow().isoformat()
        }


def example_usage():
    """
    Example: How to use enhanced writer in connector
    """

    writer = EnhancedDeltaWriter()

    # Sample data
    records = [
        {
            'tag_webid': 'tag1',
            'timestamp': datetime.utcnow() - timedelta(hours=6),  # 6 hours old = late
            'value': 123.45,
            'quality_good': True,
            'units': 'degC'
        },
        {
            'tag_webid': 'tag2',
            'timestamp': datetime.utcnow() - timedelta(minutes=30),  # 30 min = on time
            'value': 678.90,
            'quality_good': True,
            'units': 'PSI'
        },
        {
            'tag_webid': 'tag3',
            'timestamp': datetime.utcnow() + timedelta(minutes=10),  # Future = clock skew
            'value': 111.11,
            'quality_good': True,
            'units': 'GPM'
        }
    ]

    # Enrich with lateness metadata
    enriched = writer.enrich_with_lateness_metadata(records, datetime.utcnow())

    # Calculate quality metrics
    metrics = writer.calculate_write_time_quality_metrics(enriched)

    print("✅ Enriched Records:")
    for record in enriched:
        print(f"   {record['tag_webid']}: {record['lateness_category']} "
              f"({record['lateness_hours']:.1f}h old)")

    print("\n📊 Write-Time Quality Metrics:")
    print(f"   Late records: {metrics['late_percentage']:.1f}%")
    print(f"   Clock skew issues: {metrics['clock_skew_records']}")
    print(f"   Avg lateness: {metrics['avg_lateness_hours']:.1f}h")


if __name__ == "__main__":
    example_usage()
