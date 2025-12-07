"""
Delta Lake Writer for Streaming PI Data

Writes WebSocket streaming data to Unity Catalog Delta tables
with micro-batch processing and optimizations.

Part of Module 6: WebSocket Streaming Integration
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, BooleanType, IntegerType
)
from pyspark.sql.functions import col, lit, current_timestamp
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class StreamingDeltaWriter:
    """
    Writes streaming PI data to Unity Catalog Delta tables.

    Features:
    - Micro-batch writes (accumulate records before write)
    - Schema enforcement for streaming data
    - Automatic partitioning by date
    - Deduplication support
    - Optimized for high-frequency updates
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "main",
        schema: str = "bronze",
        table_name: str = "pi_streaming_timeseries"
    ):
        """
        Initialize streaming Delta Lake writer.

        Args:
            spark: Active Spark session
            catalog: Unity Catalog name
            schema: Schema (database) name
            table_name: Table name for streaming data
        """
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.table_name = table_name
        self.full_table_name = f"{catalog}.{schema}.{table_name}"

        logger.info(f"Initializing StreamingDeltaWriter for {self.full_table_name}")

        # Ensure catalog and schema exist
        self._ensure_catalog_schema()

        # Ensure streaming table exists
        self._ensure_table_exists()

    def _ensure_catalog_schema(self):
        """Create catalog and schema if they don't exist."""
        try:
            self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog}")
            logger.info(f"Catalog {self.catalog} ready")
        except Exception as e:
            logger.warning(f"Catalog creation warning (may already exist): {e}")

        try:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
            logger.info(f"Schema {self.catalog}.{self.schema} ready")
        except Exception as e:
            logger.warning(f"Schema creation warning (may already exist): {e}")

    def _ensure_table_exists(self):
        """
        Create streaming table if it doesn't exist.

        Schema matches batch ingestion schema for compatibility.
        """
        schema_ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.full_table_name} (
            tag_webid STRING COMMENT 'PI tag WebID',
            timestamp TIMESTAMP COMMENT 'Data timestamp from PI',
            value DOUBLE COMMENT 'Numeric value',
            good BOOLEAN COMMENT 'Quality flag: Good',
            questionable BOOLEAN COMMENT 'Quality flag: Questionable',
            substituted BOOLEAN COMMENT 'Quality flag: Substituted',
            uom STRING COMMENT 'Unit of measure',
            received_timestamp TIMESTAMP COMMENT 'When data received via WebSocket',
            ingestion_timestamp TIMESTAMP COMMENT 'When written to Delta Lake',
            partition_date DATE COMMENT 'Partition column (date from timestamp)'
        )
        USING DELTA
        PARTITIONED BY (partition_date)
        COMMENT 'Real-time streaming PI time-series data from WebSocket'
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'delta.deletedFileRetentionDuration' = 'interval 7 days'
        )
        """

        try:
            self.spark.sql(schema_ddl)
            logger.info(f"Table {self.full_table_name} ready")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise

    def write_batch(
        self,
        records: List[Dict[str, Any]],
        deduplicate: bool = True
    ) -> int:
        """
        Write micro-batch of streaming records to Delta Lake.

        Args:
            records: List of streaming data records with structure:
                {
                    'tag_webid': str,
                    'timestamp': str (ISO format),
                    'value': float,
                    'good': bool,
                    'questionable': bool,
                    'substituted': bool,
                    'uom': str,
                    'received_timestamp': str (ISO format)
                }
            deduplicate: Remove duplicate (tag_webid, timestamp) pairs

        Returns:
            Number of records written
        """
        if not records:
            logger.warning("Empty batch, skipping write")
            return 0

        logger.info(f"Writing batch of {len(records)} streaming records")

        try:
            # Convert to pandas DataFrame
            df_pandas = pd.DataFrame(records)

            # Convert timestamp strings to datetime
            df_pandas['timestamp'] = pd.to_datetime(df_pandas['timestamp'])
            df_pandas['received_timestamp'] = pd.to_datetime(df_pandas['received_timestamp'])

            # Add ingestion timestamp
            df_pandas['ingestion_timestamp'] = pd.Timestamp.now()

            # Add partition column
            df_pandas['partition_date'] = df_pandas['timestamp'].dt.date

            # Deduplicate if requested
            if deduplicate:
                before_count = len(df_pandas)
                df_pandas = df_pandas.drop_duplicates(
                    subset=['tag_webid', 'timestamp'],
                    keep='last'  # Keep latest value if duplicate timestamps
                )
                after_count = len(df_pandas)
                if before_count != after_count:
                    logger.info(f"Deduplicated: {before_count} → {after_count} records")

            # Convert to Spark DataFrame
            df_spark = self.spark.createDataFrame(df_pandas)

            # Write to Delta table with append mode
            df_spark.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(self.full_table_name)

            row_count = len(df_pandas)
            logger.info(f"Successfully wrote {row_count} records to {self.full_table_name}")

            return row_count

        except Exception as e:
            logger.error(f"Failed to write batch: {e}")
            raise

    def write_single_record(self, record: Dict[str, Any]):
        """
        Write single record to Delta Lake.

        Note: For high-frequency streaming, prefer write_batch() for efficiency.

        Args:
            record: Single streaming data record
        """
        self.write_batch([record], deduplicate=False)

    def optimize_table(self):
        """
        Optimize Delta table with ZORDER.

        Run periodically (e.g., hourly) to improve query performance.
        """
        try:
            logger.info(f"Optimizing {self.full_table_name}...")

            # OPTIMIZE command with ZORDER on common query columns
            self.spark.sql(f"""
                OPTIMIZE {self.full_table_name}
                ZORDER BY (tag_webid, timestamp)
            """)

            logger.info(f"Optimization complete for {self.full_table_name}")

        except Exception as e:
            logger.warning(f"Optimization failed (non-critical): {e}")

    def get_table_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the streaming table.

        Returns:
            Dictionary with table statistics
        """
        try:
            stats_query = f"""
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT tag_webid) as unique_tags,
                MIN(timestamp) as earliest_timestamp,
                MAX(timestamp) as latest_timestamp,
                MAX(ingestion_timestamp) as last_write,
                SUM(CASE WHEN good THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as quality_pct
            FROM {self.full_table_name}
            """

            result = self.spark.sql(stats_query).collect()

            if result:
                row = result[0]
                return {
                    'total_records': row['total_records'],
                    'unique_tags': row['unique_tags'],
                    'earliest_timestamp': row['earliest_timestamp'],
                    'latest_timestamp': row['latest_timestamp'],
                    'last_write': row['last_write'],
                    'quality_pct': float(row['quality_pct']) if row['quality_pct'] else 0.0
                }

            return {}

        except Exception as e:
            logger.error(f"Failed to get table stats: {e}")
            return {}

    def vacuum_old_data(self, retention_hours: int = 168):
        """
        Remove old streaming data to manage storage costs.

        Args:
            retention_hours: Delete data older than this (default 7 days)
        """
        try:
            logger.info(f"Vacuuming data older than {retention_hours} hours...")

            # Delta Lake VACUUM command
            self.spark.sql(f"""
                VACUUM {self.full_table_name}
                RETAIN {retention_hours} HOURS
            """)

            logger.info("Vacuum complete")

        except Exception as e:
            logger.warning(f"Vacuum failed (non-critical): {e}")


class StreamingBuffer:
    """
    In-memory buffer for streaming data before Delta Lake writes.

    Accumulates streaming updates and flushes to Delta Lake based on:
    - Time interval (e.g., every 60 seconds)
    - Buffer size (e.g., every 10,000 records)
    """

    def __init__(
        self,
        writer: StreamingDeltaWriter,
        flush_interval_seconds: int = 60,
        max_buffer_size: int = 10000,
        auto_optimize_interval: int = 3600  # 1 hour
    ):
        """
        Initialize streaming buffer.

        Args:
            writer: StreamingDeltaWriter instance
            flush_interval_seconds: Seconds between automatic flushes
            max_buffer_size: Maximum records before forcing flush
            auto_optimize_interval: Seconds between table optimizations
        """
        self.writer = writer
        self.buffer: List[Dict[str, Any]] = []
        self.flush_interval = flush_interval_seconds
        self.max_buffer_size = max_buffer_size
        self.last_flush = datetime.now()
        self.last_optimize = datetime.now()
        self.auto_optimize_interval = auto_optimize_interval
        self.total_records_written = 0
        self.total_flushes = 0

    def add_record(self, tag_webid: str, data: Dict[str, Any]):
        """
        Add record to buffer.

        Args:
            tag_webid: Tag WebID for the data
            data: Data record from WebSocket with fields:
                - timestamp: str
                - value: float
                - good: bool
                - questionable: bool
                - substituted: bool
                - uom: str
                - received_timestamp: str
        """
        record = {
            'tag_webid': tag_webid,
            **data
        }
        self.buffer.append(record)

        logger.debug(f"Added record to buffer: {tag_webid} (buffer size: {len(self.buffer)})")

    def should_flush(self) -> bool:
        """
        Check if buffer should be flushed.

        Returns:
            True if flush is needed (time or size threshold reached)
        """
        if not self.buffer:
            return False

        elapsed = (datetime.now() - self.last_flush).total_seconds()

        size_threshold_met = len(self.buffer) >= self.max_buffer_size
        time_threshold_met = elapsed >= self.flush_interval

        return size_threshold_met or time_threshold_met

    def flush(self) -> int:
        """
        Flush buffer to Delta Lake.

        Returns:
            Number of records written
        """
        if not self.buffer:
            logger.debug("Buffer empty, nothing to flush")
            return 0

        record_count = len(self.buffer)
        logger.info(f"Flushing {record_count} records to Delta Lake")

        try:
            # Write batch to Delta Lake
            written = self.writer.write_batch(self.buffer, deduplicate=True)

            # Clear buffer
            self.buffer.clear()
            self.last_flush = datetime.now()
            self.total_records_written += written
            self.total_flushes += 1

            logger.info(f"Flush complete: {written} records written (total: {self.total_records_written})")

            # Check if optimization needed
            self._maybe_optimize()

            return written

        except Exception as e:
            logger.error(f"Flush failed: {e}")
            # Keep buffer intact on failure for retry
            raise

    def _maybe_optimize(self):
        """Run table optimization if interval elapsed."""
        elapsed = (datetime.now() - self.last_optimize).total_seconds()

        if elapsed >= self.auto_optimize_interval:
            logger.info("Auto-optimization interval reached, optimizing table...")
            self.writer.optimize_table()
            self.last_optimize = datetime.now()

    def get_stats(self) -> Dict[str, Any]:
        """
        Get buffer statistics.

        Returns:
            Dictionary with buffer stats
        """
        return {
            'buffer_size': len(self.buffer),
            'total_records_written': self.total_records_written,
            'total_flushes': self.total_flushes,
            'avg_records_per_flush': (
                self.total_records_written / self.total_flushes
                if self.total_flushes > 0 else 0
            ),
            'last_flush': self.last_flush.isoformat(),
            'last_optimize': self.last_optimize.isoformat()
        }

    def force_flush(self) -> int:
        """
        Force immediate flush regardless of thresholds.

        Useful for graceful shutdown.

        Returns:
            Number of records written
        """
        logger.info("Force flushing buffer...")
        return self.flush()
