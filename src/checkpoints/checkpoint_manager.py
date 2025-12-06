from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max
from typing import Dict, List
from datetime import datetime, timedelta
import logging

class CheckpointManager:
    """
    Manages incremental ingestion state
    Tracks last successful timestamp per tag
    """

    def __init__(self, spark: SparkSession, checkpoint_table: str):
        self.spark = spark
        self.checkpoint_table = checkpoint_table  # e.g., "checkpoints.pi_watermarks"
        self.logger = logging.getLogger(__name__)
        self._ensure_checkpoint_table_exists()

    def _ensure_checkpoint_table_exists(self):
        """Create checkpoint table if doesn't exist"""
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.checkpoint_table} (
                tag_webid STRING,
                tag_name STRING,
                last_timestamp TIMESTAMP,
                last_ingestion_run TIMESTAMP,
                record_count BIGINT
            )
            USING DELTA
        """)

    def get_watermarks(self, tag_webids: List[str]) -> Dict[str, datetime]:
        """
        Get last successful timestamp for each tag

        Returns:
            Dict mapping tag_webid -> last_timestamp
        """
        if not tag_webids:
            return {}

        # Query checkpoint table
        df = self.spark.table(self.checkpoint_table) \
            .filter(col("tag_webid").isin(tag_webids)) \
            .select("tag_webid", "last_timestamp")

        # Convert to dict
        watermarks = {
            row.tag_webid: row.last_timestamp
            for row in df.collect()
        }

        # For tags without checkpoint, use default (e.g., 30 days ago)
        default_start = datetime.now() - timedelta(days=30)
        for tag in tag_webids:
            if tag not in watermarks:
                watermarks[tag] = default_start
                self.logger.info(f"New tag {tag}, using default start: {default_start}")

        return watermarks

    def update_watermarks(self, tag_data: Dict[str, Dict]):
        """
        Update checkpoints after successful ingestion

        Args:
            tag_data: {
                tag_webid: {
                    "tag_name": str,
                    "max_timestamp": datetime,
                    "record_count": int
                }
            }
        """

        checkpoint_rows = []
        for tag_webid, data in tag_data.items():
            checkpoint_rows.append({
                "tag_webid": tag_webid,
                "tag_name": data["tag_name"],
                "last_timestamp": data["max_timestamp"],
                "last_ingestion_run": datetime.now(),
                "record_count": data["record_count"]
            })

        checkpoint_df = self.spark.createDataFrame(checkpoint_rows)

        # Upsert checkpoint (merge on tag_webid)
        checkpoint_df.createOrReplaceTempView("checkpoint_updates")

        self.spark.sql(f"""
            MERGE INTO {self.checkpoint_table} AS target
            USING checkpoint_updates AS source
            ON target.tag_webid = source.tag_webid
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        self.logger.info(f"Updated checkpoints for {len(checkpoint_rows)} tags")
