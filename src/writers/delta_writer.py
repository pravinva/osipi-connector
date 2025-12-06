from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import *
import logging

class DeltaLakeWriter:
    """
    Writes PI data to Unity Catalog Delta tables
    Handles schema evolution, optimizations
    """

    def __init__(self, spark: SparkSession, catalog: str, schema: str):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.logger = logging.getLogger(__name__)
        self._ensure_schema_exists()

    def _ensure_schema_exists(self):
        """Create catalog and schema if don't exist"""
        self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog}")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")

    def write_timeseries(
        self,
        df: DataFrame,
        table_name: str = "pi_timeseries",
        partition_cols: List[str] = None
    ):
        """
        Write time-series data with optimizations

        Optimizations:
        - Partitioned by date for query performance
        - ZORDER by tag_webid for filtering
        - Schema evolution enabled
        """

        if df.empty:
            self.logger.warning("Empty dataframe, skipping write")
            return

        # Convert pandas to Spark DataFrame if needed
        if isinstance(df, pd.DataFrame):
            df = self.spark.createDataFrame(df)

        # Add partition column (date from timestamp)
        df = df.withColumn("partition_date", col("timestamp").cast("date"))

        full_table_name = f"{self.catalog}.{self.schema}.{table_name}"

        # Write with append mode
        df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("partition_date") \
            .saveAsTable(full_table_name)

        # Optimize with ZORDER for better query performance
        try:
            self.spark.sql(f"""
                OPTIMIZE {full_table_name}
                ZORDER BY (tag_webid, timestamp)
            """)
            self.logger.info(f"Optimized {full_table_name}")
        except Exception as e:
            self.logger.warning(f"Optimization failed (non-critical): {e}")

    def write_af_hierarchy(self, df: DataFrame):
        """Write AF hierarchy as dimension table"""
        full_table_name = f"{self.catalog}.{self.schema}.pi_asset_hierarchy"

        if isinstance(df, pd.DataFrame):
            df = self.spark.createDataFrame(df)

        # Overwrite mode for hierarchy (full refresh)
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(full_table_name)

        self.logger.info(f"Wrote {df.count()} elements to {full_table_name}")

    def write_event_frames(self, df: DataFrame):
        """Write event frames with incremental logic"""
        full_table_name = f"{self.catalog}.{self.schema}.pi_event_frames"

        if isinstance(df, pd.DataFrame):
            df = self.spark.createDataFrame(df)

        # Append mode for events
        df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(full_table_name)
