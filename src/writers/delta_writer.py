from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import *
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import TableInfo
from typing import List, Optional
import pandas as pd
import logging

class DeltaLakeWriter:
    """
    Writes PI data to Unity Catalog Delta tables
    Handles schema evolution, optimizations
    Uses Databricks SDK for catalog management
    """

    def __init__(
        self,
        spark: SparkSession,
        workspace_client: Optional[WorkspaceClient],
        catalog: str,
        schema: str,
        skip_schema_creation: bool = False
    ):
        self.spark = spark
        self.workspace_client = workspace_client
        self.catalog = catalog
        self.schema = schema
        self.logger = logging.getLogger(__name__)
        self.skip_schema_creation = skip_schema_creation

        # Skip schema creation in DLT mode
        if not skip_schema_creation:
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

        self.logger.info(f"Wrote {df.count()} events to {full_table_name}")

    def get_table_info(self, table_name: str) -> Optional[TableInfo]:
        """
        Get table metadata using Databricks SDK

        Args:
            table_name: Table name (without catalog.schema prefix)

        Returns:
            TableInfo object or None if table doesn't exist
        """
        if not self.workspace_client:
            self.logger.warning("WorkspaceClient not available, cannot get table info")
            return None

        full_table_name = f"{self.catalog}.{self.schema}.{table_name}"

        try:
            table_info = self.workspace_client.tables.get(full_table_name)
            return table_info
        except Exception as e:
            self.logger.warning(f"Table {full_table_name} not found: {e}")
            return None

    def list_tables(self) -> List[str]:
        """
        List all tables in the schema using Databricks SDK

        Returns:
            List of table names
        """
        if not self.workspace_client:
            self.logger.warning("WorkspaceClient not available, cannot list tables")
            return []

        try:
            tables = self.workspace_client.tables.list(
                catalog_name=self.catalog,
                schema_name=self.schema
            )
            return [table.name for table in tables]
        except Exception as e:
            self.logger.warning(f"Error listing tables: {e}")
            return []
