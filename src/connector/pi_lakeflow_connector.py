from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import logging
from typing import Dict, List
import pandas as pd


class PILakeflowConnector:
    """
    Main connector orchestrating all modules
    Entry point called by Lakeflow framework
    """
    
    def __init__(self, config: Dict):
        """
        Args:
            config: {
                'pi_web_api_url': str,
                'auth': {type, username, password},
                'catalog': str,
                'schema': str,
                'tags': List[str] or 'all',
                'af_database_id': str (optional),
                'include_event_frames': bool
            }
        """
        self.config = config
        self.spark = SparkSession.builder.getOrCreate()
        self.logger = logging.getLogger(__name__)
        
        # Initialize modules
        from src.auth.pi_auth_manager import PIAuthManager
        from src.client.pi_web_api_client import PIWebAPIClient
        from src.extractors.timeseries_extractor import TimeSeriesExtractor
        from src.extractors.af_extractor import AFHierarchyExtractor
        from src.extractors.event_frame_extractor import EventFrameExtractor
        from src.checkpoints.checkpoint_manager import CheckpointManager
        from src.writers.delta_writer import DeltaLakeWriter
        
        self.auth_manager = PIAuthManager(config['auth'])
        self.client = PIWebAPIClient(config['pi_web_api_url'], self.auth_manager)
        self.ts_extractor = TimeSeriesExtractor(self.client)
        self.af_extractor = AFHierarchyExtractor(self.client)
        self.ef_extractor = EventFrameExtractor(self.client)
        # Checkpoint table is in the same schema as data tables
        dlt_mode = config.get('dlt_mode', False)
        self.checkpoint_mgr = CheckpointManager(
            self.spark,
            f"{config['catalog']}.{config['schema']}.pi_watermarks",
            skip_table_creation=dlt_mode
        )
        self.writer = DeltaLakeWriter(
            self.spark,
            workspace_client=None,  # Not needed in DLT mode
            catalog=config['catalog'],
            schema=config['schema'],
            skip_schema_creation=dlt_mode
        )
    
    def run(self):
        """
        Main execution flow
        Called by Lakeflow scheduler
        """
        self.logger.info("Starting PI connector run")
        
        # Step 1: Extract AF hierarchy (full refresh, run first)
        if self.config.get('af_database_id'):
            self.logger.info("Extracting AF hierarchy...")
            af_hierarchy = self.af_extractor.extract_hierarchy(
                self.config['af_database_id']
            )
            self.writer.write_af_hierarchy(af_hierarchy)
        
        # Step 2: Get tags to ingest
        tag_webids = self._get_tag_list()
        self.logger.info(f"Processing {len(tag_webids)} tags")
        
        # Step 3: Get checkpoints (where we left off)
        watermarks = self.checkpoint_mgr.get_watermarks(tag_webids)
        
        # Step 4: Extract time-series data (incremental)
        end_time = datetime.now()
        
        all_timeseries = []
        # Process in batches of 100 tags (batch controller limit)
        BATCH_SIZE = 100
        for i in range(0, len(tag_webids), BATCH_SIZE):
            batch_tags = tag_webids[i:i+BATCH_SIZE]
            
            # Get earliest watermark for this batch
            min_start = min(watermarks[tag] for tag in batch_tags)
            
            self.logger.info(f"Extracting batch {i//BATCH_SIZE + 1}: {len(batch_tags)} tags")
            
            ts_df = self.ts_extractor.extract_recorded_data(
                tag_webids=batch_tags,
                start_time=min_start,
                end_time=end_time
            )
            
            all_timeseries.append(ts_df)
        
        # Combine and write
        if all_timeseries:
            combined_df = pd.concat(all_timeseries, ignore_index=True)
            self.writer.write_timeseries(combined_df)
            
            # Update checkpoints
            self._update_checkpoints_from_df(combined_df)
        
        # Step 5: Extract Event Frames (if enabled)
        if self.config.get('include_event_frames') and self.config.get('af_database_id'):
            self.logger.info("Extracting Event Frames...")
            
            # Get last event frame checkpoint
            last_ef_time = self._get_last_event_frame_time()
            
            ef_df = self.ef_extractor.extract_event_frames(
                database_webid=self.config['af_database_id'],
                start_time=last_ef_time,
                end_time=end_time
            )
            
            if not ef_df.empty:
                self.writer.write_event_frames(ef_df)
        
        self.logger.info("PI connector run completed successfully")
    
    def _get_tag_list(self) -> List[str]:
        """Get list of tag WebIds to ingest"""
        # For demo: hardcoded list or from config
        # Production: query /points endpoint with filters
        return self.config.get('tags', [])
    
    def _update_checkpoints_from_df(self, df: pd.DataFrame):
        """Update checkpoints based on ingested data"""
        # Group by tag, get max timestamp
        tag_stats = df.groupby('tag_webid').agg({
            'timestamp': 'max',
            'value': 'count'
        }).reset_index()
        
        tag_data = {}
        for _, row in tag_stats.iterrows():
            tag_data[row['tag_webid']] = {
                "tag_name": row['tag_webid'],  # Would map to actual name
                "max_timestamp": row['timestamp'],
                "record_count": row['value']
            }
        
        self.checkpoint_mgr.update_watermarks(tag_data)
    
    def _get_last_event_frame_time(self) -> datetime:
        """Get checkpoint for event frame extraction"""
        try:
            max_time = self.spark.sql(f"""
                SELECT MAX(start_time) as max_time
                FROM {self.config['catalog']}.{self.config['schema']}.pi_event_frames
            """).collect()[0].max_time

            return max_time if max_time else datetime.now() - timedelta(days=30)
        except:
            return datetime.now() - timedelta(days=30)

    def extract_timeseries_to_df(self):
        """
        Extract timeseries data and return as Spark DataFrame
        Simplified method for DLT usage
        """
        self.logger.info("Extracting timeseries data for DLT")

        # Get tags to ingest
        tag_webids = self._get_tag_list()
        self.logger.info(f"Processing {len(tag_webids)} tags")

        # Check if start_time/end_time provided in config (DLT mode with overlapping windows)
        if 'start_time' in self.config and 'end_time' in self.config:
            # Use config times (DLT native mode - no checkpoints needed)
            start_time = self.config['start_time']
            end_time = self.config['end_time']
            self.logger.info(f"Using config time range: {start_time} to {end_time}")
        else:
            # Fall back to checkpoint-based incremental (non-DLT mode)
            self.logger.info("Using checkpoint-based time ranges")
            watermarks = self.checkpoint_mgr.get_watermarks(tag_webids)
            end_time = datetime.now()

        all_timeseries = []
        # Process in batches of 100 tags (batch controller limit)
        BATCH_SIZE = 100
        for i in range(0, len(tag_webids), BATCH_SIZE):
            batch_tags = tag_webids[i:i+BATCH_SIZE]

            # Determine start time for this batch
            if 'start_time' in self.config:
                # DLT mode: use same time range for all tags (overlapping window)
                batch_start = start_time
            else:
                # Non-DLT mode: use checkpoint watermarks
                batch_start = min(watermarks[tag] for tag in batch_tags)

            self.logger.info(f"Extracting batch {i//BATCH_SIZE + 1}: {len(batch_tags)} tags from {batch_start} to {end_time}")

            ts_df = self.ts_extractor.extract_recorded_data(
                tag_webids=batch_tags,
                start_time=batch_start,
                end_time=end_time
            )

            all_timeseries.append(ts_df)

        # Combine all batches
        if all_timeseries:
            combined_df = pd.concat(all_timeseries, ignore_index=True)

            # Convert to Spark DataFrame
            spark_df = self.spark.createDataFrame(combined_df)

            self.logger.info(f"Extracted {combined_df.shape[0]} records")
            return spark_df
        else:
            # Return empty DataFrame with schema
            from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
            schema = StructType([
                StructField("tag_webid", StringType(), True),
                StructField("tag_name", StringType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("value", DoubleType(), True),
                StructField("good", StringType(), True)
            ])
            return self.spark.createDataFrame([], schema)

    def extract_af_hierarchy_to_df(self):
        """
        Extract AF hierarchy and return as Spark DataFrame
        Simplified method for DLT usage
        """
        self.logger.info("Extracting AF hierarchy for DLT")

        # Get all asset databases
        databases = self.af_extractor.get_asset_databases()
        self.logger.info(f"Found {len(databases)} asset databases")

        all_hierarchy = []
        for db in databases:
            db_webid = db["WebId"]
            db_name = db["Name"]
            self.logger.info(f"Extracting hierarchy from database: {db_name}")

            # Extract hierarchy for this database
            hierarchy_df = self.af_extractor.extract_hierarchy(db_webid)

            if not hierarchy_df.empty:
                # Add database info
                hierarchy_df['database_webid'] = db_webid
                hierarchy_df['database_name'] = db_name
                all_hierarchy.append(hierarchy_df)

        # Combine all databases
        if all_hierarchy:
            combined_df = pd.concat(all_hierarchy, ignore_index=True)

            # Convert to Spark DataFrame
            spark_df = self.spark.createDataFrame(combined_df)

            self.logger.info(f"Extracted {combined_df.shape[0]} AF elements")
            return spark_df
        else:
            # Return empty DataFrame with schema
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
            schema = StructType([
                StructField("element_id", StringType(), True),
                StructField("element_name", StringType(), True),
                StructField("element_path", StringType(), True),
                StructField("parent_id", StringType(), True),
                StructField("template_name", StringType(), True),
                StructField("element_type", StringType(), True),
                StructField("description", StringType(), True),
                StructField("categories", ArrayType(StringType()), True),
                StructField("depth", IntegerType(), True),
                StructField("database_webid", StringType(), True),
                StructField("database_name", StringType(), True)
            ])
            return self.spark.createDataFrame([], schema)

    def extract_event_frames_to_df(self):
        """
        Extract event frames and return as Spark DataFrame
        Simplified method for DLT usage
        """
        self.logger.info("Extracting event frames for DLT")

        # Get time range from config or use defaults
        if 'start_time' in self.config and 'end_time' in self.config:
            start_time = self.config['start_time']
            end_time = self.config['end_time']
        else:
            # Default to last 30 days
            end_time = datetime.now()
            start_time = end_time - timedelta(days=30)

        self.logger.info(f"Extracting event frames from {start_time} to {end_time}")

        # Get all asset databases
        databases = self.af_extractor.get_asset_databases()
        self.logger.info(f"Found {len(databases)} asset databases")

        all_events = []
        for db in databases:
            db_webid = db["WebId"]
            db_name = db["Name"]
            self.logger.info(f"Extracting event frames from database: {db_name}")

            # Extract event frames for this database
            events_df = self.ef_extractor.extract_event_frames(
                database_webid=db_webid,
                start_time=start_time,
                end_time=end_time
            )

            if not events_df.empty:
                # Add database info
                events_df['database_webid'] = db_webid
                events_df['database_name'] = db_name
                all_events.append(events_df)

        # Combine all databases
        if all_events:
            combined_df = pd.concat(all_events, ignore_index=True)

            # Convert event_attributes dict to string for Spark compatibility
            if 'event_attributes' in combined_df.columns:
                combined_df['event_attributes'] = combined_df['event_attributes'].apply(str)

            # Convert category list to string for Spark compatibility
            if 'category' in combined_df.columns:
                combined_df['category'] = combined_df['category'].apply(lambda x: ','.join(x) if isinstance(x, list) else '')

            # Convert referenced_elements list to string
            if 'referenced_elements' in combined_df.columns:
                combined_df['referenced_elements'] = combined_df['referenced_elements'].apply(lambda x: ','.join(x) if isinstance(x, list) else '')

            # Convert to Spark DataFrame
            spark_df = self.spark.createDataFrame(combined_df)

            self.logger.info(f"Extracted {combined_df.shape[0]} event frames")
            return spark_df
        else:
            # Return empty DataFrame with schema
            from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
            schema = StructType([
                StructField("event_frame_id", StringType(), True),
                StructField("event_name", StringType(), True),
                StructField("template_name", StringType(), True),
                StructField("start_time", TimestampType(), True),
                StructField("end_time", TimestampType(), True),
                StructField("primary_element_id", StringType(), True),
                StructField("category", StringType(), True),
                StructField("description", StringType(), True),
                StructField("duration_minutes", DoubleType(), True),
                StructField("event_attributes", StringType(), True),
                StructField("referenced_elements", StringType(), True),
                StructField("database_webid", StringType(), True),
                StructField("database_name", StringType(), True)
            ])
            return self.spark.createDataFrame([], schema)
