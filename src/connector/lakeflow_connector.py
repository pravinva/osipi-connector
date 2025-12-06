"""
OSI PI Lakeflow Connector - Main Entry Point

Databricks Lakeflow-compatible connector for OSI PI Systems.
Uses Databricks SDK for workspace integration.
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import TableInfo, TableType
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import requests
from typing import Dict, List, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PILakeflowConnector:
    """
    Main Lakeflow connector for OSI PI Systems

    Integrates with:
    - PI Web API (source)
    - Unity Catalog (destination)
    - Databricks SDK (orchestration)
    """

    def __init__(self, config: Dict):
        """
        Initialize connector with configuration

        Args:
            config: {
                'pi_web_api_url': str,
                'pi_auth_type': 'basic' | 'kerberos' | 'oauth',
                'pi_username': str (optional),
                'pi_password': str (optional),
                'catalog': str,
                'schema': str,
                'tags': List[str] or 'all',
                'af_database_id': str (optional),
                'include_event_frames': bool
            }
        """
        self.config = config
        self.spark = SparkSession.builder.getOrCreate()

        # Initialize Databricks SDK client
        self.workspace_client = WorkspaceClient()

        # Initialize components
        self._init_auth()
        self._init_extractors()
        self._init_writers()

        logger.info("PI Lakeflow Connector initialized")

    def _init_auth(self):
        """Initialize PI Web API authentication"""
        auth_type = self.config.get('pi_auth_type', 'basic')

        if auth_type == 'basic':
            # Basic authentication
            from requests.auth import HTTPBasicAuth
            self.auth = HTTPBasicAuth(
                self.config['pi_username'],
                self.config['pi_password']
            )
        elif auth_type == 'kerberos':
            # Kerberos authentication
            from requests_kerberos import HTTPKerberosAuth, OPTIONAL
            self.auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
        elif auth_type == 'oauth':
            # OAuth2 authentication
            self.auth = None  # Will use headers
            self.headers = {
                'Authorization': f"Bearer {self.config['oauth_token']}"
            }
        else:
            raise ValueError(f"Unsupported auth type: {auth_type}")

        # Create session with retry logic
        self.session = requests.Session()
        self.session.auth = self.auth

        # Test connection
        self._test_connection()

    def _test_connection(self):
        """Test PI Web API connection"""
        try:
            response = self.session.get(
                f"{self.config['pi_web_api_url']}/piwebapi",
                timeout=10
            )
            response.raise_for_status()
            logger.info("✓ PI Web API connection successful")
        except Exception as e:
            logger.error(f"✗ PI Web API connection failed: {e}")
            raise

    def _init_extractors(self):
        """Initialize data extractors"""
        # Import extractor modules
        from src.extractors.timeseries_extractor import TimeSeriesExtractor
        from src.extractors.af_extractor import AFHierarchyExtractor
        from src.extractors.event_frame_extractor import EventFrameExtractor

        self.ts_extractor = TimeSeriesExtractor(self.session, self.config)
        self.af_extractor = AFHierarchyExtractor(self.session, self.config)
        self.ef_extractor = EventFrameExtractor(self.session, self.config)

    def _init_writers(self):
        """Initialize Delta Lake writers"""
        from src.writers.delta_writer import DeltaLakeWriter

        self.writer = DeltaLakeWriter(
            self.spark,
            self.workspace_client,
            self.config['catalog'],
            self.config['schema']
        )

    def extract_timeseries(
        self,
        tag_webids: List[str],
        start_time: datetime,
        end_time: datetime,
        batch_size: int = 100
    ):
        """
        Extract time-series data using batch controller

        Args:
            tag_webids: List of PI point WebIds
            start_time: Start of time range
            end_time: End of time range
            batch_size: Number of tags per batch request (default 100)

        Returns:
            DataFrame with time-series data
        """
        logger.info(f"Extracting {len(tag_webids)} tags from {start_time} to {end_time}")

        all_data = []

        # Process in batches
        for i in range(0, len(tag_webids), batch_size):
            batch_tags = tag_webids[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}: {len(batch_tags)} tags")

            # Build batch request
            batch_payload = {
                "Requests": [
                    {
                        "Method": "GET",
                        "Resource": f"/streams/{webid}/recorded",
                        "Parameters": {
                            "startTime": start_time.isoformat() + "Z",
                            "endTime": end_time.isoformat() + "Z",
                            "maxCount": "10000"
                        }
                    }
                    for webid in batch_tags
                ]
            }

            # Execute batch
            response = self.session.post(
                f"{self.config['pi_web_api_url']}/piwebapi/batch",
                json=batch_payload,
                timeout=300
            )
            response.raise_for_status()

            # Parse results
            batch_results = response.json()['Responses']

            for j, result in enumerate(batch_results):
                if result['Status'] == 200:
                    tag_webid = batch_tags[j]
                    items = result['Content']['Items']

                    for item in items:
                        all_data.append({
                            'tag_webid': tag_webid,
                            'timestamp': item['Timestamp'],
                            'value': item.get('Value'),
                            'quality_good': item.get('Good', True),
                            'quality_questionable': item.get('Questionable', False),
                            'quality_substituted': item.get('Substituted', False),
                            'units': item.get('UnitsAbbreviation', ''),
                            'ingestion_timestamp': datetime.now().isoformat()
                        })
                else:
                    logger.warning(f"Tag {batch_tags[j]} failed: {result.get('Status')}")

        # Convert to Spark DataFrame
        import pandas as pd
        df_pandas = pd.DataFrame(all_data)
        df = self.spark.createDataFrame(df_pandas)

        logger.info(f"Extracted {len(all_data)} data points from {len(tag_webids)} tags")
        return df

    def extract_af_hierarchy(self, database_webid: str):
        """
        Extract PI Asset Framework hierarchy

        Args:
            database_webid: AF database WebId

        Returns:
            DataFrame with AF hierarchy
        """
        logger.info(f"Extracting AF hierarchy from database {database_webid}")

        return self.af_extractor.extract_hierarchy(database_webid)

    def extract_event_frames(
        self,
        database_webid: str,
        start_time: datetime,
        end_time: datetime
    ):
        """
        Extract Event Frames

        Args:
            database_webid: AF database WebId
            start_time: Start of time range
            end_time: End of time range

        Returns:
            DataFrame with event frames
        """
        logger.info(f"Extracting Event Frames from {start_time} to {end_time}")

        return self.ef_extractor.extract_event_frames(
            database_webid,
            start_time,
            end_time
        )

    def run(self):
        """
        Main execution flow for Lakeflow connector
        Called by Databricks job
        """
        logger.info("="*80)
        logger.info("OSI PI Lakeflow Connector - Starting")
        logger.info("="*80)

        try:
            # Step 1: Get tags to ingest
            tag_webids = self._get_tag_list()
            logger.info(f"Processing {len(tag_webids)} tags")

            # Step 2: Determine time range (incremental or full)
            end_time = datetime.now()
            start_time = self._get_last_checkpoint_time()
            logger.info(f"Time range: {start_time} to {end_time}")

            # Step 3: Extract time-series data
            df_timeseries = self.extract_timeseries(tag_webids, start_time, end_time)

            # Step 4: Write to Unity Catalog
            self.writer.write_timeseries(df_timeseries)

            # Step 5: Extract AF hierarchy (if configured)
            if self.config.get('af_database_id'):
                df_hierarchy = self.extract_af_hierarchy(self.config['af_database_id'])
                self.writer.write_af_hierarchy(df_hierarchy)

            # Step 6: Extract Event Frames (if configured)
            if self.config.get('include_event_frames') and self.config.get('af_database_id'):
                df_events = self.extract_event_frames(
                    self.config['af_database_id'],
                    start_time,
                    end_time
                )
                self.writer.write_event_frames(df_events)

            # Step 7: Update checkpoint
            self._update_checkpoint(end_time)

            logger.info("="*80)
            logger.info("OSI PI Lakeflow Connector - Completed Successfully")
            logger.info("="*80)

        except Exception as e:
            logger.error(f"Connector failed: {e}")
            raise

    def _get_tag_list(self) -> List[str]:
        """Get list of tag WebIds to ingest"""
        tags = self.config.get('tags', [])

        if isinstance(tags, list):
            return tags
        elif tags == 'all':
            # Query all points from PI Web API
            response = self.session.get(
                f"{self.config['pi_web_api_url']}/piwebapi/dataservers"
            )
            servers = response.json()['Items']
            server_webid = servers[0]['WebId']

            response = self.session.get(
                f"{self.config['pi_web_api_url']}/piwebapi/dataservers/{server_webid}/points",
                params={"maxCount": 10000}
            )
            points = response.json()['Items']
            return [point['WebId'] for point in points]
        else:
            raise ValueError(f"Invalid tags configuration: {tags}")

    def _get_last_checkpoint_time(self) -> datetime:
        """Get last checkpoint time for incremental load"""
        try:
            # Query checkpoint table
            checkpoint_table = f"{self.config['catalog']}.checkpoints.pi_watermarks"

            max_time = self.spark.sql(f"""
                SELECT MAX(last_timestamp) as max_time
                FROM {checkpoint_table}
            """).collect()[0].max_time

            if max_time:
                return max_time
            else:
                # No checkpoint, default to 30 days ago
                return datetime.now() - timedelta(days=30)

        except Exception:
            # Checkpoint table doesn't exist, full load
            return datetime.now() - timedelta(days=30)

    def _update_checkpoint(self, checkpoint_time: datetime):
        """Update checkpoint time"""
        logger.info(f"Updating checkpoint to {checkpoint_time}")

        checkpoint_table = f"{self.config['catalog']}.checkpoints.pi_watermarks"

        # Create checkpoint record
        checkpoint_data = [{
            'connector_id': 'pi_lakeflow_connector',
            'last_timestamp': checkpoint_time,
            'updated_at': datetime.now()
        }]

        df_checkpoint = self.spark.createDataFrame(checkpoint_data)

        # Merge into checkpoint table
        df_checkpoint.write.mode("append").saveAsTable(checkpoint_table)
        logger.info(f"✓ Checkpoint updated successfully")


# Main entry point for Databricks job
def main():
    """
    Main entry point called by Databricks job
    Configuration loaded from job parameters and secrets
    """
    try:
        from databricks.sdk.runtime import dbutils
    except ImportError:
        # If running outside Databricks, use environment variables
        import os
        config = {
            'pi_web_api_url': os.getenv('PI_WEB_API_URL'),
            'pi_auth_type': os.getenv('PI_AUTH_TYPE', 'basic'),
            'catalog': os.getenv('CATALOG', 'main'),
            'schema': os.getenv('SCHEMA', 'bronze'),
            'tags': os.getenv('TAGS', '').split(','),
            'af_database_id': os.getenv('AF_DATABASE_ID'),
            'include_event_frames': os.getenv('INCLUDE_EVENT_FRAMES', 'false') == 'true',
            'pi_username': os.getenv('PI_USERNAME'),
            'pi_password': os.getenv('PI_PASSWORD'),
            'oauth_token': os.getenv('OAUTH_TOKEN')
        }
    else:
        # Get configuration from job parameters
        config = {
            'pi_web_api_url': dbutils.widgets.get('pi_web_api_url'),
            'pi_auth_type': dbutils.widgets.get('pi_auth_type'),
            'catalog': dbutils.widgets.get('catalog'),
            'schema': dbutils.widgets.get('schema'),
            'tags': dbutils.widgets.get('tags').split(','),
            'af_database_id': dbutils.widgets.get('af_database_id'),
            'include_event_frames': dbutils.widgets.get('include_event_frames') == 'true'
        }

        # Get credentials from Databricks Secrets
        scope = "pi-connector"
        config['pi_username'] = dbutils.secrets.get(scope, 'pi_username')
        config['pi_password'] = dbutils.secrets.get(scope, 'pi_password')

        # OAuth token if needed
        if config['pi_auth_type'] == 'oauth':
            config['oauth_token'] = dbutils.secrets.get(scope, 'oauth_token')

    # Initialize and run connector
    connector = PILakeflowConnector(config)
    connector.run()


if __name__ == "__main__":
    main()
