"""
PI WebSocket Streaming Connector - Module 6

Real-time streaming connector that subscribes to PI Web API WebSocket channel
and writes to Delta Lake in near-real-time with micro-batch processing.

Features:
- WebSocket connection to PI Web API
- Real-time tag subscriptions
- Automatic buffering and flushing to Delta Lake
- Graceful shutdown with buffer flush
- Monitoring and statistics
- Works from laptop (uses Databricks CLI config)

Usage:
    connector = PIStreamingConnector(config)
    await connector.run()
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from pyspark.sql import SparkSession

from src.streaming.websocket_client import PIWebSocketClient
from src.writers.streaming_delta_writer import StreamingDeltaWriter, StreamingBuffer
from src.auth.pi_auth_manager import PIAuthManager

logger = logging.getLogger(__name__)


class PIStreamingConnector:
    """
    Main orchestrator for WebSocket streaming from PI to Delta Lake.

    Coordinates:
    - WebSocket client for real-time data
    - Delta Lake writer for storage
    - Buffer management for micro-batches
    - Health monitoring and stats
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize streaming connector.

        Args:
            config: Configuration dictionary with keys:
                - pi_web_api_url: str (PI Web API base URL)
                - auth: Dict (authentication config for PIAuthManager)
                - catalog: str (Unity Catalog name, default 'main')
                - schema: str (schema name, default 'bronze')
                - table_name: str (table name, default 'pi_streaming_timeseries')
                - tags: List[str] (tag WebIDs to subscribe to)
                - flush_interval_seconds: int (buffer flush interval, default 60)
                - max_buffer_size: int (max records before flush, default 10000)
                - warehouse_id: str (Databricks warehouse, default '4b9b953939869799')

        Example config:
            {
                'pi_web_api_url': 'https://pi-server.com/piwebapi',
                'auth': {
                    'type': 'basic',
                    'username': 'piuser',
                    'password': 'pipassword'
                },
                'catalog': 'main',
                'schema': 'bronze',
                'table_name': 'pi_streaming_timeseries',
                'tags': ['F1DP-TAG-001', 'F1DP-TAG-002'],
                'flush_interval_seconds': 60,
                'max_buffer_size': 10000,
                'warehouse_id': '4b9b953939869799'
            }
        """
        self.config = config
        self.is_running = False
        self.start_time: Optional[datetime] = None

        logger.info("Initializing PI Streaming Connector...")

        # Initialize Spark session (uses Databricks CLI config)
        logger.info("Initializing Spark session with Databricks CLI config...")
        self.spark = SparkSession.builder \
            .appName("PI WebSocket Streaming Connector") \
            .config("spark.databricks.service.server.enabled", "true") \
            .getOrCreate()

        logger.info(f"✅ Spark session initialized (Warehouse: {config.get('warehouse_id', '4b9b953939869799')})")

        # Initialize authentication
        auth_manager = PIAuthManager(
            auth_type=config['auth']['type'],
            config=config['auth']
        )

        # Initialize WebSocket client
        self.ws_client = PIWebSocketClient(
            base_url=config['pi_web_api_url'],
            auth_token=config['auth'].get('token'),
            username=config['auth'].get('username'),
            password=config['auth'].get('password')
        )

        # Initialize Delta Lake writer
        self.delta_writer = StreamingDeltaWriter(
            spark=self.spark,
            catalog=config.get('catalog', 'main'),
            schema=config.get('schema', 'bronze'),
            table_name=config.get('table_name', 'pi_streaming_timeseries')
        )

        # Initialize streaming buffer
        self.buffer = StreamingBuffer(
            writer=self.delta_writer,
            flush_interval_seconds=config.get('flush_interval_seconds', 60),
            max_buffer_size=config.get('max_buffer_size', 10000)
        )

        # Tags to subscribe to
        self.tags = config.get('tags', [])

        logger.info(f"✅ Streaming connector initialized")
        logger.info(f"   Target table: {self.delta_writer.full_table_name}")
        logger.info(f"   Tags to monitor: {len(self.tags)}")
        logger.info(f"   Flush interval: {config.get('flush_interval_seconds', 60)}s")
        logger.info(f"   Buffer size: {config.get('max_buffer_size', 10000)} records")

    async def run(self):
        """
        Start streaming connector (main entry point).

        This method:
        1. Connects to PI Web API WebSocket
        2. Subscribes to configured tags
        3. Streams data to Delta Lake with buffering
        4. Handles graceful shutdown
        """
        self.start_time = datetime.now()
        self.is_running = True

        logger.info("=" * 70)
        logger.info("🚀 Starting PI WebSocket Streaming Connector")
        logger.info("=" * 70)

        try:
            # Connect to WebSocket
            logger.info("📡 Connecting to PI Web API WebSocket...")
            connected = await self.ws_client.connect()

            if not connected:
                raise ConnectionError("Failed to establish WebSocket connection")

            logger.info("✅ WebSocket connection established")

            # Subscribe to tags
            logger.info(f"📋 Subscribing to {len(self.tags)} tags...")
            await self.ws_client.subscribe_to_multiple_tags(
                self.tags,
                self._handle_streaming_data
            )

            logger.info("✅ Tag subscriptions active")
            logger.info("")
            logger.info(f"🎯 Streaming to: {self.delta_writer.full_table_name}")
            logger.info(f"⚙️  Databricks Warehouse: {self.config.get('warehouse_id', '4b9b953939869799')}")
            logger.info("")
            logger.info("📊 Monitoring real-time PI data... (Press Ctrl+C to stop)")
            logger.info("-" * 70)

            # Listen for streaming data (blocks until connection closed)
            await self.ws_client.listen()

        except KeyboardInterrupt:
            logger.info("")
            logger.info("⚠️  Keyboard interrupt detected, shutting down gracefully...")
            await self._graceful_shutdown()

        except Exception as e:
            logger.error(f"❌ Streaming connector error: {e}")
            await self._graceful_shutdown()
            raise

        finally:
            self.is_running = False
            logger.info("✅ Streaming connector stopped")

    async def _graceful_shutdown(self):
        """Gracefully shutdown connector with buffer flush."""
        logger.info("🛑 Initiating graceful shutdown...")

        # Flush remaining buffer
        if self.buffer.buffer:
            logger.info(f"📤 Flushing remaining {len(self.buffer.buffer)} records...")
            try:
                flushed = self.buffer.force_flush()
                logger.info(f"✅ Flushed {flushed} final records to Delta Lake")
            except Exception as e:
                logger.error(f"⚠️  Final flush failed: {e}")

        # Disconnect WebSocket
        logger.info("🔌 Closing WebSocket connection...")
        await self.ws_client.disconnect()

        # Print final statistics
        self._print_final_stats()

    def _handle_streaming_data(self, tag_webid: str, data: Dict[str, Any]):
        """
        Callback for handling incoming WebSocket data.

        Args:
            tag_webid: Tag WebID
            data: Streaming data record with fields:
                - timestamp: str (ISO format)
                - value: float
                - good: bool
                - questionable: bool
                - substituted: bool
                - uom: str
                - received_timestamp: str (ISO format)
        """
        # Log receipt (at debug level to avoid spam)
        logger.debug(
            f"📥 {tag_webid}: {data['value']} {data['uom']} "
            f"at {data['timestamp']} (quality: {'Good' if data['good'] else 'Bad'})"
        )

        # Add to buffer
        self.buffer.add_record(tag_webid, data)

        # Auto-flush if thresholds met
        if self.buffer.should_flush():
            try:
                records_written = self.buffer.flush()
                logger.info(
                    f"✅ Flushed {records_written} records to Delta Lake "
                    f"(buffer: {len(self.buffer.buffer)} records)"
                )

                # Show buffer stats every 10 flushes
                if self.buffer.total_flushes % 10 == 0:
                    stats = self.buffer.get_stats()
                    logger.info(
                        f"📊 Stats: {stats['total_records_written']} total records, "
                        f"{stats['avg_records_per_flush']:.0f} avg/flush"
                    )

            except Exception as e:
                logger.error(f"❌ Flush failed: {e}")
                # Buffer remains intact for retry

    def _print_final_stats(self):
        """Print final statistics on shutdown."""
        logger.info("")
        logger.info("=" * 70)
        logger.info("📊 Final Statistics")
        logger.info("=" * 70)

        # Buffer stats
        buffer_stats = self.buffer.get_stats()
        logger.info(f"📦 Buffer Statistics:")
        logger.info(f"   Total records written: {buffer_stats['total_records_written']}")
        logger.info(f"   Total flushes: {buffer_stats['total_flushes']}")
        logger.info(f"   Avg records per flush: {buffer_stats['avg_records_per_flush']:.1f}")

        # Table stats
        try:
            table_stats = self.delta_writer.get_table_stats()
            logger.info(f"")
            logger.info(f"📊 Delta Table Statistics:")
            logger.info(f"   Table: {self.delta_writer.full_table_name}")
            logger.info(f"   Total records: {table_stats.get('total_records', 'N/A')}")
            logger.info(f"   Unique tags: {table_stats.get('unique_tags', 'N/A')}")
            logger.info(f"   Data quality: {table_stats.get('quality_pct', 0):.1f}%")
            logger.info(f"   Latest data: {table_stats.get('latest_timestamp', 'N/A')}")
        except Exception as e:
            logger.warning(f"Could not retrieve table stats: {e}")

        # Runtime stats
        if self.start_time:
            runtime = datetime.now() - self.start_time
            logger.info(f"")
            logger.info(f"⏱️  Runtime: {runtime}")

        logger.info("=" * 70)

    def get_status(self) -> Dict[str, Any]:
        """
        Get current status of streaming connector.

        Returns:
            Status dictionary with monitoring information
        """
        buffer_stats = self.buffer.get_stats()

        status = {
            'is_running': self.is_running,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'runtime_seconds': (
                (datetime.now() - self.start_time).total_seconds()
                if self.start_time else 0
            ),
            'websocket_connected': self.ws_client.is_connected,
            'subscribed_tags': len(self.tags),
            'buffer_size': buffer_stats['buffer_size'],
            'total_records_written': buffer_stats['total_records_written'],
            'total_flushes': buffer_stats['total_flushes'],
            'target_table': self.delta_writer.full_table_name
        }

        return status


# ============================================================================
# Example Usage
# ============================================================================

async def main():
    """
    Example of running the streaming connector from laptop.

    Requirements:
    - Databricks CLI configured (~/.databrickscfg)
    - PI Web API with WebSocket support
    - List of tag WebIDs to monitor
    """

    config = {
        # PI Web API connection
        'pi_web_api_url': 'https://pi-server.com/piwebapi',

        # Authentication
        'auth': {
            'type': 'basic',
            'username': 'piuser',
            'password': 'pipassword'
        },

        # Delta Lake target (uses real Databricks, never mock)
        'catalog': 'main',
        'schema': 'bronze',
        'table_name': 'pi_streaming_timeseries',

        # Tags to monitor (replace with actual WebIDs)
        'tags': [
            'F1DP-TAG-001',
            'F1DP-TAG-002',
            'F1DP-TAG-003'
        ],

        # Buffering config
        'flush_interval_seconds': 60,    # Flush every 60 seconds
        'max_buffer_size': 10000,        # Or when 10K records accumulated

        # Databricks warehouse (uses CLI config)
        'warehouse_id': '4b9b953939869799'
    }

    # Initialize and run connector
    connector = PIStreamingConnector(config)
    await connector.run()


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Run async main
    asyncio.run(main())
