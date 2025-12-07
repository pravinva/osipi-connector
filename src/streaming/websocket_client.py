"""
PI Web API WebSocket Streaming Client

Provides real-time data streaming via WebSocket channels.
Supports streaming recorded data, updates, and event frames.

Note: PI Web API WebSocket support requires PI Web API 2019+
"""

import asyncio
import json
import logging
from typing import List, Dict, Callable, Optional, Any
from datetime import datetime
import websockets
from websockets.client import WebSocketClientProtocol

logger = logging.getLogger(__name__)


class PIWebSocketClient:
    """
    WebSocket client for real-time PI data streaming.

    Establishes persistent WebSocket connection to PI Web API
    and streams updates for subscribed tags.
    """

    def __init__(
        self,
        base_url: str,
        auth_token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None
    ):
        """
        Initialize WebSocket client.

        Args:
            base_url: PI Web API base URL (e.g., https://pi-server.com/piwebapi)
            auth_token: OAuth bearer token (optional)
            username: Basic auth username (optional)
            password: Basic auth password (optional)
        """
        # Convert HTTPS URL to WSS URL
        self.ws_url = base_url.replace('https://', 'wss://').replace('http://', 'ws://')
        self.auth_token = auth_token
        self.username = username
        self.password = password
        self.websocket: Optional[WebSocketClientProtocol] = None
        self.subscriptions: Dict[str, List[Callable]] = {}
        self.is_connected = False

    async def connect(self) -> bool:
        """
        Establish WebSocket connection to PI Web API.

        Returns:
            True if connection successful
        """
        try:
            headers = {}

            # Add authentication headers
            if self.auth_token:
                headers['Authorization'] = f'Bearer {self.auth_token}'
            elif self.username and self.password:
                import base64
                credentials = base64.b64encode(
                    f'{self.username}:{self.password}'.encode()
                ).decode()
                headers['Authorization'] = f'Basic {credentials}'

            # Connect to WebSocket endpoint
            endpoint = f"{self.ws_url}/streams/channel"
            logger.info(f"Connecting to WebSocket: {endpoint}")

            self.websocket = await websockets.connect(
                endpoint,
                extra_headers=headers,
                ping_interval=30,
                ping_timeout=10
            )

            self.is_connected = True
            logger.info("WebSocket connection established")
            return True

        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")
            self.is_connected = False
            return False

    async def disconnect(self):
        """Close WebSocket connection."""
        if self.websocket:
            await self.websocket.close()
            self.is_connected = False
            logger.info("WebSocket connection closed")

    async def subscribe_to_tag(
        self,
        tag_webid: str,
        callback: Callable[[Dict[str, Any]], None]
    ):
        """
        Subscribe to real-time updates for a tag.

        Args:
            tag_webid: PI tag WebID to subscribe to
            callback: Function called when new data arrives
        """
        if not self.is_connected:
            raise ConnectionError("WebSocket not connected. Call connect() first.")

        logger.info(f"Subscribing to tag: {tag_webid}")

        # Register callback
        if tag_webid not in self.subscriptions:
            self.subscriptions[tag_webid] = []
        self.subscriptions[tag_webid].append(callback)

        # Send subscription message
        subscription_message = {
            'Action': 'Subscribe',
            'Resource': f'streams/{tag_webid}/value',
            'Parameters': {
                'updateRate': 1000  # Update every 1 second
            }
        }

        await self.websocket.send(json.dumps(subscription_message))
        logger.info(f"Subscription request sent for {tag_webid}")

    async def subscribe_to_multiple_tags(
        self,
        tag_webids: List[str],
        callback: Callable[[str, Dict[str, Any]], None]
    ):
        """
        Subscribe to multiple tags with a single callback.

        Args:
            tag_webids: List of tag WebIDs
            callback: Function called with (tag_webid, data) when data arrives
        """
        logger.info(f"Subscribing to {len(tag_webids)} tags")

        for tag_webid in tag_webids:
            # Wrap callback to include tag_webid
            tag_callback = lambda data, tid=tag_webid: callback(tid, data)
            await self.subscribe_to_tag(tag_webid, tag_callback)

    async def unsubscribe_from_tag(self, tag_webid: str):
        """
        Unsubscribe from tag updates.

        Args:
            tag_webid: Tag WebID to unsubscribe from
        """
        if not self.is_connected:
            return

        logger.info(f"Unsubscribing from tag: {tag_webid}")

        # Send unsubscribe message
        unsubscribe_message = {
            'Action': 'Unsubscribe',
            'Resource': f'streams/{tag_webid}/value'
        }

        await self.websocket.send(json.dumps(unsubscribe_message))

        # Remove callbacks
        if tag_webid in self.subscriptions:
            del self.subscriptions[tag_webid]

    async def listen(self):
        """
        Listen for incoming WebSocket messages and dispatch to callbacks.

        This method blocks and should be run in an async loop.
        """
        if not self.is_connected:
            raise ConnectionError("WebSocket not connected")

        logger.info("Starting WebSocket listener")

        try:
            async for message in self.websocket:
                await self._handle_message(message)

        except websockets.exceptions.ConnectionClosed:
            logger.warning("WebSocket connection closed")
            self.is_connected = False

        except Exception as e:
            logger.error(f"WebSocket listener error: {e}")
            self.is_connected = False

    async def _handle_message(self, message: str):
        """
        Parse and dispatch incoming WebSocket message.

        Args:
            message: Raw JSON message from WebSocket
        """
        try:
            data = json.loads(message)

            # Extract tag WebID from resource path
            resource = data.get('Resource', '')
            if '/streams/' in resource:
                tag_webid = resource.split('/streams/')[1].split('/')[0]

                # Get value data
                items = data.get('Items', [])
                if items:
                    value_data = items[0]

                    # Parse into standard format
                    parsed_data = {
                        'timestamp': value_data.get('Timestamp'),
                        'value': value_data.get('Value'),
                        'good': value_data.get('Good', True),
                        'questionable': value_data.get('Questionable', False),
                        'substituted': value_data.get('Substituted', False),
                        'uom': value_data.get('UnitsAbbreviation', ''),
                        'received_timestamp': datetime.now().isoformat()
                    }

                    # Dispatch to callbacks
                    if tag_webid in self.subscriptions:
                        for callback in self.subscriptions[tag_webid]:
                            try:
                                callback(parsed_data)
                            except Exception as cb_error:
                                logger.error(f"Callback error: {cb_error}")

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse WebSocket message: {e}")

        except Exception as e:
            logger.error(f"Error handling WebSocket message: {e}")


# StreamingBuffer moved to src/writers/streaming_delta_writer.py
# Import it from there for integration with Delta Lake


# Example usage with REAL Databricks connection
async def example_streaming_usage():
    """
    Example of WebSocket streaming with Delta Lake integration.

    Uses real Databricks configuration (never mock data):
    - Databricks CLI config (~/.databrickscfg) for authentication
    - Warehouse ID: 4b9b953939869799
    - Works from laptop or Databricks workspace
    """
    from pyspark.sql import SparkSession
    from src.writers.streaming_delta_writer import StreamingDeltaWriter, StreamingBuffer

    # Initialize Spark session (uses Databricks CLI config automatically)
    spark = SparkSession.builder \
        .appName("PI WebSocket Streaming") \
        .config("spark.databricks.service.server.enabled", "true") \
        .getOrCreate()

    # Initialize WebSocket client
    client = PIWebSocketClient(
        base_url='https://pi-server.com/piwebapi',
        username='user',
        password='pass'
    )

    # Connect to WebSocket
    await client.connect()

    # Initialize Delta Lake writer (uses REAL Databricks, no mock data)
    delta_writer = StreamingDeltaWriter(
        spark=spark,
        catalog='main',
        schema='bronze',
        table_name='pi_streaming_timeseries'
    )

    # Initialize buffer with Delta Lake writer
    buffer = StreamingBuffer(
        writer=delta_writer,
        flush_interval_seconds=60,  # Flush every 60 seconds
        max_buffer_size=10000       # Or when 10K records accumulated
    )

    # Define callback for handling streaming data
    def handle_data(tag_webid: str, data: Dict[str, Any]):
        print(f"Received: {tag_webid} = {data['value']} at {data['timestamp']}")

        # Add to buffer
        buffer.add_record(tag_webid, data)

        # Auto-flush if thresholds met
        if buffer.should_flush():
            records_written = buffer.flush()
            print(f"✅ Flushed {records_written} records to {delta_writer.full_table_name}")

    # Subscribe to tags
    tags = ['F1DP-TAG-001', 'F1DP-TAG-002', 'F1DP-TAG-003']
    await client.subscribe_to_multiple_tags(tags, handle_data)

    print(f"📡 Subscribed to {len(tags)} tags, streaming to Delta Lake...")
    print(f"🎯 Target table: {delta_writer.full_table_name}")
    print(f"⚙️  Warehouse: 4b9b953939869799 (from Databricks CLI config)")

    # Listen for updates (blocks until connection closed)
    try:
        await client.listen()
    except KeyboardInterrupt:
        print("\n⚠️  Shutting down gracefully...")
        # Force flush remaining buffer before exit
        if buffer.buffer:
            remaining = buffer.force_flush()
            print(f"✅ Flushed final {remaining} records")

    # Cleanup
    await client.disconnect()

    # Print final stats
    stats = buffer.get_stats()
    print(f"\n📊 Final Stats:")
    print(f"   Total records written: {stats['total_records_written']}")
    print(f"   Total flushes: {stats['total_flushes']}")
    print(f"   Avg records/flush: {stats['avg_records_per_flush']:.1f}")
