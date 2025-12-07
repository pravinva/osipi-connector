"""
Tests for Module 6: WebSocket Streaming Integration

Tests the complete streaming pipeline:
- StreamingDeltaWriter
- StreamingBuffer
- WebSocket integration with Delta Lake
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType
import pandas as pd

from src.writers.streaming_delta_writer import StreamingDeltaWriter, StreamingBuffer


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def mock_spark():
    """Mock Spark session for testing."""
    spark = MagicMock(spec=SparkSession)

    # Mock SQL commands
    spark.sql = Mock(return_value=None)

    # Mock createDataFrame
    mock_df = MagicMock()
    mock_df.write.format.return_value.mode.return_value.option.return_value.saveAsTable = Mock()
    spark.createDataFrame = Mock(return_value=mock_df)

    return spark


@pytest.fixture
def streaming_writer(mock_spark):
    """Create StreamingDeltaWriter instance with mocked Spark."""
    writer = StreamingDeltaWriter(
        spark=mock_spark,
        catalog='test_catalog',
        schema='test_schema',
        table_name='test_streaming_table'
    )
    return writer


@pytest.fixture
def sample_streaming_records():
    """Generate sample streaming data records."""
    base_time = datetime.now()

    return [
        {
            'tag_webid': 'F1DP-TAG-001',
            'timestamp': (base_time - timedelta(seconds=i)).isoformat(),
            'value': 42.5 + i * 0.1,
            'good': True,
            'questionable': False,
            'substituted': False,
            'uom': 'degC',
            'received_timestamp': base_time.isoformat()
        }
        for i in range(10)
    ]


# ============================================================================
# StreamingDeltaWriter Tests
# ============================================================================

def test_streaming_writer_initialization(streaming_writer, mock_spark):
    """Test StreamingDeltaWriter initialization."""
    assert streaming_writer.catalog == 'test_catalog'
    assert streaming_writer.schema == 'test_schema'
    assert streaming_writer.table_name == 'test_streaming_table'
    assert streaming_writer.full_table_name == 'test_catalog.test_schema.test_streaming_table'
    assert streaming_writer.spark == mock_spark

    # Verify catalog/schema creation was called
    assert mock_spark.sql.call_count >= 2  # CREATE CATALOG + CREATE SCHEMA + CREATE TABLE


def test_write_batch_success(streaming_writer, sample_streaming_records):
    """Test writing a batch of streaming records."""
    records_written = streaming_writer.write_batch(sample_streaming_records, deduplicate=False)

    assert records_written == 10
    assert streaming_writer.spark.createDataFrame.called


def test_write_batch_empty_records(streaming_writer):
    """Test writing empty batch returns 0."""
    records_written = streaming_writer.write_batch([], deduplicate=False)

    assert records_written == 0
    assert not streaming_writer.spark.createDataFrame.called


def test_write_batch_with_deduplication(streaming_writer):
    """Test batch write with deduplication."""
    # Create records with duplicates
    records = [
        {
            'tag_webid': 'F1DP-TAG-001',
            'timestamp': '2024-12-07T10:00:00',
            'value': 42.5,
            'good': True,
            'questionable': False,
            'substituted': False,
            'uom': 'degC',
            'received_timestamp': '2024-12-07T10:00:01'
        },
        {
            'tag_webid': 'F1DP-TAG-001',
            'timestamp': '2024-12-07T10:00:00',  # Duplicate timestamp
            'value': 43.0,  # Different value (keep this one)
            'good': True,
            'questionable': False,
            'substituted': False,
            'uom': 'degC',
            'received_timestamp': '2024-12-07T10:00:02'
        },
        {
            'tag_webid': 'F1DP-TAG-002',
            'timestamp': '2024-12-07T10:00:00',
            'value': 50.0,
            'good': True,
            'questionable': False,
            'substituted': False,
            'uom': 'bar',
            'received_timestamp': '2024-12-07T10:00:01'
        }
    ]

    records_written = streaming_writer.write_batch(records, deduplicate=True)

    # Should write 2 records after deduplication
    assert records_written == 2


def test_write_single_record(streaming_writer, sample_streaming_records):
    """Test writing single record."""
    streaming_writer.write_single_record(sample_streaming_records[0])

    assert streaming_writer.spark.createDataFrame.called


def test_optimize_table(streaming_writer, mock_spark):
    """Test table optimization."""
    streaming_writer.optimize_table()

    # Verify OPTIMIZE command was called
    mock_spark.sql.assert_any_call(
        f"OPTIMIZE {streaming_writer.full_table_name} ZORDER BY (tag_webid, timestamp)"
    )


def test_get_table_stats(streaming_writer, mock_spark):
    """Test retrieving table statistics."""
    # Mock query result
    mock_result_row = Mock()
    mock_result_row.__getitem__ = lambda self, key: {
        'total_records': 1000,
        'unique_tags': 10,
        'earliest_timestamp': datetime(2024, 12, 1),
        'latest_timestamp': datetime(2024, 12, 7),
        'last_write': datetime(2024, 12, 7, 10, 30),
        'quality_pct': 98.5
    }[key]

    mock_spark.sql.return_value.collect.return_value = [mock_result_row]

    stats = streaming_writer.get_table_stats()

    assert stats['total_records'] == 1000
    assert stats['unique_tags'] == 10
    assert stats['quality_pct'] == 98.5


def test_vacuum_old_data(streaming_writer, mock_spark):
    """Test vacuuming old data."""
    streaming_writer.vacuum_old_data(retention_hours=24)

    # Verify VACUUM command was called
    mock_spark.sql.assert_any_call(
        f"VACUUM {streaming_writer.full_table_name} RETAIN 24 HOURS"
    )


# ============================================================================
# StreamingBuffer Tests
# ============================================================================

def test_streaming_buffer_initialization(streaming_writer):
    """Test StreamingBuffer initialization."""
    buffer = StreamingBuffer(
        writer=streaming_writer,
        flush_interval_seconds=30,
        max_buffer_size=5000
    )

    assert buffer.writer == streaming_writer
    assert buffer.flush_interval == 30
    assert buffer.max_buffer_size == 5000
    assert len(buffer.buffer) == 0
    assert buffer.total_records_written == 0
    assert buffer.total_flushes == 0


def test_buffer_add_record(streaming_writer):
    """Test adding records to buffer."""
    buffer = StreamingBuffer(writer=streaming_writer)

    data = {
        'timestamp': '2024-12-07T10:00:00',
        'value': 42.5,
        'good': True,
        'questionable': False,
        'substituted': False,
        'uom': 'degC',
        'received_timestamp': '2024-12-07T10:00:01'
    }

    buffer.add_record('F1DP-TAG-001', data)

    assert len(buffer.buffer) == 1
    assert buffer.buffer[0]['tag_webid'] == 'F1DP-TAG-001'
    assert buffer.buffer[0]['value'] == 42.5


def test_buffer_should_flush_size_threshold(streaming_writer):
    """Test buffer flush on size threshold."""
    buffer = StreamingBuffer(
        writer=streaming_writer,
        flush_interval_seconds=1000,  # Long interval
        max_buffer_size=5
    )

    # Add records up to threshold
    data = {
        'timestamp': '2024-12-07T10:00:00',
        'value': 42.5,
        'good': True,
        'questionable': False,
        'substituted': False,
        'uom': 'degC',
        'received_timestamp': '2024-12-07T10:00:01'
    }

    for i in range(4):
        buffer.add_record(f'F1DP-TAG-{i:03d}', data)

    assert not buffer.should_flush()  # Not yet

    buffer.add_record('F1DP-TAG-005', data)
    assert buffer.should_flush()  # Now should flush


def test_buffer_should_flush_time_threshold(streaming_writer):
    """Test buffer flush on time threshold."""
    buffer = StreamingBuffer(
        writer=streaming_writer,
        flush_interval_seconds=1,  # 1 second
        max_buffer_size=10000
    )

    # Add one record
    data = {
        'timestamp': '2024-12-07T10:00:00',
        'value': 42.5,
        'good': True,
        'questionable': False,
        'substituted': False,
        'uom': 'degC',
        'received_timestamp': '2024-12-07T10:00:01'
    }

    buffer.add_record('F1DP-TAG-001', data)

    # Immediately check - should not flush
    assert not buffer.should_flush()

    # Manually set last_flush to past
    buffer.last_flush = datetime.now() - timedelta(seconds=2)

    # Now should flush due to time threshold
    assert buffer.should_flush()


def test_buffer_flush_success(streaming_writer, sample_streaming_records):
    """Test successful buffer flush."""
    buffer = StreamingBuffer(writer=streaming_writer)

    # Add records
    for record in sample_streaming_records:
        buffer.add_record(record['tag_webid'], {k: v for k, v in record.items() if k != 'tag_webid'})

    assert len(buffer.buffer) == 10

    # Flush
    records_written = buffer.flush()

    assert records_written == 10
    assert len(buffer.buffer) == 0  # Buffer cleared
    assert buffer.total_records_written == 10
    assert buffer.total_flushes == 1


def test_buffer_flush_empty(streaming_writer):
    """Test flushing empty buffer."""
    buffer = StreamingBuffer(writer=streaming_writer)

    records_written = buffer.flush()

    assert records_written == 0
    assert buffer.total_flushes == 0


def test_buffer_get_stats(streaming_writer, sample_streaming_records):
    """Test buffer statistics."""
    buffer = StreamingBuffer(writer=streaming_writer)

    # Add and flush records twice
    for record in sample_streaming_records:
        buffer.add_record(record['tag_webid'], {k: v for k, v in record.items() if k != 'tag_webid'})

    buffer.flush()

    for record in sample_streaming_records[:5]:
        buffer.add_record(record['tag_webid'], {k: v for k, v in record.items() if k != 'tag_webid'})

    buffer.flush()

    stats = buffer.get_stats()

    assert stats['total_records_written'] == 15  # 10 + 5
    assert stats['total_flushes'] == 2
    assert stats['avg_records_per_flush'] == 7.5  # 15 / 2


def test_buffer_force_flush(streaming_writer, sample_streaming_records):
    """Test force flush (for graceful shutdown)."""
    buffer = StreamingBuffer(
        writer=streaming_writer,
        flush_interval_seconds=10000,  # Very long interval
        max_buffer_size=10000
    )

    # Add a few records
    for record in sample_streaming_records[:3]:
        buffer.add_record(record['tag_webid'], {k: v for k, v in record.items() if k != 'tag_webid'})

    # Should not normally flush
    assert not buffer.should_flush()

    # Force flush
    records_written = buffer.force_flush()

    assert records_written == 3
    assert len(buffer.buffer) == 0


# ============================================================================
# Integration Tests
# ============================================================================

def test_end_to_end_streaming_flow(streaming_writer):
    """Test complete streaming flow: buffer → flush → Delta Lake."""
    buffer = StreamingBuffer(
        writer=streaming_writer,
        flush_interval_seconds=60,
        max_buffer_size=5
    )

    # Simulate streaming data arriving
    base_time = datetime.now()

    for i in range(12):  # More than buffer size
        data = {
            'timestamp': (base_time - timedelta(seconds=i)).isoformat(),
            'value': 42.5 + i * 0.1,
            'good': True,
            'questionable': False,
            'substituted': False,
            'uom': 'degC',
            'received_timestamp': base_time.isoformat()
        }

        buffer.add_record(f'F1DP-TAG-{i % 3:03d}', data)

        # Auto-flush when threshold met
        if buffer.should_flush():
            buffer.flush()

    # Final flush
    if buffer.buffer:
        buffer.force_flush()

    # Verify all records were written
    assert buffer.total_records_written == 12
    assert buffer.total_flushes >= 2  # Should have flushed at least twice


# ============================================================================
# Summary
# ============================================================================

def test_summary():
    """
    Module 6 Test Summary:

    ✅ StreamingDeltaWriter Tests (8 tests):
       - Initialization
       - Write batch (success, empty, deduplication)
       - Write single record
       - Table optimization
       - Table statistics
       - Vacuum old data

    ✅ StreamingBuffer Tests (8 tests):
       - Initialization
       - Add records
       - Flush thresholds (size, time)
       - Flush operations (success, empty, force)
       - Buffer statistics

    ✅ Integration Tests (1 test):
       - End-to-end streaming flow

    Total: 17 tests
    Coverage: StreamingDeltaWriter + StreamingBuffer classes

    Note: WebSocket client tests are in test_websocket.py
    """
    pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
