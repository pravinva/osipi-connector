"""
Test suite for Enhanced Delta Writer

Tests proactive late data detection at ingestion time:
- Lateness metadata enrichment
- Clock skew detection
- Write-time quality metrics
- Stream-time watermarking
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.writers.enhanced_delta_writer import EnhancedDeltaWriter


class TestLatenessMetadataEnrichment:
    """Test lateness metadata enrichment at ingestion time"""

    def test_enrich_on_time_data(self):
        """Test enrichment of on-time data"""
        writer = EnhancedDeltaWriter()
        ingestion_time = datetime(2025, 1, 7, 12, 0, 0)

        records = [
            {
                'tag_webid': 'tag1',
                'timestamp': datetime(2025, 1, 7, 11, 55, 0),  # 5 minutes old
                'value': 123.45,
                'quality_good': True
            }
        ]

        enriched = writer.enrich_with_lateness_metadata(records, ingestion_time)

        assert len(enriched) == 1
        assert enriched[0]['late_arrival'] is False
        assert enriched[0]['lateness_category'] == 'on_time'
        assert enriched[0]['lateness_hours'] < 1
        assert enriched[0]['potential_clock_skew'] is False

    def test_enrich_slightly_late_data(self):
        """Test enrichment of slightly late data (4-24 hours)"""
        writer = EnhancedDeltaWriter()
        ingestion_time = datetime(2025, 1, 7, 12, 0, 0)

        records = [
            {
                'tag_webid': 'tag1',
                'timestamp': datetime(2025, 1, 7, 6, 0, 0),  # 6 hours old
                'value': 123.45,
                'quality_good': True
            }
        ]

        enriched = writer.enrich_with_lateness_metadata(records, ingestion_time)

        assert enriched[0]['late_arrival'] is True  # >4 hours
        assert enriched[0]['lateness_category'] == 'slightly_late'
        assert 5 < enriched[0]['lateness_hours'] < 7

    def test_enrich_late_data(self):
        """Test enrichment of late data (1-7 days)"""
        writer = EnhancedDeltaWriter()
        ingestion_time = datetime(2025, 1, 7, 12, 0, 0)

        records = [
            {
                'tag_webid': 'tag1',
                'timestamp': datetime(2025, 1, 5, 12, 0, 0),  # 2 days old
                'value': 123.45,
                'quality_good': True
            }
        ]

        enriched = writer.enrich_with_lateness_metadata(records, ingestion_time)

        assert enriched[0]['late_arrival'] is True
        assert enriched[0]['lateness_category'] == 'late'
        assert 47 < enriched[0]['lateness_hours'] < 49

    def test_enrich_very_late_data(self):
        """Test enrichment of very late data (>1 week)"""
        writer = EnhancedDeltaWriter()
        ingestion_time = datetime(2025, 1, 15, 12, 0, 0)

        records = [
            {
                'tag_webid': 'tag1',
                'timestamp': datetime(2025, 1, 1, 12, 0, 0),  # 2 weeks old
                'value': 123.45,
                'quality_good': True
            }
        ]

        enriched = writer.enrich_with_lateness_metadata(records, ingestion_time)

        assert enriched[0]['late_arrival'] is True
        assert enriched[0]['lateness_category'] == 'very_late'
        assert enriched[0]['lateness_hours'] > 168  # >7 days


class TestClockSkewDetection:
    """Test clock skew detection at write time"""

    def test_detect_future_timestamp(self):
        """Test detection of timestamps in the future (clock skew)"""
        writer = EnhancedDeltaWriter()
        ingestion_time = datetime(2025, 1, 7, 12, 0, 0)

        records = [
            {
                'tag_webid': 'tag1',
                'timestamp': datetime(2025, 1, 7, 12, 10, 0),  # 10 minutes future
                'value': 123.45,
                'quality_good': True
            }
        ]

        enriched = writer.enrich_with_lateness_metadata(records, ingestion_time)

        assert enriched[0]['potential_clock_skew'] is True
        assert enriched[0]['lateness_category'] == 'future_timestamp'
        assert enriched[0]['lateness_seconds'] < 0  # Negative = future

    def test_no_clock_skew_for_small_future_offset(self):
        """Test that small future offsets (<5 min) are not flagged"""
        writer = EnhancedDeltaWriter()
        ingestion_time = datetime(2025, 1, 7, 12, 0, 0)

        records = [
            {
                'tag_webid': 'tag1',
                'timestamp': datetime(2025, 1, 7, 12, 2, 0),  # 2 minutes future
                'value': 123.45,
                'quality_good': True
            }
        ]

        enriched = writer.enrich_with_lateness_metadata(records, ingestion_time)

        assert enriched[0]['potential_clock_skew'] is False  # <5 min threshold

    def test_detect_systematic_clock_skew(self):
        """Test detection when multiple records show clock skew"""
        writer = EnhancedDeltaWriter()
        ingestion_time = datetime(2025, 1, 7, 12, 0, 0)

        records = [
            {
                'tag_webid': 'tag1',
                'timestamp': datetime(2025, 1, 7, 12, 10, 0),  # 10 min future
                'value': 123.45,
                'quality_good': True
            },
            {
                'tag_webid': 'tag2',
                'timestamp': datetime(2025, 1, 7, 12, 15, 0),  # 15 min future
                'value': 678.90,
                'quality_good': True
            }
        ]

        enriched = writer.enrich_with_lateness_metadata(records, ingestion_time)

        skew_count = sum(1 for r in enriched if r['potential_clock_skew'])
        assert skew_count == 2


class TestWriteTimeQualityMetrics:
    """Test write-time quality metrics calculation"""

    def test_calculate_quality_metrics(self):
        """Test calculation of comprehensive quality metrics"""
        writer = EnhancedDeltaWriter()

        records = [
            {
                'tag_webid': 'tag1',
                'timestamp': datetime(2025, 1, 7, 6, 0, 0),
                'value': 123.45,
                'quality_good': True,
                'late_arrival': True,
                'lateness_hours': 6.0,
                'potential_clock_skew': False
            },
            {
                'tag_webid': 'tag2',
                'timestamp': datetime(2025, 1, 7, 11, 55, 0),
                'value': None,  # Null value
                'quality_good': False,
                'late_arrival': False,
                'lateness_hours': 0.0,
                'potential_clock_skew': False
            },
            {
                'tag_webid': 'tag3',
                'timestamp': datetime(2025, 1, 7, 12, 10, 0),
                'value': 999.99,
                'quality_good': True,
                'late_arrival': False,
                'lateness_hours': -0.17,
                'potential_clock_skew': True  # Clock skew
            }
        ]

        metrics = writer.calculate_write_time_quality_metrics(records)

        assert metrics['total_records'] == 3
        assert metrics['late_records'] == 1
        assert metrics['late_percentage'] == pytest.approx(33.33, rel=0.1)
        assert metrics['clock_skew_records'] == 1
        assert metrics['null_values'] == 1
        assert metrics['null_percentage'] == pytest.approx(33.33, rel=0.1)
        assert metrics['good_quality_records'] == 2
        assert metrics['good_quality_percentage'] == pytest.approx(66.66, rel=0.1)

    def test_quality_metrics_all_good(self):
        """Test metrics when all data is good quality"""
        writer = EnhancedDeltaWriter()

        records = [
            {
                'tag_webid': f'tag{i}',
                'timestamp': datetime(2025, 1, 7, 11, 55, 0),
                'value': float(i * 100),
                'quality_good': True,
                'late_arrival': False,
                'lateness_hours': 0.0,
                'potential_clock_skew': False
            }
            for i in range(10)
        ]

        metrics = writer.calculate_write_time_quality_metrics(records)

        assert metrics['late_percentage'] == 0.0
        assert metrics['null_percentage'] == 0.0
        assert metrics['good_quality_percentage'] == 100.0
        assert metrics['clock_skew_records'] == 0

    def test_quality_metrics_empty_records(self):
        """Test metrics calculation with empty record list"""
        writer = EnhancedDeltaWriter()

        metrics = writer.calculate_write_time_quality_metrics([])

        assert metrics == {}


class TestStringTimestampHandling:
    """Test handling of string timestamps"""

    def test_handle_iso_string_timestamp(self):
        """Test conversion of ISO format string timestamps"""
        writer = EnhancedDeltaWriter()
        ingestion_time = datetime(2025, 1, 7, 12, 0, 0)

        records = [
            {
                'tag_webid': 'tag1',
                'timestamp': '2025-01-07T11:55:00',  # ISO string
                'value': 123.45,
                'quality_good': True
            }
        ]

        enriched = writer.enrich_with_lateness_metadata(records, ingestion_time)

        assert 'lateness_hours' in enriched[0]
        assert enriched[0]['lateness_hours'] < 1

    def test_handle_iso_string_with_z(self):
        """Test conversion of ISO format with Z suffix"""
        writer = EnhancedDeltaWriter()
        ingestion_time = datetime(2025, 1, 7, 12, 0, 0)

        records = [
            {
                'tag_webid': 'tag1',
                'timestamp': '2025-01-07T11:55:00Z',  # ISO string with Z
                'value': 123.45,
                'quality_good': True
            }
        ]

        enriched = writer.enrich_with_lateness_metadata(records, ingestion_time)

        assert 'lateness_hours' in enriched[0]
        assert enriched[0]['late_arrival'] is False

    def test_skip_invalid_timestamp(self):
        """Test graceful handling of invalid timestamps"""
        writer = EnhancedDeltaWriter()
        ingestion_time = datetime(2025, 1, 7, 12, 0, 0)

        records = [
            {
                'tag_webid': 'tag1',
                'timestamp': None,  # Invalid
                'value': 123.45,
                'quality_good': True
            },
            {
                'tag_webid': 'tag2',
                'timestamp': 'not a date',  # Invalid
                'value': 678.90,
                'quality_good': True
            }
        ]

        enriched = writer.enrich_with_lateness_metadata(records, ingestion_time)

        # Records should still be returned but without lateness metadata
        assert len(enriched) == 2
        assert 'lateness_hours' not in enriched[0] or enriched[0].get('lateness_hours') is None


class TestThresholdConfiguration:
    """Test configurable thresholds"""

    def test_custom_late_data_threshold(self):
        """Test custom late data threshold"""
        writer = EnhancedDeltaWriter()
        writer.late_data_threshold_seconds = 2 * 3600  # 2 hours instead of 4

        ingestion_time = datetime(2025, 1, 7, 12, 0, 0)

        records = [
            {
                'tag_webid': 'tag1',
                'timestamp': datetime(2025, 1, 7, 9, 0, 0),  # 3 hours old
                'value': 123.45,
                'quality_good': True
            }
        ]

        enriched = writer.enrich_with_lateness_metadata(records, ingestion_time)

        assert enriched[0]['late_arrival'] is True  # >2 hour threshold

    def test_default_threshold_4_hours(self):
        """Test default 4-hour threshold"""
        writer = EnhancedDeltaWriter()
        assert writer.late_data_threshold_seconds == 4 * 3600


class TestBatchProcessing:
    """Test batch processing of large record sets"""

    def test_process_large_batch(self):
        """Test processing of large record batches"""
        writer = EnhancedDeltaWriter()
        ingestion_time = datetime(2025, 1, 7, 12, 0, 0)

        # Create 1000 records
        records = [
            {
                'tag_webid': f'tag{i}',
                'timestamp': datetime(2025, 1, 7, 11, i % 60, 0),
                'value': float(i),
                'quality_good': True
            }
            for i in range(1000)
        ]

        enriched = writer.enrich_with_lateness_metadata(records, ingestion_time)

        assert len(enriched) == 1000
        assert all('lateness_hours' in r for r in enriched)

    def test_mixed_quality_batch(self):
        """Test batch with mixed data quality"""
        writer = EnhancedDeltaWriter()
        ingestion_time = datetime(2025, 1, 7, 12, 0, 0)

        records = []
        for i in range(100):
            if i % 10 == 0:
                # Every 10th record is late
                ts = datetime(2025, 1, 7, 6, 0, 0)
            elif i % 15 == 0:
                # Every 15th has future timestamp
                ts = datetime(2025, 1, 7, 13, 0, 0)
            else:
                # Rest are on-time
                ts = datetime(2025, 1, 7, 11, 55, 0)

            records.append({
                'tag_webid': f'tag{i}',
                'timestamp': ts,
                'value': float(i),
                'quality_good': i % 5 != 0  # Every 5th has bad quality
            })

        enriched = writer.enrich_with_lateness_metadata(records, ingestion_time)
        metrics = writer.calculate_write_time_quality_metrics(enriched)

        assert metrics['total_records'] == 100
        assert metrics['late_records'] > 0
        assert metrics['clock_skew_records'] > 0
        assert metrics['good_quality_percentage'] < 100


class TestIngestionTimestampPreservation:
    """Test preservation of ingestion timestamp"""

    def test_adds_ingestion_timestamp(self):
        """Test that ingestion_timestamp is added to each record"""
        writer = EnhancedDeltaWriter()
        ingestion_time = datetime(2025, 1, 7, 12, 0, 0)

        records = [
            {
                'tag_webid': 'tag1',
                'timestamp': datetime(2025, 1, 7, 11, 55, 0),
                'value': 123.45,
                'quality_good': True
            }
        ]

        enriched = writer.enrich_with_lateness_metadata(records, ingestion_time)

        assert enriched[0]['ingestion_timestamp'] == ingestion_time

    def test_ingestion_timestamp_consistent(self):
        """Test that all records get same ingestion_timestamp"""
        writer = EnhancedDeltaWriter()
        ingestion_time = datetime(2025, 1, 7, 12, 0, 0)

        records = [
            {'tag_webid': f'tag{i}', 'timestamp': datetime(2025, 1, 7, 11, 55, 0), 'value': float(i), 'quality_good': True}
            for i in range(10)
        ]

        enriched = writer.enrich_with_lateness_metadata(records, ingestion_time)

        timestamps = [r['ingestion_timestamp'] for r in enriched]
        assert all(ts == ingestion_time for ts in timestamps)


class TestEdgeCases:
    """Test edge cases and boundary conditions"""

    def test_exactly_at_threshold(self):
        """Test data exactly at late threshold (4 hours)"""
        writer = EnhancedDeltaWriter()
        ingestion_time = datetime(2025, 1, 7, 12, 0, 0)

        records = [
            {
                'tag_webid': 'tag1',
                'timestamp': datetime(2025, 1, 7, 8, 0, 0),  # Exactly 4 hours
                'value': 123.45,
                'quality_good': True
            }
        ]

        enriched = writer.enrich_with_lateness_metadata(records, ingestion_time)

        # Should not be flagged as late (threshold is >4 hours, not >=)
        assert enriched[0]['late_arrival'] is False

    def test_one_second_over_threshold(self):
        """Test data 1 second over late threshold"""
        writer = EnhancedDeltaWriter()
        ingestion_time = datetime(2025, 1, 7, 12, 0, 0)

        records = [
            {
                'tag_webid': 'tag1',
                'timestamp': datetime(2025, 1, 7, 7, 59, 59),  # 4h 0m 1s old
                'value': 123.45,
                'quality_good': True
            }
        ]

        enriched = writer.enrich_with_lateness_metadata(records, ingestion_time)

        assert enriched[0]['late_arrival'] is True

    def test_midnight_boundary(self):
        """Test handling of midnight date boundary"""
        writer = EnhancedDeltaWriter()
        ingestion_time = datetime(2025, 1, 8, 0, 5, 0)  # 5 minutes after midnight

        records = [
            {
                'tag_webid': 'tag1',
                'timestamp': datetime(2025, 1, 7, 23, 55, 0),  # 10 minutes before midnight
                'value': 123.45,
                'quality_good': True
            }
        ]

        enriched = writer.enrich_with_lateness_metadata(records, ingestion_time)

        assert enriched[0]['late_arrival'] is False  # Only 10 minutes old
        assert enriched[0]['lateness_hours'] < 1


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
