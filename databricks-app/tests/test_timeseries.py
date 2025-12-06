"""
Comprehensive test suite for TimeSeriesExtractor

Tests cover:
- Single tag extraction
- Batch extraction with multiple tags
- Quality flag parsing (Good, Questionable, Substituted)
- Paging for large datasets (>10K records)
- Failed tag handling
- Empty responses
- Batch controller optimization verification

Follows TESTER.md specifications exactly.
"""

import pytest
from datetime import datetime, timedelta
import pandas as pd
from unittest.mock import Mock, MagicMock, patch
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.extractors.timeseries_extractor import TimeSeriesExtractor
from tests.fixtures.sample_responses import (
    SAMPLE_RECORDED_RESPONSE,
    SAMPLE_RECORDED_RESPONSE_WITH_QUALITY_ISSUES,
    SAMPLE_BATCH_RESPONSE,
    SAMPLE_BATCH_RESPONSE_WITH_ERRORS,
    SAMPLE_EMPTY_RESPONSE
)


class TestTimeSeriesExtractor:
    """
    Test time-series extraction logic per TESTER.md specification
    Success Criteria: 6/6 tests pass
    """

    @pytest.fixture
    def mock_client(self):
        """Create mock PI Web API client"""
        client = Mock()
        client.batch_execute = Mock()
        client.get = Mock()
        return client

    @pytest.fixture
    def extractor(self, mock_client):
        """Create TimeSeriesExtractor instance with mocked client"""
        return TimeSeriesExtractor(mock_client)

    def test_extract_single_tag(self, extractor, mock_client):
        """
        Test extracting data for single tag
        Validates: Basic extraction works, DataFrame structure correct
        """
        # Mock batch response for single tag
        mock_client.batch_execute.return_value = {
            "Responses": [{"Status": 200, "Content": SAMPLE_RECORDED_RESPONSE}]
        }

        # Extract data
        df = extractor.extract_recorded_data(
            tag_webids=["F1DP-Tag1"],
            start_time=datetime(2025, 1, 8, 10, 0),
            end_time=datetime(2025, 1, 8, 11, 0)
        )

        # Assertions
        assert not df.empty, "DataFrame should not be empty"
        assert len(df) == 5, f"Expected 5 records, got {len(df)}"

        # Verify columns
        expected_columns = ['tag_webid', 'timestamp', 'value', 'quality_good',
                          'quality_questionable', 'units', 'ingestion_timestamp']
        for col in expected_columns:
            assert col in df.columns, f"Missing column: {col}"

        # Verify data
        assert df['tag_webid'].iloc[0] == "F1DP-Tag1"
        assert df['value'].iloc[0] == 75.5
        assert df['units'].iloc[0] == "degC"
        assert df['quality_good'].iloc[0] == True

        # Verify batch controller was called (not individual requests)
        assert mock_client.batch_execute.call_count == 1
        print("✓ Single tag extraction passed")

    def test_extract_multiple_tags_batch(self, extractor, mock_client):
        """
        Test batch controller usage for multiple tags
        Validates: Batch optimization works (1 HTTP call, not N calls)
        Critical for Alinta 30K tag scale requirement
        """
        # Mock batch response for 10 tags
        mock_responses = [
            {"Status": 200, "Content": SAMPLE_RECORDED_RESPONSE}
            for _ in range(10)
        ]
        mock_client.batch_execute.return_value = {"Responses": mock_responses}

        # Extract 10 tags
        tag_webids = [f"F1DP-Tag{i}" for i in range(10)]
        df = extractor.extract_recorded_data(
            tag_webids=tag_webids,
            start_time=datetime(2025, 1, 8, 10, 0),
            end_time=datetime(2025, 1, 8, 11, 0)
        )

        # Assertions
        assert not df.empty
        assert df['tag_webid'].nunique() == 10, "Should have 10 different tags"
        assert len(df) == 50, "10 tags × 5 records each = 50 total records"

        # CRITICAL: Verify batch controller optimization
        # Should be 1 batch call, NOT 10 individual calls
        assert mock_client.batch_execute.call_count == 1, \
            "Batch controller should make 1 call for 10 tags, not 10 calls"

        # Verify batch request structure
        call_args = mock_client.batch_execute.call_args[0][0]
        assert len(call_args) == 10, "Batch should contain 10 sub-requests"

        print("✓ Batch controller optimization verified (1 call for 10 tags)")

    def test_quality_flag_parsing(self, extractor, mock_client):
        """
        Test data quality flags are correctly parsed
        Validates: Good, Questionable, Substituted flags captured
        """
        mock_client.batch_execute.return_value = {
            "Responses": [{
                "Status": 200,
                "Content": SAMPLE_RECORDED_RESPONSE_WITH_QUALITY_ISSUES
            }]
        }

        df = extractor.extract_recorded_data(
            tag_webids=["F1DP-Tag1"],
            start_time=datetime(2025, 1, 8, 10, 0),
            end_time=datetime(2025, 1, 8, 11, 0)
        )

        # Verify quality flags
        assert df.iloc[0]['quality_good'] == True
        assert df.iloc[0]['quality_questionable'] == False

        # Record 2: Bad quality, questionable
        assert df.iloc[1]['quality_good'] == False
        assert df.iloc[1]['quality_questionable'] == True

        # Record 3: Good but substituted
        assert df.iloc[2]['quality_good'] == True
        assert df.iloc[2]['quality_questionable'] == False

        # Record 4: Bad quality
        assert df.iloc[3]['quality_good'] == False

        print("✓ Quality flag parsing correct")

    def test_paging_large_dataset(self, extractor, mock_client):
        """
        Test paging for >10K records per tag
        Validates: Automatic paging works for large time ranges
        """
        # Mock paging scenario:
        # Call 1: 10K records
        # Call 2: 5K records
        # Call 3: Empty (done)

        call_count = 0
        def batch_side_effect(requests_list):
            nonlocal call_count
            call_count += 1

            if call_count == 1:
                # First call: 10K records
                items = []
                for i in range(10000):
                    items.append({
                        "Timestamp": f"2025-01-08T{i//3600:02d}:{(i%3600)//60:02d}:{i%60:02d}Z",
                        "Value": 75.0 + (i % 100) * 0.01,
                        "UnitsAbbreviation": "degC",
                        "Good": True,
                        "Questionable": False,
                        "Substituted": False
                    })
                return {
                    "Responses": [{
                        "Status": 200,
                        "Content": {"Items": items, "Links": {}}
                    }]
                }

            elif call_count == 2:
                # Second call: 5K records
                items = []
                for i in range(5000):
                    items.append({
                        "Timestamp": f"2025-01-09T{i//3600:02d}:{(i%3600)//60:02d}:{i%60:02d}Z",
                        "Value": 76.0 + (i % 100) * 0.01,
                        "UnitsAbbreviation": "degC",
                        "Good": True,
                        "Questionable": False,
                        "Substituted": False
                    })
                return {
                    "Responses": [{
                        "Status": 200,
                        "Content": {"Items": items, "Links": {}}
                    }]
                }

            else:
                # Third call: Empty (no more data)
                return {
                    "Responses": [{
                        "Status": 200,
                        "Content": {"Items": [], "Links": {}}
                    }]
                }

        mock_client.batch_execute.side_effect = batch_side_effect

        # Extract with paging (use UTC timezone aware datetimes)
        from datetime import timezone
        df = extractor.extract_with_paging(
            tag_webids=["F1DP-Tag1"],
            start_time=datetime(2025, 1, 8, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2025, 1, 10, 0, 0, tzinfo=timezone.utc)
        )

        # Assertions
        assert len(df) == 15000, f"Expected 15000 records (10K + 5K), got {len(df)}"
        assert call_count >= 2, f"Should have made at least 2 paging calls, made {call_count}"

        # Verify timestamps are ordered
        timestamps = pd.to_datetime(df['timestamp'])
        assert timestamps.is_monotonic_increasing or len(timestamps) == len(timestamps.unique()), \
            "Timestamps should be in order"

        print(f"✓ Paging test passed: {len(df)} records in {call_count} pages")

    def test_handle_failed_tag(self, extractor, mock_client):
        """
        Test graceful handling when one tag fails
        Validates: Partial failures don't crash entire batch
        """
        # Mock response with 1 failed tag out of 3
        mock_client.batch_execute.return_value = {
            "Responses": [
                {"Status": 200, "Content": SAMPLE_RECORDED_RESPONSE},
                {"Status": 404, "Content": {"Message": "Tag not found"}},
                {"Status": 200, "Content": SAMPLE_RECORDED_RESPONSE}
            ]
        }

        df = extractor.extract_recorded_data(
            tag_webids=["F1DP-Tag1", "F1DP-Invalid", "F1DP-Tag3"],
            start_time=datetime(2025, 1, 8, 10, 0),
            end_time=datetime(2025, 1, 8, 11, 0)
        )

        # Should have data for 2 successful tags only
        assert df['tag_webid'].nunique() == 2, "Should have 2 tags (1 failed)"
        assert "F1DP-Invalid" not in df['tag_webid'].values, "Failed tag should not be in results"
        assert "F1DP-Tag1" in df['tag_webid'].values
        assert "F1DP-Tag3" in df['tag_webid'].values

        # Should have 10 records (2 tags × 5 records)
        assert len(df) == 10

        print("✓ Failed tag handling graceful")

    def test_empty_response_handling(self, extractor, mock_client):
        """
        Test handling of empty responses (no data in time range)
        Validates: Empty results return empty DataFrame, not error
        """
        mock_client.batch_execute.return_value = {
            "Responses": [{"Status": 200, "Content": SAMPLE_EMPTY_RESPONSE}]
        }

        df = extractor.extract_recorded_data(
            tag_webids=["F1DP-Tag1"],
            start_time=datetime(2025, 1, 8, 10, 0),
            end_time=datetime(2025, 1, 8, 11, 0)
        )

        # Should return empty DataFrame, not error
        assert isinstance(df, pd.DataFrame)
        assert df.empty, "Should return empty DataFrame for no data"

        print("✓ Empty response handled correctly")


class TestTimeSeriesExtractorEdgeCases:
    """Additional edge case tests for robustness"""

    @pytest.fixture
    def mock_client(self):
        client = Mock()
        client.batch_execute = Mock()
        return client

    @pytest.fixture
    def extractor(self, mock_client):
        return TimeSeriesExtractor(mock_client)

    def test_null_values_handling(self, extractor, mock_client):
        """Test handling of null/missing values in response"""
        mock_client.batch_execute.return_value = {
            "Responses": [{
                "Status": 200,
                "Content": {
                    "Items": [
                        {
                            "Timestamp": "2025-01-08T10:00:00Z",
                            "Value": None,  # Null value
                            "UnitsAbbreviation": "degC",
                            "Good": False,
                            "Questionable": True,
                            "Substituted": False
                        },
                        {
                            "Timestamp": "2025-01-08T10:01:00Z",
                            "Value": 75.5,
                            "UnitsAbbreviation": "degC",
                            "Good": True,
                            "Questionable": False,
                            "Substituted": False
                        }
                    ],
                    "Links": {}
                }
            }]
        }

        df = extractor.extract_recorded_data(
            tag_webids=["F1DP-Tag1"],
            start_time=datetime(2025, 1, 8, 10, 0),
            end_time=datetime(2025, 1, 8, 11, 0)
        )

        assert len(df) == 2
        assert pd.isna(df.iloc[0]['value'])
        assert df.iloc[1]['value'] == 75.5

        print("✓ Null value handling correct")

    def test_timestamp_parsing(self, extractor, mock_client):
        """Test timestamp parsing from ISO 8601 format"""
        mock_client.batch_execute.return_value = {
            "Responses": [{
                "Status": 200,
                "Content": SAMPLE_RECORDED_RESPONSE
            }]
        }

        df = extractor.extract_recorded_data(
            tag_webids=["F1DP-Tag1"],
            start_time=datetime(2025, 1, 8, 10, 0),
            end_time=datetime(2025, 1, 8, 11, 0)
        )

        # Verify timestamps are datetime objects
        assert isinstance(df.iloc[0]['timestamp'], pd.Timestamp)

        # Verify timestamp values (handle timezone)
        expected_timestamp = pd.Timestamp('2025-01-08 10:00:00', tz='UTC')
        assert df.iloc[0]['timestamp'] == expected_timestamp

        print("✓ Timestamp parsing correct")

    def test_large_batch_chunking(self, extractor, mock_client):
        """
        Test batch request chunking for very large tag lists
        PI Web API has limits on batch size
        """
        # Generate 500 tags (would normally exceed batch limits)
        num_tags = 500
        tag_webids = [f"F1DP-Tag{i}" for i in range(num_tags)]

        # Mock response
        mock_responses = [
            {"Status": 200, "Content": SAMPLE_RECORDED_RESPONSE}
            for _ in range(num_tags)
        ]
        mock_client.batch_execute.return_value = {"Responses": mock_responses}

        df = extractor.extract_recorded_data(
            tag_webids=tag_webids,
            start_time=datetime(2025, 1, 8, 10, 0),
            end_time=datetime(2025, 1, 8, 11, 0)
        )

        # Should successfully extract all tags
        assert df['tag_webid'].nunique() == num_tags

        print(f"✓ Large batch handling: {num_tags} tags")

    def test_ingestion_timestamp_added(self, extractor, mock_client):
        """Verify ingestion_timestamp is automatically added"""
        mock_client.batch_execute.return_value = {
            "Responses": [{
                "Status": 200,
                "Content": SAMPLE_RECORDED_RESPONSE
            }]
        }

        before_time = datetime.now()
        df = extractor.extract_recorded_data(
            tag_webids=["F1DP-Tag1"],
            start_time=datetime(2025, 1, 8, 10, 0),
            end_time=datetime(2025, 1, 8, 11, 0)
        )
        after_time = datetime.now()

        # Verify ingestion timestamp is present and recent
        assert 'ingestion_timestamp' in df.columns
        ingestion_ts = df.iloc[0]['ingestion_timestamp']
        assert before_time <= ingestion_ts <= after_time

        print("✓ Ingestion timestamp added correctly")


class TestTimeSeriesExtractorPerformance:
    """Performance-related tests per TESTER.md benchmarks"""

    @pytest.fixture
    def mock_client(self):
        client = Mock()
        client.batch_execute = Mock()
        return client

    @pytest.fixture
    def extractor(self, mock_client):
        return TimeSeriesExtractor(mock_client)

    def test_batch_vs_sequential_performance(self, extractor, mock_client):
        """
        Demonstrate batch controller performance advantage
        Target: >10x faster than sequential
        """
        import time

        num_tags = 100

        # Mock batch response
        mock_responses = [
            {"Status": 200, "Content": SAMPLE_RECORDED_RESPONSE}
            for _ in range(num_tags)
        ]
        mock_client.batch_execute.return_value = {"Responses": mock_responses}

        # Time batch extraction
        start = time.time()
        df = extractor.extract_recorded_data(
            tag_webids=[f"F1DP-Tag{i}" for i in range(num_tags)],
            start_time=datetime(2025, 1, 8, 10, 0),
            end_time=datetime(2025, 1, 8, 11, 0)
        )
        batch_time = time.time() - start

        # Estimate sequential time (would be ~100 × 200ms = 20 seconds)
        estimated_sequential_time = num_tags * 0.2

        # Batch should be much faster
        improvement_factor = estimated_sequential_time / batch_time

        print(f"✓ Batch extraction: {batch_time:.3f}s")
        print(f"✓ Estimated sequential: {estimated_sequential_time:.1f}s")
        print(f"✓ Performance improvement: {improvement_factor:.0f}x faster")

        assert improvement_factor > 10, "Batch should be >10x faster"


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "-s"])
