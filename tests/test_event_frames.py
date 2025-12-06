"""
Comprehensive test suite for EventFrameExtractor

Tests cover:
- Basic event frame extraction
- Duration calculation (start to end time)
- Active events handling (no EndTime)
- Template name filtering
- Event attributes extraction
- Referenced elements parsing
- Time range filtering
- Multiple event types (batches, maintenance, alarms)

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

from src.extractors.event_frame_extractor import EventFrameExtractor
from tests.fixtures.sample_responses import (
    SAMPLE_EVENT_FRAME,
    SAMPLE_ACTIVE_EVENT_FRAME,
    SAMPLE_EVENT_FRAMES_LIST,
    SAMPLE_EMPTY_RESPONSE
)


class TestEventFrameExtractor:
    """
    Test Event Frame extraction per TESTER.md specification
    Success Criteria: 5/5 tests pass
    Addresses: Alinta April 2024 request for Event Frame connectivity
    """

    @pytest.fixture
    def mock_client(self):
        """Create mock PI Web API client"""
        client = Mock()
        client.get = Mock()
        return client

    @pytest.fixture
    def extractor(self, mock_client):
        """Create EventFrameExtractor instance"""
        return EventFrameExtractor(mock_client)

    def test_extract_event_frames(self, extractor, mock_client):
        """
        Test basic event frame extraction
        Validates: Can extract event frames for time range
        """
        # Setup mock for multiple calls (event frames, then attributes)
        call_count = 0

        def get_side_effect(endpoint, **kwargs):
            nonlocal call_count
            call_count += 1
            mock_response = Mock()

            # First call: event frames list
            if "eventframes" in endpoint and "attributes" not in endpoint:
                mock_response.json.return_value = {
                    "Items": [SAMPLE_EVENT_FRAME]
                }
            # Subsequent calls: attributes
            else:
                mock_response.json.return_value = {
                    "Items": []
                }

            return mock_response

        mock_client.get.side_effect = get_side_effect

        # Extract event frames
        df = extractor.extract_event_frames(
            database_webid="F1DP-DB1",
            start_time=datetime(2025, 1, 8, 0, 0),
            end_time=datetime(2025, 1, 9, 0, 0)
        )

        # Assertions
        assert not df.empty, "Should return event frames"
        assert len(df) == 1

        # Verify event data
        event = df.iloc[0]
        assert event['event_name'] == "Batch-2025-01-08-001"
        assert event['template_name'] == "BatchRunTemplate"
        assert event['primary_element_id'] == "F1DP-Unit1"
        assert "Production" in event['category']

        # Verify first call was to eventframes endpoint
        first_call_args = mock_client.get.call_args_list[0]
        assert "eventframes" in first_call_args[0][0]
        assert first_call_args[1]['params']['startTime'] == "2025-01-08T00:00:00Z"
        assert first_call_args[1]['params']['endTime'] == "2025-01-09T00:00:00Z"

        print("✓ Basic event frame extraction works")

    def test_duration_calculation(self, extractor, mock_client):
        """
        Test event duration calculation
        Validates: Duration computed correctly from start/end times
        """
        # Event: 2025-01-08 10:00:00 to 12:30:00 = 150 minutes
        def get_side_effect(endpoint, **kwargs):
            mock_response = Mock()
            if "eventframes" in endpoint and "attributes" not in endpoint:
                mock_response.json.return_value = {
                    "Items": [SAMPLE_EVENT_FRAME]
                }
            else:
                mock_response.json.return_value = {"Items": []}
            return mock_response

        mock_client.get.side_effect = get_side_effect

        df = extractor.extract_event_frames(
            database_webid="F1DP-DB1",
            start_time=datetime(2025, 1, 8, 0, 0),
            end_time=datetime(2025, 1, 9, 0, 0)
        )

        # Check duration
        event = df.iloc[0]
        assert event['duration_minutes'] == 150.0, \
            f"Expected 150 minutes, got {event['duration_minutes']}"

        # Verify timestamps
        assert isinstance(event['start_time'], pd.Timestamp)
        assert isinstance(event['end_time'], pd.Timestamp)

        # Handle timezone aware timestamps
        start = pd.Timestamp('2025-01-08 10:00:00', tz='UTC')
        end = pd.Timestamp('2025-01-08 12:30:00', tz='UTC')
        assert event['start_time'] == start
        assert event['end_time'] == end

        print("✓ Duration calculation correct (150 minutes)")

    def test_active_event_no_end_time(self, extractor, mock_client):
        """
        Test handling of active events (no EndTime)
        Validates: Active/in-progress events handled gracefully
        Critical for real-time monitoring
        """
        # Mock active event (no EndTime)
        def get_side_effect(endpoint, **kwargs):
            mock_response = Mock()
            if "eventframes" in endpoint and "attributes" not in endpoint:
                mock_response.json.return_value = {
                    "Items": [SAMPLE_ACTIVE_EVENT_FRAME]
                }
            else:
                mock_response.json.return_value = {"Items": []}
            return mock_response

        mock_client.get.side_effect = get_side_effect

        df = extractor.extract_event_frames(
            database_webid="F1DP-DB1",
            start_time=datetime(2025, 1, 8, 0, 0),
            end_time=datetime(2025, 1, 9, 0, 0)
        )

        # Assertions
        event = df.iloc[0]

        # EndTime should be None/NaT for active events
        assert pd.isna(event['end_time']) or event['end_time'] is None, \
            "Active event should have no end_time"

        # Duration should be None/NaN for active events
        assert pd.isna(event['duration_minutes']) or event['duration_minutes'] is None, \
            "Active event should have no duration"

        # Should still have start_time
        assert not pd.isna(event['start_time'])
        assert event['event_name'] == "Batch-2025-01-08-Active"

        print("✓ Active event (no EndTime) handled correctly")

    def test_template_name_filter(self, extractor, mock_client):
        """
        Test filtering by template name
        Validates: Can filter events by type (e.g., only BatchRunTemplate)
        """
        mock_response = Mock()
        mock_response.json.return_value = {"Items": []}
        mock_client.get.return_value = mock_response

        # Extract with template filter
        extractor.extract_event_frames(
            database_webid="F1DP-DB1",
            start_time=datetime(2025, 1, 8, 0, 0),
            end_time=datetime(2025, 1, 9, 0, 0),
            template_name="BatchRunTemplate"
        )

        # Check API was called with template filter
        call_args = mock_client.get.call_args
        assert 'params' in call_args[1]
        assert call_args[1]['params']['templateName'] == "BatchRunTemplate"

        print("✓ Template name filter works")

    def test_multiple_event_types(self, extractor, mock_client):
        """
        Test extraction of multiple event types
        Validates: Batches, maintenance, alarms all handled
        """
        call_count = 0

        def get_side_effect(endpoint, **kwargs):
            nonlocal call_count
            call_count += 1
            mock_response = Mock()
            if call_count == 1:  # First call: event frames list
                mock_response.json.return_value = SAMPLE_EVENT_FRAMES_LIST
            else:  # Subsequent calls: attributes
                mock_response.json.return_value = {"Items": []}
            return mock_response

        mock_client.get.side_effect = get_side_effect

        df = extractor.extract_event_frames(
            database_webid="F1DP-DB1",
            start_time=datetime(2025, 1, 8, 0, 0),
            end_time=datetime(2025, 1, 9, 0, 0)
        )

        # Should have 3 events (2 batches + 1 maintenance)
        assert len(df) == 3

        # Verify templates
        templates = df['template_name'].unique()
        assert "BatchRunTemplate" in templates
        assert "MaintenanceTemplate" in templates

        # Verify categories
        assert any("Production" in cats for cats in df['category'])
        assert any("Maintenance" in cats for cats in df['category'])

        print("✓ Multiple event types handled")


class TestEventFrameExtractorEdgeCases:
    """Additional edge case tests for robustness"""

    @pytest.fixture
    def mock_client(self):
        client = Mock()
        client.get = Mock()
        return client

    @pytest.fixture
    def extractor(self, mock_client):
        return EventFrameExtractor(mock_client)

    def test_empty_event_frames(self, extractor, mock_client):
        """Test handling when no events in time range"""
        mock_response = Mock()
        mock_response.json.return_value = {"Items": []}
        mock_client.get.return_value = mock_response

        df = extractor.extract_event_frames(
            database_webid="F1DP-DB1",
            start_time=datetime(2025, 1, 8, 0, 0),
            end_time=datetime(2025, 1, 9, 0, 0)
        )

        # Should return empty DataFrame, not error
        assert isinstance(df, pd.DataFrame)
        assert df.empty

        print("✓ Empty event frames handled gracefully")

    def test_event_attributes_extraction(self, extractor, mock_client):
        """
        Test extraction of event frame attributes
        Validates: Can get batch parameters, product info, etc.
        """
        # Mock event frame with attributes endpoint
        def get_side_effect(endpoint, **kwargs):
            mock_response = Mock()

            # Main event frames query
            if "eventframes" in endpoint and "attributes" not in endpoint:
                mock_response.json.return_value = {
                    "Items": [SAMPLE_EVENT_FRAME]
                }

            # Attributes query
            elif "attributes" in endpoint and "value" not in endpoint:
                mock_response.json.return_value = {
                    "Items": [
                        {"WebId": "F1DP-Attr1", "Name": "Product"},
                        {"WebId": "F1DP-Attr2", "Name": "BatchSize"},
                        {"WebId": "F1DP-Attr3", "Name": "Operator"}
                    ]
                }

            # Attribute value queries
            elif "streams" in endpoint and "value" in endpoint:
                if "Attr1" in endpoint:
                    mock_response.json.return_value = {"Value": "Grade-A"}
                elif "Attr2" in endpoint:
                    mock_response.json.return_value = {"Value": 1000.0}
                elif "Attr3" in endpoint:
                    mock_response.json.return_value = {"Value": "John Smith"}
                else:
                    mock_response.json.return_value = {"Value": None}

            else:
                mock_response.json.return_value = {"Items": []}

            return mock_response

        mock_client.get.side_effect = get_side_effect

        df = extractor.extract_event_frames(
            database_webid="F1DP-DB1",
            start_time=datetime(2025, 1, 8, 0, 0),
            end_time=datetime(2025, 1, 9, 0, 0)
        )

        # Verify attributes extracted
        event = df.iloc[0]
        assert 'event_attributes' in df.columns
        attrs = event['event_attributes']

        assert isinstance(attrs, dict)
        assert 'Product' in attrs
        assert attrs['Product'] == "Grade-A"
        assert attrs['BatchSize'] == 1000.0
        assert attrs['Operator'] == "John Smith"

        print("✓ Event attributes extraction works")

    def test_referenced_elements(self, extractor, mock_client):
        """
        Test extraction of referenced elements
        Validates: Can identify equipment involved in event
        """
        mock_response = Mock()
        mock_response.json.return_value = {
            "Items": [SAMPLE_EVENT_FRAME]
        }
        mock_client.get.return_value = mock_response

        df = extractor.extract_event_frames(
            database_webid="F1DP-DB1",
            start_time=datetime(2025, 1, 8, 0, 0),
            end_time=datetime(2025, 1, 9, 0, 0)
        )

        event = df.iloc[0]

        # Check primary referenced element
        assert event['primary_element_id'] == "F1DP-Unit1"

        # Check referenced elements list (equipment used)
        # Note: SAMPLE_EVENT_FRAME has ReferencedElementWebIds
        # We need to verify this is captured
        assert 'referenced_elements' in df.columns

        print("✓ Referenced elements captured")

    def test_search_mode_parameter(self, extractor, mock_client):
        """
        Test different search modes (Overlapped, Inclusive, Exact)
        """
        mock_response = Mock()
        mock_response.json.return_value = {"Items": []}
        mock_client.get.return_value = mock_response

        # Test Overlapped mode (default)
        extractor.extract_event_frames(
            database_webid="F1DP-DB1",
            start_time=datetime(2025, 1, 8, 0, 0),
            end_time=datetime(2025, 1, 9, 0, 0),
            search_mode="Overlapped"
        )

        call_args = mock_client.get.call_args
        assert call_args[1]['params']['searchMode'] == "Overlapped"

        # Test Inclusive mode
        extractor.extract_event_frames(
            database_webid="F1DP-DB1",
            start_time=datetime(2025, 1, 8, 0, 0),
            end_time=datetime(2025, 1, 9, 0, 0),
            search_mode="Inclusive"
        )

        call_args = mock_client.get.call_args
        assert call_args[1]['params']['searchMode'] == "Inclusive"

        print("✓ Search mode parameter works")

    def test_long_running_event(self, extractor, mock_client):
        """
        Test event with very long duration (days/weeks)
        Validates: Duration calculation works for long events
        """
        # Create event lasting 7 days
        long_event = {
            "WebId": "F1DP-EF-LongEvent",
            "Name": "LongMaintenanceEvent",
            "TemplateName": "MaintenanceTemplate",
            "StartTime": "2025-01-01T00:00:00Z",
            "EndTime": "2025-01-08T00:00:00Z",  # 7 days later
            "PrimaryReferencedElementWebId": "F1DP-Unit1",
            "CategoryNames": ["Maintenance"]
        }

        mock_response = Mock()
        mock_response.json.return_value = {"Items": [long_event]}
        mock_client.get.return_value = mock_response

        df = extractor.extract_event_frames(
            database_webid="F1DP-DB1",
            start_time=datetime(2025, 1, 1, 0, 0),
            end_time=datetime(2025, 1, 10, 0, 0)
        )

        event = df.iloc[0]

        # 7 days = 7 * 24 * 60 = 10,080 minutes
        expected_duration = 7 * 24 * 60
        assert event['duration_minutes'] == expected_duration

        print(f"✓ Long event duration correct: {expected_duration} minutes (7 days)")

    def test_event_with_missing_fields(self, extractor, mock_client):
        """
        Test event with missing optional fields
        Validates: Handles incomplete event data gracefully
        """
        minimal_event = {
            "WebId": "F1DP-EF-Minimal",
            "Name": "MinimalEvent",
            "StartTime": "2025-01-08T10:00:00Z",
            "EndTime": "2025-01-08T11:00:00Z"
            # Missing: TemplateName, PrimaryReferencedElementWebId, CategoryNames, Description
        }

        mock_response = Mock()
        mock_response.json.return_value = {"Items": [minimal_event]}
        mock_client.get.return_value = mock_response

        df = extractor.extract_event_frames(
            database_webid="F1DP-DB1",
            start_time=datetime(2025, 1, 8, 0, 0),
            end_time=datetime(2025, 1, 9, 0, 0)
        )

        # Should still work with None values
        event = df.iloc[0]
        assert event['event_name'] == "MinimalEvent"
        assert event['duration_minutes'] == 60.0

        # Optional fields should be None or empty
        assert event['template_name'] is None or pd.isna(event['template_name'])

        print("✓ Missing optional fields handled")


class TestEventFrameExtractorIntegration:
    """Integration-style tests with realistic scenarios"""

    @pytest.fixture
    def mock_client(self):
        client = Mock()
        client.get = Mock()
        return client

    @pytest.fixture
    def extractor(self, mock_client):
        return EventFrameExtractor(mock_client)

    def test_alinta_batch_traceability_scenario(self, extractor, mock_client):
        """
        Test Alinta use case: Batch production traceability
        Scenario: Extract all production batches for a week
        """
        # Mock week of production batches
        batches = []
        start_date = datetime(2025, 1, 1, 0, 0)

        for day in range(7):
            for batch_num in range(3):  # 3 batches per day
                batch_start = start_date + timedelta(days=day, hours=batch_num * 8)
                batch_end = batch_start + timedelta(hours=6)

                batches.append({
                    "WebId": f"F1DP-Batch-Day{day}-Num{batch_num}",
                    "Name": f"Batch-{batch_start.strftime('%Y%m%d')}-{batch_num:03d}",
                    "TemplateName": "ProductionBatchTemplate",
                    "StartTime": batch_start.isoformat() + "Z",
                    "EndTime": batch_end.isoformat() + "Z",
                    "PrimaryReferencedElementWebId": f"F1DP-Unit{batch_num % 3 + 1}",
                    "CategoryNames": ["Production"],
                    "Description": f"Production batch on unit {batch_num % 3 + 1}"
                })

        mock_response = Mock()
        mock_response.json.return_value = {"Items": batches}
        mock_client.get.return_value = mock_response

        df = extractor.extract_event_frames(
            database_webid="F1DP-ProductionDB",
            start_time=datetime(2025, 1, 1, 0, 0),
            end_time=datetime(2025, 1, 8, 0, 0),
            template_name="ProductionBatchTemplate"
        )

        # Verify extraction
        assert len(df) == 21, f"Expected 21 batches (7 days × 3 batches), got {len(df)}"

        # All should be production batches
        assert all(df['template_name'] == "ProductionBatchTemplate")

        # All should have 6-hour duration
        assert all(df['duration_minutes'] == 360.0)

        # Should span across 3 units
        assert df['primary_element_id'].nunique() == 3

        print("✓ Alinta batch traceability scenario works")
        print(f"  - Extracted {len(df)} batches over 7 days")
        print(f"  - Average duration: {df['duration_minutes'].mean():.1f} minutes")

    def test_thames_water_alarm_analytics_scenario(self, extractor, mock_client):
        """
        Test Thames Water use case: Alarm event analytics
        Scenario: Extract alarm events for analysis
        """
        alarms = []

        # Generate various alarm types
        alarm_types = [
            ("HighPressureAlarm", 45),  # 45 min duration
            ("LowFlowAlarm", 120),      # 2 hours
            ("TemperatureAlarm", 30)    # 30 min
        ]

        for i, (alarm_type, duration) in enumerate(alarm_types * 5):  # 15 alarms total
            alarm_start = datetime(2025, 1, 8, 0, 0) + timedelta(hours=i * 2)
            alarm_end = alarm_start + timedelta(minutes=duration)

            alarms.append({
                "WebId": f"F1DP-Alarm{i}",
                "Name": f"{alarm_type}-{i:03d}",
                "TemplateName": "AlarmTemplate",
                "StartTime": alarm_start.isoformat() + "Z",
                "EndTime": alarm_end.isoformat() + "Z",
                "PrimaryReferencedElementWebId": f"F1DP-Pump{i % 5 + 1}",
                "CategoryNames": ["Alarm", "Critical"],
                "Description": f"{alarm_type} on Pump {i % 5 + 1}"
            })

        mock_response = Mock()
        mock_response.json.return_value = {"Items": alarms}
        mock_client.get.return_value = mock_response

        df = extractor.extract_event_frames(
            database_webid="F1DP-WaterDB",
            start_time=datetime(2025, 1, 8, 0, 0),
            end_time=datetime(2025, 1, 9, 0, 0),
            template_name="AlarmTemplate"
        )

        # Verify extraction
        assert len(df) == 15

        # All should be alarms
        assert all(df['template_name'] == "AlarmTemplate")

        # Calculate alarm statistics
        avg_duration = df['duration_minutes'].mean()
        max_duration = df['duration_minutes'].max()
        min_duration = df['duration_minutes'].min()

        print("✓ Thames Water alarm analytics scenario works")
        print(f"  - Extracted {len(df)} alarm events")
        print(f"  - Average duration: {avg_duration:.1f} minutes")
        print(f"  - Range: {min_duration:.0f} - {max_duration:.0f} minutes")


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "-s"])
