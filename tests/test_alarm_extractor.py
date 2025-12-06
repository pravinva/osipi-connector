"""
Tests for Alarm Extractor Module

Tests alarm history extraction functionality.
"""

import pytest
from unittest.mock import Mock, MagicMock
from datetime import datetime, timedelta
from src.extractors.alarm_extractor import AlarmExtractor


@pytest.fixture
def mock_client():
    """Create mock PI Web API client."""
    client = Mock()
    return client


@pytest.fixture
def alarm_extractor(mock_client):
    """Create AlarmExtractor instance with mock client."""
    return AlarmExtractor(mock_client)


class TestNotificationRules:
    """Test notification rule extraction."""

    def test_extract_notification_rules_success(self, alarm_extractor, mock_client):
        """Test successful notification rule extraction."""
        mock_client.get.return_value = {
            'Items': [
                {
                    'WebId': 'F1DP-RULE-001',
                    'Name': 'High Temperature Alert',
                    'Description': 'Alert when temperature exceeds 100C',
                    'Criteria': 'Temp > 100',
                    'IsEnabled': True,
                    'Severity': 3,
                    'NotificationMethod': 'Email',
                    'TriggerCondition': 'OnChange',
                    'CreatedDate': '2025-01-01T00:00:00Z',
                    'ModifiedDate': '2025-01-05T00:00:00Z'
                }
            ]
        }

        rules = alarm_extractor.extract_notification_rules('F1DP-DB1')

        assert len(rules) == 1
        assert rules[0]['rule_id'] == 'F1DP-RULE-001'
        assert rules[0]['rule_name'] == 'High Temperature Alert'
        assert rules[0]['is_enabled'] is True
        assert rules[0]['severity'] == 3
        mock_client.get.assert_called_once()

    def test_extract_notification_rules_empty(self, alarm_extractor, mock_client):
        """Test extraction with no rules."""
        mock_client.get.return_value = {'Items': []}

        rules = alarm_extractor.extract_notification_rules('F1DP-DB1')

        assert len(rules) == 0

    def test_extract_notification_rules_error(self, alarm_extractor, mock_client):
        """Test error handling during rule extraction."""
        mock_client.get.side_effect = Exception("Connection error")

        rules = alarm_extractor.extract_notification_rules('F1DP-DB1')

        assert len(rules) == 0


class TestAlarmEvents:
    """Test alarm event extraction."""

    def test_extract_alarm_events_success(self, alarm_extractor, mock_client):
        """Test successful alarm event extraction."""
        start_time = datetime(2025, 1, 1)
        end_time = datetime(2025, 1, 2)

        mock_client.get.return_value = {
            'Items': [
                {
                    'Id': 'ALARM-001',
                    'Timestamp': '2025-01-01T12:00:00Z',
                    'Value': 105.5,
                    'Description': 'Temperature exceeded limit',
                    'Creator': 'PI System',
                    'CreationDate': '2025-01-01T12:00:01Z',
                    'ModifiedDate': '2025-01-01T12:00:01Z',
                    'Good': True
                }
            ]
        }

        events = alarm_extractor.extract_alarm_events(
            'F1DP-TAG-001', start_time, end_time
        )

        assert len(events) == 1
        assert events[0]['annotation_id'] == 'ALARM-001'
        assert events[0]['tag_webid'] == 'F1DP-TAG-001'
        assert events[0]['value'] == 105.5
        assert events[0]['good'] is True

    def test_extract_alarm_events_max_count(self, alarm_extractor, mock_client):
        """Test max_count parameter."""
        start_time = datetime(2025, 1, 1)
        end_time = datetime(2025, 1, 2)
        mock_client.get.return_value = {'Items': []}

        alarm_extractor.extract_alarm_events(
            'F1DP-TAG-001', start_time, end_time, max_count=5000
        )

        call_args = mock_client.get.call_args
        assert call_args[1]['params']['maxCount'] == 5000


class TestActiveAlarms:
    """Test active alarm extraction."""

    def test_extract_active_alarms_success(self, alarm_extractor, mock_client):
        """Test successful active alarm extraction."""
        mock_client.get.return_value = {
            'Items': [
                {
                    'WebId': 'EF-ALARM-001',
                    'Name': 'Active Temperature Alarm',
                    'TemplateName': 'HighTempAlarmTemplate',
                    'StartTime': '2025-01-01T10:00:00Z',
                    'Severity': 2,
                    'PrimaryReferencedElementWebId': 'F1DP-ELEM-001',
                    'AreValuesCaptured': False,
                    'Description': 'Temperature exceeding threshold'
                }
            ]
        }

        alarms = alarm_extractor.extract_active_alarms('F1DP-DB1')

        assert len(alarms) == 1
        assert alarms[0]['event_frame_id'] == 'EF-ALARM-001'
        assert alarms[0]['is_active'] is True
        assert alarms[0]['acknowledged'] is False
        assert alarms[0]['severity'] == 2

    def test_extract_active_alarms_filters_non_alarms(self, alarm_extractor, mock_client):
        """Test that non-alarm event frames are filtered out."""
        mock_client.get.return_value = {
            'Items': [
                {
                    'WebId': 'EF-001',
                    'Name': 'Normal Event',
                    'TemplateName': 'ProductionRunTemplate',  # Not an alarm
                    'StartTime': '2025-01-01T10:00:00Z'
                },
                {
                    'WebId': 'EF-ALARM-001',
                    'Name': 'Alarm Event',
                    'TemplateName': 'AlarmTemplate',  # Is an alarm
                    'StartTime': '2025-01-01T11:00:00Z',
                    'Severity': 3
                }
            ]
        }

        alarms = alarm_extractor.extract_active_alarms('F1DP-DB1')

        assert len(alarms) == 1
        assert alarms[0]['event_frame_id'] == 'EF-ALARM-001'


class TestAlarmSummary:
    """Test alarm summary statistics."""

    def test_extract_alarm_summary_success(self, alarm_extractor, mock_client):
        """Test alarm summary extraction."""
        start_time = datetime(2025, 1, 1)
        end_time = datetime(2025, 1, 7)

        mock_client.get.return_value = {
            'Items': [
                {
                    'TemplateName': 'AlarmTemplate',
                    'Severity': 1,
                    'EndTime': '2025-01-02T00:00:00Z'
                },
                {
                    'TemplateName': 'AlarmTemplate',
                    'Severity': 2,
                    'EndTime': None  # Active alarm
                },
                {
                    'TemplateName': 'AlarmTemplate',
                    'Severity': 2,
                    'EndTime': '2025-01-03T00:00:00Z'
                },
                {
                    'TemplateName': 'ProductionRun',  # Not an alarm
                    'Severity': 0
                }
            ]
        }

        summary = alarm_extractor.extract_alarm_summary(
            'F1DP-DB1', start_time, end_time
        )

        assert summary['total_alarms'] == 3
        assert summary['active_alarms'] == 1
        assert summary['resolved_alarms'] == 2
        assert summary['severity_distribution'][1] == 1
        assert summary['severity_distribution'][2] == 2

    def test_extract_alarm_summary_empty(self, alarm_extractor, mock_client):
        """Test alarm summary with no alarms."""
        start_time = datetime(2025, 1, 1)
        end_time = datetime(2025, 1, 7)

        mock_client.get.return_value = {'Items': []}

        summary = alarm_extractor.extract_alarm_summary(
            'F1DP-DB1', start_time, end_time
        )

        assert summary['total_alarms'] == 0
        assert summary['active_alarms'] == 0
        assert summary['resolved_alarms'] == 0


class TestElementAlarms:
    """Test element alarm extraction."""

    def test_extract_element_alarms_success(self, alarm_extractor, mock_client):
        """Test element alarm extraction."""
        mock_client.get.return_value = {
            'Items': [
                {
                    'WebId': 'ATTR-001',
                    'Name': 'Temperature',
                    'Path': '/Plant/Unit1/Temperature',
                    'HasChildren': False,
                    'ConfigValues': {
                        'AlarmConfig': {
                            'HighLimit': 100,
                            'LowLimit': 0
                        }
                    }
                }
            ]
        }

        alarms = alarm_extractor.extract_element_alarms('F1DP-ELEM-001')

        assert len(alarms) == 1
        assert alarms[0]['attribute_name'] == 'Temperature'
        assert alarms[0]['has_alarm'] is True

    def test_extract_element_alarms_with_values(self, alarm_extractor, mock_client):
        """Test element alarm extraction with current values."""
        # First call returns attributes
        # Second call returns current value
        mock_client.get.side_effect = [
            {
                'Items': [
                    {
                        'WebId': 'ATTR-001',
                        'Name': 'Temperature',
                        'Path': '/Plant/Unit1/Temperature',
                        'HasChildren': False,
                        'ConfigValues': {'AlarmConfig': {}}
                    }
                ]
            },
            {
                'Value': 95.5,
                'Timestamp': '2025-01-01T12:00:00Z'
            }
        ]

        alarms = alarm_extractor.extract_element_alarms(
            'F1DP-ELEM-001',
            start_time=datetime(2025, 1, 1),
            end_time=datetime(2025, 1, 2)
        )

        assert len(alarms) == 1
        assert alarms[0]['current_value'] == 95.5
