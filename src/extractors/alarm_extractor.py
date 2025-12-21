"""
PI Web API Alarm History Extractor

Extracts alarm and notification data from PI Web API.
Supports both current alarms and historical alarm events.
"""

from typing import List, Dict, Optional, Any
from datetime import datetime
import logging
from ..client.pi_web_api_client import PIWebAPIClient

logger = logging.getLogger(__name__)


class AlarmExtractor:
    """
    Extracts alarm history and notifications from PI Web API.

    PI Alarms can come from:
    - PI Event Frames (process alarms)
    - PI Notifications (system alarms)
    - AF Element attributes with alarm configuration
    """

    def __init__(self, client: PIWebAPIClient):
        """
        Initialize alarm extractor.

        Args:
            client: Configured PI Web API client
        """
        self.client = client

    def extract_notification_rules(
        self,
        database_webid: str
    ) -> List[Dict[str, Any]]:
        """
        Extract notification rules configured in PI AF database.

        Args:
            database_webid: PI AF database WebID

        Returns:
            List of notification rule configurations
        """
        logger.info(f"Extracting notification rules from database {database_webid}")

        try:
            response = self.client.get(
                f"/assetdatabases/{database_webid}/notificationrules"
            )

            rules = []
            for item in response.get('Items', []):
                rule = {
                    'rule_id': item.get('WebId'),
                    'rule_name': item.get('Name'),
                    'description': item.get('Description', ''),
                    'criteria': item.get('Criteria', ''),
                    'is_enabled': item.get('IsEnabled', False),
                    'severity': item.get('Severity', 0),
                    'notification_method': item.get('NotificationMethod', ''),
                    'trigger_condition': item.get('TriggerCondition', ''),
                    'created_date': item.get('CreatedDate'),
                    'modified_date': item.get('ModifiedDate')
                }
                rules.append(rule)

            logger.info(f"Extracted {len(rules)} notification rules")
            return rules

        except Exception as e:
            logger.error(f"Failed to extract notification rules: {e}")
            return []

    def extract_alarm_events(
        self,
        tag_webid: str,
        start_time: datetime,
        end_time: datetime,
        max_count: int = 10000
    ) -> List[Dict[str, Any]]:
        """
        Extract alarm events for a specific tag.

        Args:
            tag_webid: Tag WebID to query
            start_time: Start time for alarm history
            end_time: End time for alarm history
            max_count: Maximum events to retrieve

        Returns:
            List of alarm events with timestamps and metadata
        """
        logger.info(f"Extracting alarm events for tag {tag_webid}")

        params = {
            'startTime': start_time.isoformat() + 'Z',
            'endTime': end_time.isoformat() + 'Z',
            'maxCount': max_count
        }

        try:
            # Query annotation (alarm) data
            response = self.client.get(
                f"/streams/{tag_webid}/annotations",
                params=params
            )

            events = []
            for item in response.get('Items', []):
                event = {
                    'annotation_id': item.get('Id'),
                    'tag_webid': tag_webid,
                    'timestamp': item.get('Timestamp'),
                    'value': item.get('Value'),
                    'description': item.get('Description', ''),
                    'creator': item.get('Creator', ''),
                    'annotation_time': item.get('CreationDate'),
                    'modified_time': item.get('ModifiedDate'),
                    'good': item.get('Good', True)
                }
                events.append(event)

            logger.info(f"Extracted {len(events)} alarm events")
            return events

        except Exception as e:
            logger.error(f"Failed to extract alarm events: {e}")
            return []

    def extract_element_alarms(
        self,
        element_webid: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Extract alarm configuration and status for an AF element.

        Args:
            element_webid: AF element WebID
            start_time: Optional start time for alarm history
            end_time: Optional end time for alarm history

        Returns:
            List of alarm configurations and current states
        """
        logger.info(f"Extracting element alarms for {element_webid}")

        try:
            # Get element attributes
            response = self.client.get(
                f"/elements/{element_webid}/attributes"
            )

            alarms = []
            for attr in response.get('Items', []):
                # Check if attribute has alarm configuration
                config_values = attr.get('ConfigValues', {})
                if 'AlarmConfig' in config_values or attr.get('HasChildren', False):

                    alarm = {
                        'element_webid': element_webid,
                        'attribute_id': attr.get('WebId'),
                        'attribute_name': attr.get('Name'),
                        'attribute_path': attr.get('Path'),
                        'has_alarm': True,
                        'alarm_config': config_values.get('AlarmConfig', {}),
                        'current_value': None,
                        'alarm_state': 'Unknown'
                    }

                    # Get current value if time range specified
                    if start_time and end_time:
                        try:
                            value_response = self.client.get(
                                f"/streams/{attr['WebId']}/value"
                            )
                            alarm['current_value'] = value_response.get('Value')
                            alarm['current_timestamp'] = value_response.get('Timestamp')
                        except Exception as ve:
                            logger.warning(f"Could not get value for {attr['Name']}: {ve}")

                    alarms.append(alarm)

            logger.info(f"Extracted {len(alarms)} element alarms")
            return alarms

        except Exception as e:
            logger.error(f"Failed to extract element alarms: {e}")
            return []

    def extract_active_alarms(
        self,
        database_webid: str
    ) -> List[Dict[str, Any]]:
        """
        Extract currently active alarms from AF database.

        Args:
            database_webid: PI AF database WebID

        Returns:
            List of currently active alarms
        """
        logger.info(f"Extracting active alarms from database {database_webid}")

        try:
            # Query for active event frames with alarm templates
            params = {
                'searchMode': 'InProgress',  # Only active (not yet ended)
                'maxCount': 10000
            }

            response = self.client.get(
                f"/assetdatabases/{database_webid}/eventframes",
                params=params
            )

            active_alarms = []
            for item in response.get('Items', []):
                # Filter for alarm-type event frames
                template_name = item.get('TemplateName', '')
                if 'alarm' in template_name.lower() or 'alert' in template_name.lower():
                    alarm = {
                        'event_frame_id': item.get('WebId'),
                        'alarm_name': item.get('Name'),
                        'template_name': template_name,
                        'start_time': item.get('StartTime'),
                        'severity': item.get('Severity', 0),
                        'primary_element_id': item.get('PrimaryReferencedElementWebId'),
                        'acknowledged': item.get('AreValuesCaptured', False),
                        'description': item.get('Description', ''),
                        'duration_minutes': None,  # Still active, no end time
                        'is_active': True
                    }
                    active_alarms.append(alarm)

            logger.info(f"Found {len(active_alarms)} active alarms")
            return active_alarms

        except Exception as e:
            logger.error(f"Failed to extract active alarms: {e}")
            return []

    def extract_alarm_summary(
        self,
        database_webid: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """
        Extract summary statistics for alarms in a time period.

        Args:
            database_webid: PI AF database WebID
            start_time: Start time for analysis
            end_time: End time for analysis

        Returns:
            Summary statistics dictionary
        """
        logger.info(f"Extracting alarm summary for {start_time} to {end_time}")

        try:
            params = {
                'startTime': start_time.isoformat() + 'Z',
                'endTime': end_time.isoformat() + 'Z',
                'maxCount': 10000
            }

            response = self.client.get(
                f"/assetdatabases/{database_webid}/eventframes",
                params=params
            )

            items = response.get('Items', [])

            # Filter for alarm event frames
            alarm_events = [
                item for item in items
                if 'alarm' in item.get('TemplateName', '').lower()
            ]

            # Calculate statistics
            total_alarms = len(alarm_events)
            active_alarms = sum(1 for item in alarm_events if not item.get('EndTime'))

            # Group by severity if available
            severity_counts = {}
            for item in alarm_events:
                severity = item.get('Severity', 0)
                severity_counts[severity] = severity_counts.get(severity, 0) + 1

            summary = {
                'database_webid': database_webid,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'total_alarms': total_alarms,
                'active_alarms': active_alarms,
                'resolved_alarms': total_alarms - active_alarms,
                'severity_distribution': severity_counts,
                'analysis_timestamp': datetime.now().isoformat()
            }

            logger.info(f"Alarm summary: {total_alarms} total, {active_alarms} active")
            return summary

        except Exception as e:
            logger.error(f"Failed to extract alarm summary: {e}")
            return {}
