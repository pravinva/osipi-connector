from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime
import logging

class EventFrameExtractor:
    """
    Extracts PI Event Frames
    Addresses: Alinta April 2024 request for Event Frame connectivity
    Use case: Thames Water alarm analytics, batch traceability
    """

    def __init__(self, client):
        self.client = client
        self.logger = logging.getLogger(__name__)

    def extract_event_frames(
        self,
        database_webid: str,
        start_time: datetime,
        end_time: datetime,
        template_name: Optional[str] = None,
        search_mode: str = "Overlapped"
    ) -> pd.DataFrame:
        """
        Extract event frames for time range

        Args:
            database_webid: AF database WebId
            start_time: Start of search range
            end_time: End of search range
            template_name: Filter by template (e.g., "BatchRunTemplate")
            search_mode: "Overlapped" | "Inclusive" | "Exact"

        Returns:
            DataFrame with event frame data
        """

        params = {
            "startTime": start_time.isoformat() + "Z",
            "endTime": end_time.isoformat() + "Z",
            "searchMode": search_mode
        }

        if template_name:
            params["templateName"] = template_name

        response = self.client.get(
            f"/piwebapi/assetdatabases/{database_webid}/eventframes",
            params=params
        )

        event_frames = response.json().get("Items", [])

        # Parse event frames
        events_data = []
        for ef in event_frames:
            # Extract basic event info
            event_data = {
                "event_frame_id": ef["WebId"],
                "event_name": ef["Name"],
                "template_name": ef.get("TemplateName"),
                "start_time": pd.to_datetime(ef["StartTime"]),
                "end_time": pd.to_datetime(ef.get("EndTime")) if ef.get("EndTime") else None,
                "primary_element_id": ef.get("PrimaryReferencedElementWebId"),
                "category": ef.get("CategoryNames", []),
                "description": ef.get("Description", "")
            }

            # Calculate duration
            if event_data["end_time"]:
                duration = event_data["end_time"] - event_data["start_time"]
                event_data["duration_minutes"] = duration.total_seconds() / 60
            else:
                event_data["duration_minutes"] = None

            # Get event frame attributes (batch info, product, operator, etc.)
            try:
                attributes = self._get_event_attributes(ef["WebId"])
                event_data["event_attributes"] = attributes
            except Exception as e:
                self.logger.warning(f"Failed to get attributes for {ef['Name']}: {e}")
                event_data["event_attributes"] = {}

            # Get referenced elements (equipment involved)
            event_data["referenced_elements"] = ef.get("ReferencedElementWebIds", [])

            events_data.append(event_data)

        return pd.DataFrame(events_data)

    def _get_event_attributes(self, event_frame_webid: str) -> Dict:
        """Get attribute values for event frame"""
        response = self.client.get(
            f"/piwebapi/eventframes/{event_frame_webid}/attributes"
        )

        attributes = {}
        for attr in response.json().get("Items", []):
            # Get attribute value
            attr_name = attr["Name"]

            # Try to get value (may not exist)
            try:
                value_response = self.client.get(
                    f"/piwebapi/streams/{attr['WebId']}/value"
                )
                value_data = value_response.json()
                attributes[attr_name] = value_data.get("Value")
            except:
                attributes[attr_name] = None

        return attributes
