from typing import List, Dict
import pandas as pd
from datetime import datetime, timedelta
import logging


class TimeSeriesExtractor:
    """
    Extracts time-series data from PI Web API
    Uses batch controller for 100x performance improvement
    """
    
    def __init__(self, client):
        self.client = client
        self.logger = logging.getLogger(__name__)
        self.MAX_COUNT_PER_REQUEST = 10000  # PI Web API limit
        
    def extract_recorded_data(
        self,
        tag_webids: List[str],
        start_time: datetime,
        end_time: datetime,
        max_count: int = 10000
    ) -> pd.DataFrame:
        """
        Extract historical data for multiple tags using batch controller
        
        Args:
            tag_webids: List of PI point WebIds
            start_time: Start of time range
            end_time: End of time range
            max_count: Max records per tag per request
            
        Returns:
            DataFrame with columns: tag_webid, timestamp, value, quality_good, units
        """
        
        # Build batch request for all tags
        batch_requests = []
        for webid in tag_webids:
            batch_requests.append({
                "Method": "GET",
                "Resource": f"/streams/{webid}/recorded",
                "Parameters": {
                    "startTime": start_time.isoformat() + "Z",
                    "endTime": end_time.isoformat() + "Z",
                    "maxCount": str(max_count)
                }
            })
        
        # Execute batch (single HTTP call for all tags!)
        self.logger.info(f"Extracting {len(tag_webids)} tags via batch controller")
        batch_response = self.client.batch_execute(batch_requests)
        
        # Parse batch response
        all_data = []
        for i, sub_response in enumerate(batch_response.get("Responses", [])):
            tag_webid = tag_webids[i]
            
            if sub_response.get("Status") == 200:
                content = sub_response.get("Content", {})
                items = content.get("Items", [])
                
                for item in items:
                    all_data.append({
                        "tag_webid": tag_webid,
                        "timestamp": pd.to_datetime(item["Timestamp"]),
                        "value": item.get("Value"),
                        "quality_good": item.get("Good", True),
                        "quality_questionable": item.get("Questionable", False),
                        "units": item.get("UnitsAbbreviation", ""),
                        "ingestion_timestamp": datetime.now()
                    })
            else:
                self.logger.warning(
                    f"Tag {tag_webid} failed: {sub_response.get('Status')}"
                )
        
        return pd.DataFrame(all_data)
    
    def extract_with_paging(
        self,
        tag_webids: List[str],
        start_time: datetime,
        end_time: datetime
    ) -> pd.DataFrame:
        """
        Extract data with automatic paging for large time ranges
        Handles >10,000 records per tag
        """
        all_dataframes = []
        current_start = start_time
        
        while current_start < end_time:
            # Extract chunk
            df_chunk = self.extract_recorded_data(
                tag_webids=tag_webids,
                start_time=current_start,
                end_time=end_time,
                max_count=self.MAX_COUNT_PER_REQUEST
            )
            
            if df_chunk.empty:
                break
                
            all_dataframes.append(df_chunk)
            
            # Update cursor to max timestamp + 1 second
            max_ts = df_chunk['timestamp'].max()
            current_start = max_ts + timedelta(seconds=1)
            
            self.logger.info(f"Extracted chunk up to {max_ts}")
        
        return pd.concat(all_dataframes, ignore_index=True) if all_dataframes else pd.DataFrame()
