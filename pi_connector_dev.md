# PI Web API Lakeflow Connector - Developer Specification

## Executive Summary

Build a production-ready Databricks Lakeflow connector for OSI PI System that ingests:
1. Time-series data (sensor values)
2. PI Asset Framework hierarchy (asset metadata)
3. Event Frames (process events)

**Target:** Alinta Energy validated architecture - replace "Custom PI Extract (API call)" placeholder
**Validation:** Solves April 2024 customer request for "PI AF and Event Frame connectivity"
**Competition:** AVEVA CDS limited to 2,000 tags at >5min granularity; we handle 30K+ tags at raw granularity

---

## Architecture Overview

```
┌─────────────────────────────────────────────────┐
│  PI Server (Customer On-Prem)                   │
│  ├─ PI Data Archive (time-series storage)       │
│  ├─ PI Asset Framework (hierarchy/metadata)     │
│  └─ PI Web API (REST endpoint in DMZ)           │
└────────────────┬────────────────────────────────┘
                 │ HTTPS/TLS 1.2+
                 │ (Direct Connect or Internet)
                 ▼
┌─────────────────────────────────────────────────┐
│  Lakeflow Connector (Databricks Serverless)     │
│  ┌───────────────────────────────────────────┐  │
│  │ Module 1: Authentication                  │  │
│  │ - Kerberos, Basic, OAuth                  │  │
│  └───────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────┐  │
│  │ Module 2: Time-Series Extractor           │  │
│  │ - Batch controller optimization           │  │
│  │ - Incremental cursor per tag              │  │
│  └───────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────┐  │
│  │ Module 3: AF Hierarchy Extractor          │  │
│  │ - Recursive element tree traversal        │  │
│  │ - Template metadata extraction            │  │
│  └───────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────┐  │
│  │ Module 4: Event Frame Extractor           │  │
│  │ - Process event extraction                │  │
│  │ - Event attribute collection              │  │
│  └───────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────┐  │
│  │ Module 5: Delta Lake Writer               │  │
│  │ - Schema mapping                          │  │
│  │ - Incremental merge                       │  │
│  └───────────────────────────────────────────┘  │
└─────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────┐
│  Unity Catalog (Delta Tables)                    │
│  ├─ bronze.pi_timeseries                         │
│  ├─ bronze.pi_asset_hierarchy                    │
│  ├─ bronze.pi_event_frames                       │
│  └─ checkpoints.pi_watermarks                    │
└─────────────────────────────────────────────────┘
```

---

## Module 1: Authentication Handler

### File: `src/auth/pi_auth_manager.py`

**Purpose:** Handle PI Web API authentication (Kerberos, Basic, OAuth)

**Requirements:**
- Support multiple auth types via configuration
- Refresh tokens automatically
- Handle auth errors gracefully

**Implementation:**

```python
from typing import Dict, Optional
import requests
from requests.auth import HTTPBasicAuth
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
import logging

class PIAuthManager:
    """
    Manages authentication for PI Web API
    Supports: Kerberos, Basic, OAuth
    """
    
    def __init__(self, auth_config: Dict[str, str]):
        """
        Args:
            auth_config: {
                'type': 'basic' | 'kerberos' | 'oauth',
                'username': str (for basic),
                'password': str (for basic),
                'oauth_token': str (for oauth)
            }
        """
        self.auth_type = auth_config['type']
        self.config = auth_config
        self.logger = logging.getLogger(__name__)
        
    def get_auth_handler(self):
        """Return appropriate auth handler for requests library"""
        if self.auth_type == 'basic':
            return HTTPBasicAuth(
                self.config['username'],
                self.config['password']
            )
        elif self.auth_type == 'kerberos':
            return HTTPKerberosAuth(mutual_authentication=OPTIONAL)
        elif self.auth_type == 'oauth':
            # Return None, will use headers
            return None
        else:
            raise ValueError(f"Unsupported auth type: {self.auth_type}")
    
    def get_headers(self) -> Dict[str, str]:
        """Return auth headers for requests"""
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        if self.auth_type == 'oauth':
            headers['Authorization'] = f"Bearer {self.config['oauth_token']}"
        
        return headers
    
    def test_connection(self, base_url: str) -> bool:
        """Test authentication by calling /piwebapi endpoint"""
        try:
            response = requests.get(
                f"{base_url}/piwebapi",
                auth=self.get_auth_handler(),
                headers=self.get_headers(),
                timeout=10
            )
            response.raise_for_status()
            self.logger.info("Authentication successful")
            return True
        except Exception as e:
            self.logger.error(f"Authentication failed: {e}")
            return False
```

**Test Cases (see TESTER.md):**
- ✓ Basic auth with valid credentials
- ✓ Basic auth with invalid credentials
- ✓ Kerberos auth (mock)
- ✓ Connection test success/failure

---

## Module 2: PI Web API Client

### File: `src/client/pi_web_api_client.py`

**Purpose:** HTTP client wrapper for PI Web API with retry logic

**Key Features:**
- Exponential backoff retry
- Rate limiting
- Connection pooling
- Error categorization

**Implementation:**

```python
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Dict, List, Optional
import logging
from datetime import datetime

class PIWebAPIClient:
    """
    Low-level HTTP client for PI Web API
    Handles retries, rate limiting, connection pooling
    """
    
    def __init__(self, base_url: str, auth_manager):
        self.base_url = base_url.rstrip('/')
        self.auth_manager = auth_manager
        self.session = self._create_session()
        self.logger = logging.getLogger(__name__)
        
    def _create_session(self) -> requests.Session:
        """Create session with retry strategy"""
        session = requests.Session()
        
        # Retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE"]
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20
        )
        
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.auth = self.auth_manager.get_auth_handler()
        session.headers.update(self.auth_manager.get_headers())
        
        return session
    
    def get(self, endpoint: str, params: Optional[Dict] = None) -> requests.Response:
        """GET request with error handling"""
        url = f"{self.base_url}{endpoint}"
        self.logger.debug(f"GET {url} params={params}")
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            self.logger.error(f"GET failed: {url} - {e}")
            raise
    
    def post(self, endpoint: str, json_data: Dict) -> requests.Response:
        """POST request with error handling"""
        url = f"{self.base_url}{endpoint}"
        self.logger.debug(f"POST {url}")
        
        try:
            response = self.session.post(url, json=json_data, timeout=60)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            self.logger.error(f"POST failed: {url} - {e}")
            raise
    
    def batch_execute(self, requests_list: List[Dict]) -> Dict:
        """
        Execute batch request using PI Web API batch controller
        Critical for performance with multiple tags
        """
        batch_payload = {"Requests": requests_list}
        response = self.post("/piwebapi/batch", batch_payload)
        return response.json()
```

**Test Cases:**
- ✓ Successful GET request
- ✓ Retry on 503 error
- ✓ Timeout handling
- ✓ Batch request with 100 items

---

## Module 3: Time-Series Extractor

### File: `src/extractors/timeseries_extractor.py`

**Purpose:** Extract time-series data using batch controller for performance

**Critical Optimization:** Use `/batch` endpoint for parallel extraction

**Implementation:**

```python
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
```

**Test Cases:**
- ✓ Single tag extraction
- ✓ Batch extraction (100 tags)
- ✓ Paging with >10K records
- ✓ Handle missing data
- ✓ Quality flag filtering

---

## Module 4: AF Hierarchy Extractor

### File: `src/extractors/af_extractor.py`

**Purpose:** Extract PI Asset Framework hierarchy and metadata

**This addresses Alinta April 2024 request: "PI AF connectivity"**

**Implementation:**

```python
from typing import Dict, List, Optional
import pandas as pd
import logging

class AFHierarchyExtractor:
    """
    Extracts PI Asset Framework hierarchy
    Addresses: Alinta April 2024 request for AF connectivity
    """
    
    def __init__(self, client):
        self.client = client
        self.logger = logging.getLogger(__name__)
        
    def get_asset_databases(self) -> List[Dict]:
        """List available AF databases"""
        response = self.client.get("/piwebapi/assetdatabases")
        return response.json().get("Items", [])
    
    def extract_hierarchy(
        self,
        database_webid: str,
        root_element_webid: Optional[str] = None,
        max_depth: int = 10
    ) -> pd.DataFrame:
        """
        Recursively extract AF element hierarchy
        
        Args:
            database_webid: AF database WebId
            root_element_webid: Starting element (None = database root)
            max_depth: Maximum recursion depth (safety limit)
            
        Returns:
            DataFrame with: element_id, name, path, parent_id, template_name, type
        """
        
        hierarchy_data = []
        
        def walk_elements(element_webid: str, parent_path: str = "", depth: int = 0):
            """Recursive hierarchy traversal"""
            if depth > max_depth:
                self.logger.warning(f"Max depth {max_depth} reached")
                return
            
            # Get element details
            try:
                element_response = self.client.get(f"/piwebapi/elements/{element_webid}")
                element = element_response.json()
            except Exception as e:
                self.logger.error(f"Failed to get element {element_webid}: {e}")
                return
            
            # Build element path (like /Enterprise/Site1/Unit2/Pump-101)
            element_path = f"{parent_path}/{element['Name']}"
            
            # Extract element data
            hierarchy_data.append({
                "element_id": element["WebId"],
                "element_name": element["Name"],
                "element_path": element_path,
                "parent_id": parent_path if parent_path else None,
                "template_name": element.get("TemplateName"),
                "element_type": element.get("Type", "Element"),
                "description": element.get("Description", ""),
                "categories": element.get("CategoryNames", []),
                "depth": depth
            })
            
            # Get child elements
            try:
                children_response = self.client.get(
                    f"/piwebapi/elements/{element_webid}/elements"
                )
                children = children_response.json().get("Items", [])
                
                # Recurse to children
                for child in children:
                    walk_elements(child["WebId"], element_path, depth + 1)
                    
            except Exception as e:
                self.logger.warning(f"No children for {element['Name']}: {e}")
        
        # Start traversal
        if root_element_webid:
            walk_elements(root_element_webid)
        else:
            # Start from database root elements
            db_elements_response = self.client.get(
                f"/piwebapi/assetdatabases/{database_webid}/elements"
            )
            root_elements = db_elements_response.json().get("Items", [])
            for root in root_elements:
                walk_elements(root["WebId"])
        
        return pd.DataFrame(hierarchy_data)
    
    def extract_element_attributes(self, element_webid: str) -> pd.DataFrame:
        """
        Extract attributes for an element
        Provides metadata about what data is available
        """
        response = self.client.get(f"/piwebapi/elements/{element_webid}/attributes")
        attributes = response.json().get("Items", [])
        
        attr_data = []
        for attr in attributes:
            attr_data.append({
                "attribute_id": attr["WebId"],
                "element_id": element_webid,
                "attribute_name": attr["Name"],
                "data_reference": attr.get("DataReferencePlugIn"),
                "default_uom": attr.get("DefaultUnitsName", ""),
                "type": attr.get("Type"),
                "is_configuration": attr.get("IsConfigurationItem", False)
            })
        
        return pd.DataFrame(attr_data)
```

**Test Cases:**
- ✓ Extract 3-level hierarchy
- ✓ Handle circular references (safety)
- ✓ Extract element attributes
- ✓ Handle missing templates

---

## Module 5: Event Frame Extractor

### File: `src/extractors/event_frame_extractor.py`

**Purpose:** Extract PI Event Frames (batch runs, downtime events, alarms)

**This addresses Alinta April 2024 request: "Event Frame connectivity"**

**Implementation:**

```python
from typing import Dict, List
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
```

**Test Cases:**
- ✓ Extract event frames in time range
- ✓ Filter by template name
- ✓ Extract event attributes
- ✓ Handle active events (no end time)

---

## Module 6: Checkpoint Manager

### File: `src/checkpoints/checkpoint_manager.py`

**Purpose:** Track ingestion progress per tag (incremental load)

**Implementation:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max
from typing import Dict
from datetime import datetime
import logging

class CheckpointManager:
    """
    Manages incremental ingestion state
    Tracks last successful timestamp per tag
    """
    
    def __init__(self, spark: SparkSession, checkpoint_table: str):
        self.spark = spark
        self.checkpoint_table = checkpoint_table  # e.g., "checkpoints.pi_watermarks"
        self.logger = logging.getLogger(__name__)
        self._ensure_checkpoint_table_exists()
    
    def _ensure_checkpoint_table_exists(self):
        """Create checkpoint table if doesn't exist"""
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.checkpoint_table} (
                tag_webid STRING,
                tag_name STRING,
                last_timestamp TIMESTAMP,
                last_ingestion_run TIMESTAMP,
                record_count BIGINT
            )
            USING DELTA
        """)
    
    def get_watermarks(self, tag_webids: List[str]) -> Dict[str, datetime]:
        """
        Get last successful timestamp for each tag
        
        Returns:
            Dict mapping tag_webid -> last_timestamp
        """
        if not tag_webids:
            return {}
        
        # Query checkpoint table
        df = self.spark.table(self.checkpoint_table) \
            .filter(col("tag_webid").isin(tag_webids)) \
            .select("tag_webid", "last_timestamp")
        
        # Convert to dict
        watermarks = {
            row.tag_webid: row.last_timestamp 
            for row in df.collect()
        }
        
        # For tags without checkpoint, use default (e.g., 30 days ago)
        default_start = datetime.now() - timedelta(days=30)
        for tag in tag_webids:
            if tag not in watermarks:
                watermarks[tag] = default_start
                self.logger.info(f"New tag {tag}, using default start: {default_start}")
        
        return watermarks
    
    def update_watermarks(self, tag_data: Dict[str, Dict]):
        """
        Update checkpoints after successful ingestion
        
        Args:
            tag_data: {
                tag_webid: {
                    "tag_name": str,
                    "max_timestamp": datetime,
                    "record_count": int
                }
            }
        """
        
        checkpoint_rows = []
        for tag_webid, data in tag_data.items():
            checkpoint_rows.append({
                "tag_webid": tag_webid,
                "tag_name": data["tag_name"],
                "last_timestamp": data["max_timestamp"],
                "last_ingestion_run": datetime.now(),
                "record_count": data["record_count"]
            })
        
        checkpoint_df = self.spark.createDataFrame(checkpoint_rows)
        
        # Upsert checkpoint (merge on tag_webid)
        checkpoint_df.createOrReplaceTempView("checkpoint_updates")
        
        self.spark.sql(f"""
            MERGE INTO {self.checkpoint_table} AS target
            USING checkpoint_updates AS source
            ON target.tag_webid = source.tag_webid
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        
        self.logger.info(f"Updated checkpoints for {len(checkpoint_rows)} tags")
```

**Test Cases:**
- ✓ Get watermarks for new tags (default)
- ✓ Get watermarks for existing tags
- ✓ Update watermarks after ingestion
- ✓ Upsert logic (new + existing tags)

---

## Module 7: Delta Lake Writer

### File: `src/writers/delta_writer.py`

**Purpose:** Write data to Unity Catalog Delta tables with optimizations

**Implementation:**

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import *
import logging

class DeltaLakeWriter:
    """
    Writes PI data to Unity Catalog Delta tables
    Handles schema evolution, optimizations
    """
    
    def __init__(self, spark: SparkSession, catalog: str, schema: str):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.logger = logging.getLogger(__name__)
        self._ensure_schema_exists()
    
    def _ensure_schema_exists(self):
        """Create catalog and schema if don't exist"""
        self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog}")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
    
    def write_timeseries(
        self,
        df: DataFrame,
        table_name: str = "pi_timeseries",
        partition_cols: List[str] = None
    ):
        """
        Write time-series data with optimizations
        
        Optimizations:
        - Partitioned by date for query performance
        - ZORDER by tag_webid for filtering
        - Schema evolution enabled
        """
        
        if df.empty:
            self.logger.warning("Empty dataframe, skipping write")
            return
        
        # Convert pandas to Spark DataFrame if needed
        if isinstance(df, pd.DataFrame):
            df = self.spark.createDataFrame(df)
        
        # Add partition column (date from timestamp)
        df = df.withColumn("partition_date", col("timestamp").cast("date"))
        
        full_table_name = f"{self.catalog}.{self.schema}.{table_name}"
        
        # Write with append mode
        df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("partition_date") \
            .saveAsTable(full_table_name)
        
        # Optimize with ZORDER for better query performance
        try:
            self.spark.sql(f"""
                OPTIMIZE {full_table_name}
                ZORDER BY (tag_webid, timestamp)
            """)
            self.logger.info(f"Optimized {full_table_name}")
        except Exception as e:
            self.logger.warning(f"Optimization failed (non-critical): {e}")
    
    def write_af_hierarchy(self, df: DataFrame):
        """Write AF hierarchy as dimension table"""
        full_table_name = f"{self.catalog}.{self.schema}.pi_asset_hierarchy"
        
        if isinstance(df, pd.DataFrame):
            df = self.spark.createDataFrame(df)
        
        # Overwrite mode for hierarchy (full refresh)
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(full_table_name)
        
        self.logger.info(f"Wrote {df.count()} elements to {full_table_name}")
    
    def write_event_frames(self, df: DataFrame):
        """Write event frames with incremental logic"""
        full_table_name = f"{self.catalog}.{self.schema}.pi_event_frames"
        
        if isinstance(df, pd.DataFrame):
            df = self.spark.createDataFrame(df)
        
        # Append mode for events
        df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(full_table_name)
```

**Test Cases:**
- ✓ Write time-series data
- ✓ Partition optimization
- ✓ Write AF hierarchy
- ✓ Write event frames
- ✓ Schema evolution

---

## Module 8: Main Connector

### File: `src/connector/pi_lakeflow_connector.py`

**Purpose:** Orchestrate all modules, entry point for Lakeflow

**Implementation:**

```python
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import logging
from typing import Dict, List

class PILakeflowConnector:
    """
    Main connector orchestrating all modules
    Entry point called by Lakeflow framework
    """
    
    def __init__(self, config: Dict):
        """
        Args:
            config: {
                'pi_web_api_url': str,
                'auth': {type, username, password},
                'catalog': str,
                'schema': str,
                'tags': List[str] or 'all',
                'af_database_id': str (optional),
                'include_event_frames': bool
            }
        """
        self.config = config
        self.spark = SparkSession.builder.getOrCreate()
        self.logger = logging.getLogger(__name__)
        
        # Initialize modules
        from src.auth.pi_auth_manager import PIAuthManager
        from src.client.pi_web_api_client import PIWebAPIClient
        from src.extractors.timeseries_extractor import TimeSeriesExtractor
        from src.extractors.af_extractor import AFHierarchyExtractor
        from src.extractors.event_frame_extractor import EventFrameExtractor
        from src.checkpoints.checkpoint_manager import CheckpointManager
        from src.writers.delta_writer import DeltaLakeWriter
        
        self.auth_manager = PIAuthManager(config['auth'])
        self.client = PIWebAPIClient(config['pi_web_api_url'], self.auth_manager)
        self.ts_extractor = TimeSeriesExtractor(self.client)
        self.af_extractor = AFHierarchyExtractor(self.client)
        self.ef_extractor = EventFrameExtractor(self.client)
        self.checkpoint_mgr = CheckpointManager(
            self.spark,
            f"{config['catalog']}.checkpoints.pi_watermarks"
        )
        self.writer = DeltaLakeWriter(
            self.spark,
            config['catalog'],
            config['schema']
        )
    
    def run(self):
        """
        Main execution flow
        Called by Lakeflow scheduler
        """
        self.logger.info("Starting PI connector run")
        
        # Step 1: Extract AF hierarchy (full refresh, run first)
        if self.config.get('af_database_id'):
            self.logger.info("Extracting AF hierarchy...")
            af_hierarchy = self.af_extractor.extract_hierarchy(
                self.config['af_database_id']
            )
            self.writer.write_af_hierarchy(af_hierarchy)
        
        # Step 2: Get tags to ingest
        tag_webids = self._get_tag_list()
        self.logger.info(f"Processing {len(tag_webids)} tags")
        
        # Step 3: Get checkpoints (where we left off)
        watermarks = self.checkpoint_mgr.get_watermarks(tag_webids)
        
        # Step 4: Extract time-series data (incremental)
        end_time = datetime.now()
        
        all_timeseries = []
        # Process in batches of 100 tags (batch controller limit)
        BATCH_SIZE = 100
        for i in range(0, len(tag_webids), BATCH_SIZE):
            batch_tags = tag_webids[i:i+BATCH_SIZE]
            
            # Get earliest watermark for this batch
            min_start = min(watermarks[tag] for tag in batch_tags)
            
            self.logger.info(f"Extracting batch {i//BATCH_SIZE + 1}: {len(batch_tags)} tags")
            
            ts_df = self.ts_extractor.extract_recorded_data(
                tag_webids=batch_tags,
                start_time=min_start,
                end_time=end_time
            )
            
            all_timeseries.append(ts_df)
        
        # Combine and write
        if all_timeseries:
            combined_df = pd.concat(all_timeseries, ignore_index=True)
            self.writer.write_timeseries(combined_df)
            
            # Update checkpoints
            self._update_checkpoints_from_df(combined_df)
        
        # Step 5: Extract Event Frames (if enabled)
        if self.config.get('include_event_frames') and self.config.get('af_database_id'):
            self.logger.info("Extracting Event Frames...")
            
            # Get last event frame checkpoint
            last_ef_time = self._get_last_event_frame_time()
            
            ef_df = self.ef_extractor.extract_event_frames(
                database_webid=self.config['af_database_id'],
                start_time=last_ef_time,
                end_time=end_time
            )
            
            if not ef_df.empty:
                self.writer.write_event_frames(ef_df)
        
        self.logger.info("PI connector run completed successfully")
    
    def _get_tag_list(self) -> List[str]:
        """Get list of tag WebIds to ingest"""
        # For demo: hardcoded list or from config
        # Production: query /points endpoint with filters
        return self.config.get('tags', [])
    
    def _update_checkpoints_from_df(self, df: pd.DataFrame):
        """Update checkpoints based on ingested data"""
        # Group by tag, get max timestamp
        tag_stats = df.groupby('tag_webid').agg({
            'timestamp': 'max',
            'value': 'count'
        }).reset_index()
        
        tag_data = {}
        for _, row in tag_stats.iterrows():
            tag_data[row['tag_webid']] = {
                "tag_name": row['tag_webid'],  # Would map to actual name
                "max_timestamp": row['timestamp'],
                "record_count": row['value']
            }
        
        self.checkpoint_mgr.update_watermarks(tag_data)
    
    def _get_last_event_frame_time(self) -> datetime:
        """Get checkpoint for event frame extraction"""
        try:
            max_time = self.spark.sql(f"""
                SELECT MAX(start_time) as max_time
                FROM {self.catalog}.{self.schema}.pi_event_frames
            """).collect()[0].max_time
            
            return max_time if max_time else datetime.now() - timedelta(days=30)
        except:
            return datetime.now() - timedelta(days=30)
```

---

## Configuration Schema

### File: `config/connector_config.yaml`

```yaml
# PI Web API Connector Configuration

pi_web_api:
  url: "https://pi-server.company.com/piwebapi"
  auth:
    type: "basic"  # basic | kerberos | oauth
    username: "${PI_USERNAME}"  # From Databricks Secrets
    password: "${PI_PASSWORD}"

databricks:
  catalog: "main"
  schema: "bronze"
  checkpoint_table: "checkpoints.pi_watermarks"

ingestion:
  # Option 1: Specific tags
  tags:
    - "F1DP-WebId-Plant1-Temp-PV"
    - "F1DP-WebId-Plant1-Press-PV"
  
  # Option 2: Query all points matching filter
  tag_filter:
    name_pattern: "*Temperature*"
    max_tags: 1000
  
  # AF hierarchy
  af_database_id: "F1DP-AF-Database-WebId"
  include_af_hierarchy: true
  
  # Event frames
  include_event_frames: true
  event_frame_templates:
    - "BatchRunTemplate"
    - "DowntimeTemplate"
  
  # Schedule
  incremental_window_days: 30  # How far back to check
  
performance:
  batch_size: 100  # Tags per batch request
  max_count_per_request: 10000  # PI Web API limit
  parallel_batches: 5  # Concurrent batch requests
```

---

## Delta Table Schemas

### Bronze Layer Tables

**Table 1: bronze.pi_timeseries**
```sql
CREATE TABLE bronze.pi_timeseries (
  tag_webid STRING COMMENT 'PI Point WebId (unique identifier)',
  tag_name STRING COMMENT 'Human-readable tag name',
  timestamp TIMESTAMP COMMENT 'Sample timestamp (PI Server time)',
  value DOUBLE COMMENT 'Measured value',
  quality_good BOOLEAN COMMENT 'Data quality flag - Good',
  quality_questionable BOOLEAN COMMENT 'Data quality flag - Questionable',
  quality_substituted BOOLEAN COMMENT 'Data quality flag - Substituted',
  units STRING COMMENT 'Engineering units (degC, bar, m3/h)',
  ingestion_timestamp TIMESTAMP COMMENT 'When ingested to Databricks',
  data_delay_seconds INT COMMENT 'Delay between measurement and ingestion'
)
USING DELTA
PARTITIONED BY (partition_date DATE)
COMMENT 'Raw PI time-series data - Alinta "Raw Sensor Data (lots of tags)" use case'
```

**Table 2: bronze.pi_asset_hierarchy**
```sql
CREATE TABLE bronze.pi_asset_hierarchy (
  element_id STRING PRIMARY KEY COMMENT 'AF Element WebId',
  element_name STRING COMMENT 'Element name',
  element_path STRING COMMENT 'Full path (e.g., /Enterprise/Site1/Unit2/Pump-101)',
  parent_id STRING COMMENT 'Parent element WebId',
  template_name STRING COMMENT 'AF Template name (e.g., PumpTemplate)',
  element_type STRING COMMENT 'Element type',
  description STRING COMMENT 'Element description',
  categories ARRAY<STRING> COMMENT 'Element categories',
  depth INT COMMENT 'Hierarchy depth level',
  last_updated TIMESTAMP COMMENT 'Last metadata refresh'
)
USING DELTA
COMMENT 'PI Asset Framework hierarchy - Alinta April 2024 request'
```

**Table 3: bronze.pi_event_frames**
```sql
CREATE TABLE bronze.pi_event_frames (
  event_frame_id STRING PRIMARY KEY COMMENT 'Event Frame WebId',
  event_name STRING COMMENT 'Event frame name',
  template_name STRING COMMENT 'Event frame template',
  start_time TIMESTAMP COMMENT 'Event start timestamp',
  end_time TIMESTAMP COMMENT 'Event end timestamp (NULL if active)',
  duration_minutes DOUBLE COMMENT 'Event duration',
  primary_element_id STRING COMMENT 'Primary equipment/asset WebId',
  event_attributes MAP<STRING, STRING> COMMENT 'Event attributes (Product, Operator, etc)',
  referenced_elements ARRAY<STRING> COMMENT 'All equipment involved in event',
  category ARRAY<STRING> COMMENT 'Event categories',
  description STRING COMMENT 'Event description'
)
USING DELTA
PARTITIONED BY (event_date DATE)
COMMENT 'PI Event Frames - Alinta April 2024 request, batch/downtime tracking'
```

**Table 4: checkpoints.pi_watermarks**
```sql
CREATE TABLE checkpoints.pi_watermarks (
  tag_webid STRING PRIMARY KEY,
  tag_name STRING,
  last_timestamp TIMESTAMP COMMENT 'Last successfully ingested timestamp',
  last_ingestion_run TIMESTAMP COMMENT 'When last ingestion ran',
  record_count BIGINT COMMENT 'Total records ingested'
)
USING DELTA
COMMENT 'Checkpoint tracking for incremental ingestion'
```

---

## PI Web API Endpoints Reference

### Discovery Endpoints

```http
GET /piwebapi/
# Returns API metadata and version

GET /piwebapi/dataservers
# Returns: { "Items": [ { "Name": "PIServer", "WebId": "..." } ] }

GET /piwebapi/dataservers/{webid}/points
# Params: nameFilter (e.g., "*Temp*"), maxCount
# Returns: List of PI Points
```

### Time-Series Endpoints

```http
GET /piwebapi/streams/{webid}/recorded
# Params:
#   startTime: ISO8601 (e.g., "2025-01-08T00:00:00Z")
#   endTime: ISO8601
#   maxCount: integer (max 150,000, recommend 10,000)
#   boundaryType: "Inside" | "Outside" | "Interpolated"
# Returns: {
#   "Items": [
#     {
#       "Timestamp": "2025-01-08T10:00:00Z",
#       "Value": 75.5,
#       "UnitsAbbreviation": "degC",
#       "Good": true,
#       "Questionable": false,
#       "Substituted": false
#     }
#   ],
#   "Links": { "Next": "..." }  # If paging needed
# }

POST /piwebapi/batch
# Body: {
#   "Requests": [
#     {
#       "Method": "GET",
#       "Resource": "/streams/{webid1}/recorded",
#       "Parameters": { "startTime": "...", "endTime": "..." }
#     },
#     { ... }  # Up to 100-1000 sub-requests
#   ]
# }
# Returns: { "Responses": [ { "Status": 200, "Content": {...} } ] }
```

### AF Hierarchy Endpoints

```http
GET /piwebapi/assetdatabases
# Returns: List of AF databases

GET /piwebapi/assetdatabases/{dbid}/elements
# Returns: Root elements of database

GET /piwebapi/elements/{elementid}/elements
# Returns: Child elements (for recursive traversal)

GET /piwebapi/elements/{elementid}/attributes
# Returns: Attributes of element (data references, calculations)
```

### Event Frame Endpoints

```http
GET /piwebapi/assetdatabases/{dbid}/eventframes
# Params:
#   startTime: ISO8601
#   endTime: ISO8601
#   templateName: (optional filter)
#   searchMode: "Overlapped" | "Inclusive" | "Exact"
# Returns: Event frames in time range

GET /piwebapi/eventframes/{efid}/attributes
# Returns: Attribute values for event frame
```

---

## Development Workflow with Claude Code

### Subagent Task Breakdown

**Subagent 1: Authentication & Client**
```bash
claude code "Implement PI Web API authentication module supporting Basic and Kerberos auth with connection testing"
```
- Files: `src/auth/pi_auth_manager.py`, `src/client/pi_web_api_client.py`
- Tests: `tests/test_auth.py`, `tests/test_client.py`
- Duration: 1-2 hours

**Subagent 2: Time-Series Extraction**
```bash
claude code "Build PI time-series extractor using batch controller for parallel tag extraction with paging support"
```
- Files: `src/extractors/timeseries_extractor.py`
- Tests: `tests/test_timeseries.py`
- Duration: 2-3 hours

**Subagent 3: AF Hierarchy**
```bash
claude code "Implement recursive PI Asset Framework hierarchy extraction with element and attribute metadata"
```
- Files: `src/extractors/af_extractor.py`
- Tests: `tests/test_af_extraction.py`
- Duration: 2-3 hours

**Subagent 4: Event Frames**
```bash
claude code "Create PI Event Frame extractor with attribute collection and template filtering"
```
- Files: `src/extractors/event_frame_extractor.py`
- Tests: `tests/test_event_frames.py`
- Duration: 1-2 hours

**Subagent 5: Checkpoint & Delta Writer**
```bash
claude code "Build checkpoint manager for incremental ingestion and Delta Lake writer with Unity Catalog support"
```
- Files: `src/checkpoints/checkpoint_manager.py`, `src/writers/delta_writer.py`
- Tests: `tests/test_checkpoints.py`, `tests/test_writer.py`
- Duration: 2-3 hours

**Subagent 6: Integration & Orchestration**
```bash
claude code "Create main connector orchestrator integrating all modules with error handling and logging"
```
- Files: `src/connector/pi_lakeflow_connector.py`
- Tests: `tests/test_integration.py`
- Duration: 1-2 hours

---

## Mock PI Server for Development

### File: `tests/mock_pi_server.py`

**Purpose:** Simulate PI Web API for development/testing without real PI Server

```python
from fastapi import FastAPI, Query, HTTPException
from datetime import datetime, timedelta
import random
import uvicorn

app = FastAPI(title="Mock PI Web API Server")

# Mock data
MOCK_TAGS = {
    "F1DP-Tag1": {"name": "Plant1_Temp_PV", "units": "degC", "base": 75.0},
    "F1DP-Tag2": {"name": "Plant1_Press_PV", "units": "bar", "base": 5.2},
    # Add 100 more for realistic testing
}

MOCK_AF_HIERARCHY = {
    "F1DP-DB1": {
        "Name": "ProductionDB",
        "Elements": [
            {
                "WebId": "F1DP-Site1",
                "Name": "Sydney_Plant",
                "TemplateName": "PlantTemplate",
                "Elements": [
                    {
                        "WebId": "F1DP-Unit1",
                        "Name": "Unit_1",
                        "TemplateName": "UnitTemplate"
                    }
                ]
            }
        ]
    }
}

@app.get("/piwebapi")
def root():
    return {"Version": "Mock 1.0"}

@app.get("/piwebapi/dataservers")
def list_dataservers():
    return {
        "Items": [{
            "Name": "MockPIServer",
            "WebId": "F1DP-Server1"
        }]
    }

@app.get("/piwebapi/streams/{webid}/recorded")
def get_recorded(
    webid: str,
    startTime: str,
    endTime: str,
    maxCount: int = 1000
):
    """Generate realistic time-series data"""
    start = datetime.fromisoformat(startTime.replace('Z', ''))
    end = datetime.fromisoformat(endTime.replace('Z', ''))
    
    tag_info = MOCK_TAGS.get(webid, {"base": 50.0, "units": ""})
    
    items = []
    current = start
    interval = timedelta(seconds=60)
    
    while current <= end and len(items) < maxCount:
        # Realistic sensor simulation with noise
        value = tag_info["base"] + random.gauss(0, 2)
        
        items.append({
            "Timestamp": current.isoformat() + "Z",
            "Value": round(value, 3),
            "UnitsAbbreviation": tag_info["units"],
            "Good": True,
            "Questionable": False,
            "Substituted": False
        })
        current += interval
    
    return {"Items": items}

@app.post("/piwebapi/batch")
def batch_execute(payload: dict):
    """Mock batch controller"""
    responses = []
    for req in payload.get("Requests", []):
        # Simplified: return mock data for each sub-request
        responses.append({
            "Status": 200,
            "Content": {
                "Items": [
                    {
                        "Timestamp": "2025-01-08T10:00:00Z",
                        "Value": 75.0,
                        "Good": True
                    }
                ]
            }
        })
    return {"Responses": responses}

@app.get("/piwebapi/assetdatabases/{dbid}/elements")
def get_db_elements(dbid: str):
    """Mock AF database root elements"""
    db = MOCK_AF_HIERARCHY.get(dbid, {})
    return {"Items": db.get("Elements", [])}

@app.get("/piwebapi/elements/{elementid}/elements")
def get_child_elements(elementid: str):
    """Mock AF child elements - implement recursive lookup"""
    # Simplified for demo
    return {"Items": []}

@app.get("/piwebapi/assetdatabases/{dbid}/eventframes")
def get_event_frames(
    dbid: str,
    startTime: str,
    endTime: str,
    searchMode: str = "Overlapped"
):
    """Mock event frames"""
    return {
        "Items": [
            {
                "WebId": "F1DP-EF1",
                "Name": "Batch-2025-01-08-001",
                "TemplateName": "BatchRunTemplate",
                "StartTime": "2025-01-08T10:00:00Z",
                "EndTime": "2025-01-08T12:30:00Z",
                "PrimaryReferencedElementWebId": "F1DP-Unit1"
            }
        ]
    }

# Run server
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
```

---

## Project Structure

```
pi-lakeflow-connector/
├── README.md                    # Project overview
├── DEVELOPER.md                 # This file
├── TESTER.md                    # Testing specification
├── requirements.txt             # Python dependencies
├── config/
│   ├── connector_config.yaml   # Main configuration
│   └── example_config.yaml     # Example for users
├── src/
│   ├── __init__.py
│   ├── auth/
│   │   ├── __init__.py
│   │   └── pi_auth_manager.py  # Module 1
│   ├── client/
│   │   ├── __init__.py
│   │   └── pi_web_api_client.py # Module 2
│   ├── extractors/
│   │   ├── __init__.py
│   │   ├── timeseries_extractor.py   # Module 3
│   │   ├── af_extractor.py           # Module 4
│   │   └── event_frame_extractor.py  # Module 5
│   ├── checkpoints/
│   │   ├── __init__.py
│   │   └── checkpoint_manager.py     # Module 6
│   ├── writers/
│   │   ├── __init__.py
│   │   └── delta_writer.py          # Module 7
│   └── connector/
│       ├── __init__.py
│       └── pi_lakeflow_connector.py  # Module 8 (main)
├── tests/
│   ├── __init__.py
│   ├── mock_pi_server.py        # FastAPI mock for testing
│   ├── test_auth.py
│   ├── test_client.py
│   ├── test_timeseries.py
│   ├── test_af_extraction.py
│   ├── test_event_frames.py
│   ├── test_checkpoints.py
│   ├── test_writer.py
│   ├── test_integration.py
│   └── fixtures/
│       ├── sample_recorded_response.json
│       ├── sample_af_hierarchy.json
│       └── sample_event_frames.json
├── notebooks/
│   ├── 01_setup_and_test.py     # Databricks notebook for testing
│   ├── 02_run_connector.py      # Run connector manually
│   └── 03_demo.py               # Demo notebook
└── docs/
    ├── architecture.md
    ├── alinta_use_case.md       # Document Alinta validation
    └── api_reference.md
```

---

## Python Dependencies

### File: `requirements.txt`

```
# Core
pyspark>=3.5.0
pandas>=2.0.0
requests>=2.31.0
pyyaml>=6.0

# Authentication
requests-kerberos>=0.14.0

# Testing
pytest>=7.4.0
pytest-mock>=3.12.0
pytest-cov>=4.1.0
fastapi>=0.104.0
uvicorn>=0.24.0

# Utilities
python-dateutil>=2.8.2
tenacity>=8.2.3  # For retry logic

# Development
black>=23.0.0
flake8>=6.1.0
mypy>=1.7.0
```

---

## Key Implementation Details

### Batch Controller Optimization (CRITICAL)

**Why it matters:** Without this, 1,000 tags = 1,000 HTTP requests (5+ minutes)
**With batch controller:** 1,000 tags = 10 batch requests (10-15 seconds)

**Implementation pattern:**
```python
# SLOW - DON'T DO THIS:
for tag in tags:
    response = client.get(f"/streams/{tag}/recorded?...")  # 1000 requests

# FAST - DO THIS:
batch_payload = {
    "Requests": [
        {"Method": "GET", "Resource": f"/streams/{tag}/recorded", ...}
        for tag in tags
    ]
}
response = client.post("/piwebapi/batch", json=batch_payload)  # 1 request!
```

### Error Handling Patterns

**Retry logic:**
```python
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
def extract_with_retry(self, ...):
    # API call here
    pass
```

**Data quality filtering:**
```python
# Filter out bad quality data
df_good = df[df['quality_good'] == True]

# Or flag but keep
df['data_quality'] = df.apply(
    lambda row: 'GOOD' if row['quality_good']
           else 'QUESTIONABLE' if row['quality_questionable']
           else 'BAD',
    axis=1
)
```

---

## Performance Targets

**Benchmark goals:**

| Metric | Target | Validation |
|--------|--------|------------|
| **Tags per batch** | 100 tags in <10 seconds | Alinta use case (30K tags) |
| **Historical backfill** | 10 years × 10K tags in <12 hours | ML training requirement |
| **Incremental run** | 1K tags × 10 min data in <30 sec | Scheduled refresh |
| **AF hierarchy** | 10,000 elements in <2 minutes | Large plant hierarchy |
| **Event frames** | 1 month of events in <1 minute | Batch analytics |

---

## Validation Criteria

### Must Pass (Core Functionality):
- ✓ Connect to PI Web API (auth working)
- ✓ Extract 100 tags using batch controller
- ✓ Incremental ingestion (checkpoint working)
- ✓ Write to Unity Catalog Delta tables
- ✓ Handle errors gracefully (retry logic)

### Should Pass (Enhanced - Alinta Request):
- ✓ Extract AF hierarchy (recursive traversal)
- ✓ Extract Event Frames (time range query)
- ✓ Map AF metadata to Delta schema
- ✓ Performance: 100 tags in <15 seconds

### Could Pass (Polish):
- ⚠ Monitoring dashboard (ingestion metrics)
- ⚠ Late-data detection
- ⚠ WebSocket streaming support

---

## Known Limitations & Workarounds

**Limitation 1: PI Web API maxCount**
- Limit: 150,000 records per request (configurable, often 10,000)
- Workaround: Paging using `extract_with_paging()` method

**Limitation 2: Batch controller URL length**
- Limit: ~2000 characters for URL
- Workaround: Batch size of 100 tags (WebIds ~20 chars each)

**Limitation 3: No native CDC (vs SQL Server connector)**
- PI doesn't have change tracking like SQL Server
- Workaround: Timestamp-based incremental (cursor per tag)

**Limitation 4: Late-arriving data**
- OT networks can buffer data during outages
- Workaround: 48-hour lookback window (query last 2 days on each run)

---

## Success Metrics for Hackathon

**Demo must show:**
1. ✅ Live connection to mock/real PI Server
2. ✅ Batch extraction of 100+ tags (<10 seconds)
3. ✅ AF hierarchy in Unity Catalog (browsable)
4. ✅ Event Frames extracted and queryable
5. ✅ Incremental ingestion (show checkpoint update)
6. ✅ End-to-end: PI → Bronze → Simple Silver query

**Documentation must include:**
1. ✅ Architecture diagram
2. ✅ Alinta use case validation
3. ✅ Setup instructions
4. ✅ Configuration examples
5. ✅ Performance benchmarks

**Code quality:**
1. ✅ Passes all tests (>80% coverage)
2. ✅ Type hints throughout
3. ✅ Docstrings on all functions
4. ✅ Error handling on all API calls
5. ✅ Logging at appropriate levels

---

## Customer Validation Evidence

**Include in docs:**

**Alinta Energy (Feb 2025 Architecture):**
- Slide 6: "CDS commercially viable for 2,000 tags, NOT 30,000"
- Slide 7: "Custom PI Extract (API call)" - exactly what we built
- Slide 9: "Direct feed to ADH2 for raw data (lots of tags) if required"

**Customer Request (April 2024):**
- "If you can internally push for PI AF and PI Event Frame connectivity"
- Evidence connector addresses 8-month-old customer need

**Field Intelligence:**
- Thames Water: "AVEVA not good at Alarms & Events"  
- Brookfield: Using CDS for simplicity (small scale)
- Both patterns valid for different scales/use cases

---

## Next Steps After Hackathon

**v1.1 (Post-Hackathon):**
- Add alarm history extraction
- WebSocket streaming support
- MQTT bridge option

**v2.0 (Community Contributions):**
- Ignition historian support
- Canary Labs connector
- Multi-historian aggregation

**v3.0 (Enterprise Features):**
- Advanced late-data handling
- Data quality monitoring dashboard
- PI Notifications integration
