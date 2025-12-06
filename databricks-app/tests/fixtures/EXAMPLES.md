# Test Fixtures - Integration Examples

Quick reference for using fixtures in unit and integration tests.

## Quick Import

```python
# Option 1: Import specific fixtures
from tests.fixtures import SAMPLE_RECORDED_RESPONSE, SAMPLE_BATCH_RESPONSE

# Option 2: Import all
from tests.fixtures import *

# Option 3: Direct module import
from tests.fixtures.sample_responses import SAMPLE_RECORDED_RESPONSE
```

---

## Example 1: Time-Series Extraction Test

**File:** `tests/test_timeseries.py`

```python
import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock
from tests.fixtures import SAMPLE_RECORDED_RESPONSE, SAMPLE_BATCH_RESPONSE
from src.extractors.timeseries_extractor import TimeSeriesExtractor

class TestTimeSeriesExtractor:

    @pytest.fixture
    def mock_client(self):
        return MagicMock()

    @pytest.fixture
    def extractor(self, mock_client):
        return TimeSeriesExtractor(mock_client)

    def test_single_tag_extraction(self, extractor, mock_client):
        """Test extracting recorded data for single tag"""
        # Setup mock to return sample data
        mock_client.batch_execute.return_value = {
            "Responses": [{"Status": 200, "Content": SAMPLE_RECORDED_RESPONSE}]
        }

        # Execute extraction
        df = extractor.extract_recorded_data(
            tag_webids=["F1DP-Tag1"],
            start_time=datetime(2025, 1, 8, 10, 0),
            end_time=datetime(2025, 1, 8, 11, 0)
        )

        # Validate results
        assert not df.empty
        assert len(df) == 5  # 5 records in SAMPLE_RECORDED_RESPONSE
        assert 'tag_webid' in df.columns
        assert 'timestamp' in df.columns
        assert 'value' in df.columns
        assert df['value'].min() == 75.3
        assert df['value'].max() == 76.1

    def test_multiple_tags_batch(self, extractor, mock_client):
        """Test batch extraction with multiple tags"""
        # Create batch response for 3 tags
        mock_client.batch_execute.return_value = SAMPLE_BATCH_RESPONSE

        # Extract 3 tags at once
        df = extractor.extract_recorded_data(
            tag_webids=["F1DP-Tag1", "F1DP-Tag2", "F1DP-Tag3"],
            start_time=datetime(2025, 1, 8, 10, 0),
            end_time=datetime(2025, 1, 8, 11, 0)
        )

        # Verify all tags extracted
        assert not df.empty
        assert df['tag_webid'].nunique() == 3

        # Verify batch API was called (not 3 individual calls)
        assert mock_client.batch_execute.call_count == 1

    def test_quality_flag_parsing(self, extractor, mock_client):
        """Test quality flags are correctly parsed"""
        from tests.fixtures import SAMPLE_RECORDED_RESPONSE_WITH_QUALITY_ISSUES

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

        # Check quality flags
        assert df.iloc[0]['quality_good'] == True
        assert df.iloc[1]['quality_good'] == False
        assert df.iloc[2]['quality_good'] == True
        assert df.iloc[3]['quality_good'] == False
```

---

## Example 2: AF Hierarchy Extraction Test

**File:** `tests/test_af_extraction.py`

```python
import pytest
from tests.fixtures import SAMPLE_AF_ELEMENT, SAMPLE_AF_ELEMENTS_LIST, SAMPLE_DEEP_HIERARCHY
from src.extractors.af_extractor import AFHierarchyExtractor

class TestAFHierarchyExtractor:

    @pytest.fixture
    def mock_client(self):
        return MagicMock()

    @pytest.fixture
    def extractor(self, mock_client):
        return AFHierarchyExtractor(mock_client)

    def test_parse_single_element(self):
        """Test parsing AF element structure"""
        element = SAMPLE_AF_ELEMENT

        assert element['Name'] == 'Pump-101'
        assert element['TemplateName'] == 'PumpTemplate'
        assert element['Type'] == 'Element'
        assert 'Rotating Equipment' in element['CategoryNames']
        assert 'Critical' in element['CategoryNames']
        assert len(element['Attributes']) == 2
        assert element['Attributes'][0]['Name'] == 'Speed'
        assert element['Attributes'][1]['Name'] == 'Vibration'

    def test_extract_multiple_elements(self, extractor, mock_client):
        """Test extracting list of elements"""
        mock_client.get.return_value.json.return_value = SAMPLE_AF_ELEMENTS_LIST

        elements = extractor.get_elements("F1DP-Site1")

        assert len(elements) == 3
        assert elements[0]['Name'] == 'Unit-1'
        assert elements[1]['Name'] == 'Unit-2'
        assert elements[2]['Name'] == 'Unit-3'

    def test_deep_hierarchy_traversal(self, extractor, mock_client):
        """Test recursive traversal of 3-level hierarchy"""
        mock_client.get.return_value.json.return_value = SAMPLE_DEEP_HIERARCHY

        hierarchy = extractor.extract_hierarchy("F1DP-DB1")

        # Should extract all 3 levels
        assert len(hierarchy) == 3
        assert 'Sydney_Plant' in hierarchy[0]['Name']
        assert 'Unit-1' in hierarchy[1]['Name']
        assert 'Pump-101' in hierarchy[2]['Name']

        # Check paths are constructed correctly
        assert hierarchy[0]['Path'] == '\\\\Production Database\\Sydney_Plant'
        assert hierarchy[1]['Path'] == '\\\\Production Database\\Sydney_Plant\\Unit-1'
```

---

## Example 3: Event Frame Extraction Test

**File:** `tests/test_event_frames.py`

```python
import pytest
from datetime import datetime
from tests.fixtures import (
    SAMPLE_EVENT_FRAME,
    SAMPLE_EVENT_FRAMES_LIST,
    SAMPLE_ACTIVE_EVENT_FRAME
)
from src.extractors.event_frame_extractor import EventFrameExtractor

class TestEventFrameExtractor:

    @pytest.fixture
    def mock_client(self):
        return MagicMock()

    @pytest.fixture
    def extractor(self, mock_client):
        return EventFrameExtractor(mock_client)

    def test_extract_single_event(self):
        """Test extracting event frame data"""
        event = SAMPLE_EVENT_FRAME

        assert event['Name'] == 'Batch-2025-01-08-001'
        assert event['TemplateName'] == 'BatchRunTemplate'
        assert event['IsConfirmed'] == True
        assert event['IsAcknowledged'] == False

        # Parse times
        start = datetime.fromisoformat(
            event['StartTime'].replace('Z', '+00:00')
        )
        end = datetime.fromisoformat(
            event['EndTime'].replace('Z', '+00:00')
        )
        duration_minutes = (end - start).total_seconds() / 60

        assert duration_minutes == 150.0  # 2.5 hours

    def test_extract_multiple_events(self, extractor, mock_client):
        """Test extracting list of events"""
        mock_client.get.return_value.json.return_value = SAMPLE_EVENT_FRAMES_LIST

        df = extractor.extract_event_frames(
            database_webid="F1DP-DB1",
            start_time=datetime(2025, 1, 8),
            end_time=datetime(2025, 1, 9)
        )

        assert len(df) == 3
        assert 'Batch-2025-01-08-001' in df['event_name'].values
        assert 'Batch-2025-01-08-002' in df['event_name'].values
        assert 'Maintenance-2025-01-08-001' in df['event_name'].values

    def test_active_event_no_end_time(self):
        """Test handling of active events (no EndTime)"""
        event = SAMPLE_ACTIVE_EVENT_FRAME

        assert event['StartTime'] is not None
        assert event['EndTime'] is None  # Active event
        assert event['IsConfirmed'] == False
        assert event['IsAcknowledged'] == False
```

---

## Example 4: Batch Processing Test

**File:** `tests/test_batch_processing.py`

```python
import pytest
from tests.fixtures import (
    SAMPLE_BATCH_RESPONSE,
    SAMPLE_BATCH_RESPONSE_WITH_ERRORS
)
from src.client.pi_web_api_client import PIWebAPIClient

class TestBatchProcessing:

    @pytest.fixture
    def mock_session(self):
        return MagicMock()

    @pytest.fixture
    def client(self, mock_session):
        client = PIWebAPIClient("http://localhost/piwebapi")
        client.session = mock_session
        return client

    def test_batch_all_success(self, client, mock_session):
        """Test batch request with all successful responses"""
        mock_session.post.return_value.json.return_value = SAMPLE_BATCH_RESPONSE

        result = client.batch_execute([
            {"Method": "GET", "Resource": "/streams/F1DP-Tag1/recorded"},
            {"Method": "GET", "Resource": "/streams/F1DP-Tag2/recorded"},
            {"Method": "GET", "Resource": "/streams/F1DP-Tag3/recorded"}
        ])

        # All 3 should succeed
        assert len(result['Responses']) == 3
        assert all(r['Status'] == 200 for r in result['Responses'])

    def test_batch_with_errors(self, client, mock_session):
        """Test batch request with mixed success/error responses"""
        mock_session.post.return_value.json.return_value = SAMPLE_BATCH_RESPONSE_WITH_ERRORS

        result = client.batch_execute([
            {"Method": "GET", "Resource": "/streams/F1DP-Tag1/recorded"},
            {"Method": "GET", "Resource": "/streams/F1DP-Invalid/recorded"},
            {"Method": "GET", "Resource": "/streams/F1DP-Tag3/recorded"},
            {"Method": "GET", "Resource": "/streams/F1DP-BadTag/recorded"}
        ])

        # Check mixed results
        assert len(result['Responses']) == 4
        assert result['Responses'][0]['Status'] == 200  # Success
        assert result['Responses'][1]['Status'] == 404  # Not found
        assert result['Responses'][2]['Status'] == 200  # Success
        assert result['Responses'][3]['Status'] == 500  # Server error

        # Process only successful responses
        successful = [r for r in result['Responses'] if r['Status'] == 200]
        assert len(successful) == 2
```

---

## Example 5: Error Handling Test

**File:** `tests/test_error_handling.py`

```python
import pytest
from tests.fixtures import (
    SAMPLE_ERROR_RESPONSE_404,
    SAMPLE_ERROR_RESPONSE_500,
    SAMPLE_AUTH_CHALLENGE
)
from src.client.pi_web_api_client import PIWebAPIClient

class TestErrorHandling:

    @pytest.fixture
    def mock_session(self):
        return MagicMock()

    @pytest.fixture
    def client(self, mock_session):
        client = PIWebAPIClient("http://localhost/piwebapi")
        client.session = mock_session
        return client

    def test_handle_404_not_found(self, client, mock_session):
        """Test handling of 404 Not Found error"""
        response = MagicMock()
        response.status_code = 404
        response.json.return_value = SAMPLE_ERROR_RESPONSE_404
        mock_session.get.return_value = response

        with pytest.raises(requests.exceptions.HTTPError):
            client.get("/streams/F1DP-InvalidTag/recorded")

    def test_handle_500_server_error(self, client, mock_session):
        """Test handling of 500 Internal Server Error"""
        response = MagicMock()
        response.status_code = 500
        response.json.return_value = SAMPLE_ERROR_RESPONSE_500
        mock_session.get.return_value = response

        with pytest.raises(requests.exceptions.HTTPError):
            client.get("/streams/F1DP-Tag1/recorded")

    def test_handle_auth_challenge(self, client, mock_session):
        """Test handling of 401 authentication challenge"""
        response = MagicMock()
        response.status_code = 401
        response.headers = SAMPLE_AUTH_CHALLENGE['Headers']
        mock_session.get.return_value = response

        with pytest.raises(requests.exceptions.HTTPError):
            client.get("/piwebapi/assetdatabases")
```

---

## Example 6: Integration Test with Mock Server

**File:** `tests/test_integration.py`

```python
import pytest
from datetime import datetime
from tests.fixtures import SAMPLE_BATCH_RESPONSE
from src.connector.pi_lakeflow_connector import PILakeflowConnector

class TestIntegration:

    @pytest.fixture
    def config(self):
        return {
            'pi_web_api_url': 'http://localhost:8000/piwebapi',
            'auth': {
                'type': 'basic',
                'username': 'test',
                'password': 'test'
            },
            'catalog': 'test_catalog',
            'schema': 'bronze',
            'tags': ['F1DP-Tag1', 'F1DP-Tag2', 'F1DP-Tag3'],
            'af_database_id': 'F1DP-DB1',
            'include_event_frames': True
        }

    @pytest.fixture
    def connector(self, config, mock_client):
        return PILakeflowConnector(config)

    def test_full_extraction_with_fixtures(self, connector, mock_client):
        """Test complete extraction using fixtures"""
        # Mock the batch response
        mock_client.batch_execute.return_value = SAMPLE_BATCH_RESPONSE

        # Run extraction
        connector.run()

        # Verify tables are populated
        # (In real test, these would check actual Spark tables)
        assert connector.ts_extractor.last_run_time is not None
        assert connector.af_extractor.last_run_time is not None
```

---

## Using Fixtures with Parametrized Tests

```python
import pytest
from tests.fixtures import SAMPLE_RECORDED_RESPONSE, SAMPLE_EMPTY_RESPONSE

@pytest.mark.parametrize("response,expected_count", [
    (SAMPLE_RECORDED_RESPONSE, 5),
    (SAMPLE_EMPTY_RESPONSE, 0),
])
def test_response_parsing(response, expected_count):
    """Test parsing different response types"""
    items = response.get('Items', [])
    assert len(items) == expected_count
```

---

## Best Practices

1. **Always use fixtures for mocking**
   ```python
   # Good
   mock_client.batch_execute.return_value = SAMPLE_BATCH_RESPONSE

   # Avoid hardcoding responses
   mock_client.batch_execute.return_value = {
       "Responses": [{"Status": 200, "Content": {...}}]
   }
   ```

2. **Combine fixtures for complex scenarios**
   ```python
   from tests.fixtures import SAMPLE_BATCH_RESPONSE_WITH_ERRORS

   # Use existing fixture for batch with errors
   mock_client.batch_execute.return_value = SAMPLE_BATCH_RESPONSE_WITH_ERRORS
   ```

3. **Document fixture usage in test docstrings**
   ```python
   def test_example(self):
       """
       Test something using SAMPLE_RECORDED_RESPONSE.

       Fixture provides: 5 temperature readings, 1-min intervals, mixed quality
       """
       pass
   ```

4. **Extend fixtures only when necessary**
   ```python
   # Modify fixture for specific test
   response = deepcopy(SAMPLE_RECORDED_RESPONSE)
   response['Items'][0]['Value'] = 999.9  # Edge case
   mock_client.batch_execute.return_value = {"Responses": [{"Status": 200, "Content": response}]}
   ```

---

## Running Tests with Fixtures

```bash
# Run all time-series tests
pytest tests/test_timeseries.py -v

# Run specific test
pytest tests/test_timeseries.py::TestTimeSeriesExtractor::test_single_tag_extraction -v

# Run with detailed output
pytest tests/test_timeseries.py -vv -s

# Run with coverage
pytest tests/test_timeseries.py --cov=src.extractors --cov-report=html
```

---

## Troubleshooting Fixture Usage

**Issue:** Import error
```python
# Solution: Ensure tests directory has __init__.py
touch tests/__init__.py
```

**Issue:** Fixture data doesn't match API changes
```python
# Solution: Update fixture in sample_responses.py
# Run: python validate_fixtures.py to compare with real PI
```

**Issue:** Test passes in isolation but fails in suite
```python
# Solution: Use deepcopy for fixture modifications
from copy import deepcopy
modified_response = deepcopy(SAMPLE_RECORDED_RESPONSE)
```
