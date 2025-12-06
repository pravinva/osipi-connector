# PI Web API Test Fixtures

This directory contains realistic, production-quality fixture data for unit and integration testing of the PI Lakeflow Connector.

## File Overview

### `sample_responses.py`
Contains 18 fixture definitions that simulate actual PI Web API responses.

## Core Fixtures (Required for Basic Tests)

### 1. SAMPLE_RECORDED_RESPONSE
Time-series data from a single PI Point (temperature sensor).

**Use cases:**
- Testing time-series data extraction
- Validating timestamp parsing
- Quality flag handling
- Unit conversion

**Structure:**
- 5 data points with 1-minute intervals
- Temperature values in degC (75.3 - 76.1)
- Quality flags: Good, Questionable, Substituted
- ISO 8601 UTC timestamps

**Example usage:**
```python
from tests.fixtures import SAMPLE_RECORDED_RESPONSE

df = extractor.extract_recorded_data(
    tag_webids=["F1DP-Tag1"],
    start_time=datetime(2025, 1, 8),
    end_time=datetime(2025, 1, 8, 1, 0)
)
# Verify parsing matches SAMPLE_RECORDED_RESPONSE structure
```

---

### 2. SAMPLE_AF_ELEMENT
Asset Framework element representing a pump equipment.

**Use cases:**
- AF hierarchy extraction tests
- Element attribute discovery
- Template name validation
- Category filtering

**Structure:**
- WebId: F1DP-Element-Pump101
- Name: Pump-101
- Template: PumpTemplate
- Categories: ["Rotating Equipment", "Critical"]
- 2 attributes: Speed, Vibration

**Example usage:**
```python
from tests.fixtures import SAMPLE_AF_ELEMENT

# Test element parsing
assert element['Name'] == 'Pump-101'
assert 'Critical' in element['CategoryNames']
assert len(element['Attributes']) == 2
```

---

### 3. SAMPLE_EVENT_FRAME
Production batch run event with start/end times and product metadata.

**Use cases:**
- Event frame extraction tests
- Duration calculation (2.5 hours)
- Template filtering
- Active/completed event distinction

**Structure:**
- WebId: F1DP-EF-Batch001
- StartTime: 2025-01-08T10:00:00Z
- EndTime: 2025-01-08T12:30:00Z
- Duration: 150 minutes
- ProductGrade: A (extended property)
- Operator: John Smith

**Example usage:**
```python
from tests.fixtures import SAMPLE_EVENT_FRAME

# Test event duration calculation
start = datetime.fromisoformat(SAMPLE_EVENT_FRAME['StartTime'].replace('Z', '+00:00'))
end = datetime.fromisoformat(SAMPLE_EVENT_FRAME['EndTime'].replace('Z', '+00:00'))
duration_minutes = (end - start).total_seconds() / 60
assert duration_minutes == 150.0
```

---

### 4. SAMPLE_BATCH_RESPONSE
Batch controller response containing 3 time-series requests.

**Use cases:**
- Batch controller pattern testing
- Multi-tag extraction validation
- Response parsing (3 successful responses)
- Error handling in batch scenarios

**Structure:**
- 3 Responses with Status 200
- Mix of degC and bar units
- Demonstrates parallel request efficiency

**Example usage:**
```python
from tests.fixtures import SAMPLE_BATCH_RESPONSE

mock_client.batch_execute.return_value = SAMPLE_BATCH_RESPONSE
df = extractor.extract_recorded_data(
    tag_webids=["F1DP-Tag1", "F1DP-Tag2", "F1DP-Tag3"],
    start_time=datetime(2025, 1, 8),
    end_time=datetime(2025, 1, 8, 1, 0)
)
assert df['tag_webid'].nunique() == 3
```

---

## Extended Fixtures (For Advanced Scenarios)

### 5. SAMPLE_AF_DATABASE
Asset Framework database metadata.

**Purpose:** Listing and selecting AF databases
**Fields:** Name, Description, Element/Event counts

### 6. SAMPLE_AF_ELEMENTS_LIST
Multiple AF elements (3 units under a plant).

**Purpose:** Recursive hierarchy extraction
**Pattern:** Parent → Children relationship

### 7. SAMPLE_EVENT_FRAMES_LIST
3 events with mixed types (Production + Maintenance).

**Purpose:** Event filtering and templating
**Features:** Multiple templates, different elements

### 8. SAMPLE_ATTRIBUTES_LIST
3 element attributes (Temperature, Pressure, Flow).

**Purpose:** Attribute discovery and mapping
**Units:** degC, bar, m3/h (realistic mix)

### 9. SAMPLE_ERROR_RESPONSE_404
Not Found error response.

**Purpose:** Error handling for missing resources
**Use:** Test graceful degradation

### 10. SAMPLE_ERROR_RESPONSE_500
Internal Server Error response.

**Purpose:** Test retry logic on server errors
**Use:** Transient failure scenarios

### 11. SAMPLE_AUTH_CHALLENGE
HTTP 401 authentication challenge.

**Purpose:** Auth failure detection
**Header:** WWW-Authenticate: Basic

### 12. SAMPLE_PAGED_RESPONSE
Large result set with pagination link.

**Purpose:** Testing paging mechanism
**Pattern:** 100 records + Links.Next

### 13. SAMPLE_STREAM_RESPONSE
Real-time stream data (2 tags).

**Purpose:** Stream subscription testing
**Structure:** Multiple values, same timestamp

### 14. SAMPLE_ELEMENT_TEMPLATE
Element template definition.

**Purpose:** Template structure discovery
**Features:** Base template, attribute templates

### 15. SAMPLE_EMPTY_RESPONSE
Empty result set (no items).

**Purpose:** Edge case testing (no data scenario)
**Use:** Boundary condition validation

### 16. SAMPLE_DEEP_HIERARCHY
3-level AF hierarchy (Site → Unit → Equipment).

**Purpose:** Deep recursion testing
**Includes:** Full paths for each level

### 17. SAMPLE_RECORDED_RESPONSE_WITH_QUALITY_ISSUES
Time-series with mixed data quality.

**Purpose:** Quality flag validation
**Issues:**
- Null values
- Questionable flag set
- Substituted data

### 18. SAMPLE_SUMMARY_RESPONSE
Summarized/compressed data (statistics).

**Purpose:** Summary statistics testing
**Metrics:** Average, Max, Min, StdDev, Count

### 19. SAMPLE_ACTIVE_EVENT_FRAME
Event frame still running (no EndTime).

**Purpose:** Active event handling
**Feature:** Null EndTime value

### 20. SAMPLE_BATCH_RESPONSE_WITH_ERRORS
Batch with 2 successes + 2 errors (404, 500).

**Purpose:** Batch error handling
**Pattern:** Mixed Status responses

---

## Usage Patterns

### Unit Tests - Time Series Extraction

```python
import pytest
from tests.fixtures import SAMPLE_RECORDED_RESPONSE, SAMPLE_BATCH_RESPONSE
from src.extractors.timeseries_extractor import TimeSeriesExtractor

class TestTimeSeriesExtractor:
    def test_extract_single_tag(self, mock_client):
        mock_client.batch_execute.return_value = {
            "Responses": [{"Status": 200, "Content": SAMPLE_RECORDED_RESPONSE}]
        }

        df = TimeSeriesExtractor(mock_client).extract_recorded_data(
            tag_webids=["F1DP-Tag1"],
            start_time=datetime(2025, 1, 8, 10, 0),
            end_time=datetime(2025, 1, 8, 11, 0)
        )

        assert len(df) == 5  # 5 records in SAMPLE_RECORDED_RESPONSE
        assert df['tag_webid'].iloc[0] == 'F1DP-Tag1'
        assert df['value'].min() == 75.3
        assert df['value'].max() == 76.1
```

### Unit Tests - AF Extraction

```python
from tests.fixtures import SAMPLE_AF_ELEMENT, SAMPLE_AF_ELEMENTS_LIST
from src.extractors.af_extractor import AFHierarchyExtractor

class TestAFExtraction:
    def test_parse_element(self):
        element = SAMPLE_AF_ELEMENT

        assert element['Name'] == 'Pump-101'
        assert element['TemplateName'] == 'PumpTemplate'
        assert 'Critical' in element['CategoryNames']
        assert len(element['Attributes']) == 2
```

### Unit Tests - Event Frames

```python
from tests.fixtures import SAMPLE_EVENT_FRAME, SAMPLE_ACTIVE_EVENT_FRAME
from src.extractors.event_frame_extractor import EventFrameExtractor

class TestEventFrames:
    def test_duration_calculation(self):
        event = SAMPLE_EVENT_FRAME
        start = datetime.fromisoformat(event['StartTime'].replace('Z', '+00:00'))
        end = datetime.fromisoformat(event['EndTime'].replace('Z', '+00:00'))
        duration = (end - start).total_seconds() / 60

        assert duration == 150.0  # 2.5 hours

    def test_active_event_handling(self):
        event = SAMPLE_ACTIVE_EVENT_FRAME

        assert event['EndTime'] is None
        assert event['IsConfirmed'] == False
```

### Integration Tests - Batch Handling

```python
from tests.fixtures import SAMPLE_BATCH_RESPONSE_WITH_ERRORS

class TestBatchHandling:
    def test_partial_batch_failure(self, connector, mock_client):
        mock_client.batch_execute.return_value = SAMPLE_BATCH_RESPONSE_WITH_ERRORS

        result = connector.run()

        # Should process 2 successful responses
        assert result['successful'] == 2
        assert result['failed'] == 2
        assert result['total'] == 4
```

---

## Fixture Quality Standards

All fixtures meet these standards:

1. **Realistic Data**: Values, units, and formats match real PI installations
2. **Complete Structure**: All required fields for each API endpoint
3. **ISO 8601 Timestamps**: Proper UTC formatting with 'Z' suffix
4. **Valid WebIds**: Follow PI naming conventions (F1DP-*)
5. **Error Patterns**: Include 404s, 500s, auth challenges
6. **Edge Cases**: Empty results, null values, quality issues
7. **Scale-appropriate**: Sample sizes match typical batch operations

---

## Importing Fixtures

### Option 1: From Module

```python
from tests.fixtures.sample_responses import SAMPLE_RECORDED_RESPONSE
```

### Option 2: From Package

```python
from tests.fixtures import SAMPLE_RECORDED_RESPONSE
```

### Option 3: Import All

```python
from tests.fixtures import *
```

---

## Adding New Fixtures

When adding new fixtures:

1. Add definition to `sample_responses.py`
2. Update `__all__` in `__init__.py`
3. Add usage example in this README
4. Ensure realistic data (use actual PI values as reference)
5. Include comprehensive docstring in fixture definition

---

## Testing Against Real PI (Optional)

For production validation, these fixtures can be compared against actual PI responses:

```bash
# Export real PI data to JSON
python export_pi_responses.py > real_responses.json

# Compare with fixtures
python validate_fixtures.py real_responses.json
```

---

## Version History

**v1.0** (2025-01-08)
- Initial 20 fixture definitions
- Core 4 fixtures: Recorded Data, AF Element, Event Frame, Batch Response
- 16 extended fixtures for edge cases
- Full test examples and usage patterns
