"""
Test script for mock PI Web API server
Verifies all endpoints work correctly
"""

import requests
import json
from datetime import datetime, timedelta

BASE_URL = "http://localhost:8000"

def test_root_endpoint():
    """Test PI Web API root"""
    print("Testing /piwebapi endpoint...")
    response = requests.get(f"{BASE_URL}/piwebapi")
    assert response.status_code == 200
    data = response.json()
    print(f"✓ Version: {data['Version']}")
    return data

def test_dataservers():
    """Test data servers listing"""
    print("\nTesting /piwebapi/dataservers...")
    response = requests.get(f"{BASE_URL}/piwebapi/dataservers")
    assert response.status_code == 200
    data = response.json()
    assert len(data['Items']) > 0
    print(f"✓ Found {len(data['Items'])} data server(s)")
    return data['Items'][0]

def test_list_points(server_webid):
    """Test listing PI points"""
    print(f"\nTesting /piwebapi/dataservers/{server_webid}/points...")
    response = requests.get(
        f"{BASE_URL}/piwebapi/dataservers/{server_webid}/points",
        params={"maxCount": 10}
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data['Items']) > 0
    print(f"✓ Found {len(data['Items'])} tags")
    return data['Items']

def test_recorded_data(tag_webid):
    """Test time-series data extraction"""
    print(f"\nTesting /piwebapi/streams/{tag_webid}/recorded...")

    end_time = datetime.now()
    start_time = end_time - timedelta(hours=1)

    response = requests.get(
        f"{BASE_URL}/piwebapi/streams/{tag_webid}/recorded",
        params={
            "startTime": start_time.isoformat() + "Z",
            "endTime": end_time.isoformat() + "Z",
            "maxCount": 100
        }
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data['Items']) > 0
    print(f"✓ Retrieved {len(data['Items'])} data points")
    print(f"  Sample: {data['Items'][0]['Timestamp']} = {data['Items'][0]['Value']} {data['Items'][0]['UnitsAbbreviation']}")
    return data

def test_batch_controller(tag_webids):
    """Test batch controller (CRITICAL for performance)"""
    print(f"\nTesting /piwebapi/batch with {len(tag_webids)} tags...")

    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=30)

    # Build batch request
    batch_requests = []
    for webid in tag_webids:
        batch_requests.append({
            "Method": "GET",
            "Resource": f"/streams/{webid}/recorded",
            "Parameters": {
                "startTime": start_time.isoformat() + "Z",
                "endTime": end_time.isoformat() + "Z",
                "maxCount": "50"
            }
        })

    response = requests.post(
        f"{BASE_URL}/piwebapi/batch",
        json={"Requests": batch_requests}
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data['Responses']) == len(tag_webids)

    successful = sum(1 for r in data['Responses'] if r['Status'] == 200)
    print(f"✓ Batch request successful: {successful}/{len(tag_webids)} responses OK")
    return data

def test_asset_databases():
    """Test AF database listing"""
    print("\nTesting /piwebapi/assetdatabases...")
    response = requests.get(f"{BASE_URL}/piwebapi/assetdatabases")
    assert response.status_code == 200
    data = response.json()
    assert len(data['Items']) > 0
    print(f"✓ Found {len(data['Items'])} AF database(s)")
    return data['Items'][0]

def test_af_hierarchy(db_webid):
    """Test AF hierarchy extraction"""
    print(f"\nTesting AF hierarchy for {db_webid}...")

    # Get root elements
    response = requests.get(f"{BASE_URL}/piwebapi/assetdatabases/{db_webid}/elements")
    assert response.status_code == 200
    data = response.json()
    root_elements = data['Items']
    print(f"✓ Found {len(root_elements)} root elements")

    if root_elements:
        # Get child elements
        element_webid = root_elements[0]['WebId']
        response = requests.get(f"{BASE_URL}/piwebapi/elements/{element_webid}/elements")
        assert response.status_code == 200
        children = response.json()['Items']
        print(f"✓ First element has {len(children)} children")

        # Get element attributes
        response = requests.get(f"{BASE_URL}/piwebapi/elements/{element_webid}/attributes")
        assert response.status_code == 200
        attributes = response.json()['Items']
        print(f"✓ Element has {len(attributes)} attributes")

    return root_elements

def test_event_frames(db_webid):
    """Test event frame extraction"""
    print(f"\nTesting event frames for {db_webid}...")

    end_time = datetime.now()
    start_time = end_time - timedelta(days=7)

    response = requests.get(
        f"{BASE_URL}/piwebapi/assetdatabases/{db_webid}/eventframes",
        params={
            "startTime": start_time.isoformat() + "Z",
            "endTime": end_time.isoformat() + "Z",
            "searchMode": "Overlapped"
        }
    )
    assert response.status_code == 200
    data = response.json()
    event_frames = data['Items']
    print(f"✓ Found {len(event_frames)} event frames in last 7 days")

    if event_frames:
        # Get event frame attributes
        ef_webid = event_frames[0]['WebId']
        response = requests.get(f"{BASE_URL}/piwebapi/eventframes/{ef_webid}/attributes")
        assert response.status_code == 200
        attributes = response.json()['Items']
        print(f"✓ Event frame has {len(attributes)} attributes")
        if attributes:
            print(f"  Sample attribute: {attributes[0]['Name']} = {attributes[0].get('Value')}")

    return event_frames

def test_health():
    """Test health check"""
    print("\nTesting /health endpoint...")
    response = requests.get(f"{BASE_URL}/health")
    assert response.status_code == 200
    data = response.json()
    print(f"✓ Server status: {data['status']}")
    print(f"  Mock tags: {data['mock_tags']}")
    print(f"  Mock event frames: {data['mock_event_frames']}")
    return data

def run_all_tests():
    """Run all tests"""
    print("=" * 80)
    print("Mock PI Web API Server - Test Suite")
    print("=" * 80)

    try:
        # Basic endpoints
        root_data = test_root_endpoint()
        health_data = test_health()

        # Time-series data
        server = test_dataservers()
        tags = test_list_points(server['WebId'])
        tag_webids = [tag['WebId'] for tag in tags[:5]]

        recorded_data = test_recorded_data(tag_webids[0])
        batch_data = test_batch_controller(tag_webids)

        # AF hierarchy
        db = test_asset_databases()
        af_elements = test_af_hierarchy(db['WebId'])

        # Event frames
        event_frames = test_event_frames(db['WebId'])

        print("\n" + "=" * 80)
        print("✅ ALL TESTS PASSED!")
        print("=" * 80)
        print("\nMock PI Web API Server is fully functional and ready for development.")
        print(f"\nSummary:")
        print(f"  - {health_data['mock_tags']} tags available")
        print(f"  - {len(af_elements)} root AF elements")
        print(f"  - {health_data['mock_event_frames']} event frames")
        print(f"  - Batch controller working (tested with {len(tag_webids)} tags)")

    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        return False
    except requests.exceptions.ConnectionError:
        print(f"\n❌ CONNECTION ERROR: Mock server not running at {BASE_URL}")
        print("Start the server with: python tests/mock_pi_server.py")
        return False
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True

if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
