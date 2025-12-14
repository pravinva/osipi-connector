#!/usr/bin/env python3
"""
Authentication Test Script

Tests if the Bearer token authentication fix is working correctly.
Simulates what the pipeline does when authenticating to the Databricks App.
"""

import requests
import sys
import json
from typing import Dict, Any

# Configuration
PI_WEB_API_URL = "https://osipi-webserver-1444828305810485.aws.databricksapps.com/piwebapi"
PAT_TOKEN = "YOUR_PAT_TOKEN_HERE"  # Replace with your actual PAT token


def test_authentication() -> Dict[str, Any]:
    """
    Test authentication to Databricks App using Bearer token.
    This simulates what the pipeline does.
    """

    print("=" * 80)
    print("AUTHENTICATION TEST - Bearer Token to Databricks App")
    print("=" * 80)

    # Prepare auth headers (same as pipeline code)
    auth_headers = {
        'Authorization': f'Bearer {PAT_TOKEN}',
        'Content-Type': 'application/json'
    }

    # Test 1: Simple GET request to dataservers endpoint
    print("\n[Test 1] GET /piwebapi/dataservers")
    print("-" * 80)

    try:
        response = requests.get(
            f"{PI_WEB_API_URL}/dataservers",
            headers=auth_headers,
            timeout=10
        )

        print(f"Status Code: {response.status_code} {response.reason}")

        if response.status_code == 200:
            print("✓ SUCCESS - Authentication working!")
            try:
                data = response.json()
                print(f"Response preview: {json.dumps(data, indent=2)[:300]}...")
            except json.JSONDecodeError:
                print(f"Response (non-JSON): {response.text[:200]}...")
        elif response.status_code == 401:
            print("✗ FAILED - 401 Unauthorized")
            print("The Bearer token was rejected by the Databricks App")
            print(f"Response: {response.text[:500]}")
            return {"success": False, "error": "401 Unauthorized"}
        else:
            print(f"⚠ Unexpected status code: {response.status_code}")
            print(f"Response: {response.text[:500]}")
            return {"success": False, "error": f"HTTP {response.status_code}"}

    except requests.exceptions.RequestException as e:
        print(f"✗ Request failed: {e}")
        return {"success": False, "error": str(e)}

    # Test 2: Batch request (what the pipeline actually uses)
    print("\n[Test 2] POST /piwebapi/batch (simulating pipeline request)")
    print("-" * 80)

    batch_payload = {
        "requests": [
            {
                "Method": "GET",
                "Resource": f"{PI_WEB_API_URL}/dataservers"
            }
        ]
    }

    try:
        response = requests.post(
            f"{PI_WEB_API_URL}/batch",
            headers=auth_headers,
            json=batch_payload,
            timeout=30
        )

        print(f"Status Code: {response.status_code} {response.reason}")

        if response.status_code == 200:
            print("✓ SUCCESS - Batch request authenticated!")
            data = response.json()
            print(f"Response preview: {json.dumps(data, indent=2)[:300]}...")
            return {"success": True}
        elif response.status_code == 401:
            print("✗ FAILED - 401 Unauthorized")
            print("The Bearer token was rejected for batch requests")
            print(f"Response: {response.text[:500]}")
            return {"success": False, "error": "401 Unauthorized on batch"}
        else:
            print(f"⚠ Unexpected status code: {response.status_code}")
            print(f"Response: {response.text[:500]}")
            return {"success": False, "error": f"HTTP {response.status_code}"}

    except requests.exceptions.RequestException as e:
        print(f"✗ Request failed: {e}")
        return {"success": False, "error": str(e)}


def main():
    """Main test execution"""

    result = test_authentication()

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    if result.get("success"):
        print("\n✓✓✓ ALL TESTS PASSED ✓✓✓")
        print("\nThe Bearer token authentication is working correctly.")
        print("The pipeline should be able to authenticate to the Databricks App.")
        print("\nNext steps:")
        print("  1. Deploy the updated pipeline code to Databricks workspace")
        print("  2. Trigger a test pipeline run")
        print("  3. Verify pipeline completes without 401 errors")
        sys.exit(0)
    else:
        print("\n✗✗✗ TESTS FAILED ✗✗✗")
        print(f"\nError: {result.get('error')}")
        print("\nPossible issues:")
        print("  1. PAT token is expired or invalid")
        print("  2. User doesn't have permission to access the Databricks App")
        print("  3. App configuration has changed")
        print("\nRecommended actions:")
        print("  1. Generate a new PAT token in Databricks UI")
        print("  2. Verify user has 'CAN USE' permission on osipi-webserver App")
        print("  3. Update the PAT token in sp-osipi secrets scope")
        sys.exit(1)


if __name__ == "__main__":
    main()
