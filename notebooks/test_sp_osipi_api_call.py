# Databricks notebook source
"""
Test sp-osipi Service Principal API Authentication

This script tests if sp-osipi Service Principal can successfully authenticate
and make API calls to the Databricks App (osipi-webserver).
"""

from databricks.sdk import WorkspaceClient
import requests
import json

# COMMAND ----------
# Step 1: Get sp-osipi credentials from secrets

print("=" * 80)
print("STEP 1: Retrieving sp-osipi credentials from secrets")
print("=" * 80)

try:
    CLIENT_ID = dbutils.secrets.get(scope="sp-osipi", key="sp-client-id")
    CLIENT_SECRET = dbutils.secrets.get(scope="sp-osipi", key="sp-client-secret")
    print("✓ Successfully retrieved sp-client-id and sp-client-secret from sp-osipi scope")
    print(f"  Client ID: {CLIENT_ID[:8]}... (masked)")
except Exception as e:
    print(f"✗ Failed to retrieve credentials: {e}")
    dbutils.notebook.exit("FAILED: Cannot retrieve sp-osipi credentials")

# COMMAND ----------
# Step 2: Initialize WorkspaceClient with sp-osipi credentials

print("\n" + "=" * 80)
print("STEP 2: Initialize WorkspaceClient with sp-osipi credentials")
print("=" * 80)

try:
    workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
    print(f"Workspace URL: {workspace_url}")

    wc = WorkspaceClient(
        host=workspace_url,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )
    print("✓ WorkspaceClient initialized successfully")
except Exception as e:
    print(f"✗ Failed to initialize WorkspaceClient: {e}")
    dbutils.notebook.exit("FAILED: Cannot initialize WorkspaceClient")

# COMMAND ----------
# Step 3: Get OAuth authentication headers

print("\n" + "=" * 80)
print("STEP 3: Get OAuth authentication headers")
print("=" * 80)

try:
    auth_headers = wc.config.authenticate()
    print("✓ OAuth headers retrieved successfully")
    print(f"  Headers: {list(auth_headers.keys())}")

    # Check if Authorization header is present
    if 'Authorization' in auth_headers:
        auth_value = auth_headers['Authorization']
        if auth_value.startswith('Bearer '):
            token_preview = auth_value[7:27] + "..." if len(auth_value) > 27 else auth_value[7:]
            print(f"  Authorization: Bearer {token_preview} (masked)")
        else:
            print(f"  Authorization: {auth_value[:20]}... (masked)")
    else:
        print("  ⚠ WARNING: No Authorization header found in auth_headers")
        print(f"  Available headers: {auth_headers}")
except Exception as e:
    print(f"✗ Failed to get OAuth headers: {e}")
    dbutils.notebook.exit("FAILED: Cannot get OAuth headers")

# COMMAND ----------
# Step 4: Test API call to Databricks App

print("\n" + "=" * 80)
print("STEP 4: Test API call to Databricks App")
print("=" * 80)

# Databricks App URL
pi_web_api_url = "https://osipi-webserver-1444828305810485.aws.databricksapps.com/piwebapi"
batch_endpoint = f"{pi_web_api_url}/batch"

print(f"Target URL: {batch_endpoint}")
print(f"Testing authentication with sp-osipi Service Principal...")

try:
    # Prepare a simple batch request to test authentication
    # This mimics what the pipeline does
    test_payload = {
        "requests": [
            {
                "Method": "GET",
                "Resource": f"{pi_web_api_url}/dataservers"
            }
        ]
    }

    print(f"\nSending test batch request:")
    print(f"  Method: POST")
    print(f"  URL: {batch_endpoint}")
    print(f"  Payload: {json.dumps(test_payload, indent=2)}")

    # Make the API call using the OAuth headers
    response = requests.post(
        batch_endpoint,
        headers=auth_headers,
        json=test_payload,
        timeout=30
    )

    print(f"\nResponse received:")
    print(f"  Status Code: {response.status_code}")
    print(f"  Status: {response.reason}")

    if response.status_code == 200:
        print("✓ ✓ ✓ SUCCESS! API call authenticated successfully")
        print("\nResponse preview:")
        try:
            response_json = response.json()
            print(json.dumps(response_json, indent=2)[:500])
        except:
            print(response.text[:500])
    elif response.status_code == 401:
        print("✗ ✗ ✗ AUTHENTICATION FAILED (401 Unauthorized)")
        print("\nThis means the OAuth token is not accepted by the Databricks App.")
        print("\nPossible causes:")
        print("  1. sp-osipi Service Principal is not authorized to access the App")
        print("  2. The App's Service Principal (app-40zbx9) doesn't trust sp-osipi")
        print("  3. The credentials in sp-osipi scope don't match the expected Service Principal")
        print("\nResponse body:")
        print(response.text[:500])
    else:
        print(f"✗ Unexpected response code: {response.status_code}")
        print(f"Response body: {response.text[:500]}")

    # Print full response for debugging
    print(f"\nFull response headers:")
    for header, value in response.headers.items():
        print(f"  {header}: {value}")

except requests.exceptions.RequestException as e:
    print(f"✗ ✗ ✗ REQUEST FAILED: {e}")
    print(f"\nThis is the exact error the pipeline is encountering.")
    dbutils.notebook.exit("FAILED: API request failed")

# COMMAND ----------
# Step 5: Verify Service Principal permissions (if API call succeeded)

if response.status_code == 200:
    print("\n" + "=" * 80)
    print("STEP 5: Verify Service Principal details")
    print("=" * 80)

    try:
        # Look up the sp-osipi Service Principal
        all_sps = list(wc.service_principals.list())
        osipi_sp = [sp for sp in all_sps if sp.application_id == CLIENT_ID or 'osipi' in (sp.display_name or '').lower()]

        if osipi_sp:
            sp = osipi_sp[0]
            print(f"✓ Found Service Principal:")
            print(f"  Display Name: {sp.display_name}")
            print(f"  Application ID: {sp.application_id}")
            print(f"  ID: {sp.id}")
            print(f"  Active: {sp.active}")
        else:
            print("⚠ Could not find sp-osipi Service Principal in workspace")
            print(f"  Searched for application_id={CLIENT_ID} or display_name containing 'osipi'")
    except Exception as e:
        print(f"⚠ Could not verify Service Principal details: {e}")

# COMMAND ----------
# Summary

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

if response.status_code == 200:
    print("✓ ✓ ✓ ALL TESTS PASSED")
    print("\nThe sp-osipi Service Principal can successfully authenticate to the Databricks App.")
    print("The pipeline should work correctly with these credentials.")
    print("\nIf pipelines are still failing, check:")
    print("  1. Pipeline configuration uses correct connection_name='mock_pi_connection'")
    print("  2. Pipeline configuration has correct pi.server.url")
    print("  3. Check pipeline logs for other errors")
elif response.status_code == 401:
    print("✗ ✗ ✗ AUTHENTICATION FAILED")
    print("\nThe sp-osipi Service Principal CANNOT authenticate to the Databricks App.")
    print("\nRECOMMENDED ACTIONS:")
    print("  1. Verify the Databricks App (osipi-webserver) has sp-osipi in its permissions:")
    print("     - Go to App settings → Permissions")
    print("     - Check if sp-osipi Service Principal is listed with 'CAN USE' permission")
    print("  2. Verify the credentials in sp-osipi scope match the expected Service Principal:")
    print("     - The CLIENT_ID should match a valid Service Principal application ID")
    print("     - The CLIENT_SECRET should be valid and not expired")
    print("  3. Check the Databricks App (osipi-webserver) configuration:")
    print("     - Ensure the App is configured to accept OAuth authentication")
    print("     - Check if the App requires specific Service Principal authorization")
else:
    print(f"⚠ UNEXPECTED RESULT (HTTP {response.status_code})")
    print("\nThe API responded with an unexpected status code.")
    print("Check the response details above for more information.")

print("\n" + "=" * 80)
