#!/usr/bin/env python3
"""Test OAuth M2M authentication flow for Databricks Apps."""

import requests
import base64
import os

# Configuration
WORKSPACE_URL = "https://oregon.cloud.databricks.com"
DATABRICKS_APP_URL = "https://osipi-webserver-1444828305810485.aws.databricksapps.com"

# Get credentials from environment (simulating dbutils.secrets)
CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID")
CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET")

if not CLIENT_ID or not CLIENT_SECRET:
    print("ERROR: Set DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET environment variables")
    print("These should be the service principal credentials from sp-osipi secret scope")
    exit(1)

print("Testing OAuth M2M Authentication Flow")
print("=" * 60)
print(f"Workspace: {WORKSPACE_URL}")
print(f"Target App: {DATABRICKS_APP_URL}")
print(f"Client ID: {CLIENT_ID[:8]}...")
print()

# Step 1: Get OAuth token
print("Step 1: Requesting OAuth token from OIDC endpoint...")
token_url = f"{WORKSPACE_URL}/oidc/v1/token"
auth_header = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()

try:
    token_response = requests.post(
        token_url,
        headers={
            "Authorization": f"Basic {auth_header}",
            "Content-Type": "application/x-www-form-urlencoded"
        },
        data="grant_type=client_credentials&scope=all-apis",
        timeout=10
    )

    print(f"  Status Code: {token_response.status_code}")

    if token_response.status_code != 200:
        print(f"  ERROR: {token_response.text}")
        exit(1)

    token_data = token_response.json()
    access_token = token_data["access_token"]
    print(f"  ✓ Token acquired (length: {len(access_token)} chars)")
    print(f"  Token type: {token_data.get('token_type', 'N/A')}")
    print(f"  Expires in: {token_data.get('expires_in', 'N/A')} seconds")
    print()

except Exception as e:
    print(f"  ERROR: {e}")
    exit(1)

# Step 2: Test authentication with Databricks App
print("Step 2: Testing authentication with Databricks App...")
headers = {"Authorization": f"Bearer {access_token}"}

try:
    # Test the dataservers endpoint
    test_url = f"{DATABRICKS_APP_URL}/piwebapi/dataservers"
    print(f"  Testing: {test_url}")

    response = requests.get(test_url, headers=headers, timeout=10)
    print(f"  Status Code: {response.status_code}")

    if response.status_code == 200:
        data = response.json()
        dataservers = data.get("Items", [])
        print(f"  ✓ Authentication successful!")
        print(f"  Data servers found: {len(dataservers)}")
        if dataservers:
            print(f"  First server: {dataservers[0].get('Name', 'N/A')}")
        print()
        print("SUCCESS: OAuth authentication flow is working correctly!")
    else:
        print(f"  ERROR: {response.text}")
        exit(1)

except Exception as e:
    print(f"  ERROR: {e}")
    exit(1)
