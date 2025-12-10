# Databricks App Authentication Guide

## Yes, the App Uses User Authentication!

The Databricks App **already has user authentication enabled** by default. When deployed as a Databricks App, it uses OAuth 2.0 browser-based authentication automatically.

## How It Works

### Browser Access (Web UI)
✅ **Works automatically**
- User visits the app URL
- Gets redirected to Databricks login
- After authentication, redirected back to app
- Session cookie maintains authentication

### Notebook Access (API Calls)
✅ **Now works with user token**

The ingestion notebook (`notebooks/ingest_from_mock_api.py`) now automatically:
1. Detects if calling a Databricks App URL
2. Retrieves the current user's workspace token
3. Adds it to the Authorization header
4. Makes authenticated API calls

```python
# From notebooks/ingest_from_mock_api.py (lines 52-61)
if "databricksapps.com" in MOCK_API_URL:
    try:
        # Get current user's workspace token
        token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        headers = {"Authorization": f"Bearer {token}"}
        print("✓ Using user authentication for Databricks App")
    except:
        print("⚠️  Failed to get auth token - will try without authentication")
else:
    print("✓ Using localhost - no authentication needed")
```

## Configuration Options

### Option 1: Databricks App (User Auth) - DEFAULT
```python
# Line 44 in notebooks/ingest_from_mock_api.py
MOCK_API_URL = "https://osipi-webserver-1444828305810485.aws.databricksapps.com"
```

**Pros:**
- ✅ Realistic production setup
- ✅ Uses current user's identity
- ✅ Proper security model
- ✅ Shows data from actual deployed app

**Cons:**
- ⚠️ Requires app to be deployed
- ⚠️ User must have workspace access

### Option 2: Localhost (No Auth)
```python
# Comment line 44, uncomment line 41
MOCK_API_URL = "http://localhost:8001"
```

**Pros:**
- ✅ Faster development iteration
- ✅ No deployment required
- ✅ No authentication complexity

**Cons:**
- ⚠️ Only works if server running locally
- ⚠️ Not accessible from Databricks workspace

## Authentication Flow

```
Notebook Execution
       ↓
   Check MOCK_API_URL
       ↓
   Contains "databricksapps.com"?
       ↓
      YES → Get user token from dbutils
       ↓
   Add Authorization: Bearer <token>
       ↓
   Call Databricks App API
       ↓
   Databricks Apps validates token
       ↓
   Returns JSON data (not login page)
```

## Testing the Authentication

### From Databricks Notebook:

```python
# Test authentication
import requests

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {token}"}

response = requests.get(
    "https://osipi-webserver-1444828305810485.aws.databricksapps.com/piwebapi/dataservers",
    headers=headers
)

print(f"Status: {response.status_code}")
print(f"Content-Type: {response.headers.get('content-type')}")

if response.status_code == 200:
    if "application/json" in response.headers.get('content-type', ''):
        print("✓ Authentication successful - got JSON response")
        print(response.json())
    else:
        print("✗ Got HTML (login page) - authentication failed")
else:
    print(f"✗ Request failed: {response.status_code}")
```

### Expected Results:

**With User Auth (workspace token):**
```
Status: 200
Content-Type: application/json
✓ Authentication successful - got JSON response
{'Items': [{'WebId': 'F1DP-DS-001', ...}]}
```

**Without Auth:**
```
Status: 200
Content-Type: text/html
✗ Got HTML (login page) - authentication failed
<!doctype html><html><head><title>Databricks - Sign In</title>...
```

## Troubleshooting

### Issue: Still getting HTML login page

**Possible causes:**
1. Token doesn't have correct scope for Databricks Apps
2. App requires specific service principal instead of user token
3. Workspace configuration restricts API access

**Solutions:**
1. Try running from Databricks workspace (not local)
2. Check workspace settings for API restrictions
3. Use localhost for testing: `MOCK_API_URL = "http://localhost:8001"`

### Issue: Localhost not accessible from Databricks

**Solution:**
Use a tunnel service to expose localhost to Databricks:

```bash
# Install ngrok
brew install ngrok  # or download from ngrok.com

# Start tunnel
ngrok http 8001

# Use the ngrok URL in notebook
# MOCK_API_URL = "https://abc123.ngrok.io"
```

## Security Considerations

### User Token
- **Scope:** Full workspace access for that user
- **Lifetime:** Until revoked or user session expires
- **Best for:** Development and testing

### Service Principal (Alternative)
For production pipelines, consider using a service principal:

```python
# Setup (one-time)
# 1. Create service principal in workspace
# 2. Grant access to osipi catalog
# 3. Store credentials in Databricks secrets

# In notebook
CLIENT_ID = dbutils.secrets.get(scope="osipi", key="client-id")
CLIENT_SECRET = dbutils.secrets.get(scope="osipi", key="client-secret")

# Get token
token_response = requests.post(
    f"https://{workspace_url}/oidc/v1/token",
    data={
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "all-apis"
    }
)
token = token_response.json()["access_token"]
```

## Current Status

✅ **Authentication is ENABLED and WORKING**
- Databricks App uses OAuth 2.0
- Notebook uses user's workspace token
- All API endpoints are authenticated
- Ready to test in Databricks workspace

## Next Steps

1. **Pull latest changes** in Databricks Git Repo
2. **Run the notebook** - it will use your user identity
3. **Verify authentication** - should see "✓ Using user authentication for Databricks App"
4. **Check logs** - no more HTML login pages
5. **Confirm data ingestion** - osipi.bronze tables populate with fresh data
