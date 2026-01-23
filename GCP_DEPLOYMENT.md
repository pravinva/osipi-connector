# Google Cloud Run Deployment Guide (Mock PI Web API)

## Overview

This guide covers deploying the **Mock PI Web API server** to Google Cloud Run for external access and pagination testing.

## Prerequisites

- Google Cloud SDK (`gcloud`) installed and configured
- Google Cloud project with billing enabled
- Docker runtime (Cloud Build will handle containerization)
- Git repository with the mock server code

## Architecture

The mock server is deployed as a containerized FastAPI application on Google Cloud Run:

```
./
├── Dockerfile               # Container definition
├── requirements.txt         # Python dependencies
└── mock_piwebapi/
    ├── main.py              # Cloud Run entrypoint (auth wrapper)
    └── pi_web_api.py        # PI Web API mock endpoints
```

## Deployment Steps

### 1. Navigate to Repo Root

```bash
cd /Users/pravin.varma/Documents/Demo/osipi-connector
```

### 2. Verify Dockerfile Exists

The `Dockerfile` should contain:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mock_piwebapi ./mock_piwebapi

ENV PORT=8080
EXPOSE 8080

CMD ["uvicorn", "mock_piwebapi.main:app", "--host=0.0.0.0", "--port=8080"]
```

### 3. Deploy to Google Cloud Run

```bash
gcloud run deploy mock-piwebapi \
  --source . \
  --region us-central1 \
  --project <your-project-id> \
  --platform managed \
  --allow-unauthenticated
```

**Parameters:**
- `mock-piwebapi`: Service name
- `--source .`: Build from current directory (uses Dockerfile)
- `--region us-central1`: Deployment region
- `--project <your-project-id>`: Your GCP project ID
- `--platform managed`: Use fully managed Cloud Run
- `--allow-unauthenticated`: Allow public access (for testing)

**Example:**
```bash
gcloud run deploy mock-piwebapi \
  --source . \
  --region us-central1 \
  --project project-cf351169-2591-44d2-a26 \
  --platform managed \
  --allow-unauthenticated
```

### 4. Wait for Deployment

The deployment process will:
1. Upload source code to Google Cloud Storage
2. Build container image using Cloud Build
3. Push image to Artifact Registry
4. Deploy to Cloud Run
5. Return service URL

**Expected Output:**
```
Building using Dockerfile and deploying container to Cloud Run service [mock-piwebapi]
in project [project-cf351169-2591-44d2-a26] region [us-central1]
Building and deploying...
✓ Creating Revision...
✓ Routing traffic...
Done.
Service [mock-piwebapi] revision [mock-piwebapi-00005-dmr] has been deployed
and is serving 100 percent of traffic.
Service URL: https://mock-piwebapi-912141448724.us-central1.run.app
```

### 5. Verify Deployment

Test the deployed service:

```bash
# Test health endpoint
curl https://mock-piwebapi-912141448724.us-central1.run.app/health

# Test PI Web API endpoint
curl -H "Authorization: Bearer <token>" \
  "https://mock-piwebapi-912141448724.us-central1.run.app/piwebapi/dataservers"

# Test time-series data
curl -H "Authorization: Bearer <token>" \
  "https://mock-piwebapi-912141448724.us-central1.run.app/piwebapi/streams/F1DP-Adelaide-U001-Curr-04008/recorded?startTime=*-1h&endTime=*&maxCount=100"
```

## Configuration

### Mock Data Density

The mock server generates time-series data with configurable density. For pagination testing:

**File:** `mock_piwebapi/pi_web_api.py`

**Line 447-448:**
```python
# Changed from 60 to 10 seconds for pagination testing (360 events/hour instead of 60)
items = generate_realistic_timeseries(tag_info, start, end, interval_seconds=10, max_count=maxCount)
```

**Options:**
- `interval_seconds=60`: 60 events per hour (1 per minute) - original
- `interval_seconds=10`: 361 events per hour (6 per minute) - for pagination testing
- `interval_seconds=5`: 721 events per hour (12 per minute) - for heavy pagination

### Environment Variables

Set environment variables in Cloud Run:

```bash
gcloud run services update mock-piwebapi \
  --region us-central1 \
  --project <your-project-id> \
  --set-env-vars EXPECTED_BEARER_TOKEN=<long_random_token>
```

## Updating the Deployment

### Option 1: Redeploy from Source

```bash
cd /Users/pravin.varma/Documents/Demo/osipi-connector
gcloud run deploy mock-piwebapi \
  --source . \
  --region us-central1 \
  --project <your-project-id> \
  --platform managed \
  --allow-unauthenticated
```

### Option 2: Deploy with Git Commit

```bash
# Commit your changes
git add .
git commit -m "feat: Update mock server configuration"

# Deploy from committed code
gcloud run deploy mock-piwebapi \
  --source . \
  --region us-central1 \
  --project <your-project-id> \
  --platform managed \
  --allow-unauthenticated
```

**Note:** Cloud Run builds from the working directory. To force a rebuild with cached changes, modify the Dockerfile (e.g., add a comment) to invalidate the cache.

### Option 3: Deploy from Container Image

```bash
# Build container locally
docker build -t gcr.io/<project-id>/mock-piwebapi:latest .

# Push to Google Container Registry
docker push gcr.io/<project-id>/mock-piwebapi:latest

# Deploy from image
gcloud run deploy mock-piwebapi \
  --image gcr.io/<project-id>/mock-piwebapi:latest \
  --region us-central1 \
  --project <your-project-id> \
  --platform managed \
  --allow-unauthenticated
```

## Monitoring

### View Service Details

```bash
gcloud run services describe mock-piwebapi \
  --region us-central1 \
  --project <your-project-id>
```

### View Logs

**From Command Line:**
```bash
gcloud run services logs read mock-piwebapi \
  --region us-central1 \
  --project <your-project-id> \
  --limit 50
```

**From GCP Console:**
1. Navigate to: https://console.cloud.google.com/run
2. Select `mock-piwebapi` service
3. Click **Logs** tab

### View Metrics

**From GCP Console:**
1. Navigate to: https://console.cloud.google.com/run
2. Select `mock-piwebapi` service
3. Click **Metrics** tab

**Key Metrics:**
- Request count
- Request latency
- Container CPU utilization
- Container memory utilization
- Billable container instance time

## Pagination Testing

### Test Scenarios

With `interval_seconds=10`, the mock server generates **361 events per hour**, enabling proper pagination testing:

**Scenario 1: Heavy Pagination (maxCount=10)**
```bash
curl -H "Authorization: Bearer <token>" \
  "https://mock-piwebapi-912141448724.us-central1.run.app/piwebapi/streams/F1DP-Adelaide-U001-Curr-04008/recorded?startTime=*-1h&endTime=*&maxCount=10" \
  | python3 -c "import sys, json; data = json.load(sys.stdin); print(f'Events: {len(data.get(\"Items\", []))}')"
```
Expected: 10 events (36 pages total for 361 events)

**Scenario 2: Moderate Pagination (maxCount=100)**
```bash
curl -H "Authorization: Bearer <token>" \
  "https://mock-piwebapi-912141448724.us-central1.run.app/piwebapi/streams/F1DP-Adelaide-U001-Curr-04008/recorded?startTime=*-1h&endTime=*&maxCount=100" \
  | python3 -c "import sys, json; data = json.load(sys.stdin); print(f'Events: {len(data.get(\"Items\", []))}')"
```
Expected: 100 events (4 pages total for 361 events)

**Scenario 3: No Pagination (maxCount=1000)**
```bash
curl -H "Authorization: Bearer <token>" \
  "https://mock-piwebapi-912141448724.us-central1.run.app/piwebapi/streams/F1DP-Adelaide-U001-Curr-04008/recorded?startTime=*-1h&endTime=*&maxCount=1000" \
  | python3 -c "import sys, json; data = json.load(sys.stdin); print(f'Events: {len(data.get(\"Items\", []))}')"
```
Expected: 361 events (1 page total)

## Scaling Configuration

### Auto-scaling Settings

```bash
gcloud run services update mock-piwebapi \
  --region us-central1 \
  --project <your-project-id> \
  --min-instances 0 \
  --max-instances 20 \
  --concurrency 80
```

**Parameters:**
- `--min-instances 0`: Scale to zero when idle (cost savings)
- `--max-instances 20`: Maximum concurrent instances
- `--concurrency 80`: Maximum requests per instance

### Resource Allocation

```bash
gcloud run services update mock-piwebapi \
  --region us-central1 \
  --project <your-project-id> \
  --memory 512Mi \
  --cpu 1
```

**Recommendations:**
| Tag Count | Memory | CPU | Concurrency |
|-----------|--------|-----|-------------|
| 128-1K | 256Mi | 1 | 80 |
| 10K | 512Mi | 1 | 80 |
| 30K+ | 1Gi | 2 | 40 |

## Security

### Authentication Options

**Option 1: Public Access (Current)**
```bash
gcloud run services add-iam-policy-binding mock-piwebapi \
  --region us-central1 \
  --project <your-project-id> \
  --member="allUsers" \
  --role="roles/run.invoker"
```

**Option 2: Authenticated Access**
```bash
# Remove public access
gcloud run services remove-iam-policy-binding mock-piwebapi \
  --region us-central1 \
  --project <your-project-id> \
  --member="allUsers" \
  --role="roles/run.invoker"

# Add specific user
gcloud run services add-iam-policy-binding mock-piwebapi \
  --region us-central1 \
  --project <your-project-id> \
  --member="user:pravin.varma@databricks.com" \
  --role="roles/run.invoker"
```

### Bearer Token Validation

Authentication is enforced in `mock_piwebapi/main.py`.

- If `EXPECTED_BEARER_TOKEN` is set in Cloud Run, requests must send exactly:
  - `Authorization: Bearer <EXPECTED_BEARER_TOKEN>`
- If `EXPECTED_BEARER_TOKEN` is not set, any non-empty bearer token is accepted.

## Cost Optimization

### Pricing Model

Cloud Run pricing (as of 2026):
- **CPU**: $0.00002400 per vCPU-second
- **Memory**: $0.00000250 per GiB-second
- **Requests**: $0.40 per million requests
- **Free Tier**: 2 million requests/month, 360,000 GiB-seconds/month

### Estimated Costs

| Usage Pattern | Requests/Month | Est. Cost/Month |
|---------------|----------------|-----------------|
| Development Testing | 10,000 | $0 (free tier) |
| CI/CD Integration | 100,000 | $1-2 |
| Production Staging | 1,000,000 | $5-10 |
| Heavy Load Testing | 10,000,000 | $40-50 |

**Cost-saving Tips:**
- Set `--min-instances 0` to scale to zero
- Use `--cpu-throttling` for background workloads
- Set lower `--memory` allocations
- Monitor and set request timeouts

## Troubleshooting

### Deployment Fails

**Symptom:** Build fails with dependency errors

**Solution:**
```bash
# Test requirements locally
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Check for conflicting dependencies
pip check
```

### Service Returns 503

**Symptom:** Service unavailable after deployment

**Solutions:**
1. Check logs: `gcloud run services logs read mock-piwebapi --region us-central1 --project <project-id>`
2. Verify container starts: `docker build -t test . && docker run -p 8080:8080 test`
3. Check PORT environment variable matches Dockerfile EXPOSE

### Cold Start Latency

**Symptom:** First request after idle takes 5-10 seconds

**Solutions:**
1. Set `--min-instances 1` to keep one instance warm
2. Use Cloud Scheduler to ping health endpoint every 5 minutes
3. Optimize container size (use slim base image)

### Build Takes Too Long

**Symptom:** Cloud Build times out or takes > 10 minutes

**Solutions:**
1. Check for large files in build context (add to `.gcloudignore`)
2. Use smaller base image (python:3.11-slim vs python:3.11)
3. Layer Dockerfile efficiently (COPY requirements.txt before COPY mock source)

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Deploy to Cloud Run

on:
  push:
    branches:
      - main
    paths:
      - 'mock_piwebapi/**'
      - 'Dockerfile'
      - 'requirements.txt'
      - 'GCP_DEPLOYMENT.md'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - uses: google-github-actions/auth@v1
        with:
          credentials_json: ${{ secrets.GCP_SA_KEY }}

      - uses: google-github-actions/setup-gcloud@v1

      - name: Deploy to Cloud Run
        run: |
          gcloud run deploy mock-piwebapi \
            --source . \
            --region us-central1 \
            --project project-cf351169-2591-44d2-a26 \
            --platform managed \
            --allow-unauthenticated
```

## Next Steps

After successful deployment:

1. **Set a token**: configure `EXPECTED_BEARER_TOKEN` on the Cloud Run service.
2. **Smoke test**: call `/health`, `/piwebapi`, `/piwebapi/dataservers`.
3. **Test pagination**: run the scenarios in the “Pagination Testing” section with different `maxCount` values.
4. **Monitor performance**: use Cloud Run metrics/logs to validate latency under load.
5. **Tune data density**: adjust `interval_seconds` in `mock_piwebapi/pi_web_api.py` if needed.

## Support

For issues or questions:
- Cloud Run Documentation: https://cloud.google.com/run/docs
- View logs: `gcloud run services logs read mock-piwebapi --region us-central1`
- Check service status: `gcloud run services describe mock-piwebapi --region us-central1`
- GCP Console: https://console.cloud.google.com/run
