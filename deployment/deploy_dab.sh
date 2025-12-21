#!/bin/bash
# Deploy Databricks Asset Bundle for OSI PI Connector
#
# Usage:
#   ./deploy_dab.sh [dev|prod]
#
# This script validates and deploys the DAB configuration

set -e  # Exit on error

TARGET="${1:-dev}"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "======================================================================"
echo "Deploying OSI PI Connector DAB"
echo "======================================================================"
echo "Target: $TARGET"
echo "Project: $PROJECT_ROOT"
echo ""

# Check if databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    echo "❌ Error: databricks CLI not found"
    echo "   Install: pip install databricks-cli"
    exit 1
fi

# Change to project root
cd "$PROJECT_ROOT"

# Step 1: Validate
echo "Step 1: Validating DAB configuration..."
if databricks bundle validate -t "$TARGET"; then
    echo "✓ Validation passed"
else
    echo "❌ Validation failed"
    exit 1
fi

# Step 2: Deploy
echo ""
echo "Step 2: Deploying to $TARGET..."
if databricks bundle deploy -t "$TARGET"; then
    echo "✓ Deployment successful"
else
    echo "❌ Deployment failed"
    exit 1
fi

# Step 3: Summary
echo ""
echo "======================================================================"
echo "Deployment Complete!"
echo "======================================================================"
echo ""
echo "View deployed resources:"
echo "  databricks bundle resources list -t $TARGET"
echo ""
echo "View jobs:"
echo "  databricks jobs list | grep osipi"
echo ""
echo "View pipelines:"
echo "  databricks pipelines list | grep osipi"
echo ""
echo "======================================================================"
