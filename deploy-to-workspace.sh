#!/bin/bash

##############################################################################
# OSI PI Lakeflow Connector - Deploy to Databricks Workspace
#
# Usage: ./deploy-to-workspace.sh [profile-name]
# Example: ./deploy-to-workspace.sh field-eng
##############################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
PROFILE=${1:-"DEFAULT"}
WORKSPACE_PATH="/Workspace/Shared/osipi-connector"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}OSI PI Connector Deployment${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Step 1: Verify Databricks CLI
echo -e "${YELLOW}Step 1: Verifying Databricks CLI...${NC}"
if ! command -v databricks &> /dev/null; then
    echo -e "${RED}Error: Databricks CLI not found${NC}"
    echo "Install with: pip install databricks-cli"
    exit 1
fi

if [ "$PROFILE" != "DEFAULT" ]; then
    echo "Using profile: $PROFILE"
    export DATABRICKS_CONFIG_PROFILE=$PROFILE
fi

# Test connection
if ! databricks workspace list / &> /dev/null; then
    echo -e "${RED}Error: Cannot connect to Databricks workspace${NC}"
    echo "Run: databricks auth login"
    exit 1
fi

echo -e "${GREEN}✓ Databricks CLI configured${NC}"
echo ""

# Step 2: Create workspace directory
echo -e "${YELLOW}Step 2: Creating workspace directory...${NC}"
databricks workspace mkdirs $WORKSPACE_PATH || true
databricks workspace mkdirs $WORKSPACE_PATH/src || true
databricks workspace mkdirs $WORKSPACE_PATH/config || true
databricks workspace mkdirs $WORKSPACE_PATH/tests || true
databricks workspace mkdirs $WORKSPACE_PATH/notebooks || true
databricks workspace mkdirs $WORKSPACE_PATH/demos || true
echo -e "${GREEN}✓ Directories created${NC}"
echo ""

# Step 3: Upload source code
echo -e "${YELLOW}Step 3: Uploading source code...${NC}"
databricks workspace import-dir src $WORKSPACE_PATH/src --overwrite
echo -e "${GREEN}✓ Source code uploaded${NC}"
echo ""

# Step 4: Upload configuration
echo -e "${YELLOW}Step 4: Uploading configuration...${NC}"
databricks workspace import-dir config $WORKSPACE_PATH/config --overwrite
echo -e "${GREEN}✓ Configuration uploaded${NC}"
echo ""

# Step 5: Upload mock server
echo -e "${YELLOW}Step 5: Uploading mock PI server...${NC}"
databricks workspace import tests/mock_pi_server.py \
  $WORKSPACE_PATH/tests/mock_pi_server.py \
  --language PYTHON \
  --overwrite
echo -e "${GREEN}✓ Mock server uploaded${NC}"
echo ""

# Step 6: Upload demo notebook
echo -e "${YELLOW}Step 6: Uploading demo notebook...${NC}"
if [ -f "notebooks/03_connector_demo_performance.py" ]; then
    databricks workspace import notebooks/03_connector_demo_performance.py \
      $WORKSPACE_PATH/demos/03_connector_demo_performance.py \
      --language PYTHON \
      --overwrite
    echo -e "${GREEN}✓ Demo notebook uploaded${NC}"
else
    echo -e "${YELLOW}⚠ Demo notebook not found, skipping${NC}"
fi
echo ""

# Step 7: Create init script for dependencies
echo -e "${YELLOW}Step 7: Creating dependency install script...${NC}"
cat > /tmp/osipi-install-deps.sh << 'EOF'
#!/bin/bash
pip install databricks-sdk>=0.30.0 requests>=2.31.0 fastapi>=0.104.0 uvicorn>=0.24.0 pandas>=2.0.0 matplotlib>=3.7.0 seaborn>=0.12.0
EOF

databricks fs cp /tmp/osipi-install-deps.sh dbfs:/init-scripts/osipi-install-deps.sh --overwrite || true
echo -e "${GREEN}✓ Init script created at dbfs:/init-scripts/osipi-install-deps.sh${NC}"
echo ""

# Summary
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Deployment Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "📁 Files uploaded to: $WORKSPACE_PATH"
echo ""
echo "Next steps:"
echo ""
echo "1. Create a cluster with init script:"
echo "   Init script: dbfs:/init-scripts/osipi-install-deps.sh"
echo ""
echo "2. Open demo notebook:"
echo "   $WORKSPACE_PATH/demos/03_connector_demo_performance.py"
echo ""
echo "3. Attach to cluster and run!"
echo ""
echo -e "${YELLOW}Optional: Create cluster via CLI${NC}"
echo "See QUICKSTART_DEMO.md for cluster creation commands"
echo ""
echo -e "${GREEN}Happy demoing! 🚀${NC}"
