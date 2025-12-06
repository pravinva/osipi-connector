#!/bin/bash

##############################################################################
# Simple Deployment Script (Workaround for databricks.yml parsing)
##############################################################################

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

WORKSPACE_PATH="/Workspace/Shared/osipi-connector"

# Temporarily rename databricks.yml to avoid CLI parsing issues
if [ -f "databricks.yml" ]; then
    mv databricks.yml databricks.yml.temp
    trap "mv databricks.yml.temp databricks.yml" EXIT
fi

echo -e "${GREEN}Deploying OSI PI Connector...${NC}"
echo ""

# Create directories
echo -e "${YELLOW}Creating workspace directories...${NC}"
databricks workspace mkdirs $WORKSPACE_PATH/src/auth
databricks workspace mkdirs $WORKSPACE_PATH/src/client
databricks workspace mkdirs $WORKSPACE_PATH/src/extractors
databricks workspace mkdirs $WORKSPACE_PATH/src/checkpoints
databricks workspace mkdirs $WORKSPACE_PATH/src/writers
databricks workspace mkdirs $WORKSPACE_PATH/src/connector
databricks workspace mkdirs $WORKSPACE_PATH/config
databricks workspace mkdirs $WORKSPACE_PATH/tests
databricks workspace mkdirs $WORKSPACE_PATH/demos
echo -e "${GREEN}✓ Directories created${NC}"
echo ""

# Upload source files individually
echo -e "${YELLOW}Uploading source files...${NC}"

# Auth
databricks workspace import $WORKSPACE_PATH/src/auth/__init__.py --file src/auth/__init__.py --language PYTHON --overwrite
databricks workspace import $WORKSPACE_PATH/src/auth/pi_auth_manager.py --file src/auth/pi_auth_manager.py --language PYTHON --overwrite

# Client
databricks workspace import $WORKSPACE_PATH/src/client/__init__.py --file src/client/__init__.py --language PYTHON --overwrite
databricks workspace import $WORKSPACE_PATH/src/client/pi_web_api_client.py --file src/client/pi_web_api_client.py --language PYTHON --overwrite

# Extractors
databricks workspace import $WORKSPACE_PATH/src/extractors/__init__.py --file src/extractors/__init__.py --language PYTHON --overwrite
databricks workspace import $WORKSPACE_PATH/src/extractors/timeseries_extractor.py --file src/extractors/timeseries_extractor.py --language PYTHON --overwrite
databricks workspace import $WORKSPACE_PATH/src/extractors/af_extractor.py --file src/extractors/af_extractor.py --language PYTHON --overwrite
databricks workspace import $WORKSPACE_PATH/src/extractors/event_frame_extractor.py --file src/extractors/event_frame_extractor.py --language PYTHON --overwrite

# Checkpoints
databricks workspace import $WORKSPACE_PATH/src/checkpoints/__init__.py --file src/checkpoints/__init__.py --language PYTHON --overwrite
databricks workspace import $WORKSPACE_PATH/src/checkpoints/checkpoint_manager.py --file src/checkpoints/checkpoint_manager.py --language PYTHON --overwrite

# Writers
databricks workspace import $WORKSPACE_PATH/src/writers/__init__.py --file src/writers/__init__.py --language PYTHON --overwrite
databricks workspace import $WORKSPACE_PATH/src/writers/delta_writer.py --file src/writers/delta_writer.py --language PYTHON --overwrite

# Connector
databricks workspace import $WORKSPACE_PATH/src/connector/__init__.py --file src/connector/__init__.py --language PYTHON --overwrite
databricks workspace import $WORKSPACE_PATH/src/connector/pi_lakeflow_connector.py --file src/connector/pi_lakeflow_connector.py --language PYTHON --overwrite
databricks workspace import $WORKSPACE_PATH/src/connector/lakeflow_connector.py --file src/connector/lakeflow_connector.py --language PYTHON --overwrite

echo -e "${GREEN}✓ Source code uploaded${NC}"
echo ""

# Upload mock server
echo -e "${YELLOW}Uploading mock PI server...${NC}"
databricks workspace import $WORKSPACE_PATH/tests/mock_pi_server.py --file tests/mock_pi_server.py --language PYTHON --overwrite
echo -e "${GREEN}✓ Mock server uploaded${NC}"
echo ""

# Upload demo notebook if exists
echo -e "${YELLOW}Uploading demo notebook...${NC}"
if [ -f "notebooks/03_connector_demo_performance.py" ]; then
    databricks workspace import notebooks/03_connector_demo_performance.py \
      $WORKSPACE_PATH/demos/03_connector_demo_performance.py \
      --language PYTHON \
      --overwrite
    echo -e "${GREEN}✓ Demo notebook uploaded${NC}"
else
    echo -e "${YELLOW}Demo notebook not found, skipping${NC}"
fi
echo ""

# Create init script
echo -e "${YELLOW}Creating init script...${NC}"
cat > /tmp/osipi-install-deps.sh << 'EOF'
#!/bin/bash
pip install databricks-sdk>=0.30.0 requests>=2.31.0 fastapi>=0.104.0 uvicorn>=0.24.0 pandas>=2.0.0 matplotlib>=3.7.0 seaborn>=0.12.0
EOF

databricks fs cp /tmp/osipi-install-deps.sh dbfs:/init-scripts/osipi-install-deps.sh --overwrite
echo -e "${GREEN}✓ Init script created${NC}"
echo ""

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Deployment Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Workspace: https://e2-demo-field-eng.cloud.databricks.com"
echo "Files at: $WORKSPACE_PATH"
echo ""
echo "Next steps:"
echo "1. Open: $WORKSPACE_PATH/demos/03_connector_demo_performance.py"
echo "2. Attach to a cluster"
echo "3. Run the notebook!"
echo ""
echo "Init script for new clusters: dbfs:/init-scripts/osipi-install-deps.sh"
echo ""
