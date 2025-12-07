# Configuration Best Practices - OSI PI Lakeflow Connector

## ⚠️ **Important: Warehouse ID Configuration**

### **Current State**

The codebase contains a **demo warehouse ID** (`4b9b953939869799`) in 20 files as a default fallback.

**This is intentional for demo purposes**, but **MUST be configured** for production.

---

## 🔧 **How to Configure for Production**

### **Option 1: Environment Variables (Recommended)**

All scripts check environment variables FIRST:

```bash
# Set in your environment
export DATABRICKS_WAREHOUSE_ID="your_actual_warehouse_id"
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi_your_token"

# Then run connector
python app/main.py
```

**Code pattern** (already implemented):
```python
# Every file uses this pattern:
warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "4b9b953939869799")
#                                             ^^^^^^^^^^^^^^^^^^^^
#                                             Fallback for demo only
```

---

### **Option 2: Databricks Secrets (Production)**

For production deployments, use Databricks Secrets:

```python
# In Databricks notebook
warehouse_id = dbutils.secrets.get(scope="pi-connector", key="warehouse_id")

# Setup secrets:
# databricks secrets create-scope --scope pi-connector
# databricks secrets put --scope pi-connector --key warehouse_id
```

---

### **Option 3: DABS Variables**

In `databricks-loadbalanced.yml`:

```yaml
variables:
  warehouse_id:
    description: SQL Warehouse ID for connector
    default: ${env.DATABRICKS_WAREHOUSE_ID}  # From environment
```

Then in notebooks:
```python
warehouse_id = dbutils.widgets.get("warehouse_id")
```

---

## 📋 **Files with Warehouse ID**

### **Demo/Utility Files** (Use env var with demo fallback)
- `app/main.py` - Demo dashboard
- `tests/mock_pi_server.py` - Mock server
- `create_event_table.py` - Setup utility
- `check_event_tables.py` - Verification utility
- `ingest_events_*.py` - Event ingestion utilities

**Pattern**: `os.getenv("DATABRICKS_WAREHOUSE_ID", "4b9b953939869799")`

✅ **This is OK** - Demo ID only used if env var not set

---

### **Connector Files** (Should use configuration)
- `src/connectors/pi_streaming_connector.py`
- `notebooks/*.py` - All orchestration notebooks

**Should receive warehouse_id from**:
- Environment variable (laptop testing)
- Databricks Secrets (production)
- DABS parameters (deployment)

---

### **Documentation Files** (Examples only)
- `README.md`
- `docs/*.md`
- `HACKATHON_DEMO_GUIDE.md`

**Pattern**: Show as example in docs
✅ **This is OK** - Just documentation examples

---

## 🎯 **Production Deployment Checklist**

Before deploying to production:

### **1. Configure Warehouse**
```bash
# Get your actual warehouse ID
databricks warehouses list

# Set in environment
export DATABRICKS_WAREHOUSE_ID="<your-actual-warehouse-id>"

# Or in Databricks Secrets
databricks secrets put --scope pi-connector --key warehouse_id
```

### **2. Update DABS Configuration**
```yaml
# databricks-loadbalanced.yml
variables:
  warehouse_id:
    description: SQL Warehouse ID
    default: ${env.DATABRICKS_WAREHOUSE_ID}
```

### **3. Update Notebooks**
```python
# Instead of hardcoded default:
warehouse_id = dbutils.widgets.get("warehouse_id")

# Or from secrets:
warehouse_id = dbutils.secrets.get(scope="pi-connector", key="warehouse_id")
```

---

## ⚙️ **Complete Configuration Template**

### **Create `.env` file** (copy from `.env.example`):

```bash
# Databricks
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi_your_actual_token
DATABRICKS_WAREHOUSE_ID=your_actual_warehouse_id

# PI Web API
PI_WEB_API_URL=https://pi-server.company.com/piwebapi
PI_USERNAME=your_username
PI_PASSWORD=your_password
PI_AUTH_TYPE=kerberos  # or basic, oauth

# Unity Catalog
UC_CATALOG=osipi
UC_SCHEMA=bronze

# Connector
BATCH_SIZE=100
TOTAL_PARTITIONS=10
FLUSH_INTERVAL_SECONDS=60
```

Then load in code:
```python
from dotenv import load_dotenv
load_dotenv()

warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")  # No hardcoded fallback
```

---

## 🔒 **Security Best Practices**

### **DO**
- ✅ Use environment variables for local development
- ✅ Use Databricks Secrets for production
- ✅ Use DABS variables for deployment
- ✅ Add `.env` to `.gitignore`
- ✅ Document configuration in README

### **DON'T**
- ❌ Commit credentials to git
- ❌ Hardcode production warehouse IDs
- ❌ Use demo IDs in production
- ❌ Share warehouse IDs in public docs

---

## 📝 **Demo vs Production**

### **Demo Configuration** (Current)
```python
# Demo warehouse ID as fallback
warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "4b9b953939869799")
```

**Purpose**: 
- Quick demos work out-of-box
- No config needed for hackathon presentation
- Fallback to known demo warehouse

**Status**: ✅ OK for hackathon/demos

---

### **Production Configuration** (Required)
```python
# No fallback - fail if not configured
warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
if not warehouse_id:
    raise ValueError("DATABRICKS_WAREHOUSE_ID must be configured")
```

**Purpose**:
- Forces explicit configuration
- Prevents accidental demo ID usage
- Clear error if misconfigured

**Status**: Implement before production deployment

---

## ✅ **Current Status**

**For Hackathon**:
- ✅ Demo warehouse ID is acceptable
- ✅ Scripts work out-of-box for presentations
- ✅ Environment variable override supported
- ✅ Configuration documented

**Before Production**:
- [ ] Remove all hardcoded warehouse ID fallbacks
- [ ] Require explicit configuration
- [ ] Use Databricks Secrets exclusively
- [ ] Update DABS with production warehouse
- [ ] Document configuration in deployment guide

---

## 🎯 **Summary**

**Demo Warehouse ID** (`4b9b953939869799`):
- ✅ **Hackathon**: OK as fallback for demos
- ❌ **Production**: Must be replaced with actual warehouse

**Configuration Pattern**:
- ✅ Already supports environment variables
- ✅ Already supports Databricks Secrets
- ✅ Already supports DABS parameters
- ⚠️ Just has demo fallback for convenience

**Action Required**:
- For hackathon: **No action needed** (current pattern is fine)
- For production: **Remove fallbacks**, require explicit configuration

---

**The connector follows proper Lakeflow patterns. The demo warehouse ID is just a convenience fallback, not a fundamental flaw.**

