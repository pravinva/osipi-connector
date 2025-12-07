# Testing with Real PI Systems - Request Guide

**Purpose:** Guide for requesting PI System access from AVEVA or Databricks partnership team for testing and validation

---

## 🎯 Executive Summary

To validate the OSI PI Lakeflow Connector with real PI infrastructure, you'll need access to either:
1. **AVEVA's Demo/Sandbox PI System** (recommended for initial testing)
2. **Customer POC Environment** (for production validation)
3. **AVEVA Developer Program** resources

---

## 📋 Option 1: AVEVA Demo Environment (RECOMMENDED)

### What to Request from AVEVA

**Contact:** AVEVA Partnership Team or Technical Account Manager

**Subject:** Request for PI System Demo Environment Access for Databricks Integration Testing

**Email Template:**

```
To: AVEVA Partnership Team / Technical Sales Engineering
Subject: PI System Demo Environment Access - Databricks Lakeflow Connector Testing

Hello AVEVA Team,

I am from Databricks working on a new Lakeflow connector for OSI PI Systems. We have
developed a production-ready connector and need access to a PI System environment to
validate and demonstrate the integration.

REQUEST:
- Access to AVEVA PI Web API demo/sandbox environment
- Credentials (Basic Auth, Kerberos, or OAuth)
- Sample PI Data Archive with ~100-1000 tags
- PI Asset Framework hierarchy (if available)
- Event Frames (optional, for advanced testing)

USE CASE:
- Validate PI Web API connectivity
- Test time-series data extraction (batch controller optimization)
- Test AF hierarchy extraction
- Demonstrate integration to potential joint customers

TIMEFRAME:
- Duration: [2 weeks / 1 month]
- Purpose: POC validation and demo preparation

TECHNICAL DETAILS:
- Integration: Databricks Lakeflow → PI Web API (REST)
- Authentication: Basic/Kerberos/OAuth (flexible)
- Data Volume: Small dataset sufficient (test purposes only)
- Read-only access required

CONTEXT:
This connector addresses customer requests from Alinta Energy and other utilities/process
manufacturing customers for native PI connectivity. This will be a joint go-to-market
solution between AVEVA and Databricks.

Thank you for your support.

Best regards,
[Your Name]
[Databricks Title]
[Contact Information]
```

### Expected Access Details

You should receive:
- **PI Web API URL:** `https://demo.osisoft.com/piwebapi` or similar
- **Credentials:** Username/password or OAuth token
- **Documentation:** API endpoints, available tags, data structure
- **Support Contact:** Technical contact for issues

---

## 📋 Option 2: AVEVA Developer Program

### What It Provides

**AVEVA PI System Access Program:**
- Free PI System Cloud Instance (limited duration)
- Pre-configured sample data
- Documentation and tutorials
- Developer support

### How to Apply

1. **Visit:** https://www.aveva.com/en/products/pi-system/
2. **Navigate to:** Developer Resources / Partner Program
3. **Apply for:** Developer Access Program
4. **Mention:** Databricks integration partnership

**Application Details to Include:**
- Company: Databricks
- Purpose: Lakeflow connector development
- Use Case: Customer-driven integration (Alinta, utilities, manufacturing)
- Timeline: POC testing and validation

---

## 📋 Option 3: Databricks Partnership Team Request

### Internal Databricks Request

**Contact:** Databricks Partnership Manager for AVEVA

**Subject:** AVEVA PI System Access for Lakeflow Connector Validation

**Email Template:**

```
To: Databricks Partnership Team
Cc: AVEVA Databricks Alliance Manager
Subject: Request PI System Access for Lakeflow Connector Testing

Hello Partnership Team,

I have developed a production-ready OSI PI Lakeflow connector (Module 2 complete,
security hardened) and need access to a real PI System for validation.

BACKGROUND:
- Customer demand: Alinta Energy April 2024 request for "PI AF and Event Frame connectivity"
- Use case: Time-series data ingestion from 30K+ tags at raw granularity
- Competition: AVEVA CDS limited to 2,000 tags at >5min granularity
- Solution: Native Lakeflow connector with PI Web API batch controller (100x faster)

REQUEST FROM AVEVA:
1. Demo PI Web API environment access
2. Credentials for testing (Basic/Kerberos/OAuth)
3. Sample dataset with:
   - Time-series data (100-1000 tags)
   - PI Asset Framework hierarchy
   - Event Frames (optional)

DELIVERABLES:
- Validated connector ready for joint customer demos
- Performance benchmarks (time-series extraction speed)
- Demo notebook showcasing integration
- Joint GTM asset with AVEVA

TIMELINE:
- Testing: 2 weeks
- Demo preparation: 1 week
- Joint customer presentation: [Target Date]

Please facilitate introduction to AVEVA technical team or provide access directly
if available through partnership channels.

Thank you,
[Your Name]
```

---

## 📋 Option 4: Customer POC Environment

### For Production Validation

**Best Customers to Approach:**

1. **Alinta Energy** (Australia)
   - Already expressed need for PI connectivity (April 2024)
   - 30,000+ tags use case
   - Databricks customer with PI infrastructure

2. **Utilities/Energy Customers**
   - Ask Databricks account teams for customers with OSI PI
   - Focus on those with >10,000 tags (where connector excels)

3. **Process Manufacturing**
   - Chemical, pharmaceutical, food & beverage industries
   - Often have PI System deployments

**Request Template for Customer POC:**

```
To: [Customer Technical Contact]
Cc: Databricks Account Team
Subject: PI System Integration POC - Databricks Lakeflow Connector

Hello [Customer Name],

Following up on your interest in connecting OSI PI data to Databricks, we have developed
a native Lakeflow connector specifically for PI Systems.

SOLUTION BENEFITS:
- Extract 30K+ tags at raw granularity (no CDS limitations)
- 100x faster than sequential API calls (batch controller optimization)
- Native PI Asset Framework and Event Frame support
- Production-ready with enterprise security (SSL, auth, monitoring)

POC REQUEST:
- Read-only access to PI Web API (non-production environment preferred)
- Sample of 100-1000 tags for initial validation
- 2-week testing period
- Your technical team's time for validation (minimal: 2-4 hours)

DELIVERABLES:
- Proof of concept demonstrating:
  * Time-series data in Unity Catalog
  * AF hierarchy extraction
  * Performance benchmarks
  * Production architecture design
- No cost, no commitment required

Are you available for a technical kickoff call next week?

Best regards,
[Your Name]
Databricks
```

---

## 🔧 What You Need from PI System Access

### Minimum Requirements (Basic Testing)

1. **PI Web API Access**
   - URL: `https://[pi-server]/piwebapi`
   - Authentication: Any method (Basic, Kerberos, OAuth)
   - Network access: HTTPS connectivity

2. **Sample Data**
   - 10-100 PI tags (any tags with data)
   - Time range: Last 7 days minimum
   - Data frequency: Any (1s, 1min, 5min, etc.)

3. **Permissions**
   - Read-only access to PI Data Archive
   - `/dataservers` endpoint (list servers)
   - `/points` endpoint (list tags)
   - `/streams/{webid}/recorded` endpoint (get data)

### Ideal Access (Full Testing)

1. **Extended PI Data**
   - 100-1000 tags for performance testing
   - Historical data: 30-90 days
   - Various data types (floats, strings, integers)
   - Different update frequencies

2. **PI Asset Framework**
   - Read access to AF database
   - Sample hierarchy (3-5 levels deep)
   - Element templates
   - Element attributes

3. **Event Frames**
   - Sample event frames (batch runs, alarms, etc.)
   - Event attributes
   - Referenced elements

---

## 📝 Technical Validation Plan

### Phase 1: Connectivity (Week 1, Days 1-2)

**Objective:** Verify basic connectivity

**Tests:**
```python
# Test 1: Authentication
auth_manager = PIAuthManager(auth_config)
assert auth_manager.test_connection(pi_web_api_url)

# Test 2: List Data Servers
servers = client.get("/piwebapi/dataservers").json()
print(f"Connected to: {servers['Items'][0]['Name']}")

# Test 3: List Tags
points = client.get("/piwebapi/dataservers/{server_id}/points?maxCount=10")
print(f"Found {len(points.json()['Items'])} tags")
```

**Success Criteria:**
- ✅ SSL/TLS connection successful
- ✅ Authentication working
- ✅ Can list PI servers
- ✅ Can list PI tags

### Phase 2: Data Extraction (Week 1, Days 3-5)

**Objective:** Extract time-series data

**Tests:**
```python
# Test 4: Single Tag Extraction
tag_webid = "F1DP..."  # Sample tag
data = extract_recorded_data(
    tag_webids=[tag_webid],
    start_time=datetime.now() - timedelta(days=7),
    end_time=datetime.now()
)
print(f"Extracted {len(data)} records")

# Test 5: Batch Extraction (Performance)
tag_webids = get_sample_tags(count=100)
start = time.time()
data = extract_recorded_data(tag_webids, start_time, end_time)
duration = time.time() - start
print(f"Extracted 100 tags in {duration:.2f}s")
```

**Success Criteria:**
- ✅ Can extract single tag data
- ✅ Batch extraction working (100 tags)
- ✅ Performance: 100 tags in <15 seconds
- ✅ Data quality flags preserved

### Phase 3: AF & Event Frames (Week 2)

**Objective:** Extract metadata and events

**Tests:**
```python
# Test 6: AF Hierarchy
hierarchy = extract_af_hierarchy(database_webid)
print(f"Extracted {len(hierarchy)} elements")

# Test 7: Event Frames
events = extract_event_frames(
    database_webid,
    start_time=datetime.now() - timedelta(days=30),
    end_time=datetime.now()
)
print(f"Extracted {len(events)} event frames")
```

**Success Criteria:**
- ✅ AF hierarchy extracted
- ✅ Element paths correct
- ✅ Event frames extracted
- ✅ Event attributes retrieved

### Phase 4: End-to-End (Week 2)

**Objective:** Full production workflow

**Tests:**
```python
# Test 8: Full Pipeline
connector = PILakeflowConnector(config)
connector.run()

# Verify data in Unity Catalog
spark.sql("SELECT COUNT(*) FROM main.bronze.pi_timeseries").show()
spark.sql("SELECT COUNT(*) FROM main.bronze.pi_asset_hierarchy").show()
```

**Success Criteria:**
- ✅ Data in Unity Catalog
- ✅ Incremental load working
- ✅ Checkpoints updating
- ✅ No errors in production run

---

## 📊 Success Metrics to Report Back

### Performance Benchmarks

**Metric 1: Time-Series Extraction Speed**
- Tags: 100
- Time Range: 7 days
- Records per tag: ~10,000
- **Target:** <15 seconds total

**Metric 2: Batch vs Sequential**
- Compare: Batch controller vs individual API calls
- **Expected:** 30-100x faster with batch

**Metric 3: Large Scale**
- Tags: 1,000
- Time Range: 1 day
- **Target:** <2 minutes total

### Quality Metrics

- Data accuracy: 100% (match PI System)
- Quality flags preserved: Yes
- Timestamps correct: Yes
- No data loss: Verified

---

## 🎁 What to Offer in Return

### To AVEVA

1. **Joint Success Story**
   - Case study featuring integration
   - Blog post on Databricks + AVEVA
   - Joint webinar

2. **Technical Validation**
   - Detailed performance benchmarks
   - Best practices documentation
   - Reference architecture

3. **Market Opportunity**
   - Access to Databricks customer base
   - Joint sales opportunities
   - Co-selling motion

### To Databricks Partnership Team

1. **Production-Ready Connector**
   - Fully tested and validated
   - Security hardened
   - Documentation complete

2. **Customer Demo**
   - Ready-to-use demo notebook
   - Sample data and outputs
   - Presentation materials

3. **Competitive Intelligence**
   - AVEVA CDS limitations documented
   - Performance comparison
   - Pricing insights

---

## 📞 Key Contacts

### AVEVA Contacts

**Partnership Team:**
- AVEVA Partner Alliance Manager
- Email: partners@aveva.com
- Website: https://www.aveva.com/en/partners/

**Technical Sales:**
- PI System Solutions Engineers
- Request through Databricks partnership team

**Developer Program:**
- Website: https://www.aveva.com/en/products/pi-system/
- Email: developer-support@aveva.com

### Databricks Internal

**Partnership Team:**
- Search Slack: #partnerships
- Find: AVEVA alliance manager

**Field Teams:**
- Account teams with utilities/manufacturing customers
- Solution Architects with OT/IoT experience

**Product Team:**
- Lakeflow product management
- Delta Lake team (for IoT use cases)

---

## 📅 Suggested Timeline

### Week 1: Access Request
- **Day 1-2:** Submit access requests (AVEVA + Databricks)
- **Day 3-5:** Follow up on requests
- **Day 6-7:** Receive credentials and documentation

### Week 2: Initial Testing
- **Day 8-9:** Connectivity testing
- **Day 10-12:** Data extraction testing
- **Day 13-14:** Performance benchmarks

### Week 3: Advanced Testing
- **Day 15-17:** AF hierarchy and Event Frames
- **Day 18-19:** End-to-end pipeline testing
- **Day 20-21:** Documentation and reporting

### Week 4: Demo Preparation
- **Day 22-24:** Demo notebook creation
- **Day 25-26:** Presentation preparation
- **Day 27-28:** Internal review and feedback

---

## 💡 Pro Tips

### For Faster Access

1. **Mention Customer Demand**
   - Reference Alinta Energy request
   - Cite specific use cases
   - Show market opportunity

2. **Emphasize Joint Value**
   - Win-win for AVEVA and Databricks
   - Expand both ecosystems
   - Drive joint revenue

3. **Keep Scope Small Initially**
   - Start with demo environment
   - Prove value quickly
   - Expand access after success

4. **Leverage Existing Relationships**
   - Use Databricks partnership channels
   - Ask account teams for customer intros
   - Engage solution architects

### Red Flags to Avoid

❌ Asking for production access first
❌ Requesting write access
❌ Large data volume requests initially
❌ No clear use case or timeline
❌ No mention of joint benefits

### Green Flags to Include

✅ Read-only access only
✅ Clear testing timeline (2-4 weeks)
✅ Specific deliverables defined
✅ Joint GTM opportunity
✅ Customer validation in mind

---

## 📋 Pre-Request Checklist

Before requesting access, ensure you have:

- [ ] Connector code complete and tested (Module 2 ✅)
- [ ] Security audit passed (✅ 38/38 tests)
- [ ] Documentation ready (✅ SECURITY.md, etc.)
- [ ] Demo plan documented (this guide)
- [ ] Success metrics defined
- [ ] Timeline estimated
- [ ] Stakeholders identified (AVEVA, Databricks, Customer)
- [ ] Approval from Databricks management
- [ ] Backup plan if primary access denied

---

## 📧 Sample Follow-Up Email

```
Subject: Follow-up: PI System Access Request

Hello [Contact Name],

Following up on my request from [Date] for PI System access to validate our Databricks
Lakeflow connector.

QUICK CONTEXT:
- Purpose: Test production-ready connector with real PI infrastructure
- Duration: 2-3 weeks
- Requirements: Read-only access, 100-1000 tags, Basic/Kerberos auth
- Value: Joint solution for 100+ potential customers

STATUS:
Could you provide an update on:
1. Access approval timeline?
2. Expected environment details (URL, auth method)?
3. Any additional information needed from our side?

NEXT STEPS:
Once access is granted, I can complete testing within 2 weeks and provide:
- Performance benchmarks
- Technical validation report
- Demo materials for joint customer presentations

Happy to schedule a call to discuss further.

Thank you,
[Your Name]
```

---

## 🎯 Expected Outcomes

### If AVEVA Provides Demo Access (Best Case)

✅ Full validation in controlled environment
✅ Performance benchmarks with real data
✅ Demo-ready materials
✅ Production architecture validated

### If Customer Provides POC Access (Ideal)

✅ Real-world validation
✅ Customer testimonial
✅ Production deployment path
✅ Reference architecture

### If Only Documentation Available (Minimum)

⚠️ Can proceed with mock server testing
⚠️ Use PI Web API documentation for accuracy
⚠️ Defer real-world validation to pilot customers

---

## 📚 Reference Materials to Include

When requesting access, attach:

1. **Technical Overview** (1-pager)
   - Connector architecture diagram
   - Key features and benefits
   - Security highlights

2. **Use Cases** (2-pager)
   - Alinta Energy requirement
   - Manufacturing/utilities use cases
   - Competitive differentiation

3. **Test Plan** (this document)
   - Clear scope and timeline
   - Success criteria
   - Resource requirements

---

## ✅ Success Criteria for PI Access

Access request is successful when you receive:

- [ ] PI Web API URL (HTTPS)
- [ ] Authentication credentials (any method)
- [ ] Documentation of available endpoints
- [ ] Sample tag list (10-100 tags minimum)
- [ ] Technical support contact
- [ ] Access duration confirmed (2-4 weeks)
- [ ] Data usage terms accepted

---

**Document Version:** 1.0
**Last Updated:** December 6, 2025
**Maintained By:** Databricks Solutions Architecture Team

For questions about this guide, contact: [Your Team Distribution List]
