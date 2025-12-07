# PI System Access Request - Technical Specifications

**For:** Databricks → AVEVA Alliance Team
**Purpose:** Connector validation and customer demo preparation

---

## 🎯 Executive Request Summary

**What:** Read-only access to PI Web API demo/sandbox environment
**Duration:** 4 weeks (2 weeks testing + 2 weeks demo prep)
**Purpose:** Validate production-ready Lakeflow connector for joint customer opportunities

---

## 📋 Required PI System Configuration

### **Tier 1: MINIMUM (Must Have)**

This is the bare minimum to validate basic functionality:

#### **1. PI Web API Access**
```
Component: PI Web API 2019+ (or later)
URL Format: https://[server]/piwebapi
Authentication: Basic Auth (username/password)
SSL/TLS: Valid certificate (or provide CA bundle for self-signed)
Network: Accessible from public internet OR VPN credentials provided
```

#### **2. PI Data Archive**
```
Version: PI Data Archive 2018+ (or later)
Tags Required: 100-200 tags minimum
Data Types: Primarily Float32/Float64 (analog sensors)
Historical Data: 7-30 days of data
Update Frequency: Any (1s, 1min, 5min - doesn't matter)
Data Quality: Mix of Good/Questionable/Substituted flags (realistic)
```

#### **3. Sample Tags Should Include**
```
- Temperature sensors (10-20 tags)
- Pressure sensors (10-20 tags)
- Flow rates (10-20 tags)
- Level indicators (10-20 tags)
- Status/Digital tags (10-20 tags)
- Calculated tags (5-10 tags) - optional but nice
```

**Example Tag Naming:**
- `PLANT-A.UNIT-01.TEMP-001.PV`
- `SITE-B.REACTOR-02.PRESS-101.PV`
- `FACILITY-C.TANK-03.LEVEL-201.PV`

#### **4. Required API Endpoints**
```http
GET /piwebapi                          # Root endpoint (version check)
GET /piwebapi/dataservers              # List PI servers
GET /piwebapi/dataservers/{id}/points  # List tags
GET /piwebapi/streams/{webid}/recorded # Get time-series data
POST /piwebapi/batch                   # Batch requests (CRITICAL!)
```

---

### **Tier 2: IDEAL (Strongly Recommended)**

This enables full connector validation including advanced features:

#### **5. PI Asset Framework (AF)**
```
Component: PI AF Server 2018+ (or later)
Database: 1 AF database with sample hierarchy
Elements: 50-100 elements in 3-5 level hierarchy
Templates: 3-5 element templates (e.g., Pump, Tank, Reactor)
Attributes: Mix of PI Point references and table lookups
```

**Example AF Hierarchy:**
```
Enterprise
├── Site 1 (Sydney Plant)
│   ├── Production Unit A
│   │   ├── Reactor-101
│   │   │   ├── Temperature (PI Point ref)
│   │   │   ├── Pressure (PI Point ref)
│   │   │   └── Status (String attribute)
│   │   ├── Pump-201
│   │   └── Tank-301
│   └── Production Unit B
└── Site 2 (Melbourne Plant)
    └── ...
```

#### **6. Event Frames** (Optional but Valuable)
```
Component: PI AF Event Frames
Template Types:
  - Batch/Production runs (10-20 events)
  - Maintenance events (5-10 events)
  - Alarm events (5-10 events)
Time Range: Last 30 days
Attributes: Product type, duration, operator, etc.
```

**Example Event Frames:**
```
Event: "Batch-2024-12-06-001"
  Template: BatchRunTemplate
  Start Time: 2024-12-06 08:00:00
  End Time: 2024-12-06 14:30:00
  Attributes:
    - Product: "Chemical-A"
    - Operator: "John Smith"
    - Yield: 95.5%
    - Status: "Complete"
```

---

### **Tier 3: BONUS (Nice to Have)**

These features would enable advanced demos but not required:

#### **7. Large-Scale Testing**
```
Tags: 1,000-5,000 tags (for performance benchmarking)
Historical Data: 90-365 days (for backfill testing)
High-Frequency Data: Sub-second data (0.1s, 0.5s updates)
```

#### **8. Advanced PI Features**
```
- PI Notifications (for real-time alerts)
- PI Vision screens (for visualization reference)
- PI Analysis (for calculated values)
- PI Manual Logger data
```

---

## 🔐 Authentication & Security Requirements

### **Preferred Authentication (in order)**

**1. Basic Authentication** ✅ EASIEST
```yaml
Type: Basic Auth
Provide: Username + Password
SSL: HTTPS required
Benefits: Simple to implement, works immediately
```

**2. OAuth 2.0** ✅ MOST SECURE
```yaml
Type: OAuth 2.0 Bearer Token
Provide: Client ID, Client Secret, Token endpoint
Refresh: Token refresh mechanism
Benefits: Best security, modern approach
```

**3. Kerberos** ⚠️ COMPLEX
```yaml
Type: Kerberos/Windows Authentication
Provide: Domain credentials, KDC configuration
Requirements: May need VPN or domain join
Benefits: Enterprise-grade, SSO support
```

### **Network Access**

**Option A: Public Internet** (Preferred)
```
URL: https://demo-pi.aveva.com/piwebapi
Access: Direct HTTPS from anywhere
Firewall: None required
```

**Option B: VPN Access**
```
URL: https://internal-pi.company.local/piwebapi
Access: VPN client required
Provide: VPN config file or credentials
```

**Option C: IP Whitelist**
```
URL: https://pi-demo.aveva.com/piwebapi
Access: Databricks workspace IPs whitelisted
Provide: List of IPs to whitelist
```

---

## 📊 Data Volume Requirements

### **Minimum Data Profile**

```yaml
Tags: 100-200
Time Range: 7-30 days
Update Frequency: 1-5 minutes (or any)
Data Points per Tag: ~10,000-50,000
Total Data Points: ~1-10 million
Storage Size: ~100MB-1GB (negligible)
```

### **Ideal Data Profile**

```yaml
Tags: 500-1,000
Time Range: 30-90 days
Update Frequency: Mixed (1s to 5min)
Data Points per Tag: ~50,000-500,000
Total Data Points: ~25-500 million
Storage Size: ~1-10GB
```

### **Why These Numbers?**

- **100 tags:** Validate batch controller (100 tags in 1 HTTP request)
- **1,000 tags:** Performance benchmarking (show 100x speedup vs sequential)
- **7-30 days:** Sufficient for incremental load testing
- **10,000 points/tag:** Test paging mechanism (PI Web API limit per request)

---

## 🎯 Use Case Examples to Mention

When requesting, cite these specific use cases:

### **Use Case 1: Alinta Energy** (Real Customer)
```
Customer: Alinta Energy (Australia)
Requirement: "PI AF and Event Frame connectivity" (April 2024 request)
Scale: 30,000+ tags
Current Limitation: AVEVA CDS limited to 2,000 tags
Our Solution: Native Lakeflow with batch controller (no limits)
```

### **Use Case 2: Utilities/Energy**
```
Industry: Power generation, water utilities
Data: SCADA/OT sensor data (flow, pressure, temperature)
Volume: 5,000-50,000 tags typical
Use Cases: Asset performance, predictive maintenance, compliance
```

### **Use Case 3: Process Manufacturing**
```
Industry: Chemical, pharmaceutical, food & beverage
Data: Process variables, batch data, quality metrics
Volume: 10,000-100,000 tags typical
Use Cases: Process optimization, quality analytics, batch genealogy
```

---

## 📝 Specific Questions to Ask AVEVA

When the alliance team connects you, ask these questions:

### **Access Details**
1. What is the PI Web API URL?
2. What authentication method is available? (Basic/OAuth/Kerberos)
3. Are there any rate limits or throttling?
4. What is the session timeout?
5. Is there a test/sandbox vs production environment?

### **Data Availability**
6. How many tags are available in the demo environment?
7. What is the data retention period?
8. Can we get a list of available tags before starting?
9. Are there any restricted tags or data we should avoid?
10. What time range has the most complete data?

### **PI Asset Framework**
11. Is PI AF available in this environment?
12. What AF databases are accessible?
13. How many elements are in the hierarchy?
14. Are Event Frames available?
15. What Event Frame templates exist?

### **Technical Support**
16. Who is the technical contact for issues?
17. What are the support hours/SLA?
18. Is there documentation specific to this environment?
19. Can we schedule a technical onboarding call?
20. What is the process for requesting additional access if needed?

---

## 📧 Request Email Template

Use this template when the alliance team connects you:

```
To: AVEVA Technical Team
Cc: Databricks Alliance Manager
Subject: PI System Demo Environment - Technical Requirements

Hello AVEVA Team,

Thank you for providing demo environment access for our Lakeflow connector validation.
Below are the technical requirements to ensure successful testing:

REQUIRED (Tier 1 - Must Have):
✅ PI Web API 2019+ with HTTPS access
✅ 100-200 tags with 7-30 days of historical data
✅ Basic Authentication (username/password)
✅ Batch endpoint access (/piwebapi/batch) - CRITICAL for performance
✅ Standard endpoints: dataservers, points, streams/recorded

IDEAL (Tier 2 - Strongly Recommended):
✅ PI Asset Framework with sample hierarchy (50-100 elements)
✅ Event Frames (10-20 sample events)
✅ 500-1,000 tags for performance benchmarking

NICE TO HAVE (Tier 3 - Bonus):
⚪ 1,000+ tags for large-scale testing
⚪ 90+ days historical data
⚪ High-frequency sub-second data

AUTHENTICATION PREFERENCE:
1st Choice: Basic Auth (username/password)
2nd Choice: OAuth 2.0 (with refresh token)
3rd Choice: Kerberos (if required)

USE CASE CONTEXT:
This connector addresses Alinta Energy's April 2024 request for native PI connectivity
and will serve 100+ potential joint Databricks-AVEVA customers in utilities and
manufacturing sectors.

TIMELINE:
- Weeks 1-2: Connectivity and data extraction validation
- Weeks 3-4: Demo preparation for joint customer presentations
- Deliverable: Production-ready connector + reference architecture

QUESTIONS:
1. What is the PI Web API URL and authentication method?
2. Can you provide a sample list of available tags?
3. Who is the technical contact for troubleshooting?
4. Is there environment-specific documentation?

Please let me know the best time for a technical onboarding call.

Thank you,
[Your Name]
Databricks Solutions Architect
[Email] | [Phone]
```

---

## ✅ Acceptance Criteria

You should accept the environment if it provides:

### **MINIMUM ACCEPTABLE**
- [x] PI Web API URL (HTTPS)
- [x] Authentication credentials (any method)
- [x] 50+ tags with data
- [x] 7+ days of historical data
- [x] Batch endpoint working
- [x] Technical support contact

### **IDEAL ENVIRONMENT**
- [x] Everything above, PLUS:
- [x] 100+ tags
- [x] 30+ days historical data
- [x] PI Asset Framework access
- [x] Sample Event Frames
- [x] Documentation provided
- [x] Technical onboarding call scheduled

### **RED FLAGS** (Push back if you see these)
- ❌ HTTP only (no HTTPS) - security risk
- ❌ Less than 20 tags - insufficient for testing
- ❌ Less than 24 hours of data - can't test properly
- ❌ No batch endpoint - defeats main performance feature
- ❌ No technical support - will get stuck on issues
- ❌ Excessive rate limiting (<10 requests/min)

---

## 🎯 What to Validate First (Priority Order)

Once you get access, test in this order:

### **Day 1: Connectivity** (2 hours)
1. SSL/TLS connection successful
2. Authentication working
3. Can list PI servers
4. Can list tags
5. Can retrieve single tag data

### **Day 2: Batch Performance** (4 hours)
6. Batch endpoint working (100 tags in 1 request)
7. Performance benchmark (compare vs sequential)
8. Large batch (500-1000 tags if available)

### **Day 3: Data Quality** (3 hours)
9. Quality flags preserved (Good/Bad/Questionable)
10. Timestamps accurate (timezone handling)
11. Data types correct (floats, ints, strings)

### **Day 4-5: Advanced Features** (8 hours)
12. Paging mechanism (>10,000 records per tag)
13. PI Asset Framework hierarchy
14. Event Frames extraction

### **Week 2: Integration** (20 hours)
15. Full Lakeflow pipeline
16. Unity Catalog tables
17. Incremental loading
18. Performance optimization

---

## 📊 Expected Performance Targets

When you test, aim for these benchmarks:

### **Batch Controller Performance**
```
Test: 100 tags × 10,000 records × 7 days
Expected: <15 seconds total
Comparison: Sequential would take 15-20 minutes
Speedup: ~60-80x faster
```

### **Large Scale Performance**
```
Test: 1,000 tags × 10,000 records × 1 day
Expected: <2 minutes total
Throughput: ~5 million data points/minute
```

### **Incremental Load**
```
Test: 100 tags × 1 hour new data
Expected: <5 seconds
Freshness: Near real-time capability
```

---

## 💡 Pro Tips for the Request

### **DO Say:**
✅ "Read-only access only"
✅ "2-4 week timeline"
✅ "Joint GTM opportunity for 100+ customers"
✅ "Alinta Energy use case validation"
✅ "Production-ready connector already developed"
✅ "Will provide performance benchmarks back to AVEVA"

### **DON'T Say:**
❌ "Need write access"
❌ "Unlimited duration"
❌ "Just exploring/experimenting"
❌ "Might not use it"
❌ "Building a competing product"
❌ "Will share data publicly"

### **Emphasize:**
💡 **Mutual benefit:** Validates AVEVA technology at scale
💡 **Customer demand:** Real customer requested this (Alinta)
💡 **Market opportunity:** 100+ potential joint customers
💡 **Technical validation:** Proves PI Web API performance
💡 **Partnership strengthening:** Deepens Databricks-AVEVA relationship

---

## 📞 Follow-Up Plan

If you don't hear back in 3 business days:

**Day 3 Follow-up:**
```
Subject: Following up: PI Demo Environment Request

Quick follow-up on my PI System access request from [date].

Could you provide an update on:
- Expected timeline for access?
- Any blockers or additional info needed?
- Best contact for technical questions?

Happy to schedule a call to accelerate.

Thanks!
```

**Day 7 Follow-up:**
```
Subject: Urgent: PI Access for Customer Demo

We have a customer demo scheduled for [date] and need PI access to validate.

Alternative: Can we schedule a joint demo session where AVEVA provides
screen share access to PI System for proof-of-concept?

This would be sufficient for initial validation.

Please advise urgently.
```

---

## 📚 Reference Documents to Attach

When requesting, attach these:

1. **This document** (PI_SYSTEM_REQUEST_SPECS.md)
2. **Technical overview** (1-page architecture diagram)
3. **Security audit** (SECURITY.md - proves production-ready)
4. **Test plan** (Phase 1-4 validation plan)

---

## ✅ Pre-Request Checklist

Before sending the request, confirm:

- [ ] Databricks alliance manager looped in
- [ ] Clear use case articulated (Alinta Energy)
- [ ] Timeline specified (4 weeks)
- [ ] Technical requirements documented (this doc)
- [ ] Success criteria defined
- [ ] Follow-up plan ready
- [ ] Alternative options considered (if denied)

---

**This document answers: "What sort of PI system should I be requesting?"**

**Quick Answer:**
- **Minimum:** PI Web API with 100 tags, 7 days data, Basic Auth
- **Ideal:** Above + PI AF with 50-100 elements + Event Frames
- **Access:** HTTPS, read-only, 4 weeks duration

**Priority:** Get access to **PI Web API with batch endpoint** - that's the critical piece for performance validation.

---

**Document Version:** 1.0
**Last Updated:** December 6, 2025
**Owner:** Databricks Solutions Architecture

For questions: [Your contact info]
