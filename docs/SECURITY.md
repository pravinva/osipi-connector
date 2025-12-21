# Security Guidelines - OSI PI Connector

## Overview

This document outlines security best practices and guidelines for using the OSI PI Lakeflow Connector securely in production environments.

## 🔒 Security Features

### Authentication Security
- ✅ **SSL/TLS Verification Enforced** - All connections use certificate validation
- ✅ **Kerberos Mutual Authentication** - REQUIRED by default (not OPTIONAL)
- ✅ **Secure Credential Handling** - Passwords not stored in plain text
- ✅ **Input Validation** - Username, token, and URL validation
- ✅ **No Credential Logging** - Sensitive data never logged

### Network Security
- ✅ **HTTPS Enforced** - HTTP connections blocked in production mode
- ✅ **SSRF Protection** - URL validation prevents internal network access
- ✅ **Connection Timeouts** - Prevent resource exhaustion
- ✅ **Rate Limiting Support** - Configurable rate limiting to prevent abuse

### API Security
- ✅ **Request Size Limits** - Batch size limited to 1,000 items
- ✅ **Payload Size Validation** - 10MB maximum payload size
- ✅ **URL Length Validation** - 2KB maximum URL length
- ✅ **Input Sanitization** - All user inputs validated and sanitized

### Error Handling
- ✅ **Error Message Sanitization** - No sensitive data in error messages
- ✅ **Detailed Security Logging** - Security events logged separately
- ✅ **Exception Type Logging** - Error types logged, not details

## 🔐 Credential Management

### Recommended: Databricks Secrets

**✅ SECURE - Use This Method:**

```python
# Store credentials in Databricks Secrets
# CLI: databricks secrets create-scope pi-connector
# CLI: databricks secrets put pi-connector pi_username
# CLI: databricks secrets put pi-connector pi_password

from databricks import secrets

auth_config = {
    'type': 'basic',
    'username': dbutils.secrets.get('pi-connector', 'pi_username'),
    'password': dbutils.secrets.get('pi-connector', 'pi_password')
}
```

### Environment Variables (Local Development)

**⚠️ ACCEPTABLE FOR DEV ONLY:**

```bash
# .env file (DO NOT COMMIT)
PI_USERNAME=your_username
PI_PASSWORD=your_password
```

```python
import os
from dotenv import load_dotenv

load_dotenv()

auth_config = {
    'type': 'basic',
    'username': os.getenv('PI_USERNAME'),
    'password': os.getenv('PI_PASSWORD')
}
```

###❌ NEVER DO THIS:

```python
# ❌ INSECURE - Hardcoded credentials
auth_config = {
    'type': 'basic',
    'username': 'admin',  # ❌ NEVER hardcode
    'password': 'password123'  # ❌ NEVER hardcode
}
```

## 🌐 SSL/TLS Configuration

### Production (Recommended)

```python
from src.auth.pi_auth_manager import PIAuthManager

# ✅ SSL verification enabled (default)
auth_manager = PIAuthManager(
    auth_config=auth_config,
    verify_ssl=True  # Default, can be omitted
)
```

### Custom CA Certificate

```python
# For self-signed certificates or internal CAs
auth_manager = PIAuthManager(
    auth_config=auth_config,
    verify_ssl=True
)

# Test connection with custom CA bundle
auth_manager.test_connection(
    base_url="https://pi-server.example.com/piwebapi",
    ca_bundle="/path/to/ca-bundle.crt"
)
```

### Development Only (Insecure)

```python
# ⚠️ ONLY FOR DEVELOPMENT - Not for production
auth_manager = PIAuthManager(
    auth_config=auth_config,
    verify_ssl=False  # ⚠️ Disables SSL verification
)
```

## 🔑 Authentication Methods

### 1. Kerberos (Most Secure)

```python
# ✅ RECOMMENDED for enterprise environments
auth_config = {
    'type': 'kerberos'
}

# Mutual authentication REQUIRED by default (secure)
auth_manager = PIAuthManager(
    auth_config=auth_config,
    require_mutual_auth=True  # Default
)
```

### 2. OAuth 2.0 (Secure)

```python
# ✅ RECOMMENDED for modern deployments
auth_config = {
    'type': 'oauth',
    'oauth_token': dbutils.secrets.get('pi-connector', 'oauth_token')
}

auth_manager = PIAuthManager(auth_config=auth_config)
```

### 3. Basic Authentication (Least Secure)

```python
# ⚠️ ACCEPTABLE if HTTPS is enforced
auth_config = {
    'type': 'basic',
    'username': dbutils.secrets.get('pi-connector', 'pi_username'),
    'password': dbutils.secrets.get('pi-connector', 'pi_password')
}

# MUST use HTTPS!
auth_manager = PIAuthManager(
    auth_config=auth_config,
    verify_ssl=True  # REQUIRED for Basic Auth
)
```

## 🛡️ Input Validation

### URL Validation

```python
from src.utils.security import URLValidator

# Validate before using
url = "https://pi-server.example.com/piwebapi"

if URLValidator.validate_url(url):
    client = PIWebAPIClient(url, auth_manager)
else:
    raise ValueError("Invalid or insecure URL")
```

### WebId Validation

```python
from src.utils.security import InputValidator

webid = "F1DPQ0X9jJTUG0K-6g-2EFN0wQQUktTU9OQVwtU0VSVkVSXFRBRzAwMQ"

if not InputValidator.validate_webid(webid):
    raise ValueError("Invalid WebId format")
```

### Timestamp Validation

```python
timestamp = "2025-01-08T10:00:00Z"

if not InputValidator.validate_timestamp(timestamp):
    raise ValueError("Invalid timestamp format")
```

## ⏱️ Rate Limiting

### Implement Client-Side Rate Limiting

```python
from src.utils.security import RateLimiter

# Limit to 100 requests per minute
rate_limiter = RateLimiter(max_requests=100, time_window=60.0)

def safe_api_call():
    if rate_limiter.is_allowed():
        response = client.get("/piwebapi/dataservers")
        return response
    else:
        wait_time = rate_limiter.wait_time()
        raise Exception(f"Rate limit exceeded. Wait {wait_time:.1f}s")
```

## 📊 Security Logging

### Configure Secure Logging

```python
import logging

# Configure logging with security in mind
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pi_connector.log'),
        logging.StreamHandler()
    ]
)

# Security event logging
security_logger = logging.getLogger('security')
security_logger.setLevel(logging.WARNING)

# Log security events
security_logger.warning("Failed authentication attempt from IP: X.X.X.X")
```

### What to Log (✅) and NOT Log (❌)

**✅ DO LOG:**
- Authentication success/failure (without credentials)
- API endpoint accessed
- Error types (HTTPError, ConnectionError, etc.)
- Request timestamps
- Rate limit violations
- SSL verification failures

**❌ NEVER LOG:**
- Passwords or tokens
- Full API responses with sensitive data
- Internal IP addresses
- Stack traces in production
- User personal information

## 🔍 Security Monitoring

### Monitor These Events

1. **Failed Authentication Attempts**
   - Track failed login attempts
   - Alert on repeated failures (potential brute force)

2. **SSL/TLS Errors**
   - Certificate validation failures
   - Protocol downgrade attempts

3. **Rate Limit Violations**
   - Excessive API calls
   - Potential DoS attempts

4. **Suspicious URLs**
   - Internal IP access attempts
   - Unusual endpoint requests

### Example Monitoring Setup

```python
# Custom security monitor
class SecurityMonitor:
    def __init__(self):
        self.failed_auth_count = 0
        self.max_failures = 5

    def record_auth_failure(self, reason: str):
        self.failed_auth_count += 1
        logger.warning(f"Auth failure #{self.failed_auth_count}: {reason}")

        if self.failed_auth_count >= self.max_failures:
            logger.critical("⚠️ ALERT: Too many auth failures - potential attack")
            # Send alert to security team
            self._send_security_alert()

    def _send_security_alert(self):
        # Implement alerting (email, Slack, PagerDuty, etc.)
        pass
```

## 🧪 Security Testing

### Run Security Tests

```bash
# Activate virtual environment
source venv/bin/activate

# Run security-focused tests
python -m pytest tests/test_security.py -v

# Run with security scanner
bandit -r src/ -ll

# Check for known vulnerabilities
safety check

# Run all tests including security
python -m pytest tests/ -v --cov=src
```

### Penetration Testing Checklist

- [ ] SQL Injection attempts (if applicable)
- [ ] SSRF with internal IPs
- [ ] Authentication bypass attempts
- [ ] SSL/TLS downgrade attacks
- [ ] Rate limit bypass attempts
- [ ] Payload size DoS attacks
- [ ] Path traversal attempts
- [ ] Error message information disclosure

## 📋 Security Checklist

### Before Deployment

- [ ] All credentials stored in Databricks Secrets
- [ ] SSL verification enabled (`verify_ssl=True`)
- [ ] HTTPS enforced for all connections
- [ ] Kerberos mutual authentication required
- [ ] Rate limiting configured
- [ ] Batch size limits enforced
- [ ] Security logging enabled
- [ ] No hardcoded credentials in code
- [ ] `.env` files in `.gitignore`
- [ ] Dependencies scanned for vulnerabilities
- [ ] Security tests passing
- [ ] Code reviewed by security team

### Regular Security Tasks

- [ ] **Weekly:** Review security logs
- [ ] **Monthly:** Update dependencies
- [ ] **Quarterly:** Security audit
- [ ] **Annually:** Penetration testing

## 🚨 Incident Response

### If Credentials Are Compromised

1. **Immediate Actions:**
   - Rotate compromised credentials immediately
   - Revoke API tokens
   - Check audit logs for unauthorized access
   - Notify security team

2. **Investigation:**
   - Review logs for suspicious activity
   - Identify affected systems
   - Determine scope of breach

3. **Remediation:**
   - Update all affected systems
   - Implement additional monitoring
   - Document lessons learned

### Reporting Security Issues

**DO NOT** create public GitHub issues for security vulnerabilities.

Instead, report via:
- Email: security@your-company.com
- Private security advisory on GitHub
- Databricks support channel (for product issues)

## 📚 Additional Resources

### Security Standards
- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [CWE/SANS Top 25](https://cwe.mitre.org/top25/)
- [NIST Cybersecurity Framework](https://www.nist.gov/cyberframework)

### Best Practices
- [Databricks Security Best Practices](https://docs.databricks.com/security/index.html)
- [PI System Security Hardening](https://docs.osisoft.com/bundle/pi-server/page/security-hardening.html)
- [API Security Best Practices](https://owasp.org/www-project-api-security/)

### Tools
- **Safety:** Python dependency vulnerability scanner
- **Bandit:** Python security linter
- **OWASP ZAP:** Web application security scanner
- **Trivy:** Container and dependency scanner

## 📝 Security Policy Updates

**Last Updated:** December 6, 2025
**Next Review:** March 6, 2026
**Version:** 1.0

For questions or concerns about security, contact: security-team@your-company.com
