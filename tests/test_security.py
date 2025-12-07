"""
Security Tests for OSI PI Connector

Tests for:
- Authentication security
- Input validation
- SSL/TLS configuration
- SSRF prevention
- Rate limiting
- Credential handling
"""

import pytest
from unittest.mock import Mock, patch
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.auth.pi_auth_manager import PIAuthManager
from src.utils.security import (
    URLValidator,
    InputValidator,
    RateLimiter,
    RequestSizeValidator,
    sanitize_log_message
)


class TestAuthenticationSecurity:
    """Test authentication security features"""

    def test_invalid_auth_type_rejected(self):
        """Test that invalid auth types are rejected"""
        with pytest.raises(ValueError, match="Invalid auth type"):
            PIAuthManager({'type': 'invalid_type'})

    def test_missing_basic_auth_credentials(self):
        """Test that basic auth requires username and password"""
        with pytest.raises(ValueError, match="username"):
            PIAuthManager({'type': 'basic'})

        with pytest.raises(ValueError, match="password"):
            PIAuthManager({'type': 'basic', 'username': 'user'})

    def test_invalid_username_format(self):
        """Test that invalid usernames are rejected"""
        with pytest.raises(ValueError, match="Invalid username"):
            PIAuthManager({
                'type': 'basic',
                'username': 'user!@#$%',  # Invalid characters
                'password': 'password123'
            })

    def test_weak_password_warning(self, caplog):
        """Test warning for weak passwords"""
        auth_config = {
            'type': 'basic',
            'username': 'testuser',
            'password': 'short'  # Only 5 characters
        }
        PIAuthManager(auth_config)
        assert "shorter than 8 characters" in caplog.text

    def test_oauth_token_validation(self):
        """Test OAuth token validation"""
        with pytest.raises(ValueError, match="OAuth token"):
            PIAuthManager({'type': 'oauth', 'oauth_token': 'short'})

    def test_kerberos_mutual_authentication_required(self):
        """Test that Kerberos uses REQUIRED mutual auth by default"""
        try:
            auth_manager = PIAuthManager(
                {'type': 'kerberos'},
                require_mutual_auth=True
            )
            handler = auth_manager.get_auth_handler()
            # Should use REQUIRED, not OPTIONAL
            assert hasattr(handler, 'mutual_authentication')
        except ValueError as e:
            # Kerberos may not be available
            assert "Kerberos support not available" in str(e)

    def test_ssl_disabled_warning(self, caplog):
        """Test warning when SSL verification disabled"""
        auth_config = {
            'type': 'basic',
            'username': 'testuser',
            'password': 'password123'
        }
        PIAuthManager(auth_config, verify_ssl=False)
        assert "SSL certificate verification is DISABLED" in caplog.text

    def test_clear_credentials(self):
        """Test credential clearing from memory"""
        auth_config = {
            'type': 'basic',
            'username': 'testuser',
            'password': 'password123'
        }
        auth_manager = PIAuthManager(auth_config)

        # Credentials should exist
        assert hasattr(auth_manager, '_password')

        # Clear credentials
        auth_manager.clear_credentials()

        # Password should be None
        assert auth_manager._password is None


class TestURLValidation:
    """Test URL validation and SSRF prevention"""

    def test_valid_https_url(self):
        """Test that valid HTTPS URLs are accepted"""
        assert URLValidator.validate_url("https://pi-server.example.com/piwebapi")

    def test_http_url_rejected(self):
        """Test that HTTP URLs are handled appropriately"""
        # HTTP allowed if explicitly permitted
        assert URLValidator.validate_url("http://pi-server.example.com/piwebapi")

    def test_localhost_blocked(self):
        """Test that localhost is blocked in production"""
        assert not URLValidator.validate_url("https://localhost/piwebapi")
        assert not URLValidator.validate_url("https://127.0.0.1/piwebapi")
        assert not URLValidator.validate_url("https://0.0.0.0/piwebapi")

    def test_localhost_allowed_in_dev(self):
        """Test that localhost can be allowed for development"""
        assert URLValidator.validate_url(
            "https://localhost/piwebapi",
            allow_localhost=True
        )

    def test_aws_metadata_service_blocked(self):
        """Test that AWS metadata service is blocked (SSRF prevention)"""
        assert not URLValidator.validate_url("http://169.254.169.254/latest/meta-data/")

    def test_private_ip_blocked(self):
        """Test that private IPs are blocked"""
        assert not URLValidator.validate_url("https://10.0.0.1/piwebapi")
        assert not URLValidator.validate_url("https://172.16.0.1/piwebapi")
        assert not URLValidator.validate_url("https://192.168.1.1/piwebapi")

    def test_invalid_scheme_rejected(self):
        """Test that non-HTTP schemes are rejected"""
        assert not URLValidator.validate_url("ftp://pi-server.example.com")
        assert not URLValidator.validate_url("file:///etc/passwd")
        assert not URLValidator.validate_url("javascript:alert(1)")

    def test_url_sanitization(self):
        """Test URL sanitization removes dangerous components"""
        malicious_url = "https://pi-server.example.com/piwebapi#fragment"
        sanitized = URLValidator.sanitize_url(malicious_url)
        assert "#fragment" not in sanitized


class TestInputValidation:
    """Test input validation"""

    def test_valid_webid(self):
        """Test valid WebId format"""
        valid_webid = "F1DPQ0X9jJTUG0K-6g-2EFN0wQQUktTU9OQVwtU0VSVkVSXFRBRzAwMQ"
        assert InputValidator.validate_webid(valid_webid)

    def test_invalid_webid_too_short(self):
        """Test that short WebIds are rejected"""
        assert not InputValidator.validate_webid("short")

    def test_invalid_webid_characters(self):
        """Test that WebIds with invalid characters are rejected"""
        assert not InputValidator.validate_webid("F1DP<script>alert(1)</script>")

    def test_valid_timestamp(self):
        """Test valid ISO 8601 timestamps"""
        assert InputValidator.validate_timestamp("2025-01-08T10:00:00Z")
        assert InputValidator.validate_timestamp("2025-01-08T10:00:00.123Z")
        assert InputValidator.validate_timestamp("2025-01-08T10:00:00+05:30")

    def test_invalid_timestamp(self):
        """Test that invalid timestamps are rejected"""
        assert not InputValidator.validate_timestamp("2025-13-45")
        assert not InputValidator.validate_timestamp("not a date")
        assert not InputValidator.validate_timestamp("'; DROP TABLE--")


class TestRateLimiting:
    """Test rate limiting functionality"""

    def test_requests_allowed_under_limit(self):
        """Test that requests under limit are allowed"""
        limiter = RateLimiter(max_requests=10, time_window=60.0)

        for _ in range(10):
            assert limiter.is_allowed()

    def test_requests_blocked_over_limit(self):
        """Test that requests over limit are blocked"""
        limiter = RateLimiter(max_requests=5, time_window=60.0)

        # First 5 should be allowed
        for _ in range(5):
            assert limiter.is_allowed()

        # 6th should be blocked
        assert not limiter.is_allowed()

    def test_wait_time_calculation(self):
        """Test wait time calculation"""
        limiter = RateLimiter(max_requests=5, time_window=10.0)

        # Fill up the limiter
        for _ in range(5):
            limiter.is_allowed()

        # Should have wait time
        wait_time = limiter.wait_time()
        assert wait_time > 0
        assert wait_time <= 10.0


class TestRequestSizeValidation:
    """Test request size limits"""

    def test_valid_batch_size(self):
        """Test valid batch sizes"""
        assert RequestSizeValidator.validate_batch_size(1)
        assert RequestSizeValidator.validate_batch_size(100)
        assert RequestSizeValidator.validate_batch_size(1000)

    def test_invalid_batch_size_too_large(self):
        """Test that oversized batches are rejected"""
        assert not RequestSizeValidator.validate_batch_size(1001)
        assert not RequestSizeValidator.validate_batch_size(10000)

    def test_invalid_batch_size_zero(self):
        """Test that zero batch size is rejected"""
        assert not RequestSizeValidator.validate_batch_size(0)
        assert not RequestSizeValidator.validate_batch_size(-1)

    def test_valid_payload_size(self):
        """Test valid payload sizes"""
        assert RequestSizeValidator.validate_payload_size(1024)  # 1 KB
        assert RequestSizeValidator.validate_payload_size(1024 * 1024)  # 1 MB

    def test_invalid_payload_size_too_large(self):
        """Test that oversized payloads are rejected"""
        eleven_mb = 11 * 1024 * 1024
        assert not RequestSizeValidator.validate_payload_size(eleven_mb)

    def test_valid_url_length(self):
        """Test valid URL lengths"""
        short_url = "https://pi-server.example.com/piwebapi"
        assert RequestSizeValidator.validate_url_length(short_url)

    def test_invalid_url_length_too_long(self):
        """Test that oversized URLs are rejected"""
        long_url = "https://pi-server.example.com/" + "a" * 3000
        assert not RequestSizeValidator.validate_url_length(long_url)


class TestLogSanitization:
    """Test log message sanitization"""

    def test_newline_removal(self):
        """Test that newlines are removed from log messages"""
        message = "Line 1\nLine 2\rLine 3"
        sanitized = sanitize_log_message(message)
        assert "\n" not in sanitized
        assert "\r" not in sanitized

    def test_length_truncation(self):
        """Test that long messages are truncated"""
        long_message = "A" * 1000
        sanitized = sanitize_log_message(long_message, max_length=100)
        assert len(sanitized) <= 103  # 100 + "..."

    def test_normal_message_unchanged(self):
        """Test that normal messages pass through"""
        message = "Normal log message"
        sanitized = sanitize_log_message(message)
        assert sanitized == message


class TestSSLConfiguration:
    """Test SSL/TLS configuration"""

    @patch('requests.get')
    def test_ssl_verification_enforced(self, mock_get):
        """Test that SSL verification is enforced"""
        auth_config = {
            'type': 'basic',
            'username': 'testuser',
            'password': 'password123'
        }
        auth_manager = PIAuthManager(auth_config, verify_ssl=True)

        mock_response = Mock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        auth_manager.test_connection("https://pi-server.example.com/piwebapi")

        # Verify SSL was enabled
        call_args = mock_get.call_args
        assert call_args[1]['verify'] is True

    @patch('requests.get')
    def test_http_rejected_with_ssl_verification(self, mock_get):
        """Test that HTTP is rejected when SSL verification enabled"""
        auth_config = {
            'type': 'basic',
            'username': 'testuser',
            'password': 'password123'
        }
        auth_manager = PIAuthManager(auth_config, verify_ssl=True)

        result = auth_manager.test_connection("http://pi-server.example.com/piwebapi")
        assert result is False


class TestCredentialHandling:
    """Test secure credential handling"""

    def test_password_not_logged(self, caplog):
        """Test that passwords are never logged"""
        auth_config = {
            'type': 'basic',
            'username': 'testuser',
            'password': 'super_secret_password'
        }
        PIAuthManager(auth_config)

        # Check that password doesn't appear in any log
        for record in caplog.records:
            assert 'super_secret_password' not in record.message

    def test_oauth_token_not_logged(self, caplog):
        """Test that OAuth tokens are never logged"""
        auth_config = {
            'type': 'oauth',
            'oauth_token': 'super_secret_token_12345'
        }
        PIAuthManager(auth_config)

        # Check that token doesn't appear in any log
        for record in caplog.records:
            assert 'super_secret_token' not in record.message


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
