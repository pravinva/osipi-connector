"""
Security Utilities

Helper functions for secure operations:
- URL validation and sanitization
- Input validation
- Rate limiting
- Request size limits
"""

from typing import Optional
from urllib.parse import urlparse, urlunparse
import re
import time
from collections import deque


class URLValidator:
    """
    Secure URL validation to prevent SSRF and injection attacks
    """

    # Allowed URL schemes
    ALLOWED_SCHEMES = {'http', 'https'}

    # Blocked hostnames (internal/private networks)
    BLOCKED_HOSTNAMES = {
        'localhost', '127.0.0.1', '0.0.0.0',
        '169.254.169.254',  # AWS metadata service
        '::1',  # IPv6 localhost
    }

    @classmethod
    def validate_url(cls, url: str, allow_localhost: bool = False) -> bool:
        """
        Validate URL for security

        Args:
            url: URL to validate
            allow_localhost: Allow localhost URLs (for testing)

        Returns:
            True if URL is valid and safe
        """
        try:
            parsed = urlparse(url)

            # Must have scheme and netloc
            if not parsed.scheme or not parsed.netloc:
                return False

            # Only allow http/https
            if parsed.scheme not in cls.ALLOWED_SCHEMES:
                return False

            # Check hostname
            hostname = parsed.netloc.split(':')[0].lower()

            # Block private/internal IPs
            if not allow_localhost and hostname in cls.BLOCKED_HOSTNAMES:
                return False

            # Block private IP ranges
            if not allow_localhost and cls._is_private_ip(hostname):
                return False

            return True

        except Exception:
            return False

    @classmethod
    def sanitize_url(cls, url: str) -> str:
        """
        Sanitize URL by removing dangerous components

        Args:
            url: URL to sanitize

        Returns:
            Sanitized URL
        """
        try:
            parsed = urlparse(url)

            # Remove fragments and user info
            sanitized = urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                parsed.query,
                ''  # Remove fragment
            ))

            return sanitized

        except Exception:
            return url

    @staticmethod
    def _is_private_ip(hostname: str) -> bool:
        """
        Check if hostname is a private IP address

        Args:
            hostname: Hostname to check

        Returns:
            True if private IP
        """
        # Simple regex for private IP ranges
        private_patterns = [
            r'^10\.',
            r'^172\.(1[6-9]|2[0-9]|3[01])\.',
            r'^192\.168\.',
        ]

        for pattern in private_patterns:
            if re.match(pattern, hostname):
                return True

        return False


class InputValidator:
    """
    Input validation for API parameters
    """

    @staticmethod
    def validate_webid(webid: str) -> bool:
        """
        Validate PI Web API WebId format

        Args:
            webid: WebId to validate

        Returns:
            True if valid format
        """
        # WebIds are base64-encoded, typically start with F1
        if not webid or len(webid) < 10:
            return False

        # Should only contain base64 characters
        if not re.match(r'^[A-Za-z0-9+/=_-]+$', webid):
            return False

        return True

    @staticmethod
    def validate_timestamp(timestamp: str) -> bool:
        """
        Validate ISO 8601 timestamp format

        Args:
            timestamp: Timestamp string

        Returns:
            True if valid format
        """
        # Basic ISO 8601 format validation
        iso_pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?$'
        return bool(re.match(iso_pattern, timestamp))


class RateLimiter:
    """
    Token bucket rate limiter to prevent API abuse
    """

    def __init__(self, max_requests: int = 100, time_window: float = 60.0):
        """
        Initialize rate limiter

        Args:
            max_requests: Maximum requests allowed in time window
            time_window: Time window in seconds
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = deque()

    def is_allowed(self) -> bool:
        """
        Check if request is allowed under rate limit

        Returns:
            True if request allowed
        """
        now = time.time()

        # Remove expired requests
        while self.requests and self.requests[0] < now - self.time_window:
            self.requests.popleft()

        # Check if under limit
        if len(self.requests) < self.max_requests:
            self.requests.append(now)
            return True

        return False

    def wait_time(self) -> float:
        """
        Calculate wait time until next request allowed

        Returns:
            Seconds to wait
        """
        if not self.requests:
            return 0.0

        oldest_request = self.requests[0]
        elapsed = time.time() - oldest_request

        if elapsed >= self.time_window:
            return 0.0

        return self.time_window - elapsed


class RequestSizeValidator:
    """
    Validate request sizes to prevent DoS
    """

    # Maximum sizes (configurable)
    MAX_BATCH_SIZE = 1000
    MAX_PAYLOAD_SIZE = 10 * 1024 * 1024  # 10 MB
    MAX_URL_LENGTH = 2048

    @classmethod
    def validate_batch_size(cls, batch_size: int) -> bool:
        """
        Validate batch request size

        Args:
            batch_size: Number of items in batch

        Returns:
            True if within limits
        """
        return 0 < batch_size <= cls.MAX_BATCH_SIZE

    @classmethod
    def validate_payload_size(cls, payload_bytes: int) -> bool:
        """
        Validate payload size

        Args:
            payload_bytes: Size in bytes

        Returns:
            True if within limits
        """
        return 0 < payload_bytes <= cls.MAX_PAYLOAD_SIZE

    @classmethod
    def validate_url_length(cls, url: str) -> bool:
        """
        Validate URL length

        Args:
            url: URL to validate

        Returns:
            True if within limits
        """
        return 0 < len(url) <= cls.MAX_URL_LENGTH


def sanitize_log_message(message: str, max_length: int = 500) -> str:
    """
    Sanitize log message to prevent log injection

    Args:
        message: Message to sanitize
        max_length: Maximum message length

    Returns:
        Sanitized message
    """
    # Remove newlines and control characters
    sanitized = re.sub(r'[\r\n\t]', ' ', message)

    # Truncate if too long
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length] + '...'

    return sanitized
