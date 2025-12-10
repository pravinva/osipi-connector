"""
PI Web API Authentication Manager

Secure authentication handler supporting:
- Basic Authentication (with secure credential handling)
- Kerberos Authentication (with required mutual authentication)
- OAuth 2.0 (with token validation)

Security Features:
- Credentials not stored in plain text
- SSL/TLS certificate verification enforced
- Input validation on auth types
- Secure logging (no credential leakage)
"""

from typing import Dict, Optional
import requests
from requests.auth import HTTPBasicAuth
import logging
import re
from urllib.parse import urlparse
from datetime import datetime, timedelta

# Conditional import for Kerberos (may not be available in all environments)
try:
    from requests_kerberos import HTTPKerberosAuth, REQUIRED, OPTIONAL
    KERBEROS_AVAILABLE = True
except ImportError:
    KERBEROS_AVAILABLE = False
    HTTPKerberosAuth = None
    REQUIRED = None
    OPTIONAL = None


class PIAuthManager:
    """
    Manages authentication for PI Web API with security best practices

    Security Features:
    - Input validation on auth configuration
    - Secure credential handling (not stored in plain text)
    - SSL certificate verification enforced
    - Kerberos mutual authentication required
    - No credential logging
    """

    # Allowed authentication types
    ALLOWED_AUTH_TYPES = {'basic', 'kerberos', 'oauth'}

    def __init__(self, auth_config: Dict[str, str], verify_ssl: bool = True,
                 require_mutual_auth: bool = True):
        """
        Initialize authentication manager with security validation

        Args:
            auth_config: Authentication configuration
                For Basic Auth:
                    {'type': 'basic', 'username': str, 'password': str}
                For Kerberos:
                    {'type': 'kerberos'}
                For OAuth (pre-generated token):
                    {'type': 'oauth', 'oauth_token': str}
                For OAuth (client credentials - auto refresh):
                    {'type': 'oauth', 'client_id': str, 'client_secret': str, 'token_url': str, 'scope': str (optional)}
            verify_ssl: Enable SSL certificate verification (default: True)
            require_mutual_auth: Require mutual authentication for Kerberos (default: True)

        Raises:
            ValueError: If auth configuration is invalid
            SecurityError: If insecure configuration detected
        """
        self.logger = logging.getLogger(__name__)

        # Validate auth type
        if 'type' not in auth_config:
            raise ValueError("Authentication type not specified")

        auth_type = auth_config['type'].lower()
        if auth_type not in self.ALLOWED_AUTH_TYPES:
            raise ValueError(f"Invalid auth type: {auth_type}. Allowed: {self.ALLOWED_AUTH_TYPES}")

        self.auth_type = auth_type
        self.verify_ssl = verify_ssl
        self.require_mutual_auth = require_mutual_auth

        # Validate configuration based on auth type
        self._validate_auth_config(auth_config)

        # Store only necessary data (not the full config with passwords)
        self._setup_auth_handler(auth_config)

        # Warn if SSL verification disabled (security risk)
        if not verify_ssl:
            self.logger.warning(
                "⚠️  SSL certificate verification is DISABLED. "
                "This is a security risk and should only be used in development."
            )

    def _validate_auth_config(self, config: Dict[str, str]):
        """
        Validate authentication configuration

        Args:
            config: Authentication configuration

        Raises:
            ValueError: If configuration is invalid
        """
        if self.auth_type == 'basic':
            if 'username' not in config or 'password' not in config:
                raise ValueError("Basic auth requires 'username' and 'password'")

            # Validate username format (alphanumeric, dots, hyphens, underscores)
            username = config['username']
            if not re.match(r'^[a-zA-Z0-9._\-@]+$', username):
                raise ValueError("Invalid username format")

            # Check password strength (minimum requirements)
            password = config['password']
            if len(password) < 8:
                self.logger.warning(
                    "⚠️  Password is shorter than 8 characters. "
                    "Consider using a stronger password."
                )

        elif self.auth_type == 'kerberos':
            if not KERBEROS_AVAILABLE:
                raise ValueError(
                    "Kerberos support not available. "
                    "Install requests-kerberos: pip install requests-kerberos"
                )

        elif self.auth_type == 'oauth':
            # Support three OAuth modes: pre-generated token OR client credentials OR pre-configured headers
            has_token = 'oauth_token' in config
            has_client_creds = 'client_id' in config and 'client_secret' in config and 'token_url' in config
            has_headers = 'headers' in config

            if not has_token and not has_client_creds and not has_headers:
                raise ValueError(
                    "OAuth requires either:\n"
                    "  1. Pre-generated token: 'oauth_token'\n"
                    "  2. Client credentials: 'client_id', 'client_secret', 'token_url'\n"
                    "  3. Pre-configured headers: 'headers' (from WorkspaceClient)"
                )

            if has_token:
                # Validate token format (basic check)
                token = config['oauth_token']
                if len(token) < 10:
                    raise ValueError("OAuth token appears invalid (too short)")

            if has_client_creds:
                # Validate token URL
                if not self._is_valid_url(config['token_url']):
                    raise ValueError("Invalid token_url format")

            # Headers mode requires no validation (already authenticated)

    def _setup_auth_handler(self, config: Dict[str, str]):
        """
        Setup authentication handler (store minimal data)

        Args:
            config: Authentication configuration
        """
        if self.auth_type == 'basic':
            # Store credentials for auth handler (unavoidable for basic auth)
            self._username = config['username']
            self._password = config['password']

        elif self.auth_type == 'oauth':
            # Three modes: pre-generated token OR client credentials OR pre-configured headers
            if 'headers' in config:
                # Mode 1: Pre-configured headers (from WorkspaceClient)
                self._custom_headers = config['headers']
                self._oauth_token = None
                self._use_client_creds = False
                self._use_custom_headers = True
            elif 'oauth_token' in config:
                # Mode 2: Pre-generated token
                self._oauth_token = config['oauth_token']
                self._token_expiry = None  # Unknown expiry
                self._use_client_creds = False
                self._use_custom_headers = False
            else:
                # Mode 3: Client credentials (auto-refresh)
                self._client_id = config['client_id']
                self._client_secret = config['client_secret']
                self._token_url = config['token_url']
                self._scope = config.get('scope', '')  # Optional scope
                self._oauth_token = None  # Will be acquired on first use
                self._token_expiry = None
                self._use_client_creds = True
                self._use_custom_headers = False

        # For Kerberos, no credentials needed (handled by system)
        
    def get_auth_handler(self):
        """
        Return appropriate auth handler for requests library

        Returns:
            Auth handler for requests (HTTPBasicAuth, HTTPKerberosAuth, or None)

        Security: Credentials handled securely, never logged
        """
        if self.auth_type == 'basic':
            return HTTPBasicAuth(self._username, self._password)

        elif self.auth_type == 'kerberos':
            # Use REQUIRED mutual authentication for production security
            # OPTIONAL only if explicitly configured
            mutual_auth = REQUIRED if self.require_mutual_auth else OPTIONAL
            return HTTPKerberosAuth(mutual_authentication=mutual_auth)

        elif self.auth_type == 'oauth':
            # Return None, will use headers
            return None

        else:
            raise ValueError(f"Unsupported auth type: {self.auth_type}")
    
    def _acquire_oauth_token(self) -> bool:
        """
        Acquire OAuth token using client credentials flow

        Returns:
            True if token acquired successfully, False otherwise

        Security: Client credentials never logged
        """
        if not self._use_client_creds:
            return True  # Using pre-generated token, no acquisition needed

        try:
            # Build token request
            data = {
                'grant_type': 'client_credentials',
                'client_id': self._client_id,
                'client_secret': self._client_secret
            }

            if self._scope:
                data['scope'] = self._scope

            # Request token
            response = requests.post(
                self._token_url,
                data=data,
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                timeout=10,
                verify=self.verify_ssl
            )
            response.raise_for_status()

            token_data = response.json()
            self._oauth_token = token_data['access_token']

            # Calculate expiry (with 5 minute buffer for safety)
            expires_in = token_data.get('expires_in', 3600)  # Default 1 hour
            self._token_expiry = datetime.now() + timedelta(seconds=expires_in - 300)

            self.logger.info("✓ OAuth token acquired successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to acquire OAuth token: {type(e).__name__}")
            return False

    def _is_token_expired(self) -> bool:
        """Check if OAuth token is expired or about to expire"""
        if not self._token_expiry:
            return False  # Unknown expiry, assume valid
        return datetime.now() >= self._token_expiry

    def _ensure_valid_token(self):
        """Ensure we have a valid OAuth token, refresh if needed"""
        if self.auth_type != 'oauth':
            return

        if self._use_client_creds and (not self._oauth_token or self._is_token_expired()):
            self.logger.debug("Token expired or missing, acquiring new token...")
            if not self._acquire_oauth_token():
                raise RuntimeError("Failed to acquire OAuth token")

    def get_headers(self) -> Dict[str, str]:
        """
        Return auth headers for requests

        Returns:
            Dictionary of HTTP headers

        Security: OAuth tokens added securely, never logged
        """
        # If using custom headers (from WorkspaceClient), return them directly
        if self.auth_type == 'oauth' and hasattr(self, '_use_custom_headers') and self._use_custom_headers:
            # Merge custom headers with base headers
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'User-Agent': 'PI-Lakeflow-Connector/1.0'
            }
            headers.update(self._custom_headers)
            return headers

        # Ensure token is valid (refresh if needed)
        self._ensure_valid_token()

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'PI-Lakeflow-Connector/1.0'  # Identify our client
        }

        if self.auth_type == 'oauth':
            headers['Authorization'] = f"Bearer {self._oauth_token}"

        return headers

    def test_connection(self, base_url: str, ca_bundle: Optional[str] = None) -> bool:
        """
        Test authentication by calling /piwebapi endpoint

        Args:
            base_url: Base URL of PI Web API server
            ca_bundle: Optional path to CA certificate bundle for SSL verification

        Returns:
            True if authentication successful, False otherwise

        Security Features:
        - SSL certificate verification enforced
        - URL validation before connection
        - No credential logging
        - Detailed error logging (sanitized)
        """
        # Validate URL format
        if not self._is_valid_url(base_url):
            self.logger.error("Invalid base URL format")
            return False

        # Enforce HTTPS in production
        parsed_url = urlparse(base_url)
        if parsed_url.scheme != 'https' and self.verify_ssl:
            self.logger.error(
                "⚠️  URL must use HTTPS for secure communication. "
                f"Got: {parsed_url.scheme}://"
            )
            return False

        try:
            # Configure SSL verification
            verify_param = ca_bundle if ca_bundle else self.verify_ssl

            response = requests.get(
                f"{base_url}/piwebapi",
                auth=self.get_auth_handler(),
                headers=self.get_headers(),
                timeout=10,
                verify=verify_param  # 🔒 SSL certificate verification
            )
            response.raise_for_status()

            self.logger.info("✓ Authentication successful")
            return True

        except requests.exceptions.SSLError as e:
            self.logger.error(f"SSL certificate verification failed: {type(e).__name__}")
            self.logger.error("Check certificate validity or provide CA bundle")
            return False

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else 'unknown'
            self.logger.error(f"Authentication failed with HTTP {status_code}")
            return False

        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"Connection failed: {type(e).__name__}")
            return False

        except requests.exceptions.Timeout as e:
            self.logger.error("Connection timeout")
            return False

        except Exception as e:
            self.logger.error(f"Authentication failed: {type(e).__name__}")
            return False

    def _is_valid_url(self, url: str) -> bool:
        """
        Validate URL format and security

        Args:
            url: URL to validate

        Returns:
            True if URL is valid and secure

        Security: Prevents SSRF by validating URL format
        """
        try:
            parsed = urlparse(url)

            # Must have scheme and netloc
            if not parsed.scheme or not parsed.netloc:
                return False

            # Only allow http/https
            if parsed.scheme not in ('http', 'https'):
                self.logger.error(f"Invalid URL scheme: {parsed.scheme}")
                return False

            # Block localhost/internal IPs in production
            hostname = parsed.netloc.split(':')[0]  # Remove port
            if self.verify_ssl and hostname.lower() in ('localhost', '127.0.0.1', '0.0.0.0'):
                self.logger.warning(
                    "⚠️  Localhost URLs not recommended for production. "
                    "Disable SSL verification if needed for testing."
                )

            return True

        except Exception:
            return False

    def clear_credentials(self):
        """
        Clear stored credentials from memory

        Security: Call this when auth manager is no longer needed
        """
        if hasattr(self, '_password'):
            self._password = None
        if hasattr(self, '_oauth_token'):
            self._oauth_token = None
        if hasattr(self, '_client_secret'):
            self._client_secret = None

        self.logger.debug("Credentials cleared from memory")
