"""
PI Web API HTTP Client

Low-level HTTP client for PI Web API with:
- Exponential backoff retry logic
- Connection pooling
- Rate limiting support
- Error categorization and handling
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Dict, List, Optional
import logging
from datetime import datetime


class PIWebAPIClient:
    """
    Low-level HTTP client for PI Web API
    Handles retries, rate limiting, connection pooling
    """

    def __init__(self, base_url: str, auth_manager, timeout: int = 30):
        """
        Initialize PI Web API client

        Args:
            base_url: Base URL of PI Web API (e.g., https://pi-server.com/piwebapi)
            auth_manager: PIAuthManager instance for authentication
            timeout: Default timeout for requests in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.auth_manager = auth_manager
        self.default_timeout = timeout
        self.session = self._create_session()
        self.logger = logging.getLogger(__name__)

    def _create_session(self) -> requests.Session:
        """
        Create session with retry strategy and connection pooling

        Returns:
            Configured requests.Session instance
        """
        session = requests.Session()

        # Retry strategy with exponential backoff
        # Retries on: 429 (rate limit), 500, 502, 503, 504 (server errors)
        retry_strategy = Retry(
            total=3,  # Total number of retries
            backoff_factor=1,  # Wait 1s, 2s, 4s between retries
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE"],
            raise_on_status=False  # Don't raise on max retries, let us handle it
        )

        # HTTP adapter with connection pooling
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,  # Number of connection pools to cache
            pool_maxsize=20  # Maximum number of connections per pool
        )

        # Mount adapter for both HTTP and HTTPS
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set authentication
        session.auth = self.auth_manager.get_auth_handler()
        session.headers.update(self.auth_manager.get_headers())

        return session

    def get(self, endpoint: str, params: Optional[Dict] = None, timeout: Optional[int] = None) -> requests.Response:
        """
        Execute GET request with error handling

        Args:
            endpoint: API endpoint (e.g., /piwebapi/dataservers)
            params: Optional query parameters
            timeout: Optional timeout override

        Returns:
            Response object

        Raises:
            requests.exceptions.RequestException: On request failure
        """
        url = f"{self.base_url}{endpoint}"
        timeout = timeout or self.default_timeout

        self.logger.debug(f"GET {url} params={params}")

        try:
            response = self.session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            self.logger.debug(f"GET {url} - Status: {response.status_code}")
            return response

        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error for GET {url}: {e}")
            self._log_error_details(e.response)
            raise

        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"Connection error for GET {url}: {e}")
            raise

        except requests.exceptions.Timeout as e:
            self.logger.error(f"Timeout for GET {url}: {e}")
            raise

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed for GET {url}: {e}")
            raise

    def post(self, endpoint: str, json_data: Dict, timeout: Optional[int] = None) -> requests.Response:
        """
        Execute POST request with error handling

        Args:
            endpoint: API endpoint (e.g., /piwebapi/batch)
            json_data: JSON payload
            timeout: Optional timeout override (default 60s for POST)

        Returns:
            Response object

        Raises:
            requests.exceptions.RequestException: On request failure
        """
        url = f"{self.base_url}{endpoint}"
        timeout = timeout or 60  # Longer timeout for POST operations

        self.logger.debug(f"POST {url} - Payload size: {len(str(json_data))} bytes")

        # Helpful auth debugging for Databricks Apps failures (401/302)
        try:
            auth_val = (self.session.headers.get("Authorization") or "").strip()
            try:
            auth_val = (self.session.headers.get("Authorization") or "").strip()
            if auth_val.startswith("Bearer " ):
                token = auth_val[7:]
                token_kind = "jwt" if token.startswith("ey") else ("pat" if token.startswith("dapi") else "unknown")
                self.logger.error(f"[auth-debug] Authorization present: Bearer <{token_kind}> (masked)")
            elif auth_val:
                self.logger.error("[auth-debug] Authorization present (non-bearer, masked)")
            else:
                self.logger.error("[auth-debug] Authorization header MISSING on request")
        except Exception:
            pass

        try:
            # Do not follow redirects automatically; redirects to login are a strong signal of token mismatch.
            response = self.session.post(url, json=json_data, timeout=timeout, allow_redirects=False)
            response.raise_for_status()
            self.logger.debug(f"POST {url} - Status: {response.status_code}")
            return response

        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error for POST {url}: {e}")
            self._log_error_details(e.response)
            raise

        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"Connection error for POST {url}: {e}")
            raise

        except requests.exceptions.Timeout as e:
            self.logger.error(f"Timeout for POST {url}: {e}")
            raise

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed for POST {url}: {e}")
            raise

    def batch_execute(self, requests_list: List[Dict]) -> Dict:
        """
        Execute batch request using PI Web API batch controller

        This is CRITICAL for performance with multiple tags.
        Example: 100 tags via batch = 1 HTTP request vs 100 individual requests

        Args:
            requests_list: List of batch request dictionaries
                [
                    {
                        "Method": "GET",
                        "Resource": "/streams/{webid}/recorded",
                        "Parameters": {"startTime": "...", "endTime": "..."}
                    },
                    ...
                ]

        Returns:
            Batch response dictionary with "Responses" key

        Example:
            >>> requests = [
            ...     {"Method": "GET", "Resource": f"/streams/{tag1}/recorded", ...},
            ...     {"Method": "GET", "Resource": f"/streams/{tag2}/recorded", ...}
            ... ]
            >>> response = client.batch_execute(requests)
            >>> for sub_response in response["Responses"]:
            ...     if sub_response["Status"] == 200:
            ...         data = sub_response["Content"]
        """
        if not requests_list:
            self.logger.warning("Empty batch request list provided")
            return {"Responses": []}

        batch_payload = {"Requests": requests_list}
        batch_size = len(requests_list)

        self.logger.info(f"Executing batch request with {batch_size} sub-requests")

        try:
            response = self.post("/piwebapi/batch", batch_payload, timeout=120)
            response_data = response.json()

            # Validate response structure
            if "Responses" not in response_data:
                raise ValueError("Invalid batch response: missing 'Responses' key")

            # Log success/failure counts
            statuses = [r.get("Status") for r in response_data["Responses"]]
            success_count = sum(1 for s in statuses if s == 200)
            failure_count = batch_size - success_count

            self.logger.info(
                f"Batch completed: {success_count} succeeded, {failure_count} failed"
            )

            return response_data

        except Exception as e:
            self.logger.error(f"Batch execution failed: {e}")
            raise

    def _log_error_details(self, response: Optional[requests.Response]):
        """
        Log detailed error information from response

        Args:
            response: Failed response object
        """
        if response is None:
            return

        try:
            self.logger.error(
                f"[auth-debug] response status={response.status_code} "
                f"redirect={response.is_redirect} location={response.headers.get('Location', '')} "
                f"www-authenticate={response.headers.get('WWW-Authenticate', '')}"
            )
            self.logger.error(f"[auth-debug] response content-type={response.headers.get('content-type','')}")
        except Exception:
            pass

        try:
            error_data = response.json()
            self.logger.error(f"Error details: {error_data}")
        except:
            self.logger.error(f"Error response text: {response.text[:500]}")

    def close(self):
        """Close the session and release connections"""
        if self.session:
            self.session.close()
            self.logger.info("Client session closed")

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
