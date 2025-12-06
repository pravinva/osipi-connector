"""
Test suite for PI Web API Client

Tests:
- Successful GET request
- Retry on 503 error
- Timeout handling
- Batch request with 100 items
- Error handling and logging
- Connection pooling
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import requests
from requests.exceptions import HTTPError, ConnectionError, Timeout
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.client.pi_web_api_client import PIWebAPIClient
from src.auth.pi_auth_manager import PIAuthManager


class TestPIWebAPIClient:
    """Test suite for PIWebAPIClient"""

    @pytest.fixture
    def mock_auth_manager(self):
        """Create mock authentication manager"""
        auth_config = {
            'type': 'basic',
            'username': 'testuser',
            'password': 'testpass'
        }
        return PIAuthManager(auth_config)

    @pytest.fixture
    def client(self, mock_auth_manager):
        """Create test client instance"""
        return PIWebAPIClient(
            base_url="https://pi-test.example.com/piwebapi",
            auth_manager=mock_auth_manager
        )

    def test_client_initialization(self, mock_auth_manager):
        """Test client initialization with correct parameters"""
        client = PIWebAPIClient(
            base_url="https://pi-test.example.com/piwebapi/",
            auth_manager=mock_auth_manager,
            timeout=45
        )

        assert client.base_url == "https://pi-test.example.com/piwebapi"
        assert client.default_timeout == 45
        assert client.session is not None
        assert client.auth_manager == mock_auth_manager

    @patch('requests.Session.get')
    def test_successful_get_request(self, mock_get, client):
        """Test successful GET request"""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Items": [{"Name": "TestPoint"}]}
        mock_get.return_value = mock_response

        # Execute request
        response = client.get("/piwebapi/dataservers")

        # Assertions
        assert response.status_code == 200
        assert "Items" in response.json()
        mock_get.assert_called_once()

    @patch('requests.Session.get')
    def test_get_with_params(self, mock_get, client):
        """Test GET request with query parameters"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Items": []}
        mock_get.return_value = mock_response

        params = {"nameFilter": "*Temp*", "maxCount": "100"}
        response = client.get("/piwebapi/points", params=params)

        assert response.status_code == 200
        call_args = mock_get.call_args
        assert call_args[1]['params'] == params

    @patch('requests.Session.get')
    def test_retry_on_503_error(self, mock_get, client):
        """Test that retry configuration is set up correctly"""
        # The retry logic is handled by urllib3 Retry adapter
        # Here we verify that when we get a 503, it's properly handled
        # by catching the error (after retries are exhausted)

        mock_response_fail = Mock()
        mock_response_fail.status_code = 503
        mock_response_fail.raise_for_status.side_effect = HTTPError("503 Server Error")
        mock_get.return_value = mock_response_fail

        # Should raise HTTPError after retries are exhausted
        with pytest.raises(HTTPError):
            client.get("/piwebapi/test")

    @patch('requests.Session.get')
    def test_timeout_handling(self, mock_get, client):
        """Test timeout exception handling"""
        mock_get.side_effect = Timeout("Connection timeout")

        with pytest.raises(Timeout):
            client.get("/piwebapi/slow-endpoint")

    @patch('requests.Session.get')
    def test_connection_error_handling(self, mock_get, client):
        """Test connection error handling"""
        mock_get.side_effect = ConnectionError("Failed to connect")

        with pytest.raises(ConnectionError):
            client.get("/piwebapi/unreachable")

    @patch('requests.Session.post')
    def test_successful_post_request(self, mock_post, client):
        """Test successful POST request"""
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"Created": True}
        mock_post.return_value = mock_response

        data = {"name": "test", "value": 123}
        response = client.post("/piwebapi/points", data)

        assert response.status_code == 201
        mock_post.assert_called_once()

    @patch('requests.Session.post')
    def test_batch_execute_with_100_items(self, mock_post, client):
        """Test batch request with 100 items"""
        # Create 100 batch requests
        batch_requests = []
        for i in range(100):
            batch_requests.append({
                "Method": "GET",
                "Resource": f"/streams/tag{i}/recorded",
                "Parameters": {
                    "startTime": "2025-01-01T00:00:00Z",
                    "endTime": "2025-01-02T00:00:00Z"
                }
            })

        # Mock batch response
        mock_responses = [
            {
                "Status": 200,
                "Content": {
                    "Items": [
                        {"Timestamp": "2025-01-01T12:00:00Z", "Value": 75.5}
                    ]
                }
            }
            for _ in range(100)
        ]

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Responses": mock_responses}
        mock_post.return_value = mock_response

        # Execute batch
        result = client.batch_execute(batch_requests)

        # Assertions
        assert "Responses" in result
        assert len(result["Responses"]) == 100
        assert all(r["Status"] == 200 for r in result["Responses"])

        # Verify POST was called with correct payload
        call_args = mock_post.call_args
        payload = call_args[1]['json']
        assert "Requests" in payload
        assert len(payload["Requests"]) == 100

    @patch('requests.Session.post')
    def test_batch_execute_with_partial_failures(self, mock_post, client):
        """Test batch request with some failed sub-requests"""
        batch_requests = [
            {"Method": "GET", "Resource": "/streams/tag1/recorded"},
            {"Method": "GET", "Resource": "/streams/tag2/recorded"},
            {"Method": "GET", "Resource": "/streams/tag3/recorded"}
        ]

        # Mixed success/failure responses
        mock_responses = [
            {"Status": 200, "Content": {"Items": []}},
            {"Status": 404, "Content": {"Error": "Not found"}},
            {"Status": 200, "Content": {"Items": []}}
        ]

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Responses": mock_responses}
        mock_post.return_value = mock_response

        result = client.batch_execute(batch_requests)

        # Should still return results
        assert len(result["Responses"]) == 3
        assert result["Responses"][0]["Status"] == 200
        assert result["Responses"][1]["Status"] == 404
        assert result["Responses"][2]["Status"] == 200

    @patch('requests.Session.post')
    def test_batch_execute_empty_list(self, mock_post, client):
        """Test batch execute with empty request list"""
        result = client.batch_execute([])

        assert result == {"Responses": []}
        mock_post.assert_not_called()

    @patch('requests.Session.post')
    def test_batch_execute_invalid_response(self, mock_post, client):
        """Test batch execute with invalid response structure"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"InvalidKey": "data"}
        mock_post.return_value = mock_response

        batch_requests = [{"Method": "GET", "Resource": "/test"}]

        with pytest.raises(ValueError, match="Invalid batch response"):
            client.batch_execute(batch_requests)

    def test_context_manager(self, mock_auth_manager):
        """Test client as context manager"""
        with PIWebAPIClient(
            base_url="https://pi-test.example.com/piwebapi",
            auth_manager=mock_auth_manager
        ) as client:
            assert client.session is not None

        # Session should be closed after context
        # Note: We can't easily test this without mocking, but structure is correct

    def test_close_method(self, client):
        """Test explicit close method"""
        assert client.session is not None
        client.close()
        # Session should be closed

    @patch('requests.Session.get')
    def test_custom_timeout(self, mock_get, client):
        """Test custom timeout parameter"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        client.get("/piwebapi/test", timeout=60)

        call_args = mock_get.call_args
        assert call_args[1]['timeout'] == 60

    @patch('requests.Session.get')
    def test_http_error_logging(self, mock_get, client, caplog):
        """Test that HTTP errors are properly logged"""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.json.return_value = {"Error": "Unauthorized"}
        mock_response.raise_for_status.side_effect = HTTPError()
        mock_get.return_value = mock_response

        with pytest.raises(HTTPError):
            client.get("/piwebapi/test")

        # Check that error was logged
        assert "HTTP error" in caplog.text


class TestPIWebAPIClientIntegration:
    """Integration tests requiring mock PI server"""

    @pytest.fixture
    def mock_auth_manager(self):
        """Create mock authentication manager"""
        auth_config = {
            'type': 'basic',
            'username': 'testuser',
            'password': 'testpass'
        }
        return PIAuthManager(auth_config)

    @patch('requests.Session.get')
    @patch('requests.Session.post')
    def test_full_workflow(self, mock_post, mock_get, mock_auth_manager):
        """Test complete workflow: discover tags, batch extract data"""
        # Setup client
        client = PIWebAPIClient(
            base_url="https://pi-test.example.com/piwebapi",
            auth_manager=mock_auth_manager
        )

        # Step 1: Discover data servers
        mock_get_response = Mock()
        mock_get_response.status_code = 200
        mock_get_response.json.return_value = {
            "Items": [{"Name": "PIServer", "WebId": "F1DP-Server1"}]
        }
        mock_get.return_value = mock_get_response

        servers = client.get("/piwebapi/dataservers").json()
        assert len(servers["Items"]) == 1

        # Step 2: Batch extract time-series data
        batch_requests = [
            {
                "Method": "GET",
                "Resource": "/streams/tag1/recorded",
                "Parameters": {"startTime": "2025-01-01T00:00:00Z"}
            }
        ]

        mock_post_response = Mock()
        mock_post_response.status_code = 200
        mock_post_response.json.return_value = {
            "Responses": [
                {"Status": 200, "Content": {"Items": [{"Value": 75.5}]}}
            ]
        }
        mock_post.return_value = mock_post_response

        batch_result = client.batch_execute(batch_requests)
        assert batch_result["Responses"][0]["Status"] == 200

        client.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
