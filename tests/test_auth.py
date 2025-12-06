import pytest
from src.auth.pi_auth_manager import PIAuthManager
from requests.auth import HTTPBasicAuth


class TestPIAuthManager:
    """Test authentication handler"""
    
    def test_basic_auth_initialization(self):
        """Test Basic auth setup"""
        config = {
            'type': 'basic',
            'username': 'testuser',
            'password': 'testpass'
        }
        
        auth_mgr = PIAuthManager(config)
        auth_handler = auth_mgr.get_auth_handler()
        
        assert isinstance(auth_handler, HTTPBasicAuth)
        assert auth_handler.username == 'testuser'
        assert auth_handler.password == 'testpass'
    
    def test_basic_auth_headers(self):
        """Test header generation"""
        config = {'type': 'basic', 'username': 'test', 'password': 'test'}
        auth_mgr = PIAuthManager(config)
        headers = auth_mgr.get_headers()
        
        assert 'Content-Type' in headers
        assert headers['Content-Type'] == 'application/json'
    
    def test_oauth_auth_headers(self):
        """Test OAuth header generation"""
        config = {
            'type': 'oauth',
            'oauth_token': 'test-token-12345'
        }
        
        auth_mgr = PIAuthManager(config)
        headers = auth_mgr.get_headers()
        
        assert 'Authorization' in headers
        assert headers['Authorization'] == 'Bearer test-token-12345'
    
    def test_invalid_auth_type(self):
        """Test invalid auth type raises error"""
        config = {'type': 'invalid'}
        auth_mgr = PIAuthManager(config)
        
        with pytest.raises(ValueError):
            auth_mgr.get_auth_handler()
    
    def test_connection_test_failure(self):
        """Test connection failure handling"""
        config = {'type': 'basic', 'username': 'test', 'password': 'test'}
        auth_mgr = PIAuthManager(config)
        
        # Invalid URL
        result = auth_mgr.test_connection("http://invalid-server:9999")
        assert result == False
