from typing import Dict, Optional
import requests
from requests.auth import HTTPBasicAuth
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
import logging


class PIAuthManager:
    """
    Manages authentication for PI Web API
    Supports: Kerberos, Basic, OAuth
    """
    
    def __init__(self, auth_config: Dict[str, str]):
        """
        Args:
            auth_config: {
                'type': 'basic' | 'kerberos' | 'oauth',
                'username': str (for basic),
                'password': str (for basic),
                'oauth_token': str (for oauth)
            }
        """
        self.auth_type = auth_config['type']
        self.config = auth_config
        self.logger = logging.getLogger(__name__)
        
    def get_auth_handler(self):
        """Return appropriate auth handler for requests library"""
        if self.auth_type == 'basic':
            return HTTPBasicAuth(
                self.config['username'],
                self.config['password']
            )
        elif self.auth_type == 'kerberos':
            return HTTPKerberosAuth(mutual_authentication=OPTIONAL)
        elif self.auth_type == 'oauth':
            # Return None, will use headers
            return None
        else:
            raise ValueError(f"Unsupported auth type: {self.auth_type}")
    
    def get_headers(self) -> Dict[str, str]:
        """Return auth headers for requests"""
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        if self.auth_type == 'oauth':
            headers['Authorization'] = f"Bearer {self.config['oauth_token']}"
        
        return headers
    
    def test_connection(self, base_url: str) -> bool:
        """Test authentication by calling /piwebapi endpoint"""
        try:
            response = requests.get(
                f"{base_url}/piwebapi",
                auth=self.get_auth_handler(),
                headers=self.get_headers(),
                timeout=10
            )
            response.raise_for_status()
            self.logger.info("Authentication successful")
            return True
        except Exception as e:
            self.logger.error(f"Authentication failed: {e}")
            return False
