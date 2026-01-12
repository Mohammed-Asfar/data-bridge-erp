"""
DataBridge ERP - HTTP Connector
Handles data retrieval from HTTP/HTTPS endpoints
"""

import json
import urllib.request
import urllib.parse
import urllib.error
import ssl
from typing import Optional, Union


class HTTPConnector:
    """
    HTTP source connector for retrieving data from HTTP/HTTPS endpoints.
    
    Usage:
        connector = HTTPConnector(
            url='https://api.example.com/data',
            headers={'Authorization': 'Bearer token'}
        )
        data = connector.fetch_data()
    """
    
    def __init__(
        self,
        url: str,
        headers: Optional[dict] = None,
        auth: Optional[dict] = None,
        timeout: int = 30,
        verify_ssl: bool = True
    ):
        """
        Initialize HTTP connector.
        
        Args:
            url: Target URL
            headers: HTTP headers to include in requests
            auth: Authentication config {'type': 'basic|bearer', 'username', 'password', 'token'}
            timeout: Request timeout in seconds
            verify_ssl: Whether to verify SSL certificates
        """
        self.url = url
        self.headers = headers or {}
        self.auth = auth
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        
        # Setup authentication
        self._setup_auth()
    
    def _setup_auth(self) -> None:
        """Configure authentication headers."""
        if not self.auth:
            return
        
        auth_type = self.auth.get('type', 'bearer').lower()
        
        if auth_type == 'bearer':
            token = self.auth.get('token', '')
            self.headers['Authorization'] = f'Bearer {token}'
            
        elif auth_type == 'basic':
            import base64
            username = self.auth.get('username', '')
            password = self.auth.get('password', '')
            credentials = base64.b64encode(f'{username}:{password}'.encode()).decode()
            self.headers['Authorization'] = f'Basic {credentials}'
            
        elif auth_type == 'api_key':
            key_name = self.auth.get('key_name', 'X-API-Key')
            key_value = self.auth.get('key_value', '')
            self.headers[key_name] = key_value
    
    def _create_ssl_context(self) -> ssl.SSLContext:
        """Create SSL context based on verification settings."""
        if self.verify_ssl:
            return ssl.create_default_context()
        else:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            return ctx
    
    def fetch_data(
        self,
        method: str = 'GET',
        params: Optional[dict] = None,
        body: Optional[Union[dict, str, bytes]] = None,
        extra_headers: Optional[dict] = None
    ) -> bytes:
        """
        Fetch data from the HTTP endpoint.
        
        Args:
            method: HTTP method (GET, POST, PUT, etc.)
            params: URL query parameters
            body: Request body (dict will be JSON encoded)
            extra_headers: Additional headers for this request
            
        Returns:
            Response body as bytes
        """
        # Build URL with query parameters
        url = self.url
        if params:
            query_string = urllib.parse.urlencode(params)
            separator = '&' if '?' in url else '?'
            url = f'{url}{separator}{query_string}'
        
        # Prepare headers
        headers = {**self.headers}
        if extra_headers:
            headers.update(extra_headers)
        
        # Prepare body
        data = None
        if body is not None:
            if isinstance(body, dict):
                data = json.dumps(body).encode('utf-8')
                headers['Content-Type'] = 'application/json'
            elif isinstance(body, str):
                data = body.encode('utf-8')
            else:
                data = body
        
        # Create request
        request = urllib.request.Request(
            url,
            data=data,
            headers=headers,
            method=method.upper()
        )
        
        # Execute request
        try:
            ssl_context = self._create_ssl_context()
            with urllib.request.urlopen(
                request,
                timeout=self.timeout,
                context=ssl_context
            ) as response:
                return response.read()
                
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8', errors='ignore')
            raise HTTPError(
                status_code=e.code,
                reason=e.reason,
                body=error_body
            )
        except urllib.error.URLError as e:
            raise ConnectionError(f'Failed to connect to {url}: {e.reason}')
    
    def fetch_json(
        self,
        method: str = 'GET',
        params: Optional[dict] = None,
        body: Optional[dict] = None
    ) -> dict:
        """
        Fetch JSON data from the HTTP endpoint.
        
        Args:
            method: HTTP method
            params: URL query parameters
            body: Request body dict
            
        Returns:
            Parsed JSON response
        """
        response = self.fetch_data(method, params, body)
        return json.loads(response.decode('utf-8'))
    
    def download_file(self, output_filename: Optional[str] = None) -> tuple:
        """
        Download a file from the URL.
        
        Args:
            output_filename: Optional filename (extracted from URL if not provided)
            
        Returns:
            Tuple of (content_bytes, filename, content_type)
        """
        response_bytes = self.fetch_data()
        
        # Try to determine filename
        if not output_filename:
            # Extract from URL path
            parsed = urllib.parse.urlparse(self.url)
            path_parts = parsed.path.split('/')
            output_filename = path_parts[-1] if path_parts else 'download'
        
        return response_bytes, output_filename


class HTTPError(Exception):
    """HTTP error with status code and response body."""
    
    def __init__(self, status_code: int, reason: str, body: str):
        self.status_code = status_code
        self.reason = reason
        self.body = body
        super().__init__(f'HTTP {status_code} {reason}: {body[:200]}')
