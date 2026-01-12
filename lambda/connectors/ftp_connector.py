"""
DataBridge ERP - FTP Connector
Handles data retrieval from FTP servers
"""

import ftplib
import io
from typing import Optional


class FTPConnector:
    """
    FTP source connector for retrieving files from FTP servers.
    
    Usage:
        connector = FTPConnector(
            host='ftp.example.com',
            username='user',
            password='pass'
        )
        content = connector.download_file('/path/to/file.csv')
    """
    
    def __init__(
        self,
        host: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        port: int = 21,
        timeout: int = 30,
        use_tls: bool = False
    ):
        """
        Initialize FTP connector.
        
        Args:
            host: FTP server hostname or IP
            username: FTP username (None for anonymous)
            password: FTP password
            port: FTP port (default: 21)
            timeout: Connection timeout in seconds
            use_tls: Use FTP over TLS (FTPS)
        """
        self.host = host
        self.username = username or 'anonymous'
        self.password = password or ''
        self.port = port
        self.timeout = timeout
        self.use_tls = use_tls
        self._ftp = None
    
    def connect(self) -> None:
        """Establish connection to FTP server."""
        if self.use_tls:
            self._ftp = ftplib.FTP_TLS(timeout=self.timeout)
        else:
            self._ftp = ftplib.FTP(timeout=self.timeout)
        
        self._ftp.connect(self.host, self.port)
        self._ftp.login(self.username, self.password)
        
        if self.use_tls:
            self._ftp.prot_p()  # Enable data channel encryption
    
    def disconnect(self) -> None:
        """Close FTP connection."""
        if self._ftp:
            try:
                self._ftp.quit()
            except Exception:
                self._ftp.close()
            self._ftp = None
    
    def download_file(self, remote_path: str) -> bytes:
        """
        Download a file from the FTP server.
        
        Args:
            remote_path: Full path to the file on FTP server
            
        Returns:
            File content as bytes
        """
        try:
            self.connect()
            
            buffer = io.BytesIO()
            self._ftp.retrbinary(f'RETR {remote_path}', buffer.write)
            buffer.seek(0)
            
            return buffer.read()
            
        finally:
            self.disconnect()
    
    def list_directory(self, remote_path: str = '/') -> list:
        """
        List files in a directory on the FTP server.
        
        Args:
            remote_path: Directory path to list
            
        Returns:
            List of file/directory names
        """
        try:
            self.connect()
            
            self._ftp.cwd(remote_path)
            files = self._ftp.nlst()
            
            return files
            
        finally:
            self.disconnect()
    
    def download_multiple(self, remote_paths: list) -> dict:
        """
        Download multiple files from the FTP server.
        
        Args:
            remote_paths: List of file paths to download
            
        Returns:
            Dictionary mapping file paths to content bytes
        """
        try:
            self.connect()
            
            results = {}
            for path in remote_paths:
                try:
                    buffer = io.BytesIO()
                    self._ftp.retrbinary(f'RETR {path}', buffer.write)
                    buffer.seek(0)
                    results[path] = buffer.read()
                except ftplib.error_perm as e:
                    results[path] = None
                    print(f"Failed to download {path}: {e}")
            
            return results
            
        finally:
            self.disconnect()
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        return False
