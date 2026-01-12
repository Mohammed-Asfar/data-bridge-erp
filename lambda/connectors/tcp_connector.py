"""
DataBridge ERP - TCP Connector
Handles data retrieval from TCP endpoints
"""

import socket
from typing import Optional


class TCPConnector:
    """
    TCP source connector for retrieving data from TCP endpoints.
    
    Usage:
        connector = TCPConnector(host='server.example.com', port=9000)
        data = connector.receive_data(send_data=b'GET DATA')
    """
    
    def __init__(
        self,
        host: str,
        port: int,
        timeout: int = 30,
        buffer_size: int = 4096
    ):
        """
        Initialize TCP connector.
        
        Args:
            host: TCP server hostname or IP
            port: TCP port number
            timeout: Socket timeout in seconds
            buffer_size: Buffer size for receiving data
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.buffer_size = buffer_size
        self._socket = None
    
    def connect(self) -> None:
        """Establish TCP connection."""
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(self.timeout)
        self._socket.connect((self.host, self.port))
    
    def disconnect(self) -> None:
        """Close TCP connection."""
        if self._socket:
            try:
                self._socket.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            self._socket.close()
            self._socket = None
    
    def send(self, data: bytes) -> None:
        """
        Send data over the TCP connection.
        
        Args:
            data: Data to send
        """
        if not self._socket:
            raise ConnectionError('Not connected. Call connect() first.')
        
        self._socket.sendall(data)
    
    def receive(self, size: Optional[int] = None) -> bytes:
        """
        Receive data from the TCP connection.
        
        Args:
            size: Number of bytes to receive (None for all available)
            
        Returns:
            Received data as bytes
        """
        if not self._socket:
            raise ConnectionError('Not connected. Call connect() first.')
        
        if size:
            return self._socket.recv(size)
        else:
            # Receive until connection closes or timeout
            chunks = []
            while True:
                try:
                    chunk = self._socket.recv(self.buffer_size)
                    if not chunk:
                        break
                    chunks.append(chunk)
                except socket.timeout:
                    break
            return b''.join(chunks)
    
    def receive_until(self, delimiter: bytes) -> bytes:
        """
        Receive data until a delimiter is encountered.
        
        Args:
            delimiter: Byte sequence to stop at
            
        Returns:
            Received data including delimiter
        """
        if not self._socket:
            raise ConnectionError('Not connected. Call connect() first.')
        
        data = b''
        while delimiter not in data:
            chunk = self._socket.recv(1)
            if not chunk:
                break
            data += chunk
        
        return data
    
    def receive_data(
        self,
        send_data: Optional[bytes] = None,
        timeout: Optional[int] = None,
        expect_size: Optional[int] = None
    ) -> bytes:
        """
        Connect, optionally send data, and receive response.
        
        Args:
            send_data: Data to send after connecting
            timeout: Override timeout for this operation
            expect_size: Expected response size (None for all)
            
        Returns:
            Received data as bytes
        """
        original_timeout = self.timeout
        
        try:
            if timeout:
                self.timeout = timeout
            
            self.connect()
            
            if send_data:
                self.send(send_data)
            
            return self.receive(expect_size)
            
        finally:
            self.disconnect()
            self.timeout = original_timeout
    
    def send_receive(
        self,
        request: bytes,
        response_size: Optional[int] = None
    ) -> bytes:
        """
        Convenience method to send a request and get response.
        
        Args:
            request: Request data to send
            response_size: Expected response size
            
        Returns:
            Response data as bytes
        """
        return self.receive_data(send_data=request, expect_size=response_size)
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        return False


class TCPStreamReader:
    """
    Helper class for reading structured data from TCP streams.
    """
    
    def __init__(self, connector: TCPConnector):
        self.connector = connector
        self._buffer = b''
    
    def read_line(self, encoding: str = 'utf-8') -> str:
        """Read a line (until newline) from the stream."""
        while b'\n' not in self._buffer:
            chunk = self.connector.receive(1024)
            if not chunk:
                break
            self._buffer += chunk
        
        if b'\n' in self._buffer:
            line, self._buffer = self._buffer.split(b'\n', 1)
            return line.decode(encoding).rstrip('\r')
        else:
            line = self._buffer
            self._buffer = b''
            return line.decode(encoding)
    
    def read_bytes(self, count: int) -> bytes:
        """Read exactly count bytes from the stream."""
        while len(self._buffer) < count:
            chunk = self.connector.receive(min(count - len(self._buffer), 4096))
            if not chunk:
                break
            self._buffer += chunk
        
        result = self._buffer[:count]
        self._buffer = self._buffer[count:]
        return result
