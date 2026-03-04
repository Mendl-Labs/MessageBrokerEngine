"""Synchronous and asynchronous publisher clients for MessageBrokerEngine."""

from __future__ import annotations

import socket
import struct
import threading
from types import TracebackType
from typing import Optional, Type

from .protocol import encode_publish, SUBSCRIBE_ACK

# Default connection parameters matching broker defaults.
DEFAULT_PORT: int = 9000
DEFAULT_CONNECT_TIMEOUT: float = 5.0
DEFAULT_SEND_TIMEOUT: float = 5.0


class BrokerPublisher:
    """Thread-safe synchronous publisher that connects to MessageBrokerEngine.

    Implements the broker wire protocol:
      PUBLISH -> [0x01][topic_len: u32 LE][topic: bytes][data_len: u32 LE][data: bytes]

    Args:
        host:            Broker hostname or IP address.
        port:            Broker TCP port (default: 9000).
        connect_timeout: Seconds to wait while establishing the TCP connection.
        send_timeout:    Seconds to wait for each send to complete.

    Example::

        with BrokerPublisher("localhost", 9000) as pub:
            pub.publish("market_data.kraken.XBTUSD", b"payload")
    """

    def __init__(
        self,
        host: str,
        port: int = DEFAULT_PORT,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        send_timeout: float = DEFAULT_SEND_TIMEOUT,
    ) -> None:
        self._host = host
        self._port = port
        self._connect_timeout = connect_timeout
        self._send_timeout = send_timeout
        self._sock: Optional[socket.socket] = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Open the TCP connection to the broker.

        Raises:
            OSError: If the connection cannot be established.
        """
        with self._lock:
            if self._sock is not None:
                return
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.settimeout(self._connect_timeout)
            sock.connect((self._host, self._port))
            sock.settimeout(self._send_timeout)
            self._sock = sock

    def close(self) -> None:
        """Close the TCP connection."""
        with self._lock:
            if self._sock is not None:
                try:
                    self._sock.close()
                except OSError:
                    pass
                self._sock = None

    def is_connected(self) -> bool:
        """Return ``True`` if the socket is currently open."""
        return self._sock is not None

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    def publish(self, topic: str, data: bytes) -> None:
        """Publish *data* to *topic*.

        Encodes the message as a PUBLISH frame and sends it over TCP.

        Args:
            topic: Topic name.
            data:  Raw payload bytes.

        Raises:
            RuntimeError: If not connected.
            OSError:       On a network error.
        """
        frame = encode_publish(topic, data)
        with self._lock:
            if self._sock is None:
                raise RuntimeError(
                    "BrokerPublisher is not connected. Call connect() first."
                )
            self._sock.sendall(frame)

    # ------------------------------------------------------------------
    # Context-manager support
    # ------------------------------------------------------------------

    def __enter__(self) -> "BrokerPublisher":
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()
