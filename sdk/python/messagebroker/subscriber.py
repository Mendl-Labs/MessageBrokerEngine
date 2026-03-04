"""Synchronous subscriber client for MessageBrokerEngine."""

from __future__ import annotations

import socket
import struct
import threading
from types import TracebackType
from typing import Callable, Dict, List, Optional, Tuple, Type

from .protocol import encode_subscribe, SUBSCRIBE_ACK

# Type alias for user-supplied message callbacks.
MessageHandler = Callable[[str, bytes], None]

# Default connection parameters matching broker defaults.
DEFAULT_PORT: int = 9000
DEFAULT_CONNECT_TIMEOUT: float = 5.0
DEFAULT_RECV_TIMEOUT: float = 1.0  # short so that stop() is responsive


class BrokerSubscriber:
    """Synchronous subscriber that receives messages from MessageBrokerEngine.

    Implements the broker wire protocol:

      SUBSCRIBE     -> [0x02][topic_len: u32 LE][topic_pattern: bytes]
      SUBSCRIBE_ACK <- [0x03][topic_len: u32 LE][topic: bytes]
      MESSAGE       <- [topic_len: u32 LE][topic: bytes][data_len: u32 LE][data: bytes]

    Handlers are called in the thread that calls :meth:`run` or
    :meth:`run_in_background`.

    Args:
        host:            Broker hostname or IP address.
        port:            Broker TCP port (default: 9000).
        connect_timeout: Seconds to wait while establishing the TCP connection.
        recv_timeout:    Seconds to wait for each recv; controls how quickly
                         :meth:`stop` takes effect.

    Example::

        def on_message(topic: str, data: bytes) -> None:
            print(f"{topic}: {data!r}")

        with BrokerSubscriber("localhost", 9000) as sub:
            sub.subscribe("market_data.kraken.*", on_message)
            sub.run()
    """

    def __init__(
        self,
        host: str,
        port: int = DEFAULT_PORT,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        recv_timeout: float = DEFAULT_RECV_TIMEOUT,
    ) -> None:
        self._host = host
        self._port = port
        self._connect_timeout = connect_timeout
        self._recv_timeout = recv_timeout
        self._sock: Optional[socket.socket] = None
        self._handlers: Dict[str, List[MessageHandler]] = {}
        self._stop_event = threading.Event()
        self._bg_thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Open the TCP connection to the broker.

        Raises:
            OSError: If the connection cannot be established.
        """
        if self._sock is not None:
            return
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(self._connect_timeout)
        sock.connect((self._host, self._port))
        sock.settimeout(self._recv_timeout)
        self._sock = sock

    def close(self) -> None:
        """Stop the receive loop (if running) and close the TCP connection."""
        self._stop_event.set()
        if self._bg_thread is not None:
            self._bg_thread.join(timeout=5.0)
            self._bg_thread = None
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None
        self._stop_event.clear()

    def is_connected(self) -> bool:
        """Return ``True`` if the socket is currently open."""
        return self._sock is not None

    # ------------------------------------------------------------------
    # Subscription management
    # ------------------------------------------------------------------

    def subscribe(self, topic_pattern: str, handler: MessageHandler) -> None:
        """Register *handler* for messages matching *topic_pattern*.

        Sends a SUBSCRIBE frame to the broker and waits for the
        SUBSCRIBE_ACK before returning.

        Args:
            topic_pattern: Topic or wildcard pattern (e.g. ``"market_data.*"``).
            handler:       Callable invoked with ``(topic, data)`` for each
                           matching message.

        Raises:
            RuntimeError: If not connected.
            OSError:       On a network error.
            TimeoutError:  If the SUBSCRIBE_ACK is not received in time.
        """
        if self._sock is None:
            raise RuntimeError(
                "BrokerSubscriber is not connected. Call connect() first."
            )

        frame = encode_subscribe(topic_pattern)
        self._sock.sendall(frame)

        # Wait for SUBSCRIBE_ACK [0x03][topic_len: u32 LE][topic: bytes]
        msg_type_byte = self._recv_exactly(1)
        if msg_type_byte[0] != SUBSCRIBE_ACK:
            raise RuntimeError(
                f"Expected SUBSCRIBE_ACK (0x03), got 0x{msg_type_byte[0]:02X}"
            )
        topic_len = struct.unpack("<I", self._recv_exactly(4))[0]
        _acked_topic = self._recv_exactly(topic_len).decode("utf-8")

        if topic_pattern not in self._handlers:
            self._handlers[topic_pattern] = []
        self._handlers[topic_pattern].append(handler)

    # ------------------------------------------------------------------
    # Receive loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Block and dispatch incoming messages until :meth:`stop` is called.

        Raises:
            RuntimeError: If not connected.
        """
        if self._sock is None:
            raise RuntimeError(
                "BrokerSubscriber is not connected. Call connect() first."
            )
        self._stop_event.clear()
        while not self._stop_event.is_set():
            try:
                topic, data = self._read_message()
            except socket.timeout:
                continue
            except OSError:
                break
            self._dispatch(topic, data)

    def run_in_background(self) -> None:
        """Start the receive loop in a daemon thread."""
        if self._bg_thread is not None and self._bg_thread.is_alive():
            return
        self._stop_event.clear()
        self._bg_thread = threading.Thread(target=self.run, daemon=True)
        self._bg_thread.start()

    def stop(self) -> None:
        """Signal the receive loop to exit."""
        self._stop_event.set()

    # ------------------------------------------------------------------
    # Context-manager support
    # ------------------------------------------------------------------

    def __enter__(self) -> "BrokerSubscriber":
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _recv_exactly(self, n: int) -> bytes:
        """Read exactly *n* bytes from the socket."""
        buf = b""
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))  # type: ignore[union-attr]
            if not chunk:
                raise OSError("Connection closed by broker")
            buf += chunk
        return buf

    def _read_message(self) -> Tuple[str, bytes]:
        """Read one message frame: [topic_len][topic][data_len][data]."""
        topic_len_bytes = self._recv_exactly(4)
        topic_len = struct.unpack("<I", topic_len_bytes)[0]
        topic = self._recv_exactly(topic_len).decode("utf-8")

        data_len_bytes = self._recv_exactly(4)
        data_len = struct.unpack("<I", data_len_bytes)[0]
        data = self._recv_exactly(data_len)

        return topic, data

    def _dispatch(self, topic: str, data: bytes) -> None:
        """Call all registered handlers that match *topic*."""
        for pattern, handlers in self._handlers.items():
            if _topic_matches(pattern, topic):
                for handler in handlers:
                    handler(topic, data)


def _topic_matches(pattern: str, topic: str) -> bool:
    """Return ``True`` if *topic* matches *pattern*.

    Supported wildcards (mirrors broker behaviour):

    * ``*``  – matches any characters within a single path segment
      (i.e. between two ``.`` separators).
    * ``#``  – matches any characters across multiple path segments.
    * Exact string match if no wildcards are present.
    """
    if pattern == topic:
        return True
    # Multi-level wildcard '#' matches everything after the prefix.
    if "#" in pattern:
        prefix = pattern[: pattern.index("#")]
        return topic.startswith(prefix)
    # Single-level wildcard '*' per segment.
    pattern_parts = pattern.split(".")
    topic_parts = topic.split(".")
    if len(pattern_parts) != len(topic_parts):
        return False
    return all(p == "*" or p == t for p, t in zip(pattern_parts, topic_parts))
