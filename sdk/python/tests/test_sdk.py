"""Tests for messagebroker Python SDK.

These tests are self-contained (no live broker required) and validate:
- Wire-protocol encoding / decoding helpers
- BrokerPublisher behaviour with a mock socket
- BrokerSubscriber behaviour with a mock socket
- Topic pattern matching
"""

from __future__ import annotations

import socket
import struct
import threading
from unittest.mock import MagicMock, patch

import pytest

from messagebroker import BrokerPublisher, BrokerSubscriber
from messagebroker.protocol import (
    PUBLISH,
    SUBSCRIBE,
    SUBSCRIBE_ACK,
    encode_publish,
    encode_subscribe,
    decode_incoming_message,
)
from messagebroker.subscriber import _topic_matches


# ---------------------------------------------------------------------------
# Protocol encoding
# ---------------------------------------------------------------------------


class TestEncodePublish:
    def test_basic_frame_structure(self):
        frame = encode_publish("test.topic", b"hello")
        assert frame[0] == PUBLISH
        # topic_len
        topic_bytes = b"test.topic"
        topic_len = struct.unpack_from("<I", frame, 1)[0]
        assert topic_len == len(topic_bytes)
        # topic
        offset = 1 + 4
        assert frame[offset : offset + topic_len] == topic_bytes
        # data_len
        offset += topic_len
        data_len = struct.unpack_from("<I", frame, offset)[0]
        assert data_len == 5
        # data
        offset += 4
        assert frame[offset : offset + data_len] == b"hello"

    def test_empty_payload(self):
        frame = encode_publish("a.b", b"")
        assert frame[0] == PUBLISH
        data_len_pos = 1 + 4 + 3  # after cmd + topic_len + topic
        data_len = struct.unpack_from("<I", frame, data_len_pos)[0]
        assert data_len == 0

    def test_unicode_topic(self):
        frame = encode_publish("données.test", b"x")
        assert frame[0] == PUBLISH

    def test_topic_too_long_raises(self):
        with pytest.raises(ValueError, match="Topic too long"):
            encode_publish("x" * 1025, b"data")

    def test_payload_too_large_raises(self):
        with pytest.raises(ValueError, match="Payload too large"):
            encode_publish("topic", b"x" * (16 * 1024 * 1024 + 1))


class TestEncodeSubscribe:
    def test_basic_frame_structure(self):
        frame = encode_subscribe("market_data.*")
        assert frame[0] == SUBSCRIBE
        pattern_bytes = b"market_data.*"
        pattern_len = struct.unpack_from("<I", frame, 1)[0]
        assert pattern_len == len(pattern_bytes)
        assert frame[5:] == pattern_bytes

    def test_pattern_too_long_raises(self):
        with pytest.raises(ValueError, match="Topic pattern too long"):
            encode_subscribe("x" * 1025)


class TestDecodeIncomingMessage:
    def _build_frame(self, topic: str, data: bytes) -> bytes:
        topic_bytes = topic.encode("utf-8")
        return (
            struct.pack("<I", len(topic_bytes))
            + topic_bytes
            + struct.pack("<I", len(data))
            + data
        )

    def test_basic_decode(self):
        frame = self._build_frame("market_data.kraken.XBTUSD", b"payload")
        topic, payload = decode_incoming_message(frame)
        assert topic == "market_data.kraken.XBTUSD"
        assert payload == b"payload"

    def test_empty_data(self):
        frame = self._build_frame("some.topic", b"")
        topic, payload = decode_incoming_message(frame)
        assert topic == "some.topic"
        assert payload == b""

    def test_too_short_raises(self):
        with pytest.raises(ValueError, match="Frame too short"):
            decode_incoming_message(b"\x00\x00")

    def test_truncated_topic_raises(self):
        # topic_len says 10 but only 3 bytes follow
        bad = struct.pack("<I", 10) + b"abc"
        with pytest.raises(ValueError, match="truncated"):
            decode_incoming_message(bad)

    def test_truncated_data_raises(self):
        topic_bytes = b"t"
        bad = struct.pack("<I", 1) + topic_bytes + struct.pack("<I", 100) + b"x"
        with pytest.raises(ValueError, match="truncated"):
            decode_incoming_message(bad)


# ---------------------------------------------------------------------------
# Topic pattern matching
# ---------------------------------------------------------------------------


class TestTopicMatches:
    @pytest.mark.parametrize(
        "pattern, topic, expected",
        [
            ("exact.topic", "exact.topic", True),
            ("exact.topic", "other.topic", False),
            ("market_data.*", "market_data.kraken", True),
            ("market_data.*", "market_data.binance", True),
            ("market_data.*", "market_data.kraken.XBTUSD", False),
            ("market_data.#", "market_data.kraken.XBTUSD", True),
            ("market_data.#", "market_data.binance.BTCUSDT", True),
            ("market_data.#", "other.kraken.XBTUSD", False),
            ("a.*.c", "a.b.c", True),
            ("a.*.c", "a.bb.c", True),
            ("a.*.c", "a.b.d", False),
        ],
    )
    def test_pattern(self, pattern, topic, expected):
        assert _topic_matches(pattern, topic) == expected


# ---------------------------------------------------------------------------
# BrokerPublisher with a mock socket
# ---------------------------------------------------------------------------


def _make_mock_socket():
    """Return a MagicMock that behaves like a connected socket."""
    sock = MagicMock(spec=socket.socket)
    sock.sendall = MagicMock()
    return sock


class TestBrokerPublisher:
    def test_connect_and_publish(self):
        mock_sock = _make_mock_socket()

        with patch("socket.socket", return_value=mock_sock):
            pub = BrokerPublisher("localhost", 9000)
            pub.connect()
            assert pub.is_connected()
            pub.publish("test.topic", b"data")

        # sendall should have been called with a valid PUBLISH frame
        call_args = mock_sock.sendall.call_args[0][0]
        assert call_args[0] == PUBLISH

    def test_publish_without_connect_raises(self):
        pub = BrokerPublisher("localhost", 9000)
        with pytest.raises(RuntimeError, match="not connected"):
            pub.publish("topic", b"data")

    def test_context_manager_connects_and_closes(self):
        mock_sock = _make_mock_socket()
        with patch("socket.socket", return_value=mock_sock):
            with BrokerPublisher("localhost", 9000) as pub:
                assert pub.is_connected()
                pub.publish("a.b", b"x")
        assert not pub.is_connected()
        mock_sock.close.assert_called()

    def test_double_connect_is_idempotent(self):
        mock_sock = _make_mock_socket()
        with patch("socket.socket", return_value=mock_sock) as mock_cls:
            pub = BrokerPublisher("localhost", 9000)
            pub.connect()
            pub.connect()  # second call should be a no-op
            assert mock_cls.call_count == 1

    def test_close_when_not_connected(self):
        pub = BrokerPublisher("localhost", 9000)
        pub.close()  # should not raise
        assert not pub.is_connected()


# ---------------------------------------------------------------------------
# BrokerSubscriber with a mock socket
# ---------------------------------------------------------------------------


def _build_ack(topic_pattern: str) -> bytes:
    """Build a SUBSCRIBE_ACK frame as the broker would send it."""
    pattern_bytes = topic_pattern.encode("utf-8")
    return (
        struct.pack("<B", SUBSCRIBE_ACK)
        + struct.pack("<I", len(pattern_bytes))
        + pattern_bytes
    )


def _build_message_frame(topic: str, data: bytes) -> bytes:
    """Build a message frame as the broker would send it."""
    topic_bytes = topic.encode("utf-8")
    return (
        struct.pack("<I", len(topic_bytes))
        + topic_bytes
        + struct.pack("<I", len(data))
        + data
    )


class TestBrokerSubscriber:
    def test_subscribe_without_connect_raises(self):
        sub = BrokerSubscriber("localhost", 9000)
        with pytest.raises(RuntimeError, match="not connected"):
            sub.subscribe("topic", lambda t, d: None)

    def test_context_manager_connects_and_closes(self):
        mock_sock = _make_mock_socket()
        # Provide enough bytes for subscribe + ack
        ack = _build_ack("test.topic")
        mock_sock.recv = MagicMock(side_effect=_ByteSource(ack))

        with patch("socket.socket", return_value=mock_sock):
            with BrokerSubscriber("localhost", 9000) as sub:
                assert sub.is_connected()
                sub.subscribe("test.topic", lambda t, d: None)
        assert not sub.is_connected()
        mock_sock.close.assert_called()

    def test_subscribe_sends_correct_frame(self):
        mock_sock = _make_mock_socket()
        ack = _build_ack("market_data.*")
        mock_sock.recv = MagicMock(side_effect=_ByteSource(ack))

        with patch("socket.socket", return_value=mock_sock):
            sub = BrokerSubscriber("localhost", 9000)
            sub.connect()
            sub.subscribe("market_data.*", lambda t, d: None)

        sent = mock_sock.sendall.call_args[0][0]
        assert sent[0] == SUBSCRIBE
        pattern_len = struct.unpack_from("<I", sent, 1)[0]
        pattern = sent[5 : 5 + pattern_len].decode()
        assert pattern == "market_data.*"

    def test_dispatch_calls_handler(self):
        mock_sock = _make_mock_socket()
        ack = _build_ack("a.b")
        msg = _build_message_frame("a.b", b"hello")
        # After the ack, supply the message frame, then trigger timeout
        mock_sock.recv = MagicMock(side_effect=_ByteSource(ack + msg, then_timeout=True))

        received = []
        with patch("socket.socket", return_value=mock_sock):
            sub = BrokerSubscriber("localhost", 9000)
            sub.connect()
            sub.subscribe("a.b", lambda t, d: received.append((t, d)))
            # Run one iteration manually via a short run
            sub._stop_event.clear()
            # Directly test _read_message + _dispatch
            topic, data = sub._read_message()
            sub._dispatch(topic, data)

        assert received == [("a.b", b"hello")]

    def test_wildcard_dispatch(self):
        received = []

        # Directly test the dispatch mechanism
        sub = BrokerSubscriber.__new__(BrokerSubscriber)
        sub._handlers = {"market_data.*": [lambda t, d: received.append((t, d))]}
        sub._dispatch("market_data.kraken", b"x")

        assert len(received) == 1
        assert received[0][0] == "market_data.kraken"


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


class _ByteSource:
    """Returns data byte-by-byte from a buffer; raises timeout after exhaustion."""

    def __init__(self, data: bytes, then_timeout: bool = False) -> None:
        self._data = data
        self._pos = 0
        self._then_timeout = then_timeout

    def __call__(self, n: int) -> bytes:
        chunk = self._data[self._pos : self._pos + n]
        if not chunk:
            if self._then_timeout:
                raise socket.timeout("timed out")
            raise OSError("no more data")
        self._pos += len(chunk)
        return chunk
