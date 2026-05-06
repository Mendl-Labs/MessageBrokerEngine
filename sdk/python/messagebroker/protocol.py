"""Wire-protocol helpers for MessageBrokerEngine.

Wire format (all integers are little-endian):

  PUBLISH   -> [0x01][topic_len: u32][topic: bytes][data_len: u32][data: bytes]
  SUBSCRIBE -> [0x02][topic_len: u32][topic_pattern: bytes]

Incoming (server -> client):
  SUBSCRIBE_ACK -> [0x03][topic_len: u32][topic: bytes]
  MESSAGE       -> [topic_len: u32][topic: bytes][data_len: u32][data: bytes]
"""

from __future__ import annotations

import struct
from typing import Tuple

# Message-type bytes used on the wire.
PUBLISH: int = 0x01
SUBSCRIBE: int = 0x02
SUBSCRIBE_ACK: int = 0x03

# Maximum sizes enforced by the broker (see hostbuilder/src/lib.rs).
_MAX_TOPIC_LEN: int = 1024
_MAX_DATA_LEN: int = 16 * 1024 * 1024  # 16 MiB


def encode_publish(topic: str, data: bytes) -> bytes:
    """Return the bytes for a PUBLISH frame.

    Args:
        topic: Topic name to publish on.
        data:  Payload bytes to send.

    Returns:
        A ``bytes`` object ready to be written to the broker TCP socket.

    Raises:
        ValueError: If *topic* or *data* exceed broker limits.
    """
    topic_bytes = topic.encode("utf-8")
    if len(topic_bytes) > _MAX_TOPIC_LEN:
        raise ValueError(
            f"Topic too long ({len(topic_bytes)} bytes, max {_MAX_TOPIC_LEN})"
        )
    if len(data) > _MAX_DATA_LEN:
        raise ValueError(
            f"Payload too large ({len(data)} bytes, max {_MAX_DATA_LEN})"
        )

    return (
        struct.pack("<B", PUBLISH)
        + struct.pack("<I", len(topic_bytes))
        + topic_bytes
        + struct.pack("<I", len(data))
        + data
    )


def encode_subscribe(topic_pattern: str) -> bytes:
    """Return the bytes for a SUBSCRIBE frame.

    Args:
        topic_pattern: Topic or wildcard pattern to subscribe to.

    Returns:
        A ``bytes`` object ready to be written to the broker TCP socket.

    Raises:
        ValueError: If *topic_pattern* exceeds the broker limit.
    """
    pattern_bytes = topic_pattern.encode("utf-8")
    if len(pattern_bytes) > _MAX_TOPIC_LEN:
        raise ValueError(
            f"Topic pattern too long ({len(pattern_bytes)} bytes, max {_MAX_TOPIC_LEN})"
        )

    return (
        struct.pack("<B", SUBSCRIBE)
        + struct.pack("<I", len(pattern_bytes))
        + pattern_bytes
    )


def decode_incoming_message(data: bytes) -> Tuple[str, bytes]:
    """Decode a raw message frame sent by the broker to a subscriber.

    The broker sends messages without a leading message-type byte:

        [topic_len: u32 LE][topic: bytes][data_len: u32 LE][data: bytes]

    Args:
        data: The complete frame bytes received from the broker.

    Returns:
        A ``(topic, payload)`` tuple.

    Raises:
        ValueError: If *data* is too short or the frame is malformed.
    """
    offset = 0
    if len(data) < 4:
        raise ValueError("Frame too short to contain topic length")

    topic_len = struct.unpack_from("<I", data, offset)[0]
    offset += 4

    if offset + topic_len > len(data):
        raise ValueError("Frame truncated (topic)")
    topic = data[offset : offset + topic_len].decode("utf-8")
    offset += topic_len

    if offset + 4 > len(data):
        raise ValueError("Frame truncated (data length)")
    data_len = struct.unpack_from("<I", data, offset)[0]
    offset += 4

    if offset + data_len > len(data):
        raise ValueError("Frame truncated (data)")
    payload = data[offset : offset + data_len]
    return topic, payload
