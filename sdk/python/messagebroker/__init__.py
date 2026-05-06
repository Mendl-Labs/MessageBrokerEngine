"""MessageBrokerEngine Python SDK.

Provides publisher and subscriber clients for connecting to
the MessageBrokerEngine from Python applications.

Example usage::

    from messagebroker import BrokerPublisher, BrokerSubscriber

    # Publish a message
    with BrokerPublisher("localhost", 9000) as pub:
        pub.publish("market_data.kraken.XBTUSD", b"<payload>")

    # Subscribe and receive messages
    def on_message(topic: str, data: bytes) -> None:
        print(f"Received on {topic}: {data!r}")

    with BrokerSubscriber("localhost", 9000) as sub:
        sub.subscribe("market_data.kraken.*", on_message)
        sub.run()
"""

from .publisher import BrokerPublisher
from .subscriber import BrokerSubscriber, MessageHandler
from .protocol import (
    encode_publish,
    encode_subscribe,
    decode_incoming_message,
    PUBLISH,
    SUBSCRIBE,
    SUBSCRIBE_ACK,
)

__all__ = [
    "BrokerPublisher",
    "BrokerSubscriber",
    "MessageHandler",
    "encode_publish",
    "encode_subscribe",
    "decode_incoming_message",
    "PUBLISH",
    "SUBSCRIBE",
    "SUBSCRIBE_ACK",
]
