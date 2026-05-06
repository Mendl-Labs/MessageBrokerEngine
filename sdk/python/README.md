# MessageBrokerEngine Python SDK

Python client library for [MessageBrokerEngine](../../README.md) — an
ultra-high-performance message broker engineered for financial applications.

## Requirements

- Python 3.8+
- No mandatory runtime dependencies (pure-Python, stdlib-only)

## Installation

```bash
pip install .
# or, for development
pip install -e ".[dev]"
```

## Quick Start

### Publishing messages

```python
from messagebroker import BrokerPublisher

with BrokerPublisher("localhost", 9000) as pub:
    pub.publish("market_data.kraken.XBTUSD", b"<protobuf-payload>")
```

### Subscribing to messages

```python
from messagebroker import BrokerSubscriber

def on_message(topic: str, data: bytes) -> None:
    print(f"[{topic}] {len(data)} bytes")

with BrokerSubscriber("localhost", 9000) as sub:
    sub.subscribe("market_data.kraken.*", on_message)
    sub.run()        # blocks; call sub.stop() from another thread to exit
```

### Background subscription

```python
import time
from messagebroker import BrokerSubscriber

received = []

with BrokerSubscriber("localhost", 9000) as sub:
    sub.subscribe("signals.#", lambda t, d: received.append((t, d)))
    sub.run_in_background()
    time.sleep(10)   # do other work while messages arrive
```

## Wire Protocol

The SDK implements the MessageBrokerEngine binary wire protocol over TCP:

| Direction | Frame |
|-----------|-------|
| Client → Broker (publish) | `[0x01][topic_len: u32 LE][topic][data_len: u32 LE][data]` |
| Client → Broker (subscribe) | `[0x02][topic_len: u32 LE][topic_pattern]` |
| Broker → Client (subscribe ack) | `[0x03][topic_len: u32 LE][topic]` |
| Broker → Client (message) | `[topic_len: u32 LE][topic][data_len: u32 LE][data]` |

## Topic Wildcards

| Pattern | Matches |
|---------|---------|
| `market_data.*` | Single segment: `market_data.kraken`, `market_data.binance` |
| `market_data.#` | Multiple segments: `market_data.kraken.XBTUSD` |
| `signals.execution` | Exact match only |

## Running Tests

```bash
pip install -e ".[dev]"
pytest tests/
```
