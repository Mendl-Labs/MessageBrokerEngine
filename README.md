# MessageBrokerEngine

**Ultra-High Performance Message Broker**

A blazing-fast, sub-microsecond latency message broker engineered for high-frequency trading and mission-critical financial applications.

---

## Table of Contents

- [Overview](#overview)
- [Performance](#performance)
- [Architecture](#architecture)
- [Workspace Crates](#workspace-crates)
- [Topics & Routing](#topics--routing)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Protocol](#protocol)
- [Reliability Features](#reliability-features)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Environment Variables](#environment-variables)

---

## Overview

MessageBrokerEngine is a generic pub/sub message broker suitable as the inter-service communication backbone of a distributed trading platform. It:

1. **Routes messages** between any number of publisher/subscriber services with sub-microsecond latency
2. **Persists messages** via Write-Ahead Log (WAL) for crash recovery
3. **Handles backpressure** with adaptive flow control
4. **Supports patterns** including pub/sub, wildcards, and regex routing
5. **Compresses data** for bandwidth optimization (LZ4, Gzip, Snappy)

### Key Characteristics

| Attribute | Value |
|-----------|-------|
| **Single Message Latency** | 176ns |
| **Throughput (Single Thread)** | 900K msg/s |
| **Batch Processing** | 13M elem/s |
| **Multi-core Scaling** | 75.5% efficiency |
| **Protocol** | Protocol Buffers |

---

## Performance

### Benchmark Results

| Metric | Result | Target | Status |
|--------|--------|--------|--------|
| **Single Message Latency** | 176ns | <500ns | ✅ 2.8x better |
| **Throughput (Single Thread)** | 900K msg/s | 500K | ✅ 1.8x better |
| **Batch Processing** | 13M elem/s | 10M | ✅ 1.3x better |
| **RDTSC Overhead** | 59ns | <100ns | ✅ Pass |
| **Multi-core Scaling (2 threads)** | 75.5% | >70% | ✅ Pass |

### Optimization Techniques

- **RDTSC timestamping** for x86_64/ARM64
- **Cache-line alignment** to prevent false sharing
- **Zero-copy operations** where supported
- **SIMD vectorization** for batch processing
- **Lock-free data structures** using Crossbeam

---

## Architecture

### System Context

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        DISTRIBUTED APPLICATION                               │
│                                                                              │
│   ┌──────────────┐                                     ┌──────────────┐     │
│   │  Service A   │                                     │  Service B   │     │
│   │              │                                     │              │     │
│   │ • Subscribe  │                                     │ • Subscribe  │     │
│   │ • Publish    │                                     │ • Publish    │     │
│   └──────┬───────┘                                     └──────┬───────┘     │
│          │                                                    │             │
│          │              ┌───────────────────┐                 │             │
│          └─────────────▶│ MessageBrokerEngine│◀────────────────┘             │
│                         │                   │                               │
│                         │ • 176ns latency   │                               │
│                         │ • 900K msg/s      │                               │
│                         │ • WAL persistence │                               │
│                         │ • Topic routing   │                               │
│                         └───────────────────┘                               │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Internal Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        MESSAGE BROKER ENGINE                                 │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                        PUBLISHER CLIENTS                                │ │
│  │                                                                         │ │
│  │   • Message Batching        • Priority Queues       • CPU Affinity     │ │
│  │   • Smart Buffering         • Zero-Alloc            • Ring Buffers     │ │
│  └─────────────────────────────────┬───────────────────────────────────────┘ │
│                                    │                                         │
│                                    ▼                                         │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                        BROKER HOST                                      │ │
│  │                                                                         │ │
│  │   ┌──────────────┐  ┌─────────────┐  ┌──────────────┐                  │ │
│  │   │   💾 WAL     │  │ 🌊 FLOW     │  │ 🗜️ COMPRESS  │                  │ │
│  │   │ • Recovery   │  │ • Adaptive  │  │ • LZ4/Gzip   │                  │ │
│  │   │ • Checksums  │  │ • Breaker   │  │ • Snappy     │                  │ │
│  │   └──────────────┘  └─────────────┘  └──────────────┘                  │ │
│  │                                                                         │ │
│  │   ┌─────────────────────────────────────────────────────────────────┐  │ │
│  │   │                    TOPIC MANAGER                                 │  │ │
│  │   │                                                                  │  │ │
│  │   │   • Pattern Matching (Regex + Wildcards)                        │  │ │
│  │   │   • Route Caching (98.7% hit rate)                              │  │ │
│  │   │   • Subscription Management                                     │  │ │
│  │   └─────────────────────────────────────────────────────────────────┘  │ │
│  └─────────────────────────────────────┬───────────────────────────────────┘ │
│                                        │                                     │
│                                        ▼                                     │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                        SUBSCRIBER CLIENTS                               │ │
│  │                                                                         │ │
│  │   • Lock-free Queues        • Pattern Subscriptions  • Auto-reconnect  │ │
│  │   • Backpressure Handling   • Message Ordering       • Deduplication   │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Workspace Crates

| Crate | Purpose |
|-------|---------|
| `hostbuilder/` | Core broker server with WAL & flow control |
| `publisher/` | High-performance publisher client |
| `subscriber/` | Lock-free subscriber client |
| `topicmanager/` | Pattern-based routing engine |
| `protocol/` | Protocol Buffers & compression |
| `program/` | Main executable with benchmarks |

---

## Topics & Routing

### Topic Naming Convention

```
{domain}.{category}.{specific}

Examples:
  market_data.subscriptions           # Subscription requests
  market_data.kraken.XBTUSD           # Kraken XBTUSD market data
  market_data.binance.BTCUSDT         # Binance BTCUSDT market data
  signals.execution                   # Trading signals
```

### Example Topics

| Topic | Publisher | Subscriber | Description |
|-------|-----------|------------|-------------|
| `market_data.subscriptions` | consumer service | data feed service | Subscription requests |
| `market_data.{exchange}.{symbol}` | data feed service | consumer service | Real-time market data |
| `signals.execution` | strategy service | (internal) | Trading signals |

### Pattern Matching

**Wildcards:**
```
market_data.*           # Single level: market_data.subscriptions
market_data.#           # Multi level: market_data.kraken.XBTUSD
market_data.kraken.*    # All Kraken symbols
```

**Regex:**
```
^market_data\.kraken\.(XBT|ETH).*$   # Kraken BTC or ETH pairs
```

---

## Quick Start

### Prerequisites

- Rust 1.82+
- Protocol Buffers compiler (`protoc`)

### Build

```powershell
cd MessageBrokerEngine

# Build in release mode
cargo build --release --workspace
```

### Run

```powershell
# Start the broker
cargo run --release --bin program

# Default port: 9000
```

### Run Benchmarks

```powershell
cargo run --release --bin benchmark
```

---

## Configuration

### Main Configuration

```toml
[server]
host = "0.0.0.0"
port = 9000
max_connections = 1000

[performance]
batch_size = 100
batch_timeout_us = 100
buffer_size = 65536
io_threads = 4

[wal]
enabled = true
path = "./data/wal"
sync_interval_ms = 100
max_file_size_mb = 100

[flow_control]
enabled = true
high_watermark = 80  # percent
low_watermark = 60   # percent
circuit_breaker_threshold = 1000

[compression]
enabled = true
algorithm = "lz4"  # lz4, gzip, snappy
threshold_bytes = 1024
```

---

## Protocol

### Wire Format

The broker itself is payload-agnostic: `Publisher::publish()` takes an already-serialized `Vec<u8>` and a topic string, and has no knowledge of what's inside. The `protocol` crate ships one convenience schema, `PublishRequest` (Protocol Buffers via `prost`), used by the platform's own services:

```protobuf
message PublishRequest {
  string topic = 1;
  oneof payload {
    Order order = 2;
    Trade trade = 3;
    Quote quote = 4;
    ExecutionReport execution_report = 5;
    RiskAlert risk_alert = 6;
    SystemStatus system_status = 7;
    bytes raw_data = 8;
    PortfolioMessage portfolio_payload = 9;
    MarketMessage market_payload = 10;
    // ...plus strategy-deployment, market-data-subscription, and bar-aggregation variants
  }
}
```

You are not required to use this schema — bring your own protobuf/JSON/whatever and pass the serialized bytes straight to `publish()`. `PublishRequest` exists so multiple platform services agree on one wire format for the message types they actually share; extend `protocol/src/generated.rs` (or `build.rs`'s `.proto` inputs) with your own variants as needed.

### Client Libraries

**Publisher** (`publisher` crate — synchronous API; internally drives its own connection):
```rust
use publisher::{Publisher, PublisherConfig};

let mut publisher = Publisher::new(PublisherConfig::new("localhost:9000"))?;
publisher.publish(data, "market_data.kraken.XBTUSD")?; // data: Vec<u8>
```

**Subscriber** (`subscriber` crate — connect, register topics, then poll):
```rust
use subscriber::{Subscriber, ConnectionConfig};

let mut subscriber = Subscriber::new(
    ConnectionConfig::new("localhost:9000"),
    &["market_data.kraken.XBTUSD"],
)?;
subscriber.start()?;

for (topic_idx, msg) in subscriber.poll_all_messages(100) {
    println!("Received on topic {topic_idx}: {:?}", msg);
}
```

---

## Reliability Features

### Write-Ahead Log (WAL)

- **Persistence**: All messages written to disk before acknowledgment
- **Recovery**: Replay messages after crash
- **Checksums**: CRC32 validation for data integrity
- **Rotation**: Automatic log file rotation

### Flow Control

- **Adaptive backpressure**: Slow down publishers when subscribers lag
- **High/Low watermarks**: Configurable thresholds
- **Circuit breaker**: Protect against cascade failures

### Compression

| Algorithm | Ratio | Speed |
|-----------|-------|-------|
| LZ4 | 50-60% | Fastest |
| Snappy | 55-65% | Fast |
| Gzip | 70-80% | Slower |

---

## Kubernetes Deployment

### Helm Install

```powershell
# Development
helm upgrade --install message-broker ./k8s/message-broker-helm `
  -f values-dev.yaml --namespace messagebroker-dev --create-namespace

# Production
helm upgrade --install message-broker ./k8s/message-broker-helm `
  -f values-prod.yaml --namespace messagebroker
```

### Key Helm Values

| Value | Description | Default |
|-------|-------------|---------|
| `replicaCount` | Number of replicas | 3 |
| `resources.limits.memory` | Memory limit | 2Gi |
| `resources.limits.cpu` | CPU limit | 2 |
| `persistence.size` | WAL storage size | 10Gi |
| `persistence.storageClass` | Storage class | fast-ssd |

### High Availability

```yaml
# values-prod.yaml
replicaCount: 3

affinity:
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchLabels:
            app: message-broker
        topologyKey: kubernetes.io/hostname
```

---

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `BROKER_HOST` | Bind address | No (default: 0.0.0.0) |
| `BROKER_PORT` | Listen port | No (default: 9000) |
| `WAL_PATH` | WAL storage directory | No (default: ./data/wal) |
| `RUST_LOG` | Log level (info, debug, trace) | No |

---

## Testing

```powershell
# Unit tests
cargo test --workspace

# Integration tests
cargo test --workspace -- --ignored

# Benchmarks
cargo run --release --bin benchmark

# Load testing
cargo run --release --example load_test -- --publishers 10 --rate 100000
```

---

## Key Files

| File | Purpose |
|------|---------|
| `program/src/main.rs` | Entry point |
| `hostbuilder/src/lib.rs` | Core broker server |
| `publisher/src/lib.rs` | Publisher client |
| `subscriber/src/lib.rs` | Subscriber client |
| `topicmanager/src/lib.rs` | Topic routing |
| `protocol/src/lib.rs` | Protocol Buffers |
| `protos/` | Proto definitions |

---

## Monitoring

### Prometheus Metrics

```
# Message throughput
messagebroker_messages_total{topic="market_data.kraken.XBTUSD"}

# Latency histogram
messagebroker_latency_seconds{quantile="0.99"}

# Active connections
messagebroker_connections_active

# WAL size
messagebroker_wal_size_bytes

# Backpressure events
messagebroker_backpressure_events_total
```

### Health Check

```bash
curl http://localhost:9001/health
```

---

## Troubleshooting

### High Latency

1. Check WAL disk I/O performance
2. Verify compression isn't bottleneck
3. Review subscriber processing speed
4. Check network latency between services

### Message Loss

1. Verify WAL is enabled
2. Check disk space for WAL
3. Review flow control settings
4. Confirm subscribers are connected

### Connection Issues

1. Check port availability (default: 9000)
2. Verify firewall rules
3. Review max_connections setting
4. Check for resource exhaustion

---

## License

Functional Source License, Version 1.1, ALv2 Future License (FSL-1.1-ALv2) — see [LICENSE](LICENSE). Free for internal use, non-commercial research/education, and professional services; converts to Apache License 2.0 two years after each version's release.
