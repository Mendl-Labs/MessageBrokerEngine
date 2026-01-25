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

MessageBrokerEngine is the inter-service communication backbone of the TradingPlatform. It:

1. **Routes messages** between DataEngine and SignalEngine with sub-microsecond latency
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
│                           TRADING PLATFORM                                   │
│                                                                              │
│   ┌──────────────┐                                     ┌──────────────┐     │
│   │ SignalEngine │                                     │  DataEngine  │     │
│   │              │                                     │              │     │
│   │ • Subscribe  │                                     │ • Subscribe  │     │
│   │ • Publish    │                                     │ • Publish    │     │
│   │   signals    │                                     │   market data│     │
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

### TradingPlatform Topics

| Topic | Publisher | Subscriber | Description |
|-------|-----------|------------|-------------|
| `market_data.subscriptions` | SignalEngine | DataEngine | Subscription requests |
| `market_data.{exchange}.{symbol}` | DataEngine | SignalEngine | Real-time market data |
| `signals.execution` | SignalEngine | (internal) | Trading signals |

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

### Message Format (Protocol Buffers)

```protobuf
message BrokerMessage {
  string topic = 1;
  bytes payload = 2;
  int64 timestamp = 3;
  string message_id = 4;
  map<string, string> headers = 5;
}

message PublishRequest {
  repeated BrokerMessage messages = 1;
}

message SubscribeRequest {
  string topic_pattern = 1;
  bool use_regex = 2;
}
```

### Client Libraries

**Publisher:**
```rust
use messagebroker_publisher::Publisher;

let publisher = Publisher::connect("tcp://localhost:9000").await?;
publisher.publish("market_data.kraken.XBTUSD", &data).await?;
```

**Subscriber:**
```rust
use messagebroker_subscriber::Subscriber;

let subscriber = Subscriber::connect("tcp://localhost:9000").await?;
subscriber.subscribe("market_data.kraken.*").await?;

while let Some(msg) = subscriber.recv().await {
    println!("Received: {:?}", msg);
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
