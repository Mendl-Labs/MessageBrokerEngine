# High-Performance Market Data Distribution System

A low-latency, high-throughput pub/sub message broker optimized for distributing financial market data in high-frequency trading environments.

## Overview

This system provides a highly efficient message distribution framework designed for performance-critical applications, particularly in financial trading systems. The architecture follows a publish-subscribe pattern with extensive optimizations for minimal latency and maximum throughput.

## Components

### Host Builder (`hostbuilder`)

The core server component that manages client connections and message routing.

- TCP server with configurable socket parameters
- Connection pooling and lifecycle management
- Support for thousands of concurrent clients
- Platform-specific optimizations for Windows and Unix-based systems
- Comprehensive metrics collection

### Topic Manager (`topicmanager`)

Manages the publish-subscribe model for efficient message distribution.

- Topic-based subscription model
- Concurrent message delivery to multiple subscribers
- Dynamic subscriber management
- Performance metrics per topic

### Publisher (`publisher`)

Client library for publishing messages to the broker.

- Non-blocking message queuing
- Batched message delivery
- Automatic reconnection
- Configurable QoS settings
- CPU affinity and thread priority optimizations

### Subscriber (`subscriber`)

Client library for receiving messages from specific topics.

- Lock-free ring buffers for message consumption
- Topic filtering
- Latency tracking
- Reconnection handling
- Optimized socket configurations

## Key Features

- **Ultra-Low Latency**: Optimized for sub-millisecond message delivery
- **High Throughput**: Capable of handling millions of messages per second
- **HFT-Ready**: Specific optimizations for financial market data workflows
- **Metrics & Monitoring**: Comprehensive performance and health metrics
- **Cross-Platform**: Support for Linux, Windows, and macOS with platform-specific optimizations
- **Production-Grade**: Error handling, logging, and testing for production environments

## Performance Optimizations

- Memory pre-allocation to minimize GC and allocation overhead
- Lock-free data structures where possible
- Fine-grained locking strategies
- Nanosecond-precision timing
- TCP socket tuning (TCP_NODELAY, TCP_QUICKACK)
- Custom connection backlog settings
- CPU affinity controls
- Real-time thread priorities on Linux
- Buffer size optimizations

## Requirements

- Rust 1.50+
- Tokio runtime
- libc (for platform-specific optimizations)
- Protocol Buffers for message serialization

## Configuration

The system uses a JSON configuration file (`appsettings.json`) with the following structure:

```json
{
  "MessageBrokerServerConfiguration": {
    "MessageBrokerServerSettings": {
      "Address": "0.0.0.0",
      "Port": 8080,
      "Backlog": 1024,
      "TcpNoDelay": true,
      "TcpKeepAliveSec": 30,
      "ReceiveBufferSize": 16384,
      "SendBufferSize": 16384,
      "MaxConnections": 10000,
      "WorkerThreads": 16
    }
  }
}
```

## Usage Examples

### Starting the Server

```rust
use hostbuilder::{HostedObject, HostedObjectTrait};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = HostedObject::build()?;
    engine.run().await?;
    Ok(())
}
```

### Publishing Messages

```rust
use publisher::{Publisher, PublisherConfig};
use protocol::broker::messages::{Order, Orders};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure publisher
    let config = PublisherConfig::new("localhost:8080")
        .with_tcp_nodelay(true)
        .with_topics(vec!["orders.btcusd".to_string()]);
    
    // Create and start publisher
    let mut publisher = Publisher::new(config)?;
    publisher.start()?;
    
    // Create order
    let order = Order {
        unique_id: "order1".to_string(),
        symbol: "BTC/USD".to_string(),
        exchange: "binance".to_string(),
        price_level: 50000.0,
        quantity: 1.5,
        side: "BUY".to_string(),
        event: "NEW".to_string(),
    };
    
    // Publish order
    publisher.publish_order(0, order)?;
    
    Ok(())
}
```

### Subscribing to Messages

```rust
use subscriber::{Subscriber, ConnectionConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure subscriber
    let config = ConnectionConfig::new("localhost:8080")
        .with_tcp_nodelay(true);
    
    // Create and start subscriber
    let mut subscriber = Subscriber::new(config, &["orders.btcusd"])?;
    subscriber.start()?;
    
    // Get topic index
    let topic_idx = subscriber.find_topic_index("orders.btcusd").unwrap();
    
    // Process messages
    loop {
        if let Some(msg) = subscriber.get_message(topic_idx) {
            println!("Received message on topic: {}", msg.get_topic().as_str());
            // Process message data...
        }
    }
}
```

## Performance Tuning

For optimal performance in production environments:

1. Set appropriate CPU affinity for publisher and subscriber clients
2. Configure real-time thread priorities on Linux systems
3. Tune socket buffer sizes based on message volume
4. Adjust worker thread counts based on available CPU cores
5. Monitor metrics to identify bottlenecks

## License

[License information]