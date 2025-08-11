// Quick validation test for the ultra-high performance message broker
use std::sync::Arc;
use std::time::Instant;

use publisher::{UltraFastPublisher, UltraFastPublisherConfig};
use subscriber::{UltraFastSubscriber, UltraFastSubscriberConfig, MessageHandler, FixedTopic, UltraFastError};
use hostbuilder::{MessageBrokerHost, BrokerConfig};
use topicmanager::UltraFastTopicManager;
use protocol::broker::messages::{Orders, Trades, Order, Trade};

struct TestMessageHandler {
    messages_received: std::sync::atomic::AtomicU64,
}

impl TestMessageHandler {
    fn new() -> Self {
        Self {
            messages_received: std::sync::atomic::AtomicU64::new(0),
        }
    }
    
    fn get_count(&self) -> u64 {
        self.messages_received.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl MessageHandler for TestMessageHandler {
    fn handle_orders(&self, _orders: &Orders, _topic: &FixedTopic, latency_ns: u64) {
        self.messages_received.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        println!("📈 Received orders message with {}ns latency ({:.3}μs)", 
                 latency_ns, latency_ns as f64 / 1000.0);
    }

    fn handle_trades(&self, _trades: &Trades, _topic: &FixedTopic, latency_ns: u64) {
        self.messages_received.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        println!("💱 Received trades message with {}ns latency ({:.3}μs)", 
                 latency_ns, latency_ns as f64 / 1000.0);
    }

    fn handle_wallets(&self, _wallets: &protocol::broker::messages::Wallets, _topic: &FixedTopic, latency_ns: u64) {
        self.messages_received.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        println!("💰 Received wallets message with {}ns latency ({:.3}μs)", 
                 latency_ns, latency_ns as f64 / 1000.0);
    }

    fn handle_error(&self, error: UltraFastError, topic: &FixedTopic) {
        eprintln!("❌ Error handling message on topic '{}': {:?}", topic.as_str(), error);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("🚀 ULTRA-HIGH PERFORMANCE MESSAGE BROKER VALIDATION TEST 🚀");
    println!("═══════════════════════════════════════════════════════════════");

    // Test 1: Cross-platform compatibility
    println!("\n📋 Test 1: Cross-platform Compatibility");
    println!("Target Architecture: {}", std::env::consts::ARCH);
    println!("Operating System: {}", std::env::consts::OS);

    // Test 2: Publisher creation and basic functionality
    println!("\n📋 Test 2: Publisher Performance");
    let pub_config = UltraFastPublisherConfig::default();
    let mut publisher = UltraFastPublisher::new(pub_config)?;
    publisher.start()?;

    let orders = Orders {
        orders: vec![Order {
            unique_id: "ULTRA_FAST_001".to_string(),
            symbol: "BTCUSD".to_string(),
            exchange: "ULTRA_EXCHANGE".to_string(),
            price_level: 50000.0,
            quantity: 1.0,
            side: "BUY".to_string(),
            event: "NEW".to_string(),
        }],
    };

    let start_time = Instant::now();
    for i in 0..1000 {
        publisher.publish_orders(&orders, "ultra_fast_orders")?;
        if i % 100 == 0 {
            println!("   Published {} orders...", i + 1);
        }
    }
    let publish_duration = start_time.elapsed();
    
    let pub_metrics = publisher.get_metrics();
    println!("✅ Published 1000 orders in {:.2}ms", publish_duration.as_millis());
    println!("   • Average throughput: {:.2} messages/second", 1000.0 / publish_duration.as_secs_f64());
    println!("   • Messages published: {}", pub_metrics.messages_published);
    println!("   • Bytes published: {}", pub_metrics.bytes_published);

    // Test 3: Subscriber performance
    println!("\n📋 Test 3: Subscriber Performance");
    let sub_config = UltraFastSubscriberConfig {
        topics: vec!["orders".to_string(), "trades".to_string()],
        ..Default::default()
    };
    
    let mut subscriber = UltraFastSubscriber::new(sub_config)?;
    let handler = Arc::new(TestMessageHandler::new());
    subscriber.start(Arc::clone(&handler))?;

    // Simulate message reception
    for i in 0..500 {
        subscriber.simulate_message_received("orders", 0, 1024)?;
        subscriber.simulate_message_received("trades", 1, 512)?;
    }

    // Wait for processing
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    let sub_metrics = subscriber.get_metrics();
    println!("✅ Subscriber processed {} messages", handler.get_count());
    println!("   • Messages received: {}", sub_metrics.messages_received);
    println!("   • Average latency: {}ns ({:.3}μs)", sub_metrics.avg_latency_ns, sub_metrics.avg_latency_ns as f64 / 1000.0);
    println!("   • Min latency: {}ns ({:.3}μs)", sub_metrics.min_latency_ns, sub_metrics.min_latency_ns as f64 / 1000.0);
    println!("   • Max latency: {}ns ({:.3}μs)", sub_metrics.max_latency_ns, sub_metrics.max_latency_ns as f64 / 1000.0);

    // Test 4: Topic Manager performance
    println!("\n📋 Test 4: Topic Manager Performance");
    let topic_manager = UltraFastTopicManager::new();
    
    let start_time = Instant::now();
    for i in 0..1000 {
        let topic_name = format!("ultra_topic_{}", i);
        topic_manager.create_topic(&topic_name).await?;
    }
    let topic_creation_duration = start_time.elapsed();
    
    println!("✅ Created 1000 topics in {:.2}ms", topic_creation_duration.as_millis());
    println!("   • Topics/second: {:.2}", 1000.0 / topic_creation_duration.as_secs_f64());
    println!("   • Topic count: {}", topic_manager.get_topic_count());

    // Test 5: Message Broker Host configuration
    println!("\n📋 Test 5: Message Broker Host Configuration");
    let broker_config = BrokerConfig {
        host: "127.0.0.1".to_string(),
        port: 8080,
        max_connections: 1000,
        max_topics: 10000,
        flush_interval_ms: 1, // 1ms for ultra-low latency
        cpu_affinity: None, // Cross-platform compatibility
        realtime_priority: None, // Cross-platform compatibility
    };

    let broker = MessageBrokerHost::new(broker_config);
    let broker_metrics = broker.get_metrics();
    
    println!("✅ Message Broker Host configured");
    println!("   • Initial connections: {}", broker_metrics.connections_accepted);
    println!("   • Initial messages: {}", broker_metrics.messages_processed);

    // Cleanup
    publisher.shutdown();
    subscriber.shutdown();
    
    // Final Performance Assessment
    println!("\n🏆 FINAL PERFORMANCE ASSESSMENT 🏆");
    println!("═══════════════════════════════════════════════════");
    
    let publisher_throughput = 1000.0 / publish_duration.as_secs_f64();
    let subscriber_latency_us = sub_metrics.avg_latency_ns as f64 / 1000.0;
    let topic_creation_speed = 1000.0 / topic_creation_duration.as_secs_f64();
    
    println!("📊 KEY METRICS:");
    println!("   • Publisher Throughput: {:.2} messages/second", publisher_throughput);
    println!("   • Subscriber Average Latency: {:.3}μs", subscriber_latency_us);
    println!("   • Topic Creation Speed: {:.2} topics/second", topic_creation_speed);
    println!("   • Architecture: {} ({})", std::env::consts::ARCH, std::env::consts::OS);

    // Performance scoring
    let mut score = 0;
    
    if publisher_throughput > 10000.0 {
        println!("✅ Publisher Throughput: EXCELLENT (>10K/s)");
        score += 3;
    } else if publisher_throughput > 1000.0 {
        println!("🟡 Publisher Throughput: GOOD (>1K/s)");
        score += 2;
    } else {
        println!("🔴 Publisher Throughput: NEEDS IMPROVEMENT");
        score += 1;
    }

    if subscriber_latency_us < 1.0 {
        println!("✅ Subscriber Latency: ULTRA-FAST (<1μs)");
        score += 4;
    } else if subscriber_latency_us < 10.0 {
        println!("🟡 Subscriber Latency: FAST (<10μs)");
        score += 3;
    } else if subscriber_latency_us < 100.0 {
        println!("🟡 Subscriber Latency: GOOD (<100μs)");
        score += 2;
    } else {
        println!("🔴 Subscriber Latency: NEEDS IMPROVEMENT");
        score += 1;
    }

    if topic_creation_speed > 1000.0 {
        println!("✅ Topic Creation: EXCELLENT (>1K/s)");
        score += 3;
    } else if topic_creation_speed > 100.0 {
        println!("🟡 Topic Creation: GOOD (>100/s)");
        score += 2;
    } else {
        println!("🔴 Topic Creation: NEEDS IMPROVEMENT");
        score += 1;
    }

    // Final score
    println!("\n🎯 OVERALL PERFORMANCE SCORE: {}/10", score);
    
    match score {
        9..=10 => {
            println!("🏆 OUTSTANDING! Ultra-High Performance Message Broker");
            println!("🚀 Ready for sub-microsecond latency trading platforms!");
        }
        7..=8 => {
            println!("🥈 EXCELLENT! High Performance Message Broker");
            println!("⚡ Suitable for high-frequency trading applications!");
        }
        5..=6 => {
            println!("🥉 GOOD! Standard Performance Message Broker");
            println!("📈 Good for general trading applications!");
        }
        _ => {
            println!("⚠️  Performance needs optimization for trading workloads");
        }
    }

    println!("\n✅ Ultra-High Performance Message Broker validation completed!");
    println!("═══════════════════════════════════════════════════");

    Ok(())
}
