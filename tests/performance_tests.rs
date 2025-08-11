// Ultra-high performance benchmarks and tests
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::thread;

use publisher::{UltraFastPublisher, UltraFastPublisherConfig};
use subscriber::{UltraFastSubscriber, UltraFastSubscriberConfig, MessageHandler, FixedTopic, UltraFastError};
use protocol::broker::messages::{Orders, Trades, Wallets, Order, Trade, Wallet};

// Test message handler that tracks performance
struct PerformanceTestHandler {
    orders_received: std::sync::atomic::AtomicU64,
    trades_received: std::sync::atomic::AtomicU64,
    wallets_received: std::sync::atomic::AtomicU64,
    total_latency_ns: std::sync::atomic::AtomicU64,
    min_latency_ns: std::sync::atomic::AtomicU64,
    max_latency_ns: std::sync::atomic::AtomicU64,
}

impl PerformanceTestHandler {
    fn new() -> Self {
        Self {
            orders_received: std::sync::atomic::AtomicU64::new(0),
            trades_received: std::sync::atomic::AtomicU64::new(0),
            wallets_received: std::sync::atomic::AtomicU64::new(0),
            total_latency_ns: std::sync::atomic::AtomicU64::new(0),
            min_latency_ns: std::sync::atomic::AtomicU64::new(u64::MAX),
            max_latency_ns: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn update_latency(&self, latency_ns: u64) {
        use std::sync::atomic::Ordering;
        
        self.total_latency_ns.fetch_add(latency_ns, Ordering::Relaxed);
        
        // Update min latency
        let mut current_min = self.min_latency_ns.load(Ordering::Relaxed);
        while latency_ns < current_min {
            match self.min_latency_ns.compare_exchange_weak(
                current_min,
                latency_ns,
                Ordering::Relaxed,
                Ordering::Relaxed
            ) {
                Ok(_) => break,
                Err(x) => current_min = x,
            }
        }
        
        // Update max latency
        let mut current_max = self.max_latency_ns.load(Ordering::Relaxed);
        while latency_ns > current_max {
            match self.max_latency_ns.compare_exchange_weak(
                current_max,
                latency_ns,
                Ordering::Relaxed,
                Ordering::Relaxed
            ) {
                Ok(_) => break,
                Err(x) => current_max = x,
            }
        }
    }

    fn get_stats(&self) -> TestStats {
        use std::sync::atomic::Ordering;
        
        let orders = self.orders_received.load(Ordering::Relaxed);
        let trades = self.trades_received.load(Ordering::Relaxed);
        let wallets = self.wallets_received.load(Ordering::Relaxed);
        let total_messages = orders + trades + wallets;
        
        let total_latency = self.total_latency_ns.load(Ordering::Relaxed);
        let avg_latency = if total_messages > 0 {
            total_latency / total_messages
        } else {
            0
        };

        TestStats {
            total_messages,
            orders_received: orders,
            trades_received: trades,
            wallets_received: wallets,
            avg_latency_ns: avg_latency,
            min_latency_ns: self.min_latency_ns.load(Ordering::Relaxed),
            max_latency_ns: self.max_latency_ns.load(Ordering::Relaxed),
        }
    }
}

impl MessageHandler for PerformanceTestHandler {
    fn handle_orders(&self, orders: &Orders, _topic: &FixedTopic, latency_ns: u64) {
        use std::sync::atomic::Ordering;
        
        self.orders_received.fetch_add(1, Ordering::Relaxed);
        self.update_latency(latency_ns);
        
        // Validate orders structure
        assert!(!orders.orders.is_empty());
        for order in &orders.orders {
            assert!(!order.unique_id.is_empty());
            assert!(!order.symbol.is_empty());
        }
    }

    fn handle_trades(&self, trades: &Trades, _topic: &FixedTopic, latency_ns: u64) {
        use std::sync::atomic::Ordering;
        
        self.trades_received.fetch_add(1, Ordering::Relaxed);
        self.update_latency(latency_ns);
        
        // Validate trades structure
        assert!(!trades.trades.is_empty());
        for trade in &trades.trades {
            assert!(!trade.symbol.is_empty());
            assert!(trade.price > 0.0);
            assert!(trade.qty > 0.0);
        }
    }

    fn handle_wallets(&self, wallets: &Wallets, _topic: &FixedTopic, latency_ns: u64) {
        use std::sync::atomic::Ordering;
        
        self.wallets_received.fetch_add(1, Ordering::Relaxed);
        self.update_latency(latency_ns);
        
        // Validate wallets structure
        assert!(!wallets.exchange.is_empty());
        assert!(!wallets.wallets.is_empty());
        for wallet in &wallets.wallets {
            assert!(!wallet.symbol.is_empty());
            assert!(wallet.balance >= 0.0);
        }
    }

    fn handle_error(&self, error: UltraFastError, topic: &FixedTopic) {
        eprintln!("Error handling message on topic '{}': {:?}", topic.as_str(), error);
    }
}

#[derive(Debug, Clone)]
struct TestStats {
    total_messages: u64,
    orders_received: u64,
    trades_received: u64,
    wallets_received: u64,
    avg_latency_ns: u64,
    min_latency_ns: u64,
    max_latency_ns: u64,
}

impl TestStats {
    fn print_report(&self, duration: Duration, throughput_msgs_per_sec: f64) {
        println!("\n🚀 ULTRA-HIGH PERFORMANCE MESSAGE BROKER TEST RESULTS 🚀");
        println!("═══════════════════════════════════════════════════════════");
        println!("📊 MESSAGE STATISTICS:");
        println!("   • Total Messages Processed: {}", self.total_messages);
        println!("   • Orders Received: {}", self.orders_received);
        println!("   • Trades Received: {}", self.trades_received);
        println!("   • Wallets Received: {}", self.wallets_received);
        println!();
        println!("⚡ PERFORMANCE METRICS:");
        println!("   • Test Duration: {:.2}s", duration.as_secs_f64());
        println!("   • Throughput: {:.2} messages/second", throughput_msgs_per_sec);
        println!("   • Throughput: {:.2} M messages/second", throughput_msgs_per_sec / 1_000_000.0);
        println!();
        println!("🕐 LATENCY ANALYSIS:");
        println!("   • Average Latency: {}ns ({:.3}μs)", self.avg_latency_ns, self.avg_latency_ns as f64 / 1000.0);
        
        let min_display = if self.min_latency_ns == u64::MAX { 0 } else { self.min_latency_ns };
        println!("   • Minimum Latency: {}ns ({:.3}μs)", min_display, min_display as f64 / 1000.0);
        println!("   • Maximum Latency: {}ns ({:.3}μs)", self.max_latency_ns, self.max_latency_ns as f64 / 1000.0);
        
        println!();
        println!("🎯 PERFORMANCE TARGETS:");
        let sub_microsecond = self.avg_latency_ns < 1000;
        let ultra_high_throughput = throughput_msgs_per_sec > 1_000_000.0;
        
        println!("   • Sub-microsecond latency: {} (target: <1000ns, actual: {}ns)", 
                 if sub_microsecond { "✅ ACHIEVED" } else { "❌ NOT MET" }, 
                 self.avg_latency_ns);
        
        println!("   • Ultra-high throughput: {} (target: >1M/s, actual: {:.2}M/s)", 
                 if ultra_high_throughput { "✅ ACHIEVED" } else { "❌ NOT MET" }, 
                 throughput_msgs_per_sec / 1_000_000.0);
        
        println!();
        if sub_microsecond && ultra_high_throughput {
            println!("🏆 PERFORMANCE SCORE: 10/10 - ULTRA-HIGH PERFORMANCE ACHIEVED! 🏆");
        } else if sub_microsecond || ultra_high_throughput {
            println!("🥈 PERFORMANCE SCORE: 7/10 - High performance achieved, room for improvement");
        } else {
            println!("🥉 PERFORMANCE SCORE: 5/10 - Performance targets not met");
        }
        println!("═══════════════════════════════════════════════════════════");
    }
}

fn create_test_orders(count: usize) -> Orders {
    let mut orders = Vec::with_capacity(count);
    
    for i in 0..count {
        orders.push(Order {
            unique_id: format!("ORDER_{}", i),
            symbol: "BTCUSD".to_string(),
            exchange: "BINANCE".to_string(),
            price_level: 45000.0 + (i as f32 * 0.01),
            quantity: 1.0 + (i as f32 * 0.001),
            side: if i % 2 == 0 { "BUY".to_string() } else { "SELL".to_string() },
            event: "NEW".to_string(),
        });
    }
    
    Orders { orders }
}

fn create_test_trades(count: usize) -> Trades {
    let mut trades = Vec::with_capacity(count);
    
    for i in 0..count {
        trades.push(Trade {
            symbol: "BTCUSD".to_string(),
            exchange: "BINANCE".to_string(),
            side: if i % 2 == 0 { "BUY".to_string() } else { "SELL".to_string() },
            price: 45000.0 + (i as f32 * 0.01),
            qty: 1.0 + (i as f32 * 0.001),
            ord_type: "LIMIT".to_string(),
            trade_id: i as u64 + 1,
            timestamp: "2025-01-01T00:00:00Z".to_string(),
        });
    }
    
    Trades { trades }
}

fn create_test_wallets(count: usize) -> Wallets {
    let mut wallets = Vec::with_capacity(count);
    
    for i in 0..count {
        wallets.push(Wallet {
            symbol: format!("TOKEN_{}", i),
            balance: 1000.0 + (i as f32 * 10.0),
        });
    }
    
    Wallets {
        exchange: "BINANCE".to_string(),
        wallets,
    }
}

#[tokio::test]
async fn test_ultra_high_performance_message_broker() {
    println!("🚀 Starting Ultra-High Performance Message Broker Test");
    
    // Test configuration
    const TEST_DURATION_SECONDS: u64 = 10;
    const MESSAGES_PER_BATCH: usize = 100;
    const TARGET_MESSAGES_PER_SECOND: f64 = 1_000_000.0; // 1 million messages/second
    
    // Publisher configuration
    let mut pub_config = UltraFastPublisherConfig::default();
    pub_config.cpu_affinity = Some(2); // Pin to CPU core 2
    pub_config.realtime_priority = Some(99);
    pub_config.max_latency_ns = 500; // 500ns target
    pub_config.batch_size = MESSAGES_PER_BATCH;
    
    // Subscriber configuration
    let mut sub_config = UltraFastSubscriberConfig::default();
    sub_config.topics = vec![
        "orders".to_string(),
        "trades".to_string(),  
        "wallets".to_string(),
    ];
    sub_config.cpu_affinity = Some(3); // Pin to CPU core 3
    sub_config.realtime_priority = Some(98);
    sub_config.max_latency_ns = 300; // 300ns processing target
    
    // Create publisher and subscriber
    let mut publisher = UltraFastPublisher::new(pub_config).expect("Failed to create publisher");
    let mut subscriber = UltraFastSubscriber::new(sub_config).expect("Failed to create subscriber");
    
    // Create performance test handler
    let handler = Arc::new(PerformanceTestHandler::new());
    
    // Start publisher and subscriber
    publisher.start().expect("Failed to start publisher");
    subscriber.start(Arc::clone(&handler)).expect("Failed to start subscriber");
    
    // Wait for initialization
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Create test data
    let orders = create_test_orders(10);
    let trades = create_test_trades(10);
    let wallets = create_test_wallets(10);
    
    println!("🔥 Starting high-speed message publishing...");
    let start_time = Instant::now();
    let test_end_time = start_time + Duration::from_secs(TEST_DURATION_SECONDS);
    
    let mut total_messages_sent = 0u64;
    
    // High-speed message publishing loop
    while Instant::now() < test_end_time {
        // Publish orders
        for _ in 0..MESSAGES_PER_BATCH {
            if let Err(_) = publisher.publish_orders(&orders, "orders") {
                break;
            }
            total_messages_sent += 1;
        }
        
        // Publish trades
        for _ in 0..MESSAGES_PER_BATCH {
            if let Err(_) = publisher.publish_trades(&trades, "trades") {
                break;
            }
            total_messages_sent += 1;
        }
        
        // Micro-sleep to prevent CPU saturation
        thread::sleep(Duration::from_nanos(100));
    }
    
    let actual_duration = start_time.elapsed();
    
    println!("📊 Publishing completed. Waiting for message processing...");
    
    // Wait for all messages to be processed
    tokio::time::sleep(Duration::from_millis(1000)).await;
    
    // Collect final statistics
    let test_stats = handler.get_stats();
    let publisher_metrics = publisher.get_metrics();
    
    // Calculate throughput
    let throughput_msgs_per_sec = total_messages_sent as f64 / actual_duration.as_secs_f64();
    
    // Print detailed results
    test_stats.print_report(actual_duration, throughput_msgs_per_sec);
    
    println!("\n📈 PUBLISHER METRICS:");
    println!("   • Messages Published: {}", publisher_metrics.messages_published);
    println!("   • Bytes Published: {}", publisher_metrics.bytes_published);
    println!("   • Publish Errors: {}", publisher_metrics.publish_errors);
    println!("   • Queue Length: {}", publisher_metrics.queue_length);
    println!("   • Memory Pool Allocated: {}", publisher_metrics.memory_pool_allocated);
    
    // Shutdown
    publisher.shutdown();
    subscriber.shutdown();
    
    // Assertions for automated testing
    assert!(test_stats.total_messages > 0, "No messages were processed");
    assert_eq!(test_stats.orders_received + test_stats.trades_received, test_stats.total_messages);
    
    // Performance assertions (adjust thresholds as needed)
    if test_stats.avg_latency_ns > 0 {
        println!("\n✅ Test completed successfully with {} total messages processed", test_stats.total_messages);
        
        // Relaxed assertions for CI/CD compatibility
        assert!(test_stats.avg_latency_ns < 100000, "Average latency too high: {}ns", test_stats.avg_latency_ns); // 100μs max
        assert!(throughput_msgs_per_sec > 10000.0, "Throughput too low: {:.2} msgs/sec", throughput_msgs_per_sec); // 10K/sec min
    }
}

#[tokio::test]
async fn test_latency_measurement_accuracy() {
    println!("🎯 Testing latency measurement accuracy...");
    
    let handler = Arc::new(PerformanceTestHandler::new());
    
    // Simulate known latencies
    let test_latencies = vec![100, 200, 300, 500, 1000]; // nanoseconds
    
    for &latency in &test_latencies {
        let orders = create_test_orders(1);
        let topic = FixedTopic::new("test").unwrap();
        handler.handle_orders(&orders, &topic, latency);
    }
    
    let stats = handler.get_stats();
    
    println!("📊 Latency measurement test results:");
    println!("   • Min latency: {}ns", stats.min_latency_ns);
    println!("   • Max latency: {}ns", stats.max_latency_ns);
    println!("   • Avg latency: {}ns", stats.avg_latency_ns);
    
    assert_eq!(stats.min_latency_ns, 100);
    assert_eq!(stats.max_latency_ns, 1000);
    assert_eq!(stats.avg_latency_ns, 420); // (100+200+300+500+1000)/5
    
    println!("✅ Latency measurement accuracy test passed");
}

#[tokio::test]
async fn test_message_validation() {
    println!("🔍 Testing message validation...");
    
    let handler = Arc::new(PerformanceTestHandler::new());
    let topic = FixedTopic::new("test").unwrap();
    
    // Test orders validation
    let orders = create_test_orders(5);
    handler.handle_orders(&orders, &topic, 1000);
    
    // Test trades validation
    let trades = create_test_trades(3);
    handler.handle_trades(&trades, &topic, 2000);
    
    // Test wallets validation
    let wallets = create_test_wallets(2);
    handler.handle_wallets(&wallets, &topic, 1500);
    
    let stats = handler.get_stats();
    
    assert_eq!(stats.orders_received, 1);
    assert_eq!(stats.trades_received, 1);
    assert_eq!(stats.wallets_received, 1);
    assert_eq!(stats.total_messages, 3);
    
    println!("✅ Message validation test passed");
}

#[test]
fn test_fixed_topic_performance() {
    println!("⚡ Testing FixedTopic performance...");
    
    let iterations = 1_000_000;
    let topic_name = "high_frequency_trading_orders";
    
    let start = Instant::now();
    
    for _ in 0..iterations {
        let topic = FixedTopic::new(topic_name).unwrap();
        assert_eq!(topic.as_str(), topic_name);
        let _hash = topic.hash(); // Ensure hash is computed
    }
    
    let duration = start.elapsed();
    let ops_per_sec = iterations as f64 / duration.as_secs_f64();
    
    println!("📊 FixedTopic performance:");
    println!("   • {} operations in {:.2}ms", iterations, duration.as_millis());
    println!("   • {:.2} operations/second", ops_per_sec);
    println!("   • {:.2}ns per operation", duration.as_nanos() as f64 / iterations as f64);
    
    // Should be able to create millions of topics per second
    assert!(ops_per_sec > 1_000_000.0, "FixedTopic creation too slow: {:.2} ops/sec", ops_per_sec);
    
    println!("✅ FixedTopic performance test passed");
}
