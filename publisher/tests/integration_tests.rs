use std::time::Duration;
use serial_test::serial;

use publisher::{
    UltraFastPublisher, PublisherConfig, PerformanceStats, MessagePriority, 
    Publisher, Order, PendingMessage
};

#[tokio::test]
async fn test_performance_stats_creation() {
    let stats = PerformanceStats::new();
    let (messages_sent, avg_latency, total_bytes, peak_throughput, connection_failures, queue_depth) = stats.get_stats();
    
    assert_eq!(messages_sent, 0);
    assert_eq!(avg_latency, 0.0);
    assert_eq!(total_bytes, 0);
    assert_eq!(peak_throughput, 0);
    assert_eq!(connection_failures, 0);
    assert_eq!(queue_depth, 0);
}

#[tokio::test]
async fn test_performance_stats_recording() {
    let stats = PerformanceStats::new();
    
    // Record some performance metrics
    stats.record_message_sent(1000, 256); // 1000ns latency, 256 bytes
    stats.record_message_sent(2000, 512); // 2000ns latency, 512 bytes
    stats.record_connection_failure();
    
    let (messages_sent, avg_latency, total_bytes, _, connection_failures, _) = stats.get_stats();
    
    assert_eq!(messages_sent, 2);
    assert_eq!(total_bytes, 768); // 256 + 512
    assert_eq!(connection_failures, 1);
    // Average latency should be around 1500ns ((1000 + 2000) / 2)
    assert!(avg_latency > 1400.0 && avg_latency < 1600.0);
}

#[tokio::test]
async fn test_performance_stats_reset() {
    let stats = PerformanceStats::new();
    
    // Record some metrics
    stats.record_message_sent(1000, 256);
    stats.record_connection_failure();
    
    // Verify metrics were recorded
    let (messages_sent, _, total_bytes, _, connection_failures, _) = stats.get_stats();
    assert_eq!(messages_sent, 1);
    assert_eq!(total_bytes, 256);
    assert_eq!(connection_failures, 1);
    
    // Reset and verify
    stats.reset();
    let (messages_sent, avg_latency, total_bytes, peak_throughput, connection_failures, queue_depth) = stats.get_stats();
    assert_eq!(messages_sent, 0);
    assert_eq!(avg_latency, 0.0);
    assert_eq!(total_bytes, 0);
    assert_eq!(peak_throughput, 0);
    assert_eq!(connection_failures, 0);
    assert_eq!(queue_depth, 0);
}

#[tokio::test]
async fn test_publisher_config_builder() {
    let config = PublisherConfig::new("127.0.0.1:8080")
        .with_batch_size(100)
        .with_flush_interval(Duration::from_millis(50))
        .with_tcp_nodelay(true)
        .with_send_buffer_size(64 * 1024)
        .with_topics(vec!["topic1".to_string(), "topic2".to_string()])
        .with_retry_attempts(3)
        .with_connection_timeout(Duration::from_secs(5));
    
    assert_eq!(config.broker_address, "127.0.0.1:8080");
    assert_eq!(config.batch_size, 100);
    assert_eq!(config.flush_interval, Duration::from_millis(50));
    assert_eq!(config.tcp_nodelay, true);
    assert_eq!(config.send_buffer_size, 64 * 1024);
    assert_eq!(config.topics, vec!["topic1".to_string(), "topic2".to_string()]);
    assert_eq!(config.retry_attempts, 3);
    assert_eq!(config.connection_timeout, Duration::from_secs(5));
}

#[tokio::test]
async fn test_message_priority_enum() {
    use std::mem;
    
    // Test that MessagePriority is properly defined
    let high = MessagePriority::High;
    let normal = MessagePriority::Normal;
    let low = MessagePriority::Low;
    
    // Ensure enum takes minimal memory
    assert_eq!(mem::size_of_val(&high), 1);
    assert_eq!(mem::size_of_val(&normal), 1);
    assert_eq!(mem::size_of_val(&low), 1);
    
    // Test conversion to u8
    assert_eq!(high as u8, 2);
    assert_eq!(normal as u8, 1);
    assert_eq!(low as u8, 0);
}

#[tokio::test]
async fn test_pending_message_creation() {
    let message = PendingMessage {
        topic: "test-topic".to_string(),
        data: b"test data".to_vec(),
        priority: MessagePriority::High,
        timestamp: 1234567890,
        sequence: 42,
    };
    
    assert_eq!(message.topic, "test-topic");
    assert_eq!(message.data, b"test data");
    assert!(matches!(message.priority, MessagePriority::High));
    assert_eq!(message.timestamp, 1234567890);
    assert_eq!(message.sequence, 42);
}

#[tokio::test]
#[serial]
async fn test_ultra_fast_publisher_creation() {
    let config = PublisherConfig::new("127.0.0.1:8080")
        .with_batch_size(50)
        .with_tcp_nodelay(true);
    
    let publisher = UltraFastPublisher::new(config);
    
    // Test performance stats retrieval
    let (messages_sent, avg_latency, total_bytes, peak_throughput, connection_failures, queue_depth) = 
        publisher.get_performance_stats();
    
    assert_eq!(messages_sent, 0);
    assert_eq!(avg_latency, 0.0);
    assert_eq!(total_bytes, 0);
    assert_eq!(peak_throughput, 0);
    assert_eq!(connection_failures, 0);
    assert_eq!(queue_depth, 0);
}

#[tokio::test]
#[serial]
async fn test_ultra_fast_publisher_stats_reset() {
    let config = PublisherConfig::new("127.0.0.1:8080");
    let publisher = UltraFastPublisher::new(config);
    
    // Reset stats (should work without errors)
    publisher.reset_performance_stats();
    
    let (messages_sent, avg_latency, total_bytes, peak_throughput, connection_failures, queue_depth) = 
        publisher.get_performance_stats();
    
    assert_eq!(messages_sent, 0);
    assert_eq!(avg_latency, 0.0);
    assert_eq!(total_bytes, 0);
    assert_eq!(peak_throughput, 0);
    assert_eq!(connection_failures, 0);
    assert_eq!(queue_depth, 0);
}

#[tokio::test]
async fn test_order_structure() {
    let order = Order {
        unique_id: "order_123".to_string(),
        symbol: "BTCUSD".to_string(),
        exchange: "BINANCE".to_string(),
        price_level: 50000.0,
        quantity: 1.5,
        side: "BUY".to_string(),
        event: "NEW".to_string(),
    };
    
    assert_eq!(order.unique_id, "order_123");
    assert_eq!(order.symbol, "BTCUSD");
    assert_eq!(order.exchange, "BINANCE");
    assert_eq!(order.price_level, 50000.0);
    assert_eq!(order.quantity, 1.5);
    assert_eq!(order.side, "BUY");
    assert_eq!(order.event, "NEW");
}

#[test]
#[serial]
fn test_publisher_creation() {
    let config = PublisherConfig::new("127.0.0.1:8080")
        .with_topics(vec!["orders".to_string(), "trades".to_string()]);
    
    let _publisher = Publisher::new(config.clone());
    
    // Test that publisher is created with proper configuration
    // We can't easily test internal state, but we can verify it doesn't panic
    assert!(true); // Placeholder assertion - publisher created successfully
}

#[tokio::test]
async fn test_concurrent_performance_stats() {
    let stats = std::sync::Arc::new(PerformanceStats::new());
    let mut handles = vec![];
    
    // Spawn multiple tasks to update stats concurrently
    for i in 0..10 {
        let stats_clone = std::sync::Arc::clone(&stats);
        let handle = tokio::spawn(async move {
            for j in 0..100 {
                stats_clone.record_message_sent((i * 100 + j) as u64, 256);
                if j % 10 == 0 {
                    stats_clone.record_connection_failure();
                }
            }
        });
        handles.push(handle);
    }
    
    // Wait for all tasks to complete
    for handle in handles {
        handle.await.unwrap();
    }
    
    let (messages_sent, _, total_bytes, _, connection_failures, _) = stats.get_stats();
    
    // Should have recorded 10 * 100 = 1000 messages
    assert_eq!(messages_sent, 1000);
    // Should have recorded 1000 * 256 = 256000 bytes
    assert_eq!(total_bytes, 256000);
    // Should have recorded 10 * 10 = 100 connection failures (every 10th message)
    assert_eq!(connection_failures, 100);
}

#[tokio::test]
async fn test_message_priority_ordering() {
    let high = MessagePriority::High;
    let normal = MessagePriority::Normal;
    let low = MessagePriority::Low;
    
    // Test that priority values are in correct order for sorting
    assert!(high as u8 > normal as u8);
    assert!(normal as u8 > low as u8);
    assert!(high as u8 > low as u8);
}

#[tokio::test]
async fn test_performance_stats_edge_cases() {
    let stats = PerformanceStats::new();
    
    // Test with zero latency
    stats.record_message_sent(0, 100);
    let (messages_sent, avg_latency, total_bytes, _, _, _) = stats.get_stats();
    assert_eq!(messages_sent, 1);
    assert_eq!(avg_latency, 0.0);
    assert_eq!(total_bytes, 100);
    
    // Test with zero bytes
    stats.reset();
    stats.record_message_sent(1000, 0);
    let (messages_sent, avg_latency, total_bytes, _, _, _) = stats.get_stats();
    assert_eq!(messages_sent, 1);
    assert_eq!(avg_latency, 1000.0);
    assert_eq!(total_bytes, 0);
}

#[tokio::test]
async fn test_publisher_config_defaults() {
    let config = PublisherConfig::new("localhost:9000");
    
    assert_eq!(config.broker_address, "localhost:9000");
    assert_eq!(config.batch_size, 1000); // Default batch size
    assert_eq!(config.flush_interval, Duration::from_millis(10)); // Default flush interval
    assert_eq!(config.tcp_nodelay, true); // Default TCP nodelay
    assert_eq!(config.send_buffer_size, 65536); // Default send buffer
    assert_eq!(config.topics.len(), 0); // Default empty topics
    assert_eq!(config.retry_attempts, 3); // Default retry attempts
    assert_eq!(config.connection_timeout, Duration::from_secs(10)); // Default timeout
}

#[tokio::test]
async fn test_performance_under_load() {
    let stats = PerformanceStats::new();
    let start = std::time::Instant::now();
    
    // Simulate high-load scenario
    let num_operations = 10000;
    for i in 0..num_operations {
        stats.record_message_sent((i % 1000) as u64, (i % 512) + 256);
        if i % 100 == 0 {
            stats.record_connection_failure();
        }
    }
    
    let duration = start.elapsed();
    let (messages_sent, avg_latency, total_bytes, _, connection_failures, _) = stats.get_stats();
    
    assert_eq!(messages_sent, num_operations as u64);
    assert_eq!(connection_failures, (num_operations / 100) as u64);
    
    // Performance assertion - should complete in reasonable time
    assert!(duration.as_millis() < 100, "Operations took too long: {:?}", duration);
    
    // Verify average latency calculation
    assert!(avg_latency >= 0.0 && avg_latency <= 1000.0);
    
    println!("Completed {} operations in {:?}", num_operations, duration);
    println!("Average latency: {:.2}ns", avg_latency);
    println!("Total bytes: {}", total_bytes);
}
