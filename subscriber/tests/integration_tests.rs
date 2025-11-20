use std::time::Duration;
use std::sync::Arc;
use serial_test::serial;

use subscriber::{
    UltraFastSubscriber, PerformanceStats, UltraFastMessage, 
    ConnectionConfig, Subscriber, MessageHandler
};

#[tokio::test]
async fn test_performance_stats_creation() {
    let stats = PerformanceStats::new();
    
    // Test initial state  
    let (count, avg_latency, min_latency, max_latency) = stats.get_stats();
    assert_eq!(count, 0);
    assert_eq!(avg_latency, 0.0);
    assert_eq!(min_latency, u64::MAX); // Initial min is MAX
    assert_eq!(max_latency, 0);
}

#[tokio::test]
async fn test_performance_stats_operations() {
    let stats = PerformanceStats::new();
    
    // Record some operations
    stats.record_latency(1000);
    stats.record_latency(2000);
    
    let (count, avg_latency, min_latency, max_latency) = stats.get_stats();
    assert_eq!(count, 2);
    assert_eq!(min_latency, 1000);
    assert_eq!(max_latency, 2000);
    // Average should be around 1500
    assert!(avg_latency > 1400.0 && avg_latency < 1600.0);
}

#[tokio::test]
async fn test_ultra_fast_message_creation() {
    let message = UltraFastMessage {
        topic: "test-topic".to_string(),
        data: b"Hello, World!".to_vec(),
        timestamp: 1234567890,
        sequence: 42,
    };
    
    assert_eq!(message.topic, "test-topic");
    assert_eq!(message.data, b"Hello, World!");
    assert_eq!(message.timestamp, 1234567890);
    assert_eq!(message.sequence, 42);
}

#[tokio::test]
async fn test_ultra_fast_message_methods() {
    let message = UltraFastMessage::new(
        "method-test".to_string(),
        b"test data".to_vec(),
        100,
    );
    
    assert_eq!(message.get_topic(), "method-test");
    assert_eq!(message.get_data(), b"test data");
    assert_eq!(message.get_sequence(), 100);
    assert!(message.get_timestamp() > 0);
}

#[tokio::test]
async fn test_ultra_fast_message_data_integrity() {
    let message = UltraFastMessage {
        topic: "integrity-test".to_string(),
        data: vec![0, 1, 2, 3, 4, 5, 255],
        timestamp: u64::MAX,
        sequence: u64::MAX,
    };
    
    // Test boundary values
    assert_eq!(message.timestamp, u64::MAX);
    assert_eq!(message.sequence, u64::MAX);
    assert_eq!(message.data, vec![0, 1, 2, 3, 4, 5, 255]);
}

#[tokio::test]
#[serial]
async fn test_ultra_fast_subscriber_creation() {
    let _subscriber = UltraFastSubscriber::new(42);
    
    // Test subscriber creation doesn't panic
    assert!(true); // Placeholder - subscriber created successfully
}

#[tokio::test]
async fn test_connection_config_creation() {
    let config = ConnectionConfig {
        address: "127.0.0.1".to_string(),
        port: 8080,
        tcp_nodelay: true,
        receive_buffer_size: 64 * 1024,
        connection_timeout: Duration::from_secs(10),
        keepalive: true,
    };
    
    assert_eq!(config.address, "127.0.0.1");
    assert_eq!(config.port, 8080);
    assert_eq!(config.tcp_nodelay, true);
    assert_eq!(config.receive_buffer_size, 64 * 1024);
    assert_eq!(config.connection_timeout, Duration::from_secs(10));
    assert_eq!(config.keepalive, true);
}

#[tokio::test]
async fn test_connection_config_new_method() {
    let config = ConnectionConfig::new("192.168.1.1:9000");
    
    assert_eq!(config.address, "192.168.1.1");
    assert_eq!(config.port, 9000);
    assert_eq!(config.tcp_nodelay, true);
    assert_eq!(config.receive_buffer_size, 65536);
    assert_eq!(config.connection_timeout, Duration::from_secs(5));
    assert_eq!(config.keepalive, true);
}

#[tokio::test]
async fn test_connection_config_default_port() {
    let config = ConnectionConfig::new("localhost");
    
    assert_eq!(config.address, "localhost");
    assert_eq!(config.port, 8080); // Default port
    assert_eq!(config.tcp_nodelay, true);
}

#[tokio::test]
#[serial]
async fn test_subscriber_creation() {
    let config = ConnectionConfig::new("127.0.0.1:8080");
    let topics = ["topic1", "topic2"];
    
    let subscriber_result = Subscriber::new(config, &topics);
    
    // Test that subscriber is created successfully
    assert!(subscriber_result.is_ok());
}

#[tokio::test]
async fn test_concurrent_performance_stats() {
    let stats = Arc::new(PerformanceStats::new());
    let mut handles = vec![];
    
    // Spawn multiple tasks to update stats concurrently
    for i in 0..10 {
        let stats_clone = Arc::clone(&stats);
        let handle = tokio::spawn(async move {
            for j in 0..100 {
                stats_clone.record_latency(((i * 100 + j) % 1000) as u64);
            }
        });
        handles.push(handle);
    }
    
    // Wait for all tasks to complete
    for handle in handles {
        handle.await.unwrap();
    }
    
    // Verify final counts
    let (count, _, _, _) = stats.get_stats();
    assert_eq!(count, 1000);
}

#[tokio::test]
async fn test_performance_stats_reset() {
    let stats = PerformanceStats::new();
    
    // Record some data
    stats.record_latency(1000);
    
    // Verify data was recorded
    let (count, _, _, _) = stats.get_stats();
    assert_eq!(count, 1);
    
    // Reset stats
    stats.reset();
    
    // Verify reset
    let (count, avg_latency, min_latency, max_latency) = stats.get_stats();
    assert_eq!(count, 0);
    assert_eq!(avg_latency, 0.0);
    assert_eq!(min_latency, u64::MAX); // Min resets to MAX
    assert_eq!(max_latency, 0);
}

#[tokio::test]
async fn test_message_handler_trait() {
    // Test that we can create a simple message handler implementation
    struct TestHandler;
    
    impl MessageHandler for TestHandler {
        fn handle_message(&self, topic: &str, data: &[u8]) -> Result<(), subscriber::UltraFastError> {
            // Simple test implementation
            assert!(!topic.is_empty());
            assert!(!data.is_empty());
            Ok(())
        }
    }
    
    let handler = TestHandler;
    
    // Test message handling
    let result = handler.handle_message("test-topic", b"test data");
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_ultra_fast_message_properties() {
    let original = UltraFastMessage {
        topic: "property-test".to_string(),
        data: b"test data".to_vec(),
        timestamp: 987654321,
        sequence: 100,
    };
    
    // Test that we can access all properties
    assert_eq!(original.topic, "property-test");
    assert_eq!(original.data, b"test data");
    assert_eq!(original.timestamp, 987654321);
    assert_eq!(original.sequence, 100);
    
    // Test helper methods
    assert_eq!(original.get_topic(), "property-test");
    assert_eq!(original.get_data(), b"test data");
    assert_eq!(original.get_timestamp(), 987654321);
    assert_eq!(original.get_sequence(), 100);
}

#[tokio::test]
async fn test_connection_config_edge_cases() {
    // Test with minimal values
    let minimal_config = ConnectionConfig {
        address: "localhost".to_string(),
        port: 1,
        tcp_nodelay: false,
        receive_buffer_size: 1,
        connection_timeout: Duration::from_millis(1),
        keepalive: false,
    };
    
    assert_eq!(minimal_config.address, "localhost");
    assert_eq!(minimal_config.port, 1);
    assert_eq!(minimal_config.receive_buffer_size, 1);
    
    // Test with maximum reasonable values
    let max_config = ConnectionConfig {
        address: "very-long-hostname.example.com".to_string(),
        port: 65535,
        tcp_nodelay: true,
        receive_buffer_size: 16 * 1024 * 1024, // 16MB
        connection_timeout: Duration::from_secs(3600),
        keepalive: true,
    };
    
    assert_eq!(max_config.port, 65535);
    assert_eq!(max_config.receive_buffer_size, 16 * 1024 * 1024);
}

#[tokio::test]
async fn test_performance_under_load() {
    let stats = PerformanceStats::new();
    let start = std::time::Instant::now();
    
    // Simulate high-load scenario
    let num_operations = 10000;
    for i in 0..num_operations {
        stats.record_latency(((i % 1000) + 1) as u64);
    }
    
    let duration = start.elapsed();
    
    let (count, _, _, _) = stats.get_stats();
    assert_eq!(count, num_operations as u64);
    
    // Performance assertion - should complete in reasonable time
    assert!(duration.as_millis() < 100, "Operations took too long: {:?}", duration);
    
    println!("Completed {} operations in {:?}", num_operations, duration);
}

#[tokio::test]
async fn test_memory_usage_patterns() {
    // Test that creating many messages doesn't cause memory issues
    let mut messages = Vec::new();
    
    for i in 0..1000 {
        let message = UltraFastMessage {
            topic: format!("topic_{}", i % 10),
            data: vec![i as u8; (i % 100) + 1],
            timestamp: i as u64,
            sequence: i as u64,
        };
        messages.push(message);
    }
    
    assert_eq!(messages.len(), 1000);
    
    // Verify some random messages
    assert_eq!(messages[0].topic, "topic_0");
    assert_eq!(messages[500].topic, "topic_0"); // 500 % 10 = 0
    assert_eq!(messages[999].sequence, 999);
}
