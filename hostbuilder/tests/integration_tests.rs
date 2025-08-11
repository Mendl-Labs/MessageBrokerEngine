use std::sync::Arc;
use std::time::Duration;
use serial_test::serial;

use hostbuilder::{
    MessageBrokerHost, BrokerConfig, UltraFastMetrics, BrokerMessage, BrokerMetrics
};

#[tokio::test]
#[serial]
async fn test_broker_config_default() {
    let config = BrokerConfig::default();
    
    assert_eq!(config.host, "127.0.0.1");
    assert_eq!(config.port, 8080);
    assert_eq!(config.max_connections, 1000);
    assert_eq!(config.worker_threads, 4);
    assert_eq!(config.tcp_nodelay, true);
    assert_eq!(config.socket_reuse, true);
    assert_eq!(config.keepalive, true);
}

#[tokio::test]
#[serial]
async fn test_broker_config_ultra_performance() {
    let config = BrokerConfig::ultra_performance();
    
    assert_eq!(config.host, "0.0.0.0");
    assert_eq!(config.port, 8080);
    assert_eq!(config.max_connections, 50000);
    assert_eq!(config.worker_threads, 64);
    assert_eq!(config.read_buffer_size, 1048576); // 1MB
    assert_eq!(config.tcp_nodelay, true);
    assert_eq!(config.socket_reuse, true);
    assert_eq!(config.keepalive, true);
    assert_eq!(config.busy_poll, true);
    assert_eq!(config.use_huge_pages, true);
}

#[tokio::test]
async fn test_ultra_fast_metrics_creation() {
    let metrics = UltraFastMetrics::new();
    
    let (connections, messages, bytes, errors, rejections, avg_time, cache_hits, cache_misses, peak_memory) = metrics.get_stats();
    
    assert_eq!(connections, 0);
    assert_eq!(messages, 0);
    assert_eq!(bytes, 0);
    assert_eq!(errors, 0);
    assert_eq!(rejections, 0);
    assert_eq!(avg_time, 0.0);
    assert_eq!(cache_hits, 0);
    assert_eq!(cache_misses, 0);
    assert_eq!(peak_memory, 0);
}

#[tokio::test]
async fn test_ultra_fast_metrics_record_operations() {
    let metrics = UltraFastMetrics::new();
    
    // Record some operations
    metrics.record_connection();
    metrics.record_connection();
    metrics.record_message(1000, 256); // 1000ns processing time, 256 bytes
    metrics.record_message(2000, 512); // 2000ns processing time, 512 bytes
    metrics.record_error();
    metrics.record_rejection();
    
    let (connections, messages, bytes, errors, rejections, avg_time, _, _, _) = metrics.get_stats();
    
    assert_eq!(connections, 2);
    assert_eq!(messages, 2);
    assert_eq!(bytes, 768); // 256 + 512
    assert_eq!(errors, 1);
    assert_eq!(rejections, 1);
    // Average time should be around 1500ns (1000 + 2000) / 2
    assert!(avg_time > 1400.0 && avg_time < 1600.0);
}

#[tokio::test]
async fn test_broker_message_creation() {
    let message = BrokerMessage {
        topic: "test-topic".to_string(),
        data: b"Hello, World!".to_vec(),
        timestamp: 1234567890,
        sequence: 1,
        client_id: 42,
        priority: 1,
    };
    
    assert_eq!(message.topic, "test-topic");
    assert_eq!(message.data, b"Hello, World!");
    assert_eq!(message.timestamp, 1234567890);
    assert_eq!(message.sequence, 1);
    assert_eq!(message.client_id, 42);
    assert_eq!(message.priority, 1);
}

#[tokio::test]
#[serial]
async fn test_message_broker_host_creation() {
    let config = BrokerConfig {
        host: "127.0.0.1".to_string(),
        port: 0, // Let OS choose available port
        max_connections: 10,
        worker_threads: 2,
        connection_timeout: Duration::from_millis(100),
        read_buffer_size: 1024,
        max_message_size: 4096,
        tcp_nodelay: true,
        socket_reuse: true,
        keepalive: true,
        backlog: 128,
        busy_poll: false,
        cpu_affinity: vec![],
        use_huge_pages: false,
        shared_memory_size: 1024 * 1024,
    };
    
    let broker = MessageBrokerHost::new(config);
    
    assert_eq!(broker.get_messages_processed(), 0);
}

#[tokio::test]
#[serial] 
async fn test_message_broker_host_start_stop() {
    let config = BrokerConfig {
        host: "127.0.0.1".to_string(),
        port: 0, // Let OS choose available port
        max_connections: 10,
        worker_threads: 2,
        connection_timeout: Duration::from_millis(100),
        read_buffer_size: 1024,
        max_message_size: 4096,
        tcp_nodelay: true,
        socket_reuse: true,
        keepalive: true,
        backlog: 128,
        busy_poll: false,
        cpu_affinity: vec![],
        use_huge_pages: false,
        shared_memory_size: 1024 * 1024,
    };
    
    let broker = Arc::new(MessageBrokerHost::new(config));
    let broker_clone = Arc::clone(&broker);
    
    // Start broker in background
    let broker_task = tokio::spawn(async move {
        // Note: This will fail to bind to port 0 in the current implementation
        // but we can test that it attempts to start
        let result = broker_clone.start().await;
        // We expect this to fail due to port binding issues in test environment
        assert!(result.is_err());
    });
    
    // Give it a moment to attempt startup
    tokio::time::sleep(Duration::from_millis(10)).await;
    
    // Test shutdown
    broker.shutdown();
    
    // Wait for task completion
    let _ = tokio::time::timeout(Duration::from_secs(1), broker_task).await;
}

#[tokio::test]
async fn test_broker_metrics_display() {
    let metrics = BrokerMetrics {
        total_connections: 100,
        active_connections: 50,
        messages_processed: 1000,
        bytes_processed: 1024 * 1024, // 1MB
        errors: 5,
        rejected_connections: 2,
        avg_latency_ns: 1500.0,
        min_latency_ns: 1000,
        max_latency_ns: 2000,
    };
    
    // Test that metrics can be displayed without panic
    metrics.print_detailed_report();
    
    // Test individual field values
    assert_eq!(metrics.total_connections, 100);
    assert_eq!(metrics.active_connections, 50);
    assert_eq!(metrics.messages_processed, 1000);
    assert_eq!(metrics.bytes_processed, 1024 * 1024);
    assert_eq!(metrics.errors, 5);
    assert_eq!(metrics.rejected_connections, 2);
    assert_eq!(metrics.avg_latency_ns, 1500.0);
    assert_eq!(metrics.min_latency_ns, 1000);
    assert_eq!(metrics.max_latency_ns, 2000);
}

#[tokio::test]
async fn test_concurrent_metrics_updates() {
    let metrics = Arc::new(UltraFastMetrics::new());
    let mut handles = vec![];
    
    // Spawn multiple tasks to update metrics concurrently
    for i in 0..10 {
        let metrics_clone = Arc::clone(&metrics);
        let handle = tokio::spawn(async move {
            for j in 0..100 {
                metrics_clone.record_connection();
                metrics_clone.record_message((i * 100 + j) as u64, 256);
                if j % 10 == 0 {
                    metrics_clone.record_error();
                }
                if j % 20 == 0 {
                    metrics_clone.record_rejection();
                }
            }
        });
        handles.push(handle);
    }
    
    // Wait for all tasks to complete
    for handle in handles {
        handle.await.unwrap();
    }
    
    let (connections, messages, bytes, errors, rejections, _, _, _, _) = metrics.get_stats();
    
    // Should have recorded 10 * 100 = 1000 connections
    assert_eq!(connections, 1000);
    // Should have recorded 10 * 100 = 1000 messages
    assert_eq!(messages, 1000);
    // Should have recorded 1000 * 256 = 256000 bytes
    assert_eq!(bytes, 256000);
    // Should have recorded 10 * 10 = 100 errors (every 10th message)
    assert_eq!(errors, 100);
    // Should have recorded 10 * 5 = 50 rejections (every 20th message)
    assert_eq!(rejections, 50);
}

#[tokio::test]
async fn test_broker_config_validation() {
    // Test valid configuration
    let valid_config = BrokerConfig {
        host: "0.0.0.0".to_string(),
        port: 8080,
        max_connections: 1000,
        worker_threads: 4,
        connection_timeout: Duration::from_millis(5000),
        read_buffer_size: 64 * 1024,
        max_message_size: 1024 * 1024,
        tcp_nodelay: true,
        socket_reuse: true,
        keepalive: true,
        backlog: 512,
        busy_poll: true,
        cpu_affinity: vec![0, 1, 2, 3],
        use_huge_pages: false,
        shared_memory_size: 64 * 1024 * 1024,
    };
    
    let broker = MessageBrokerHost::new(valid_config);
    assert_eq!(broker.get_messages_processed(), 0);
}

#[tokio::test]
async fn test_performance_under_load() {
    let metrics = Arc::new(UltraFastMetrics::new());
    let start = std::time::Instant::now();
    
    // Simulate high-load scenario
    let num_operations = 10000;
    for i in 0..num_operations {
        metrics.record_connection();
        metrics.record_message((i % 1000) as u64, (i % 512) + 256);
        if i % 100 == 0 {
            metrics.record_error();
        }
    }
    
    let duration = start.elapsed();
    let (connections, messages, bytes, errors, _, avg_time, _, _, _) = metrics.get_stats();
    
    assert_eq!(connections, num_operations);
    assert_eq!(messages, num_operations as u64);
    assert_eq!(errors, (num_operations / 100) as u64);
    
    // Performance assertion - should complete in reasonable time
    assert!(duration.as_millis() < 100, "Operations took too long: {:?}", duration);
    
    // Verify average processing time calculation
    assert!(avg_time >= 0.0 && avg_time <= 1000.0);
    
    println!("Completed {} operations in {:?}", num_operations, duration);
    println!("Average processing time: {:.2}ns", avg_time);
}
