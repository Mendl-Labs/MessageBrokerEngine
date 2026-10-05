use std::time::Duration;
use std::sync::Arc;
use serial_test::serial;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

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
    
    // Test subscriber creation doesn't panic: reaching this point without panicking is the assertion.
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
    assert!(config.tcp_nodelay);
    assert_eq!(config.receive_buffer_size, 64 * 1024);
    assert_eq!(config.connection_timeout, Duration::from_secs(10));
    assert!(config.keepalive);
}

#[tokio::test]
async fn test_connection_config_new_method() {
    let config = ConnectionConfig::new("192.168.1.1:9000");
    
    assert_eq!(config.address, "192.168.1.1");
    assert_eq!(config.port, 9000);
    assert!(config.tcp_nodelay);
    assert_eq!(config.receive_buffer_size, 65536);
    assert_eq!(config.connection_timeout, Duration::from_secs(5));
    assert!(config.keepalive);
}

#[tokio::test]
async fn test_connection_config_default_port() {
    let config = ConnectionConfig::new("localhost");
    
    assert_eq!(config.address, "localhost");
    assert_eq!(config.port, 8080); // Default port
    assert!(config.tcp_nodelay);
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

// ---------------------------------------------------------------------------
// Regression coverage for the subscribe-after-start() fix (hardening pass,
// 2026-08-27). Every test above this point constructs values or checks stats
// -- none of them exercise a real TCP connection, so none of them could ever
// have caught this. The bug only manifests once a topic is subscribed AFTER
// start() has moved the read half into the background reader task.
// ---------------------------------------------------------------------------

/// Minimal fake broker speaking just enough of the real wire protocol for
/// these tests: acks every SUBSCRIBE frame ([0x02][topic_len:u32 LE][topic])
/// it receives with a SUBSCRIBE_ACK ([0x03][topic_len:u32 LE][topic]), and
/// exposes a channel the test can use to push an arbitrary raw frame (e.g. a
/// PUBLISH frame) to the connected client on demand. Reading and writing run
/// as two separate tasks over owned, independent halves so the ACKs the
/// reader produces and whatever the test pushes both funnel through the same
/// mpsc channel onto the one write half, instead of needing a shared lock or
/// `select!` (and the cancel-safety questions that would raise for
/// `read_u8`/`read_u32_le`/`read_exact`).
async fn spawn_fake_broker() -> (std::net::SocketAddr, tokio::sync::mpsc::Sender<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (frame_tx, mut frame_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(16);
    let ack_tx = frame_tx.clone();

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (mut read_half, mut write_half) = stream.into_split();

        let writer_task = tokio::spawn(async move {
            while let Some(frame) = frame_rx.recv().await {
                if write_half.write_all(&frame).await.is_err() {
                    break;
                }
            }
        });

        while let Ok(tag) = read_half.read_u8().await {
            if tag != 0x02 {
                break;
            }
            let Ok(topic_len) = read_half.read_u32_le().await else { break };
            let mut topic_buf = vec![0u8; topic_len as usize];
            if read_half.read_exact(&mut topic_buf).await.is_err() {
                break;
            }
            let mut ack = vec![0x03u8];
            ack.extend_from_slice(&topic_len.to_le_bytes());
            ack.extend_from_slice(&topic_buf);
            if ack_tx.send(ack).await.is_err() {
                break;
            }
        }
        let _ = writer_task.await;
    });

    (addr, frame_tx)
}

/// Encodes a raw PUBLISH frame the fake broker can push to the client:
/// [topic_len:u32 LE][topic][data_len:u32 LE][data] -- no leading type byte,
/// matching the real reader loop's framing (subscriber/src/lib.rs).
fn encode_publish_frame(topic: &str, data: &[u8]) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();
    let mut frame = Vec::new();
    frame.extend_from_slice(&(topic_bytes.len() as u32).to_le_bytes());
    frame.extend_from_slice(topic_bytes);
    frame.extend_from_slice(&(data.len() as u32).to_le_bytes());
    frame.extend_from_slice(data);
    frame
}

// Regression: subscribing to a NEW topic after start() had already been
// called failed unconditionally with ConnectionFailed -- start() moves the
// read half into its own background reader task, so a later
// subscribe_to_topic() had nothing left to read the SUBSCRIBE_ACK from
// directly. Exactly the "one long-lived subscriber, topics added over its
// lifetime" pattern a real caller (e.g. a per-run topic added as each new
// client connects) needs, and the one usage pattern with zero prior coverage.
#[tokio::test]
#[serial]
async fn subscribe_to_topic_after_start_succeeds_and_receives_messages() {
    let (addr, publish_tx) = spawn_fake_broker().await;
    std::env::set_var("MESSAGE_BROKER_URL", format!("{}", addr));

    let subscriber = UltraFastSubscriber::new(1);

    // Pre-start subscribe -- exercises the existing direct-read path, must
    // keep working exactly as before.
    subscriber.subscribe_to_topic("topic-a").await
        .expect("pre-start subscribe should succeed");

    subscriber.start();
    // Reader task startup is async; give it a moment to begin its loop.
    // subscribe_after_start itself doesn't depend on this (it only touches
    // the shared writer and the pending_acks map), but without this the
    // fake broker's ACK for topic-a has nowhere to land yet.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Post-start subscribe -- this exact call used to fail unconditionally.
    let result = subscriber.subscribe_to_topic("topic-b").await;
    assert_eq!(result, Ok(()), "post-start subscribe must succeed, not ConnectionFailed");

    // Confirm it's not just a fake success -- a real PUBLISH for the
    // post-start topic must actually be delivered end to end.
    publish_tx.send(encode_publish_frame("topic-b", b"hello")).await.unwrap();

    let mut delivered = None;
    for _ in 0..50 {
        if let Some(msg) = subscriber.get_message_from_topic("topic-b") {
            delivered = Some(msg);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let msg = delivered.expect("message published on the post-start topic was never delivered");
    assert_eq!(msg.get_data(), b"hello");

    subscriber.stop();
    std::env::remove_var("MESSAGE_BROKER_URL");
}

// A subscribe_to_topic() call that never gets acked (broker never responds)
// must fail after the bounded wait, not hang the caller forever.
#[tokio::test]
#[serial]
async fn subscribe_to_topic_after_start_times_out_when_broker_never_acks() {
    // A listener that accepts but never reads/writes anything -- the client
    // connects successfully, sends its SUBSCRIBE frame into the void, and
    // never gets an ack for it.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = listener.accept().await;
        // Hold the connection open, never respond.
        std::future::pending::<()>().await;
    });
    std::env::set_var("MESSAGE_BROKER_URL", format!("{}", addr));

    let subscriber = UltraFastSubscriber::new(2);
    // No pre-start subscribe this time -- start() with zero topics still
    // spawns the reader task (via its cold-start reconnect path).
    subscriber.start();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        subscriber.subscribe_to_topic("never-acked"),
    ).await.expect("subscribe_to_topic must return on its own bounded timeout, not hang");
    assert_eq!(result, Err(subscriber::UltraFastError::ConnectionFailed));

    subscriber.stop();
    std::env::remove_var("MESSAGE_BROKER_URL");
}
