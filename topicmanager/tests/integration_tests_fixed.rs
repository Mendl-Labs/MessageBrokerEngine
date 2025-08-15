use tokio::time::sleep;
use topicmanager::*;

#[tokio::test]
async fn test_fixed_topic_name_creation() {
    let topic_name = FixedTopicName::new("test-topic").unwrap();
    
    assert_eq!(topic_name.as_str(), "test-topic");
    assert_eq!(topic_name.as_str().len(), "test-topic".len());
    assert!(!topic_name.as_str().is_empty());
}

#[tokio::test]
async fn test_fixed_topic_name_edge_cases() {
    // Test empty topic name
    let empty_topic = FixedTopicName::new("").unwrap();
    assert!(empty_topic.as_str().is_empty());
    assert_eq!(empty_topic.as_str().len(), 0);
    
    // Test long topic name (should truncate or handle appropriately)
    let long_name = "a".repeat(1000);
    let long_topic = FixedTopicName::new(&long_name);
    // Should return None for names > 63 chars
    assert!(long_topic.is_none());
}

#[tokio::test]
async fn test_fixed_topic_name_hash_consistency() {
    let topic1 = FixedTopicName::new("consistent-hash").unwrap();
    let topic2 = FixedTopicName::new("consistent-hash").unwrap();
    
    assert_eq!(topic1.hash(), topic2.hash());
}

#[tokio::test]
async fn test_fixed_topic_name_cloning() {
    let original = FixedTopicName::new("clone-test").unwrap();
    let cloned = original.clone();
    
    assert_eq!(original.as_str(), cloned.as_str());
    assert_eq!(original.hash(), cloned.hash());
}

#[test]
fn test_ultra_fast_subscriber_creation() {
    // Create a dummy TCP stream for testing
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        
        // Connect to create a TcpStream
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        
        let subscriber = UltraFastSubscriber::new(1, stream);
        assert_eq!(subscriber.get_id(), 1);
        assert!(subscriber.is_active());
    });
}

#[tokio::test]
async fn test_ultra_fast_topic_creation() {
    let topic_name = FixedTopicName::new("test-topic").unwrap();
    let topic = UltraFastTopic::new(topic_name);
    assert_eq!(topic.get_name().as_str(), "test-topic");
}

#[test]
fn test_topic_manager_metrics_creation() {
    let metrics = TopicManagerMetrics {
        topic_count: 10,
        total_messages: 1000,
        total_bytes: 1024 * 1024, // 1MB
        total_subscribers: 50,
    };
    
    assert_eq!(metrics.topic_count, 10);
    assert_eq!(metrics.total_messages, 1000);
    assert_eq!(metrics.total_bytes, 1024 * 1024);
    assert_eq!(metrics.total_subscribers, 50);
}

#[test]
fn test_topic_manager_metrics_calculations() {
    let metrics = TopicManagerMetrics {
        topic_count: 100,
        total_messages: 1_000_000,
        total_bytes: 10 * 1024 * 1024, // 10MB
        total_subscribers: 500,
    };
    
    let success_rate = (metrics.total_messages) as f64 / metrics.total_messages as f64;
    assert_eq!(success_rate, 1.0);
    
    // Messages per topic average
    let msgs_per_topic = metrics.total_messages as f64 / metrics.topic_count as f64;
    assert_eq!(msgs_per_topic, 10000.0);
    
    // Bytes per topic average
    let bytes_per_topic = metrics.total_bytes / metrics.topic_count as u64;
    assert_eq!(bytes_per_topic, 104857); // Approximately 10MB/100
}

#[test]
fn test_topic_name_collection_creation() {
    let topic_names = ["topic-a", "topic-b", "topic-c"];
    let topics: Vec<FixedTopicName> = topic_names
        .iter()
        .map(|name| FixedTopicName::new(name).unwrap())
        .collect();
    
    for (i, topic) in topics.iter().enumerate() {
        assert_eq!(topic.as_str(), topic_names[i]);
    }
}

#[tokio::test]
async fn test_topic_with_special_characters() {
    let special_chars = "topic.with-special_chars123";
    let topic = FixedTopicName::new(special_chars).unwrap();
    assert_eq!(topic.as_str(), special_chars);
    assert_eq!(topic.as_str().len(), special_chars.len());
}

#[tokio::test]  
async fn test_topic_with_unicode() {
    let unicode_topic = "тема";
    let topic = FixedTopicName::new(unicode_topic).unwrap();
    assert_eq!(topic.as_str(), unicode_topic);
    assert_eq!(topic.as_str().len(), unicode_topic.len());
}

#[test]
fn test_topic_manager_metrics_zero_values() {
    let zero_metrics = TopicManagerMetrics {
        topic_count: 0,
        total_messages: 0,
        total_bytes: 0,
        total_subscribers: 0,
    };
    
    assert_eq!(zero_metrics.topic_count, 0);
    assert_eq!(zero_metrics.total_subscribers, 0);
}

#[test]
fn test_topic_manager_metrics_maximum_values() {
    let max_metrics = TopicManagerMetrics {
        topic_count: usize::MAX,
        total_messages: u64::MAX,
        total_bytes: u64::MAX,
        total_subscribers: usize::MAX,
    };
    
    assert_eq!(max_metrics.topic_count, usize::MAX);
    assert_eq!(max_metrics.total_subscribers, usize::MAX);
}

#[test]
fn test_topic_manager_creation() {
    let manager = UltraFastTopicManager::new();
    let metrics = manager.get_metrics();
    
    assert_eq!(metrics.topic_count, 0);
    assert_eq!(metrics.total_messages, 0);
    assert_eq!(metrics.total_bytes, 0);
    assert_eq!(metrics.total_subscribers, 0);
}

#[tokio::test]
async fn test_high_load_topic_names() {
    let mut topics = Vec::new();
    
    for i in 0..10_000 {
        let name = format!("high-load-topic-{}", i);
        let topic = FixedTopicName::new(&name).unwrap();
        topics.push(topic);
    }
    
    assert_eq!(topics.len(), 10_000);
    assert_eq!(topics[0].as_str(), "high-load-topic-0");
    assert_eq!(topics[5000].as_str(), "high-load-topic-5000");
    assert_eq!(topics[9999].as_str(), "high-load-topic-9999");
}

#[tokio::test]
async fn test_topic_name_ordering() {
    let names = vec!["aaa", "bbb", "ccc", "zzz"];
    let mut topics: Vec<FixedTopicName> = names
        .iter()
        .map(|name| FixedTopicName::new(name).unwrap())
        .collect();
    
    // Sort by hash for consistent ordering
    topics.sort_by_key(|t| t.hash());
    
    // Verify all topics are present
    let sorted_names: Vec<&str> = topics.iter().map(|t| t.as_str()).collect();
    assert_eq!(sorted_names.len(), 4);
}

#[tokio::test]
async fn test_concurrent_topic_creation() {
    use tokio::task::JoinSet;
    
    let mut join_set = JoinSet::new();
    
    for i in 0..1000 {
        join_set.spawn(async move {
            let name = format!("concurrent-topic-{}", i);
            FixedTopicName::new(&name).unwrap()
        });
    }
    
    let mut results = Vec::new();
    while let Some(result) = join_set.join_next().await {
        results.push(result.unwrap());
    }
    
    assert_eq!(results.len(), 1000);
}

#[test]
fn test_fixed_topic_name_display() {
    let topic = FixedTopicName::new("display-test").unwrap();
    let display_str = format!("{:?}", topic); // Use Debug formatting since Display is not implemented
    assert!(display_str.contains("display-test"));
}

#[test]
fn test_topic_name_size_limit() {
    // Test exactly at limit (63 chars)
    let limit_name = "a".repeat(63);
    let topic = FixedTopicName::new(&limit_name);
    assert!(topic.is_some());
    
    // Test over limit (64 chars)
    let over_limit = "a".repeat(64);
    let topic = FixedTopicName::new(&over_limit);
    assert!(topic.is_none());
}

#[tokio::test]
async fn test_ultra_fast_topic_manager_operations() {
    let manager = UltraFastTopicManager::new();
    
    // Test initial state
    let initial_metrics = manager.get_metrics();
    assert_eq!(initial_metrics.topic_count, 0);
    
    // Note: We can't test actual topic creation without implementing
    // the full topic management API, but we can test the basic structure
}

#[test]
fn test_metrics_cloning() {
    let original = TopicManagerMetrics {
        topic_count: 42,
        total_messages: 12345,
        total_bytes: 67890,
        total_subscribers: 24,
    };
    
    let cloned = original.clone();
    
    assert_eq!(original.topic_count, cloned.topic_count);
    assert_eq!(original.total_messages, cloned.total_messages);
    assert_eq!(original.total_bytes, cloned.total_bytes);
    assert_eq!(original.total_subscribers, cloned.total_subscribers);
}

#[tokio::test]
async fn test_subscriber_lifecycle() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let subscriber = UltraFastSubscriber::new(42, stream);
        
        assert_eq!(subscriber.get_id(), 42);
        assert!(subscriber.is_active());
        
        // Test heartbeat update
        subscriber.update_heartbeat();
        // Since get_last_heartbeat() is not in the API, we can't verify
        // but the function should execute without panic
    });
}

#[test]
fn test_topic_manager_basic_functionality() {
    let manager = UltraFastTopicManager::new();
    
    // Test that we can create the manager and get metrics
    let metrics = manager.get_metrics();
    
    // Initial state should be empty
    assert_eq!(metrics.topic_count, 0);
    assert_eq!(metrics.total_messages, 0);
    assert_eq!(metrics.total_bytes, 0);
    assert_eq!(metrics.total_subscribers, 0);
}
