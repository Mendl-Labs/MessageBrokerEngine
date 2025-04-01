use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mockall::automock;
use prost::bytes::BytesMut;
use prost::Message;
use protocol::broker::messages::publish_request::Payload;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

// Constants for optimized performance
const MAX_QUEUED_MESSAGES: usize = 10000;
const MAX_TOPIC_SUBSCRIBERS: usize = 1000;
const CHANNEL_CAPACITY: usize = 1024;
const FLUSH_INTERVAL_MS: u64 = 1;
const METRICS_INTERVAL_MS: u64 = 1000;
const CONNECTION_TIMEOUT_MS: u64 = 100;

type TokioMutex<T> = tokio::sync::Mutex<T>;

// Performance metrics
#[derive(Debug, Clone, Default)]
pub struct TopicMetrics {
    subscriber_count: usize,
    messages_published: u64,
    messages_delivered: u64,
    messages_dropped: u64,
    publish_errors: u64,
    avg_publish_latency_ns: u64,
    max_publish_latency_ns: u64,
}

// Message with metadata
#[derive(Debug)]
struct QueuedMessage {
    payload: Option<Payload>,
    timestamp: Instant,
    topic: String,
}

// Subscriber with health metrics
#[derive(Debug)]
struct Subscriber {
    stream: Arc<TokioMutex<TcpStream>>,
    active: AtomicBool,
    error_count: AtomicUsize,
    last_success: Arc<TokioMutex<Instant>>,  // Use TokioMutex consistently
}

impl Subscriber {
    fn new(stream: Arc<TokioMutex<TcpStream>>) -> Self {
        Self {
            stream,
            active: AtomicBool::new(true),
            error_count: AtomicUsize::new(0),
            last_success: Arc::new(TokioMutex::new(Instant::now())),
        }
    }

    fn mark_error(&self) {
        let count = self.error_count.fetch_add(1, Ordering::Relaxed);
        if count > 5 {
            self.active.store(false, Ordering::Relaxed);
        }
    }

    fn mark_success(&self) {
        self.error_count.store(0, Ordering::Relaxed);
        // Use tokio::spawn to avoid blocking
        let last_success = self.last_success.clone();
        tokio::spawn(async move {
            if let Ok(mut time) = last_success.try_lock() {
                *time = Instant::now();
            }
            // If we can't get the lock immediately, just skip updating
        });
    }

    fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    fn is_stale(&self, timeout: Duration) -> bool {
        match self.last_success.try_lock() {
            Ok(time) => time.elapsed() > timeout,
            // If we can't get the lock, assume it's not stale to avoid false positives
            Err(_) => false,
        }
    }
}

#[automock]
#[async_trait]
pub trait TopicManagerTrait {
    async fn subscribe<'a>(
        &'a self,
        topics: Vec<&'a str>,
        stream: Arc<TokioMutex<TcpStream>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;
    
    async fn publish<'a>(
        &'a self, 
        topics: Vec<&'a str>, 
        message: Option<Payload>
    ) -> Result<(), Box<dyn Error + Send + Sync>>;
    
    // New methods for HFT optimization
    fn get_metrics(&self, topic: &str) -> Option<TopicMetrics>;
    async fn prune_stale_subscribers(&self) -> usize;
}

// Message sender channel
type MessageSender = tokio::sync::mpsc::Sender<QueuedMessage>;
type MessageReceiver = tokio::sync::mpsc::Receiver<QueuedMessage>;

#[derive(Debug, Clone)]
pub struct TopicManager {
    // Use RwLock instead of Mutex for better read concurrency
    topics_subscribers: Arc<RwLock<HashMap<String, Vec<Arc<Subscriber>>>>>,
    topics_metrics: Arc<RwLock<HashMap<String, TopicMetrics>>>,
    message_sender: MessageSender,
    worker_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    running: Arc<AtomicBool>,
}

impl TopicManager {
    pub fn new() -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel(CHANNEL_CAPACITY);
        let topics_subscribers = Arc::new(RwLock::new(HashMap::new()));
        let topics_metrics = Arc::new(RwLock::new(HashMap::new()));
        let running = Arc::new(AtomicBool::new(true));
        let worker_handle = Arc::new(Mutex::new(None));
        
        // Start the background worker
        let handle = tokio::spawn(Self::message_processor(
            topics_subscribers.clone(),
            topics_metrics.clone(),
            receiver,
            running.clone(),
        ));
        
        // Store the handle in worker_handle
        {
            let worker_handle_clone = worker_handle.clone();
            tokio::spawn(async move {
                let mut h = worker_handle_clone.lock().await;
                *h = Some(handle);
            });
        }
        
        Self {
            topics_subscribers,
            topics_metrics,
            message_sender: sender,
            worker_handle,
            running,
        }
    }
    
    // Background task that processes messages
    async fn message_processor(
        topics_subscribers: Arc<RwLock<HashMap<String, Vec<Arc<Subscriber>>>>>,
        topics_metrics: Arc<RwLock<HashMap<String, TopicMetrics>>>,
        mut receiver: MessageReceiver,
        running: Arc<AtomicBool>,
    ) {
        let mut last_metrics_update = Instant::now();
        
        while running.load(Ordering::Relaxed) {
            // Process all available messages
            while let Ok(message) = receiver.try_recv() {
                let topic = message.topic.clone();
                let timestamp = message.timestamp;
                let payload = message.payload;
                
                // Get subscribers for this topic
                let subscribers = {
                    let subscriber_map = topics_subscribers.read().await;
                    match subscriber_map.get(&topic) {
                        Some(subs) => subs.iter()
                            .filter(|s| s.is_active())
                            .map(|s| Arc::clone(s))
                            .collect::<Vec<_>>(),
                        None => {
                            debug!("No subscribers for topic: {}", topic);
                            Vec::new()
                        }
                    }
                };
                
                if subscribers.is_empty() {
                    continue;
                }
                
                // Encode the payload once
                let buf = match &payload {
                    Some(p) => {
                        let mut buf = BytesMut::with_capacity(p.encoded_len());
                        p.encode(&mut buf);
                        buf
                    }
                    None => {
                        debug!("Empty payload for topic: {}", topic);
                        BytesMut::new()
                    }
                };
                
                if buf.is_empty() {
                    continue;
                }
                
                // Send to all subscribers concurrently
                let delivery_futures = subscribers.iter().map(|subscriber| {
                    let buf = buf.clone();
                    let subscriber = Arc::clone(subscriber);
                    async move {
                        let stream_result = subscriber.stream.try_lock();
                        match stream_result {
                            Ok(mut stream) => {
                                match stream.write_all(&buf).await {
                                    Ok(_) => {
                                        subscriber.mark_success();
                                        true
                                    }
                                    Err(e) => {
                                        subscriber.mark_error();
                                        warn!("Failed to write to stream: {}", e);
                                        false
                                    }
                                }
                            }
                            Err(_) => {
                                // Stream is locked, skip for now
                                false
                            }
                        }
                    }
                });
                
                // Wait for all deliveries to complete with timeout
                let results = futures::future::join_all(delivery_futures).await;
                
                // Update metrics
                let successful_deliveries = results.iter().filter(|&&r| r).count();
                let latency_ns = timestamp.elapsed().as_nanos() as u64;
                
                {
                    let mut metrics_map = topics_metrics.write().await;
                    let metrics = metrics_map.entry(topic.clone()).or_default();
                    metrics.messages_published += 1;
                    metrics.messages_delivered += successful_deliveries as u64;
                    metrics.messages_dropped += (subscribers.len() - successful_deliveries) as u64;
                    
                    // Update latency metrics
                    metrics.avg_publish_latency_ns = if metrics.avg_publish_latency_ns == 0 {
                        latency_ns
                    } else {
                        (metrics.avg_publish_latency_ns * 9 + latency_ns) / 10
                    };
                    metrics.max_publish_latency_ns = metrics.max_publish_latency_ns.max(latency_ns);
                }
            }
            
            // Update subscriber counts periodically
            if last_metrics_update.elapsed() > Duration::from_millis(METRICS_INTERVAL_MS) {
                let topics = {
                    let subscribers_map = topics_subscribers.read().await;
                    subscribers_map.keys().cloned().collect::<Vec<_>>()
                };
                
                for topic in topics {
                    let subscriber_count = {
                        let subscribers_map = topics_subscribers.read().await;
                        subscribers_map.get(&topic)
                            .map(|subs| subs.iter().filter(|s| s.is_active()).count())
                            .unwrap_or(0)
                    };
                    
                    {
                        let mut metrics_map = topics_metrics.write().await;
                        let metrics = metrics_map.entry(topic).or_default();
                        metrics.subscriber_count = subscriber_count;
                    }
                }
                
                last_metrics_update = Instant::now();
            }
            
            // Short sleep to avoid CPU spinning
            tokio::time::sleep(Duration::from_millis(FLUSH_INTERVAL_MS)).await;
        }
    }
    
    // Helper method to prune inactive subscribers from a topic
    async fn prune_topic_subscribers(&self, topic: &str) -> usize {
        let mut subscribers_map = self.topics_subscribers.write().await;
        
        if let Some(subscribers) = subscribers_map.get_mut(topic) {
            let original_count = subscribers.len();
            
            // Remove inactive subscribers
            subscribers.retain(|sub| sub.is_active());
            
            // Return number of pruned subscribers
            original_count - subscribers.len()
        } else {
            0
        }
    }
}

#[async_trait]
impl TopicManagerTrait for TopicManager {
    async fn subscribe<'a>(
        &'a self,
        topics: Vec<&'a str>,
        stream: Arc<Mutex<TcpStream>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if topics.is_empty() {
            error!("Topic is empty");
            return Err("Topic cannot be empty".into());
        }
        
        let subscriber = Arc::new(Subscriber::new(stream));
        
        // Acquire write lock
        let mut topics_map = self.topics_subscribers.write().await;
        
        for topic in topics {
            info!("Subscribe to topic: {}", topic);
            
            let topic_subscribers = topics_map
                .entry(topic.to_string())
                .or_insert_with(Vec::new);
                
            // Check if we're already at capacity
            if topic_subscribers.len() >= MAX_TOPIC_SUBSCRIBERS {
                // Find and remove inactive subscribers first
                topic_subscribers.retain(|sub| sub.is_active() && !sub.is_stale(Duration::from_millis(CONNECTION_TIMEOUT_MS)));
                
                // If still at capacity, reject the subscription
                if topic_subscribers.len() >= MAX_TOPIC_SUBSCRIBERS {
                    warn!("Too many subscribers for topic: {}", topic);
                    return Err(format!("Too many subscribers for topic: {}", topic).into());
                }
            }
            
            // Add the subscriber
            topic_subscribers.push(subscriber.clone());
            
            // Update metrics
            let mut metrics_map = self.topics_metrics.write().await;
            let metrics = metrics_map.entry(topic.to_string()).or_default();
            metrics.subscriber_count = topic_subscribers.len();
        }
        
        Ok(())
    }

    async fn publish<'a>(
        &'a self,
        topics: Vec<&'a str>,
        message: Option<Payload>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let now = Instant::now();
        
        // Check if any of the topics have subscribers before doing work
        {
            let subscribers_map = self.topics_subscribers.read().await;
            let has_subscribers = topics.iter().any(|&topic| {
                subscribers_map.get(topic)
                    .map(|subs| subs.iter().any(|s| s.is_active()))
                    .unwrap_or(false)
            });
            
            if !has_subscribers {
                debug!("No active subscribers for any of the topics");
                return Ok(());
            }
        }
        
        // Queue message for each topic
        for &topic in &topics {
            let queued_message = QueuedMessage {
                payload: message.clone(),
                timestamp: now,
                topic: topic.to_string(),
            };
            
            // Send to worker with timeout
            if let Err(e) = tokio::time::timeout(
                Duration::from_millis(CONNECTION_TIMEOUT_MS),
                self.message_sender.send(queued_message)
            ).await {
                error!("Failed to enqueue message: {}", e);
                
                // Update error metrics
                let mut metrics_map = self.topics_metrics.write().await;
                let metrics = metrics_map.entry(topic.to_string()).or_default();
                metrics.publish_errors += 1;
                
                return Err("Failed to enqueue message".into());
            }
        }
        
        Ok(())
    }
    
    fn get_metrics(&self, topic: &str) -> Option<TopicMetrics> {
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let metrics_map = self.topics_metrics.read().await;
                metrics_map.get(topic).cloned()
            })
        })
    }
    
    async fn prune_stale_subscribers(&self) -> usize {
        let topics = {
            let subscribers_map = self.topics_subscribers.read().await;
            subscribers_map.keys().cloned().collect::<Vec<_>>()
        };
        
        let mut total_pruned = 0;
        for topic in topics {
            total_pruned += self.prune_topic_subscribers(&topic).await;
        }
        
        total_pruned
    }
}

impl Drop for TopicManager {
    fn drop(&mut self) {
        // Signal the worker to shut down
        self.running.store(false, Ordering::Relaxed);
        
        // We can't await the handle here, but tokio will clean it up
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::broker::messages::{market_message::{self}, publish_request, MarketMessage, Order, Orders, PublishRequest, Trade, Trades};
    use tokio::net::TcpListener;
    use uuid::{self, Uuid};

    #[tokio::test]
    async fn test_topic_manager_new() {
        let manager = TopicManager::new();
        assert!(manager.topics_subscribers.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_subscribe_valid_topic_and_stream() {
        let mut mock = MockTopicManagerTrait::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let stream = TcpStream::connect(addr).await.unwrap();
        let stream = Arc::new(Mutex::new(stream));

        mock.expect_subscribe()
            .withf(|topic, _| topic == &vec!["test_topic"])
            .returning(|_, _| Ok(()));

        let result = mock.subscribe(vec!["test_topic"], stream).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_subscribe_empty_topic() {
        let mut mock = MockTopicManagerTrait::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let stream = TcpStream::connect(addr).await.unwrap();
        let stream = Arc::new(Mutex::new(stream));

        mock.expect_subscribe()
            .withf(|topic, _| topic.is_empty())
            .returning(|_, _| Err("Topic cannot be empty".into()));

        let result = mock.subscribe(vec![], stream).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_publish_valid_topic_and_message_order() {
        let mut mock = MockTopicManagerTrait::new();

        let topics = vec!["test_topic"];

        let big_decimal_price = 24.0;

        let bid_decimal_quantity = 10.0;

        let order = Order {
            unique_id: Uuid::new_v4().to_string(),
            symbol: "BTC/USD".to_string(),
            exchange: "binance".to_string(),
            price_level: big_decimal_price,
            quantity: bid_decimal_quantity,
            side: "BUY".to_string(),
            event: "NEW".to_string(),
        };

        let orders = Orders {
            orders: vec![order],
        };

        let market_message = MarketMessage {
            payload: Some(market_message::Payload::OrdersPaylaod(orders)),
        };

        let message = PublishRequest {
            topics: topics.iter().map(|&s| s.to_string()).collect(),
            payload: Some(publish_request::Payload::MarketPayload(market_message)),
        };

        // Clone the payload before passing it to mock.publish
        let payload_clone = message.payload.clone();

        mock.expect_publish()
            .withf(move |topics, message| {
                *topics == vec!["test_topic"] && message.is_some()
            })
            .returning(|_, _| Ok(()));

        let result = mock.publish(vec!["test_topic"], payload_clone).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_publish_valid_topic_and_message_trade() {
        let mut mock = MockTopicManagerTrait::new();

        let topics = vec!["test_topic"];

        let big_decimal_price = 24.0;

        let bid_decimal_quantity = 10.0;

        let trade = Trade {
            symbol: "BTC/USD".to_string(),
            exchange: "binance".to_string(),
            side: "BUY".to_string(),
            price: big_decimal_price,
            qty: bid_decimal_quantity,
            ord_type: "LIMIT".to_string(),
            trade_id: 1234567890,
            timestamp: "".to_string(),
        };

        let trades = Trades {
            trades: vec![trade],
        };

        let market_message = MarketMessage {
            payload: Some(market_message::Payload::TradesPayload(trades)),
        };

        let message = PublishRequest {
            topics: topics.iter().map(|&s| s.to_string()).collect(),
            payload: Some(publish_request::Payload::MarketPayload(market_message)),
        };

        // Clone the payload before passing it to mock.publish
        let payload_clone = message.payload.clone();

        mock.expect_publish()
            .withf(move |topics, message| {
                *topics == vec!["test_topic"] && message.is_some()
            })
            .returning(|_, _| Ok(()));

        let result = mock.publish(vec!["test_topic"], payload_clone).await;
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_metrics() {
        // Create manager and add metrics
        let manager = TopicManager::new();
        
        {
            let mut metrics_map = manager.topics_metrics.write().await;
            let metrics = metrics_map.entry("test_topic".to_string()).or_default();
            metrics.messages_published = 100;
            metrics.messages_delivered = 95;
            metrics.subscriber_count = 5;
        }
        
        // Test get_metrics
        let metrics = manager.get_metrics("test_topic");
        assert!(metrics.is_some());
        let metrics = metrics.unwrap();
        assert_eq!(metrics.messages_published, 100);
        assert_eq!(metrics.messages_delivered, 95);
        assert_eq!(metrics.subscriber_count, 5);
    }
    
    #[tokio::test]
    async fn test_prune_stale_subscribers() {
        // Create manager and add some subscribers
        let manager = TopicManager::new();
        
        // Create TCP streams for testing
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        
        // Add some subscribers
        let mut streams = Vec::new();
        for _ in 0..3 {
            let stream = TcpStream::connect(addr).await.unwrap();
            let stream = Arc::new(Mutex::new(stream));
            streams.push(stream.clone());
            
            manager.subscribe(vec!["test_topic"], stream).await.unwrap();
        }
        
        // Mark some subscribers as inactive
        {
            let subs_map = manager.topics_subscribers.read().await;
            let subs = subs_map.get("test_topic").unwrap();
            subs[0].active.store(false, Ordering::Relaxed);
            subs[1].active.store(false, Ordering::Relaxed);
        }
        
        // Prune inactive subscribers
        let pruned = manager.prune_stale_subscribers().await;
        assert_eq!(pruned, 2);
        
        // Check that only one subscriber remains
        let subs_map = manager.topics_subscribers.read().await;
        let subs = subs_map.get("test_topic").unwrap();
        assert_eq!(subs.len(), 1);
    }
}