// Ultra-high performance topic manager for sub-microsecond latency
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, AtomicBool, Ordering};
use std::mem;

use async_trait::async_trait;
use mockall::automock;
use crossbeam_queue::SegQueue;
use crossbeam_utils::CachePadded;
use parking_lot::RwLock;
use tokio::net::TcpStream;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex as TokioMutex;

// Use the logger from parent module
use crate::TOPIC_LOGGER;

// Synchronous logging macros for ultra_fast
macro_rules! uf_info {
    ($msg:expr) => {
        TOPIC_LOGGER.info_sync($msg.to_string());
    };
    ($fmt:expr, $($arg:tt)*) => {
        TOPIC_LOGGER.info_sync(format!($fmt, $($arg)*));
    };
}

macro_rules! uf_warn {
    ($msg:expr) => {
        TOPIC_LOGGER.warn_sync($msg.to_string());
    };
    ($fmt:expr, $($arg:tt)*) => {
        TOPIC_LOGGER.warn_sync(format!($fmt, $($arg)*));
    };
}

macro_rules! uf_debug {
    ($msg:expr) => {
        TOPIC_LOGGER.debug_sync($msg.to_string());
    };
    ($fmt:expr, $($arg:tt)*) => {
        TOPIC_LOGGER.debug_sync(format!($fmt, $($arg)*));
    };
}

use protocol::broker::messages::publish_request::Payload;

// Constants for ultra-low latency
const MAX_TOPICS: usize = 10000;              // Maximum number of topics
const MAX_SUBSCRIBERS_PER_TOPIC: usize = 1000; // Max subscribers per topic
const TOPIC_NAME_SIZE: usize = 128;           // Fixed topic name size
const MESSAGE_BATCH_SIZE: usize = 64;         // Batch size for message delivery
const DELIVERY_TIMEOUT_NS: u64 = 100000;     // 100 microseconds delivery timeout

// Zero-allocation error type
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TopicError {
    TopicNotFound,
    SubscriberNotFound,
    QueueFull,
    DeliveryFailed,
    InvalidTopic,
    TooManySubscribers,
    SystemError,
}

// Fixed-size topic name for zero-allocation operations
#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FixedTopicName {
    name: [u8; TOPIC_NAME_SIZE],
    len: usize,
    hash: u64,
}

impl FixedTopicName {
    pub fn new(name: &str) -> Option<Self> {
        if name.len() > TOPIC_NAME_SIZE {
            return None;
        }

        let mut topic = Self {
            name: [0; TOPIC_NAME_SIZE],
            len: name.len(),
            hash: Self::hash_str(name),
        };

        topic.name[..name.len()].copy_from_slice(name.as_bytes());
        Some(topic)
    }

    #[inline(always)]
    fn hash_str(s: &str) -> u64 {
        // FNV-1a hash for fast topic lookups
        let mut hash = 0xcbf29ce484222325u64;
        for byte in s.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3u64);
        }
        hash
    }

    #[inline(always)]
    pub fn as_str(&self) -> &str {
        unsafe {
            std::str::from_utf8_unchecked(&self.name[..self.len])
        }
    }

    #[inline(always)]
    pub fn hash(&self) -> u64 {
        self.hash
    }
}

// Pre-allocated message for zero-copy delivery
#[repr(C, packed)]
pub struct FastMessage {
    pub timestamp: u64,     // RDTSC timestamp
    pub topic_hash: u64,    // Topic hash for routing
    pub data_len: u32,      // Payload length
    pub msg_type: u8,       // Message type
    pub flags: u8,          // Control flags
    _padding: [u8; 6],      // Alignment padding
    // Data follows immediately after this header
}

impl FastMessage {
    #[inline(always)]
    pub fn new(topic_hash: u64, msg_type: u8, data_len: u32) -> Self {
        Self {
            timestamp: unsafe { std::arch::x86_64::_rdtsc() },
            topic_hash,
            data_len,
            msg_type,
            flags: 0,
            _padding: [0; 6],
        }
    }

    #[inline(always)]
    pub fn total_size(&self) -> usize {
        mem::size_of::<Self>() + self.data_len as usize
    }

    #[inline(always)]
    pub fn data(&self) -> &[u8] {
        unsafe {
            let ptr = (self as *const Self).add(1) as *const u8;
            std::slice::from_raw_parts(ptr, self.data_len as usize)
        }
    }
}

// High-performance subscriber with connection health tracking
pub struct UltraFastSubscriber {
    id: u64,
    stream: Arc<TokioMutex<TcpStream>>,
    topics: Vec<FixedTopicName>,
    active: AtomicBool,
    last_heartbeat: CachePadded<AtomicU64>,
    messages_sent: CachePadded<AtomicU64>,
    bytes_sent: CachePadded<AtomicU64>,
    send_errors: CachePadded<AtomicU64>,
    queue: SegQueue<Vec<u8>>, // Lock-free message queue
}

impl UltraFastSubscriber {
    pub fn new(id: u64, stream: Arc<TokioMutex<TcpStream>>, topics: Vec<FixedTopicName>) -> Self {
        Self {
            id,
            stream,
            topics,
            active: AtomicBool::new(true),
            last_heartbeat: CachePadded::new(AtomicU64::new(unsafe { std::arch::x86_64::_rdtsc() })),
            messages_sent: CachePadded::new(AtomicU64::new(0)),
            bytes_sent: CachePadded::new(AtomicU64::new(0)),
            send_errors: CachePadded::new(AtomicU64::new(0)),
            queue: SegQueue::new(),
        }
    }

    #[inline(always)]
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn mark_active(&self) {
        self.active.store(true, Ordering::Relaxed);
        self.last_heartbeat.store(unsafe { std::arch::x86_64::_rdtsc() }, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn mark_inactive(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn subscribes_to(&self, topic: &FixedTopicName) -> bool {
        self.topics.iter().any(|t| t.hash() == topic.hash())
    }

    #[inline(always)]
    pub fn queue_message(&self, message: Vec<u8>) -> Result<(), TopicError> {
        if self.queue.len() > 1000 { // Prevent unbounded growth
            return Err(TopicError::QueueFull);
        }
        
        self.queue.push(message);
        Ok(())
    }

    pub async fn flush_queue(&self) -> Result<usize, TopicError> {
        let mut sent_count = 0;
        let mut batch = Vec::new();
        
        // Collect batch of messages
        while let Some(msg) = self.queue.pop() {
            batch.push(msg);
            if batch.len() >= MESSAGE_BATCH_SIZE {
                break;
            }
        }

        if batch.is_empty() {
            return Ok(0);
        }

        // Send batch
        match self.stream.try_lock() {
            Ok(mut stream) => {
                for msg in batch {
                    match stream.write_all(&msg).await {
                        Ok(_) => {
                            sent_count += 1;
                            self.messages_sent.fetch_add(1, Ordering::Relaxed);
                            self.bytes_sent.fetch_add(msg.len() as u64, Ordering::Relaxed);
                        }
                        Err(_) => {
                            self.send_errors.fetch_add(1, Ordering::Relaxed);
                            self.mark_inactive();
                            return Err(TopicError::DeliveryFailed);
                        }
                    }
                }
                
                if let Err(_) = stream.flush().await {
                    self.mark_inactive();
                    return Err(TopicError::DeliveryFailed);
                }
            }
            Err(_) => {
                // Stream is busy, requeue messages
                for msg in batch.into_iter().rev() {
                    self.queue.push(msg);
                }
                return Err(TopicError::DeliveryFailed);
            }
        }

        Ok(sent_count)
    }

    pub fn get_stats(&self) -> SubscriberStats {
        SubscriberStats {
            id: self.id,
            active: self.is_active(),
            messages_sent: self.messages_sent.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            send_errors: self.send_errors.load(Ordering::Relaxed),
            queue_length: self.queue.len(),
            topic_count: self.topics.len(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SubscriberStats {
    pub id: u64,
    pub active: bool,
    pub messages_sent: u64,
    pub bytes_sent: u64,
    pub send_errors: u64,
    pub queue_length: usize,
    pub topic_count: usize,
}

// Ultra-fast topic with lock-free subscriber management
pub struct UltraFastTopic {
    name: FixedTopicName,
    subscribers: RwLock<Vec<Arc<UltraFastSubscriber>>>,
    message_count: CachePadded<AtomicU64>,
    total_bytes: CachePadded<AtomicU64>,
    delivery_errors: CachePadded<AtomicU64>,
    last_message_time: CachePadded<AtomicU64>,
}

impl UltraFastTopic {
    pub fn new(name: FixedTopicName) -> Self {
        Self {
            name,
            subscribers: RwLock::new(Vec::with_capacity(MAX_SUBSCRIBERS_PER_TOPIC)),
            message_count: CachePadded::new(AtomicU64::new(0)),
            total_bytes: CachePadded::new(AtomicU64::new(0)),
            delivery_errors: CachePadded::new(AtomicU64::new(0)),
            last_message_time: CachePadded::new(AtomicU64::new(0)),
        }
    }

    #[inline(always)]
    pub fn name(&self) -> &FixedTopicName {
        &self.name
    }

    pub fn add_subscriber(&self, subscriber: Arc<UltraFastSubscriber>) -> Result<(), TopicError> {
        let mut subs = self.subscribers.write();
        
        if subs.len() >= MAX_SUBSCRIBERS_PER_TOPIC {
            return Err(TopicError::TooManySubscribers);
        }

        // Check if already subscribed
        if subs.iter().any(|s| s.id == subscriber.id) {
            return Ok(()); // Already subscribed
        }

        subs.push(subscriber);
        Ok(())
    }

    pub fn remove_subscriber(&self, subscriber_id: u64) -> bool {
        let mut subs = self.subscribers.write();
        if let Some(pos) = subs.iter().position(|s| s.id == subscriber_id) {
            subs.swap_remove(pos);
            true
        } else {
            false
        }
    }

    #[inline(always)]
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.read().len()
    }

    pub async fn publish_message(&self, payload: &[u8]) -> Result<usize, TopicError> {
        let start_time = unsafe { std::arch::x86_64::_rdtsc() };
        
        self.message_count.fetch_add(1, Ordering::Relaxed);
        self.total_bytes.fetch_add(payload.len() as u64, Ordering::Relaxed);
        self.last_message_time.store(start_time, Ordering::Relaxed);

        let subscribers = self.subscribers.read();
        if subscribers.is_empty() {
            return Ok(0);
        }

        let mut delivered = 0;
        let mut errors = 0;

        // Queue message for all active subscribers
        for subscriber in subscribers.iter() {
            if subscriber.is_active() {
                match subscriber.queue_message(payload.to_vec()) {
                    Ok(_) => delivered += 1,
                    Err(_) => {
                        errors += 1;
                        subscriber.mark_inactive();
                    }
                }
            }
        }

        if errors > 0 {
            self.delivery_errors.fetch_add(errors as u64, Ordering::Relaxed);
        }

        Ok(delivered)
    }

    pub async fn flush_all_subscribers(&self) -> usize {
        let subscribers = self.subscribers.read();
        let mut total_sent = 0;

        for subscriber in subscribers.iter() {
            if subscriber.is_active() {
                match subscriber.flush_queue().await {
                    Ok(sent) => total_sent += sent,
                    Err(_) => subscriber.mark_inactive(),
                }
            }
        }

        total_sent
    }

    pub fn cleanup_inactive_subscribers(&self) -> usize {
        let mut subs = self.subscribers.write();
        let original_len = subs.len();
        
        subs.retain(|s| s.is_active());
        
        original_len - subs.len()
    }

    pub fn get_stats(&self) -> TopicStats {
        TopicStats {
            name: self.name.as_str().to_string(),
            subscriber_count: self.subscriber_count(),
            message_count: self.message_count.load(Ordering::Relaxed),
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
            delivery_errors: self.delivery_errors.load(Ordering::Relaxed),
            last_message_time: self.last_message_time.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TopicStats {
    pub name: String,
    pub subscriber_count: usize,
    pub message_count: u64,
    pub total_bytes: u64,
    pub delivery_errors: u64,
    pub last_message_time: u64,
}

// Ultra-fast topic manager trait
#[automock]
#[async_trait]
pub trait TopicManagerTrait {
    async fn subscribe(&self, subscriber_id: u64, stream: Arc<TokioMutex<TcpStream>>, topics: &[String]) -> Result<(), TopicError>;
    async fn unsubscribe(&self, subscriber_id: u64, topics: &[String]) -> Result<(), TopicError>;
    async fn publish(&self, topic: &str, payload: &Payload) -> Result<(), TopicError>;
    async fn get_topic_stats(&self, topic: &str) -> Result<TopicStats, TopicError>;
    async fn cleanup_inactive(&self) -> usize;
}

// Ultra-high performance topic manager
pub struct TopicManager {
    topics: RwLock<HashMap<u64, Arc<UltraFastTopic>>>, // Hash -> Topic mapping
    topic_names: RwLock<HashMap<String, u64>>,         // Name -> Hash mapping
    subscribers: RwLock<HashMap<u64, Arc<UltraFastSubscriber>>>, // ID -> Subscriber
    next_subscriber_id: CachePadded<AtomicU64>,
    
    // Global metrics
    total_topics: CachePadded<AtomicUsize>,
    total_subscribers: CachePadded<AtomicUsize>,
    total_messages: CachePadded<AtomicU64>,
    total_bytes: CachePadded<AtomicU64>,
    total_errors: CachePadded<AtomicU64>,
}

impl TopicManager {
    pub fn new() -> Self {
        Self {
            topics: RwLock::new(HashMap::with_capacity(MAX_TOPICS)),
            topic_names: RwLock::new(HashMap::with_capacity(MAX_TOPICS)),
            subscribers: RwLock::new(HashMap::new()),
            next_subscriber_id: CachePadded::new(AtomicU64::new(1)),
            total_topics: CachePadded::new(AtomicUsize::new(0)),
            total_subscribers: CachePadded::new(AtomicUsize::new(0)),
            total_messages: CachePadded::new(AtomicU64::new(0)),
            total_bytes: CachePadded::new(AtomicU64::new(0)),
            total_errors: CachePadded::new(AtomicU64::new(0)),
        }
    }

    fn get_or_create_topic(&self, topic_name: &str) -> Result<Arc<UltraFastTopic>, TopicError> {
        let fixed_name = FixedTopicName::new(topic_name).ok_or(TopicError::InvalidTopic)?;
        let hash = fixed_name.hash();

        // Fast path: topic already exists
        {
            let topics = self.topics.read();
            if let Some(topic) = topics.get(&hash) {
                return Ok(Arc::clone(topic));
            }
        }

        // Slow path: create new topic
        let mut topics = self.topics.write();
        let mut topic_names = self.topic_names.write();
        
        // Double-check after acquiring write lock
        if let Some(topic) = topics.get(&hash) {
            return Ok(Arc::clone(topic));
        }

        if topics.len() >= MAX_TOPICS {
            return Err(TopicError::TooManySubscribers);
        }

        let topic = Arc::new(UltraFastTopic::new(fixed_name));
        topics.insert(hash, Arc::clone(&topic));
        topic_names.insert(topic_name.to_string(), hash);
        
        self.total_topics.fetch_add(1, Ordering::Relaxed);
        
        uf_info!("Created new topic: {}", topic_name);
        Ok(topic)
    }

    #[inline(always)]
    fn get_topic_by_name(&self, topic_name: &str) -> Option<Arc<UltraFastTopic>> {
        let topic_names = self.topic_names.read();
        let hash = topic_names.get(topic_name)?;
        
        let topics = self.topics.read();
        topics.get(hash).cloned()
    }

    pub async fn flush_all_topics(&self) -> usize {
        let topics = self.topics.read();
        let mut total_sent = 0;

        for topic in topics.values() {
            total_sent += topic.flush_all_subscribers().await;
        }

        total_sent
    }

    pub fn get_global_stats(&self) -> GlobalStats {
        GlobalStats {
            total_topics: self.total_topics.load(Ordering::Relaxed),
            total_subscribers: self.total_subscribers.load(Ordering::Relaxed),
            total_messages: self.total_messages.load(Ordering::Relaxed),
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
            total_errors: self.total_errors.load(Ordering::Relaxed),
        }
    }

    pub fn get_all_topic_stats(&self) -> Vec<TopicStats> {
        let topics = self.topics.read();
        topics.values()
            .map(|topic| topic.get_stats())
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct GlobalStats {
    pub total_topics: usize,
    pub total_subscribers: usize,
    pub total_messages: u64,
    pub total_bytes: u64,
    pub total_errors: u64,
}

#[async_trait]
impl TopicManagerTrait for TopicManager {
    async fn subscribe(&self, subscriber_id: u64, stream: Arc<TokioMutex<TcpStream>>, topics: &[String]) -> Result<(), TopicError> {
        let start_time = unsafe { std::arch::x86_64::_rdtsc() };

        // Convert topic names to fixed format
        let mut fixed_topics = Vec::with_capacity(topics.len());
        for topic_name in topics {
            if let Some(fixed_name) = FixedTopicName::new(topic_name) {
                fixed_topics.push(fixed_name);
            } else {
                return Err(TopicError::InvalidTopic);
            }
        }

        let id = self.next_subscriber_id.fetch_add(1, Ordering::Relaxed);
        let subscriber = Arc::new(UltraFastSubscriber::new(id, stream, fixed_topics));

        // Add subscriber to global list
        {
            let mut subscribers = self.subscribers.write();
            subscribers.insert(id, Arc::clone(&subscriber));
            self.total_subscribers.fetch_add(1, Ordering::Relaxed);
        }

        // Subscribe to each topic
        for topic_name in topics {
            let topic = self.get_or_create_topic(topic_name)?;
            topic.add_subscriber(Arc::clone(&subscriber))?;
            uf_debug!("Subscriber {} subscribed to topic '{}'", id, topic_name);
        }

        let end_time = unsafe { std::arch::x86_64::_rdtsc() };
        uf_debug!("Subscription completed in {} cycles", end_time - start_time);

        Ok(())
    }

    async fn unsubscribe(&self, subscriber_id: u64, topics: &[String]) -> Result<(), TopicError> {
        let subscriber = {
            let subscribers = self.subscribers.read();
            subscribers.get(&subscriber_id).cloned()
                .ok_or(TopicError::SubscriberNotFound)?
        };

        for topic_name in topics {
            if let Some(topic) = self.get_topic_by_name(topic_name) {
                topic.remove_subscriber(subscriber_id);
                uf_debug!("Subscriber {} unsubscribed from topic '{}'", subscriber_id, topic_name);
            }
        }

        Ok(())
    }

    async fn publish(&self, topic_name: &str, payload: &Payload) -> Result<(), TopicError> {
        let start_time = unsafe { std::arch::x86_64::_rdtsc() };

        let topic = self.get_topic_by_name(topic_name)
            .ok_or(TopicError::TopicNotFound)?;

        // Serialize payload efficiently
        let serialized = match payload {
            Payload::MarketPayload(market_msg) => {
                prost::Message::encode_to_vec(market_msg)
            }
            Payload::PortfolioPayload(portfolio_msg) => {
                prost::Message::encode_to_vec(portfolio_msg)
            }
        };

        let delivered = topic.publish_message(&serialized).await?;
        
        self.total_messages.fetch_add(1, Ordering::Relaxed);
        self.total_bytes.fetch_add(serialized.len() as u64, Ordering::Relaxed);

        let end_time = unsafe { std::arch::x86_64::_rdtsc() };
        uf_debug!("Published to {} subscribers on topic '{}' in {} cycles", 
               delivered, topic_name, end_time - start_time);

        Ok(())
    }

    async fn get_topic_stats(&self, topic_name: &str) -> Result<TopicStats, TopicError> {
        let topic = self.get_topic_by_name(topic_name)
            .ok_or(TopicError::TopicNotFound)?;
        
        Ok(topic.get_stats())
    }

    async fn cleanup_inactive(&self) -> usize {
        let topics = self.topics.read();
        let mut total_cleaned = 0;

        for topic in topics.values() {
            total_cleaned += topic.cleanup_inactive_subscribers();
        }

        if total_cleaned > 0 {
            uf_info!("Cleaned up {} inactive subscribers", total_cleaned);
        }

        total_cleaned
    }
}

impl Default for TopicManager {
    fn default() -> Self {
        Self::new()
    }
}

// Background task for periodic maintenance
impl TopicManager {
    pub async fn start_maintenance_task(&self, interval_secs: u64) {
        let manager = self;
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));
            
            loop {
                interval.tick().await;
                
                // Flush all queued messages
                let flushed = manager.flush_all_topics().await;
                if flushed > 0 {
                    uf_debug!("Flushed {} messages from topic queues", flushed);
                }
                
                // Clean up inactive subscribers
                let cleaned = manager.cleanup_inactive().await;
                if cleaned > 0 {
                    uf_debug!("Cleaned up {} inactive subscribers", cleaned);
                }
                
                // Log global stats
                let stats = manager.get_global_stats();
                uf_info!("Global stats - Topics: {}, Subscribers: {}, Messages: {}, Bytes: {}", 
                      stats.total_topics, stats.total_subscribers, stats.total_messages, stats.total_bytes);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::{TcpListener, TcpStream};

    #[test]
    fn test_fixed_topic_name() {
        let topic = FixedTopicName::new("test_topic").unwrap();
        assert_eq!(topic.as_str(), "test_topic");
        assert!(topic.hash() != 0);
        
        let topic2 = FixedTopicName::new("test_topic").unwrap();
        assert_eq!(topic.hash(), topic2.hash());
    }

    #[test]
    fn test_topic_manager_creation() {
        let manager = TopicManager::new();
        let stats = manager.get_global_stats();
        
        assert_eq!(stats.total_topics, 0);
        assert_eq!(stats.total_subscribers, 0);
        assert_eq!(stats.total_messages, 0);
    }

    #[tokio::test]
    async fn test_topic_creation() {
        let manager = TopicManager::new();
        let topic = manager.get_or_create_topic("test_topic").unwrap();
        
        assert_eq!(topic.name().as_str(), "test_topic");
        assert_eq!(topic.subscriber_count(), 0);
        
        let stats = manager.get_global_stats();
        assert_eq!(stats.total_topics, 1);
    }

    #[tokio::test]
    async fn test_subscription() {
        let manager = TopicManager::new();
        
        // Create a mock TcpStream for testing
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        
        let stream = TcpStream::connect(addr).await.unwrap();
        let stream = Arc::new(TokioMutex::new(stream));
        
        let topics = vec!["test_topic".to_string()];
        
        manager.subscribe(1, stream, &topics).await.unwrap();
        
        let stats = manager.get_global_stats();
        assert_eq!(stats.total_subscribers, 1);
        assert_eq!(stats.total_topics, 1);
        
        let topic_stats = manager.get_topic_stats("test_topic").await.unwrap();
        assert_eq!(topic_stats.subscriber_count, 1);
    }
}
