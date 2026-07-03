// Ultra-high performance subscriber for sub-microsecond latency

#[macro_use]
pub mod logging_facade;

use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::collections::HashMap;

use crossbeam::queue::SegQueue;
use parking_lot::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

/// Cross-platform timestamp function optimized for ultra-low latency
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn get_rdtsc() -> u64 {
    unsafe { std::arch::x86_64::_rdtsc() }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn get_rdtsc() -> u64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline(always)]
fn get_rdtsc() -> u64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

// Ultra-fast error types for zero-allocation error handling
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum UltraFastError {
    QueueFull,
    ConnectionFailed,
    DeserializationFailed,
    InvalidMessage,
    Timeout,
    SystemError,
    TopicNotFound,
    BufferTooSmall,
}

// Health metrics for subscriber monitoring
#[derive(Debug, Clone)]
pub struct HealthMetrics {
    connected: bool,
    error_count: u64,
}

impl HealthMetrics {
    pub fn get_connected(&self) -> bool {
        self.connected
    }
    
    pub fn get_error_count(&self) -> u64 {
        self.error_count
    }
}

// Performance statistics for monitoring ultra-low latency
#[derive(Default)]
pub struct PerformanceStats {
    messages_processed: AtomicU64,
    total_latency_ns: AtomicU64,
    min_latency_ns: AtomicU64,
    max_latency_ns: AtomicU64,
    last_reset: AtomicU64,
}

impl PerformanceStats {
    pub fn new() -> Self {
        Self {
            messages_processed: AtomicU64::new(0),
            total_latency_ns: AtomicU64::new(0),
            min_latency_ns: AtomicU64::new(u64::MAX),
            max_latency_ns: AtomicU64::new(0),
            last_reset: AtomicU64::new(get_rdtsc()),
        }
    }

    #[inline(always)]
    pub fn record_latency(&self, latency_ns: u64) {
        self.messages_processed.fetch_add(1, Ordering::Relaxed);
        self.total_latency_ns.fetch_add(latency_ns, Ordering::Relaxed);
        
        // Update min latency atomically
        let mut min = self.min_latency_ns.load(Ordering::Relaxed);
        while min > latency_ns {
            match self.min_latency_ns.compare_exchange_weak(
                min, latency_ns, Ordering::Relaxed, Ordering::Relaxed
            ) {
                Ok(_) => break,
                Err(x) => min = x,
            }
        }
        
        // Update max latency atomically
        let mut max = self.max_latency_ns.load(Ordering::Relaxed);
        while max < latency_ns {
            match self.max_latency_ns.compare_exchange_weak(
                max, latency_ns, Ordering::Relaxed, Ordering::Relaxed
            ) {
                Ok(_) => break,
                Err(x) => max = x,
            }
        }
    }

    pub fn get_stats(&self) -> (u64, f64, u64, u64) {
        let count = self.messages_processed.load(Ordering::Relaxed);
        let total = self.total_latency_ns.load(Ordering::Relaxed);
        let min = self.min_latency_ns.load(Ordering::Relaxed);
        let max = self.max_latency_ns.load(Ordering::Relaxed);
        
        let avg = if count > 0 { total as f64 / count as f64 } else { 0.0 };
        // Don't convert u64::MAX to 0 - tests expect the raw value
        
        (count, avg, min, max)
    }

    pub fn reset(&self) {
        self.messages_processed.store(0, Ordering::Relaxed);
        self.total_latency_ns.store(0, Ordering::Relaxed);
        self.min_latency_ns.store(u64::MAX, Ordering::Relaxed);
        self.max_latency_ns.store(0, Ordering::Relaxed);
        self.last_reset.store(get_rdtsc(), Ordering::Relaxed);
    }
}

// Message structure for ultra-fast processing
pub struct UltraFastMessage {
    pub topic: String,
    pub data: Vec<u8>,
    pub timestamp: u64,
    pub sequence: u64,
}

impl UltraFastMessage {
    pub fn new(topic: String, data: Vec<u8>, sequence: u64) -> Self {
        Self {
            topic,
            data,
            timestamp: get_rdtsc(),
            sequence,
        }
    }

    pub fn get_topic(&self) -> &str {
        &self.topic
    }

    pub fn get_data(&self) -> &[u8] {
        &self.data
    }

    pub fn get_timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn get_sequence(&self) -> u64 {
        self.sequence
    }
}

// Message handler trait for processing incoming messages
pub trait MessageHandler: Send + Sync {
    fn handle_message(&self, topic: &str, data: &[u8]) -> Result<(), UltraFastError>;
}

// Main ultra-high performance subscriber
pub struct UltraFastSubscriber {
    subscriber_id: u64,
    subscribed_topics: Arc<RwLock<HashMap<String, Arc<SegQueue<UltraFastMessage>>>>>,
    is_running: AtomicBool,
    performance_stats: Arc<PerformanceStats>,
    last_heartbeat: AtomicU64,
    messages_received: AtomicU64,
    reader: Arc<Mutex<Option<OwnedReadHalf>>>,
    writer: Arc<Mutex<Option<OwnedWriteHalf>>>,
    reader_started: Arc<AtomicBool>,
}

impl Clone for UltraFastSubscriber {
    fn clone(&self) -> Self {
        Self {
            subscriber_id: self.subscriber_id,
            subscribed_topics: Arc::clone(&self.subscribed_topics),
            is_running: AtomicBool::new(self.is_running.load(Ordering::Relaxed)),
            performance_stats: Arc::clone(&self.performance_stats),
            last_heartbeat: AtomicU64::new(self.last_heartbeat.load(Ordering::Relaxed)),
            messages_received: AtomicU64::new(self.messages_received.load(Ordering::Relaxed)),
            reader: Arc::clone(&self.reader),
            writer: Arc::clone(&self.writer),
            reader_started: Arc::clone(&self.reader_started),
        }
    }
}

impl UltraFastSubscriber {
    pub fn new(subscriber_id: u64) -> Self {
        Self {
            subscriber_id,
            subscribed_topics: Arc::new(RwLock::new(HashMap::new())),
            is_running: AtomicBool::new(false),
            performance_stats: Arc::new(PerformanceStats::new()),
            last_heartbeat: AtomicU64::new(get_rdtsc()),
            messages_received: AtomicU64::new(0),
            reader: Arc::new(Mutex::new(None)),
            writer: Arc::new(Mutex::new(None)),
            reader_started: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn subscribe_to_topic(&self, topic_name: &str) -> Result<(), UltraFastError> {
        {
            let mut topics = self.subscribed_topics.write();

            if !topics.contains_key(topic_name) {
                topics.insert(topic_name.to_string(), Arc::new(SegQueue::new()));
            }
        }

        // Ensure the TCP connection is ready before sending SUBSCRIBE frames.
        self.ensure_connection().await?;
        self.send_subscribe(topic_name).await?;
        self.wait_for_subscribe_ack(topic_name).await?;
        
        Ok(())
    }

    pub async fn unsubscribe_from_topic(&self, topic_name: &str) -> Result<(), UltraFastError> {
        let mut topics = self.subscribed_topics.write();
        topics.remove(topic_name);
        Ok(())
    }

    pub fn get_subscribed_topics(&self) -> Vec<String> {
        let topics = self.subscribed_topics.read();
        topics.keys().cloned().collect()
    }

    #[inline(always)]
    pub fn get_message_from_topic(&self, topic_name: &str) -> Option<UltraFastMessage> {
        let topics = self.subscribed_topics.read();
        if let Some(queue) = topics.get(topic_name) {
            if let Some(message) = queue.pop() {
                let latency = get_rdtsc().saturating_sub(message.timestamp);
                self.performance_stats.record_latency(latency);
                self.messages_received.fetch_add(1, Ordering::Relaxed);
                Some(message)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub async fn set_message_handler<H>(&self, _topic_name: &str, _handler: H) 
    where 
        H: MessageHandler + 'static
    {
        // Implementation for message handler would go here
        // This is a simplified version focusing on the core structure
    }

    #[inline(always)]
    pub fn record_message_received(&self, message_timestamp: u64) {
        let current_time = get_rdtsc();
        let latency = current_time.saturating_sub(message_timestamp);
        
        self.performance_stats.record_latency(latency);
        self.messages_received.fetch_add(1, Ordering::Relaxed);
        self.last_heartbeat.store(current_time, Ordering::Relaxed);
    }

    pub fn get_performance_stats(&self) -> (u64, f64, u64, u64) {
        self.performance_stats.get_stats()
    }

    pub fn reset_performance_stats(&self) {
        self.performance_stats.reset();
    }

    pub fn get_subscriber_id(&self) -> u64 {
        self.subscriber_id
    }

    pub fn get_messages_received(&self) -> u64 {
        self.messages_received.load(Ordering::Relaxed)
    }

    pub fn update_heartbeat(&self) {
        let current_time = get_rdtsc();
        self.last_heartbeat.store(current_time, Ordering::Relaxed);
    }

    pub fn get_last_heartbeat(&self) -> u64 {
        self.last_heartbeat.load(Ordering::Relaxed)
    }

    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }

    pub fn start(&self) {
        self.is_running.store(true, Ordering::Relaxed);

        if self
            .reader_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let reader = Arc::clone(&self.reader);
            let topics = Arc::clone(&self.subscribed_topics);
            let is_running = self.is_running.load(Ordering::Relaxed);
            let running = Arc::new(AtomicBool::new(is_running));
            running.store(true, Ordering::Relaxed);

            let stats = Arc::clone(&self.performance_stats);
            let heartbeat = Arc::new(AtomicU64::new(self.last_heartbeat.load(Ordering::Relaxed)));
            let msg_count = Arc::new(AtomicU64::new(self.messages_received.load(Ordering::Relaxed)));

            let writer = Arc::clone(&self.writer);
            tokio::spawn(async move {
                let mut reader_half = {
                    let mut guard = reader.lock().await;
                    match guard.take() {
                        Some(r) => r,
                        // Never connected (start() before any subscribe):
                        // fall through to the reconnect path below.
                        None => match Self::reconnect_and_resubscribe(&writer, &topics).await {
                            Some(r) => r,
                            None => return,
                        },
                    }
                };

                loop {
                    if !running.load(Ordering::Relaxed) {
                        break;
                    }

                    // On ANY read failure the broker connection is gone (broker
                    // restart, network partition). The old behavior was to
                    // `break` and exit this task silently — the subscriber then
                    // looked alive (get_message_from_topic polls empty queues)
                    // but was deaf FOREVER; a broker pod restart caused a
                    // multi-day silent market-data outage in production.
                    // Instead: reconnect with backoff and replay every
                    // SUBSCRIBE so the broker's fresh routing table knows us.
                    macro_rules! read_or_reconnect {
                        ($read:expr) => {
                            match $read {
                                Ok(v) => v,
                                Err(e) => {
                                    log_warn_sync!(
                                        &*logging_facade::SUBSCRIBER_LOGGER,
                                        "Broker connection lost ({e:?}) — reconnecting and re-subscribing"
                                    );
                                    match Self::reconnect_and_resubscribe(&writer, &topics).await {
                                        Some(r) => {
                                            reader_half = r;
                                            continue;
                                        }
                                        None => break,
                                    }
                                }
                            }
                        };
                    }

                    let first = read_or_reconnect!(reader_half.read_u8().await);

                    // Framing-sanity failures also force a reconnect: a bad
                    // length means the stream is desynced mid-frame, and no
                    // amount of further reading recovers alignment.
                    macro_rules! desync_reconnect {
                        ($why:expr) => {{
                            log_warn_sync!(
                                &*logging_facade::SUBSCRIBER_LOGGER,
                                "Broker stream desynced ({}) — reconnecting and re-subscribing",
                                $why
                            );
                            match Self::reconnect_and_resubscribe(&writer, &topics).await {
                                Some(r) => {
                                    reader_half = r;
                                    continue;
                                }
                                None => break,
                            }
                        }};
                    }

                    // SUBSCRIBE_ACK frame from broker: [0x03][topic_len:u32][topic:bytes]
                    if first == 0x03 {
                        let ack_len = read_or_reconnect!(reader_half.read_u32_le().await) as usize;
                        if ack_len > 1024 {
                            desync_reconnect!(format!("ack_len={}", ack_len));
                        }
                        let mut ack_topic = vec![0u8; ack_len];
                        read_or_reconnect!(reader_half.read_exact(&mut ack_topic).await);
                        continue;
                    }

                    // PUBLISH frame from broker: [topic_len:u32][topic][data_len:u32][data]
                    let mut len_rest = [0u8; 3];
                    read_or_reconnect!(reader_half.read_exact(&mut len_rest).await);

                    let topic_len = (first as u32)
                        | ((len_rest[0] as u32) << 8)
                        | ((len_rest[1] as u32) << 16)
                        | ((len_rest[2] as u32) << 24);
                    let topic_len = topic_len as usize;
                    if topic_len == 0 || topic_len > 1024 {
                        desync_reconnect!(format!("topic_len={}", topic_len));
                    }

                    let mut topic_buf = vec![0u8; topic_len];
                    read_or_reconnect!(reader_half.read_exact(&mut topic_buf).await);
                    let topic = String::from_utf8_lossy(&topic_buf).to_string();

                    let data_len = read_or_reconnect!(reader_half.read_u32_le().await) as usize;
                    if data_len > 16 * 1024 * 1024 {
                        desync_reconnect!(format!("data_len={}", data_len));
                    }

                    let mut data = vec![0u8; data_len];
                    read_or_reconnect!(reader_half.read_exact(&mut data).await);

                    let message = UltraFastMessage::new(topic.clone(), data, msg_count.load(Ordering::Relaxed) + 1);
                    if let Some(queue) = topics.read().get(&topic) {
                        queue.push(message);
                        msg_count.fetch_add(1, Ordering::Relaxed);
                        heartbeat.store(get_rdtsc(), Ordering::Relaxed);
                        stats.record_latency(0);
                    }
                }
            });
        }
    }

    pub fn stop(&self) {
        self.is_running.store(false, Ordering::Relaxed);
    }

    /// Re-establish the broker TCP connection and replay a SUBSCRIBE frame for
    /// every topic this subscriber holds, returning the fresh read half.
    ///
    /// Called from the reader task whenever the connection drops (broker pod
    /// restart, network partition) or the frame stream desyncs. The broker
    /// keeps its routing table in memory only, so after a broker restart every
    /// subscriber MUST re-send its SUBSCRIBEs or it receives nothing forever.
    /// Retries with exponential backoff (1s → 30s cap) until it succeeds; the
    /// reader task has no useful work to do without a connection.
    ///
    /// The new write half is installed into the shared writer slot while the
    /// SUBSCRIBE frames are sent under the same lock, so concurrent
    /// `subscribe_to_topic` calls cannot interleave partial frames.
    async fn reconnect_and_resubscribe(
        writer: &Arc<Mutex<Option<OwnedWriteHalf>>>,
        topics: &Arc<RwLock<HashMap<String, Arc<SegQueue<UltraFastMessage>>>>>,
    ) -> Option<OwnedReadHalf> {
        let mut delay = Duration::from_secs(1);
        loop {
            let address = broker_address_from_env();
            match TcpStream::connect(&address).await {
                Ok(stream) => {
                    let _ = stream.set_nodelay(true);
                    let (read_half, write_half) = stream.into_split();

                    let topic_names: Vec<String> = topics.read().keys().cloned().collect();
                    let mut guard = writer.lock().await;
                    *guard = Some(write_half);
                    let w = guard.as_mut().expect("writer just installed");

                    let mut ok = true;
                    for t in &topic_names {
                        let bytes = t.as_bytes();
                        if w.write_all(&[0x02]).await.is_err()
                            || w.write_u32_le(bytes.len() as u32).await.is_err()
                            || w.write_all(bytes).await.is_err()
                        {
                            ok = false;
                            break;
                        }
                    }
                    if ok && w.flush().await.is_err() {
                        ok = false;
                    }
                    drop(guard);

                    if ok {
                        log_info_sync!(
                            &*logging_facade::SUBSCRIBER_LOGGER,
                            "Reconnected to broker at {} and re-subscribed {} topic(s): {:?}",
                            address,
                            topic_names.len(),
                            topic_names
                        );
                        return Some(read_half);
                    }
                    // Writes failed on the fresh socket — treat as a failed
                    // attempt and back off.
                }
                Err(e) => {
                    log_warn_sync!(
                        &*logging_facade::SUBSCRIBER_LOGGER,
                        "Broker reconnect to {} failed ({e}); retrying in {:?}",
                        address,
                        delay
                    );
                }
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(30));
        }
    }

    async fn ensure_connection(&self) -> Result<(), UltraFastError> {
        {
            let writer_guard = self.writer.lock().await;
            if writer_guard.is_some() {
                return Ok(());
            }
        }

        let address = broker_address_from_env();
        let stream = TcpStream::connect(&address)
            .await
            .map_err(|_| UltraFastError::ConnectionFailed)?;
        let _ = stream.set_nodelay(true);

        let (read_half, write_half) = stream.into_split();
        {
            let mut writer_guard = self.writer.lock().await;
            *writer_guard = Some(write_half);
        }
        {
            let mut reader_guard = self.reader.lock().await;
            *reader_guard = Some(read_half);
        }

        Ok(())
    }

    async fn send_subscribe(&self, topic_name: &str) -> Result<(), UltraFastError> {
        let topic_bytes = topic_name.as_bytes();
        let mut writer_guard = self.writer.lock().await;
        let writer = writer_guard
            .as_mut()
            .ok_or(UltraFastError::ConnectionFailed)?;

        writer
            .write_all(&[0x02])
            .await
            .map_err(|_| UltraFastError::ConnectionFailed)?;
        writer
            .write_u32_le(topic_bytes.len() as u32)
            .await
            .map_err(|_| UltraFastError::ConnectionFailed)?;
        writer
            .write_all(topic_bytes)
            .await
            .map_err(|_| UltraFastError::ConnectionFailed)?;
        writer
            .flush()
            .await
            .map_err(|_| UltraFastError::ConnectionFailed)?;

        Ok(())
    }

    async fn wait_for_subscribe_ack(&self, _topic_name: &str) -> Result<(), UltraFastError> {
        let mut reader_guard = self.reader.lock().await;
        let reader = reader_guard
            .as_mut()
            .ok_or(UltraFastError::ConnectionFailed)?;

        let ack_type = reader
            .read_u8()
            .await
            .map_err(|_| UltraFastError::ConnectionFailed)?;
        if ack_type != 0x03 {
            return Err(UltraFastError::InvalidMessage);
        }

        let topic_len = reader
            .read_u32_le()
            .await
            .map_err(|_| UltraFastError::ConnectionFailed)? as usize;
        if topic_len > 1024 {
            return Err(UltraFastError::InvalidMessage);
        }

        let mut topic_buf = vec![0u8; topic_len];
        reader
            .read_exact(&mut topic_buf)
            .await
            .map_err(|_| UltraFastError::ConnectionFailed)?;

        Ok(())
    }
}

fn broker_address_from_env() -> String {
    if let Ok(url) = std::env::var("MESSAGE_BROKER_URL") {
        let normalized = url
            .trim()
            .trim_start_matches("tcp://")
            .trim_start_matches("http://")
            .trim_start_matches("https://")
            .to_string();
        if !normalized.is_empty() {
            return normalized;
        }
    }

    let host = std::env::var("MESSAGE_BROKER_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = std::env::var("MESSAGE_BROKER_PORT").unwrap_or_else(|_| "8080".to_string());
    format!("{}:{}", host, port)
}

impl Drop for UltraFastSubscriber {
    fn drop(&mut self) {
        self.stop();
    }
}

// Connection configuration for ultra-fast connections
#[derive(Clone)]
pub struct ConnectionConfig {
    pub address: String,
    pub port: u16,
    pub tcp_nodelay: bool,
    pub receive_buffer_size: usize,
    pub connection_timeout: Duration,
    pub keepalive: bool,
}

impl ConnectionConfig {
    pub fn new(address: &str) -> Self {
        let parts: Vec<&str> = address.split(':').collect();
        let (addr, port) = if parts.len() == 2 {
            (parts[0].to_string(), parts[1].parse().unwrap_or(8080))
        } else {
            (address.to_string(), 8080)
        };

        Self {
            address: addr,
            port,
            tcp_nodelay: true,
            receive_buffer_size: 65536,
            connection_timeout: Duration::from_secs(5),
            keepalive: true,
        }
    }

    pub fn with_tcp_nodelay(mut self, nodelay: bool) -> Self {
        self.tcp_nodelay = nodelay;
        self
    }

    pub fn with_receive_buffer_size(mut self, size: usize) -> Self {
        self.receive_buffer_size = size;
        self
    }

    pub fn with_connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }

    pub fn with_keepalive(mut self, keepalive: bool) -> Self {
        self.keepalive = keepalive;
        self
    }
}

// Subscriber with simplified interface for backward compatibility
#[derive(Clone)]
pub struct Subscriber {
    inner: UltraFastSubscriber,
    #[allow(dead_code)]
    config: ConnectionConfig,
    topics: Vec<String>,
}

impl Subscriber {
    pub fn new(config: ConnectionConfig, topics: &[&str]) -> Result<Self, UltraFastError> {
        let subscriber_id = get_rdtsc(); // Use timestamp as unique ID
        let inner = UltraFastSubscriber::new(subscriber_id);
        
        Ok(Self {
            inner,
            config,
            topics: topics.iter().map(|&s| s.to_string()).collect(),
        })
    }

    pub fn start(&mut self) -> Result<(), UltraFastError> {
        // Subscribe to all topics
        for _topic in &self.topics {
            // In a real implementation, this would establish network connections
            // and begin receiving messages from the broker
        }
        
        self.inner.start();
        Ok(())
    }

    pub fn find_topic_index(&self, topic: &str) -> Option<usize> {
        self.topics.iter().position(|t| t == topic)
    }

    pub fn get_message(&self, topic_idx: usize) -> Option<UltraFastMessage> {
        if let Some(topic) = self.topics.get(topic_idx) {
            self.inner.get_message_from_topic(topic)
        } else {
            None
        }
    }

    pub fn stop(&mut self) {
        self.inner.stop();
    }

    // Additional methods needed by portfoliohandler
    pub fn poll_all_messages(&self, max_messages: usize) -> Vec<(usize, UltraFastMessage)> {
        let mut messages = Vec::new();
        
        for (idx, topic) in self.topics.iter().enumerate() {
            if messages.len() >= max_messages {
                break;
            }
            
            if let Some(message) = self.inner.get_message_from_topic(topic) {
                messages.push((idx, message));
            }
        }
        
        messages
    }

    pub fn get_health_metrics(&self) -> Result<HealthMetrics, UltraFastError> {
        Ok(HealthMetrics {
            connected: self.inner.is_running(),
            error_count: 0, // Could track actual errors if implemented
        })
    }

    pub fn is_stale(&self, max_age_ms: u64) -> bool {
        let current_time = get_rdtsc();
        let last_heartbeat = self.inner.get_last_heartbeat();
        
        // Convert max_age_ms to nanoseconds for comparison
        let max_age_ns = max_age_ms * 1_000_000;
        
        current_time.saturating_sub(last_heartbeat) > max_age_ns
    }

    pub fn reconnect(&mut self) -> Result<(), UltraFastError> {
        // In a real implementation, this would reconnect to the broker
        self.inner.stop();
        self.inner.start();
        Ok(())
    }

    pub fn get_topic_name(&self, topic_idx: usize) -> Option<String> {
        self.topics.get(topic_idx).cloned()
    }
}
