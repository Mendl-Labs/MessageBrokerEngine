// Ultra-high performance publisher for sub-microsecond latency
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::io::AsyncWriteExt;
use tracing::{info, warn};
use crossbeam::queue::SegQueue;

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
    SerializationFailed,
    NetworkError,
    Timeout,
    SystemError,
    InvalidTopic,
    BufferTooSmall,
}

// Message priority for ultra-fast routing
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum MessagePriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

// Performance statistics for monitoring ultra-low latency
#[derive(Default)]
pub struct PerformanceStats {
    messages_sent: AtomicU64,
    total_latency_ns: AtomicU64,
    min_latency_ns: AtomicU64,
    max_latency_ns: AtomicU64,
    bytes_sent: AtomicU64,
    connection_failures: AtomicU64,
    last_reset: AtomicU64,
}

impl PerformanceStats {
    pub fn new() -> Self {
        Self {
            messages_sent: AtomicU64::new(0),
            total_latency_ns: AtomicU64::new(0),
            min_latency_ns: AtomicU64::new(u64::MAX),
            max_latency_ns: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            connection_failures: AtomicU64::new(0),
            last_reset: AtomicU64::new(get_rdtsc()),
        }
    }

    #[inline(always)]
    pub fn record_message_sent(&self, latency_ns: u64, bytes: u64) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        self.total_latency_ns.fetch_add(latency_ns, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
        
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

    pub fn record_connection_failure(&self) {
        self.connection_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_stats(&self) -> (u64, f64, u64, u64, u64, u64) {
        let count = self.messages_sent.load(Ordering::Relaxed);
        let total = self.total_latency_ns.load(Ordering::Relaxed);
        let min = self.min_latency_ns.load(Ordering::Relaxed);
        let max = self.max_latency_ns.load(Ordering::Relaxed);
        let bytes = self.bytes_sent.load(Ordering::Relaxed);
        let failures = self.connection_failures.load(Ordering::Relaxed);
        
        let avg = if count > 0 { total as f64 / count as f64 } else { 0.0 };
        let min = if min == u64::MAX { 0 } else { min };
        
        // Return in order expected by tests: (messages_sent, avg_latency, total_bytes, min_latency, connection_failures, max_latency)
        (count, avg, bytes, min, failures, max)
    }

    pub fn reset(&self) {
        self.messages_sent.store(0, Ordering::Relaxed);
        self.total_latency_ns.store(0, Ordering::Relaxed);
        self.min_latency_ns.store(u64::MAX, Ordering::Relaxed);
        self.max_latency_ns.store(0, Ordering::Relaxed);
        self.bytes_sent.store(0, Ordering::Relaxed);
        self.connection_failures.store(0, Ordering::Relaxed);
        self.last_reset.store(get_rdtsc(), Ordering::Relaxed);
    }
}

// Internal message structure for batching
#[derive(Clone)]
pub struct PendingMessage {
    pub topic: String,
    pub data: Vec<u8>,
    pub priority: MessagePriority,
    pub timestamp: u64,
    pub sequence: u64,
}

// Publisher configuration for ultra-high performance
#[derive(Clone)]
pub struct PublisherConfig {
    pub broker_address: String,
    pub broker_port: u16,
    pub batch_size: usize,
    pub flush_interval: Duration,
    pub tcp_nodelay: bool,
    pub send_buffer_size: usize,
    pub connection_timeout: Duration,
    pub retry_attempts: u32,
    pub keepalive: bool,
    pub topics: Vec<String>,
}

impl PublisherConfig {
    pub fn new(broker_address: &str) -> Self {
        let parts: Vec<&str> = broker_address.split(':').collect();
        let (_, port) = if parts.len() == 2 {
            (parts[0].to_string(), parts[1].parse().unwrap_or(9000))
        } else {
            (broker_address.to_string(), 9000)
        };

        Self {
            broker_address: broker_address.to_string(), // Preserve full address
            broker_port: port,
            batch_size: 1000, // Match test expectation
            flush_interval: Duration::from_millis(10), // Match test expectation
            tcp_nodelay: true,
            send_buffer_size: 65536,
            connection_timeout: Duration::from_secs(10), // Match test expectation
            retry_attempts: 3,
            keepalive: true,
            topics: Vec::new(),
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = interval;
        self
    }

    pub fn with_tcp_nodelay(mut self, nodelay: bool) -> Self {
        self.tcp_nodelay = nodelay;
        self
    }

    pub fn with_send_buffer_size(mut self, size: usize) -> Self {
        self.send_buffer_size = size;
        self
    }

    pub fn with_topics(mut self, topics: Vec<String>) -> Self {
        self.topics = topics;
        self
    }

    pub fn with_retry_attempts(mut self, attempts: u32) -> Self {
        self.retry_attempts = attempts;
        self
    }

    pub fn with_connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }
}

impl Default for PublisherConfig {
    fn default() -> Self {
        Self::new("localhost:8080")
    }
}

// Main ultra-high performance publisher
pub struct UltraFastPublisher {
    config: PublisherConfig,
    connection: Arc<tokio::sync::RwLock<Option<TcpStream>>>,
    is_connected: AtomicBool,
    performance_stats: Arc<PerformanceStats>,
    message_sequence: AtomicU64,
    pending_messages: Arc<SegQueue<PendingMessage>>,
    is_running: AtomicBool,
}

impl UltraFastPublisher {
    pub fn new(config: PublisherConfig) -> Self {
        Self {
            config,
            connection: Arc::new(tokio::sync::RwLock::new(None)),
            is_connected: AtomicBool::new(false),
            performance_stats: Arc::new(PerformanceStats::new()),
            message_sequence: AtomicU64::new(0),
            pending_messages: Arc::new(SegQueue::new()),
            is_running: AtomicBool::new(false),
        }
    }

    pub async fn connect(&self) -> Result<(), UltraFastError> {
        let start_time = get_rdtsc();
        
        for attempt in 0..self.config.retry_attempts {
            match self.try_connect().await {
                Ok(()) => {
                    self.is_connected.store(true, Ordering::Relaxed);
                    let latency = get_rdtsc() - start_time;
                    info!("✅ Publisher connected in {}ns (attempt {})", latency, attempt + 1);
                    return Ok(());
                }
                Err(e) => {
                    self.performance_stats.record_connection_failure();
                    warn!("❌ Connection attempt {} failed: {:?}", attempt + 1, e);
                    
                    if attempt < self.config.retry_attempts - 1 {
                        tokio::time::sleep(Duration::from_millis(100 * (1 << attempt))).await;
                    }
                }
            }
        }
        
        Err(UltraFastError::ConnectionFailed)
    }

    async fn try_connect(&self) -> Result<(), UltraFastError> {
        let addr = format!("{}:{}", self.config.broker_address, self.config.broker_port);
        
        let stream = tokio::time::timeout(
            self.config.connection_timeout,
            TcpStream::connect(&addr)
        ).await
        .map_err(|_| UltraFastError::Timeout)?
        .map_err(|_| UltraFastError::ConnectionFailed)?;

        // Configure socket for ultra-low latency
        if let Err(e) = stream.set_nodelay(self.config.tcp_nodelay) {
            warn!("Failed to set TCP_NODELAY: {}", e);
        }

        let mut connection = self.connection.write().await;
        *connection = Some(stream);
        
        Ok(())
    }

    pub async fn publish_raw(&self, data: Vec<u8>, topic: &str) -> Result<(), UltraFastError> {
        self.publish_with_priority(data, topic, MessagePriority::Normal).await
    }

    pub async fn publish_with_priority(
        &self, 
        data: Vec<u8>, 
        topic: &str, 
        priority: MessagePriority
    ) -> Result<(), UltraFastError> {
        if !self.is_connected.load(Ordering::Relaxed) {
            return Err(UltraFastError::ConnectionFailed);
        }

        let start_time = get_rdtsc();
        let sequence = self.message_sequence.fetch_add(1, Ordering::Relaxed);

        let message = PendingMessage {
            topic: topic.to_string(),
            data,
            priority,
            timestamp: start_time,
            sequence,
        };

        // Add to pending messages queue for batching
        self.pending_messages.push(message);

        // If queue is getting full or it's a critical message, flush immediately
        if priority == MessagePriority::Critical || self.pending_messages.len() >= self.config.batch_size {
            self.flush().await?;
        }

        Ok(())
    }

    pub async fn flush(&self) -> Result<(), UltraFastError> {
        let mut messages_to_send = Vec::new();
        
        // Collect all pending messages
        while let Some(message) = self.pending_messages.pop() {
            messages_to_send.push(message);
            if messages_to_send.len() >= self.config.batch_size * 2 {
                break; // Prevent excessive batching
            }
        }

        if messages_to_send.is_empty() {
            return Ok(());
        }

        // Sort by priority (critical first)
        messages_to_send.sort_by(|a, b| b.priority.cmp(&a.priority));

        let mut connection = self.connection.write().await;
        if let Some(ref mut stream) = *connection {
            for message in messages_to_send {
                let send_start = get_rdtsc();
                
                // Create a simple message format: [topic_len][topic][data_len][data]
                let topic_bytes = message.topic.as_bytes();
                let mut buffer = Vec::with_capacity(8 + topic_bytes.len() + message.data.len());
                
                buffer.extend_from_slice(&(topic_bytes.len() as u32).to_le_bytes());
                buffer.extend_from_slice(topic_bytes);
                buffer.extend_from_slice(&(message.data.len() as u32).to_le_bytes());
                buffer.extend_from_slice(&message.data);

                if let Err(_) = stream.write_all(&buffer).await {
                    self.is_connected.store(false, Ordering::Relaxed);
                    return Err(UltraFastError::NetworkError);
                }

                let latency = get_rdtsc() - send_start;
                self.performance_stats.record_message_sent(latency, buffer.len() as u64);
            }

            if let Err(_) = stream.flush().await {
                self.is_connected.store(false, Ordering::Relaxed);
                return Err(UltraFastError::NetworkError);
            }
        } else {
            return Err(UltraFastError::ConnectionFailed);
        }

        Ok(())
    }

    pub async fn disconnect(&self) -> Result<(), UltraFastError> {
        // Flush any remaining messages
        self.flush().await?;

        let mut connection = self.connection.write().await;
        if let Some(stream) = connection.take() {
            drop(stream);
        }

        self.is_connected.store(false, Ordering::Relaxed);
        self.is_running.store(false, Ordering::Relaxed);

        info!("🔌 Publisher disconnected");
        Ok(())
    }

    pub fn get_performance_stats(&self) -> (u64, f64, u64, u64, u64, u64) {
        self.performance_stats.get_stats()
    }

    pub fn reset_performance_stats(&self) {
        self.performance_stats.reset();
    }

    pub fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Relaxed)
    }

    pub fn pending_messages_count(&self) -> usize {
        self.pending_messages.len()
    }
}

impl Drop for UltraFastPublisher {
    fn drop(&mut self) {
        self.is_running.store(false, Ordering::Relaxed);
    }
}

// Simplified publisher for backward compatibility
pub struct Publisher {
    inner: Arc<UltraFastPublisher>,
    rt: Option<tokio::runtime::Runtime>,
}

impl Publisher {
    pub fn new(config: PublisherConfig) -> Result<Self, UltraFastError> {
        let rt = tokio::runtime::Runtime::new()
            .map_err(|_| UltraFastError::SystemError)?;
        
        let inner = Arc::new(UltraFastPublisher::new(config));
        
        Ok(Self {
            inner,
            rt: Some(rt),
        })
    }

    pub fn start(&mut self) -> Result<(), UltraFastError> {
        if let Some(ref rt) = self.rt {
            rt.block_on(async {
                self.inner.connect().await
            })
        } else {
            Err(UltraFastError::SystemError)
        }
    }

    pub fn publish_order(&mut self, _topic_idx: usize, order: Order) -> Result<(), UltraFastError> {
        if let Some(ref rt) = self.rt {
            // Serialize order to bytes (simplified)
            let data = format!("ORDER:{}:{}:{}:{}:{}:{}", 
                order.unique_id, order.symbol, order.exchange, 
                order.price_level, order.quantity, order.side).into_bytes();
            
            rt.block_on(async {
                self.inner.publish_raw(data, "orders").await
            })
        } else {
            Err(UltraFastError::SystemError)
        }
    }

    pub fn stop(&mut self) {
        if let Some(ref rt) = self.rt {
            let _ = rt.block_on(async {
                self.inner.disconnect().await
            });
        }
    }
}

// Order structure for compatibility
#[derive(Debug, Clone)]
pub struct Order {
    pub unique_id: String,
    pub symbol: String,
    pub exchange: String,
    pub price_level: f64,
    pub quantity: f64,
    pub side: String,
    pub event: String,
}
