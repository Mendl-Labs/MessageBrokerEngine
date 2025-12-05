// Performance optimizations for MessageBroker Publisher
// Implements recommendations from latency profiling analysis

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use parking_lot::RwLock as ParkingRwLock;
use tokio::sync::Notify;
use tokio::io::AsyncWriteExt;
use crossbeam::queue::SegQueue;

use crate::{
    UltraFastPublisher, UltraFastError, PublisherConfig, PendingMessage, 
    MessagePriority, PerformanceStats, get_rdtsc, logging_facade::PUBLISHER_LOGGER
};

/// Enhanced publisher with latency optimizations
pub struct OptimizedPublisher {
    inner: Arc<UltraFastPublisher>,
    background_flush_handle: Option<tokio::task::JoinHandle<()>>,
    flush_notify: Arc<Notify>,
    config: PublisherConfig,
    batch_buffer: Arc<ParkingRwLock<Vec<PendingMessage>>>,
    is_running: Arc<AtomicBool>,
    performance_stats: Arc<PerformanceStats>,
}

/// Batching configuration for optimal performance
#[derive(Clone)]
pub struct BatchingConfig {
    /// Maximum messages per batch
    pub max_batch_size: usize,
    /// Maximum time to wait before flushing (microseconds)
    pub max_flush_delay_us: u64,
    /// Enable adaptive batching based on load
    pub adaptive_batching: bool,
    /// Memory pool size for pre-allocated buffers
    pub memory_pool_size: usize,
    /// Enable priority-based batching
    pub priority_batching: bool,
}

impl Default for BatchingConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,         // Smaller batches for lower latency
            max_flush_delay_us: 50,      // 50μs max delay
            adaptive_batching: true,
            memory_pool_size: 1024,
            priority_batching: true,
        }
    }
}

/// High-performance batch processor
pub struct BatchProcessor {
    config: BatchingConfig,
    batch_buffer: Arc<ParkingRwLock<Vec<PendingMessage>>>,
    memory_pool: Arc<SegQueue<Vec<u8>>>,
    last_flush: Instant,
    messages_in_current_batch: usize,
}

impl BatchProcessor {
    pub fn new(config: BatchingConfig) -> Self {
        let memory_pool = Arc::new(SegQueue::new());
        
        // Pre-populate memory pool
        for _ in 0..config.memory_pool_size {
            memory_pool.push(Vec::with_capacity(8192)); // 8KB buffers
        }
        
        let batch_capacity = config.max_batch_size;
        
        Self {
            config,
            batch_buffer: Arc::new(ParkingRwLock::new(Vec::with_capacity(batch_capacity))),
            memory_pool,
            last_flush: Instant::now(),
            messages_in_current_batch: 0,
        }
    }
    
    /// Get a buffer from the memory pool (zero allocation)
    pub fn get_buffer(&self) -> Vec<u8> {
        self.memory_pool.pop().unwrap_or_else(|| Vec::with_capacity(8192))
    }
    
    /// Return a buffer to the memory pool
    pub fn return_buffer(&self, mut buffer: Vec<u8>) {
        buffer.clear();
        if buffer.capacity() >= 4096 && buffer.capacity() <= 16384 {
            self.memory_pool.push(buffer);
        }
    }
    
    /// Add message to batch and determine if flush is needed
    pub fn add_message(&mut self, message: PendingMessage) -> bool {
        let should_flush = self.should_flush_batch(&message);
        
        {
            let mut batch = self.batch_buffer.write();
            batch.push(message);
            self.messages_in_current_batch = batch.len();
        }
        
        should_flush
    }
    
    /// Determine if batch should be flushed based on size, time, and priority
    fn should_flush_batch(&self, message: &PendingMessage) -> bool {
        // Critical messages always flush immediately
        if message.priority == MessagePriority::Critical {
            return true;
        }
        
        // Size-based flushing
        if self.messages_in_current_batch >= self.config.max_batch_size {
            return true;
        }
        
        // Time-based flushing
        if self.last_flush.elapsed().as_micros() as u64 >= self.config.max_flush_delay_us {
            return true;
        }
        
        // Adaptive batching - flush more aggressively under high load
        if self.config.adaptive_batching && message.priority == MessagePriority::High {
            return self.messages_in_current_batch >= self.config.max_batch_size / 2;
        }
        
        false
    }
    
    /// Extract messages for flushing
    pub fn extract_batch(&mut self) -> Vec<PendingMessage> {
        let mut batch = self.batch_buffer.write();
        let messages = std::mem::replace(&mut *batch, Vec::with_capacity(self.config.max_batch_size));
        self.messages_in_current_batch = 0;
        self.last_flush = Instant::now();
        messages
    }
}

impl OptimizedPublisher {
    pub fn new(config: PublisherConfig, batching_config: BatchingConfig) -> Self {
        let inner = Arc::new(UltraFastPublisher::new(config.clone()));
        let batch_buffer = Arc::new(ParkingRwLock::new(Vec::with_capacity(batching_config.max_batch_size)));
        let performance_stats = Arc::new(PerformanceStats::new());
        
        Self {
            inner,
            background_flush_handle: None,
            flush_notify: Arc::new(Notify::new()),
            config,
            batch_buffer,
            is_running: Arc::new(AtomicBool::new(false)),
            performance_stats,
        }
    }
    
    /// Start the publisher with background processing
    pub async fn start(&mut self) -> Result<(), UltraFastError> {
        // Connect the inner publisher
        self.inner.connect().await?;
        
        // Mark as running
        self.is_running.store(true, Ordering::Relaxed);
        
        // Start background flush task
        self.start_background_flush_task().await;
        
        log_info!(PUBLISHER_LOGGER, "🚀 OptimizedPublisher started with background flushing");
        Ok(())
    }
    
    /// Start background task for time-based flushing
    async fn start_background_flush_task(&mut self) {
        let inner = Arc::clone(&self.inner);
        let batch_buffer = Arc::clone(&self.batch_buffer);
        let flush_notify: Arc<Notify> = Arc::clone(&self.flush_notify);
        let is_running = Arc::clone(&self.is_running);
        let stats = Arc::clone(&self.performance_stats);
        let flush_interval = self.config.flush_interval;
        
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(flush_interval);
            
            while is_running.load(Ordering::Relaxed) {
                tokio::select! {
                    _ = interval.tick() => {
                        // Time-based flush
                        if let Err(e) = Self::flush_batch(&inner, &batch_buffer, &stats).await {
                            log_error!(PUBLISHER_LOGGER, "Background flush failed: {:?}", e);
                        }
                    }
                    _ = flush_notify.notified() => {
                        // Event-based flush (for critical messages)
                        if let Err(e) = Self::flush_batch(&inner, &batch_buffer, &stats).await {
                            log_error!(PUBLISHER_LOGGER, "Event-triggered flush failed: {:?}", e);
                        }
                    }
                }
            }
            
            log_debug!(PUBLISHER_LOGGER, "Background flush task ended");
        });
        
        self.background_flush_handle = Some(handle);
    }
    
    /// Optimized publish with zero-copy batching
    pub async fn publish_optimized(&self, data: Vec<u8>, topic: &str, priority: MessagePriority) -> Result<(), UltraFastError> {
        if !self.inner.is_connected() {
            return Err(UltraFastError::ConnectionFailed);
        }
        
        let start_time = get_rdtsc();
        let sequence = self.inner.message_sequence.fetch_add(1, Ordering::Relaxed);
        
        let message = PendingMessage {
            topic: topic.to_string(),
            data,
            priority,
            timestamp: start_time,
            sequence,
        };
        
        // Add to batch buffer
        let should_flush = {
            let mut batch = self.batch_buffer.write();
            batch.push(message);
            
            // Determine if immediate flush is needed
            priority == MessagePriority::Critical || 
            batch.len() >= self.config.batch_size
        };
        
        // Immediate flush for critical messages or full batches
        if should_flush {
            self.flush_notify.notify_one();
            if priority == MessagePriority::Critical {
                // Wait for flush to complete for critical messages
                Self::flush_batch(&self.inner, &self.batch_buffer, &self.performance_stats).await?;
            }
        }
        
        Ok(())
    }
    
    /// High-performance batch flushing
    async fn flush_batch(
        inner: &Arc<UltraFastPublisher>,
        batch_buffer: &Arc<ParkingRwLock<Vec<PendingMessage>>>,
        stats: &Arc<PerformanceStats>
    ) -> Result<(), UltraFastError> {
        let messages_to_send = {
            let mut batch = batch_buffer.write();
            if batch.is_empty() {
                return Ok(());
            }
            let capacity = batch.capacity();
            std::mem::replace(&mut *batch, Vec::with_capacity(capacity))
        };
        
        if messages_to_send.is_empty() {
            return Ok(());
        }
        
        let batch_start = get_rdtsc();
        
        // Sort by priority for optimal processing order
        let mut sorted_messages = messages_to_send;
        sorted_messages.sort_by(|a, b| b.priority.cmp(&a.priority));
        
        let total_bytes;
        
        let mut connection = inner.connection.write().await;
        if let Some(ref mut stream) = *connection {
            // Pre-allocate buffer for entire batch to minimize allocations
            let estimated_size: usize = sorted_messages.iter()
                .map(|m| 8 + m.topic.len() + m.data.len())
                .sum();
            let mut batch_buffer = Vec::with_capacity(estimated_size);
            
            // Serialize entire batch into single buffer
            for message in &sorted_messages {
                let topic_bytes = message.topic.as_bytes();
                
                batch_buffer.extend_from_slice(&(topic_bytes.len() as u32).to_le_bytes());
                batch_buffer.extend_from_slice(topic_bytes);
                batch_buffer.extend_from_slice(&(message.data.len() as u32).to_le_bytes());
                batch_buffer.extend_from_slice(&message.data);
            }
            
            // Single write call for entire batch
            if let Err(_) = stream.write_all(&batch_buffer).await {
                inner.is_connected.store(false, Ordering::Relaxed);
                return Err(UltraFastError::NetworkError);
            }
            
            if let Err(_) = stream.flush().await {
                inner.is_connected.store(false, Ordering::Relaxed);
                return Err(UltraFastError::NetworkError);
            }
            
            total_bytes = batch_buffer.len() as u64;
        } else {
            return Err(UltraFastError::ConnectionFailed);
        }
        
        let batch_latency = get_rdtsc() - batch_start;
        let avg_message_latency = batch_latency / sorted_messages.len() as u64;
        
        // Record statistics for all messages in batch
        for _ in &sorted_messages {
            stats.record_message_sent(avg_message_latency, total_bytes / sorted_messages.len() as u64);
        }
        
        log_debug!(PUBLISHER_LOGGER, "Batch flushed: {} messages, {} bytes, {}μs", 
            sorted_messages.len(), total_bytes, batch_latency / 1000);
        
        Ok(())
    }
    
    /// Force immediate flush of all pending messages
    pub async fn flush(&self) -> Result<(), UltraFastError> {
        Self::flush_batch(&self.inner, &self.batch_buffer, &self.performance_stats).await
    }
    
    /// Stop the publisher and background tasks
    pub async fn stop(&mut self) -> Result<(), UltraFastError> {
        self.is_running.store(false, Ordering::Relaxed);
        
        // Flush any remaining messages
        self.flush().await?;
        
        // Wait for background task to complete
        if let Some(handle) = self.background_flush_handle.take() {
            let _ = handle.await;
        }
        
        // Disconnect the inner publisher
        self.inner.disconnect().await?;
        
        log_info!(PUBLISHER_LOGGER, "🛑 OptimizedPublisher stopped");
        Ok(())
    }
    
    /// Get performance statistics
    pub fn get_performance_stats(&self) -> (u64, f64, u64, u64, u64, u64) {
        self.performance_stats.get_stats()
    }
    
    /// Check if publisher is connected
    pub fn is_connected(&self) -> bool {
        self.inner.is_connected()
    }
    
    /// Get count of pending messages
    pub fn pending_messages_count(&self) -> usize {
        self.batch_buffer.read().len()
    }
}

impl Drop for OptimizedPublisher {
    fn drop(&mut self) {
        self.is_running.store(false, Ordering::Relaxed);
    }
}
