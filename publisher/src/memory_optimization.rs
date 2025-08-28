// Memory optimization for ultra-low latency publishing
// Implements memory pool and zero-copy recommendations from latency profiling

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use crossbeam::queue::SegQueue;

/// High-performance memory pool for message buffers
pub struct MessageBufferPool {
    small_buffers: SegQueue<Vec<u8>>,      // 1KB buffers
    medium_buffers: SegQueue<Vec<u8>>,     // 8KB buffers  
    large_buffers: SegQueue<Vec<u8>>,      // 64KB buffers
    pool_stats: Arc<PoolStatistics>,
    config: PoolConfig,
}

/// Memory pool configuration
#[derive(Clone)]
pub struct PoolConfig {
    pub small_buffer_size: usize,
    pub medium_buffer_size: usize,
    pub large_buffer_size: usize,
    pub small_pool_size: usize,
    pub medium_pool_size: usize,
    pub large_pool_size: usize,
    pub enable_preallocation: bool,
    pub auto_scaling: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            small_buffer_size: 1024,      // 1KB
            medium_buffer_size: 8192,     // 8KB
            large_buffer_size: 65536,     // 64KB
            small_pool_size: 1000,        // 1000 small buffers
            medium_pool_size: 500,        // 500 medium buffers
            large_pool_size: 100,         // 100 large buffers
            enable_preallocation: true,
            auto_scaling: true,
        }
    }
}

/// Pool statistics for monitoring
#[derive(Default)]
pub struct PoolStatistics {
    small_allocated: AtomicUsize,
    medium_allocated: AtomicUsize,
    large_allocated: AtomicUsize,
    small_returned: AtomicUsize,
    medium_returned: AtomicUsize,
    large_returned: AtomicUsize,
    cache_hits: AtomicUsize,
    cache_misses: AtomicUsize,
}

impl PoolStatistics {
    pub fn get_stats(&self) -> PoolStats {
        PoolStats {
            small_allocated: self.small_allocated.load(Ordering::Relaxed),
            medium_allocated: self.medium_allocated.load(Ordering::Relaxed),
            large_allocated: self.large_allocated.load(Ordering::Relaxed),
            small_returned: self.small_returned.load(Ordering::Relaxed),
            medium_returned: self.medium_returned.load(Ordering::Relaxed),
            large_returned: self.large_returned.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
        }
    }
    
    pub fn reset(&self) {
        self.small_allocated.store(0, Ordering::Relaxed);
        self.medium_allocated.store(0, Ordering::Relaxed);
        self.large_allocated.store(0, Ordering::Relaxed);
        self.small_returned.store(0, Ordering::Relaxed);
        self.medium_returned.store(0, Ordering::Relaxed);
        self.large_returned.store(0, Ordering::Relaxed);
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
pub struct PoolStats {
    pub small_allocated: usize,
    pub medium_allocated: usize,
    pub large_allocated: usize,
    pub small_returned: usize,
    pub medium_returned: usize,
    pub large_returned: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
}

impl PoolStats {
    pub fn hit_rate(&self) -> f64 {
        let total_requests = self.cache_hits + self.cache_misses;
        if total_requests == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total_requests as f64
        }
    }
    
    pub fn total_allocated(&self) -> usize {
        self.small_allocated + self.medium_allocated + self.large_allocated
    }
    
    pub fn total_returned(&self) -> usize {
        self.small_returned + self.medium_returned + self.large_returned
    }
    
    pub fn memory_efficiency(&self) -> f64 {
        let total_alloc = self.total_allocated();
        let total_return = self.total_returned();
        if total_alloc == 0 {
            0.0
        } else {
            total_return as f64 / total_alloc as f64
        }
    }
}

impl MessageBufferPool {
    pub fn new(config: PoolConfig) -> Self {
        let pool = Self {
            small_buffers: SegQueue::new(),
            medium_buffers: SegQueue::new(),
            large_buffers: SegQueue::new(),
            pool_stats: Arc::new(PoolStatistics::default()),
            config,
        };
        
        if pool.config.enable_preallocation {
            pool.preallocate_buffers();
        }
        
        pool
    }
    
    /// Pre-allocate buffers for immediate availability
    fn preallocate_buffers(&self) {
        // Pre-allocate small buffers
        for _ in 0..self.config.small_pool_size {
            let buffer = Vec::with_capacity(self.config.small_buffer_size);
            self.small_buffers.push(buffer);
        }
        
        // Pre-allocate medium buffers
        for _ in 0..self.config.medium_pool_size {
            let buffer = Vec::with_capacity(self.config.medium_buffer_size);
            self.medium_buffers.push(buffer);
        }
        
        // Pre-allocate large buffers
        for _ in 0..self.config.large_pool_size {
            let buffer = Vec::with_capacity(self.config.large_buffer_size);
            self.large_buffers.push(buffer);
        }
        
        tracing::info!("Pre-allocated {} small, {} medium, {} large buffers",
            self.config.small_pool_size, self.config.medium_pool_size, self.config.large_pool_size);
    }
    
    /// Get buffer optimized for the required size
    pub fn get_buffer(&self, required_size: usize) -> Vec<u8> {
        let buffer = if required_size <= self.config.small_buffer_size {
            self.get_small_buffer()
        } else if required_size <= self.config.medium_buffer_size {
            self.get_medium_buffer()
        } else {
            self.get_large_buffer()
        };
        
        buffer
    }
    
    /// Get a small buffer (1KB)
    pub fn get_small_buffer(&self) -> Vec<u8> {
        if let Some(mut buffer) = self.small_buffers.pop() {
            buffer.clear();
            self.pool_stats.small_allocated.fetch_add(1, Ordering::Relaxed);
            self.pool_stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            buffer
        } else {
            self.pool_stats.cache_misses.fetch_add(1, Ordering::Relaxed);
            Vec::with_capacity(self.config.small_buffer_size)
        }
    }
    
    /// Get a medium buffer (8KB)
    pub fn get_medium_buffer(&self) -> Vec<u8> {
        if let Some(mut buffer) = self.medium_buffers.pop() {
            buffer.clear();
            self.pool_stats.medium_allocated.fetch_add(1, Ordering::Relaxed);
            self.pool_stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            buffer
        } else {
            self.pool_stats.cache_misses.fetch_add(1, Ordering::Relaxed);
            Vec::with_capacity(self.config.medium_buffer_size)
        }
    }
    
    /// Get a large buffer (64KB)
    pub fn get_large_buffer(&self) -> Vec<u8> {
        if let Some(mut buffer) = self.large_buffers.pop() {
            buffer.clear();
            self.pool_stats.large_allocated.fetch_add(1, Ordering::Relaxed);
            self.pool_stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            buffer
        } else {
            self.pool_stats.cache_misses.fetch_add(1, Ordering::Relaxed);
            Vec::with_capacity(self.config.large_buffer_size)
        }
    }
    
    /// Return buffer to the appropriate pool
    pub fn return_buffer(&self, mut buffer: Vec<u8>) {
        buffer.clear();
        
        let capacity = buffer.capacity();
        
        // Determine which pool to return to based on capacity
        if capacity >= self.config.small_buffer_size && capacity < self.config.medium_buffer_size {
            // Prevent pool from growing too large
            if self.small_buffers.len() < self.config.small_pool_size * 2 {
                self.small_buffers.push(buffer);
                self.pool_stats.small_returned.fetch_add(1, Ordering::Relaxed);
            }
        } else if capacity >= self.config.medium_buffer_size && capacity < self.config.large_buffer_size {
            if self.medium_buffers.len() < self.config.medium_pool_size * 2 {
                self.medium_buffers.push(buffer);
                self.pool_stats.medium_returned.fetch_add(1, Ordering::Relaxed);
            }
        } else if capacity >= self.config.large_buffer_size {
            if self.large_buffers.len() < self.config.large_pool_size * 2 {
                self.large_buffers.push(buffer);
                self.pool_stats.large_returned.fetch_add(1, Ordering::Relaxed);
            }
        }
        // If buffer doesn't fit any category or pools are full, let it drop
    }
    
    /// Get pool statistics
    pub fn get_statistics(&self) -> PoolStats {
        self.pool_stats.get_stats()
    }
    
    /// Reset pool statistics
    pub fn reset_statistics(&self) {
        self.pool_stats.reset();
    }
    
    /// Get current pool sizes
    pub fn get_pool_sizes(&self) -> (usize, usize, usize) {
        (
            self.small_buffers.len(),
            self.medium_buffers.len(),
            self.large_buffers.len()
        )
    }
    
    /// Check pool health and provide recommendations
    pub fn health_check(&self) -> PoolHealthReport {
        let stats = self.get_statistics();
        let (small_size, medium_size, large_size) = self.get_pool_sizes();
        
        let mut recommendations = Vec::new();
        let mut health_score = 100.0;
        
        // Check hit rate
        if stats.hit_rate() < 0.8 {
            recommendations.push("⚠️  Low cache hit rate - consider increasing pool sizes".to_string());
            health_score -= 20.0;
        } else if stats.hit_rate() > 0.95 {
            recommendations.push("✅ Excellent cache hit rate".to_string());
        }
        
        // Check pool depletion
        if small_size == 0 && stats.cache_misses > 0 {
            recommendations.push("⚠️  Small buffer pool is depleted".to_string());
            health_score -= 15.0;
        }
        
        if medium_size == 0 && stats.cache_misses > 0 {
            recommendations.push("⚠️  Medium buffer pool is depleted".to_string());
            health_score -= 15.0;
        }
        
        if large_size == 0 && stats.cache_misses > 0 {
            recommendations.push("⚠️  Large buffer pool is depleted".to_string());
            health_score -= 15.0;
        }
        
        // Check memory efficiency
        if stats.memory_efficiency() < 0.7 {
            recommendations.push("⚠️  Low memory efficiency - many buffers not being returned".to_string());
            health_score -= 10.0;
        }
        
        // Positive recommendations
        if health_score > 90.0 {
            recommendations.push("🚀 Memory pool is operating at optimal efficiency".to_string());
        }
        
        PoolHealthReport {
            health_score,
            cache_hit_rate: stats.hit_rate(),
            memory_efficiency: stats.memory_efficiency(),
            pool_sizes: (small_size, medium_size, large_size),
            recommendations,
            stats,
        }
    }
}

/// Health report for memory pool monitoring
#[derive(Debug)]
pub struct PoolHealthReport {
    pub health_score: f64,
    pub cache_hit_rate: f64,
    pub memory_efficiency: f64,
    pub pool_sizes: (usize, usize, usize),
    pub recommendations: Vec<String>,
    pub stats: PoolStats,
}

impl PoolHealthReport {
    pub fn print_report(&self) {
        println!("\n🧠 MEMORY POOL HEALTH REPORT");
        println!("============================");
        println!("📊 Health Score: {:.1}/100", self.health_score);
        println!("🎯 Cache Hit Rate: {:.1}%", self.cache_hit_rate * 100.0);
        println!("♻️  Memory Efficiency: {:.1}%", self.memory_efficiency * 100.0);
        println!("📦 Pool Sizes: {} small, {} medium, {} large", 
            self.pool_sizes.0, self.pool_sizes.1, self.pool_sizes.2);
        
        println!("\n📈 Detailed Statistics:");
        println!("├─ Total Allocated: {}", self.stats.total_allocated());
        println!("├─ Total Returned: {}", self.stats.total_returned());
        println!("├─ Cache Hits: {}", self.stats.cache_hits);
        println!("└─ Cache Misses: {}", self.stats.cache_misses);
        
        if !self.recommendations.is_empty() {
            println!("\n💡 Recommendations:");
            for rec in &self.recommendations {
                println!("├─ {}", rec);
            }
        }
    }
}

/// Zero-copy message builder for optimal performance
pub struct ZeroCopyMessageBuilder {
    buffer_pool: Arc<MessageBufferPool>,
    current_buffer: Option<Vec<u8>>,
    estimated_size: usize,
}

impl ZeroCopyMessageBuilder {
    pub fn new(buffer_pool: Arc<MessageBufferPool>) -> Self {
        Self {
            buffer_pool,
            current_buffer: None,
            estimated_size: 0,
        }
    }
    
    /// Start building a message with estimated size
    pub fn start_message(&mut self, estimated_size: usize) -> &mut Self {
        self.estimated_size = estimated_size;
        self.current_buffer = Some(self.buffer_pool.get_buffer(estimated_size));
        self
    }
    
    /// Add topic to message
    pub fn add_topic(&mut self, topic: &str) -> &mut Self {
        if let Some(ref mut buffer) = self.current_buffer {
            let topic_bytes = topic.as_bytes();
            buffer.extend_from_slice(&(topic_bytes.len() as u32).to_le_bytes());
            buffer.extend_from_slice(topic_bytes);
        }
        self
    }
    
    /// Add payload to message
    pub fn add_payload(&mut self, payload: &[u8]) -> &mut Self {
        if let Some(ref mut buffer) = self.current_buffer {
            buffer.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            buffer.extend_from_slice(payload);
        }
        self
    }
    
    /// Finalize message and take ownership of buffer
    pub fn build(mut self) -> Option<Vec<u8>> {
        self.current_buffer.take()
    }
    
    /// Return buffer to pool without building message
    pub fn cancel(mut self) {
        if let Some(buffer) = self.current_buffer.take() {
            self.buffer_pool.return_buffer(buffer);
        }
    }
}

impl Drop for ZeroCopyMessageBuilder {
    fn drop(&mut self) {
        if let Some(buffer) = self.current_buffer.take() {
            self.buffer_pool.return_buffer(buffer);
        }
    }
}
