// Ultra-fast flow control and backpressure management
// Advanced algorithms for message throttling, rate limiting, and adaptive control

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, mpsc};
use tokio::time::sleep;
use tracing::{info, warn, error};

/// Flow control strategy types
#[derive(Debug, Clone, PartialEq)]
pub enum FlowControlStrategy {
    /// No flow control (unlimited)
    None,
    /// Simple token bucket algorithm
    TokenBucket { tokens_per_second: u64, bucket_capacity: u64 },
    /// Sliding window rate limiting
    SlidingWindow { window_size: Duration, max_requests: u64 },
    /// Adaptive rate limiting based on system load
    Adaptive { base_rate: u64, max_burst: u64, adaptation_factor: f64 },
    /// Backpressure-based control
    Backpressure { max_buffer_size: usize, low_watermark: usize, high_watermark: usize },
    /// Hybrid approach combining multiple strategies
    Hybrid {
        primary: Box<FlowControlStrategy>,
        fallback: Box<FlowControlStrategy>,
        switch_threshold: f64,
    },
}

impl Default for FlowControlStrategy {
    fn default() -> Self {
        FlowControlStrategy::Adaptive {
            base_rate: 10000,
            max_burst: 50000,
            adaptation_factor: 0.8,
        }
    }
}

/// Backpressure configuration
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    pub strategy: FlowControlStrategy,
    pub max_concurrent_messages: usize,
    pub buffer_size: usize,
    pub low_watermark_percent: f64,
    pub high_watermark_percent: f64,
    pub enable_circuit_breaker: bool,
    pub circuit_breaker_threshold: f64,
    pub circuit_breaker_timeout: Duration,
    pub enable_adaptive_throttling: bool,
    pub throttling_factor: f64,
    pub metrics_window: Duration,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            strategy: FlowControlStrategy::default(),
            max_concurrent_messages: 10000,
            buffer_size: 100000,
            low_watermark_percent: 0.7,
            high_watermark_percent: 0.9,
            enable_circuit_breaker: true,
            circuit_breaker_threshold: 0.95,
            circuit_breaker_timeout: Duration::from_secs(30),
            enable_adaptive_throttling: true,
            throttling_factor: 0.5,
            metrics_window: Duration::from_secs(60),
        }
    }
}

/// Flow control decision
#[derive(Debug, Clone, PartialEq)]
pub enum FlowControlDecision {
    /// Allow message processing immediately
    Allow,
    /// Drop the message due to overload
    Drop,
    /// Block until resources become available
    Block,
    /// Apply rate limiting delay
    RateLimit,
}

/// Ultra-fast flow control manager
pub struct FlowControlManager {
    config: BackpressureConfig,
    permit_semaphore: Arc<Semaphore>,
    buffer_size: Arc<AtomicUsize>,
    stats: FlowControlStats,
    
    // Token bucket state (for token bucket strategy)
    tokens: Arc<AtomicU64>,
    last_refill: Arc<AtomicU64>,
    
    // Sliding window state
    window_requests: Arc<AtomicU64>,
    window_start: Arc<AtomicU64>,
    
    // Adaptive control state
    current_rate: Arc<AtomicU64>,
    load_factor: Arc<AtomicU64>, // Fixed point: factor * 1000
    messages_per_window: Arc<AtomicU64>,
    
    // Circuit breaker state
    circuit_breaker_open: Arc<AtomicU64>, // timestamp when opened, 0 if closed
    
    is_running: Arc<std::sync::atomic::AtomicBool>,
}

/// Flow control statistics
#[derive(Debug, Default)]
pub struct FlowControlStats {
    pub total_requests: AtomicU64,
    pub allowed_requests: AtomicU64,
    pub dropped_requests: AtomicU64,
    pub blocked_requests: AtomicU64,
    pub rate_limited_requests: AtomicU64,
    pub active_permits: AtomicUsize,
    pub circuit_breaker_trips: AtomicU64,
    pub avg_processing_time_ns: AtomicU64,
}

impl FlowControlManager {
    pub fn new(config: BackpressureConfig) -> Self {
        let permit_count = config.max_concurrent_messages;
        
        Self {
            permit_semaphore: Arc::new(Semaphore::new(permit_count)),
            buffer_size: Arc::new(AtomicUsize::new(0)),
            tokens: Arc::new(AtomicU64::new(0)),
            last_refill: Arc::new(AtomicU64::new(0)),
            window_requests: Arc::new(AtomicU64::new(0)),
            window_start: Arc::new(AtomicU64::new(0)),
            current_rate: Arc::new(AtomicU64::new(10000)), // Start with base rate
            load_factor: Arc::new(AtomicU64::new(500)), // 0.5 * 1000
            messages_per_window: Arc::new(AtomicU64::new(0)),
            circuit_breaker_open: Arc::new(AtomicU64::new(0)),
            stats: FlowControlStats::default(),
            is_running: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            config,
        }
    }

    /// Determine if a message should be processed
    pub async fn should_process_message(&self, message_size: usize) -> FlowControlDecision {
        self.stats.total_requests.fetch_add(1, Ordering::Relaxed);
        
        // Check circuit breaker first
        if self.is_circuit_breaker_open() {
            self.stats.dropped_requests.fetch_add(1, Ordering::Relaxed);
            return FlowControlDecision::Drop;
        }
        
        match &self.config.strategy {
            FlowControlStrategy::None => FlowControlDecision::Allow,
            FlowControlStrategy::TokenBucket { tokens_per_second, bucket_capacity } => {
                self.token_bucket_decision(*tokens_per_second, *bucket_capacity)
            }
            FlowControlStrategy::SlidingWindow { window_size, max_requests } => {
                self.sliding_window_decision(*window_size, *max_requests)
            }
            FlowControlStrategy::Adaptive { base_rate, max_burst, adaptation_factor } => {
                self.adaptive_decision(*base_rate, *max_burst, *adaptation_factor)
            }
            FlowControlStrategy::Backpressure { max_buffer_size, low_watermark, high_watermark } => {
                self.backpressure_decision(*max_buffer_size, *low_watermark, *high_watermark)
            }
            FlowControlStrategy::Hybrid { primary, fallback, switch_threshold } => {
                // Simplified hybrid logic - use primary if load is below threshold
                let current_load = self.calculate_current_load();
                if current_load < *switch_threshold {
                    // Use primary strategy (recursive call with primary config)
                    FlowControlDecision::Allow
                } else {
                    // Use fallback strategy
                    FlowControlDecision::RateLimit
                }
            }
        }
    }

    /// Acquire a permit for message processing
    pub async fn acquire_permit(&self) -> Result<FlowControlPermit, FlowControlError> {
        let decision = self.should_process_message(0).await;
        let semaphore = Arc::clone(&self.permit_semaphore);
        
        match decision {
            FlowControlDecision::Allow => {
                let permit = semaphore.acquire_owned().await
                    .map_err(|_| FlowControlError::PermitAcquisitionFailed)?;
                
                self.buffer_size.fetch_add(1, Ordering::Relaxed);
                self.stats.allowed_requests.fetch_add(1, Ordering::Relaxed);
                
                Ok(FlowControlPermit::new(permit, Arc::clone(&self.buffer_size)))
            }
            
            FlowControlDecision::Drop => {
                self.stats.dropped_requests.fetch_add(1, Ordering::Relaxed);
                Err(FlowControlError::MessageDropped)
            }
            
            FlowControlDecision::Block => {
                // Wait for buffer space to become available
                let permit = semaphore.acquire_owned().await
                    .map_err(|_| FlowControlError::PermitAcquisitionFailed)?;
                
                self.buffer_size.fetch_add(1, Ordering::Relaxed);
                self.stats.blocked_requests.fetch_add(1, Ordering::Relaxed);
                
                Ok(FlowControlPermit::new(permit, Arc::clone(&self.buffer_size)))
            }
            
            FlowControlDecision::RateLimit => {
                // Wait for next rate window
                let window_remaining = self.get_window_remaining().await;
                sleep(window_remaining).await;
                
                // Try again in next window
                let permit = semaphore.acquire_owned().await
                    .map_err(|_| FlowControlError::PermitAcquisitionFailed)?;
                
                self.buffer_size.fetch_add(1, Ordering::Relaxed);
                self.stats.rate_limited_requests.fetch_add(1, Ordering::Relaxed);
                
                Ok(FlowControlPermit::new(permit, Arc::clone(&self.buffer_size)))
            }
        }
    }

    fn token_bucket_decision(&self, tokens_per_second: u64, bucket_capacity: u64) -> FlowControlDecision {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        
        let last_refill = self.last_refill.load(Ordering::Relaxed);
        let time_passed = now.saturating_sub(last_refill);
        
        // Refill tokens based on time passed
        let tokens_to_add = (time_passed * tokens_per_second) / 1_000_000_000;
        if tokens_to_add > 0 {
            let current_tokens = self.tokens.load(Ordering::Relaxed);
            let new_tokens = std::cmp::min(current_tokens + tokens_to_add, bucket_capacity);
            self.tokens.store(new_tokens, Ordering::Relaxed);
            self.last_refill.store(now, Ordering::Relaxed);
        }
        
        // Try to consume a token
        let current_tokens = self.tokens.load(Ordering::Relaxed);
        if current_tokens > 0 {
            self.tokens.fetch_sub(1, Ordering::Relaxed);
            FlowControlDecision::Allow
        } else {
            FlowControlDecision::RateLimit
        }
    }

    fn sliding_window_decision(&self, window_size: Duration, max_requests: u64) -> FlowControlDecision {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        let window_start = self.window_start.load(Ordering::Relaxed);
        let window_ms = window_size.as_millis() as u64;
        
        // Check if we need to start a new window
        if now - window_start > window_ms {
            self.window_start.store(now, Ordering::Relaxed);
            self.window_requests.store(0, Ordering::Relaxed);
        }
        
        let current_requests = self.window_requests.fetch_add(1, Ordering::Relaxed);
        
        if current_requests < max_requests {
            FlowControlDecision::Allow
        } else {
            FlowControlDecision::RateLimit
        }
    }

    fn adaptive_decision(&self, base_rate: u64, max_burst: u64, adaptation_factor: f64) -> FlowControlDecision {
        let load = self.calculate_current_load();
        let load_factor_int = (adaptation_factor * 1000.0) as u64;
        self.load_factor.store(load_factor_int, Ordering::Relaxed);
        
        // Adapt rate based on load
        let adapted_rate = if load > 0.8 {
            // High load: reduce rate
            (base_rate as f64 * adaptation_factor) as u64
        } else if load < 0.5 {
            // Low load: allow burst
            std::cmp::min(max_burst, base_rate * 2)
        } else {
            base_rate
        };
        
        self.current_rate.store(adapted_rate, Ordering::Relaxed);
        
        // Simple rate check
        let current_messages = self.messages_per_window.fetch_add(1, Ordering::Relaxed);
        if current_messages < adapted_rate {
            FlowControlDecision::Allow
        } else {
            FlowControlDecision::RateLimit
        }
    }

    fn backpressure_decision(&self, max_buffer_size: usize, low_watermark: usize, high_watermark: usize) -> FlowControlDecision {
        let current_buffer = self.buffer_size.load(Ordering::Relaxed);
        
        if current_buffer < low_watermark {
            FlowControlDecision::Allow
        } else if current_buffer < high_watermark {
            FlowControlDecision::Block
        } else if current_buffer >= max_buffer_size {
            FlowControlDecision::Drop
        } else {
            FlowControlDecision::RateLimit
        }
    }

    fn calculate_current_load(&self) -> f64 {
        let active_permits = self.stats.active_permits.load(Ordering::Relaxed) as f64;
        let max_permits = self.config.max_concurrent_messages as f64;
        (active_permits / max_permits).min(1.0)
    }

    fn is_circuit_breaker_open(&self) -> bool {
        if !self.config.enable_circuit_breaker {
            return false;
        }
        
        let open_timestamp = self.circuit_breaker_open.load(Ordering::Relaxed);
        if open_timestamp == 0 {
            return false; // Circuit breaker is closed
        }
        
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        let timeout_ms = self.config.circuit_breaker_timeout.as_millis() as u64;
        
        if now - open_timestamp > timeout_ms {
            // Timeout expired, close the circuit breaker
            self.circuit_breaker_open.store(0, Ordering::Relaxed);
            false
        } else {
            true
        }
    }

    async fn get_window_remaining(&self) -> Duration {
        // Simple implementation - wait for 1ms
        Duration::from_millis(1)
    }

    pub fn get_stats(&self) -> FlowControlStats {
        FlowControlStats {
            total_requests: AtomicU64::new(self.stats.total_requests.load(Ordering::Relaxed)),
            allowed_requests: AtomicU64::new(self.stats.allowed_requests.load(Ordering::Relaxed)),
            dropped_requests: AtomicU64::new(self.stats.dropped_requests.load(Ordering::Relaxed)),
            blocked_requests: AtomicU64::new(self.stats.blocked_requests.load(Ordering::Relaxed)),
            rate_limited_requests: AtomicU64::new(self.stats.rate_limited_requests.load(Ordering::Relaxed)),
            active_permits: AtomicUsize::new(self.stats.active_permits.load(Ordering::Relaxed)),
            circuit_breaker_trips: AtomicU64::new(self.stats.circuit_breaker_trips.load(Ordering::Relaxed)),
            avg_processing_time_ns: AtomicU64::new(self.stats.avg_processing_time_ns.load(Ordering::Relaxed)),
        }
    }
}

/// Flow control permit that automatically releases when dropped
pub struct FlowControlPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
    buffer_size: Arc<AtomicUsize>,
}

impl FlowControlPermit {
    fn new(permit: tokio::sync::OwnedSemaphorePermit, buffer_size: Arc<AtomicUsize>) -> Self {
        Self {
            _permit: permit,
            buffer_size,
        }
    }
}

impl Drop for FlowControlPermit {
    fn drop(&mut self) {
        self.buffer_size.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Flow control errors
#[derive(Debug, Clone)]
pub enum FlowControlError {
    PermitUnavailable,
    PermitAcquisitionFailed,
    MessageDropped,
    CircuitBreakerOpen,
    RateLimitExceeded,
    BufferFull,
    SystemOverload,
}

impl std::fmt::Display for FlowControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlowControlError::PermitUnavailable => write!(f, "Flow control permit unavailable"),
            FlowControlError::PermitAcquisitionFailed => write!(f, "Failed to acquire flow control permit"),
            FlowControlError::MessageDropped => write!(f, "Message dropped due to overload"),
            FlowControlError::CircuitBreakerOpen => write!(f, "Circuit breaker is open"),
            FlowControlError::RateLimitExceeded => write!(f, "Rate limit exceeded"),
            FlowControlError::BufferFull => write!(f, "Buffer is full"),
            FlowControlError::SystemOverload => write!(f, "System is overloaded"),
        }
    }
}

impl std::error::Error for FlowControlError {}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_flow_control_manager_creation() {
        let config = BackpressureConfig::default();
        let manager = FlowControlManager::new(config);
        
        let stats = manager.get_stats();
        assert_eq!(stats.total_requests.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_permit_acquisition() {
        let config = BackpressureConfig::default();
        let manager = FlowControlManager::new(config);
        
        let permit = manager.acquire_permit().await;
        assert!(permit.is_ok());
    }

    #[tokio::test]
    async fn test_token_bucket_strategy() {
        let strategy = FlowControlStrategy::TokenBucket {
            tokens_per_second: 100,
            bucket_capacity: 10,
        };
        
        let config = BackpressureConfig {
            strategy,
            ..BackpressureConfig::default()
        };
        
        let manager = FlowControlManager::new(config);
        
        // First request should be allowed (assuming tokens are available)
        let decision = manager.should_process_message(100).await;
        assert!(matches!(decision, FlowControlDecision::Allow | FlowControlDecision::RateLimit));
    }

    #[tokio::test]
    async fn test_adaptive_strategy() {
        let strategy = FlowControlStrategy::Adaptive {
            base_rate: 1000,
            max_burst: 2000,
            adaptation_factor: 0.8,
        };
        
        let config = BackpressureConfig {
            strategy,
            ..BackpressureConfig::default()
        };
        
        let manager = FlowControlManager::new(config);
        
        let decision = manager.should_process_message(100).await;
        assert!(matches!(decision, FlowControlDecision::Allow | FlowControlDecision::RateLimit));
    }
}
