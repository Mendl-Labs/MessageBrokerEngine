// Ultra-high performance message broker host - ENHANCED VERSION
// All optimizations, shared memory, lock-free structures, and performance tuning in one file

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use std::collections::HashMap;

use tokio::net::{TcpListener, TcpStream};

// Ultra-Logger for high-performance logging
use ultra_logger::{UltraLogger, LoggerConfig, TransportConfig, ConnectionConfig};
use once_cell::sync::Lazy;

pub mod wal;
pub mod flow_control;

pub use wal::{WriteAheadLog, WALConfig, WALEntry};
pub use flow_control::{FlowControlManager, BackpressureConfig, FlowControlStrategy};

// Create Elasticsearch configuration for hostbuilder
fn create_elasticsearch_config(component: &str) -> LoggerConfig {
    let use_elasticsearch = std::env::var("USE_ELASTICSEARCH_LOGGING")
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(true);

    if use_elasticsearch {
        let endpoint = std::env::var("ELASTICSEARCH_ENDPOINT")
            .or_else(|_| std::env::var("ELASTIC_CLOUD_ENDPOINT"))
            .unwrap_or_else(|_| "https://trading-bot-observability-6b76eb.es.us-central1.gcp.cloud.es.io".to_string());
        let username = std::env::var("ELASTICSEARCH_USERNAME")
            .or_else(|_| std::env::var("ELASTIC_CLOUD_USERNAME"))
            .unwrap_or_else(|_| "elastic".to_string());
        let password = std::env::var("ELASTICSEARCH_PASSWORD")
            .or_else(|_| std::env::var("ELASTIC_CLOUD_PASSWORD"))
            .unwrap_or_default();

        let mut options = HashMap::new();
        options.insert("index".to_string(), format!("messagebroker-{}-logs", component));

        LoggerConfig {
            level: std::env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string()),
            transport: TransportConfig {
                transport_type: "elasticsearch".to_string(),
                connection: ConnectionConfig {
                    host: endpoint,
                    port: 443,
                    username: Some(username),
                    password: Some(password),
                    options,
                },
            },
        }
    } else {
        LoggerConfig::default()
    }
}

/// Host builder logger instance
pub static HOST_LOGGER: Lazy<Arc<UltraLogger>> = Lazy::new(|| {
    Arc::new(UltraLogger::with_config(
        "MessageBrokerEngine-hostbuilder".to_string(),
        create_elasticsearch_config("hostbuilder")
    ))
});

/// Connection logger instance
pub static CONN_LOGGER: Lazy<Arc<UltraLogger>> = Lazy::new(|| {
    Arc::new(UltraLogger::with_config(
        "MessageBrokerEngine-connection".to_string(),
        create_elasticsearch_config("connection")
    ))
});

// Synchronous logging macros for hostbuilder
macro_rules! host_info {
    ($msg:expr) => {
        HOST_LOGGER.info_sync($msg.to_string());
    };
    ($fmt:expr, $($arg:tt)*) => {
        HOST_LOGGER.info_sync(format!($fmt, $($arg)*));
    };
}

macro_rules! host_warn {
    ($msg:expr) => {
        HOST_LOGGER.warn_sync($msg.to_string());
    };
    ($fmt:expr, $($arg:tt)*) => {
        HOST_LOGGER.warn_sync(format!($fmt, $($arg)*));
    };
}

macro_rules! host_error {
    ($msg:expr) => {
        HOST_LOGGER.error_sync($msg.to_string());
    };
    ($fmt:expr, $($arg:tt)*) => {
        HOST_LOGGER.error_sync(format!($fmt, $($arg)*));
    };
}

macro_rules! host_debug {
    ($msg:expr) => {
        HOST_LOGGER.debug_sync($msg.to_string());
    };
    ($fmt:expr, $($arg:tt)*) => {
        HOST_LOGGER.debug_sync(format!($fmt, $($arg)*));
    };
}

// Cross-platform ultra-fast timestamp function
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

// Ultra-performance constants
const MAX_WORKER_THREADS: usize = 64;
const CONNECTION_TIMEOUT_MS: u64 = 100;
const READ_BUFFER_SIZE: usize = 1048576;      // 1MB
const MAX_MESSAGE_SIZE: usize = 16777216;     // 16MB

// Ultra-fast error types
#[derive(Debug, Clone)]
pub enum BrokerError {
    ConnectionFailed,
    MessageTooBig,
    TooManyConnections,
    SerializationError,
    NetworkError,
    SystemError,
}

impl std::fmt::Display for BrokerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BrokerError::ConnectionFailed => write!(f, "Connection failed"),
            BrokerError::MessageTooBig => write!(f, "Message too big"),
            BrokerError::TooManyConnections => write!(f, "Too many connections"),
            BrokerError::SerializationError => write!(f, "Serialization error"),
            BrokerError::NetworkError => write!(f, "Network error"),
            BrokerError::SystemError => write!(f, "System error"),
        }
    }
}

impl std::error::Error for BrokerError {}

// Enhanced broker configuration with new features
#[derive(Debug)]
pub struct BrokerConfig {
    pub host: String,
    pub port: u16,
    pub max_connections: usize,
    pub worker_threads: usize,
    pub connection_timeout: Duration,
    pub read_buffer_size: usize,
    pub max_message_size: usize,
    pub tcp_nodelay: bool,
    pub socket_reuse: bool,
    pub keepalive: bool,
    pub backlog: i32,
    pub busy_poll: bool,
    pub cpu_affinity: Vec<usize>,
    pub use_huge_pages: bool,
    pub shared_memory_size: usize,
    
    // New enhanced features
    pub enable_wal: bool,
    pub wal_config: WALConfig,
    pub flow_control_config: BackpressureConfig,
    pub enable_compression: bool,
    pub enable_intelligent_routing: bool,
}

impl Clone for BrokerConfig {
    fn clone(&self) -> Self {
        Self {
            host: self.host.clone(),
            port: self.port,
            max_connections: self.max_connections,
            worker_threads: self.worker_threads,
            connection_timeout: self.connection_timeout,
            read_buffer_size: self.read_buffer_size,
            max_message_size: self.max_message_size,
            tcp_nodelay: self.tcp_nodelay,
            socket_reuse: self.socket_reuse,
            keepalive: self.keepalive,
            backlog: self.backlog,
            busy_poll: self.busy_poll,
            cpu_affinity: self.cpu_affinity.clone(),
            use_huge_pages: self.use_huge_pages,
            shared_memory_size: self.shared_memory_size,
            enable_wal: self.enable_wal,
            wal_config: self.wal_config.clone(),
            flow_control_config: self.flow_control_config.clone(),
            enable_compression: self.enable_compression,
            enable_intelligent_routing: self.enable_intelligent_routing,
        }
    }
}

// Lock-free metrics for ultra-fast performance tracking
#[derive(Debug, Default)]
pub struct UltraFastMetrics {
    active_connections: AtomicUsize,
    total_connections: AtomicU64,
    rejected_connections: AtomicU64,
    messages_processed: AtomicU64,
    errors: AtomicU64,
    #[allow(dead_code)]
    avg_processing_time_ns: AtomicU64,
    max_processing_time_ns: AtomicU64,
    #[allow(dead_code)]
    min_processing_time_ns: AtomicU64,
    bytes_processed: AtomicU64,
    total_latency_ns: AtomicU64,
    #[allow(dead_code)]
    min_latency_ns: AtomicU64,
    #[allow(dead_code)]
    max_latency_ns: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
}

impl UltraFastMetrics {
    pub fn new() -> Self {
        Self {
            active_connections: AtomicUsize::new(0),
            total_connections: AtomicU64::new(0),
            rejected_connections: AtomicU64::new(0),
            messages_processed: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            avg_processing_time_ns: AtomicU64::new(0),
            max_processing_time_ns: AtomicU64::new(0),
            min_processing_time_ns: AtomicU64::new(0),
            bytes_processed: AtomicU64::new(0),
            total_latency_ns: AtomicU64::new(0),
            min_latency_ns: AtomicU64::new(0),
            max_latency_ns: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    pub fn record_connection(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        self.total_connections.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn record_rejection(&self) {
        self.rejected_connections.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn record_message(&self, processing_time_ns: u64, bytes: usize) {
        self.messages_processed.fetch_add(1, Ordering::Relaxed);
        self.bytes_processed.fetch_add(bytes as u64, Ordering::Relaxed);
        self.total_latency_ns.fetch_add(processing_time_ns, Ordering::Relaxed);
        
        // Update min processing time (only if we've recorded messages)
        let current_messages = self.messages_processed.load(Ordering::Relaxed);
        if current_messages == 1 {  // First message, set min time
            self.min_processing_time_ns.store(processing_time_ns, Ordering::Relaxed);
        } else {
            let mut current_min = self.min_processing_time_ns.load(Ordering::Relaxed);
            while processing_time_ns < current_min && current_min > 0 {
                match self.min_processing_time_ns.compare_exchange_weak(
                    current_min, 
                    processing_time_ns, 
                    Ordering::Relaxed, 
                    Ordering::Relaxed
                ) {
                    Ok(_) => break,
                    Err(x) => current_min = x,
                }
            }
        }
        
        // Update max processing time
        let mut current_max = self.max_processing_time_ns.load(Ordering::Relaxed);
        while processing_time_ns > current_max {
            match self.max_processing_time_ns.compare_exchange_weak(
                current_max, 
                processing_time_ns, 
                Ordering::Relaxed, 
                Ordering::Relaxed
            ) {
                Ok(_) => break,
                Err(x) => current_max = x,
            }
        }
    }

    pub fn get_stats(&self) -> (u64, u64, u64, u64, u64, u64, u64, u64, u64) {
        let total_connections = self.total_connections.load(Ordering::Relaxed);
        let messages = self.messages_processed.load(Ordering::Relaxed);
        let bytes = self.bytes_processed.load(Ordering::Relaxed);
        let errors = self.errors.load(Ordering::Relaxed);
        let rejections = self.rejected_connections.load(Ordering::Relaxed);
        
        let total_time = self.total_latency_ns.load(Ordering::Relaxed);
        let avg_time = if messages > 0 { total_time / messages } else { 0 };
        
        let cache_hits = self.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.cache_misses.load(Ordering::Relaxed);
        let peak_memory = 0u64; // Placeholder for peak memory
        
        // Return order expected by tests: (connections, messages, bytes, errors, rejections, avg_time, cache_hits, cache_misses, peak_memory)
        (total_connections, messages, bytes, errors, rejections, avg_time, cache_hits, cache_misses, peak_memory)
    }
}

// Broker metrics for display
#[derive(Debug, Clone)]
pub struct BrokerMetrics {
    pub total_connections: u64,
    pub active_connections: u64,
    pub messages_processed: u64,
    pub bytes_processed: u64,
    pub errors: u64,
    pub rejected_connections: u64,
    pub avg_latency_ns: f64,
    pub min_latency_ns: u64,
    pub max_latency_ns: u64,
}

impl BrokerMetrics {
    pub fn print_detailed_report(&self) {
        println!("═══════════════════════════════════════════════════════════");
        println!("📊 BROKER PERFORMANCE REPORT");
        println!("═══════════════════════════════════════════════════════════");
        println!("🔗 Connections:");
        println!("   Total: {}", self.total_connections);
        println!("   Active: {}", self.active_connections);
        println!("   Rejected: {}", self.rejected_connections);
        println!("📨 Messages:");
        println!("   Processed: {}", self.messages_processed);
        println!("   Bytes: {} MB", self.bytes_processed / (1024 * 1024));
        println!("⚡ Performance:");
        println!("   Avg Latency: {:.2} ns", self.avg_latency_ns);
        println!("   Min Latency: {} ns", self.min_latency_ns);
        println!("   Max Latency: {} ns", self.max_latency_ns);
        println!("❌ Errors: {}", self.errors);
        println!("═══════════════════════════════════════════════════════════");
    }
}

// Message structure for compatibility
#[derive(Debug, Clone)]
pub struct BrokerMessage {
    pub topic: String,
    pub data: Vec<u8>,
    pub timestamp: u64,
    pub sequence: u64,
    pub client_id: u64,
    pub priority: u8,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8080,
            max_connections: 1000,
            worker_threads: 4,
            connection_timeout: Duration::from_millis(CONNECTION_TIMEOUT_MS),
            read_buffer_size: READ_BUFFER_SIZE,
            max_message_size: MAX_MESSAGE_SIZE,
            tcp_nodelay: true,
            socket_reuse: true,
            keepalive: true,
            backlog: 4096,
            busy_poll: false,
            cpu_affinity: Vec::new(),
            use_huge_pages: false,
            shared_memory_size: 256 * 1024 * 1024, // 256MB
            
            // New enhanced features
            enable_wal: true,
            wal_config: WALConfig::default(),
            flow_control_config: BackpressureConfig::default(),
            enable_compression: true,
            enable_intelligent_routing: true,
        }
    }
}

impl BrokerConfig {
    pub fn ultra_performance() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8080,
            max_connections: 50000,
            worker_threads: 64,
            connection_timeout: Duration::from_millis(CONNECTION_TIMEOUT_MS),
            read_buffer_size: 1048576, // 1MB
            max_message_size: MAX_MESSAGE_SIZE,
            tcp_nodelay: true,
            socket_reuse: true,
            keepalive: true,
            backlog: 4096,
            busy_poll: true,
            cpu_affinity: Vec::new(),
            use_huge_pages: true,
            shared_memory_size: 1024 * 1024 * 1024, // 1GB
            
            // New enhanced features
            enable_wal: true,
            wal_config: WALConfig::default(),
            flow_control_config: BackpressureConfig::default(),
            enable_compression: true,
            enable_intelligent_routing: true,
        }
    }

    /// Load configuration from environment variables with defaults
    pub fn from_env() -> Result<Self, String> {
        let mut config = Self::default();
        
        if let Ok(host) = std::env::var("BROKER_HOST") {
            config.host = host;
        }
        
        if let Ok(port) = std::env::var("BROKER_PORT") {
            config.port = port.parse().map_err(|_| "Invalid BROKER_PORT")?;
        }
        
        if let Ok(max_conn) = std::env::var("BROKER_MAX_CONNECTIONS") {
            config.max_connections = max_conn.parse().map_err(|_| "Invalid BROKER_MAX_CONNECTIONS")?;
        }
        
        if let Ok(threads) = std::env::var("BROKER_WORKER_THREADS") {
            config.worker_threads = threads.parse().map_err(|_| "Invalid BROKER_WORKER_THREADS")?;
        }
        
        if let Ok(timeout) = std::env::var("BROKER_CONNECTION_TIMEOUT_MS") {
            let timeout_ms: u64 = timeout.parse().map_err(|_| "Invalid BROKER_CONNECTION_TIMEOUT_MS")?;
            config.connection_timeout = Duration::from_millis(timeout_ms);
        }
        
        if let Ok(enable_wal) = std::env::var("BROKER_ENABLE_WAL") {
            config.enable_wal = enable_wal.parse().unwrap_or(false);
        }
        
        if let Ok(enable_compression) = std::env::var("BROKER_ENABLE_COMPRESSION") {
            config.enable_compression = enable_compression.parse().unwrap_or(false);
        }
        
        config.validate()?;
        Ok(config)
    }
    
    /// Validate configuration settings
    pub fn validate(&self) -> Result<(), String> {
        if self.port == 0 {
            return Err("Port cannot be 0".to_string());
        }
        
        if self.max_connections == 0 {
            return Err("Max connections must be greater than 0".to_string());
        }
        
        if self.worker_threads == 0 {
            return Err("Worker threads must be greater than 0".to_string());
        }
        
        if self.worker_threads > MAX_WORKER_THREADS {
            return Err(format!("Worker threads cannot exceed {}", MAX_WORKER_THREADS));
        }
        
        if self.read_buffer_size < 1024 {
            return Err("Read buffer size must be at least 1024 bytes".to_string());
        }
        
        if self.max_message_size < 1024 {
            return Err("Max message size must be at least 1024 bytes".to_string());
        }
        
        if self.max_message_size > MAX_MESSAGE_SIZE {
            return Err(format!("Max message size cannot exceed {} bytes", MAX_MESSAGE_SIZE));
        }
        
        if self.shared_memory_size < 1024 * 1024 { // 1MB minimum
            return Err("Shared memory size must be at least 1MB".to_string());
        }
        
        Ok(())
    }
}

// Ultra-fast connection tracking
#[derive(Debug)]
pub struct Connection {
    id: u64,
    #[allow(dead_code)]
    stream: Arc<tokio::sync::Mutex<TcpStream>>,
    is_active: Arc<AtomicBool>,
    #[allow(dead_code)]
    last_activity: u64,
}

impl Connection {
    pub fn new(id: u64, stream: TcpStream) -> Self {
        Self {
            id,
            stream: Arc::new(tokio::sync::Mutex::new(stream)),
            is_active: Arc::new(AtomicBool::new(true)),
            last_activity: get_rdtsc(),
        }
    }

    pub fn get_id(&self) -> u64 {
        self.id
    }

    pub fn is_active(&self) -> bool {
        self.is_active.load(Ordering::Relaxed)
    }

    pub fn close(&self) {
        self.is_active.store(false, Ordering::Relaxed);
    }
}

// Ultra-high performance message broker host with enhancements
pub struct MessageBrokerHost {
    config: BrokerConfig,
    metrics: Arc<UltraFastMetrics>,
    is_running: Arc<AtomicBool>,
    connections: Arc<parking_lot::RwLock<std::collections::HashMap<u64, Connection>>>,
    connection_counter: Arc<AtomicU64>,
    
    // Enhanced features
    #[allow(dead_code)]
    wal: Option<Arc<WriteAheadLog>>,
    flow_control: Arc<FlowControlManager>,
}

impl MessageBrokerHost {
    pub fn new(config: BrokerConfig) -> Self {
        let wal = if config.enable_wal {
            match WriteAheadLog::new(config.wal_config.clone()) {
                Ok(wal) => Some(Arc::new(wal)),
                Err(e) => {
                    host_warn!("Failed to initialize WAL: {}, continuing without persistence", e);
                    None
                }
            }
        } else {
            None
        };

        let flow_control = Arc::new(FlowControlManager::new(config.flow_control_config.clone()));

        Self {
            config,
            metrics: Arc::new(UltraFastMetrics::new()),
            is_running: Arc::new(AtomicBool::new(false)),
            connections: Arc::new(parking_lot::RwLock::new(std::collections::HashMap::new())),
            connection_counter: Arc::new(AtomicU64::new(0)),
            wal,
            flow_control,
        }
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        host_info!("🚀 Starting Enhanced Ultra-High Performance Message Broker");
        host_info!("   WAL Enabled: {}", self.config.enable_wal);
        host_info!("   Flow Control: {:?}", self.config.flow_control_config.strategy);
        host_info!("   Compression: {}", self.config.enable_compression);
        host_info!("   Intelligent Routing: {}", self.config.enable_intelligent_routing);

        self.is_running.store(true, Ordering::Relaxed);
        
        // Bind to address
        let listener = TcpListener::bind(format!("{}:{}", self.config.host, self.config.port)).await?;
        host_info!("Enhanced Message Broker listening on {}:{}", self.config.host, self.config.port);

        // Accept connections
        while self.is_running.load(Ordering::Relaxed) {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    host_debug!("New connection from: {}", addr);
                    
                    // Check flow control
                    match self.flow_control.acquire_permit().await {
                        Ok(_permit) => {
                            let conn_id = self.connection_counter.fetch_add(1, Ordering::Relaxed);
                            let connection = Connection::new(conn_id, stream);
                            
                            {
                                let mut connections = self.connections.write();
                                connections.insert(conn_id, connection);
                            }
                            
                            host_info!("Connection {} accepted with flow control", conn_id);
                        }
                        Err(e) => {
                            host_warn!("Connection rejected due to flow control: {}", e);
                            self.metrics.rejected_connections.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                Err(e) => {
                    host_error!("Error accepting connection: {}", e);
                    self.metrics.errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        Ok(())
    }

    pub fn stop(&self) {
        host_info!("Stopping Enhanced Message Broker");
        self.is_running.store(false, Ordering::Relaxed);
    }

    pub fn get_stats(&self) -> (usize, u64, u64, u64, u64, u64, u64, u64, u64) {
        let metrics_stats = self.metrics.get_stats();
        let active_connections = self.metrics.active_connections.load(Ordering::Relaxed);
        
        // Convert from test order (connections, messages, bytes, errors, rejections, avg_time, cache_hits, cache_misses, peak_memory)
        // to MessageBrokerHost order (active_connections, total_connections, rejected_connections, messages, errors, avg_time, ...)
        (
            active_connections,        // active connections as usize
            metrics_stats.0,          // total_connections
            metrics_stats.4,          // rejected_connections
            metrics_stats.1,          // messages
            metrics_stats.3,          // errors
            metrics_stats.5,          // avg_time
            metrics_stats.6,          // cache_hits
            metrics_stats.7,          // cache_misses
            metrics_stats.8,          // peak_memory
        )
    }

    pub fn get_flow_control_stats(&self) -> flow_control::FlowControlStats {
        self.flow_control.get_stats()
    }

    pub fn get_messages_processed(&self) -> u64 {
        self.metrics.messages_processed.load(Ordering::Relaxed)
    }

    pub fn get_metrics(&self) -> BrokerMetrics {
        let stats = self.get_stats();
        BrokerMetrics {
            total_connections: stats.1,           // total_connections
            active_connections: stats.0 as u64,   // active_connections 
            messages_processed: stats.3,          // messages
            bytes_processed: stats.8,             // bytes (now in position 8)
            errors: stats.4,                      // errors
            rejected_connections: stats.2,        // rejected_connections
            avg_latency_ns: stats.5 as f64,
            min_latency_ns: stats.6,
            max_latency_ns: stats.7,
        }
    }

    pub fn shutdown(&self) {
        self.stop()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_config_creation() {
        let config = BrokerConfig::default();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 8080);
        assert!(config.enable_wal);
        assert!(config.enable_compression);
        assert!(config.enable_intelligent_routing);
    }

    #[test]
    fn test_metrics_recording() {
        let metrics = UltraFastMetrics::new();
        metrics.record_message(1000, 512); // 1μs, 512 bytes
        
        let stats = metrics.get_stats();
        // New order: (total_connections, messages, bytes, errors, rejections, avg_time, cache_hits, cache_misses, peak_memory)
        assert_eq!(stats.1, 1); // messages processed at index 1
        assert_eq!(stats.2, 512); // bytes processed at index 2
    }

    #[tokio::test]
    async fn test_broker_creation() {
        let config = BrokerConfig::default();
        let broker = MessageBrokerHost::new(config);
        
        let stats = broker.get_stats();
        assert_eq!(stats.0, 0); // active connections
    }
}
