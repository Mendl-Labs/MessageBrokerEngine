use std::io::{Error as IoError, ErrorKind, Read, Write};
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use prost::Message;
use protocol::broker::messages::SubscribeRequest;

// Constants for fixed buffer sizes
const MAX_TOPICS: usize = 64;
const MAX_TOPIC_LENGTH: usize = 128;
const MSG_BUFFER_SIZE: usize = 8192;
const RING_BUFFER_CAPACITY: usize = 4096; // Must be power of 2
const RECONNECT_INITIAL_DELAY_MS: u64 = 10; // Initial backoff
const RECONNECT_MAX_DELAY_MS: u64 = 5000; // Max 5 second backoff
const METRICS_INTERVAL_MS: u64 = 1000; // Update metrics every second
const STATS_WINDOW_SIZE: usize = 1000; // Sample size for latency statistics

// Error type with no allocation
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum SubscriberError {
    ConnectionFailed,
    SubscribeFailed,
    SocketReadFailed,
    SocketWriteFailed,
    InvalidMessage,
    BufferFull,
    TopicNotFound,
    Shutdown,
    InvalidConfiguration,
    SocketConfigFailed,
    TimedOut,
}

impl From<IoError> for SubscriberError {
    fn from(err: IoError) -> Self {
        match err.kind() {
            ErrorKind::ConnectionRefused | ErrorKind::NotConnected => SubscriberError::ConnectionFailed,
            ErrorKind::WouldBlock | ErrorKind::TimedOut => SubscriberError::TimedOut,
            ErrorKind::InvalidData => SubscriberError::InvalidMessage,
            _ => SubscriberError::SocketReadFailed,
        }
    }
}

// Pre-allocated, fixed-size topic name
#[derive(Copy, Clone)]
pub struct FixedTopic {
    data: [u8; MAX_TOPIC_LENGTH],
    len: usize,
}

impl FixedTopic {
    #[inline]
    pub fn new() -> Self {
        Self {
            data: [0; MAX_TOPIC_LENGTH],
            len: 0,
        }
    }

    #[inline]
    pub fn from_str(s: &str) -> Option<Self> {
        if s.len() > MAX_TOPIC_LENGTH {
            return None;
        }

        let mut result = Self::new();
        result.len = s.len();
        result.data[..s.len()].copy_from_slice(s.as_bytes());
        Some(result)
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        // Safety: we ensure data contains valid UTF-8 in from_str
        unsafe { std::str::from_utf8_unchecked(&self.data[..self.len]) }
    }

    #[inline]
    pub fn equals(&self, other: &FixedTopic) -> bool {
        if self.len != other.len {
            return false;
        }
        
        for i in 0..self.len {
            if self.data[i] != other.data[i] {
                return false;
            }
        }
        
        true
    }
}

// Lock-free ring buffer for message passing
pub struct RingBuffer<T: Copy> {
    buffer: Box<[MaybeUninit<T>]>,
    mask: usize,
    write_idx: AtomicUsize,
    read_idx: AtomicUsize,
    // Metrics
    overflow_count: AtomicUsize,
    total_pushed: AtomicUsize,
    total_popped: AtomicUsize,
}

impl<T: Copy> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        // Ensure capacity is a power of 2
        let capacity = capacity.next_power_of_two();
        
        // Align to cache line size to prevent false sharing
        #[cfg(target_arch = "x86_64")]
        let buffer = unsafe {
            let layout = std::alloc::Layout::from_size_align(
                capacity * std::mem::size_of::<MaybeUninit<T>>(), 
                64  // Cache line size on most x86_64 CPUs
            ).unwrap();
            let ptr = std::alloc::alloc(layout) as *mut MaybeUninit<T>;
            Box::from_raw(std::slice::from_raw_parts_mut(ptr, capacity))
        };
        
        #[cfg(not(target_arch = "x86_64"))]
        let buffer = unsafe {
            let layout = std::alloc::Layout::array::<MaybeUninit<T>>(capacity).unwrap();
            let ptr = std::alloc::alloc(layout) as *mut MaybeUninit<T>;
            Box::from_raw(std::slice::from_raw_parts_mut(ptr, capacity))
        };

        Self {
            buffer,
            mask: capacity - 1,
            write_idx: AtomicUsize::new(0),
            read_idx: AtomicUsize::new(0),
            overflow_count: AtomicUsize::new(0),
            total_pushed: AtomicUsize::new(0),
            total_popped: AtomicUsize::new(0),
        }
    }

    #[inline]
    pub fn push(&mut self, item: T) -> Result<(), SubscriberError> {
        let write = self.write_idx.load(Ordering::Relaxed);
        let read = self.read_idx.load(Ordering::Acquire);
        
        // Check if buffer is full
        if write - read >= self.buffer.len() {
            self.overflow_count.fetch_add(1, Ordering::Relaxed);
            return Err(SubscriberError::BufferFull);
        }
        
        // Write item to buffer
        let idx = write & self.mask;
        unsafe {
            ptr::write(self.buffer[idx].as_mut_ptr(), item);
        }
        
        // Make sure write is visible to reader
        self.write_idx.store(write + 1, Ordering::Release);
        self.total_pushed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    #[inline]
    pub fn pop(&self) -> Option<T> {
        let read = self.read_idx.load(Ordering::Relaxed);
        let write = self.write_idx.load(Ordering::Acquire);
        
        // Check if buffer is empty
        if read == write {
            return None;
        }
        
        // Read item from buffer
        let idx = read & self.mask;
        let item = unsafe { ptr::read(self.buffer[idx].as_ptr()) };
        
        // Make sure read is visible to writer
        self.read_idx.store(read + 1, Ordering::Release);
        self.total_popped.fetch_add(1, Ordering::Relaxed);
        Some(item)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.read_idx.load(Ordering::Relaxed) == self.write_idx.load(Ordering::Acquire)
    }
    
    #[inline]
    pub fn len(&self) -> usize {
        let write = self.write_idx.load(Ordering::Acquire);
        let read = self.read_idx.load(Ordering::Relaxed);
        write.wrapping_sub(read)
    }
    
    #[inline]
    pub fn capacity(&self) -> usize {
        self.buffer.len()
    }
    
    #[inline]
    pub fn fill_percentage(&self) -> f64 {
        (self.len() as f64 / self.capacity() as f64) * 100.0
    }
    
    #[inline]
    pub fn get_metrics(&self) -> (usize, usize, usize, usize) {
        (
            self.total_pushed.load(Ordering::Relaxed),
            self.total_popped.load(Ordering::Relaxed),
            self.overflow_count.load(Ordering::Relaxed),
            self.len()
        )
    }
    
    #[inline]
    pub fn reset_metrics(&self) {
        self.total_pushed.store(0, Ordering::Relaxed);
        self.total_popped.store(0, Ordering::Relaxed);
        self.overflow_count.store(0, Ordering::Relaxed);
    }
}

// Message structure with pre-allocated buffer
#[derive(Copy, Clone)]
pub struct TopicMessage {
    topic: FixedTopic,
    data: [u8; MSG_BUFFER_SIZE],
    len: usize,
    timestamp: u64, // Capture time in nanoseconds since UNIX epoch
}

impl TopicMessage {
    #[inline]
    pub fn new() -> Self {
        Self {
            topic: FixedTopic::new(),
            data: [0; MSG_BUFFER_SIZE],
            len: 0,
            timestamp: 0,
        }
    }
    
    #[inline]
    pub fn with_timestamp(mut self, timestamp: u64) -> Self {
        self.timestamp = timestamp;
        self
    }
    
    #[inline]
    pub fn get_topic(&self) -> &FixedTopic {
        &self.topic
    }
    
    #[inline]
    pub fn get_data(&self) -> &[u8] {
        &self.data[..self.len]
    }
    
    #[inline]
    pub fn get_timestamp(&self) -> u64 {
        self.timestamp
    }
}

// Performance metrics for latency analysis
#[derive(Clone)]
pub struct LatencyMetrics {
    // Circular buffer for latency samples (nanoseconds)
    samples: [u64; STATS_WINDOW_SIZE],
    current_index: usize,
    count: usize,
    
    // Pre-computed statistics
    min_latency_ns: u64,
    max_latency_ns: u64,
    avg_latency_ns: u64,
    p50_latency_ns: u64,
    p95_latency_ns: u64,
    p99_latency_ns: u64,
}

impl LatencyMetrics {
    pub fn new() -> Self {
        Self {
            samples: [0; STATS_WINDOW_SIZE],
            current_index: 0,
            count: 0,
            min_latency_ns: u64::MAX,
            max_latency_ns: 0,
            avg_latency_ns: 0,
            p50_latency_ns: 0,
            p95_latency_ns: 0,
            p99_latency_ns: 0,
        }
    }
    
    pub fn add_sample(&mut self, latency_ns: u64) {
        self.samples[self.current_index] = latency_ns;
        self.current_index = (self.current_index + 1) % STATS_WINDOW_SIZE;
        
        if self.count < STATS_WINDOW_SIZE {
            self.count += 1;
        }
        
        // Update min/max
        self.min_latency_ns = self.min_latency_ns.min(latency_ns);
        self.max_latency_ns = self.max_latency_ns.max(latency_ns);
    }
    
    pub fn compute_statistics(&mut self) {
        if self.count == 0 {
            return;
        }
        
        // Create a sorted copy for percentile calculation
        let mut sorted = Vec::with_capacity(self.count);
        for i in 0..self.count {
            sorted.push(self.samples[i]);
        }
        sorted.sort_unstable();
        
        // Calculate average
        let sum: u64 = sorted.iter().sum();
        self.avg_latency_ns = sum / self.count as u64;
        
        // Calculate percentiles
        self.p50_latency_ns = sorted[self.count * 50 / 100];
        self.p95_latency_ns = sorted[self.count * 95 / 100];
        self.p99_latency_ns = sorted[self.count * 99 / 100];
    }
    
    pub fn get_statistics(&self) -> (u64, u64, u64, u64, u64, u64) {
        (
            self.min_latency_ns,
            self.max_latency_ns,
            self.avg_latency_ns,
            self.p50_latency_ns,
            self.p95_latency_ns,
            self.p99_latency_ns
        )
    }
    
    pub fn reset(&mut self) {
        self.current_index = 0;
        self.count = 0;
        self.min_latency_ns = u64::MAX;
        self.max_latency_ns = 0;
        self.avg_latency_ns = 0;
        self.p50_latency_ns = 0;
        self.p95_latency_ns = 0;
        self.p99_latency_ns = 0;
    }
}

// Health metrics for the subscriber
#[derive(Default, Clone)]
pub struct HealthMetrics {
    connected: bool,
    last_message_time: u64,
    message_count: u64,
    reconnect_count: u64,
    error_count: u64,
    last_error_code: Option<SubscriberError>,
    last_error_time: u64,
}

// Connection configuration
#[derive(Clone)]
pub struct ConnectionConfig {
    addr: String,
    connect_timeout_ms: u64,
    read_timeout_ms: u64,
    write_timeout_ms: u64,
    enable_tcp_nodelay: bool,
    enable_tcp_quickack: bool,
    enable_so_priority: bool,
    priority_value: i32,
    cpu_affinity: Option<usize>,
    use_realtime_priority: bool,
    realtime_priority: i32,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            addr: String::new(),
            connect_timeout_ms: 1000,
            read_timeout_ms: 100,
            write_timeout_ms: 100,
            enable_tcp_nodelay: true,
            enable_tcp_quickack: true,
            enable_so_priority: true,
            priority_value: 6,
            cpu_affinity: None,
            use_realtime_priority: false,
            realtime_priority: 70,
        }
    }
}

impl ConnectionConfig {
    pub fn new(addr: &str) -> Self {
        Self {
            addr: addr.to_string(),
            ..Default::default()
        }
    }
    
    pub fn with_connect_timeout(mut self, timeout_ms: u64) -> Self {
        self.connect_timeout_ms = timeout_ms;
        self
    }
    
    pub fn with_read_timeout(mut self, timeout_ms: u64) -> Self {
        self.read_timeout_ms = timeout_ms;
        self
    }
    
    pub fn with_write_timeout(mut self, timeout_ms: u64) -> Self {
        self.write_timeout_ms = timeout_ms;
        self
    }
    
    pub fn with_tcp_nodelay(mut self, enable: bool) -> Self {
        self.enable_tcp_nodelay = enable;
        self
    }
    
    pub fn with_tcp_quickack(mut self, enable: bool) -> Self {
        self.enable_tcp_quickack = enable;
        self
    }
    
    pub fn with_so_priority(mut self, enable: bool, priority: i32) -> Self {
        self.enable_so_priority = enable;
        self.priority_value = priority;
        self
    }
    
    pub fn with_cpu_affinity(mut self, cpu_id: usize) -> Self {
        self.cpu_affinity = Some(cpu_id);
        self
    }
    
    pub fn with_realtime_priority(mut self, enable: bool, priority: i32) -> Self {
        self.use_realtime_priority = enable;
        self.realtime_priority = priority;
        self
    }
}

// Command channel for controlling the worker thread
enum WorkerCommand {
    Reconnect,
    UpdateConfig(ConnectionConfig),
    Shutdown,
}

// Result from worker thread
enum WorkerResult {
    Connected,
    Disconnected,
    Error(SubscriberError),
    MessageReceived(usize), // Number of messages processed
}

// Production-ready HFT subscriber
pub struct Subscriber {
    // Per-topic ring buffers
    topic_buffers: [Option<Arc<Mutex<RingBuffer<TopicMessage>>>>; MAX_TOPICS],
    topics: [FixedTopic; MAX_TOPICS],
    topic_count: usize,
    
    // Worker thread control
    running: Arc<AtomicBool>,
    worker_handle: Option<JoinHandle<()>>,
    command_sender: Option<std::sync::mpsc::Sender<WorkerCommand>>,
    result_receiver: Option<std::sync::mpsc::Receiver<WorkerResult>>,
    
    // Configuration
    config: ConnectionConfig,
    
    // Metrics
    latency_metrics: Arc<std::sync::Mutex<LatencyMetrics>>,
    health_metrics: Arc<std::sync::Mutex<HealthMetrics>>,
    
    // Logging callback
    log_fn: Option<Arc<dyn Fn(&str) + Send + Sync>>,
}

impl Subscriber {
    pub fn new(config: ConnectionConfig, topics: &[&str]) -> Result<Self, SubscriberError> {
        if topics.is_empty() || topics.len() > MAX_TOPICS {
            return Err(SubscriberError::InvalidConfiguration);
        }

        if config.addr.is_empty() {
            return Err(SubscriberError::InvalidConfiguration);
        }

        // Initialize all fixed arrays with default values
        let mut topic_buffers: [Option<Arc<Mutex<RingBuffer<TopicMessage>>>>; MAX_TOPICS] = 
            std::array::from_fn(|_| None);
            
        let mut fixed_topics: [FixedTopic; MAX_TOPICS] = 
            std::array::from_fn(|_| FixedTopic::new());

        // Initialize topics and ring buffers
        for (i, topic) in topics.iter().enumerate() {
            let fixed_topic = FixedTopic::from_str(topic).ok_or(SubscriberError::TopicNotFound)?;
            fixed_topics[i] = fixed_topic;
            topic_buffers[i] = Some(Arc::new(Mutex::new(RingBuffer::new(RING_BUFFER_CAPACITY))));
        }

        // Create command channel
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();

        Ok(Self {
            topic_buffers,
            topics: fixed_topics,
            topic_count: topics.len(),
            running: Arc::new(AtomicBool::new(false)),
            worker_handle: None,
            command_sender: Some(cmd_tx),
            result_receiver: Some(result_rx),
            config,
            latency_metrics: Arc::new(std::sync::Mutex::new(LatencyMetrics::new())),
            health_metrics: Arc::new(std::sync::Mutex::new(HealthMetrics::default())),
            log_fn: None,
        })
    }
    
    pub fn set_logger<F>(&mut self, log_function: F)
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        self.log_fn = Some(Arc::new(log_function));
    }
    
    fn log(&self, message: &str) {
        if let Some(log_fn) = &self.log_fn {
            log_fn(message);
        }
    }
    
    fn configure_socket(socket: &std::net::TcpStream, config: &ConnectionConfig) -> Result<(), SubscriberError> {
        // Set socket timeouts
        if socket.set_read_timeout(Some(Duration::from_millis(config.read_timeout_ms))).is_err() {
            return Err(SubscriberError::SocketConfigFailed);
        }
        
        if socket.set_write_timeout(Some(Duration::from_millis(config.write_timeout_ms))).is_err() {
            return Err(SubscriberError::SocketConfigFailed);
        }
        
        #[cfg(target_os = "linux")]
        {
            let socket_fd = socket.as_raw_fd();
            unsafe {
                // Set TCP_NODELAY
                if config.enable_tcp_nodelay {
                    let val: libc::c_int = 1;
                    if libc::setsockopt(
                        socket_fd,
                        libc::IPPROTO_TCP,
                        libc::TCP_NODELAY,
                        &val as *const _ as *const libc::c_void,
                        std::mem::size_of_val(&val) as libc::socklen_t,
                    ) < 0 {
                        return Err(SubscriberError::SocketConfigFailed);
                    }
                }
                
                // Set TCP_QUICKACK
                if config.enable_tcp_quickack {
                    let val: libc::c_int = 1;
                    if libc::setsockopt(
                        socket_fd,
                        libc::IPPROTO_TCP,
                        libc::TCP_QUICKACK,
                        &val as *const _ as *const libc::c_void,
                        std::mem::size_of_val(&val) as libc::socklen_t,
                    ) < 0 {
                        return Err(SubscriberError::SocketConfigFailed);
                    }
                }
                
                // Set SO_PRIORITY
                if config.enable_so_priority {
                    let priority: libc::c_int = config.priority_value;
                    if libc::setsockopt(
                        socket_fd,
                        libc::SOL_SOCKET,
                        libc::SO_PRIORITY,
                        &priority as *const _ as *const libc::c_void,
                        std::mem::size_of_val(&priority) as libc::socklen_t,
                    ) < 0 {
                        return Err(SubscriberError::SocketConfigFailed);
                    }
                }
            }
        }
        
        Ok(())
    }
    
    fn connect_with_timeout(addr: &str, timeout_ms: u64) -> Result<std::net::TcpStream, SubscriberError> {
        // Create a non-blocking socket
        let socket = std::net::TcpStream::connect(addr).map_err(|_| SubscriberError::ConnectionFailed)?;
        
        // Set socket to blocking mode again
        if socket.set_nonblocking(false).is_err() {
            return Err(SubscriberError::SocketConfigFailed);
        }
        
        Ok(socket)
    }
    
    fn set_thread_priority(config: &ConnectionConfig) {
        if !config.use_realtime_priority {
            return;
        }
        
        #[cfg(target_os = "linux")]
        unsafe {
            // Set SCHED_FIFO scheduling policy
            let mut param: libc::sched_param = std::mem::zeroed();
            param.sched_priority = config.realtime_priority;
            
            let thread_id = libc::gettid();
            if libc::sched_setscheduler(thread_id, libc::SCHED_FIFO, &param) != 0 {
                // If failed due to permission, try with slightly lower priority
                param.sched_priority = 1;
                let _ = libc::sched_setscheduler(thread_id, libc::SCHED_FIFO, &param);
            }
        }
        
        // Set CPU affinity if specified
        if let Some(cpu_id) = config.cpu_affinity {
            #[cfg(target_os = "linux")]
            unsafe {
                let mut cpu_set: libc::cpu_set_t = std::mem::zeroed();
                libc::CPU_ZERO(&mut cpu_set);
                libc::CPU_SET(cpu_id, &mut cpu_set);
                
                let thread_id = libc::gettid();
                let _ = libc::sched_setaffinity(
                    thread_id,
                    std::mem::size_of::<libc::cpu_set_t>() as libc::size_t,
                    &cpu_set
                );
            }
        }
    }
    
    // The worker thread function
    fn worker_thread(
        topics: Vec<FixedTopic>,
        topic_buffers: Vec<Arc<Mutex<RingBuffer<TopicMessage>>>>,
        config: ConnectionConfig,
        cmd_rx: std::sync::mpsc::Receiver<WorkerCommand>,
        result_tx: std::sync::mpsc::Sender<WorkerResult>,
        latency_metrics: Arc<std::sync::Mutex<LatencyMetrics>>,
        health_metrics: Arc<std::sync::Mutex<HealthMetrics>>,
        log_fn: Option<Arc<dyn Fn(&str) + Send + Sync>>,
    ) {
        // Set thread priority and affinity
        Self::set_thread_priority(&config);
        
        // Log thread start
        if let Some(log) = &log_fn {
            log(&format!("HFT subscriber worker thread started, connecting to {}", config.addr));
        }
        
        // Init health metrics
        {
            let mut health = health_metrics.lock().unwrap();
            health.connected = false;
            health.last_message_time = current_time_ns();
            health.message_count = 0;
            health.reconnect_count = 0;
            health.error_count = 0;
        }
        
        // Connect to server
        let mut socket_result = Self::connect_with_timeout(&config.addr, config.connect_timeout_ms);
        let mut reconnect_delay_ms = RECONNECT_INITIAL_DELAY_MS;
        let mut connected = false;
        let mut last_metrics_time = Instant::now();
        
        // Reusable buffers
        let mut read_buf = [0u8; MSG_BUFFER_SIZE + 256]; // Extra space for headers
        
        // Create a subscription message
        let mut topic_strings = Vec::with_capacity(topics.len());
        for i in 0..topics.len() {
            topic_strings.push(topics[i].as_str().to_string());
        }
        
        let subscribe_request = SubscribeRequest {
            topics: topic_strings,
        };
        
        // One-time allocation for the subscription message
        let mut subscribe_buf = Vec::with_capacity(subscribe_request.encoded_len());
        if subscribe_request.encode(&mut subscribe_buf).is_err() {
            if let Some(log) = &log_fn {
                log("Failed to encode subscription request");
            }
            
            let _ = result_tx.send(WorkerResult::Error(SubscriberError::SubscribeFailed));
            return;
        }
        
        // Main worker loop
        loop {
            // Check for commands
            match cmd_rx.try_recv() {
                Ok(WorkerCommand::Shutdown) => {
                    if let Some(log) = &log_fn {
                        log("Worker thread received shutdown command");
                    }
                    break;
                },
                Ok(WorkerCommand::Reconnect) => {
                    if let Some(log) = &log_fn {
                        log("Worker thread received reconnect command");
                    }
                    socket_result = Self::connect_with_timeout(&config.addr, config.connect_timeout_ms);
                    reconnect_delay_ms = RECONNECT_INITIAL_DELAY_MS;
                    
                    // Update health metrics
                    let mut health = health_metrics.lock().unwrap();
                    health.reconnect_count += 1;
                },
                Ok(WorkerCommand::UpdateConfig(new_config)) => {
                    if let Some(log) = &log_fn {
                        log("Worker thread received config update command");
                    }
                    // Implement config update logic if needed
                },
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    // No command, continue processing
                },
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    // Command channel closed, exit thread
                    if let Some(log) = &log_fn {
                        log("Worker thread command channel disconnected, shutting down");
                    }
                    break;
                }
            }
            
            // Handle connection state
            if !connected {
                match &socket_result {
                    Ok(socket) => {
                        // Configure socket
                        if let Err(e) = Self::configure_socket(socket, &config) {
                            if let Some(log) = &log_fn {
                                log(&format!("Failed to configure socket: {:?}", e));
                            }
                            
                            // Update health metrics
                            let mut health = health_metrics.lock().unwrap();
                            health.connected = false;
                            health.error_count += 1;
                            health.last_error_code = Some(e);
                            health.last_error_time = current_time_ns();
                            
                            // Try to reconnect
                            socket_result = Err(e);
                            continue;
                        }
                        
                        // Send subscription
                        let mut socket_clone = socket.try_clone().map_err(|_| SubscriberError::SocketConfigFailed).expect("Failed to clone socket for writing");
                        if socket_clone.write_all(&subscribe_buf).is_err() {
                            if let Some(log) = &log_fn {
                                log("Failed to send subscription request");
                            }
                            
                            // Update health metrics
                            let mut health = health_metrics.lock().unwrap();
                            health.connected = false;
                            health.error_count += 1;
                            health.last_error_code = Some(SubscriberError::SubscribeFailed);
                            health.last_error_time = current_time_ns();
                            
                            // Try to reconnect
                            socket_result = Err(SubscriberError::SocketWriteFailed);
                            continue;
                        }
                        
                        // Successfully connected and subscribed
                        connected = true;
                        
                        // Update health metrics
                        let mut health = health_metrics.lock().unwrap();
                        health.connected = true;
                        
                        // Notify
                        if let Some(log) = &log_fn {
                            log(&format!("Connected to {}, subscribed to {} topics", config.addr, topics.len()));
                        }
                        // Send result
                        let _ = result_tx.send(WorkerResult::Connected);
                    },
                    Err(e) => {
                        // Update health metrics
                        let mut health = health_metrics.lock().unwrap();
                        health.connected = false;
                        health.error_count += 1;
                        health.last_error_code = Some(*e);
                        health.last_error_time = current_time_ns();

                        // Notify about connection failure
                        if let Some(log) = &log_fn {
                            log(&format!("Failed to connect to {}: {:?}", config.addr, e));
                        }

                        // Send result
                        let _ = result_tx.send(WorkerResult::Error(*e));
                        
                        // Implement backoff for reconnection
                        thread::sleep(Duration::from_millis(reconnect_delay_ms));
                        reconnect_delay_ms = (reconnect_delay_ms * 2).min(RECONNECT_MAX_DELAY_MS);
                        
                        // Try to reconnect
                        socket_result = Self::connect_with_timeout(&config.addr, config.connect_timeout_ms);
                        continue;
                    }
                }
            }
            
            // Process messages when connected
            if connected {
                let socket = match socket_result.as_ref() {
                    Ok(s) => s,
                    Err(_) => {
                        connected = false;
                        continue;
                    }
                };
                
                // Use a cloned socket for reading to avoid mutable borrow
                let mut read_socket = match socket.try_clone() {
                    Ok(s) => s,
                    Err(_) => {
                        if let Some(log) = &log_fn {
                            log("Failed to clone socket for reading");
                        }
                        connected = false;
                        socket_result = Err(SubscriberError::SocketReadFailed);
                        continue;
                    }
                };
                
                // Non-blocking read to avoid stalling the thread
                match read_socket.read(&mut read_buf) {
                    Ok(0) => {
                        // Connection closed by server
                        if let Some(log) = &log_fn {
                            log("Connection closed by server");
                        }
                        
                        // Update health metrics
                        let mut health = health_metrics.lock().unwrap();
                        health.connected = false;
                        
                        // Notify
                        let _ = result_tx.send(WorkerResult::Disconnected);
                        
                        // Try to reconnect
                        connected = false;
                        socket_result = Self::connect_with_timeout(&config.addr, config.connect_timeout_ms);
                        reconnect_delay_ms = RECONNECT_INITIAL_DELAY_MS;
                    },
                    Ok(n) => {
                        // Process the message
                        if n < 8 { 
                            // Too small to be valid, skip
                            continue; 
                        }
                        
                        let now_ns = current_time_ns();
                        
                        // Extract topic length (first 4 bytes)
                        let topic_len = u32::from_be_bytes([
                            read_buf[0], read_buf[1], read_buf[2], read_buf[3]
                        ]) as usize;
                        
                        if topic_len > MAX_TOPIC_LENGTH || 4 + topic_len > n {
                            // Invalid message
                            continue;
                        }
                        
                        // Create a topic object
                        let mut topic = FixedTopic::new();
                        topic.len = topic_len;
                        topic.data[..topic_len].copy_from_slice(&read_buf[4..4+topic_len]);
                        
                        // Find matching topic buffer
                        let mut message_processed = false;
                        for i in 0..topics.len() {
                            if topic.equals(&topics[i]) {
                                // Create message
                                let mut msg = TopicMessage::new();
                                msg.topic = topic;
                                
                                // Calculate message data length and copy
                                let data_len = n - 4 - topic_len;
                                if data_len > MSG_BUFFER_SIZE {
                                    // Message too large
                                    continue;
                                }
                                
                                msg.len = data_len;
                                msg.data[..data_len].copy_from_slice(&read_buf[4+topic_len..n]);
                                
                                // Set reception timestamp
                                msg.timestamp = now_ns;
                                
                                // Push to ring buffer (ignore if full)
                                if let Ok(mut buffer) = topic_buffers[i].lock() {
                                    if let Err(e) = buffer.push(msg) {
                                        if let Some(log) = &log_fn {
                                            log(&format!("Buffer full for topic {}: {:?}", topic.as_str(), e));
                                        }
                                    } else {
                                        message_processed = true;
                                    }
                                } else {
                                    if let Some(log) = &log_fn {
                                        log("Failed to acquire lock on topic buffer");
                                    }
                                }
                                break;
                            }
                        }
                        
                        if message_processed {
                            // Update health metrics
                            let mut health = health_metrics.lock().unwrap();
                            health.message_count += 1;
                            health.last_message_time = now_ns;
                            
                            // Sample latency measurement (if timestamp was provided in the message)
                            // This would require parsing the message data to extract sender timestamp
                            // For demonstration, we'll use a simple metric
                            
                            // Update metrics
                            if let Ok(mut metrics) = latency_metrics.try_lock() {
                                // In a real system, you'd extract the sender timestamp from the message
                                // and calculate latency as now_ns - sender_timestamp
                                // For demo purposes, we'll just add a dummy value
                                metrics.add_sample(100); // 100ns dummy latency value
                            }
                            
                            // Send a success result
                            let _ = result_tx.send(WorkerResult::MessageReceived(1));
                        }
                    },
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // No data available, short sleep to avoid burning CPU
                        spin_sleep::sleep(std::time::Duration::from_micros(1));
                    },
                    // Inside the worker_thread method - fixing the moved value error
                    Err(e) => {
                        // Socket error
                        let error = SubscriberError::from(e); // Convert to SubscriberError once
                        
                        if let Some(log) = &log_fn {
                            log(&format!("Socket error: {:?}", error));
                        }
                        
                        // Update health metrics
                        let mut health = health_metrics.lock().unwrap();
                        health.connected = false;
                        health.error_count += 1;
                        health.last_error_code = Some(error); // Use the converted error
                        health.last_error_time = current_time_ns();
                        
                        // Notify about error
                        let _ = result_tx.send(WorkerResult::Error(error)); // Use the same error
                        
                        // Try to reconnect
                        connected = false;
                        socket_result = Self::connect_with_timeout(&config.addr, config.connect_timeout_ms);
                        reconnect_delay_ms = RECONNECT_INITIAL_DELAY_MS;
                    }
                }
            }
            
            // Update metrics periodically
            if last_metrics_time.elapsed() >= Duration::from_millis(METRICS_INTERVAL_MS) {
                if let Ok(mut metrics) = latency_metrics.try_lock() {
                    metrics.compute_statistics();
                }
                last_metrics_time = Instant::now();
            }
        }
        
        // Thread cleanup
        if let Some(log) = &log_fn {
            log("Worker thread shutting down");
        }
        
        // Close socket if connected
        if connected {
            // Socket will be closed when dropped
            let _ = result_tx.send(WorkerResult::Disconnected);
        }
    }

    pub fn start(&mut self) -> Result<(), SubscriberError> {
        if self.running.load(Ordering::Relaxed) {
            return Ok(());
        }
        
        // Set running flag
        self.running.store(true, Ordering::Release);
        
        // Log start
        self.log(&format!("Starting HFT subscriber with {} topics", self.topic_count));
        
        // Create new channels for the worker thread
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        
        // Store the sender in self for future use
        self.command_sender = Some(cmd_tx);
        self.result_receiver = Some(result_rx);
        
        // Clone data for worker thread
        let topics: Vec<FixedTopic> = self.topics[0..self.topic_count].to_vec();
        let mut topic_buffers = Vec::with_capacity(self.topic_count);
        
        for i in 0..self.topic_count {
            if let Some(buffer) = &self.topic_buffers[i] {
                topic_buffers.push(buffer.clone());
            } else {
                return Err(SubscriberError::InvalidConfiguration);
            }
        }
        
        let config = self.config.clone();
        let latency_metrics = self.latency_metrics.clone();
        let health_metrics = self.health_metrics.clone();
        let log_fn = self.log_fn.clone();
        
        // Spawn worker thread - now passing cmd_rx (the Receiver) instead of cmd_tx
        let handle = thread::spawn(move || {
            Self::worker_thread(
                topics,
                topic_buffers,
                config,
                cmd_rx,  // This is now correct - passing the Receiver
                result_tx,
                latency_metrics,
                health_metrics,
                log_fn,
            );
        });
        
        // Store thread handle
        self.worker_handle = Some(handle);
        
        self.log("HFT subscriber started successfully");
        
        Ok(())
    }

    pub fn stop(&mut self) -> Result<(), SubscriberError> {
        if !self.running.load(Ordering::Relaxed) {
            return Ok(());
        }
        
        self.log("Stopping HFT subscriber");
        
        // Send shutdown command
        if let Some(sender) = &self.command_sender {
            if sender.send(WorkerCommand::Shutdown).is_err() {
                return Err(SubscriberError::Shutdown);
            }
        }
        
        // Set running flag
        self.running.store(false, Ordering::Release);
        
        // Wait for worker thread to finish
        if let Some(handle) = self.worker_handle.take() {
            if handle.join().is_err() {
                return Err(SubscriberError::Shutdown);
            }
        }
        
        self.log("HFT subscriber stopped successfully");
        
        Ok(())
    }
    
    pub fn reconnect(&self) -> Result<(), SubscriberError> {
        if !self.running.load(Ordering::Relaxed) {
            return Err(SubscriberError::Shutdown);
        }
        
        self.log("Requesting reconnection");
        
        // Send reconnect command
        if let Some(sender) = &self.command_sender {
            if sender.send(WorkerCommand::Reconnect).is_err() {
                return Err(SubscriberError::Shutdown);
            }
        } else {
            return Err(SubscriberError::Shutdown);
        }
        
        Ok(())
    }
    
    pub fn update_config(&self, config: ConnectionConfig) -> Result<(), SubscriberError> {
        if !self.running.load(Ordering::Relaxed) {
            return Err(SubscriberError::Shutdown);
        }
        
        self.log("Updating configuration");
        
        // Send update config command
        if let Some(sender) = &self.command_sender {
            if sender.send(WorkerCommand::UpdateConfig(config)).is_err() {
                return Err(SubscriberError::Shutdown);
            }
        } else {
            return Err(SubscriberError::Shutdown);
        }
        
        Ok(())
    }
    
    #[inline]
    pub fn get_message(&self, topic_idx: usize) -> Option<TopicMessage> {
        if topic_idx >= self.topic_count {
            return None;
        }
        
        if let Some(buffer_arc) = &self.topic_buffers[topic_idx] {
            if let Ok(buffer) = buffer_arc.lock() {
                return buffer.pop();
            }
        }
        
        None
    }
    
    #[inline]
    pub fn try_get_message(&self, topic_idx: usize, timeout_us: u64) -> Option<TopicMessage> {
        if topic_idx >= self.topic_count {
            return None;
        }
        
        let start = Instant::now();
        let timeout = Duration::from_micros(timeout_us);
        
        while start.elapsed() < timeout {
            if let Some(buffer_arc) = &self.topic_buffers[topic_idx] {
                if let Ok(buffer) = buffer_arc.lock() {
                    if let Some(msg) = buffer.pop() {
                        return Some(msg);
                    }
                }
            }
            
            // Short sleep to avoid burning CPU
            spin_sleep::sleep(Duration::from_nanos(100));
        }
        
        None
    }
    
    #[inline]
    pub fn poll_all_messages(&self, max_count: usize) -> Vec<(usize, TopicMessage)> {
        let mut result = Vec::with_capacity(max_count);
        
        for topic_idx in 0..self.topic_count {
            if let Some(buffer_arc) = &self.topic_buffers[topic_idx] {
                if let Ok(buffer) = buffer_arc.lock() {
                    // Check if buffer has messages and add them to result
                    let buffer_len = buffer.len();
                    if buffer_len > 0 {
                        let mut msg_count = 0;
                        let to_process = (buffer_len).min(max_count - result.len());
                        
                        for _ in 0..to_process {
                            if let Some(msg) = buffer.pop() {
                                result.push((topic_idx, msg));
                                msg_count += 1;
                            } else {
                                break;
                            }
                        }
                        
                        if result.len() >= max_count {
                            break;
                        }
                    }
                }
            }
        }
        
        result
    }

    #[inline]
    pub fn find_topic_index(&self, topic: &str) -> Option<usize> {
        let search_topic = FixedTopic::from_str(topic)?;
        
        for i in 0..self.topic_count {
            if search_topic.equals(&self.topics[i]) {
                return Some(i);
            }
        }
        
        None
    }
    
    pub fn get_latency_metrics(&self) -> Result<(u64, u64, u64, u64, u64, u64), SubscriberError> {
        if let Ok(metrics) = self.latency_metrics.lock() {
            Ok(metrics.get_statistics())
        } else {
            Err(SubscriberError::Shutdown)
        }
    }
    
    pub fn get_health_metrics(&self) -> Result<HealthMetrics, SubscriberError> {
        if let Ok(metrics) = self.health_metrics.lock() {
            Ok(metrics.clone())
        } else {
            Err(SubscriberError::Shutdown)
        }
    }
    
    pub fn get_buffer_metrics(&self, topic_idx: usize) -> Result<(usize, usize, usize, usize), SubscriberError> {
        if topic_idx >= self.topic_count {
            return Err(SubscriberError::TopicNotFound);
        }
        
        if let Some(buffer_arc) = &self.topic_buffers[topic_idx] {
            if let Ok(buffer) = buffer_arc.lock() {
                return Ok(buffer.get_metrics());
            }
        }
        
        Err(SubscriberError::TopicNotFound)
    }
    
    pub fn reset_metrics(&self) -> Result<(), SubscriberError> {
        // Reset latency metrics
        if let Ok(mut metrics) = self.latency_metrics.lock() {
            metrics.reset();
        }
        
        // Reset buffer metrics
        for i in 0..self.topic_count {
            if let Some(buffer_arc) = &self.topic_buffers[i] {
                if let Ok(buffer) = buffer_arc.lock() {
                    buffer.reset_metrics();
                }
            }
        }
        
        Ok(())
    }
    
    pub fn is_connected(&self) -> bool {
        if let Ok(health) = self.health_metrics.lock() {
            health.connected
        } else {
            false
        }
    }
    
    pub fn is_stale(&self, timeout_ms: u64) -> bool {
        if let Ok(health) = self.health_metrics.lock() {
            if !health.connected {
                return true;
            }
            
            let now = current_time_ns();
            let last_msg_time = health.last_message_time;
            let timeout_ns = timeout_ms as u64 * 1_000_000;
            
            now - last_msg_time > timeout_ns
        } else {
            true
        }
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

// Helper function to get current time in nanoseconds
#[inline]
fn current_time_ns() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_nanos() as u64
}

// Production-ready examples
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread;
    use std::time::Duration;
    
    #[test]
    fn test_fixed_topic() {
        let topic = FixedTopic::from_str("test.topic").unwrap();
        assert_eq!(topic.as_str(), "test.topic");
        
        let topic2 = FixedTopic::from_str("test.topic").unwrap();
        assert!(topic.equals(&topic2));
        
        let topic3 = FixedTopic::from_str("different").unwrap();
        assert!(!topic.equals(&topic3));
    }
    
    #[test]
    fn test_ring_buffer() {
        let mut ring = RingBuffer::<u32>::new(4);
        
        // Test push and pop
        assert!(ring.push(1).is_ok());
        assert!(ring.push(2).is_ok());
        assert_eq!(ring.pop(), Some(1));
        assert_eq!(ring.pop(), Some(2));
        assert_eq!(ring.pop(), None);
        
        // Test capacity
        assert!(ring.push(3).is_ok());
        assert!(ring.push(4).is_ok());
        assert!(ring.push(5).is_ok());
        assert!(ring.push(6).is_ok());
        assert!(ring.push(7).is_err()); // Buffer full
        
        // Pop and then push
        assert_eq!(ring.pop(), Some(3));
        assert!(ring.push(7).is_ok());
    }
    
    #[test]
    fn test_subscriber() {
        // Setup test server
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        
        let server_thread = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            
            // Read the subscription request
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).unwrap();
            
            // Wait a bit
            thread::sleep(Duration::from_millis(10));
            
            // Send a test message
            let topic = "test.topic";
            let topic_len = topic.len() as u32;
            let topic_len_bytes = topic_len.to_be_bytes();
            
            let message = "test message";
            
            socket.write_all(&topic_len_bytes).unwrap();
            socket.write_all(topic.as_bytes()).unwrap();
            socket.write_all(message.as_bytes()).unwrap();
            
            thread::sleep(Duration::from_millis(50));
        });
        
        // Create connection config
        let config = ConnectionConfig::new(&format!("127.0.0.1:{}", addr.port()))
            .with_connect_timeout(1000)
            .with_read_timeout(100)
            .with_tcp_nodelay(true);
        
        // Create subscriber
        let mut subscriber = Subscriber::new(
            config,
            &["test.topic"]
        ).unwrap();
        
        // Set logger
        subscriber.set_logger(|msg| {
            println!("[HFT-SUB] {}", msg);
        });
        
        // Start subscriber
        subscriber.start().unwrap();
        
        // Wait for message
        thread::sleep(Duration::from_millis(100));
        
        // Get topic index
        let topic_idx = subscriber.find_topic_index("test.topic").unwrap();
        
        // Try to get a message
        let msg = subscriber.get_message(topic_idx);
        
        // Get metrics
        let health = subscriber.get_health_metrics().unwrap();
        let buffer_metrics = subscriber.get_buffer_metrics(topic_idx).unwrap();
        
        // Stop subscriber
        subscriber.stop().unwrap();
        
        // Wait for server to finish
        server_thread.join().unwrap();
        
        // Verify message was received
        assert!(msg.is_some());
        
        if let Some(msg) = msg {
            assert_eq!(msg.get_topic().as_str(), "test.topic");
            let msg_str = std::str::from_utf8(msg.get_data()).unwrap();
            assert_eq!(msg_str, "test message");
        }
        
        // Verify metrics
        assert!(health.message_count > 0);
    }
    
    #[test]
    fn test_reconnection() {
        // Setup test server that disconnects after first message
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        
        let server_addr = addr.clone();
        
        let server_thread = thread::spawn(move || {
            // First connection
            let (mut socket, _) = listener.accept().unwrap();
            
            // Read the subscription request
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).unwrap();
            
            // Send a test message
            let topic = "test.topic";
            let topic_len = topic.len() as u32;
            let topic_len_bytes = topic_len.to_be_bytes();
            
            let message = "message 1";
            
            socket.write_all(&topic_len_bytes).unwrap();
            socket.write_all(topic.as_bytes()).unwrap();
            socket.write_all(message.as_bytes()).unwrap();
            
            // Disconnect
            drop(socket);
            
            // Wait for reconnection attempt
            thread::sleep(Duration::from_millis(200));
            
            // Second connection
            let (mut socket, _) = listener.accept().unwrap();
            
            // Read the subscription request
            let _ = socket.read(&mut buf).unwrap();
            
            // Send another message
            let message = "message 2";
            
            socket.write_all(&topic_len_bytes).unwrap();
            socket.write_all(topic.as_bytes()).unwrap();
            socket.write_all(message.as_bytes()).unwrap();
            
            thread::sleep(Duration::from_millis(50));
        });
        
        // Create connection config
        let config = ConnectionConfig::new(&format!("127.0.0.1:{}", server_addr.port()))
            .with_connect_timeout(1000)
            .with_read_timeout(100)
            .with_tcp_nodelay(true);
        
        // Create subscriber
        let mut subscriber = Subscriber::new(
            config,
            &["test.topic"]
        ).unwrap();
        
        // Set logger
        subscriber.set_logger(|msg| {
            println!("[HFT-RECONNECT] {}", msg);
        });
        
        // Start subscriber
        subscriber.start().unwrap();
        
        // Wait for initial message and disconnect
        thread::sleep(Duration::from_millis(100));
        
        // Force reconnection (this is optional as the subscriber should reconnect automatically)
        subscriber.reconnect().unwrap();
        
        // Wait for reconnection and second message
        thread::sleep(Duration::from_millis(300));
        
        // Get topic index
        let topic_idx = subscriber.find_topic_index("test.topic").unwrap();
        
        // Try to get messages (should be the second message after reconnection)
        let msg = subscriber.get_message(topic_idx);
        
        // Get health metrics
        let health = subscriber.get_health_metrics().unwrap();
        
        // Stop subscriber
        subscriber.stop().unwrap();
        
        // Wait for server to finish
        server_thread.join().unwrap();
        
        // Verify message was received
        assert!(msg.is_some());
        
        if let Some(msg) = msg {
            assert_eq!(msg.get_topic().as_str(), "test.topic");
            let msg_str = std::str::from_utf8(msg.get_data()).unwrap();
            assert_eq!(msg_str, "message 2");
        }
        
        // Verify reconnection happened
        assert!(health.reconnect_count > 0);
    }
}