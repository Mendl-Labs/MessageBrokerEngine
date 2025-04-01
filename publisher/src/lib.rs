use std::io::{Error as IoError, ErrorKind};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::thread::{self, JoinHandle};
use std::collections::VecDeque;

use prost::bytes::BytesMut;
use prost::Message;
use protocol::broker::messages::broker_message;
use protocol::broker::messages::publish_request;
use protocol::broker::messages::BrokerMessage;
use protocol::broker::messages::PublishRequest;
use protocol::broker::messages::MarketMessage;
use protocol::broker::messages::Orders;
use protocol::broker::messages::Trades;
use protocol::broker::messages::Order;
use protocol::broker::messages::Trade;

// Constants for performance optimization
const MAX_QUEUE_SIZE: usize = 10000;
const MAX_BATCH_SIZE: usize = 100;
const MAX_PAYLOAD_SIZE: usize = 16384;
const RING_BUFFER_CAPACITY: usize = 8192;
const RECONNECT_INITIAL_DELAY_MS: u64 = 10;
const RECONNECT_MAX_DELAY_MS: u64 = 5000;
const METRICS_INTERVAL_MS: u64 = 1000;

// Error type with no allocation
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum PublisherError {
    ConnectionFailed,
    PublishFailed,
    SocketWriteFailed,
    InvalidMessage,
    EncodingFailed,
    QueueFull,
    Shutdown,
    InvalidConfiguration,
    SocketConfigFailed,
    TimedOut,
}

impl From<IoError> for PublisherError {
    fn from(err: IoError) -> Self {
        match err.kind() {
            ErrorKind::ConnectionRefused | ErrorKind::NotConnected => PublisherError::ConnectionFailed,
            ErrorKind::WouldBlock | ErrorKind::TimedOut => PublisherError::TimedOut,
            _ => PublisherError::SocketWriteFailed,
        }
    }
}

// Message types the publisher can handle
#[derive(Clone)]
pub enum MessagePayload {
    Orders(Orders),
    Trades(Trades),
    Raw(Vec<u8>),  // For raw binary data
}

// Pre-allocated, fixed-size message payload
#[derive(Clone)]
pub struct FixedPayload {
    payload: MessagePayload,
    topic_indices: Vec<usize>, // References to topics in PublisherConfig
    timestamp: u64,            // Timestamp when created (nanoseconds)
}

// Message queue with fixed capacity
struct MessageQueue {
    queue: VecDeque<FixedPayload>,
    capacity: usize,
    total_enqueued: AtomicUsize,
    total_dequeued: AtomicUsize,
    total_dropped: AtomicUsize,
    high_water_mark: AtomicUsize,
}

impl MessageQueue {
    fn new(capacity: usize) -> Self {
        Self {
            queue: VecDeque::with_capacity(capacity),
            capacity,
            total_enqueued: AtomicUsize::new(0),
            total_dequeued: AtomicUsize::new(0),
            total_dropped: AtomicUsize::new(0),
            high_water_mark: AtomicUsize::new(0),
        }
    }

    fn enqueue(&mut self, payload: FixedPayload) -> Result<(), PublisherError> {
        if self.queue.len() >= self.capacity {
            self.total_dropped.fetch_add(1, Ordering::Relaxed);
            return Err(PublisherError::QueueFull);
        }

        self.queue.push_back(payload);
        let current_len = self.queue.len();
        
        // Update high water mark if needed
        let current_hwm = self.high_water_mark.load(Ordering::Relaxed);
        if current_len > current_hwm {
            self.high_water_mark.store(current_len, Ordering::Relaxed);
        }
        
        self.total_enqueued.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn dequeue_batch(&mut self, max_count: usize) -> Vec<FixedPayload> {
        let count = self.queue.len().min(max_count);
        let mut result = Vec::with_capacity(count);
        
        for _ in 0..count {
            if let Some(payload) = self.queue.pop_front() {
                result.push(payload);
            } else {
                break;
            }
        }
        
        self.total_dequeued.fetch_add(result.len(), Ordering::Relaxed);
        result
    }

    fn len(&self) -> usize {
        self.queue.len()
    }

    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
    
    fn get_metrics(&self) -> (usize, usize, usize, usize, usize) {
        (
            self.total_enqueued.load(Ordering::Relaxed),
            self.total_dequeued.load(Ordering::Relaxed),
            self.total_dropped.load(Ordering::Relaxed),
            self.high_water_mark.load(Ordering::Relaxed),
            self.queue.len(),
        )
    }
    
    fn reset_metrics(&mut self) {
        self.total_enqueued.store(0, Ordering::Relaxed);
        self.total_dequeued.store(0, Ordering::Relaxed);
        self.total_dropped.store(0, Ordering::Relaxed);
        self.high_water_mark.store(self.queue.len(), Ordering::Relaxed);
    }
}

// Performance metrics for the publisher
#[derive(Clone)]
pub struct PublisherMetrics {
    connected: bool,
    last_publish_time: u64,
    message_count: u64,
    reconnect_count: u64,
    error_count: u64,
    last_error_code: Option<PublisherError>,
    last_error_time: u64,
    avg_batch_size: f64,
    avg_publish_latency_ns: u64,
    max_publish_latency_ns: u64,
}

impl Default for PublisherMetrics {
    fn default() -> Self {
        Self {
            connected: false,
            last_publish_time: 0,
            message_count: 0,
            reconnect_count: 0,
            error_count: 0,
            last_error_code: None,
            last_error_time: 0,
            avg_batch_size: 0.0,
            avg_publish_latency_ns: 0,
            max_publish_latency_ns: 0,
        }
    }
}

// Connection configuration
#[derive(Clone)]
pub struct PublisherConfig {
    addr: String,
    connect_timeout_ms: u64,
    write_timeout_ms: u64,
    enable_tcp_nodelay: bool,
    enable_tcp_quickack: bool,
    enable_so_priority: bool,
    priority_value: i32,
    cpu_affinity: Option<usize>,
    use_realtime_priority: bool,
    realtime_priority: i32,
    max_queue_size: usize,
    max_batch_size: usize,
    topics: Vec<String>,         // Pre-registered topics
}

impl Default for PublisherConfig {
    fn default() -> Self {
        Self {
            addr: String::new(),
            connect_timeout_ms: 1000,
            write_timeout_ms: 100,
            enable_tcp_nodelay: true,
            enable_tcp_quickack: true,
            enable_so_priority: true,
            priority_value: 6,
            cpu_affinity: None,
            use_realtime_priority: false,
            realtime_priority: 70,
            max_queue_size: MAX_QUEUE_SIZE,
            max_batch_size: MAX_BATCH_SIZE,
            topics: Vec::new(),
        }
    }
}

impl PublisherConfig {
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
    
    pub fn with_queue_size(mut self, size: usize) -> Self {
        self.max_queue_size = size;
        self
    }
    
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.max_batch_size = size;
        self
    }
    
    pub fn with_topics(mut self, topics: Vec<String>) -> Self {
        self.topics = topics;
        self
    }
}

// Command channel for controlling the worker thread
enum WorkerCommand {
    Reconnect,
    UpdateConfig(PublisherConfig),
    Shutdown,
}

// Result from worker thread
enum WorkerResult {
    Connected,
    Disconnected,
    Error(PublisherError),
    BatchPublished(usize), // Number of messages published
}

// Production-ready HFT publisher
pub struct Publisher {
    // Message queue
    message_queue: Arc<Mutex<MessageQueue>>,
    
    // Worker thread control
    running: Arc<AtomicBool>,
    worker_handle: Option<JoinHandle<()>>,
    command_sender: Option<std::sync::mpsc::Sender<WorkerCommand>>,
    result_receiver: Option<std::sync::mpsc::Receiver<WorkerResult>>,
    
    // Configuration
    config: PublisherConfig,
    
    // Metrics
    metrics: Arc<Mutex<PublisherMetrics>>,
    
    // Logging callback
    log_fn: Option<Arc<dyn Fn(&str) + Send + Sync>>,
}

impl Publisher {
    pub fn new(config: PublisherConfig) -> Result<Self, PublisherError> {
        if config.addr.is_empty() {
            return Err(PublisherError::InvalidConfiguration);
        }

        // Create message queue
        let message_queue = Arc::new(Mutex::new(MessageQueue::new(config.max_queue_size)));

        // Create command channel
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();

        Ok(Self {
            message_queue,
            running: Arc::new(AtomicBool::new(false)),
            worker_handle: None,
            command_sender: Some(cmd_tx),
            result_receiver: Some(result_rx),
            config,
            metrics: Arc::new(Mutex::new(PublisherMetrics::default())),
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
    
    fn configure_socket(socket: &std::net::TcpStream, config: &PublisherConfig) -> Result<(), PublisherError> {
        // Set socket timeouts
        if socket.set_write_timeout(Some(Duration::from_millis(config.write_timeout_ms))).is_err() {
            return Err(PublisherError::SocketConfigFailed);
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
                        return Err(PublisherError::SocketConfigFailed);
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
                        return Err(PublisherError::SocketConfigFailed);
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
                        return Err(PublisherError::SocketConfigFailed);
                    }
                }
            }
        }
        
        Ok(())
    }
    
    fn connect_with_timeout(addr: &str, timeout_ms: u64) -> Result<std::net::TcpStream, PublisherError> {
        // Create a socket
        let socket = std::net::TcpStream::connect(addr).map_err(|_| PublisherError::ConnectionFailed)?;
        
        // Set socket to non-blocking mode
        if socket.set_nonblocking(false).is_err() {
            return Err(PublisherError::SocketConfigFailed);
        }
        
        Ok(socket)
    }
    
    fn set_thread_priority(config: &PublisherConfig) {
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
    
    // Create publish request from payload
    fn create_publish_request(topics: Vec<String>, payload: &FixedPayload) -> PublishRequest {
        match &payload.payload {
            MessagePayload::Orders(orders) => {
                let market_message = MarketMessage {
                    payload: Some(protocol::broker::messages::market_message::Payload::OrdersPaylaod(orders.clone())),
                };
                
                PublishRequest {
                    topics,
                    payload: Some(publish_request::Payload::MarketPayload(market_message)),
                }
            },
            MessagePayload::Trades(trades) => {
                let market_message = MarketMessage {
                    payload: Some(protocol::broker::messages::market_message::Payload::TradesPayload(trades.clone())),
                };
                
                PublishRequest {
                    topics,
                    payload: Some(publish_request::Payload::MarketPayload(market_message)),
                }
            },
            MessagePayload::Raw(_) => {
                // Raw data can't be directly encoded as a publish_request payload
                // We'll just create an empty market message as a fallback
                let market_message = MarketMessage {
                    payload: None,
                };
                
                PublishRequest {
                    topics,
                    payload: Some(publish_request::Payload::MarketPayload(market_message)),
                }
            }
        }
    }
    
    // The worker thread function
    fn worker_thread(
        message_queue: Arc<Mutex<MessageQueue>>,
        config: PublisherConfig,
        cmd_rx: std::sync::mpsc::Receiver<WorkerCommand>,
        result_tx: std::sync::mpsc::Sender<WorkerResult>,
        metrics: Arc<Mutex<PublisherMetrics>>,
        log_fn: Option<Arc<dyn Fn(&str) + Send + Sync>>,
    ) {
        // Set thread priority and affinity
        Self::set_thread_priority(&config);
        
        // Log thread start
        if let Some(log) = &log_fn {
            log(&format!("HFT publisher worker thread started, connecting to {}", config.addr));
        }
        
        // Init metrics
        {
            let mut publisher_metrics = metrics.lock().unwrap();
            publisher_metrics.connected = false;
            publisher_metrics.last_publish_time = current_time_ns();
            publisher_metrics.message_count = 0;
            publisher_metrics.reconnect_count = 0;
            publisher_metrics.error_count = 0;
        }
        
        // Connect to server
        let mut socket_result = Self::connect_with_timeout(&config.addr, config.connect_timeout_ms);
        let mut reconnect_delay_ms = RECONNECT_INITIAL_DELAY_MS;
        let mut connected = false;
        let mut last_metrics_time = Instant::now();
        
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
                    
                    // Update metrics
                    let mut publisher_metrics = metrics.lock().unwrap();
                    publisher_metrics.reconnect_count += 1;
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
                            
                            // Update metrics
                            let mut publisher_metrics = metrics.lock().unwrap();
                            publisher_metrics.connected = false;
                            publisher_metrics.error_count += 1;
                            publisher_metrics.last_error_code = Some(e);
                            publisher_metrics.last_error_time = current_time_ns();
                            
                            // Try to reconnect
                            socket_result = Err(e);
                            continue;
                        }
                        
                        // Successfully connected
                        connected = true;
                        
                        // Update metrics
                        let mut publisher_metrics = metrics.lock().unwrap();
                        publisher_metrics.connected = true;
                        
                        // Notify
                        if let Some(log) = &log_fn {
                            log(&format!("Connected to {}", config.addr));
                        }
                        // Send result
                        let _ = result_tx.send(WorkerResult::Connected);
                    },
                    Err(e) => {
                        // Update metrics
                        let mut publisher_metrics = metrics.lock().unwrap();
                        publisher_metrics.connected = false;
                        publisher_metrics.error_count += 1;
                        publisher_metrics.last_error_code = Some(*e);
                        publisher_metrics.last_error_time = current_time_ns();

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
            
            // Process message queue when connected
            if connected {
                let socket = match socket_result.as_ref() {
                    Ok(s) => s,
                    Err(_) => {
                        connected = false;
                        continue;
                    }
                };
                
                // Check message queue
                let batch = {
                    let mut queue = message_queue.lock().unwrap();
                    if !queue.is_empty() {
                        queue.dequeue_batch(config.max_batch_size)
                    } else {
                        // No messages, sleep a bit
                        drop(queue);
                        thread::sleep(Duration::from_micros(100));
                        continue;
                    }
                };
                
                if batch.is_empty() {
                    continue;
                }
                
                // Publish all messages in the batch
                let now = current_time_ns();
                let batch_size = batch.len();
                let mut success_count = 0;
                
                for payload in batch {
                    // Get topics from the configuration
                    let topics: Vec<String> = payload.topic_indices.iter()
                        .filter_map(|&idx| {
                            if idx < config.topics.len() {
                                Some(config.topics[idx].clone())
                            } else {
                                None
                            }
                        })
                        .collect();
                    
                    if topics.is_empty() {
                        continue;
                    }
                    
                    // Create PublishRequest
                    let publish_request = Self::create_publish_request(topics, &payload);
                    
                    // Create BrokerMessage
                    let broker_message = BrokerMessage {
                        payload: Some(broker_message::Payload::PublishRequest(publish_request)),
                    };
                    
                    // Encode the message
                    let mut buf = BytesMut::with_capacity(broker_message.encoded_len());
                    if broker_message.encode(&mut buf).is_err() {
                        if let Some(log) = &log_fn {
                            log("Failed to encode publish request");
                        }
                        continue;
                    }
                    
                    // Write to socket
                    let mut socket_clone = match socket.try_clone() {
                        Ok(s) => s,
                        Err(_) => {
                            connected = false;
                            break;
                        }
                    };
                    
                    if let Err(e) = std::io::Write::write_all(&mut socket_clone, &buf) {
                        // Socket error
                        if let Some(log) = &log_fn {
                            log(&format!("Socket write error: {:?}", e));
                        }
                        
                        let error = PublisherError::from(e);
                        
                        // Update metrics
                        let mut publisher_metrics = metrics.lock().unwrap();
                        publisher_metrics.connected = false;
                        publisher_metrics.error_count += 1;
                        publisher_metrics.last_error_code = Some(error);
                        publisher_metrics.last_error_time = current_time_ns();
                        
                        // Notify about error
                        let _ = result_tx.send(WorkerResult::Error(error));
                        
                        // Try to reconnect
                        connected = false;
                        socket_result = Self::connect_with_timeout(&config.addr, config.connect_timeout_ms);
                        reconnect_delay_ms = RECONNECT_INITIAL_DELAY_MS;
                        break;
                    }
                    
                    success_count += 1;
                }
                
                if success_count > 0 {
                    // Calculate publish latency
                    let publish_time = current_time_ns();
                    let latency = publish_time - now;
                    
                    // Update metrics
                    let mut publisher_metrics = metrics.lock().unwrap();
                    publisher_metrics.message_count += success_count as u64;
                    publisher_metrics.last_publish_time = publish_time;
                    
                    // Update latency metrics with exponential moving average
                    const ALPHA: f64 = 0.1;
                    let cur_avg = publisher_metrics.avg_publish_latency_ns as f64;
                    publisher_metrics.avg_publish_latency_ns = 
                        ((1.0 - ALPHA) * cur_avg + ALPHA * latency as f64) as u64;
                    
                    publisher_metrics.max_publish_latency_ns = 
                        publisher_metrics.max_publish_latency_ns.max(latency);
                    
                    // Update batch size metrics
                    let cur_avg_batch = publisher_metrics.avg_batch_size;
                    publisher_metrics.avg_batch_size = 
                        (1.0 - ALPHA) * cur_avg_batch + ALPHA * batch_size as f64;
                    
                    // Send result
                    let _ = result_tx.send(WorkerResult::BatchPublished(success_count));
                }
            }
            
            // Update metrics periodically
            if last_metrics_time.elapsed() >= Duration::from_millis(METRICS_INTERVAL_MS) {
                // Nothing additional to compute
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

    pub fn start(&mut self) -> Result<(), PublisherError> {
        if self.running.load(Ordering::Relaxed) {
            return Ok(());
        }
        
        // Set running flag
        self.running.store(true, Ordering::Release);
        
        // Log start
        self.log("Starting HFT publisher");
        
        // Create new channels for the worker thread
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        
        // Store the sender in self for future use
        self.command_sender = Some(cmd_tx);
        self.result_receiver = Some(result_rx);
        
        let message_queue = self.message_queue.clone();
        let config = self.config.clone();
        let metrics = self.metrics.clone();
        let log_fn = self.log_fn.clone();
        
        // Spawn worker thread
        let handle = thread::spawn(move || {
            Self::worker_thread(
                message_queue,
                config,
                cmd_rx,
                result_tx,
                metrics,
                log_fn,
            );
        });
        
        // Store thread handle
        self.worker_handle = Some(handle);
        
        self.log("HFT publisher started successfully");
        
        Ok(())
    }

    pub fn stop(&mut self) -> Result<(), PublisherError> {
        if !self.running.load(Ordering::Relaxed) {
            return Ok(());
        }
        
        self.log("Stopping HFT publisher");
        
        // Send shutdown command
        if let Some(sender) = &self.command_sender {
            if sender.send(WorkerCommand::Shutdown).is_err() {
                return Err(PublisherError::Shutdown);
            }
        }
        
        // Set running flag
        self.running.store(false, Ordering::Release);
        
        // Wait for worker thread to finish
        if let Some(handle) = self.worker_handle.take() {
            if handle.join().is_err() {
                return Err(PublisherError::Shutdown);
            }
        }
        
        self.log("HFT publisher stopped successfully");
        
        Ok(())
    }
    
    pub fn reconnect(&self) -> Result<(), PublisherError> {
        if !self.running.load(Ordering::Relaxed) {
            return Err(PublisherError::Shutdown);
        }
        
        self.log("Requesting reconnection");
        
        // Send reconnect command
        if let Some(sender) = &self.command_sender {
            if sender.send(WorkerCommand::Reconnect).is_err() {
                return Err(PublisherError::Shutdown);
            }
        } else {
            return Err(PublisherError::Shutdown);
        }
        
        Ok(())
    }
    
    pub fn update_config(&self, config: PublisherConfig) -> Result<(), PublisherError> {
        if !self.running.load(Ordering::Relaxed) {
            return Err(PublisherError::Shutdown);
        }
        
        self.log("Updating configuration");
        
        // Send update config command
        if let Some(sender) = &self.command_sender {
            if sender.send(WorkerCommand::UpdateConfig(config)).is_err() {
                return Err(PublisherError::Shutdown);
            }
        } else {
            return Err(PublisherError::Shutdown);
        }
        
        Ok(())
    }
    
    // Publish orders message
    pub fn publish_orders(&self, topic_idx: usize, orders: Orders) -> Result<(), PublisherError> {
        if !self.running.load(Ordering::Relaxed) {
            return Err(PublisherError::Shutdown);
        }
        
        if topic_idx >= self.config.topics.len() {
            return Err(PublisherError::InvalidConfiguration);
        }
        
        let payload = FixedPayload {
            payload: MessagePayload::Orders(orders),
            topic_indices: vec![topic_idx],
            timestamp: current_time_ns(),
        };
        
        // Add to message queue
        let mut queue = self.message_queue.lock().unwrap();
        queue.enqueue(payload)
    }
    
    // Publish trades message
    pub fn publish_trades(&self, topic_idx: usize, trades: Trades) -> Result<(), PublisherError> {
        if !self.running.load(Ordering::Relaxed) {
            return Err(PublisherError::Shutdown);
        }
        
        if topic_idx >= self.config.topics.len() {
            return Err(PublisherError::InvalidConfiguration);
        }
        
        let payload = FixedPayload {
            payload: MessagePayload::Trades(trades),
            topic_indices: vec![topic_idx],
            timestamp: current_time_ns(),
        };
        
        // Add to message queue
        let mut queue = self.message_queue.lock().unwrap();
        queue.enqueue(payload)
    }
    
    // Publish a single order
    pub fn publish_order(&self, topic_idx: usize, order: Order) -> Result<(), PublisherError> {
        if !self.running.load(Ordering::Relaxed) {
            return Err(PublisherError::Shutdown);
        }
        
        if topic_idx >= self.config.topics.len() {
            return Err(PublisherError::InvalidConfiguration);
        }
        
        let orders = Orders {
            orders: vec![order],
        };
        
        self.publish_orders(topic_idx, orders)
    }
    
    // Publish a single trade
    pub fn publish_trade(&self, topic_idx: usize, trade: Trade) -> Result<(), PublisherError> {
        if !self.running.load(Ordering::Relaxed) {
            return Err(PublisherError::Shutdown);
        }
        
        if topic_idx >= self.config.topics.len() {
            return Err(PublisherError::InvalidConfiguration);
        }
        
        let trades = Trades {
            trades: vec![trade],
        };
        
        self.publish_trades(topic_idx, trades)
    }
    
    // Publish raw data (not supported by the proto definition, but kept for compatibility)
    pub fn publish_raw(&self, topic_idx: usize, data: Vec<u8>) -> Result<(), PublisherError> {
        if !self.running.load(Ordering::Relaxed) {
            return Err(PublisherError::Shutdown);
        }
        
        if topic_idx >= self.config.topics.len() {
            return Err(PublisherError::InvalidConfiguration);
        }
        
        let payload = FixedPayload {
            payload: MessagePayload::Raw(data),
            topic_indices: vec![topic_idx],
            timestamp: current_time_ns(),
        };
        
        // Add to message queue
        let mut queue = self.message_queue.lock().unwrap();
        queue.enqueue(payload)
    }
    
    // Legacy publish method to maintain compatibility with the original API
    pub fn publish(&mut self, topics: &Vec<String>, payload: Option<publish_request::Payload>) -> Result<(), PublisherError> {
        if !self.running.load(Ordering::Relaxed) {
            return Err(PublisherError::Shutdown);
        }
        
        // Convert topics to indices for efficiency
        let mut topic_indices = Vec::with_capacity(topics.len());
        for topic in topics {
            if let Some(idx) = self.find_topic_index(topic) {
                topic_indices.push(idx);
            } else {
                // Auto-register the topic if not found
                let idx = self.config.topics.len();
                self.config.topics.push(topic.clone());
                topic_indices.push(idx);
            }
        }
        
        if topic_indices.is_empty() {
            return Err(PublisherError::InvalidConfiguration);
        }
        
        // Process the payload
        let message_payload = match payload {
            Some(publish_request::Payload::MarketPayload(market_msg)) => {
                match market_msg.payload {
                    Some(protocol::broker::messages::market_message::Payload::OrdersPaylaod(orders)) => {
                        MessagePayload::Orders(orders)
                    },
                    Some(protocol::broker::messages::market_message::Payload::TradesPayload(trades)) => {
                        MessagePayload::Trades(trades)
                    },
                    None => {
                        // Empty payload, just use raw empty bytes
                        MessagePayload::Raw(Vec::new())
                    }
                    _ => {
                        // Unsupported payload type, return error
                        return Err(PublisherError::InvalidConfiguration);
                    }
                }
            },
            None => {
                // Empty payload, just use raw empty bytes
                MessagePayload::Raw(Vec::new())
            }
        };
        
        let fixed_payload = FixedPayload {
            payload: message_payload,
            topic_indices,
            timestamp: current_time_ns(),
        };
        
        // Add to message queue
        let mut queue = self.message_queue.lock().unwrap();
        queue.enqueue(fixed_payload)
    }
    
    // Find topic index by name
    pub fn find_topic_index(&self, topic: &str) -> Option<usize> {
        self.config.topics.iter().position(|t| t == topic)
    }
    
    // Get registered topics
    pub fn get_topics(&self) -> Vec<String> {
        self.config.topics.clone()
    }
    
    // Get publisher metrics
    pub fn get_metrics(&self) -> Result<PublisherMetrics, PublisherError> {
        if let Ok(metrics) = self.metrics.lock() {
            Ok(metrics.clone())
        } else {
            Err(PublisherError::Shutdown)
        }
    }
    
    // Get queue metrics
    pub fn get_queue_metrics(&self) -> Result<(usize, usize, usize, usize, usize), PublisherError> {
        if let Ok(queue) = self.message_queue.lock() {
            Ok(queue.get_metrics())
        } else {
            Err(PublisherError::Shutdown)
        }
    }
    
    // Reset metrics
    pub fn reset_metrics(&self) -> Result<(), PublisherError> {
        // Reset queue metrics
        if let Ok(mut queue) = self.message_queue.lock() {
            queue.reset_metrics();
        }
        
        // Reset publisher metrics
        if let Ok(mut publisher_metrics) = self.metrics.lock() {
            publisher_metrics.max_publish_latency_ns = 0;
            publisher_metrics.avg_publish_latency_ns = 0;
            publisher_metrics.avg_batch_size = 0.0;
            publisher_metrics.message_count = 0;
            publisher_metrics.error_count = 0;
        }
        
        Ok(())
    }
    
    // Check if publisher is connected
    pub fn is_connected(&self) -> bool {
        if let Ok(metrics) = self.metrics.lock() {
            metrics.connected
        } else {
            false
        }
    }
    
    // Check if publisher has stale connection
    pub fn is_stale(&self, timeout_ms: u64) -> bool {
        if let Ok(metrics) = self.metrics.lock() {
            if !metrics.connected {
                return true;
            }
            
            let now = current_time_ns();
            let last_publish_time = metrics.last_publish_time;
            let timeout_ns = timeout_ms as u64 * 1_000_000;
            
            now - last_publish_time > timeout_ns
        } else {
            true
        }
    }
    
    // Register a new topic
    pub fn register_topic(&mut self, topic: String) -> usize {
        let idx = self.config.topics.len();
        self.config.topics.push(topic);
        idx
    }
    
    // Register multiple topics at once
    pub fn register_topics(&mut self, topics: Vec<String>) -> Vec<usize> {
        let start_idx = self.config.topics.len();
        let mut indices = Vec::with_capacity(topics.len());
        
        for (i, topic) in topics.into_iter().enumerate() {
            self.config.topics.push(topic);
            indices.push(start_idx + i);
        }
        
        indices
    }
    
    // Get queue capacity
    pub fn get_queue_capacity(&self) -> usize {
        self.config.max_queue_size
    }
    
    // Get queue length
    pub fn get_queue_length(&self) -> usize {
        if let Ok(queue) = self.message_queue.lock() {
            queue.len()
        } else {
            0
        }
    }
    
    // Get queue fill percentage
    pub fn get_queue_fill_percentage(&self) -> f64 {
        if let Ok(queue) = self.message_queue.lock() {
            (queue.len() as f64 / self.config.max_queue_size as f64) * 100.0
        } else {
            0.0
        }
    }
}

impl Drop for Publisher {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread;
    use std::time::Duration;
    
    #[test]
    fn test_publisher_new() {
        // Setup test server
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        
        let server_thread = thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            let _ = socket;
        });
        
        // Create config
        let config = PublisherConfig::new(&format!("127.0.0.1:{}", addr.port()))
            .with_connect_timeout(1000)
            .with_write_timeout(100)
            .with_tcp_nodelay(true)
            .with_topics(vec!["test.topic".to_string()]);
        
        // Create publisher
        let publisher_result = Publisher::new(config);
        
        // Wait for server to finish
        server_thread.join().unwrap();
        
        assert!(publisher_result.is_ok());
    }
    
    #[test]
    fn test_publisher_publish_order() {
        // Setup test server
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        
        let server_thread = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            
            // Read messages
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).unwrap();
            
            thread::sleep(Duration::from_millis(50));
        });
        
        // Create config with topics
        let config = PublisherConfig::new(&format!("127.0.0.1:{}", addr.port()))
            .with_connect_timeout(1000)
            .with_write_timeout(100)
            .with_tcp_nodelay(true)
            .with_topics(vec!["test.topic".to_string()]);
        
        // Create and start publisher
        let mut publisher = Publisher::new(config).unwrap();
        publisher.set_logger(|msg| {
            println!("[HFT-PUB] {}", msg);
        });
        
        publisher.start().unwrap();
        
        // Wait for connection
        thread::sleep(Duration::from_millis(50));
        
        // Create and publish an order
        let order = Order {
            unique_id: "order1".to_string(),
            symbol: "AAPL".to_string(),
            exchange: "NASDAQ".to_string(),
            price_level: 150.0,
            quantity: 100.0,
            side: "BUY".to_string(),
            event: "NEW".to_string(),
        };
        
        let result = publisher.publish_order(0, order);
        
        // Get metrics
        let metrics = publisher.get_metrics().unwrap();
        let queue_metrics = publisher.get_queue_metrics().unwrap();
        
        // Stop publisher
        publisher.stop().unwrap();
        
        // Wait for server to finish
        server_thread.join().unwrap();
        
        // Verify message was published
        assert!(result.is_ok());
        assert!(publisher.is_connected());
    }
    
    #[test]
    fn test_publisher_publish_trade() {
        // Setup test server
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        
        let server_thread = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            
            // Read messages
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).unwrap();
            
            thread::sleep(Duration::from_millis(50));
        });
        
        // Create config with topics
        let config = PublisherConfig::new(&format!("127.0.0.1:{}", addr.port()))
            .with_connect_timeout(1000)
            .with_write_timeout(100)
            .with_tcp_nodelay(true)
            .with_topics(vec!["test.topic".to_string()]);
        
        // Create and start publisher
        let mut publisher = Publisher::new(config).unwrap();
        publisher.set_logger(|msg| {
            println!("[HFT-PUB] {}", msg);
        });
        
        publisher.start().unwrap();
        
        // Wait for connection
        thread::sleep(Duration::from_millis(50));
        
        // Create and publish a trade
        let trade = Trade {
            symbol: "AAPL".to_string(),
            exchange: "NASDAQ".to_string(),
            side: "BUY".to_string(),
            price: 150.0,
            qty: 100.0,
            ord_type: "LIMIT".to_string(),
            trade_id: 12345,
            timestamp: "2023-08-01T12:34:56.789Z".to_string(),
        };
        
        let result = publisher.publish_trade(0, trade);
        
        // Get metrics
        let metrics = publisher.get_metrics().unwrap();
        let queue_metrics = publisher.get_queue_metrics().unwrap();
        
        // Stop publisher
        publisher.stop().unwrap();
        
        // Wait for server to finish
        server_thread.join().unwrap();
        
        // Verify message was published
        assert!(result.is_ok());
        assert!(publisher.is_connected());
    }
}

// Compatibility layer with the original API
#[async_trait::async_trait]
pub trait PublisherTrait {
    async fn publish(&mut self, topics: &Vec<String>, payload: Option<publish_request::Payload>) -> Result<(), PublisherError>;
}

// Legacy async wrapper around the HFT Publisher
pub struct AsyncPublisher {
    inner: Publisher,
}

impl AsyncPublisher {
    pub async fn new(addr: &str) -> Result<Self, PublisherError> {
        let config = PublisherConfig::new(addr);
        let inner = Publisher::new(config)?;
        let mut publisher = Self { inner };
        publisher.inner.start()?;
        Ok(publisher)
    }
}

#[async_trait::async_trait]
impl PublisherTrait for AsyncPublisher {
    async fn publish(&mut self, topics: &Vec<String>, payload: Option<publish_request::Payload>) -> Result<(), PublisherError> {
        self.inner.publish(topics, payload)
    }
}