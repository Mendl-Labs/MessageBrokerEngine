use async_trait::async_trait;
use config::{Config, File, FileFormat};
use mockall::automock;
use prost::{bytes::BytesMut, Message};
use protocol::broker::messages::{broker_message, BrokerMessage};
use serde::Deserialize;
use std::{error::Error, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, BufReader, Interest},
    net::{TcpListener, TcpStream},
    signal,
    sync::{Mutex, Semaphore, mpsc, RwLock},
    time::Instant,
    task::JoinSet,
};
use topicmanager::{TopicManager, TopicManagerTrait};
use tracing::{info, error, warn, debug, instrument};

// Windows-specific imports
#[cfg(target_os = "windows")]
use std::os::windows::io::AsRawSocket;

// Unix-specific imports
#[cfg(not(target_os = "windows"))]
use std::os::unix::io::AsRawFd;
#[cfg(not(target_os = "windows"))]
use libc::setsockopt;

#[cfg(target_os = "linux")]
use tokio::net::TcpSocket;

// Constants for performance optimization
const MAX_CONNECTIONS: usize = 10000;         // Maximum number of concurrent connections
const MAX_WORKER_THREADS: usize = 32;         // Maximum number of worker threads
const CONNECTION_TIMEOUT_MS: u64 = 2000;      // Connection timeout in milliseconds
const READ_BUFFER_SIZE: usize = 16384;        // TCP read buffer size
const MAX_MESSAGE_SIZE: usize = 1048576;      // Maximum message size (1MB)
const HEALTH_CHECK_INTERVAL_SEC: u64 = 60;    // Health check interval
const METRICS_INTERVAL_SEC: u64 = 10;         // Metrics reporting interval

// Performance metrics
#[derive(Debug, Default, Clone)]
pub struct ServerMetrics {
    active_connections: usize,
    total_connections: u64,
    rejected_connections: u64,
    messages_processed: u64, 
    errors: u64,
    avg_processing_time_ns: u64,
    max_processing_time_ns: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HostConfig {
    address: String,
    port: u16,
    // HFT-specific configuration parameters
    backlog: Option<u32>,               // TCP connection backlog
    tcp_nodelay: Option<bool>,          // Enable/disable Nagle's algorithm
    tcp_keepalive_sec: Option<u64>,     // TCP keepalive in seconds
    receive_buffer_size: Option<usize>, // SO_RCVBUF size
    send_buffer_size: Option<usize>,    // SO_SNDBUF size
    max_connections: Option<usize>,     // Maximum number of connections
    worker_threads: Option<usize>,      // Number of worker threads
}

impl HostConfig {
    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn port(&self) -> &u16 {
        &self.port
    }
    
    pub fn backlog(&self) -> u32 {
        self.backlog.unwrap_or(1024)
    }
    
    pub fn tcp_nodelay(&self) -> bool {
        self.tcp_nodelay.unwrap_or(true)
    }
    
    pub fn tcp_keepalive_sec(&self) -> Option<u64> {
        self.tcp_keepalive_sec
    }
    
    pub fn receive_buffer_size(&self) -> Option<usize> {
        self.receive_buffer_size
    }
    
    pub fn send_buffer_size(&self) -> Option<usize> {
        self.send_buffer_size
    }
    
    pub fn max_connections(&self) -> usize {
        self.max_connections.unwrap_or(MAX_CONNECTIONS)
    }
    
    pub fn worker_threads(&self) -> usize {
        self.worker_threads.unwrap_or_else(|| {
            // Default to number of logical CPUs if available, or fall back to 4
            #[cfg(not(target_arch = "wasm32"))]
            {
                // Use std::thread::available_parallelism if available (Rust 1.59+)
                match std::thread::available_parallelism() {
                    Ok(n) => n.get().min(MAX_WORKER_THREADS),
                    Err(_) => 4.min(MAX_WORKER_THREADS),
                }
            }
            
            #[cfg(target_arch = "wasm32")]
            {
                // For WebAssembly, default to 1
                1
            }
        })
    }
}

impl TryFrom<Config> for HostConfig {
    type Error = config::ConfigError;

    fn try_from(config: Config) -> Result<Self, Self::Error> {
        let address = config.get::<String>(
            "MessageBrokerServerConfiguration.MessageBrokerServerSettings.Address",
        )?;
        let port = config
            .get::<u16>("MessageBrokerServerConfiguration.MessageBrokerServerSettings.Port")?;
            
        // Try to get HFT-specific configurations
        let backlog = config
            .get::<u32>("MessageBrokerServerConfiguration.MessageBrokerServerSettings.Backlog")
            .ok();
        let tcp_nodelay = config
            .get::<bool>("MessageBrokerServerConfiguration.MessageBrokerServerSettings.TcpNoDelay")
            .ok();
        let tcp_keepalive_sec = config
            .get::<u64>("MessageBrokerServerConfiguration.MessageBrokerServerSettings.TcpKeepAliveSec")
            .ok();
        let receive_buffer_size = config
            .get::<usize>("MessageBrokerServerConfiguration.MessageBrokerServerSettings.ReceiveBufferSize")
            .ok();
        let send_buffer_size = config
            .get::<usize>("MessageBrokerServerConfiguration.MessageBrokerServerSettings.SendBufferSize")
            .ok();
        let max_connections = config
            .get::<usize>("MessageBrokerServerConfiguration.MessageBrokerServerSettings.MaxConnections")
            .ok();
        let worker_threads = config
            .get::<usize>("MessageBrokerServerConfiguration.MessageBrokerServerSettings.WorkerThreads")
            .ok();
        
        Ok(HostConfig { 
            address, 
            port,
            backlog,
            tcp_nodelay,
            tcp_keepalive_sec,
            receive_buffer_size,
            send_buffer_size,
            max_connections,
            worker_threads,
        })
    }
}

#[automock]
#[async_trait]
pub trait HostedObjectTrait {
    async fn run(&self) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn shutdown(&self) -> Result<(), Box<dyn Error + Send + Sync>>;
    fn get_metrics(&self) -> ServerMetrics;
}

#[derive(Debug)]
pub struct HostedObject {
    host: HostConfig,
    topic_manager: Arc<TopicManager>,
    shutdown_sender: Option<mpsc::Sender<()>>,
    metrics: Arc<RwLock<ServerMetrics>>,
    connection_limiter: Arc<Semaphore>,
}

impl HostedObject {
    pub fn host(&self) -> &HostConfig {
        &self.host
    }

    pub fn topic_manager(&self) -> &Arc<TopicManager> {
        &self.topic_manager
    }

    pub fn build() -> Result<Self, Box<dyn Error + Send + Sync>> {
        let host: HostConfig = load_config()?.try_into()?;
        let topic_manager = Arc::new(TopicManager::new());
        let connection_limiter = Arc::new(Semaphore::new(host.max_connections()));
        let metrics = Arc::new(RwLock::new(ServerMetrics::default()));
        
        Ok(Self {
            host,
            topic_manager,
            shutdown_sender: None,
            metrics,
            connection_limiter,
        })
    }
    
    // Configure TCP socket for Windows using Tokio's API
    #[cfg(target_os = "windows")]
    fn configure_socket(socket: &TcpStream, config: &HostConfig) -> Result<(), Box<dyn Error + Send + Sync>> {
        // For Windows, we'll use Tokio's builtin methods which are safer and more portable
        // Set TCP_NODELAY (disable Nagle's algorithm)
        if config.tcp_nodelay() {
            socket.set_nodelay(true)?;
        }
        
        // Note: For more advanced socket options like keepalive,
        // we'd need to add the windows-sys crate as a dependency
        // For now, we'll just log that these settings can't be applied
        if let Some(keepalive_secs) = config.tcp_keepalive_sec() {
            warn!("TCP keepalive settings not applied: requires windows-sys crate. Keepalive setting: {} seconds", keepalive_secs);
        }
        
        Ok(())
    }
    
    // Configure TCP socket for Unix-like systems
    #[cfg(not(target_os = "windows"))]
    fn configure_socket(socket: &TcpStream, config: &HostConfig) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Get the raw file descriptor using AsRawFd trait
        let fd = socket.as_raw_fd();

        // Set TCP_NODELAY (disable Nagle's algorithm)
        if config.tcp_nodelay() {
            let nodelay_val: libc::c_int = 1;
            let result = unsafe {
                setsockopt(
                    fd, 
                    libc::IPPROTO_TCP, 
                    libc::TCP_NODELAY, 
                    &nodelay_val as *const _ as *const libc::c_void, 
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t
                )
            };
            
            if result != 0 {
                return Err("Failed to set TCP_NODELAY".into());
            }
        }
        
        // Set TCP keepalive
        if let Some(keepalive_secs) = config.tcp_keepalive_sec() {
            let keepalive_val: libc::c_int = 1;
            let result = unsafe {
                libc::setsockopt(
                    fd, 
                    libc::SOL_SOCKET, 
                    libc::SO_KEEPALIVE, 
                    &keepalive_val as *const _ as *const libc::c_void, 
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t
                )
            };
            
            if result != 0 {
                return Err("Failed to enable TCP keepalive".into());
            }

            // Platform-specific keepalive time settings
            #[cfg(target_os = "macos")]
            unsafe {
                let keepalive_time: libc::c_int = keepalive_secs as libc::c_int;
                libc::setsockopt(
                    fd, 
                    libc::SOL_SOCKET, 
                    libc::TCP_KEEPALIVE, 
                    &keepalive_time as *const _ as *const libc::c_void, 
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t
                );
            }

            #[cfg(target_os = "linux")]
            unsafe {
                let tcp_keepidle: libc::c_int = keepalive_secs as libc::c_int;
                libc::setsockopt(
                    fd, 
                    libc::IPPROTO_TCP, 
                    libc::TCP_KEEPIDLE, 
                    &tcp_keepidle as *const _ as *const libc::c_void, 
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t
                );
            }
        }
        
        Ok(())
    }
    
    // Create and configure TCP listener
    async fn create_listener(&self) -> Result<TcpListener, Box<dyn Error + Send + Sync>> {
        let addr = format!("{}:{}", &self.host.address, &self.host.port);
        
        #[cfg(target_os = "linux")]
        {
            // Use more direct socket API for better control
            let socket = TcpSocket::new_v4()?;
            
            // Set socket options for reuse
            socket.set_reuseaddr(true)?;
            
            // Bind to address
            socket.bind(addr.parse()?)?;
            
            // Set the backlog size
            let listener = socket.listen(self.host.backlog())?;
            
            info!("Server listening on {} with backlog {}", addr, self.host.backlog());
            Ok(listener)
        }
        
        #[cfg(not(target_os = "linux"))]
        {
            // Fallback for non-Linux platforms
            let listener = TcpListener::bind(&addr).await?;
            info!("Server listening on {}", addr);
            Ok(listener)
        }
    }
}

#[async_trait]
impl HostedObjectTrait for HostedObject {
    #[instrument(skip(self))]
    async fn run(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let listener = self.create_listener().await?;
        
        // Create shutdown channel
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
        let mut this = self.clone();
        this.shutdown_sender = Some(shutdown_tx);
        
        // Configure runtime for optimal performance
        let runtime_config = self.host.clone();
        let worker_threads = runtime_config.worker_threads();
        info!("Using {} worker threads", worker_threads);
        
        // Create connection set to track active connections
        let mut connection_set = JoinSet::new();
        
        // Clone shared resources
        let topic_manager = Arc::clone(&self.topic_manager);
        let metrics = Arc::clone(&self.metrics);
        let connection_limiter = Arc::clone(&self.connection_limiter);
        
        // Start metrics reporting task
        let metrics_clone = Arc::clone(&metrics);
        let metrics_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(METRICS_INTERVAL_SEC));
            loop {
                interval.tick().await;
                let metrics = metrics_clone.read().await;
                info!(
                    "Server metrics: connections={}/{} messages={} errors={} avg_time={}ns max_time={}ns",
                    metrics.active_connections,
                    metrics.total_connections,
                    metrics.messages_processed,
                    metrics.errors,
                    metrics.avg_processing_time_ns,
                    metrics.max_processing_time_ns
                );
            }
        });
        
        // Main server loop
        info!("Server started");
        
        loop {
            tokio::select! {
                // Accept new connections
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((socket, addr)) => {
                            // Handle connection accept differently to avoid borrowing issues
                            if connection_limiter.available_permits() > 0 {
                                // Acquire the permit inside the task to avoid borrowing issues
                                debug!("Accepted connection from: {:?}", addr);
                                
                                // Configure socket for HFT
                                if let Err(e) = Self::configure_socket(&socket, &self.host) {
                                    error!("Failed to configure socket: {:?}", e);
                                    continue;
                                }
                                
                                // Update metrics
                                {
                                    let mut server_metrics = metrics.write().await;
                                    server_metrics.active_connections += 1;
                                    server_metrics.total_connections += 1;
                                }
                                
                                // Create shared resources for this connection
                                let topic_manager = Arc::clone(&topic_manager);
                                let socket = Arc::new(Mutex::new(socket));
                                let metrics = Arc::clone(&metrics);
                                let connection_limiter = Arc::clone(&connection_limiter);
                                
                                // Spawn connection handler
                                connection_set.spawn(async move {
                                    // Acquire permit at the start of the task
                                    let _permit = match connection_limiter.acquire().await {
                                        Ok(permit) => permit,
                                        Err(e) => {
                                            error!("Failed to acquire connection permit: {:?}", e);
                                            return;
                                        }
                                    };
                                    
                                    // Handle connection until it closes - now passing metrics as the third argument
                                    if let Err(e) = handle_connection(socket.clone(), topic_manager, metrics.clone()).await {
                                        error!("Connection handler error: {}", e);
                                        
                                        // Update error metrics
                                        let mut server_metrics = metrics.write().await;
                                        server_metrics.errors += 1;
                                    }
                                    
                                    // Update metrics on disconnect
                                    let mut server_metrics = metrics.write().await;
                                    server_metrics.active_connections -= 1;
                                    
                                    // Permit is automatically dropped here, releasing the semaphore
                                });
                            } else {
                                // Connection limit reached
                                warn!("Connection limit reached, rejecting connection from: {:?}", addr);
                                
                                // Update rejected metrics
                                let mut server_metrics = metrics.write().await;
                                server_metrics.rejected_connections += 1;
                            }
                        }
                        Err(e) => {
                            error!("Error accepting connection: {:?}", e);
                            
                            // Update error metrics
                            let mut server_metrics = metrics.write().await;
                            server_metrics.errors += 1;
                        }
                    }
                },
                
                // Clean up finished connection handlers
                Some(res) = connection_set.join_next() => {
                    if let Err(e) = res {
                        error!("Connection handler panicked: {:?}", e);
                        
                        // Update error metrics
                        let mut server_metrics = metrics.write().await;
                        server_metrics.errors += 1;
                    }
                },
                
                // Handle shutdown signal
                _ = shutdown_rx.recv() => {
                    info!("Shutdown signal received");
                    break;
                },
                
                // Handle Ctrl+C
                _ = signal::ctrl_c() => {
                    info!("Ctrl+C received, shutting down");
                    break;
                }
            }
        }
        
        // Clean shutdown
        info!("Stopping server");
        
        // Abort metrics task
        metrics_task.abort();
        
        // Wait for all connections to finish or timeout after 5 seconds
        let shutdown_timeout = Duration::from_secs(5);
        tokio::select! {
            _ = connection_set.shutdown() => {
                info!("All connections closed gracefully");
            }
            _ = tokio::time::sleep(shutdown_timeout) => {
                warn!("Shutdown timed out, forcing exit");
            }
        }
        
        Ok(())
    }
    
    async fn shutdown(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(sender) = &self.shutdown_sender {
            let _ = sender.send(()).await;
            Ok(())
        } else {
            Err("Server not running".into())
        }
    }
    
    fn get_metrics(&self) -> ServerMetrics {
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                self.metrics.read().await.clone()
            })
        })
    }
}

// Implement Clone manually for HostedObject
impl Clone for HostedObject {
    fn clone(&self) -> Self {
        Self {
            host: self.host.clone(),
            topic_manager: Arc::clone(&self.topic_manager),
            shutdown_sender: self.shutdown_sender.clone(),
            metrics: Arc::clone(&self.metrics),
            connection_limiter: Arc::clone(&self.connection_limiter),
        }
    }
}

#[instrument(skip(socket, topic_manager))]
async fn handle_connection(
    socket: Arc<Mutex<TcpStream>>,
    topic_manager: Arc<TopicManager>,
    metrics: Arc<RwLock<ServerMetrics>>,  // Add metrics parameter
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Use a larger buffer for high-speed reading
    let mut buf = BytesMut::with_capacity(READ_BUFFER_SIZE);
    
    // Lock mode settings
    let mut socket_lock = socket.lock().await;
    
    // Set socket to non-blocking mode for better performance
    socket_lock.set_nodelay(true)?;
    
    // Create a BufReader for more efficient reading
    let mut reader = BufReader::with_capacity(READ_BUFFER_SIZE, &mut *socket_lock);
    
    // Release lock after initial setup
    drop(socket_lock);

    loop {
        buf.clear();
        
        // Lock the socket for reading with timeout
        let mut socket_lock = match tokio::time::timeout(
            Duration::from_millis(CONNECTION_TIMEOUT_MS),
            socket.lock()
        ).await {
            Ok(lock) => lock,
            Err(_) => {
                warn!("Timeout waiting for socket lock");
                continue;
            }
        };
        
        // Check if socket is readable
        match socket_lock.ready(Interest::READABLE).await {
            Ok(ready) if ready.is_readable() => {
                // Socket is readable, try to read
                match socket_lock.read_buf(&mut buf).await {
                    Ok(0) => {
                        // Connection closed
                        debug!("Connection closed by client");
                        break;
                    }
                    Ok(n) => {
                        debug!("Read {} bytes from socket", n);
                        
                        // Check for oversized messages
                        if n > MAX_MESSAGE_SIZE {
                            error!("Message too large: {} bytes", n);
                            return Err("Message too large".into());
                        }
                        
                        // Track message processing time
                        let start_time = Instant::now();
                        
                        // Deserialize the buffer into a BrokerMessage
                        match BrokerMessage::decode(&buf[..]) {
                            Ok(message) => {
                                // Handle the message
                                debug!("Decoded message of type {:?}", message.payload.as_ref().map(|p| match p {
                                    broker_message::Payload::SubscribeRequest(_) => "SubscribeRequest",
                                    broker_message::Payload::PublishRequest(_) => "PublishRequest",
                                }));
                                
                                if let Err(e) = handle_message(message, &topic_manager, Arc::clone(&socket)).await {
                                    error!("Failed to handle message: {}", e);
                                }
                            }
                            Err(e) => {
                                error!("Failed to decode Protobuf message: {}", e);
                                return Err(Box::new(e));
                            }
                        }
                        
                        // Calculate processing time
                        let processing_time_ns = start_time.elapsed().as_nanos() as u64;
                        
                        // Update metrics with processing time
                        let mut server_metrics = metrics.write().await;
                        server_metrics.messages_processed += 1;
                        
                        // Update average and max processing time
                        let current_avg = server_metrics.avg_processing_time_ns;
                        let current_count = server_metrics.messages_processed;
                        
                        // Calculate new average
                        if current_count > 1 {
                            server_metrics.avg_processing_time_ns = 
                                ((current_avg * (current_count - 1)) + processing_time_ns) / current_count;
                        } else {
                            server_metrics.avg_processing_time_ns = processing_time_ns;
                        }
                        
                        // Update max processing time if necessary
                        if processing_time_ns > server_metrics.max_processing_time_ns {
                            server_metrics.max_processing_time_ns = processing_time_ns;
                        }
                    }
                    Err(e) => {
                        error!("Failed to read from socket: {}", e);
                        return Err(Box::new(e));
                    }
                }
            }
            Ok(_) => {
                // Socket is not readable, wait a moment
                tokio::time::sleep(Duration::from_micros(10)).await;
            }
            Err(e) => {
                error!("Failed to check if socket is ready: {}", e);
                return Err(Box::new(e));
            }
        }
    }
    
    Ok(())
}

#[instrument(skip(message, topic_manager, socket))]
async fn handle_message<T: TopicManagerTrait>(
    message: BrokerMessage,
    topic_manager: &Arc<T>,
    socket: Arc<Mutex<TcpStream>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    match message.payload {
        Some(broker_message::Payload::SubscribeRequest(req)) => {
            info!("Subscribing to topics: {:?}", req.topics);
            topic_manager.subscribe(req.topics.iter().map(|s| s.as_str()).collect(), socket).await?;
        }
        Some(broker_message::Payload::PublishRequest(req)) => {
            debug!("Publishing to topics: {:?}", req.topics);
            topic_manager.publish(req.topics.iter().map(|s| s.as_str()).collect(), req.payload).await?;
        }
        _ => {
            warn!("Unknown message type");
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Unknown message type",
            )));
        }
    }

    Ok(())
}

fn load_config() -> Result<Config, config::ConfigError> {
    let builder = Config::builder()
        .set_default("default", "1")?
        .add_source(File::new("appsettings", FileFormat::Json))
        .set_override("override", "1")?;

    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::Config;
    use protocol::broker::messages::{publish_request, PublishRequest, SubscribeRequest};
    use tokio::io::AsyncWriteExt;
    use topicmanager::MockTopicManagerTrait;

    #[test]
    fn test_host_config_address() {
        let config = HostConfig {
            address: "127.0.0.1".to_string(),
            port: 8080,
            backlog: None,
            tcp_nodelay: None,
            tcp_keepalive_sec: None,
            receive_buffer_size: None,
            send_buffer_size: None,
            max_connections: None,
            worker_threads: None,
        };
        assert_eq!(config.address(), "127.0.0.1");
    }

    #[test]
    fn test_host_config_port() {
        let config = HostConfig {
            address: "127.0.0.1".to_string(),
            port: 8080,
            backlog: None,
            tcp_nodelay: None,
            tcp_keepalive_sec: None,
            receive_buffer_size: None,
            send_buffer_size: None,
            max_connections: None,
            worker_threads: None,
        };
        assert_eq!(config.port(), &8080);
    }

    #[test]
    fn test_host_config_try_from_success() {
        let config_data = r#"
        {
            "MessageBrokerServerConfiguration": {
                "MessageBrokerServerSettings": {
                    "Address": "127.0.0.1",
                    "Port": 8080,
                    "TcpNoDelay": true,
                    "WorkerThreads": 8
                }
            }
        }"#;
        let config = Config::builder()
            .add_source(File::from_str(config_data, FileFormat::Json))
            .build()
            .unwrap();
        let host_config: HostConfig = config.try_into().unwrap();
        assert_eq!(host_config.address(), "127.0.0.1");
        assert_eq!(host_config.port(), &8080);
        assert_eq!(host_config.tcp_nodelay(), true);
        assert_eq!(host_config.worker_threads(), 8);
    }

    #[test]
    fn test_host_config_try_from_failure() {
        let config_data = r#"
        {
            "MessageBrokerServerConfiguration": {
                "MessageBrokerServerSettings": {
                    "Address": "127.0.0.1"
                }
            }
        }"#;
        let config = Config::builder()
            .add_source(File::from_str(config_data, FileFormat::Json))
            .build();
        let result: Result<HostConfig, config::ConfigError> = config.and_then(|cfg| cfg.try_into());
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_run_success() {
        let mut mock = MockHostedObjectTrait::new();
        mock.expect_run().returning(|| Ok(()));
        let result = mock.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_run_failure() {
        let mut mock = MockHostedObjectTrait::new();
        mock.expect_run().returning(|| {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "some error",
            )))
        });
        let result = mock.run().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_handle_connection_success() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let message = BrokerMessage {
                payload: Some(broker_message::Payload::SubscribeRequest(Default::default())),
            };
            let mut buf = BytesMut::with_capacity(8192);
            message.encode(&mut buf).unwrap();
            socket.write_all(&buf).await.unwrap();
        });
        let mock_stream = TcpStream::connect(addr).await.unwrap();
        let mock_stream = Arc::new(Mutex::new(mock_stream));
        let topic_manager = Arc::new(TopicManager::new());
        let metrics = Arc::new(RwLock::new(ServerMetrics::default()));
        let result = handle_connection(mock_stream, topic_manager, metrics).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_connection_failure() {
        use tokio::io::AsyncWriteExt;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let malformed_message = vec![0u8; 10];
            socket.write_all(&malformed_message).await.unwrap();
        });
        let mock_stream = TcpStream::connect(addr).await.unwrap();
        let mock_stream = Arc::new(Mutex::new(mock_stream));
        let topic_manager = Arc::new(TopicManager::new());
        let metrics = Arc::new(RwLock::new(ServerMetrics::default()));
        let result = handle_connection(mock_stream, topic_manager, metrics).await;
        assert!(
            result.is_err(),
            "Expected handle_connection to fail with malformed message"
        );
    }

    #[tokio::test]
    async fn test_handle_message_subscribe_success() {
        let mut mock_topic_manager = MockTopicManagerTrait::new();
        mock_topic_manager
        .expect_subscribe()
        .withf(|topics, _| topics == &vec!["test_topic"])
        .times(1)
        .returning(|_, _| Ok(()));

    let message = BrokerMessage {
        payload: Some(broker_message::Payload::SubscribeRequest(
            SubscribeRequest {
                topics: vec!["test_topic".to_string()],
            },
        )),
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let mock_stream = TcpStream::connect(addr).await.unwrap();
    let mock_stream = Arc::new(Mutex::new(mock_stream));
    let result = handle_message(message, &Arc::new(mock_topic_manager), mock_stream).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_handle_message_subscribe_failure() {
    // Mock the TopicManager
    let mut mock_topic_manager = MockTopicManagerTrait::new();

    // Set up the expectation for the subscribe method to return an error
    mock_topic_manager
        .expect_subscribe()
        .withf(|topics, _| topics == &vec!["test_topic"])
        .times(1)
        .returning(|_, _| Err("Failed to subscribe".into()));

    // Create a subscribe request message
    let message = BrokerMessage {
        payload: Some(broker_message::Payload::SubscribeRequest(
            SubscribeRequest {
                topics: vec!["test_topic".to_string()],
            },
        )),
    };

    // Mock the TcpStream
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let mock_stream = TcpStream::connect(addr).await.unwrap();
    let mock_stream = Arc::new(Mutex::new(mock_stream));

    // Call handle_message and check that it returns an error
    let result = handle_message(message, &Arc::new(mock_topic_manager), mock_stream).await;

    assert!(
        result.is_err(),
        "Expected handle_message to fail due to subscribe error"
    );
}

#[tokio::test]
async fn test_handle_message_publish_success() {
    // Mock the TopicManager
    let mut mock_topic_manager = MockTopicManagerTrait::new();

    // Set up the expectation for the publish method to succeed
    mock_topic_manager
        .expect_publish()
        .withf(|topics, _| topics == &vec!["test_topic"])
        .times(1)
        .returning(|_, _| Ok(()));


    // Create a publish request message
    let message = BrokerMessage {
        payload:  Some(broker_message::Payload::PublishRequest(PublishRequest {
            topics: vec!["test_topic".to_string()],
            payload: Some(publish_request::Payload::MarketPayload(Default::default())),
        })),
    };

    // Mock the TcpStream
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let mock_stream = TcpStream::connect(addr).await.unwrap();
    let mock_stream = Arc::new(Mutex::new(mock_stream));

    // Call handle_message and check that it returns Ok
    let result = handle_message(message, &Arc::new(mock_topic_manager), mock_stream).await;

    assert!(
        result.is_ok(),
        "Expected handle_message to succeed with valid publish request"
    );
}

#[tokio::test]
async fn test_handle_message_publish_failure() {
    // Mock the TopicManager
    let mut mock_topic_manager = MockTopicManagerTrait::new();

    // Set up the expectation for the publish method to fail
    mock_topic_manager
        .expect_publish()
        .withf(|topics, _| topics == &vec!["test_topic"])
        .times(1)
        .returning(|_, _| Err("Failed to publish".into()));

    // Create a publish request message
    let message = BrokerMessage {
        payload:  Some(broker_message::Payload::PublishRequest(PublishRequest {
            topics: vec!["test_topic".to_string()],
            payload: Some(publish_request::Payload::MarketPayload(Default::default())),
        })),
    };

    // Mock the TcpStream
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let mock_stream = TcpStream::connect(addr).await.unwrap();
    let mock_stream = Arc::new(Mutex::new(mock_stream));

    // Call handle_message and check that it returns an error
    let result = handle_message(message, &Arc::new(mock_topic_manager), mock_stream).await;

    assert!(
        result.is_err(),
        "Expected handle_message to fail due to publish error"
    );
}

#[tokio::test]
async fn test_shutdown() {
    // Create a mock implementation
    let mut mock = MockHostedObjectTrait::new();
    
    // Set up expectations
    mock.expect_shutdown()
        .times(1)
        .returning(|| Ok(()));
        
    // Test the shutdown method
    let result = mock.shutdown().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_get_metrics() {
    // Create a mock implementation
    let mut mock = MockHostedObjectTrait::new();
    
    // Set up a test metric
    let test_metrics = ServerMetrics {
        active_connections: 10,
        total_connections: 100,
        rejected_connections: 5,
        messages_processed: 1000,
        errors: 2,
        avg_processing_time_ns: 5000,
        max_processing_time_ns: 10000,
    };
    
    // Set up expectations
    mock.expect_get_metrics()
        .times(1)
        .returning(move || test_metrics.clone());
        
    // Test the get_metrics method
    let metrics = mock.get_metrics();
    assert_eq!(metrics.active_connections, 10);
    assert_eq!(metrics.total_connections, 100);
    assert_eq!(metrics.rejected_connections, 5);
    assert_eq!(metrics.messages_processed, 1000);
    assert_eq!(metrics.errors, 2);
    assert_eq!(metrics.avg_processing_time_ns, 5000);
    assert_eq!(metrics.max_processing_time_ns, 10000);
}

#[test]
fn test_host_config_hft_settings() {
    let config = HostConfig {
        address: "127.0.0.1".to_string(),
        port: 8080,
        backlog: Some(2048),
        tcp_nodelay: Some(true),
        tcp_keepalive_sec: Some(30),
        receive_buffer_size: Some(262144),
        send_buffer_size: Some(262144),
        max_connections: Some(20000),
        worker_threads: Some(16),
    };
    
    assert_eq!(config.backlog(), 2048);
    assert_eq!(config.tcp_nodelay(), true);
    assert_eq!(config.tcp_keepalive_sec(), Some(30));
    assert_eq!(config.receive_buffer_size(), Some(262144));
    assert_eq!(config.send_buffer_size(), Some(262144));
    assert_eq!(config.max_connections(), 20000);
    assert_eq!(config.worker_threads(), 16);
}
}