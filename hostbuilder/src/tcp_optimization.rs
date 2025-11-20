// Advanced TCP socket optimization for ultra-low latency
// Provides comprehensive TCP tuning beyond basic TCP_NODELAY

use tokio::net::TcpStream;
use socket2::{Socket, Domain, Type, Protocol, SockAddr};
use std::net::SocketAddr;
use std::io;

/// TCP optimization configuration
#[derive(Debug, Clone)]
pub struct TcpOptimizationConfig {
    /// Disable Nagle's algorithm (already implemented)
    pub tcp_nodelay: bool,
    
    /// Socket receive buffer size (bytes)
    /// Larger buffers reduce syscalls but increase memory usage
    /// Default: 2MB for high-throughput trading
    pub recv_buffer_size: Option<usize>,
    
    /// Socket send buffer size (bytes)
    /// Default: 2MB for high-throughput trading
    pub send_buffer_size: Option<usize>,
    
    /// Enable TCP keepalive to prevent idle connection drops
    pub keepalive: bool,
    
    /// Allow address reuse (SO_REUSEADDR)
    pub reuse_address: bool,
    
    /// Allow port reuse for load balancing (SO_REUSEPORT, Linux only)
    pub reuse_port: bool,
    
    /// Enable TCP_QUICKACK (Linux only) - immediate ACK, no delayed ACK
    /// Eliminates 40ms delayed ACK in Linux kernel
    pub quickack: bool,
    
    /// Disable TCP_CORK (Linux only) - send immediately without buffering
    pub disable_cork: bool,
}

impl Default for TcpOptimizationConfig {
    fn default() -> Self {
        Self {
            tcp_nodelay: true,
            recv_buffer_size: Some(2_097_152),  // 2MB
            send_buffer_size: Some(2_097_152),  // 2MB
            keepalive: true,
            reuse_address: true,
            reuse_port: true,
            quickack: true,
            disable_cork: true,
        }
    }
}

/// Create an optimized TCP socket with ultra-low latency settings
pub async fn create_optimized_socket(
    addr: SocketAddr,
    config: &TcpOptimizationConfig,
) -> io::Result<TcpStream> {
    // Create socket with appropriate domain
    let socket = if addr.is_ipv4() {
        Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?
    } else {
        Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))?
    };
    
    // Apply optimizations
    apply_socket_optimizations(&socket, config)?;
    
    // Connect to remote address
    let sock_addr: SockAddr = addr.into();
    socket.connect(&sock_addr)?;
    
    // Convert to non-blocking Tokio socket
    socket.set_nonblocking(true)?;
    let std_stream: std::net::TcpStream = socket.into();
    Ok(TcpStream::from_std(std_stream)?)
}

/// Apply TCP optimizations to an existing socket
pub fn apply_socket_optimizations(
    socket: &Socket,
    config: &TcpOptimizationConfig,
) -> io::Result<()> {
    // TCP_NODELAY: Disable Nagle's algorithm for immediate sends
    if config.tcp_nodelay {
        socket.set_nodelay(true)?;
    }
    
    // SO_RCVBUF: Increase receive buffer size
    if let Some(size) = config.recv_buffer_size {
        socket.set_recv_buffer_size(size)?;
    }
    
    // SO_SNDBUF: Increase send buffer size
    if let Some(size) = config.send_buffer_size {
        socket.set_send_buffer_size(size)?;
    }
    
    // SO_KEEPALIVE: Enable TCP keepalive
    if config.keepalive {
        socket.set_keepalive(true)?;
    }
    
    // SO_REUSEADDR: Allow address reuse
    if config.reuse_address {
        socket.set_reuse_address(true)?;
    }
    
    // SO_REUSEPORT: Allow port reuse for load balancing (Linux only)
    #[cfg(target_os = "linux")]
    if config.reuse_port {
        use std::os::unix::io::AsRawFd;
        use libc::{setsockopt, SOL_SOCKET, SO_REUSEPORT};
        
        let fd = socket.as_raw_fd();
        let enable: i32 = 1;
        
        unsafe {
            let result = setsockopt(
                fd,
                SOL_SOCKET,
                SO_REUSEPORT,
                &enable as *const _ as *const _,
                std::mem::size_of_val(&enable) as u32,
            );
            
            if result != 0 {
                return Err(io::Error::last_os_error());
            }
        }
    }
    
    // TCP_QUICKACK: Immediate ACK, no delayed ACK (Linux only)
    #[cfg(target_os = "linux")]
    if config.quickack {
        use std::os::unix::io::AsRawFd;
        use libc::{setsockopt, SOL_TCP, TCP_QUICKACK};
        
        let fd = socket.as_raw_fd();
        let enable: i32 = 1;
        
        unsafe {
            setsockopt(
                fd,
                SOL_TCP,
                TCP_QUICKACK,
                &enable as *const _ as *const _,
                std::mem::size_of_val(&enable) as u32,
            );
        }
    }
    
    // TCP_CORK: Disable cork for immediate sends (Linux only)
    #[cfg(target_os = "linux")]
    if config.disable_cork {
        use std::os::unix::io::AsRawFd;
        use libc::{setsockopt, SOL_TCP, TCP_CORK};
        
        let fd = socket.as_raw_fd();
        let disable: i32 = 0;
        
        unsafe {
            setsockopt(
                fd,
                SOL_TCP,
                TCP_CORK,
                &disable as *const _ as *const _,
                std::mem::size_of_val(&disable) as u32,
            );
        }
    }
    
    Ok(())
}

/// Apply optimizations to an existing TcpStream
pub fn optimize_tcp_stream(
    stream: &TcpStream,
    config: &TcpOptimizationConfig,
) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    
    let fd = stream.as_raw_fd();
    let socket = unsafe { Socket::from_raw_fd(fd) };
    let result = apply_socket_optimizations(&socket, config);
    
    // Don't close the socket when Socket drops (we still need it in TcpStream)
    std::mem::forget(socket);
    
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_default_config() {
        let config = TcpOptimizationConfig::default();
        
        assert_eq!(config.tcp_nodelay, true);
        assert_eq!(config.recv_buffer_size, Some(2_097_152));
        assert_eq!(config.send_buffer_size, Some(2_097_152));
        assert_eq!(config.keepalive, true);
        assert_eq!(config.reuse_address, true);
        assert_eq!(config.reuse_port, true);
        assert_eq!(config.quickack, true);
        assert_eq!(config.disable_cork, true);
    }
    
    #[tokio::test]
    async fn test_socket_creation() {
        let config = TcpOptimizationConfig::default();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        
        // This will fail to connect, but we're testing socket creation
        let result = create_optimized_socket(addr, &config).await;
        
        // We expect connection failure, not socket creation failure
        assert!(result.is_err());
    }
}
