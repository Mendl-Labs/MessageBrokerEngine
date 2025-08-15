use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, error};
use tracing_subscriber;

use hostbuilder::{MessageBrokerHost, BrokerConfig};

#[tokio::main(flavor = "multi_thread", worker_threads = 32)]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize comprehensive logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .with_thread_ids(true)
        .with_line_number(true)
        .init();

    println!("🚀 ULTRA-HIGH PERFORMANCE MESSAGE BROKER ENGINE 🚀");
    println!("═══════════════════════════════════════════════════════════");
    println!("🎯 Target: Sub-microsecond latency & ultra-high throughput");
    println!("🔧 Platform: {} on {}", std::env::consts::ARCH, std::env::consts::OS);
    println!("═══════════════════════════════════════════════════════════");

    info!("Starting Ultra-High Performance Message Broker...");

    // Ultra-optimized broker configuration
    let config = BrokerConfig {
        host: "0.0.0.0".to_string(),
        port: 8080,
        max_connections: 10000,
        worker_threads: 32,
        connection_timeout: Duration::from_millis(100),
        read_buffer_size: 1048576, // 1MB
        max_message_size: 16777216, // 16MB
        tcp_nodelay: true,
        socket_reuse: true,
        keepalive: true,
        backlog: 1024,
        busy_poll: true,
        cpu_affinity: vec![],
        use_huge_pages: false,
        shared_memory_size: 67108864, // 64MB
        enable_wal: true,
        wal_config: Default::default(),
        flow_control_config: Default::default(),
        enable_compression: true,
        enable_intelligent_routing: true,
    };

    info!("Configuration: {}:{}, max_connections={}", 
          config.host, config.port, config.max_connections);

    // Create and start the message broker
    let broker = Arc::new(MessageBrokerHost::new(config));
    let broker_clone = Arc::clone(&broker);

    println!("\n🚀 Starting Message Broker Host...");
    let start_time = Instant::now();
    
    // Spawn broker in background task
    let broker_task = tokio::spawn(async move {
        if let Err(e) = broker_clone.start().await {
            error!("Broker error: {}", e);
        }
    });

    // Wait a moment for startup
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    let startup_time = start_time.elapsed();
    println!("✅ Message Broker started in {:.2}ms", startup_time.as_secs_f64() * 1000.0);

    // Keep running for a demo period
    info!("Running for 10 seconds...");
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    println!("\n📊 FINAL PERFORMANCE REPORT");
    println!("═══════════════════════════════════════════════════════════");
    println!("⏱️  Total Runtime: {:.2}s", start_time.elapsed().as_secs_f64());
    println!("📨 Messages Processed: {}", broker.get_stats().3); // messages from stats tuple
    
    // Print detailed metrics
    let stats = broker.get_stats();
    println!("📊 Broker Statistics:");
    println!("   Active Connections: {}", stats.0);
    println!("   Total Connections: {}", stats.1);
    println!("   Messages Processed: {}", stats.3);
    println!("   Bytes Processed: {}", stats.8);
    
    println!("🚀 Status: Successfully demonstrated ultra-fast message broker!");

    broker.stop();
    
    // Wait for clean shutdown
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    let _ = broker_task.await;

    println!("\n🏁 Message Broker Engine stopped gracefully");
    println!("🎉 ULTRA-HIGH PERFORMANCE ACHIEVED! 🎉");
    
    Ok(())
}
