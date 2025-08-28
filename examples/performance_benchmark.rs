// Performance benchmark comparing baseline vs optimized MessageBroker
// This will validate our optimization improvements against the simulation baseline

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{info, Level};

use publisher::{
    OptimizedPublisher, BatchingConfig,
    cpu_optimization::{CpuOptimizer, CpuAffinityConfig},
    memory_optimization::{MessageBufferPool, PoolConfig, ZeroCopyMessageBuilder}
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();

    info!("🏁 MessageBrokerEngine Performance Benchmark");
    info!("📊 Target: Validate optimization improvements against 14.31ms baseline");

    // Expected baseline from simulation: 14.31ms average latency
    let baseline_latency_ms = 14.31;
    let target_improvement = 0.5; // Target <500μs for algorithmic trading
    
    println!("\n🎯 PERFORMANCE BENCHMARK GOALS");
    println!("==============================");
    println!("📈 Baseline Average Latency: {:.2}ms", baseline_latency_ms);
    println!("🎯 Target Latency: <{:.2}ms (algorithmic trading)", target_improvement);
    println!("📊 Expected Improvement: >95% latency reduction");
    println!("🚀 MessageBroker Bottleneck: 35% → <5%");

    // 1. Setup CPU Optimizations
    info!("⚙️  Configuring CPU optimizations...");
    let cpu_config = CpuAffinityConfig::default();
    let _cpu_optimizer = CpuOptimizer::new(cpu_config);
    
    // 2. Setup Memory Pools
    info!("🧠 Initializing optimized memory pools...");
    let pool_config = PoolConfig {
        small_buffer_size: 1024,
        medium_buffer_size: 8192,
        large_buffer_size: 65536,
        small_pool_size: 2000,    // High-frequency trading needs
        medium_pool_size: 1000,
        large_pool_size: 200,
        enable_preallocation: true,
        auto_scaling: true,
    };
    let buffer_pool = Arc::new(MessageBufferPool::new(pool_config));
    
    // 3. Configure Optimized Publisher
    info!("📤 Setting up OptimizedPublisher...");
    let batching_config = BatchingConfig {
        max_batch_size: 100,          // Aggressive batching
        max_flush_delay_us: 50,       // 50μs max delay
        adaptive_batching: true,
        memory_pool_size: 2000,
        priority_batching: true,
    };
    
    // Note: Using localhost for benchmark - in production this would be actual MessageBroker
    let publisher = OptimizedPublisher::new(
        "127.0.0.1:9092".to_string(),
        batching_config,
        buffer_pool.clone(),
    ).await?;

    info!("✅ All optimizations configured successfully");

    // 4. Benchmark Scenario
    println!("\n🏃 BENCHMARK EXECUTION");
    println!("======================");
    
    let benchmark_duration = Duration::from_secs(10);
    let target_throughput = 1000; // 1K msg/s (higher than simulation)
    let total_messages = (benchmark_duration.as_secs() * target_throughput) as usize;
    
    info!("📈 Benchmark Parameters:");
    info!("   Duration: {:?}", benchmark_duration);
    info!("   Target Throughput: {} msg/sec", target_throughput);
    info!("   Total Messages: {}", total_messages);

    // Simulate realistic DataEngine message patterns
    let topics = vec![
        "market.data.kraken.level3",
        "market.data.kraken.trades", 
        "market.data.kraken.balances"
    ];

    let start_time = Instant::now();
    let mut publish_latencies = Vec::with_capacity(total_messages);
    let mut messages_sent = 0;

    info!("🚀 Starting optimized message publishing...");
    
    let benchmark_start = Instant::now();
    while benchmark_start.elapsed() < benchmark_duration {
        // Batch processing
        for i in 0..50 {
            if messages_sent >= total_messages {
                break;
            }
            
            let topic = &topics[messages_sent % topics.len()];
            let payload = generate_realistic_message(messages_sent);
            
            // Use optimized zero-copy message building
            let mut builder = ZeroCopyMessageBuilder::new(buffer_pool.clone());
            builder.start_message(payload.len() + topic.len() + 32);
            builder.add_topic(topic);
            builder.add_payload(&payload);
            let message_buffer = builder.build().expect("Message build failed");
            
            let publish_start = Instant::now();
            publisher.publish_raw(topic, message_buffer).await?;
            let publish_latency = publish_start.elapsed();
            
            publish_latencies.push(publish_latency);
            messages_sent += 1;
        }
        
        // Small yield to prevent overwhelming
        tokio::task::yield_now().await;
    }
    
    let total_duration = start_time.elapsed();
    let actual_throughput = messages_sent as f64 / total_duration.as_secs_f64();

    // 5. Performance Analysis
    println!("\n📊 OPTIMIZATION PERFORMANCE RESULTS");
    println!("=====================================");
    
    analyze_benchmark_results(
        messages_sent,
        total_duration,
        &publish_latencies,
        baseline_latency_ms,
        target_improvement,
        actual_throughput,
        &buffer_pool
    );

    // 6. Memory Pool Health Check
    println!("\n🧠 MEMORY OPTIMIZATION VALIDATION");
    println!("==================================");
    let health_report = buffer_pool.health_check();
    health_report.print_report();

    println!("\n🎉 BENCHMARK COMPLETED");
    println!("======================");
    info!("📋 Performance validation complete");
    info!("💡 Results show optimized MessageBrokerEngine performance");

    Ok(())
}

fn generate_realistic_message(sequence: usize) -> Vec<u8> {
    // Generate realistic market data message similar to DataEngine output
    let message = format!(
        r#"{{"type":"level3","sequence":{},"timestamp":"{}","product_id":"BTC-USD","side":"{}","order_id":"{}","size":"{:.8}","price":"{:.2}","order_type":"limit"}}"#,
        sequence,
        chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ"),
        if sequence % 2 == 0 { "buy" } else { "sell" },
        format!("order_{}", sequence),
        0.01 + (sequence as f64 * 0.001) % 1.0,
        45000.0 + (sequence as f64 * 0.1) % 1000.0
    );
    message.into_bytes()
}

fn analyze_benchmark_results(
    messages_sent: usize,
    total_duration: Duration,
    publish_latencies: &[Duration],
    baseline_latency_ms: f64,
    target_latency_ms: f64,
    throughput: f64,
    buffer_pool: &MessageBufferPool,
) {
    let mut sorted_latencies: Vec<_> = publish_latencies.iter()
        .map(|d| d.as_nanos() as f64 / 1_000_000.0) // Convert to milliseconds
        .collect();
    sorted_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let mean_latency = sorted_latencies.iter().sum::<f64>() / sorted_latencies.len() as f64;
    let p50_latency = sorted_latencies[sorted_latencies.len() / 2];
    let p95_latency = sorted_latencies[sorted_latencies.len() * 95 / 100];
    let p99_latency = sorted_latencies[sorted_latencies.len() * 99 / 100];
    let max_latency = *sorted_latencies.last().unwrap();

    println!("📈 Throughput: {:.0} messages/sec", throughput);
    println!("⏱️  Mean Publish Latency: {:.3}ms", mean_latency);
    println!("📊 P50 Latency: {:.3}ms", p50_latency);
    println!("📊 P95 Latency: {:.3}ms", p95_latency);
    println!("📊 P99 Latency: {:.3}ms", p99_latency);
    println!("📊 Max Latency: {:.3}ms", max_latency);

    // Performance improvement analysis
    let improvement_factor = baseline_latency_ms / mean_latency;
    let improvement_percent = (1.0 - (mean_latency / baseline_latency_ms)) * 100.0;

    println!("\n🎯 OPTIMIZATION IMPACT ANALYSIS");
    println!("================================");
    println!("📉 Baseline Latency: {:.2}ms", baseline_latency_ms);
    println!("⚡ Optimized Latency: {:.3}ms", mean_latency);
    println!("📈 Improvement Factor: {:.1}x faster", improvement_factor);
    println!("📊 Latency Reduction: {:.1}%", improvement_percent);

    // Performance classification
    println!("\n🏆 PERFORMANCE CLASSIFICATION:");
    if mean_latency < target_latency_ms {
        println!("🏆 EXCELLENT - Target achieved! (<{:.1}ms)", target_latency_ms);
        println!("✅ Suitable for high-frequency algorithmic trading");
    } else if mean_latency < 1.0 {
        println!("✅ VERY GOOD - Sub-millisecond latency achieved");
        println!("✅ Suitable for most algorithmic trading strategies");
    } else if mean_latency < 5.0 {
        println!("👍 GOOD - Significant improvement over baseline");
        println!("⚠️  May require additional optimization for HFT");
    } else if mean_latency < baseline_latency_ms {
        println!("📈 IMPROVED - Better than baseline");
        println!("🔄 Additional optimization recommended");
    } else {
        println!("🔴 NO IMPROVEMENT - Optimization ineffective");
        println!("🚨 Requires investigation and re-optimization");
    }

    // Memory efficiency
    let pool_stats = buffer_pool.get_statistics();
    println!("\n🧠 Memory Optimization Efficiency:");
    println!("├─ Cache Hit Rate: {:.1}%", pool_stats.hit_rate() * 100.0);
    println!("├─ Memory Efficiency: {:.1}%", pool_stats.memory_efficiency() * 100.0);
    println!("└─ Total Allocations: {}", pool_stats.total_allocated());

    // Success criteria validation
    println!("\n✅ SUCCESS CRITERIA VALIDATION:");
    println!("├─ Latency Target: {} {}", 
        if mean_latency < target_latency_ms { "✅ ACHIEVED" } else { "❌ MISSED" },
        format!("(<{:.1}ms)", target_latency_ms));
    println!("├─ Improvement: {} {}", 
        if improvement_percent > 50.0 { "✅ SIGNIFICANT" } else { "⚠️  MODERATE" },
        format!("({:.1}%)", improvement_percent));
    println!("├─ Memory Efficiency: {} {}", 
        if pool_stats.hit_rate() > 0.9 { "✅ EXCELLENT" } else { "⚠️  GOOD" },
        format!("({:.1}%)", pool_stats.hit_rate() * 100.0));
    println!("└─ Throughput: {} {}", 
        if throughput > 500.0 { "✅ HIGH" } else { "⚠️  MODERATE" },
        format!("({:.0} msg/s)", throughput));
}
