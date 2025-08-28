// Deep Performance Profiler for MessageBrokerEngine
// Identifies specific bottlenecks in our optimized system

use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use tracing::{info, Level};

use publisher::{
    cpu_optimization::{CpuOptimizer, CpuAffinityConfig},
    memory_optimization::{MessageBufferPool, PoolConfig, ZeroCopyMessageBuilder}
};

#[derive(Debug, Clone)]
struct ProfilingMetrics {
    operation_name: String,
    total_time: Duration,
    min_time: Duration,
    max_time: Duration,
    call_count: usize,
    samples: Vec<Duration>,
}

impl ProfilingMetrics {
    fn new(operation_name: String) -> Self {
        Self {
            operation_name,
            total_time: Duration::ZERO,
            min_time: Duration::MAX,
            max_time: Duration::ZERO,
            call_count: 0,
            samples: Vec::new(),
        }
    }

    fn record(&mut self, duration: Duration) {
        self.total_time += duration;
        self.min_time = self.min_time.min(duration);
        self.max_time = self.max_time.max(duration);
        self.call_count += 1;
        self.samples.push(duration);
    }

    fn average(&self) -> Duration {
        if self.call_count > 0 {
            self.total_time / self.call_count as u32
        } else {
            Duration::ZERO
        }
    }

    fn percentile(&self, p: f64) -> Duration {
        if self.samples.is_empty() {
            return Duration::ZERO;
        }
        let mut sorted = self.samples.clone();
        sorted.sort();
        let index = ((sorted.len() as f64 - 1.0) * p / 100.0).round() as usize;
        sorted[index.min(sorted.len() - 1)]
    }
}

struct DeepProfiler {
    metrics: HashMap<String, ProfilingMetrics>,
}

impl DeepProfiler {
    fn new() -> Self {
        Self {
            metrics: HashMap::new(),
        }
    }

    fn start_operation(&self, _name: &str) -> Instant {
        Instant::now()
    }

    fn end_operation(&mut self, name: &str, start_time: Instant) {
        let duration = start_time.elapsed();
        let metric = self.metrics.entry(name.to_string())
            .or_insert_with(|| ProfilingMetrics::new(name.to_string()));
        metric.record(duration);
    }

    fn print_detailed_report(&self) {
        println!("\n🔍 DEEP PERFORMANCE PROFILING REPORT");
        println!("=====================================");
        
        let mut sorted_metrics: Vec<_> = self.metrics.values().collect();
        sorted_metrics.sort_by_key(|m| std::cmp::Reverse(m.average()));

        let total_operations = sorted_metrics.iter().map(|m| m.call_count).sum::<usize>();
        let total_time: Duration = sorted_metrics.iter().map(|m| m.total_time).sum();

        println!("📊 SUMMARY:");
        println!("├─ Total Operations: {}", total_operations);
        println!("├─ Total Time: {:.3}ms", total_time.as_nanos() as f64 / 1_000_000.0);
        println!("└─ Overall Average: {:.3}ms per operation", 
            (total_time.as_nanos() as f64 / total_operations as f64) / 1_000_000.0);

        println!("\n📈 PERFORMANCE BREAKDOWN BY OPERATION:");
        println!("=====================================");

        for (i, metric) in sorted_metrics.iter().enumerate() {
            let avg_ms = metric.average().as_nanos() as f64 / 1_000_000.0;
            let total_ms = metric.total_time.as_nanos() as f64 / 1_000_000.0;
            let percentage = (total_ms / (total_time.as_nanos() as f64 / 1_000_000.0)) * 100.0;
            let min_ms = metric.min_time.as_nanos() as f64 / 1_000_000.0;
            let max_ms = metric.max_time.as_nanos() as f64 / 1_000_000.0;
            let p95_ms = metric.percentile(95.0).as_nanos() as f64 / 1_000_000.0;

            println!("\n{}. 🎯 {}", i + 1, metric.operation_name);
            println!("   ├─ Average: {:.3}ms", avg_ms);
            println!("   ├─ Total Time: {:.3}ms ({:.1}%)", total_ms, percentage);
            println!("   ├─ Call Count: {}", metric.call_count);
            println!("   ├─ Min/Max: {:.3}ms / {:.3}ms", min_ms, max_ms);
            println!("   └─ P95: {:.3}ms", p95_ms);

            // Identify bottlenecks
            if percentage > 20.0 {
                println!("   🚨 MAJOR BOTTLENECK - Consuming {:.1}% of total time", percentage);
            } else if percentage > 10.0 {
                println!("   ⚠️  SIGNIFICANT IMPACT - {:.1}% of total time", percentage);
            } else if percentage > 5.0 {
                println!("   📊 MODERATE IMPACT - {:.1}% of total time", percentage);
            }
        }

        println!("\n🎯 OPTIMIZATION RECOMMENDATIONS:");
        println!("================================");
        
        // Top 3 bottlenecks
        for (i, metric) in sorted_metrics.iter().take(3).enumerate() {
            let total_ms = metric.total_time.as_nanos() as f64 / 1_000_000.0;
            let percentage = (total_ms / (total_time.as_nanos() as f64 / 1_000_000.0)) * 100.0;
            
            println!("{}. Focus on '{}' - {:.1}% of total time", i + 1, metric.operation_name, percentage);
            
            // Specific recommendations
            match metric.operation_name.as_str() {
                "message_build" => println!("   💡 Consider: Zero-copy serialization, pre-computed headers"),
                "async_publish" => println!("   💡 Consider: Batching, connection pooling, kernel bypass"),
                "memory_allocation" => println!("   💡 Consider: Larger buffer pools, custom allocators"),
                "cpu_optimization" => println!("   💡 Consider: Thread affinity, SIMD instructions"),
                "network_io" => println!("   💡 Consider: DPDK, RDMA, UDP instead of TCP"),
                "serialization" => println!("   💡 Consider: FlatBuffers, Cap'n Proto, custom binary protocol"),
                "queue_operations" => println!("   💡 Consider: Lock-free queues, SPSC channels"),
                _ => println!("   💡 Consider: Algorithm optimization, caching, parallelization"),
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();

    info!("🔬 Deep Performance Profiler Starting");
    info!("🎯 Target: Identify remaining 9.85ms bottlenecks");

    let mut profiler = DeepProfiler::new();

    // 1. Setup optimizations with profiling
    info!("⚙️  Setting up optimizations...");
    let setup_start = profiler.start_operation("optimization_setup");
    
    let cpu_config = CpuAffinityConfig::default();
    let _cpu_optimizer = CpuOptimizer::new(cpu_config);
    
    let pool_config = PoolConfig {
        small_buffer_size: 1024,
        medium_buffer_size: 8192,
        large_buffer_size: 65536,
        small_pool_size: 2000,
        medium_pool_size: 1000,
        large_pool_size: 200,
        enable_preallocation: true,
        auto_scaling: true,
    };
    let buffer_pool = Arc::new(MessageBufferPool::new(pool_config));
    
    profiler.end_operation("optimization_setup", setup_start);

    // 2. Run comprehensive profiled test
    info!("🚀 Starting deep profiling session...");
    
    let test_duration = Duration::from_secs(5); // Shorter for detailed analysis
    let target_messages = 1000; // Focus on quality over quantity
    let mut messages_processed = 0;

    let test_start = Instant::now();
    while test_start.elapsed() < test_duration && messages_processed < target_messages {
        
        // Profile: Message Generation
        let msg_gen_start = profiler.start_operation("message_generation");
        let topic = "market.data.kraken.level3";
        let payload = generate_realistic_message(messages_processed);
        profiler.end_operation("message_generation", msg_gen_start);

        // Profile: Memory Allocation
        let mem_alloc_start = profiler.start_operation("memory_allocation");
        let mut builder = ZeroCopyMessageBuilder::new(buffer_pool.clone());
        profiler.end_operation("memory_allocation", mem_alloc_start);

        // Profile: Message Building
        let msg_build_start = profiler.start_operation("message_build");
        builder.start_message(payload.len() + topic.len() + 64);
        builder.add_topic(topic);
        builder.add_payload(&payload);
        let message_buffer = builder.build().expect("Message build failed");
        profiler.end_operation("message_build", msg_build_start);

        // Profile: CPU Optimization
        let cpu_opt_start = profiler.start_operation("cpu_optimization");
        // Simulate CPU-intensive work (encoding, compression, etc.)
        let _checksum = calculate_message_checksum(&message_buffer);
        profiler.end_operation("cpu_optimization", cpu_opt_start);

        // Profile: Serialization
        let serialization_start = profiler.start_operation("serialization");
        let serialized_message = serialize_message(&message_buffer, topic);
        profiler.end_operation("serialization", serialization_start);

        // Profile: Queue Operations
        let queue_start = profiler.start_operation("queue_operations");
        // Simulate queue/batching operations
        simulate_queue_operations(&serialized_message);
        profiler.end_operation("queue_operations", queue_start);

        // Profile: Network I/O Simulation (the big one!)
        let network_start = profiler.start_operation("network_io");
        simulate_realistic_network_io(&serialized_message, messages_processed).await;
        profiler.end_operation("network_io", network_start);

        // Profile: Async Publish
        let publish_start = profiler.start_operation("async_publish");
        simulate_async_publish_overhead().await;
        profiler.end_operation("async_publish", publish_start);

        // Profile: Memory Cleanup
        let cleanup_start = profiler.start_operation("memory_cleanup");
        // Message buffer automatically cleaned up by RAII
        std::mem::drop(message_buffer);
        profiler.end_operation("memory_cleanup", cleanup_start);

        messages_processed += 1;

        // Yield occasionally to prevent busy-waiting
        if messages_processed % 50 == 0 {
            tokio::task::yield_now().await;
        }
    }

    let total_test_time = test_start.elapsed();
    
    info!("✅ Deep profiling completed");
    info!("📊 Processed {} messages in {:.3}ms", messages_processed, 
        total_test_time.as_nanos() as f64 / 1_000_000.0);

    // 3. Generate comprehensive analysis
    profiler.print_detailed_report();

    // 4. Additional system-level analysis
    print_system_analysis(&buffer_pool, messages_processed, total_test_time);

    // 5. Bottleneck identification
    identify_critical_bottlenecks(&profiler);

    println!("\n🎯 DEEP PROFILING COMPLETED");
    println!("============================");
    info!("🔬 Detailed performance analysis complete");
    info!("💡 Use results to target specific optimizations");

    Ok(())
}

fn generate_realistic_message(sequence: usize) -> Vec<u8> {
    // Generate realistic market data with variable size
    let base_size = 200 + (sequence % 300); // 200-500 bytes
    let mut message = Vec::with_capacity(base_size);
    
    // Simulate JSON market data
    let json_data = format!(
        r#"{{"type":"level3","sequence":{},"timestamp":"{}","product_id":"BTC-USD","side":"{}","order_id":"order_{}","size":"{:.8}","price":"{:.2}","maker_order_id":"maker_{}","remaining_size":"{:.8}"}}"#,
        sequence,
        chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ"),
        if sequence % 2 == 0 { "buy" } else { "sell" },
        sequence,
        0.001 + (sequence as f64 * 0.0001) % 1.0,
        45000.0 + (sequence as f64 * 0.1) % 2000.0,
        sequence + 1000,
        0.0005 + (sequence as f64 * 0.00005) % 0.5
    );
    
    message.extend_from_slice(json_data.as_bytes());
    
    // Pad to target size for realistic message sizes
    while message.len() < base_size {
        message.push(b' ');
    }
    
    message
}

fn calculate_message_checksum(message: &[u8]) -> u32 {
    // Simulate CPU-intensive checksum calculation
    let mut checksum = 0u32;
    for (i, &byte) in message.iter().enumerate() {
        checksum = checksum.wrapping_add(byte as u32 * (i as u32 + 1));
        // Add some computational complexity
        checksum = checksum.wrapping_mul(1103515245).wrapping_add(12345);
    }
    checksum
}

fn serialize_message(message: &[u8], topic: &str) -> Vec<u8> {
    // Simulate message serialization overhead
    let mut serialized = Vec::with_capacity(message.len() + topic.len() + 32);
    
    // Add headers (simulate protocol overhead)
    serialized.extend_from_slice(b"MSGV1");
    serialized.extend_from_slice(&(topic.len() as u32).to_be_bytes());
    serialized.extend_from_slice(topic.as_bytes());
    serialized.extend_from_slice(&(message.len() as u32).to_be_bytes());
    serialized.extend_from_slice(message);
    
    // Add checksum
    let checksum = calculate_message_checksum(message);
    serialized.extend_from_slice(&checksum.to_be_bytes());
    
    serialized
}

fn simulate_queue_operations(message: &[u8]) {
    // Simulate queue/batching operations overhead
    let _queue_position = message.len() % 1000;
    let _batch_id = message.len() / 100;
    
    // Simulate some queue management work
    for i in 0..10 {
        let _priority = (message[i % message.len()] as usize + i) % 3;
    }
}

async fn simulate_realistic_network_io(message: &[u8], sequence: usize) {
    // This simulates the network I/O that was our biggest bottleneck
    // in the previous performance test
    
    let base_network_delay = Duration::from_micros(100); // Base network stack overhead
    let size_factor = message.len() / 100; // Larger messages take longer
    let congestion_factor = if sequence % 10 == 0 { 2 } else { 1 }; // Simulate network congestion
    
    let network_delay = base_network_delay * (size_factor as u32 + congestion_factor);
    
    tokio::time::sleep(network_delay).await;
}

async fn simulate_async_publish_overhead() {
    // Simulate async/await overhead, task scheduling, etc.
    let async_overhead = Duration::from_micros(20);
    tokio::time::sleep(async_overhead).await;
    
    // Simulate some async coordination work
    tokio::task::yield_now().await;
}

fn print_system_analysis(buffer_pool: &MessageBufferPool, messages: usize, duration: Duration) {
    println!("\n🖥️  SYSTEM-LEVEL ANALYSIS");
    println!("=========================");
    
    let throughput = messages as f64 / duration.as_secs_f64();
    let avg_latency_ms = duration.as_nanos() as f64 / (messages as f64 * 1_000_000.0);
    
    println!("⚡ System Performance:");
    println!("├─ Messages Processed: {}", messages);
    println!("├─ Total Duration: {:.3}ms", duration.as_nanos() as f64 / 1_000_000.0);
    println!("├─ Throughput: {:.1} msg/sec", throughput);
    println!("└─ Average Latency: {:.3}ms", avg_latency_ms);
    
    println!("\n🧠 Memory Pool Analysis:");
    let stats = buffer_pool.get_statistics();
    println!("├─ Cache Hit Rate: {:.1}%", stats.hit_rate() * 100.0);
    println!("├─ Total Allocations: {}", stats.total_allocated());
    println!("├─ Memory Efficiency: {:.1}%", stats.memory_efficiency() * 100.0);
    println!("└─ Pool Health: {:.0}/100", buffer_pool.health_check().health_score);
}

fn identify_critical_bottlenecks(profiler: &DeepProfiler) {
    println!("\n🚨 CRITICAL BOTTLENECK IDENTIFICATION");
    println!("====================================");
    
    let mut bottlenecks = Vec::new();
    let total_time: Duration = profiler.metrics.values().map(|m| m.total_time).sum();
    
    for metric in profiler.metrics.values() {
        let percentage = (metric.total_time.as_nanos() as f64 / total_time.as_nanos() as f64) * 100.0;
        let avg_ms = metric.average().as_nanos() as f64 / 1_000_000.0;
        
        if percentage > 15.0 || avg_ms > 1.0 {
            bottlenecks.push((metric.operation_name.clone(), percentage, avg_ms));
        }
    }
    
    bottlenecks.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    
    if bottlenecks.is_empty() {
        println!("✅ No critical bottlenecks identified");
        println!("💡 Performance is well-distributed across operations");
    } else {
        println!("🚨 Critical bottlenecks found:");
        for (i, (name, percentage, avg_ms)) in bottlenecks.iter().enumerate() {
            println!("{}. {} - {:.1}% of time, {:.3}ms average", 
                i + 1, name, percentage, avg_ms);
                
            // Specific action items
            match name.as_str() {
                "network_io" => {
                    println!("   🎯 ACTION: Implement kernel bypass (DPDK), UDP transport");
                    println!("   🎯 ACTION: Connection pooling, persistent connections");
                }
                "message_build" => {
                    println!("   🎯 ACTION: Pre-computed message templates");
                    println!("   🎯 ACTION: Zero-copy serialization with FlatBuffers");
                }
                "serialization" => {
                    println!("   🎯 ACTION: Binary protocols instead of JSON");
                    println!("   🎯 ACTION: SIMD-optimized serialization");
                }
                "async_publish" => {
                    println!("   🎯 ACTION: Reduce async overhead with sync batching");
                    println!("   🎯 ACTION: Custom task scheduler for publishing");
                }
                _ => {
                    println!("   🎯 ACTION: Profile deeper, consider algorithm changes");
                }
            }
        }
    }
    
    println!("\n💡 NEXT STEPS:");
    println!("==============");
    if let Some((top_bottleneck, percentage, _)) = bottlenecks.first() {
        println!("🎯 Priority 1: Optimize '{}' ({:.1}% of total time)", top_bottleneck, percentage);
        println!("📈 Expected impact: {:.1}ms reduction if eliminated", 
            percentage / 100.0 * (total_time.as_nanos() as f64 / 1_000_000.0));
    }
    
    if bottlenecks.len() > 1 {
        let (second_bottleneck, percentage, _) = &bottlenecks[1];
        println!("🎯 Priority 2: Optimize '{}' ({:.1}% of total time)", second_bottleneck, percentage);
    }
}
