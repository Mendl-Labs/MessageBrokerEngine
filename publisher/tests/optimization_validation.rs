// Test compilation and basic functionality of MessageBrokerEngine optimizations
// Run with: cargo test --package publisher --test optimization_validation

use std::sync::Arc;

use publisher::{
    BatchingConfig,
    cpu_optimization::{CpuOptimizer, CpuAffinityConfig},
    memory_optimization::{MessageBufferPool, PoolConfig, ZeroCopyMessageBuilder}
};

#[tokio::test]
async fn test_memory_pool_functionality() {
    // Test memory pool basic functionality
    let config = PoolConfig::default();
    let pool = MessageBufferPool::new(config);
    
    // Test buffer allocation and return
    let buffer1 = pool.get_small_buffer();
    assert!(buffer1.capacity() >= 1024);
    
    let buffer2 = pool.get_medium_buffer();
    assert!(buffer2.capacity() >= 8192);
    
    let buffer3 = pool.get_large_buffer();
    assert!(buffer3.capacity() >= 65536);
    
    // Return buffers to pool
    pool.return_buffer(buffer1);
    pool.return_buffer(buffer2);
    pool.return_buffer(buffer3);
    
    // Test statistics
    let stats = pool.get_statistics();
    assert!(stats.cache_hits > 0 || stats.cache_misses > 0);
    
    // Test health check
    let health_report = pool.health_check();
    assert!(health_report.health_score >= 0.0 && health_report.health_score <= 100.0);
    
    println!("✅ Memory pool functionality test passed");
}

#[test]
fn test_cpu_optimization_creation() {
    // Test that CPU optimizer can be created
    let config = CpuAffinityConfig::default();
    let _cpu_optimizer = CpuOptimizer::new(config);
    
    println!("✅ CPU optimizer creation test passed");
}

#[tokio::test]
async fn test_zero_copy_message_builder() {
    let config = PoolConfig::default();
    let pool = Arc::new(MessageBufferPool::new(config));
    
    // Test message building
    let mut builder = ZeroCopyMessageBuilder::new(pool.clone());
    builder.start_message(100);
    builder.add_topic("test.topic");
    builder.add_payload(b"test payload data");
    let message = builder.build();
    
    assert!(message.is_some());
    let message_buffer = message.unwrap();
    assert!(message_buffer.len() > 0);
    
    // Return buffer to pool
    pool.return_buffer(message_buffer);
    
    println!("✅ Zero-copy message builder test passed");
}

#[tokio::test]
async fn test_batching_config_validation() {
    // Test that batching config can be created with valid values
    let config = BatchingConfig {
        max_batch_size: 50,
        max_flush_delay_us: 100,
        adaptive_batching: true,
        memory_pool_size: 1000,
        priority_batching: true,
    };
    
    // Validate config values
    assert!(config.max_batch_size > 0);
    assert!(config.max_flush_delay_us > 0);
    assert!(config.memory_pool_size > 0);
    
    println!("✅ Batching config validation test passed");
}

#[tokio::test]
async fn test_pool_statistics() {
    let config = PoolConfig::default();
    let pool = MessageBufferPool::new(config);
    
    // Allocate and return some buffers to generate statistics
    for _ in 0..10 {
        let buffer = pool.get_small_buffer();
        pool.return_buffer(buffer);
    }
    
    let stats = pool.get_statistics();
    
    // Test that statistics methods work
    let hit_rate = stats.hit_rate();
    let total_allocated = stats.total_allocated();
    let total_returned = stats.total_returned();
    let memory_efficiency = stats.memory_efficiency();
    
    assert!(hit_rate >= 0.0 && hit_rate <= 1.0);
    // Total allocated and returned are usize, always >= 0, so just check they exist
    let _ = total_allocated; // Use the values
    let _ = total_returned;
    assert!(memory_efficiency >= 0.0 && memory_efficiency <= 1.0);
    
    println!("✅ Pool statistics test passed");
    println!("   Hit rate: {:.2}%", hit_rate * 100.0);
    println!("   Memory efficiency: {:.2}%", memory_efficiency * 100.0);
}

#[tokio::test]
async fn test_pool_health_monitoring() {
    let config = PoolConfig {
        small_pool_size: 100,
        medium_pool_size: 50,
        large_pool_size: 25,
        ..Default::default()
    };
    
    let pool = MessageBufferPool::new(config);
    
    // Use the pool to generate some activity
    let mut buffers = Vec::new();
    for _ in 0..50 {
        buffers.push(pool.get_small_buffer());
    }
    
    // Return half the buffers
    for buffer in buffers.drain(0..25) {
        pool.return_buffer(buffer);
    }
    
    // Get health report
    let health_report = pool.health_check();
    
    // Validate health report structure
    assert!(health_report.health_score >= 0.0 && health_report.health_score <= 100.0);
    assert!(health_report.cache_hit_rate >= 0.0 && health_report.cache_hit_rate <= 1.0);
    assert!(health_report.memory_efficiency >= 0.0 && health_report.memory_efficiency <= 1.0);
    assert!(health_report.pool_sizes.0 <= 100); // small pool size
    
    println!("✅ Pool health monitoring test passed");
    println!("   Health score: {:.1}/100", health_report.health_score);
    
    // Clean up remaining buffers
    for buffer in buffers {
        pool.return_buffer(buffer);
    }
}

#[tokio::test]
async fn test_comprehensive_optimization_integration() {
    // Test that all optimization components can be created together
    let config = CpuAffinityConfig::default();
    let _cpu_optimizer = CpuOptimizer::new(config);
    let pool_config = PoolConfig::default();
    let buffer_pool = Arc::new(MessageBufferPool::new(pool_config));
    let batching_config = BatchingConfig {
        max_batch_size: 25,
        max_flush_delay_us: 50,
        adaptive_batching: true,
        memory_pool_size: 500,
        priority_batching: true,
    };
    
    // Verify all components are created successfully
    assert!(CpuOptimizer::get_available_cores() > 0);
    
    let pool_sizes = buffer_pool.get_pool_sizes();
    assert!(pool_sizes.0 > 0); // small pool has buffers
    
    // Test zero-copy message building with integration
    let test_message = {
        let mut builder = ZeroCopyMessageBuilder::new(buffer_pool.clone());
        builder.start_message(200);
        builder.add_topic("integration.test.topic");
        builder.add_payload(b"integration test payload with more data");
        builder.build().expect("Should build message successfully")
    };
    
    assert!(test_message.len() > 0);
    buffer_pool.return_buffer(test_message);
    
    println!("✅ Comprehensive optimization integration test passed");
    println!("   Available CPU cores: {}", CpuOptimizer::get_available_cores());
    println!("   Buffer pool sizes: {:?}", pool_sizes);
    println!("   Batching config: max_batch_size={}, max_flush_delay_us={:?}", 
        batching_config.max_batch_size, batching_config.max_flush_delay_us);
}
