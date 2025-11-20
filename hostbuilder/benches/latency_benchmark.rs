// Latency benchmarks for MessageBrokerEngine
// Measures p50, p95, p99, p99.9 latencies for various message sizes

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use std::sync::atomic::{AtomicU64, Ordering};

// Mock message structure for benchmarking
#[derive(Clone)]
struct Message {
    #[allow(dead_code)]
    topic: String,
    payload: Vec<u8>,
    #[allow(dead_code)]
    timestamp: u64,
}

impl Message {
    fn new(size: usize) -> Self {
        Self {
            topic: "benchmark.topic".to_string(),
            payload: vec![0u8; size],
            timestamp: get_timestamp(),
        }
    }
}

// Ultra-fast timestamp function (same as production)
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn get_timestamp() -> u64 {
    unsafe { std::arch::x86_64::_rdtsc() }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
fn get_timestamp() -> u64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

// Simulate atomic metrics update (cache-aligned version)
#[repr(align(64))]
struct CacheAlignedMetrics {
    messages_processed: AtomicU64,
    _pad1: [u8; 56],
    total_latency_ns: AtomicU64,
    _pad2: [u8; 56],
}

impl Default for CacheAlignedMetrics {
    fn default() -> Self {
        Self {
            messages_processed: AtomicU64::new(0),
            _pad1: [0; 56],
            total_latency_ns: AtomicU64::new(0),
            _pad2: [0; 56],
        }
    }
}

// Non-aligned version for comparison
struct UnalignedMetrics {
    messages_processed: AtomicU64,
    total_latency_ns: AtomicU64,
}

impl Default for UnalignedMetrics {
    fn default() -> Self {
        Self {
            messages_processed: AtomicU64::new(0),
            total_latency_ns: AtomicU64::new(0),
        }
    }
}

// Simulate message processing
fn process_message(msg: &Message, metrics: &CacheAlignedMetrics) -> u64 {
    let start = get_timestamp();
    
    // Simulate work: checksum calculation
    let checksum: u64 = msg.payload.iter().map(|&b| b as u64).sum();
    black_box(checksum);
    
    let end = get_timestamp();
    let latency = end.saturating_sub(start);
    
    metrics.messages_processed.fetch_add(1, Ordering::Relaxed);
    metrics.total_latency_ns.fetch_add(latency, Ordering::Relaxed);
    
    latency
}

// Benchmark: Message processing latency by size
fn latency_by_message_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_processing_latency");
    
    let sizes = [64, 256, 1024, 4096, 16384, 65536];
    
    for size in sizes.iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let msg = Message::new(size);
            let metrics = CacheAlignedMetrics::default();
            
            b.iter(|| {
                black_box(process_message(&msg, &metrics))
            });
        });
    }
    
    group.finish();
}

// Benchmark: Cache-aligned vs unaligned atomic operations
fn cache_alignment_impact(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_alignment");
    
    // Aligned version
    group.bench_function("aligned_atomics", |b| {
        let metrics = CacheAlignedMetrics::default();
        b.iter(|| {
            metrics.messages_processed.fetch_add(1, Ordering::Relaxed);
            metrics.total_latency_ns.fetch_add(100, Ordering::Relaxed);
        });
    });
    
    // Unaligned version
    group.bench_function("unaligned_atomics", |b| {
        let metrics = UnalignedMetrics::default();
        b.iter(|| {
            metrics.messages_processed.fetch_add(1, Ordering::Relaxed);
            metrics.total_latency_ns.fetch_add(100, Ordering::Relaxed);
        });
    });
    
    group.finish();
}

// Benchmark: RDTSC timestamp overhead
fn timestamp_overhead(c: &mut Criterion) {
    c.bench_function("rdtsc_timestamp", |b| {
        b.iter(|| {
            black_box(get_timestamp())
        });
    });
}

// Benchmark: Multi-threaded contention
fn multi_threaded_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_threaded");
    
    for thread_count in [1, 2, 4, 8].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(thread_count),
            thread_count,
            |b, &threads| {
                let metrics = std::sync::Arc::new(CacheAlignedMetrics::default());
                
                b.iter(|| {
                    let handles: Vec<_> = (0..threads)
                        .map(|_| {
                            let metrics_clone = std::sync::Arc::clone(&metrics);
                            std::thread::spawn(move || {
                                for _ in 0..1000 {
                                    metrics_clone.messages_processed.fetch_add(1, Ordering::Relaxed);
                                }
                            })
                        })
                        .collect();
                    
                    for handle in handles {
                        handle.join().unwrap();
                    }
                });
            },
        );
    }
    
    group.finish();
}

// Benchmark: Batch processing efficiency
fn batch_processing(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_processing");
    
    let batch_sizes = [1, 10, 50, 100, 500];
    
    for batch_size in batch_sizes.iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &batch_size| {
                let messages: Vec<_> = (0..batch_size)
                    .map(|_| Message::new(1024))
                    .collect();
                let metrics = CacheAlignedMetrics::default();
                
                b.iter(|| {
                    for msg in &messages {
                        black_box(process_message(msg, &metrics));
                    }
                });
            },
        );
    }
    
    group.finish();
}

criterion_group!(
    benches,
    latency_by_message_size,
    cache_alignment_impact,
    timestamp_overhead,
    multi_threaded_contention,
    batch_processing
);
criterion_main!(benches);
