// Throughput benchmarks for MessageBrokerEngine
// Measures messages per second under various load conditions

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

// Mock high-performance queue (similar to crossbeam SegQueue)
use crossbeam::queue::SegQueue;

#[derive(Clone)]
struct Message {
    #[allow(dead_code)]
    id: u64,
    payload: Vec<u8>,
}

impl Message {
    fn new(id: u64, size: usize) -> Self {
        Self {
            id,
            payload: vec![0u8; size],
        }
    }
}

// High-performance message queue
struct MessageQueue {
    queue: Arc<SegQueue<Message>>,
    metrics: Arc<AtomicU64>,
}

impl MessageQueue {
    fn new() -> Self {
        Self {
            queue: Arc::new(SegQueue::new()),
            metrics: Arc::new(AtomicU64::new(0)),
        }
    }
    
    fn push(&self, msg: Message) {
        self.queue.push(msg);
        self.metrics.fetch_add(1, Ordering::Relaxed);
    }
    
    fn pop(&self) -> Option<Message> {
        self.queue.pop()
    }
    
    #[allow(dead_code)]
    fn messages_processed(&self) -> u64 {
        self.metrics.load(Ordering::Relaxed)
    }
}

// Benchmark: Single-threaded throughput
fn single_threaded_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_threaded_throughput");
    
    let message_counts = [1_000, 10_000, 100_000];
    
    for count in message_counts.iter() {
        group.throughput(Throughput::Elements(*count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(count),
            count,
            |b, &count| {
                b.iter(|| {
                    let queue = MessageQueue::new();
                    
                    // Push messages
                    for i in 0..count {
                        queue.push(Message::new(i as u64, 1024));
                    }
                    
                    // Pop messages
                    let mut processed = 0;
                    while let Some(_msg) = queue.pop() {
                        processed += 1;
                        black_box(processed);
                    }
                    
                    assert_eq!(processed, count);
                });
            },
        );
    }
    
    group.finish();
}

// Benchmark: Multi-threaded producer/consumer throughput
fn multi_threaded_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_threaded_throughput");
    group.measurement_time(Duration::from_secs(10));
    
    let configurations = [
        (1, 1),   // 1 producer, 1 consumer
        (2, 2),   // 2 producers, 2 consumers
        (4, 4),   // 4 producers, 4 consumers
        (8, 4),   // 8 producers, 4 consumers
    ];
    
    for (producers, consumers) in configurations.iter() {
        group.bench_with_input(
            BenchmarkId::new("producers_consumers", format!("{}p_{}c", producers, consumers)),
            &(*producers, *consumers),
            |b, &(producers, consumers)| {
                b.iter(|| {
                    let queue = Arc::new(MessageQueue::new());
                    let stop_flag = Arc::new(AtomicU64::new(0));
                    let messages_per_producer = 10_000;
                    
                    // Start producers
                    let producer_handles: Vec<_> = (0..producers)
                        .map(|id| {
                            let queue_clone = Arc::clone(&queue);
                            std::thread::spawn(move || {
                                for i in 0..messages_per_producer {
                                    let msg = Message::new((id * messages_per_producer + i) as u64, 1024);
                                    queue_clone.push(msg);
                                }
                            })
                        })
                        .collect();
                    
                    // Start consumers
                    let consumer_handles: Vec<_> = (0..consumers)
                        .map(|_| {
                            let queue_clone = Arc::clone(&queue);
                            let stop_clone = Arc::clone(&stop_flag);
                            std::thread::spawn(move || {
                                let mut processed = 0;
                                loop {
                                    if let Some(_msg) = queue_clone.pop() {
                                        processed += 1;
                                    } else if stop_clone.load(Ordering::Relaxed) == 1 {
                                        break;
                                    } else {
                                        std::thread::yield_now();
                                    }
                                }
                                processed
                            })
                        })
                        .collect();
                    
                    // Wait for producers
                    for handle in producer_handles {
                        handle.join().unwrap();
                    }
                    
                    // Signal consumers to stop after queue drains
                    while !queue.queue.is_empty() {
                        std::thread::yield_now();
                    }
                    stop_flag.store(1, Ordering::Relaxed);
                    
                    // Wait for consumers
                    let total_consumed: u64 = consumer_handles
                        .into_iter()
                        .map(|h| h.join().unwrap())
                        .sum();
                    
                    assert_eq!(total_consumed, (producers * messages_per_producer) as u64);
                });
            },
        );
    }
    
    group.finish();
}

// Benchmark: Lock-free queue operations
fn lock_free_queue_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("lock_free_operations");
    
    // Push operation
    group.bench_function("push", |b| {
        let queue = SegQueue::new();
        b.iter(|| {
            queue.push(black_box(Message::new(1, 1024)));
        });
    });
    
    // Pop operation
    group.bench_function("pop", |b| {
        let queue = SegQueue::new();
        // Pre-fill queue
        for i in 0..10_000 {
            queue.push(Message::new(i, 1024));
        }
        
        b.iter(|| {
            black_box(queue.pop());
        });
    });
    
    // Push-pop pair
    group.bench_function("push_pop_pair", |b| {
        let queue = SegQueue::new();
        b.iter(|| {
            queue.push(Message::new(1, 1024));
            black_box(queue.pop());
        });
    });
    
    group.finish();
}

// Benchmark: Memory pool efficiency
fn memory_pool_efficiency(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_pool");
    
    // Simulate buffer pool
    let small_pool: Arc<SegQueue<Vec<u8>>> = Arc::new(SegQueue::new());
    let medium_pool: Arc<SegQueue<Vec<u8>>> = Arc::new(SegQueue::new());
    let large_pool: Arc<SegQueue<Vec<u8>>> = Arc::new(SegQueue::new());
    
    // Pre-allocate buffers
    for _ in 0..1000 {
        small_pool.push(Vec::with_capacity(1024));
        medium_pool.push(Vec::with_capacity(8192));
        large_pool.push(Vec::with_capacity(65536));
    }
    
    // Benchmark buffer allocation from pool
    group.bench_function("pool_allocation", |b| {
        let pool = Arc::clone(&medium_pool);
        b.iter(|| {
            if let Some(mut buffer) = pool.pop() {
                buffer.clear();
                black_box(&buffer);
                pool.push(buffer);
            }
        });
    });
    
    // Benchmark direct allocation
    group.bench_function("direct_allocation", |b| {
        b.iter(|| {
            let buffer: Vec<u8> = Vec::with_capacity(8192);
            black_box(buffer);
        });
    });
    
    group.finish();
}

// Benchmark: Batch message processing
fn batch_message_processing(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_processing_throughput");
    
    let batch_sizes = [10, 50, 100, 500, 1000];
    
    for batch_size in batch_sizes.iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &batch_size| {
                let messages: Vec<_> = (0..batch_size)
                    .map(|i| Message::new(i as u64, 1024))
                    .collect();
                
                b.iter(|| {
                    let mut processed = 0;
                    for msg in &messages {
                        // Simulate processing: compute checksum
                        let checksum: u64 = msg.payload.iter().map(|&b| b as u64).sum();
                        black_box(checksum);
                        processed += 1;
                    }
                    black_box(processed);
                });
            },
        );
    }
    
    group.finish();
}

// Benchmark: Adaptive batching simulation
fn adaptive_batching(c: &mut Criterion) {
    let mut group = c.benchmark_group("adaptive_batching");
    
    // Simulate low load (small batches)
    group.bench_function("low_load_batching", |b| {
        let queue = MessageQueue::new();
        
        b.iter(|| {
            // Simulate 10 messages arriving
            for i in 0..10 {
                queue.push(Message::new(i, 1024));
            }
            
            // Process in small batches
            let mut batch = Vec::with_capacity(10);
            while let Some(msg) = queue.pop() {
                batch.push(msg);
                if batch.len() >= 10 {
                    // Process batch
                    for msg in &batch {
                        let checksum: u64 = msg.payload.iter().map(|&b| b as u64).sum();
                        black_box(checksum);
                    }
                    batch.clear();
                }
            }
        });
    });
    
    // Simulate high load (large batches)
    group.bench_function("high_load_batching", |b| {
        let queue = MessageQueue::new();
        
        b.iter(|| {
            // Simulate 500 messages arriving
            for i in 0..500 {
                queue.push(Message::new(i, 1024));
            }
            
            // Process in large batches
            let mut batch = Vec::with_capacity(50);
            while let Some(msg) = queue.pop() {
                batch.push(msg);
                if batch.len() >= 50 {
                    // Process batch
                    for msg in &batch {
                        let checksum: u64 = msg.payload.iter().map(|&b| b as u64).sum();
                        black_box(checksum);
                    }
                    batch.clear();
                }
            }
        });
    });
    
    group.finish();
}

criterion_group!(
    benches,
    single_threaded_throughput,
    multi_threaded_throughput,
    lock_free_queue_ops,
    memory_pool_efficiency,
    batch_message_processing,
    adaptive_batching
);
criterion_main!(benches);
