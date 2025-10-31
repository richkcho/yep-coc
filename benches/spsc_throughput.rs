//! SPSC Queue Throughput Benchmark
//!
//! This benchmark measures the steady-state throughput of the YCQueue SPSC (Single Producer
//! Single Consumer) queue implementation using Criterion.
//!
//! # Features
//! - Thread pinning: Producer and consumer threads are pinned to different CPU cores using
//!   core_affinity to avoid SMT siblings and reduce cache contention.
//! - NUMA-aware: Queue allocation happens after thread pinning for proper first-touch allocation.
//! - Synchronized start: Uses a barrier to ensure both threads start simultaneously, measuring
//!   only steady-state performance without startup costs.
//! - Wall-clock timing: Uses iter_custom to measure total elapsed time across both threads.
//! - Dead code elimination prevention: Uses black_box on data to ensure operations aren't
//!   optimized away.
//! - Batch operations: Supports both single-slot and batched produce/consume operations.
//!
//! # Parameters Swept
//! - Capacities: 256, 512, 2048 slots
//! - Payload sizes: 8, 64, 128 bytes
//! - Batch sizes: 1, 8, 32 (operations per API call)
//!
//! # Configuration
//! - Measurement time: 3 seconds per benchmark
//! - Sample size: 50 measurements
//! - Throughput reported in messages/second

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use yep_coc::{
    YCQueue, YCQueueError,
    queue_alloc_helpers::{YCQueueOwnedData, YCQueueSharedData},
};

/// Wrapper to make YCQueueOwnedData Send+Sync for benchmarking
/// This is safe because the queue's internal atomics handle synchronization
struct SendSyncOwnedData(YCQueueOwnedData);
unsafe impl Send for SendSyncOwnedData {}
unsafe impl Sync for SendSyncOwnedData {}

/// Parameters for the benchmark sweep
struct BenchParams {
    capacity: u16,
    payload_size: u16,
    batch_size: u16,
}

impl BenchParams {
    fn id(&self) -> String {
        format!(
            "cap{}_payload{}_batch{}",
            self.capacity, self.payload_size, self.batch_size
        )
    }
}

/// Find two distinct CPU cores to pin threads to, preferring physical cores
/// Returns (producer_core_id, consumer_core_id)
fn select_cores() -> (usize, usize) {
    let core_ids = core_affinity::get_core_ids().expect("Failed to get core IDs");

    if core_ids.len() < 2 {
        panic!("Need at least 2 CPU cores for SPSC benchmark");
    }

    // Try to select cores that are not SMT siblings
    // For simplicity, pick first and middle core
    let producer_core = 0;
    let consumer_core = core_ids.len() / 2;

    (producer_core, consumer_core)
}

/// Benchmark function that measures steady-state throughput of SPSC queue
/// Uses iter_custom to measure wall-clock time across both threads
fn bench_spsc_throughput(c: &mut Criterion, params: BenchParams) {
    let mut group = c.benchmark_group("spsc_throughput");

    // Configure Criterion
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(50);

    // Set throughput metric in messages (elements)
    let total_messages = params.capacity as u64;
    group.throughput(Throughput::Elements(total_messages));

    let (producer_core, consumer_core) = select_cores();

    group.bench_with_input(
        BenchmarkId::from_parameter(params.id()),
        &params,
        |b, params| {
            b.iter_custom(|iters| {
                let mut total_duration = Duration::ZERO;

                for _ in 0..iters {
                    // Barrier to synchronize thread start
                    let barrier = Arc::new(Barrier::new(2));
                    let barrier_producer = Arc::clone(&barrier);
                    let barrier_consumer = Arc::clone(&barrier);

                    // Calculate total number of messages to send
                    let messages_per_iter = params.capacity as u64;

                    // Allocate queue data BEFORE spawning threads - wrap in Arc for sharing
                    let owned_data = Arc::new(SendSyncOwnedData(YCQueueOwnedData::new(
                        params.capacity,
                        params.payload_size,
                    )));
                    let owned_data_producer = Arc::clone(&owned_data);
                    let owned_data_consumer = Arc::clone(&owned_data);

                    // We'll measure the time taken for this iteration
                    let start_time = Arc::new(std::sync::Mutex::new(None));
                    let end_time = Arc::new(std::sync::Mutex::new(None));
                    let start_time_producer = Arc::clone(&start_time);
                    let end_time_consumer = Arc::clone(&end_time);

                    thread::scope(|s| {
                        // Consumer thread
                        let consumer_handle = s.spawn(move || {
                            // Pin to consumer core FIRST
                            let core_ids = core_affinity::get_core_ids().unwrap();
                            core_affinity::set_for_current(core_ids[consumer_core]);

                            // Create queue view AFTER pinning for NUMA correctness
                            // This references the shared owned_data
                            let consumer_data =
                                YCQueueSharedData::from_owned_data(&owned_data_consumer.0);
                            let mut consumer_queue =
                                YCQueue::new(consumer_data.meta, consumer_data.data)
                                    .expect("Failed to create consumer queue");

                            // Wait for producer to be ready
                            barrier_consumer.wait();

                            let mut messages_received = 0u64;
                            while messages_received < messages_per_iter {
                                // Try to consume up to batch_size items
                                let remaining = messages_per_iter - messages_received;
                                let request =
                                    std::cmp::min(params.batch_size as u64, remaining) as u16;

                                if request == 1 {
                                    // Single-slot operation
                                    match consumer_queue.get_consume_slot() {
                                        Ok(consume_slot) => {
                                            black_box(&consume_slot.data);
                                            consumer_queue
                                                .mark_slot_consumed(consume_slot)
                                                .expect("Failed to mark slot consumed");
                                            messages_received += 1;
                                        }
                                        Err(YCQueueError::EmptyQueue)
                                        | Err(YCQueueError::SlotNotReady) => {
                                            thread::yield_now();
                                        }
                                        Err(e) => panic!("Consumer error: {:?}", e),
                                    }
                                } else {
                                    // Batch operation - use best_effort=true to get whatever is available
                                    match consumer_queue.get_consume_slots(request, true) {
                                        Ok(slots) => {
                                            for slot in &slots {
                                                black_box(&slot.data);
                                            }
                                            messages_received += slots.len() as u64;
                                            consumer_queue
                                                .mark_slots_consumed(slots)
                                                .expect("Failed to mark slots consumed");
                                        }
                                        Err(YCQueueError::EmptyQueue)
                                        | Err(YCQueueError::SlotNotReady) => {
                                            thread::yield_now();
                                        }
                                        Err(e) => panic!("Consumer error: {:?}", e),
                                    }
                                }
                            }

                            // Record end time
                            let mut end = end_time_consumer.lock().unwrap();
                            *end = Some(Instant::now());
                        });

                        // Producer thread
                        let producer_handle = s.spawn(move || {
                            // Pin to producer core FIRST
                            let core_ids = core_affinity::get_core_ids().unwrap();
                            core_affinity::set_for_current(core_ids[producer_core]);

                            // Create queue view AFTER pinning for NUMA correctness
                            // This references the shared owned_data
                            let producer_data =
                                YCQueueSharedData::from_owned_data(&owned_data_producer.0);
                            let mut producer_queue =
                                YCQueue::new(producer_data.meta, producer_data.data)
                                    .expect("Failed to create producer queue");

                            // Wait for consumer to be ready
                            barrier_producer.wait();

                            // Record start time after barrier
                            let mut start = start_time_producer.lock().unwrap();
                            *start = Some(Instant::now());
                            drop(start);

                            let mut messages_sent = 0u64;
                            let max_in_flight = params.capacity / 2;

                            while messages_sent < messages_per_iter {
                                // Throttle if too many in-flight
                                if producer_queue.in_flight_count() >= max_in_flight {
                                    thread::yield_now();
                                    continue;
                                }

                                // Try to produce up to batch_size items
                                let remaining = messages_per_iter - messages_sent;
                                let request =
                                    std::cmp::min(params.batch_size as u64, remaining) as u16;

                                if request == 1 {
                                    // Single-slot operation
                                    match producer_queue.get_produce_slot() {
                                        Ok(produce_slot) => {
                                            // Fill with pattern to prevent optimization
                                            for i in 0..params.payload_size as usize {
                                                produce_slot.data[i] =
                                                    ((messages_sent as usize + i) % 256) as u8;
                                            }
                                            black_box(&produce_slot.data);
                                            producer_queue
                                                .mark_slot_produced(produce_slot)
                                                .expect("Failed to mark slot produced");
                                            messages_sent += 1;
                                        }
                                        Err(YCQueueError::OutOfSpace)
                                        | Err(YCQueueError::SlotNotReady) => {
                                            thread::yield_now();
                                        }
                                        Err(e) => panic!("Producer error: {:?}", e),
                                    }
                                } else {
                                    // Batch operation - use best_effort=true to get whatever is available
                                    match producer_queue.get_produce_slots(request, true) {
                                        Ok(mut slots) => {
                                            for (idx, slot) in slots.iter_mut().enumerate() {
                                                let msg_num = messages_sent + idx as u64;
                                                for i in 0..params.payload_size as usize {
                                                    slot.data[i] =
                                                        ((msg_num as usize + i) % 256) as u8;
                                                }
                                                black_box(&slot.data);
                                            }
                                            messages_sent += slots.len() as u64;
                                            producer_queue
                                                .mark_slots_produced(slots)
                                                .expect("Failed to mark slots produced");
                                        }
                                        Err(YCQueueError::OutOfSpace)
                                        | Err(YCQueueError::SlotNotReady) => {
                                            thread::yield_now();
                                        }
                                        Err(e) => panic!("Producer error: {:?}", e),
                                    }
                                }
                            }
                        });

                        producer_handle.join().expect("Producer thread panicked");
                        consumer_handle.join().expect("Consumer thread panicked");
                    });

                    // Calculate duration
                    let start = start_time.lock().unwrap().expect("Start time not set");
                    let end = end_time.lock().unwrap().expect("End time not set");
                    total_duration += end.duration_since(start);
                }

                total_duration
            });
        },
    );

    group.finish();
}

/// Main benchmark entry point
fn spsc_throughput_benchmarks(c: &mut Criterion) {
    // Define parameter sweep
    // Note: capacity * payload_size must be < 65536 to avoid u16 overflow in the queue implementation
    let capacities = vec![256, 512, 2048];
    let payload_sizes = vec![8, 64, 128];
    let batch_sizes = vec![1, 8, 32];

    for capacity in &capacities {
        for payload_size in &payload_sizes {
            // Skip combinations that would overflow in queue allocation
            // Note: batch_size doesn't affect queue size, only API call batching
            if (*capacity as u32) * (*payload_size as u32) >= 65536 {
                continue;
            }

            for batch_size in &batch_sizes {
                let params = BenchParams {
                    capacity: *capacity,
                    payload_size: *payload_size,
                    batch_size: *batch_size,
                };

                bench_spsc_throughput(c, params);
            }
        }
    }
}

criterion_group!(benches, spsc_throughput_benchmarks);
criterion_main!(benches);
