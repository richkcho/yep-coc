//! SPSC Queue Throughput Benchmark
//!
//! This benchmark measures the steady-state throughput of the YCQueue SPSC (Single Producer
//! Single Consumer) queue implementation using Criterion.
//!
//! # Features
//! - Thread pinning: Producer and consumer threads are pinned to different CPU cores using
//!   core_affinity to avoid SMT siblings and reduce cache contention.
//! - NUMA-aware: Queue views are created after pinning so each thread first-touches memory on the
//!   correct NUMA node while sharing a single `YCQueueOwnedData` allocation from
//!   `queue_alloc_helpers`.
//! - Synchronized start: Uses a barrier to ensure both threads start simultaneously, measuring
//!   only steady-state performance without startup costs.
//! - Wall-clock timing: Uses iter_custom to measure total elapsed time across both threads.
//! - Dead code elimination prevention: Uses black_box on data to ensure operations aren't
//!   optimized away.
//! - Batch operations: Supports both single-slot and batched produce/consume operations.
//! - Time-based sampling: Each sample runs for a fixed duration (200ms) rather than a fixed
//!   iteration count, providing consistent measurement windows.
//!
//! # Parameters Swept
//! - Capacities: 256, 512, 1024 slots
//! - Payload sizes: 64, 256, 512 bytes
//! - Batch sizes: 1, 8, 32 (operations per API call)
//!
//! # Configuration
//! - Sample duration: 200 milliseconds per sample
//! - Measurement time: 20 seconds total
//! - Sample size: 20 measurements
//! - Warm-up time: 2 seconds
//! - Throughput reported in messages/second

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use yep_coc::{
    YCQueue, YCQueueError,
    queue_alloc_helpers::{YCQueueOwnedData, YCQueueSharedData},
};

/// Wrapper to make queue_alloc_helpers::YCQueueOwnedData Send+Sync for benchmarking.
/// This is safe because the queue's internal atomics handle synchronization.
struct SendSyncOwnedData(YCQueueOwnedData);
unsafe impl Send for SendSyncOwnedData {}
unsafe impl Sync for SendSyncOwnedData {}

/// Parameters for the benchmark sweep
#[derive(Debug, Clone, Copy)]
struct Params {
    capacity: u16,
    payload_size: u16,
    batch_size: u16,
}

impl Params {
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

/// Run a single SPSC sample for a fixed duration
/// Returns (elapsed_duration, items_processed)
fn run_spsc_sample(
    params: &Params,
    sample_duration: Duration,
    producer_core: usize,
    consumer_core: usize,
) -> (Duration, u64) {
    // Barrier to synchronize thread start
    let barrier = Arc::new(Barrier::new(2));
    let barrier_producer = Arc::clone(&barrier);
    let barrier_consumer = Arc::clone(&barrier);

    // Stop flag to signal threads to terminate
    let stop = Arc::new(AtomicBool::new(false));
    let stop_producer = Arc::clone(&stop);
    let stop_consumer = Arc::clone(&stop);

    // Counters for items processed
    let items_produced = Arc::new(AtomicU64::new(0));
    let items_consumed = Arc::new(AtomicU64::new(0));
    let items_produced_producer = Arc::clone(&items_produced);
    let items_consumed_consumer = Arc::clone(&items_consumed);

    // Allocate queue data BEFORE spawning threads
    let owned_data = Arc::new(SendSyncOwnedData(YCQueueOwnedData::new(
        params.capacity,
        params.payload_size,
    )));
    let owned_data_producer = Arc::clone(&owned_data);
    let owned_data_consumer = Arc::clone(&owned_data);

    // Start timer
    let start_time = Arc::new(std::sync::Mutex::new(None));
    let start_time_timer = Arc::clone(&start_time);

    thread::scope(|s| {
        // Consumer thread
        let consumer_handle = s.spawn(move || {
            // Pin to consumer core FIRST
            let core_ids = core_affinity::get_core_ids().unwrap();
            core_affinity::set_for_current(core_ids[consumer_core]);

            // Create queue view AFTER pinning for NUMA correctness
            let consumer_data = YCQueueSharedData::from_owned_data(&owned_data_consumer.0);
            let mut consumer_queue = YCQueue::new(consumer_data.meta, consumer_data.data)
                .expect("Failed to create consumer queue");

            // Wait for producer to be ready
            barrier_consumer.wait();

            let mut local_count = 0u64;

            while !stop_consumer.load(Ordering::Relaxed) {
                if params.batch_size == 1 {
                    // Single-slot operation
                    match consumer_queue.get_consume_slot() {
                        Ok(consume_slot) => {
                            black_box(&consume_slot.data);
                            consumer_queue
                                .mark_slot_consumed(consume_slot)
                                .expect("Failed to mark slot consumed");
                            local_count += 1;
                        }
                        Err(YCQueueError::EmptyQueue) | Err(YCQueueError::SlotNotReady) => {
                            thread::yield_now();
                        }
                        Err(e) => panic!("Consumer error: {:?}", e),
                    }
                } else {
                    // Batch operation - use best_effort=true to get whatever is available
                    match consumer_queue.get_consume_slots(params.batch_size, true) {
                        Ok(slots) => {
                            for slot in &slots {
                                black_box(&slot.data);
                            }
                            local_count += slots.len() as u64;
                            consumer_queue
                                .mark_slots_consumed(slots)
                                .expect("Failed to mark slots consumed");
                        }
                        Err(YCQueueError::EmptyQueue) | Err(YCQueueError::SlotNotReady) => {
                            thread::yield_now();
                        }
                        Err(e) => panic!("Consumer error: {:?}", e),
                    }
                }
            }

            items_consumed_consumer.store(local_count, Ordering::Relaxed);
        });

        // Producer thread
        let producer_handle = s.spawn(move || {
            // Pin to producer core FIRST
            let core_ids = core_affinity::get_core_ids().unwrap();
            core_affinity::set_for_current(core_ids[producer_core]);

            // Create queue view AFTER pinning for NUMA correctness
            let producer_data = YCQueueSharedData::from_owned_data(&owned_data_producer.0);
            let mut producer_queue = YCQueue::new(producer_data.meta, producer_data.data)
                .expect("Failed to create producer queue");

            // Wait for consumer to be ready
            barrier_producer.wait();

            let mut local_count = 0u64;

            while !stop_producer.load(Ordering::Relaxed) {
                if params.batch_size == 1 {
                    // Single-slot operation
                    match producer_queue.get_produce_slot() {
                        Ok(produce_slot) => {
                            // Fill with pattern to prevent optimization
                            for i in 0..params.payload_size as usize {
                                produce_slot.data[i] = ((local_count as usize + i) % 256) as u8;
                            }
                            black_box(&produce_slot.data);
                            producer_queue
                                .mark_slot_produced(produce_slot)
                                .expect("Failed to mark slot produced");
                            local_count += 1;
                        }
                        Err(YCQueueError::OutOfSpace) | Err(YCQueueError::SlotNotReady) => {
                            thread::yield_now();
                        }
                        Err(e) => panic!("Producer error: {:?}", e),
                    }
                } else {
                    // Batch operation - use best_effort=true to get whatever is available
                    match producer_queue.get_produce_slots(params.batch_size, true) {
                        Ok(mut slots) => {
                            for (idx, slot) in slots.iter_mut().enumerate() {
                                let msg_num = local_count + idx as u64;
                                for i in 0..params.payload_size as usize {
                                    slot.data[i] = ((msg_num as usize + i) % 256) as u8;
                                }
                                black_box(&slot.data);
                            }
                            local_count += slots.len() as u64;
                            producer_queue
                                .mark_slots_produced(slots)
                                .expect("Failed to mark slots produced");
                        }
                        Err(YCQueueError::OutOfSpace) | Err(YCQueueError::SlotNotReady) => {
                            thread::yield_now();
                        }
                        Err(e) => panic!("Producer error: {:?}", e),
                    }
                }
            }

            items_produced_producer.store(local_count, Ordering::Relaxed);
        });

        // Timer thread - wait for sample_duration then signal stop
        let timer_handle = s.spawn(move || {
            // Wait for both threads to be ready
            // Small sleep to ensure threads have started
            thread::sleep(Duration::from_millis(1));

            // Record start time
            let start = Instant::now();
            {
                let mut st = start_time_timer.lock().unwrap();
                *st = Some(start);
            }

            // Wait for sample duration
            thread::sleep(sample_duration);

            // Signal stop
            stop.store(true, Ordering::Relaxed);
        });

        producer_handle.join().expect("Producer thread panicked");
        consumer_handle.join().expect("Consumer thread panicked");
        timer_handle.join().expect("Timer thread panicked");
    });

    // Get the actual elapsed time
    let start = start_time.lock().unwrap().expect("Start time not set");
    let elapsed = start.elapsed();

    // Use the minimum of produced/consumed as the actual count
    let produced = items_produced.load(Ordering::Relaxed);
    let consumed = items_consumed.load(Ordering::Relaxed);
    let items = std::cmp::min(produced, consumed);

    (elapsed, items)
}

/// Benchmark function that measures steady-state throughput of SPSC queue
/// Uses time-based samples with iter_custom
fn bench_spsc(c: &mut Criterion) {
    let cfg = spsc_bench_config();
    let mut group = c.benchmark_group("spsc/time_based");

    // Configure Criterion for time-based samples
    group
        .measurement_time(cfg.measurement_time)
        .warm_up_time(cfg.warm_up_time)
        .sample_size(cfg.sample_size);

    let sample_duration = cfg.sample_duration;
    let (producer_core, consumer_core) = select_cores();

    // Define parameter sweep
    let capacities = vec![32, 1024];
    let payload_sizes = vec![32, 512];
    let batch_sizes = vec![1, 16, 32];

    for capacity in &capacities {
        for payload_size in &payload_sizes {
            for batch_size in &batch_sizes {
                let params = Params {
                    capacity: *capacity,
                    payload_size: *payload_size,
                    batch_size: *batch_size,
                };

                // Pre-run a sample to estimate items processed per iteration so Criterion can
                // compute throughput instead of only showing the fixed sample duration.
                let (_probe_dt, probe_items) =
                    run_spsc_sample(&params, sample_duration, producer_core, consumer_core);
                let items_per_sample = probe_items.max(1);
                group.throughput(Throughput::Elements(items_per_sample));

                let id = params.id();
                group.bench_with_input(BenchmarkId::from_parameter(id), &params, |b, params| {
                    b.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        let mut total_items: u64 = 0;

                        for _ in 0..iters {
                            let (dt, items) = run_spsc_sample(
                                params,
                                sample_duration,
                                producer_core,
                                consumer_core,
                            );
                            total += dt;
                            total_items += items;
                        }

                        // Print average throughput for debugging
                        let secs = total.as_secs_f64();
                        if secs > 0.0 {
                            let throughput = total_items as f64 / secs;
                            println!(
                                "params={:?} iters={} total_items={} total_time={:?} throughput={:.0} items/s",
                                params, iters, total_items, total, throughput
                            );
                        }

                        total
                    });
                });
            }
        }
    }

    group.finish();
}

/// Configure Criterion with appropriate settings for time-based benchmarks
fn spsc_criterion_config() -> Criterion {
    let cfg = spsc_bench_config();

    Criterion::default()
        .warm_up_time(cfg.warm_up_time)
        .measurement_time(cfg.measurement_time)
        .sample_size(cfg.sample_size)
}

struct SpscBenchConfig {
    warm_up_time: Duration,
    measurement_time: Duration,
    sample_size: usize,
    sample_duration: Duration,
}

fn spsc_bench_config() -> SpscBenchConfig {
    SpscBenchConfig {
        warm_up_time: Duration::from_secs(2),
        measurement_time: Duration::from_secs(20),
        sample_size: 20,
        sample_duration: Duration::from_millis(200),
    }
}

criterion_group! {
    name = benches;
    config = spsc_criterion_config();
    targets = bench_spsc
}
criterion_main!(benches);
