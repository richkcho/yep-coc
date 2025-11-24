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
//! - Capacities: 32, 1024 slots
//! - Payload sizes: 32, 512 bytes
//! - Batch sizes: 1, 16, 32 (operations per API call)
//!
//! # Configuration
//! - Sample duration: 200 milliseconds per sample
//! - Measurement settings: use Criterion defaults unless overridden on the CLI
//! - Throughput reported in messages/second

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use test_support::utils::{backoff, cache_line_size};
use yep_coc::{
    YCQueue, YCQueueError,
    queue_alloc_helpers::{CursorCacheLines, YCQueueOwnedData, YCQueueSharedData},
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

#[derive(Debug, Clone)]
struct CoreInfo {
    id: usize,
    package_id: Option<u32>,
    core_id: Option<u32>,
}

/// Find two distinct CPU cores to pin threads to, preferring physical cores
/// Returns (producer_core_id, consumer_core_id)
fn select_cores() -> (usize, usize) {
    let core_ids = core_affinity::get_core_ids().expect("Failed to get core IDs");

    if core_ids.len() < 2 {
        panic!("Need at least 2 CPU cores for SPSC benchmark");
    }

    let core_info = core_ids
        .iter()
        .map(|cid| CoreInfo {
            id: cid.id,
            package_id: read_topology_value(cid.id, "physical_package_id"),
            core_id: read_topology_value(cid.id, "core_id"),
        })
        .collect::<Vec<_>>();

    if let Some((a, b)) = pick_nonsmt_same_package(&core_info) {
        return (a, b);
    }

    // Fallback: pick first and middle core
    let producer_core = core_info[0].id;
    let consumer_core = core_info[core_info.len() / 2].id;

    (producer_core, consumer_core)
}

fn pick_nonsmt_same_package(core_info: &[CoreInfo]) -> Option<(usize, usize)> {
    let mut by_pkg: HashMap<u32, Vec<&CoreInfo>> = HashMap::new();

    for info in core_info {
        if let Some(pkg) = info.package_id {
            by_pkg.entry(pkg).or_default().push(info);
        }
    }

    for infos in by_pkg.values() {
        if infos.len() < 2 {
            continue;
        }

        for (i, a) in infos.iter().enumerate() {
            for b in infos.iter().skip(i + 1) {
                if let (Some(core_a), Some(core_b)) = (a.core_id, b.core_id)
                    && (core_a == core_b)
                {
                    continue;
                }
                return Some((a.id, b.id));
            }
        }
    }

    None
}

fn read_topology_value(cpu_id: usize, file: &str) -> Option<u32> {
    let path = format!("/sys/devices/system/cpu/cpu{cpu_id}/topology/{file}");
    let contents = std::fs::read_to_string(path).ok()?;
    contents.trim().parse().ok()
}

/// Run a single SPSC sample for a fixed duration
/// Returns (elapsed_duration, items_processed)
fn run_spsc_sample(
    params: &Params,
    sample_duration: Duration,
    producer_core: usize,
    consumer_core: usize,
    touch_stride: usize,
) -> (Duration, u64) {
    // Barrier to synchronize thread start
    let barrier = Arc::new(Barrier::new(3));
    let barrier_producer = Arc::clone(&barrier);
    let barrier_consumer = Arc::clone(&barrier);
    let barrier_timer = Arc::clone(&barrier);

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
    let owned_data = Arc::new(SendSyncOwnedData(YCQueueOwnedData::new_with_cursor_layout(
        params.capacity,
        params.payload_size,
        CursorCacheLines::Split,
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
            let mut backoff_pow = 0;

            while !stop_consumer.load(Ordering::Relaxed) {
                if params.batch_size == 1 {
                    // Single-slot operation
                    match consumer_queue.get_consume_slot() {
                        Ok(consume_slot) => {
                            touch_consume(consume_slot.data, touch_stride);
                            black_box(&consume_slot.data);
                            consumer_queue
                                .mark_slot_consumed(consume_slot)
                                .expect("Failed to mark slot consumed");
                            local_count += 1;
                            backoff_pow = 0;
                        }
                        Err(YCQueueError::EmptyQueue) | Err(YCQueueError::SlotNotReady) => {
                            backoff(&mut backoff_pow);
                        }
                        Err(e) => panic!("Consumer error: {:?}", e),
                    }
                } else {
                    // Batch operation - use best_effort=true to get whatever is available
                    match consumer_queue.get_consume_slots(params.batch_size, true) {
                        Ok(slots) => {
                            slots.iter().for_each(|slot| {
                                touch_consume(slot.data, touch_stride);
                                black_box(&slot.data);
                            });
                            local_count += slots.len() as u64;
                            consumer_queue
                                .mark_slots_consumed(slots)
                                .expect("Failed to mark slots consumed");
                            backoff_pow = 0;
                        }
                        Err(YCQueueError::EmptyQueue) | Err(YCQueueError::SlotNotReady) => {
                            backoff(&mut backoff_pow);
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
            let mut backoff_pow = 0;

            while !stop_producer.load(Ordering::Relaxed) {
                if params.batch_size == 1 {
                    // Single-slot operation
                    match producer_queue.get_produce_slot() {
                        Ok(produce_slot) => {
                            touch_produce(produce_slot.data, touch_stride);
                            black_box(&produce_slot.data);
                            producer_queue
                                .mark_slot_produced(produce_slot)
                                .expect("Failed to mark slot produced");
                            local_count += 1;
                            backoff_pow = 0;
                        }
                        Err(YCQueueError::OutOfSpace) | Err(YCQueueError::SlotNotReady) => {
                            backoff(&mut backoff_pow);
                        }
                        Err(e) => panic!("Producer error: {:?}", e),
                    }
                } else {
                    // Batch operation - use best_effort=true to get whatever is available
                    match producer_queue.get_produce_slots(params.batch_size, true) {
                        Ok(mut slots) => {
                            slots.iter_mut().for_each(|slot| {
                                touch_produce(slot.data, touch_stride);
                                black_box(&slot.data);
                            });
                            local_count += slots.len() as u64;
                            producer_queue
                                .mark_slots_produced(slots)
                                .expect("Failed to mark slots produced");
                            backoff_pow = 0;
                        }
                        Err(YCQueueError::OutOfSpace) | Err(YCQueueError::SlotNotReady) => {
                            backoff(&mut backoff_pow);
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
            barrier_timer.wait();

            // Record start time exactly when all threads release
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

/// Check if verbose mode is enabled via command line args or environment variable
fn is_verbose_mode() -> bool {
    // Check if CRITERION_DEBUG environment variable is set
    if std::env::var("CRITERION_DEBUG").is_ok() {
        return true;
    }

    // Check command line arguments for --verbose or -v flag
    std::env::args().any(|arg| arg == "--verbose" || arg == "-v")
}

fn touch_produce(data: &mut [u8], stride: usize) {
    for idx in (0..data.len()).step_by(stride) {
        data[idx] = data[idx].wrapping_add(1);
    }
}

fn touch_consume(data: &[u8], stride: usize) {
    for idx in (0..data.len()).step_by(stride) {
        black_box(data[idx]);
    }
}

/// Benchmark function that measures steady-state throughput of SPSC queue
/// Uses time-based samples with iter_custom
fn bench_spsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc");

    // Use a fixed sample duration for consistent measurement windows
    let sample_duration = Duration::from_millis(200);
    let (producer_core, consumer_core) = select_cores();

    // Define parameter sweep
    let capacities = vec![32, 1024];
    let payload_sizes = vec![32, 512];
    let batch_sizes = vec![1, 16, 32];
    let touch_stride = std::cmp::max(1, cache_line_size() as usize);

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
                let (_probe_dt, probe_items) = run_spsc_sample(
                    &params,
                    sample_duration,
                    producer_core,
                    consumer_core,
                    touch_stride,
                );
                let items_per_sample = probe_items.max(1);

                // this is telling criterion that the time reported below corresponds to processing
                // items_per_sample items.
                group.throughput(Throughput::Elements(items_per_sample));

                let is_verbose = is_verbose_mode();
                let id = params.id();
                group.bench_with_input(BenchmarkId::from_parameter(id), &params, |b, params| {
                    b.iter_custom(|iters| {

                        let (time_taken, items_processed) = run_spsc_sample(
                            params,
                            sample_duration * u32::try_from(iters).expect("iters was too large to fit in u32"),
                            producer_core,
                            consumer_core,
                            touch_stride,
                        );

                        // Print average throughput for debugging only when verbose is enabled
                        let secs = time_taken.as_secs_f64();
                        if secs > 0.0 && is_verbose {
                            let throughput = items_processed as f64 / secs;
                            println!(
                                "params={:?} iters={} items_processed={} time_taken={:?} throughput={:.0} items/s",
                                params, iters, items_processed, time_taken, throughput
                            );
                        }

                        // the duration here is per `items_per_sample`` items.
                        Duration::from_nanos((time_taken.as_nanos() * items_processed as u128 / (iters as u128 * items_per_sample as u128)) as u64)
                    });
                });
            }
        }
    }

    group.finish();
}

criterion_group!(benches, bench_spsc);
criterion_main!(benches);
