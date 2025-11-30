//! MPMC Queue Throughput Benchmark
//!
//! Measures steady-state throughput of the YCQueue MPMC implementation using Criterion with
//! time-based samples. Threads are pinned to distinct cores, start simultaneously via a barrier,
//! and run for fixed-duration samples to avoid unbounded loops if logic regresses.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::collections::HashSet;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use test_support::utils::{backoff, cache_line_size};
use yep_coc::{
    YCQueue, YCQueueError,
    queue_alloc_helpers::{YCQueueOwnedData, YCQueueSharedData},
};

/// Wrapper to make queue_alloc_helpers::YCQueueOwnedData Send+Sync for benchmarking.
struct SendSyncOwnedData(YCQueueOwnedData);
unsafe impl Send for SendSyncOwnedData {}
unsafe impl Sync for SendSyncOwnedData {}

#[derive(Debug, Clone, Copy)]
struct Params {
    capacity: u16,
    payload_size: u16,
    batch_size: u16,
    producers: u16,
    consumers: u16,
}

impl Params {
    fn id(&self) -> String {
        format!(
            "cap{}_payload{}_batch{}_prod{}_cons{}",
            self.capacity, self.payload_size, self.batch_size, self.producers, self.consumers
        )
    }
}

#[derive(Debug, Clone)]
struct CoreInfo {
    id: usize,
    package_id: Option<u32>,
    core_id: Option<u32>,
}

fn read_topology_value(cpu_id: usize, file: &str) -> Option<u32> {
    let path = format!("/sys/devices/system/cpu/cpu{cpu_id}/topology/{file}");
    let contents = std::fs::read_to_string(path).ok()?;
    contents.trim().parse().ok()
}

/// Choose CPU cores for the requested thread count, preferring unique physical cores first.
fn select_cores(count: usize) -> Vec<usize> {
    let core_ids = core_affinity::get_core_ids().expect("Failed to get core IDs");
    if core_ids.len() < count {
        panic!("Need at least {count} CPU cores for MPMC benchmark");
    }

    let core_info = core_ids
        .iter()
        .map(|cid| CoreInfo {
            id: cid.id,
            package_id: read_topology_value(cid.id, "physical_package_id"),
            core_id: read_topology_value(cid.id, "core_id"),
        })
        .collect::<Vec<_>>();

    let mut chosen = Vec::with_capacity(count);
    let mut used_phys = HashSet::new();

    for info in &core_info {
        if let (Some(pkg), Some(core)) = (info.package_id, info.core_id) {
            let key = (pkg, core);
            if used_phys.insert(key) {
                chosen.push(info.id);
                if chosen.len() == count {
                    return chosen;
                }
            }
        }
    }

    for info in &core_info {
        if !chosen.contains(&info.id) {
            chosen.push(info.id);
            if chosen.len() == count {
                break;
            }
        }
    }

    chosen
}

/// Run a single MPMC sample for a fixed duration.
/// Returns (elapsed_duration, items_processed)
fn run_mpmc_sample(params: &Params, sample_duration: Duration) -> (Duration, u64) {
    let touch_stride = std::cmp::max(1, cache_line_size() as usize);
    let total_workers = params.producers as usize + params.consumers as usize;
    let core_selection = select_cores(total_workers);
    let (producer_cores, consumer_cores) = core_selection.split_at(params.producers as usize);

    let barrier = Arc::new(Barrier::new(total_workers + 1)); // +1 for timer
    let stop = Arc::new(AtomicBool::new(false));

    let produced = Arc::new(AtomicU64::new(0));
    let consumed = Arc::new(AtomicU64::new(0));

    let owned_data = Arc::new(SendSyncOwnedData(YCQueueOwnedData::new(
        params.capacity,
        params.payload_size,
    )));

    let start_time = Arc::new(std::sync::Mutex::new(None));
    let start_time_timer = Arc::clone(&start_time);

    thread::scope(|s| {
        let mut handles = Vec::with_capacity(total_workers + 1);

        for (idx, core_id) in producer_cores.iter().copied().enumerate() {
            let barrier = Arc::clone(&barrier);
            let stop = Arc::clone(&stop);
            let produced = Arc::clone(&produced);
            let owned_data = Arc::clone(&owned_data);
            let params = *params;

            handles.push(s.spawn(move || {
                let cores = core_affinity::get_core_ids().unwrap();
                core_affinity::set_for_current(cores[core_id]);

                let shared = YCQueueSharedData::from_owned_data(&owned_data.0);
                let mut queue = YCQueue::new(shared.meta, shared.data)
                    .expect("Failed to create producer queue");

                barrier.wait();

                let mut local = 0u64;
                let mut backoff_pow = 0;

                while !stop.load(Ordering::Relaxed) {
                    if params.batch_size == 1 {
                        match queue.get_produce_slot() {
                            Ok(slot) => {
                                touch_produce(slot.data, touch_stride);
                                black_box(&slot.data);
                                queue
                                    .mark_slot_produced(slot)
                                    .expect("Failed to mark slot produced");
                                local += 1;
                                backoff_pow = 0;
                            }
                            Err(YCQueueError::OutOfSpace) | Err(YCQueueError::SlotNotReady) => {
                                backoff(&mut backoff_pow);
                            }
                            Err(e) => panic!("Producer {idx} error: {e:?}"),
                        }
                    } else {
                        match queue.get_produce_slots(params.batch_size, true) {
                            Ok(mut slots) => {
                                slots.iter_mut().for_each(|slot| {
                                    touch_produce(slot.data, touch_stride);
                                    black_box(&slot.data);
                                });
                                local += slots.len() as u64;
                                queue
                                    .mark_slots_produced(slots)
                                    .expect("Failed to mark slots produced");
                                backoff_pow = 0;
                            }
                            Err(YCQueueError::OutOfSpace) | Err(YCQueueError::SlotNotReady) => {
                                backoff(&mut backoff_pow);
                            }
                            Err(e) => panic!("Producer {idx} error: {e:?}"),
                        }
                    }
                }

                produced.fetch_add(local, Ordering::Relaxed);
            }));
        }

        for (idx, core_id) in consumer_cores.iter().copied().enumerate() {
            let barrier = Arc::clone(&barrier);
            let stop = Arc::clone(&stop);
            let consumed = Arc::clone(&consumed);
            let owned_data = Arc::clone(&owned_data);
            let params = *params;

            handles.push(s.spawn(move || {
                let cores = core_affinity::get_core_ids().unwrap();
                core_affinity::set_for_current(cores[core_id]);

                let shared = YCQueueSharedData::from_owned_data(&owned_data.0);
                let mut queue = YCQueue::new(shared.meta, shared.data)
                    .expect("Failed to create consumer queue");

                barrier.wait();

                let mut local = 0u64;
                let mut backoff_pow = 0;

                while !stop.load(Ordering::Relaxed) {
                    if params.batch_size == 1 {
                        match queue.get_consume_slot() {
                            Ok(slot) => {
                                touch_consume(slot.data, touch_stride);
                                black_box(&slot.data);
                                queue
                                    .mark_slot_consumed(slot)
                                    .expect("Failed to mark slot consumed");
                                local += 1;
                                backoff_pow = 0;
                            }
                            Err(YCQueueError::EmptyQueue) | Err(YCQueueError::SlotNotReady) => {
                                backoff(&mut backoff_pow);
                            }
                            Err(e) => panic!("Consumer {idx} error: {e:?}"),
                        }
                    } else {
                        match queue.get_consume_slots(params.batch_size, true) {
                            Ok(slots) => {
                                slots.iter().for_each(|slot| {
                                    touch_consume(slot.data, touch_stride);
                                    black_box(&slot.data);
                                });
                                local += slots.len() as u64;
                                queue
                                    .mark_slots_consumed(slots)
                                    .expect("Failed to mark slots consumed");
                                backoff_pow = 0;
                            }
                            Err(YCQueueError::EmptyQueue) | Err(YCQueueError::SlotNotReady) => {
                                backoff(&mut backoff_pow);
                            }
                            Err(e) => panic!("Consumer {idx} error: {e:?}"),
                        }
                    }
                }

                consumed.fetch_add(local, Ordering::Relaxed);
            }));
        }

        // Timer thread
        {
            let barrier = Arc::clone(&barrier);
            let stop = Arc::clone(&stop);
            handles.push(s.spawn(move || {
                barrier.wait();
                let start = Instant::now();
                {
                    let mut st = start_time_timer.lock().unwrap();
                    *st = Some(start);
                }
                thread::sleep(sample_duration);
                stop.store(true, Ordering::Relaxed);
            }));
        }

        for handle in handles {
            handle.join().expect("Worker thread panicked");
        }
    });

    let start = start_time.lock().unwrap().expect("Start time not set");
    let elapsed = start.elapsed();

    let produced = produced.load(Ordering::Relaxed);
    let consumed = consumed.load(Ordering::Relaxed);
    let items = std::cmp::min(produced, consumed);

    (elapsed, items)
}

fn is_verbose_mode() -> bool {
    if std::env::var("CRITERION_DEBUG").is_ok() {
        return true;
    }

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

fn bench_mpmc(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpmc");

    let sample_duration = Duration::from_millis(200);

    let capacities = vec![32, 1024];
    let payload_sizes = vec![32, 512];
    let batch_sizes = vec![1, 16, 32];
    let producer_counts = vec![2, 4];
    let consumer_counts = vec![2, 4];

    for capacity in &capacities {
        for payload_size in &payload_sizes {
            for batch_size in &batch_sizes {
                for producers in &producer_counts {
                    for consumers in &consumer_counts {
                        let params = Params {
                            capacity: *capacity,
                            payload_size: *payload_size,
                            batch_size: *batch_size,
                            producers: *producers,
                            consumers: *consumers,
                        };

                        let (_probe_dt, probe_items) = run_mpmc_sample(&params, sample_duration);
                        let items_per_sample = probe_items.max(1);

                        group.throughput(Throughput::Elements(items_per_sample));

                        let is_verbose = is_verbose_mode();
                        let id = params.id();
                        group.bench_with_input(
                            BenchmarkId::from_parameter(id),
                            &params,
                            |b, params| {
                                b.iter_custom(|iters| {
                                    let duration = sample_duration
                                        .checked_mul(
                                            u32::try_from(iters)
                                                .expect("iters was too large to fit in u32"),
                                        )
                                        .expect("sample_duration overflow");

                                    let (time_taken, items_processed) =
                                        run_mpmc_sample(params, duration);

                                    let secs = time_taken.as_secs_f64();
                                    if secs > 0.0 && is_verbose {
                                        let throughput = items_processed as f64 / secs;
                                        println!(
                                            "params={:?} iters={} items_processed={} time_taken={:?} throughput={:.0} items/s",
                                            params, iters, items_processed, time_taken, throughput
                                        );
                                    }

                                    Duration::from_nanos(
                                        (time_taken.as_nanos()
                                            * items_processed as u128
                                            / (iters as u128 * items_per_sample as u128))
                                            as u64,
                                    )
                                });
                            },
                        );
                    }
                }
            }
        }
    }

    group.finish();
}

criterion_group!(benches, bench_mpmc);
criterion_main!(benches);
