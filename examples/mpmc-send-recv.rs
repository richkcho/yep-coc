//! A multi-producer, multi-consumer send-recv example using YCQueue
//!
//! Note that in general, using multiple producers and consumers on MPMC
//! queues is not recommended due to potential performance degradation.

use clap::Parser;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use test_support::utils::{align_to_cache_line, backoff, copy_str_to_slice, str_from_u8};
use yep_coc::{
    YCQueue, YCQueueError,
    queue_alloc_helpers::{YCQueueOwnedData, YCQueueSharedData},
};

const PATTERN: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
const INDEX_PREFIX_LEN: usize = std::mem::size_of::<u32>();

/// A multi-producer, multi-consumer send-recv example using YCQueue
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Queue depth (total number of slots)
    #[arg(short = 'd', long, default_value = "64")]
    queue_depth: u16,

    /// Number of messages to send/receive per queue operation
    #[arg(short = 'b', long, default_value = "1")]
    batch_size: u16,

    /// Maximum number of in-flight messages
    #[arg(short = 'f', long, default_value = "32")]
    in_flight_count: u16,

    /// Length of messages to send and validate in bytes (will be aligned to cache line size)
    #[arg(short = 'l', long, default_value = "0")]
    msg_check_len: u16,

    /// Total number of messages to send
    #[arg(short = 'n', long, default_value = "10000")]
    msg_count: u32,

    /// Timeout in seconds for producer/consumer loops
    #[arg(short = 't', long, default_value = "10")]
    timeout_secs: u64,

    /// Number of producer threads
    #[arg(short = 'p', long, default_value = "2")]
    producer_threads: u16,

    /// Number of consumer threads
    #[arg(short = 'c', long, default_value = "2")]
    consumer_threads: u16,

    /// Enable verbose logging
    #[arg(short = 'v', long, default_value_t = false)]
    verbose: bool,
}

fn warn_for_thread_counts(args: &Args) {
    let available_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let total_threads = args.producer_threads as usize + args.consumer_threads as usize;

    if total_threads > available_cpus {
        eprintln!(
            "Warning: total thread count ({total_threads}) exceeds available CPUs ({available_cpus})",
        );
    }

    if args.producer_threads as usize > available_cpus {
        eprintln!(
            "Warning: producer thread count ({}) exceeds available CPUs ({available_cpus})",
            args.producer_threads
        );
    }

    if args.consumer_threads as usize > available_cpus {
        eprintln!(
            "Warning: consumer thread count ({}) exceeds available CPUs ({available_cpus})",
            args.consumer_threads
        );
    }
}

fn main() {
    let default_message = "hello there";
    let args = Args::parse();

    warn_for_thread_counts(&args);

    let validation_len = if args.msg_check_len > 0 {
        std::cmp::max(args.msg_check_len, INDEX_PREFIX_LEN as u16)
    } else {
        0
    };
    let msg_len = if args.msg_check_len > 0 {
        validation_len
    } else {
        std::cmp::max(args.msg_check_len, default_message.len() as u16)
    };

    // Align message length to cache line size
    let slot_size = align_to_cache_line(msg_len);

    println!("Starting multi-send-recv test with:");
    println!("  Queue depth: {}", args.queue_depth);
    println!("  Max in-flight messages: {}", args.in_flight_count);
    println!("  Batch size: {}", args.batch_size);
    println!("  Message length: {msg_len}");
    println!("  Queue slot size: {slot_size} bytes");
    println!("  Total messages: {}", args.msg_count);
    println!("  Producer threads: {}", args.producer_threads);
    println!("  Consumer threads: {}", args.consumer_threads);

    if args.in_flight_count != 0 && args.in_flight_count > args.queue_depth {
        panic!("in_flight_count cannot be larger than queue_depth");
    }

    if args.producer_threads == 0 {
        panic!("At least one producer thread is required");
    }

    if args.consumer_threads == 0 {
        panic!("At least one consumer thread is required");
    }

    if args.batch_size == 0 {
        panic!("batch_size must be greater than zero");
    }

    if args.batch_size > args.queue_depth {
        panic!("batch_size cannot exceed queue_depth");
    }

    let owned_data = YCQueueOwnedData::new(args.queue_depth, slot_size);

    let mut producer_queues = Vec::with_capacity(args.producer_threads as usize);
    let mut consumer_queues = Vec::with_capacity(args.consumer_threads as usize);

    for _ in 0..args.producer_threads {
        let shared = YCQueueSharedData::from_owned_data(&owned_data);
        producer_queues.push(YCQueue::new(shared.meta, shared.data).unwrap());
    }

    for _ in 0..args.consumer_threads {
        let shared = YCQueueSharedData::from_owned_data(&owned_data);
        consumer_queues.push(YCQueue::new(shared.meta, shared.data).unwrap());
    }

    let consumed_count = Arc::new(AtomicU32::new(0));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(
        args.producer_threads as usize + args.consumer_threads as usize + 1,
    ));
    let validation_storage = Arc::new(Mutex::new(Vec::with_capacity(if args.msg_check_len > 0 {
        args.msg_count as usize
    } else {
        0
    })));
    let producer_thread_count = args.producer_threads as u32;
    let base_messages_per_thread = args.msg_count / producer_thread_count;
    let extra_messages = args.msg_count % producer_thread_count;

    let earliest_producer_start = Arc::new(Mutex::new(None::<std::time::Instant>));
    let latest_consumer_end = Arc::new(Mutex::new(None::<std::time::Instant>));

    thread::scope(|s| {
        let mut next_index = 0u32;

        // Timer thread: coordinate start and enforce timeout
        {
            let stop = Arc::clone(&stop);
            let done = Arc::clone(&done);
            let barrier = Arc::clone(&barrier);
            s.spawn(move || {
                barrier.wait();
                let timeout = std::time::Duration::from_secs(args.timeout_secs);
                let deadline = std::time::Instant::now() + timeout;
                while !done.load(Ordering::Relaxed) && std::time::Instant::now() < deadline {
                    thread::sleep(std::time::Duration::from_millis(50));
                }
                if !done.load(Ordering::Relaxed) {
                    stop.store(true, Ordering::Relaxed);
                }
            });
        }

        for (thread_idx, mut queue) in producer_queues.into_iter().enumerate() {
            let extra = if thread_idx < extra_messages as usize {
                1u32
            } else {
                0u32
            };
            let range_start = next_index;
            let range_end = range_start + base_messages_per_thread + extra;
            next_index = range_end;

            let validation_len = validation_len as usize;
            let verbose = args.verbose;
            let in_flight_limit =
                if args.in_flight_count == 0 || args.in_flight_count == args.queue_depth {
                    None
                } else {
                    Some(args.in_flight_count)
                };
            let msg_check_len = args.msg_check_len;
            let batch_size = args.batch_size;
            let earliest_start = Arc::clone(&earliest_producer_start);
            let stop = Arc::clone(&stop);
            let barrier = Arc::clone(&barrier);
            let done = Arc::clone(&done);

            s.spawn(move || {
                let mut local_sent = 0_u32;
                let mut next_msg_index = range_start;
                let thread_start = std::time::Instant::now();
                let mut backoff_pow = 0;
                barrier.wait();

                while next_msg_index < range_end {
                    if stop.load(Ordering::Relaxed) {
                        panic!("Producer thread {thread_idx} timed out after {:?} (range end {range_end})", std::time::Duration::from_secs(args.timeout_secs));
                    }
                    loop {
                        let remaining = range_end - next_msg_index;
                        let request = if let Some(limit) = in_flight_limit {
                            let in_flight = queue.in_flight_count();
                            if in_flight >= limit {
                                backoff(&mut backoff_pow);
                                continue;
                            }
                            let available_in_flight = limit.saturating_sub(queue.in_flight_count());
                            batch_size.min(available_in_flight).min(remaining as u16)
                        } else {
                            batch_size.min(remaining as u16)
                        };

                        if request == 0 {
                            if stop.load(Ordering::Relaxed) {
                                panic!("Producer thread {thread_idx} timed out waiting for in-flight space");
                            }
                            backoff(&mut backoff_pow);
                            continue;
                        }

                        match queue.get_produce_slots(request, true) {
                            Ok(mut slots) => {
                                for slot in slots.iter_mut() {
                                    if msg_check_len > 0 {
                                        slot.data[..INDEX_PREFIX_LEN]
                                            .copy_from_slice(&next_msg_index.to_le_bytes());

                                        for i in 0..validation_len.saturating_sub(INDEX_PREFIX_LEN)
                                        {
                                            let b = PATTERN.as_bytes()
                                                [(next_msg_index as usize + i) % PATTERN.len()];
                                            slot.data[INDEX_PREFIX_LEN + i] = b;
                                        }
                                    } else {
                                        copy_str_to_slice(default_message, slot.data);
                                    }

                                    if verbose {
                                        println!("Producer thread sent message {next_msg_index}");
                                    }

                                    next_msg_index += 1;
                                }

                                local_sent += slots.len() as u32;
                                queue.mark_slots_produced(slots).unwrap();
                                backoff_pow = 0;
                                break;
                            }
                            Err(YCQueueError::OutOfSpace) | Err(YCQueueError::SlotNotReady) => {
                                backoff(&mut backoff_pow);
                            }
                            Err(e) => panic!("Producer error: {e:?}"),
                        }
                    }
                }

                // Record the earliest start time across all producer threads, but after work is done
                {
                    let mut guard = earliest_start.lock().unwrap();
                    match *guard {
                        Some(ref mut current) => {
                            if thread_start < *current {
                                *current = thread_start;
                            }
                        }
                        None => {
                            *guard = Some(thread_start);
                        }
                    }
                }

                if verbose {
                    println!("Producer thread finished after sending {local_sent} messages",);
                }
                done.store(true, Ordering::Relaxed);
            });
        }

        for mut queue in consumer_queues {
            let consumed_count = Arc::clone(&consumed_count);
            let validation_len = validation_len as usize;
            let msg_count = args.msg_count;
            let verbose = args.verbose;
            let validation_storage = Arc::clone(&validation_storage);
            let msg_check_len = args.msg_check_len;
            let latest_end = Arc::clone(&latest_consumer_end);
            let stop = Arc::clone(&stop);
            let barrier = Arc::clone(&barrier);
            let done = Arc::clone(&done);

            s.spawn(move || {
                let mut local_received = 0_u32;
                let mut backoff_pow = 0;
                let mut local_validations = if msg_check_len > 0 {
                    Vec::with_capacity((msg_count as usize / 2) + 1)
                } else {
                    Vec::new()
                };
                barrier.wait();
                loop {
                    if stop.load(Ordering::Relaxed) {
                        panic!(
                            "Consumer timed out after {:?} while waiting to receive",
                            std::time::Duration::from_secs(args.timeout_secs)
                        );
                    }
                    if consumed_count.load(Ordering::Relaxed) >= msg_count {
                        break;
                    }

                    match queue.get_consume_slots(args.batch_size, true) {
                        Ok(slots) => {
                            let processed = slots.len() as u32;
                            for slot in &slots {
                                let msg_index = consumed_count.fetch_add(1, Ordering::Relaxed);

                                if msg_index >= msg_count {
                                    panic!("Received more messages than expected");
                                }

                                if msg_check_len > 0 {
                                    local_validations.push(slot.data[..validation_len].to_vec());
                                    if verbose {
                                        println!("Consumer thread received message {msg_index}");
                                    }
                                } else {
                                    let msg = str_from_u8(slot.data);
                                    if verbose {
                                        println!(
                                            "Consumer thread received message {msg_index}: {msg}"
                                        );
                                    }
                                }
                            }

                            queue.mark_slots_consumed(slots).unwrap();
                            local_received += processed;
                            backoff_pow = 0;
                        }
                        Err(YCQueueError::EmptyQueue) | Err(YCQueueError::SlotNotReady) => {
                            backoff(&mut backoff_pow);
                        }
                        Err(e) => panic!("Consumer error: {e:?}"),
                    }
                }

                if verbose {
                    println!("Consumer thread finished after receiving {local_received} messages");
                }

                let thread_end = std::time::Instant::now();
                {
                    let mut guard = latest_end.lock().unwrap();
                    match *guard {
                        Some(ref mut current) => {
                            if thread_end > *current {
                                *current = thread_end;
                            }
                        }
                        None => {
                            *guard = Some(thread_end);
                        }
                    }
                }

                if msg_check_len > 0 && !local_validations.is_empty() {
                    let mut guard = validation_storage.lock().unwrap();
                    guard.extend(local_validations);
                }
                done.store(true, Ordering::Relaxed);
            });
        }
    });

    let earliest_start = *earliest_producer_start.lock().unwrap();
    let latest_end = *latest_consumer_end.lock().unwrap();
    let elapsed = match (earliest_start, latest_end) {
        (Some(start), Some(end)) => end.duration_since(start),
        _ => std::time::Duration::default(),
    };
    println!(
        "Multi-send-recv test finished in {:?} us!",
        elapsed.as_micros()
    );

    let messages = Arc::try_unwrap(validation_storage)
        .expect("validation storage still has outstanding references")
        .into_inner()
        .expect("validation storage mutex poisoned");

    if args.msg_check_len == 0 {
        return;
    }

    if messages.len() != args.msg_count as usize {
        panic!(
            "Expected {} validated messages but collected {}",
            args.msg_count,
            messages.len()
        );
    }

    let mut seen = vec![false; args.msg_count as usize];
    let payload_len = (validation_len as usize).saturating_sub(INDEX_PREFIX_LEN);

    for message in messages {
        if message.len() < INDEX_PREFIX_LEN {
            panic!("Validated message shorter than index header");
        }

        let mut index_bytes = [0u8; 4];
        index_bytes.copy_from_slice(&message[..INDEX_PREFIX_LEN]);
        let index = u32::from_le_bytes(index_bytes);

        if index >= args.msg_count {
            panic!("Received message index {index} out of expected range");
        }

        if seen[index as usize] {
            panic!("Duplicate message index {index} detected during validation");
        }
        seen[index as usize] = true;

        for (offset, &byte) in message[INDEX_PREFIX_LEN..INDEX_PREFIX_LEN + payload_len]
            .iter()
            .enumerate()
        {
            let expected = PATTERN.as_bytes()[(index as usize + offset) % PATTERN.len()];
            if byte != expected {
                panic!(
                    "Message content mismatch at message {index}, byte {offset}:\nExpected: '{expected}'\nReceived: '{byte}'",
                );
            }
        }
    }

    if seen.iter().any(|received| !received) {
        panic!("Not all expected message indices were observed during validation");
    }
}
