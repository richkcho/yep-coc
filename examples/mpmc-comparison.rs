use clap::Parser;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use test_support::utils::{align_to_cache_line, copy_str_to_slice, str_from_u8};
use yep_coc::{
    YCQueue, YCQueueError,
    queue_alloc_helpers::{YCQueueOwnedData, YCQueueSharedData},
};

const PATTERN: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
const INDEX_PREFIX_LEN: usize = std::mem::size_of::<u32>();

/// Compare multi-producer/multi-consumer performance between YCQueue, Mutex+VecDeque, and Flume.
#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Queue depth (total number of slots)
    #[arg(short = 'd', long, default_value = "64")]
    queue_depth: u16,

    /// Maximum number of in-flight messages
    #[arg(short = 'f', long, default_value = "32")]
    in_flight_count: u16,

    /// Length of messages to send and validate in bytes (will be aligned to cache line size)
    #[arg(short = 'l', long, default_value = "0")]
    msg_check_len: u16,

    /// Total number of messages to send
    #[arg(short = 'n', long, default_value = "10000")]
    msg_count: u32,

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

fn run_ycqueue(args: &Args, slot_size: u16, default_message: &str) -> Duration {
    let validation_len = if args.msg_check_len > 0 {
        std::cmp::max(args.msg_check_len, INDEX_PREFIX_LEN as u16)
    } else {
        0
    };

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
    let validation_storage = Arc::new(Mutex::new(Vec::with_capacity(if args.msg_check_len > 0 {
        args.msg_count as usize
    } else {
        0
    })));
    let producer_thread_count = args.producer_threads as u32;
    let base_messages_per_thread = args.msg_count / producer_thread_count;
    let extra_messages = args.msg_count % producer_thread_count;

    let start_time: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
    let end_time: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
    
    // Create barrier to synchronize all threads before starting benchmark
    let total_threads = args.producer_threads as usize + args.consumer_threads as usize;
    let barrier = Arc::new(Barrier::new(total_threads));

    thread::scope(|s| {
        let mut next_index = 0u32;

        // Spawn producer threads
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
            let in_flight_limit = args.in_flight_count;
            let msg_check_len = args.msg_check_len;
            let start_time = Arc::clone(&start_time);
            let barrier = Arc::clone(&barrier);

            s.spawn(move || {
                // Wait for all threads to be ready
                barrier.wait();
                
                // First producer thread sets the start time after barrier
                if thread_idx == 0 {
                    *start_time.lock().unwrap() = Some(Instant::now());
                }

                for msg_index in range_start..range_end {
                    loop {
                        if queue.in_flight_count() >= in_flight_limit {
                            thread::yield_now();
                            continue;
                        }

                        match queue.get_produce_slot() {
                            Ok(slot) => {
                                if msg_check_len > 0 {
                                    slot.data[..INDEX_PREFIX_LEN]
                                        .copy_from_slice(&msg_index.to_le_bytes());

                                    for i in 0..validation_len.saturating_sub(INDEX_PREFIX_LEN) {
                                        let b = PATTERN.as_bytes()
                                            [(msg_index as usize + i) % PATTERN.len()];
                                        slot.data[INDEX_PREFIX_LEN + i] = b;
                                    }
                                } else {
                                    copy_str_to_slice(default_message, slot.data);
                                }

                                if verbose {
                                    println!("YCQueue send: {}", str_from_u8(slot.data));
                                }

                                queue.mark_slot_produced(slot).unwrap();
                                break;
                            }
                            Err(YCQueueError::OutOfSpace) | Err(YCQueueError::SlotNotReady) => {
                                thread::yield_now();
                            }
                            Err(e) => panic!("YCQueue producer error: {e:?}"),
                        }
                    }
                }
            });
        }

        // Spawn consumer threads
        for mut queue in consumer_queues {
            let consumed_count = Arc::clone(&consumed_count);
            let validation_len = validation_len as usize;
            let msg_count = args.msg_count;
            let verbose = args.verbose;
            let validation_storage = Arc::clone(&validation_storage);
            let msg_check_len = args.msg_check_len;
            let end_time = Arc::clone(&end_time);
            let barrier = Arc::clone(&barrier);

            s.spawn(move || {
                // Wait for all threads to be ready
                barrier.wait();
                
                let mut local_validations = if msg_check_len > 0 {
                    Vec::with_capacity((msg_count as usize / 2) + 1)
                } else {
                    Vec::new()
                };

                loop {
                    let current_count = consumed_count.load(Ordering::Relaxed);
                    if current_count >= msg_count {
                        break;
                    }

                    match queue.get_consume_slot() {
                        Ok(slot) => {
                            let msg_index = consumed_count.fetch_add(1, Ordering::Relaxed);

                            if msg_index >= msg_count {
                                // Another thread already consumed the last message
                                break;
                            }

                            if msg_check_len > 0 {
                                local_validations.push(slot.data[..validation_len].to_vec());
                            }

                            if verbose {
                                let s = str_from_u8(slot.data);
                                println!("YCQueue recv: {s}");
                            }

                            queue.mark_slot_consumed(slot).unwrap();
                        }
                        Err(YCQueueError::EmptyQueue) | Err(YCQueueError::SlotNotReady) => {
                            thread::yield_now();
                        }
                        Err(e) => panic!("YCQueue consumer error: {e:?}"),
                    }
                }

                // Record end time from last consumer to finish
                *end_time.lock().unwrap() = Some(Instant::now());

                if msg_check_len > 0 && !local_validations.is_empty() {
                    let mut guard = validation_storage.lock().unwrap();
                    guard.extend(local_validations);
                }
            });
        }
    });

    // Validate messages if needed
    if args.msg_check_len > 0 {
        let messages = Arc::try_unwrap(validation_storage)
            .expect("validation storage still has outstanding references")
            .into_inner()
            .expect("validation storage mutex poisoned");

        if messages.len() != args.msg_count as usize {
            panic!(
                "YCQueue: Expected {} validated messages but collected {}",
                args.msg_count,
                messages.len()
            );
        }

        let mut seen = vec![false; args.msg_count as usize];
        let payload_len = (validation_len as usize).saturating_sub(INDEX_PREFIX_LEN);

        for message in messages {
            if message.len() < INDEX_PREFIX_LEN {
                panic!("YCQueue: Validated message shorter than index header");
            }

            let mut index_bytes = [0u8; 4];
            index_bytes.copy_from_slice(&message[..INDEX_PREFIX_LEN]);
            let index = u32::from_le_bytes(index_bytes);

            if index >= args.msg_count {
                panic!("YCQueue: Received message index {index} out of expected range");
            }

            if seen[index as usize] {
                panic!("YCQueue: Duplicate message index {index} detected during validation");
            }
            seen[index as usize] = true;

            for (offset, &byte) in message[INDEX_PREFIX_LEN..INDEX_PREFIX_LEN + payload_len]
                .iter()
                .enumerate()
            {
                let expected = PATTERN.as_bytes()[(index as usize + offset) % PATTERN.len()];
                if byte != expected {
                    panic!(
                        "YCQueue: Message content mismatch at message {index}, byte {offset}:\nExpected: '{expected}'\nReceived: '{byte}'",
                    );
                }
            }
        }

        if seen.iter().any(|received| !received) {
            panic!("YCQueue: Not all expected message indices were observed during validation");
        }
    }

    let start = start_time.lock().unwrap().unwrap();
    let end = end_time.lock().unwrap().unwrap();
    end.duration_since(start)
}

fn run_mutex_vecdeque(args: &Args, slot_size: u16, default_message: &str) -> Duration {
    let validation_len = if args.msg_check_len > 0 {
        std::cmp::max(args.msg_check_len, INDEX_PREFIX_LEN as u16) as usize
    } else {
        0
    };

    let queue: Arc<Mutex<VecDeque<Vec<u8>>>> = Arc::new(Mutex::new(VecDeque::with_capacity(
        args.queue_depth as usize,
    )));

    let consumed_count = Arc::new(AtomicU32::new(0));
    let validation_storage = Arc::new(Mutex::new(Vec::with_capacity(if args.msg_check_len > 0 {
        args.msg_count as usize
    } else {
        0
    })));
    let producer_thread_count = args.producer_threads as u32;
    let base_messages_per_thread = args.msg_count / producer_thread_count;
    let extra_messages = args.msg_count % producer_thread_count;

    let start_time: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
    let end_time: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
    
    // Create barrier to synchronize all threads before starting benchmark
    let total_threads = args.producer_threads as usize + args.consumer_threads as usize;
    let barrier = Arc::new(Barrier::new(total_threads));

    thread::scope(|s| {
        let mut next_index = 0u32;

        // Spawn producer threads
        for thread_idx in 0..args.producer_threads as usize {
            let extra = if thread_idx < extra_messages as usize {
                1u32
            } else {
                0u32
            };
            let range_start = next_index;
            let range_end = range_start + base_messages_per_thread + extra;
            next_index = range_end;

            let queue = Arc::clone(&queue);
            let args = args.clone();
            let start_time = Arc::clone(&start_time);
            let barrier = Arc::clone(&barrier);

            s.spawn(move || {
                // Wait for all threads to be ready
                barrier.wait();
                
                // First producer thread sets the start time after barrier
                if thread_idx == 0 {
                    *start_time.lock().unwrap() = Some(Instant::now());
                }

                for msg_index in range_start..range_end {
                    loop {
                        // Enforce in-flight limit using current queue length
                        let can_send = {
                            let q = queue.lock().unwrap();
                            q.len() < args.in_flight_count as usize
                                && q.len() < args.queue_depth as usize
                        };

                        if !can_send {
                            thread::yield_now();
                            continue;
                        }

                        // Build message buffer
                        let mut buf = vec![0u8; slot_size as usize];
                        if args.msg_check_len > 0 {
                            buf[..INDEX_PREFIX_LEN].copy_from_slice(&msg_index.to_le_bytes());

                            for i in 0..validation_len.saturating_sub(INDEX_PREFIX_LEN) {
                                let b =
                                    PATTERN.as_bytes()[(msg_index as usize + i) % PATTERN.len()];
                                buf[INDEX_PREFIX_LEN + i] = b;
                            }
                        } else {
                            copy_str_to_slice(default_message, &mut buf);
                        }

                        if args.verbose {
                            println!("Mutex+VecDeque send: {}", str_from_u8(&buf));
                        }

                        // Enqueue
                        {
                            let mut q = queue.lock().unwrap();
                            if q.len() < args.queue_depth as usize {
                                q.push_back(buf);
                                break;
                            }
                        }
                    }
                }
            });
        }

        // Spawn consumer threads
        for _ in 0..args.consumer_threads {
            let queue = Arc::clone(&queue);
            let consumed_count = Arc::clone(&consumed_count);
            let args = args.clone();
            let validation_storage = Arc::clone(&validation_storage);
            let end_time = Arc::clone(&end_time);
            let barrier = Arc::clone(&barrier);

            s.spawn(move || {
                // Wait for all threads to be ready
                barrier.wait();
                
                let mut local_validations = if args.msg_check_len > 0 {
                    Vec::with_capacity((args.msg_count as usize / 2) + 1)
                } else {
                    Vec::new()
                };

                loop {
                    let current_count = consumed_count.load(Ordering::Relaxed);
                    if current_count >= args.msg_count {
                        break;
                    }

                    let maybe_msg = {
                        let mut q = queue.lock().unwrap();
                        q.pop_front()
                    };

                    if let Some(buf) = maybe_msg {
                        let msg_index = consumed_count.fetch_add(1, Ordering::Relaxed);

                        if msg_index >= args.msg_count {
                            // Another thread already consumed the last message
                            break;
                        }

                        if args.msg_check_len > 0 {
                            local_validations.push(buf[..validation_len].to_vec());
                        }

                        if args.verbose {
                            let s = str_from_u8(&buf);
                            println!("Mutex+VecDeque recv: {s}");
                        }
                    } else {
                        thread::yield_now();
                    }
                }

                *end_time.lock().unwrap() = Some(Instant::now());

                if args.msg_check_len > 0 && !local_validations.is_empty() {
                    let mut guard = validation_storage.lock().unwrap();
                    guard.extend(local_validations);
                }
            });
        }
    });

    // Validate messages if needed
    if args.msg_check_len > 0 {
        let messages = Arc::try_unwrap(validation_storage)
            .expect("validation storage still has outstanding references")
            .into_inner()
            .expect("validation storage mutex poisoned");

        if messages.len() != args.msg_count as usize {
            panic!(
                "Mutex+VecDeque: Expected {} validated messages but collected {}",
                args.msg_count,
                messages.len()
            );
        }

        let mut seen = vec![false; args.msg_count as usize];
        let payload_len = validation_len.saturating_sub(INDEX_PREFIX_LEN);

        for message in messages {
            if message.len() < INDEX_PREFIX_LEN {
                panic!("Mutex+VecDeque: Validated message shorter than index header");
            }

            let mut index_bytes = [0u8; 4];
            index_bytes.copy_from_slice(&message[..INDEX_PREFIX_LEN]);
            let index = u32::from_le_bytes(index_bytes);

            if index >= args.msg_count {
                panic!("Mutex+VecDeque: Received message index {index} out of expected range");
            }

            if seen[index as usize] {
                panic!(
                    "Mutex+VecDeque: Duplicate message index {index} detected during validation"
                );
            }
            seen[index as usize] = true;

            for (offset, &byte) in message[INDEX_PREFIX_LEN..INDEX_PREFIX_LEN + payload_len]
                .iter()
                .enumerate()
            {
                let expected = PATTERN.as_bytes()[(index as usize + offset) % PATTERN.len()];
                if byte != expected {
                    panic!(
                        "Mutex+VecDeque: Message content mismatch at message {index}, byte {offset}:\nExpected: '{expected}'\nReceived: '{byte}'",
                    );
                }
            }
        }

        if seen.iter().any(|received| !received) {
            panic!(
                "Mutex+VecDeque: Not all expected message indices were observed during validation"
            );
        }
    }

    let start = start_time.lock().unwrap().unwrap();
    let end = end_time.lock().unwrap().unwrap();
    end.duration_since(start)
}

fn run_flume(args: &Args, slot_size: u16, default_message: &str) -> Duration {
    let validation_len = if args.msg_check_len > 0 {
        std::cmp::max(args.msg_check_len, INDEX_PREFIX_LEN as u16) as usize
    } else {
        0
    };

    let (sender, receiver) = flume::bounded::<Vec<u8>>(args.queue_depth as usize);

    let consumed_count = Arc::new(AtomicU32::new(0));
    let validation_storage = Arc::new(Mutex::new(Vec::with_capacity(if args.msg_check_len > 0 {
        args.msg_count as usize
    } else {
        0
    })));
    let producer_thread_count = args.producer_threads as u32;
    let base_messages_per_thread = args.msg_count / producer_thread_count;
    let extra_messages = args.msg_count % producer_thread_count;

    let start_time: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
    let end_time: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
    
    // Create barrier to synchronize all threads before starting benchmark
    let total_threads = args.producer_threads as usize + args.consumer_threads as usize;
    let barrier = Arc::new(Barrier::new(total_threads));

    thread::scope(|s| {
        let mut next_index = 0u32;

        // Spawn producer threads
        for thread_idx in 0..args.producer_threads as usize {
            let extra = if thread_idx < extra_messages as usize {
                1u32
            } else {
                0u32
            };
            let range_start = next_index;
            let range_end = range_start + base_messages_per_thread + extra;
            next_index = range_end;

            let sender = sender.clone();
            let args = args.clone();
            let start_time = Arc::clone(&start_time);
            let barrier = Arc::clone(&barrier);

            s.spawn(move || {
                // Wait for all threads to be ready
                barrier.wait();
                
                // First producer thread sets the start time after barrier
                if thread_idx == 0 {
                    *start_time.lock().unwrap() = Some(Instant::now());
                }

                for msg_index in range_start..range_end {
                    loop {
                        if sender.len() >= args.in_flight_count as usize {
                            thread::yield_now();
                            continue;
                        }

                        let mut buf = vec![0u8; slot_size as usize];
                        if args.msg_check_len > 0 {
                            buf[..INDEX_PREFIX_LEN].copy_from_slice(&msg_index.to_le_bytes());

                            for i in 0..validation_len.saturating_sub(INDEX_PREFIX_LEN) {
                                let b =
                                    PATTERN.as_bytes()[(msg_index as usize + i) % PATTERN.len()];
                                buf[INDEX_PREFIX_LEN + i] = b;
                            }
                        } else {
                            copy_str_to_slice(default_message, &mut buf);
                        }

                        if args.verbose {
                            println!("Flume send: {}", str_from_u8(&buf));
                        }

                        sender.send(buf).expect("Flume channel closed");
                        break;
                    }
                }
            });
        }

        // Spawn consumer threads
        for _ in 0..args.consumer_threads {
            let receiver = receiver.clone();
            let consumed_count = Arc::clone(&consumed_count);
            let args = args.clone();
            let validation_storage = Arc::clone(&validation_storage);
            let end_time = Arc::clone(&end_time);
            let barrier = Arc::clone(&barrier);

            s.spawn(move || {
                // Wait for all threads to be ready
                barrier.wait();
                
                let mut local_validations = if args.msg_check_len > 0 {
                    Vec::with_capacity((args.msg_count as usize / 2) + 1)
                } else {
                    Vec::new()
                };

                loop {
                    let current_count = consumed_count.load(Ordering::Relaxed);
                    if current_count >= args.msg_count {
                        break;
                    }

                    match receiver.try_recv() {
                        Ok(buf) => {
                            let msg_index = consumed_count.fetch_add(1, Ordering::Relaxed);

                            if msg_index >= args.msg_count {
                                // Another thread already consumed the last message
                                break;
                            }

                            if args.msg_check_len > 0 {
                                local_validations.push(buf[..validation_len].to_vec());
                            }

                            if args.verbose {
                                let s = str_from_u8(&buf);
                                println!("Flume recv: {s}");
                            }
                        }
                        Err(_) => {
                            thread::yield_now();
                        }
                    }
                }

                *end_time.lock().unwrap() = Some(Instant::now());

                if args.msg_check_len > 0 && !local_validations.is_empty() {
                    let mut guard = validation_storage.lock().unwrap();
                    guard.extend(local_validations);
                }
            });
        }
    });

    // Validate messages if needed
    if args.msg_check_len > 0 {
        let messages = Arc::try_unwrap(validation_storage)
            .expect("validation storage still has outstanding references")
            .into_inner()
            .expect("validation storage mutex poisoned");

        if messages.len() != args.msg_count as usize {
            panic!(
                "Flume: Expected {} validated messages but collected {}",
                args.msg_count,
                messages.len()
            );
        }

        let mut seen = vec![false; args.msg_count as usize];
        let payload_len = validation_len.saturating_sub(INDEX_PREFIX_LEN);

        for message in messages {
            if message.len() < INDEX_PREFIX_LEN {
                panic!("Flume: Validated message shorter than index header");
            }

            let mut index_bytes = [0u8; 4];
            index_bytes.copy_from_slice(&message[..INDEX_PREFIX_LEN]);
            let index = u32::from_le_bytes(index_bytes);

            if index >= args.msg_count {
                panic!("Flume: Received message index {index} out of expected range");
            }

            if seen[index as usize] {
                panic!("Flume: Duplicate message index {index} detected during validation");
            }
            seen[index as usize] = true;

            for (offset, &byte) in message[INDEX_PREFIX_LEN..INDEX_PREFIX_LEN + payload_len]
                .iter()
                .enumerate()
            {
                let expected = PATTERN.as_bytes()[(index as usize + offset) % PATTERN.len()];
                if byte != expected {
                    panic!(
                        "Flume: Message content mismatch at message {index}, byte {offset}:\nExpected: '{expected}'\nReceived: '{byte}'",
                    );
                }
            }
        }

        if seen.iter().any(|received| !received) {
            panic!("Flume: Not all expected message indices were observed during validation");
        }
    }

    let start = start_time.lock().unwrap().unwrap();
    let end = end_time.lock().unwrap().unwrap();
    end.duration_since(start)
}

fn main() {
    let default_message = "hello there";
    let args = Args::parse();

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
    let slot_size = align_to_cache_line(msg_len);

    println!("Comparison run with:");
    println!("  Queue depth: {}", args.queue_depth);
    println!("  Max in-flight messages: {}", args.in_flight_count);
    println!("  Message length: {msg_len}");
    println!("  Slot size (aligned): {slot_size} bytes");
    println!("  Total messages: {}", args.msg_count);
    println!("  Producer threads: {}", args.producer_threads);
    println!("  Consumer threads: {}", args.consumer_threads);

    if args.in_flight_count == 0 || args.queue_depth == 0 {
        panic!("in_flight_count and queue_depth must be greater than zero");
    }
    if args.in_flight_count > args.queue_depth {
        panic!("in_flight_count cannot be larger than queue_depth");
    }
    if args.producer_threads == 0 {
        panic!("At least one producer thread is required");
    }
    if args.consumer_threads == 0 {
        panic!("At least one consumer thread is required");
    }

    let yc_dur = run_ycqueue(&args, slot_size, default_message);
    let mv_dur = run_mutex_vecdeque(&args, slot_size, default_message);
    let flume_dur = run_flume(&args, slot_size, default_message);

    println!("\nResults (lower is better):");
    println!(
        "  YCQueue:          {:.3} us",
        yc_dur.as_nanos() as f64 / 1_000.0
    );
    println!(
        "  Mutex+VecDeque:   {:.3} us",
        mv_dur.as_nanos() as f64 / 1_000.0
    );
    println!(
        "  Flume (bounded):  {:.3} us",
        flume_dur.as_nanos() as f64 / 1_000.0
    );

    let yc_msgs_per_sec = (args.msg_count as f64) / yc_dur.as_secs_f64();
    let mv_msgs_per_sec = (args.msg_count as f64) / mv_dur.as_secs_f64();
    let flume_msgs_per_sec = (args.msg_count as f64) / flume_dur.as_secs_f64();
    println!("\nThroughput:");
    println!("  YCQueue:          {:.2} msgs/s", yc_msgs_per_sec);
    println!("  Mutex+VecDeque:   {:.2} msgs/s", mv_msgs_per_sec);
    println!("  Flume (bounded):  {:.2} msgs/s", flume_msgs_per_sec);
}
