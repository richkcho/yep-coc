use clap::Parser;
use std::collections::VecDeque;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use test_support::utils::{align_to_cache_line, copy_str_to_slice, str_from_u8};
use yep_coc::{
    YCQueue, YCQueueError,
    queue_alloc_helpers::{YCQueueOwnedData, YCQueueSharedData},
};

#[cfg(feature = "futex")]
use yep_coc::{
    YCFutexQueue,
    queue_alloc_helpers::{YCFutexQueueOwnedData, YCFutexQueueSharedData},
};

const PATTERN: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

/// Compare single-producer/single-consumer performance between YCQueue and Mutex+VecDeque.
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

    /// Enable verbose logging
    #[arg(short = 'v', long, default_value_t = false)]
    verbose: bool,
}

fn run_ycqueue(args: &Args, slot_size: u16, default_message: &str) -> Duration {
    let owned_data = YCQueueOwnedData::new(args.queue_depth, slot_size);
    let consumer_data = YCQueueSharedData::from_owned_data(&owned_data);
    let producer_data = YCQueueSharedData::from_owned_data(&owned_data);

    let mut consumer_queue = YCQueue::new(consumer_data.meta, consumer_data.data).unwrap();
    let mut producer_queue = YCQueue::new(producer_data.meta, producer_data.data).unwrap();

    let start_time: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
    let end_time: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));

    // Create barrier to synchronize both threads before starting benchmark
    let barrier = Arc::new(Barrier::new(2));

    thread::scope(|s| {
        {
            let end_time = Arc::clone(&end_time);
            let barrier = Arc::clone(&barrier);
            s.spawn(move || {
            // Wait for both threads to be ready
            barrier.wait();

            let mut messages_received = 0u32;
            while messages_received < args.msg_count {
                match consumer_queue.get_consume_slot() {
                    Ok(consume_slot) => {
                        // Optional validation
                        if args.msg_check_len > 0 {
                            for i in 0..args.msg_check_len as usize {
                                let expected = PATTERN.as_bytes()
                                    [(messages_received as usize + i) % PATTERN.len()];
                                let got = consume_slot.data[i];
                                if expected != got {
                                    panic!(
                                        "YCQueue mismatch at message {}, byte {}: expected '{}' got '{}'",
                                        messages_received, i, expected, got
                                    );
                                }
                            }
                        }

                        if args.verbose {
                            let s = str_from_u8(consume_slot.data);
                            println!("YCQueue recv: {s}");
                        }

                        consumer_queue.mark_slot_consumed(consume_slot).unwrap();
                        messages_received += 1;
                    }
                    Err(YCQueueError::EmptyQueue) | Err(YCQueueError::SlotNotReady) => {
                        thread::yield_now();
                    }
                    Err(e) => panic!("YCQueue consumer error: {e:?}"),
                }
            }
                *end_time.lock().unwrap() = Some(Instant::now());
            });
        }

        {
            let start_time = Arc::clone(&start_time);
            let barrier = Arc::clone(&barrier);
            s.spawn(move || {
                // Wait for both threads to be ready
                barrier.wait();

                // Producer thread sets the start time after barrier
                *start_time.lock().unwrap() = Some(Instant::now());

                let mut messages_sent = 0u32;
                while messages_sent < args.msg_count {
                    if producer_queue.in_flight_count() >= args.in_flight_count {
                        thread::yield_now();
                        continue;
                    }

                    match producer_queue.get_produce_slot() {
                        Ok(produce_slot) => {
                            if args.msg_check_len > 0 {
                                for i in 0..args.msg_check_len as usize {
                                    let b = PATTERN.as_bytes()
                                        [(messages_sent as usize + i) % PATTERN.len()];
                                    produce_slot.data[i] = b;
                                }
                            } else {
                                copy_str_to_slice(default_message, produce_slot.data);
                            }

                            if args.verbose {
                                println!("YCQueue send: {}", str_from_u8(produce_slot.data));
                            }

                            producer_queue.mark_slot_produced(produce_slot).unwrap();
                            messages_sent += 1;
                        }
                        Err(YCQueueError::OutOfSpace) | Err(YCQueueError::SlotNotReady) => {
                            thread::yield_now();
                        }
                        Err(e) => panic!("YCQueue producer error: {e:?}"),
                    }
                }
            });
        }
    });

    let start = start_time.lock().unwrap().unwrap();
    let end = end_time.lock().unwrap().unwrap();
    end.duration_since(start)
}

#[cfg(feature = "futex")]
fn run_ycfutexqueue(args: &Args, slot_size: u16, default_message: &str) -> Duration {
    let owned_data = YCFutexQueueOwnedData::new(args.queue_depth, slot_size);
    let consumer_data = YCFutexQueueSharedData::from_owned_data(&owned_data);
    let producer_data = YCFutexQueueSharedData::from_owned_data(&owned_data);

    let mut consumer_queue = YCFutexQueue::new(
        YCQueue::new(consumer_data.data.meta, consumer_data.data.data).unwrap(),
        consumer_data.count,
    );
    let mut producer_queue = YCFutexQueue::new(
        YCQueue::new(producer_data.data.meta, producer_data.data.data).unwrap(),
        producer_data.count,
    );

    let start_time: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
    let end_time: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));

    // Create barrier to synchronize both threads before starting benchmark
    let barrier = Arc::new(Barrier::new(2));

    let timeout = Duration::from_millis(1);

    thread::scope(|s| {
        {
            let end_time = Arc::clone(&end_time);
            let barrier = Arc::clone(&barrier);
            s.spawn(move || {
                // Wait for both threads to be ready
                barrier.wait();

                let mut messages_received = 0u32;
                while messages_received < args.msg_count {
                    match consumer_queue.get_consume_slot(timeout) {
                        Ok(consume_slot) => {
                            // Optional validation
                            if args.msg_check_len > 0 {
                                for i in 0..args.msg_check_len as usize {
                                    let expected = PATTERN.as_bytes()
                                        [(messages_received as usize + i) % PATTERN.len()];
                                    let got = consume_slot.data[i];
                                    if expected != got {
                                        panic!(
                                            "YCFutexQueue mismatch at message {}, byte {}: expected '{}' got '{}'",
                                            messages_received, i, expected, got
                                        );
                                    }
                                }
                            }

                            if args.verbose {
                                let s = str_from_u8(consume_slot.data);
                                println!("YCFutexQueue recv: {s}");
                            }

                            consumer_queue.mark_slot_consumed(consume_slot).unwrap();
                            messages_received += 1;
                        }
                        Err(YCQueueError::Timeout) => {
                            thread::yield_now();
                        }
                        Err(e) => panic!("YCFutexQueue consumer error: {e:?}"),
                    }
                }
                *end_time.lock().unwrap() = Some(Instant::now());
            });
        }

        {
            let start_time = Arc::clone(&start_time);
            let barrier = Arc::clone(&barrier);
            s.spawn(move || {
                // Wait for both threads to be ready
                barrier.wait();

                // Producer thread sets the start time after barrier
                *start_time.lock().unwrap() = Some(Instant::now());

                let mut messages_sent = 0u32;
                while messages_sent < args.msg_count {
                    if producer_queue.queue.in_flight_count() >= args.in_flight_count {
                        thread::yield_now();
                        continue;
                    }

                    match producer_queue.get_produce_slot(timeout) {
                        Ok(produce_slot) => {
                            if args.msg_check_len > 0 {
                                for i in 0..args.msg_check_len as usize {
                                    let b = PATTERN.as_bytes()
                                        [(messages_sent as usize + i) % PATTERN.len()];
                                    produce_slot.data[i] = b;
                                }
                            } else {
                                copy_str_to_slice(default_message, produce_slot.data);
                            }

                            if args.verbose {
                                println!("YCFutexQueue send: {}", str_from_u8(produce_slot.data));
                            }

                            producer_queue.mark_slot_produced(produce_slot).unwrap();
                            messages_sent += 1;
                        }
                        Err(YCQueueError::Timeout) => {
                            thread::yield_now();
                        }
                        Err(e) => panic!("YCFutexQueue producer error: {e:?}"),
                    }
                }
            });
        }
    });

    let start = start_time.lock().unwrap().unwrap();
    let end = end_time.lock().unwrap().unwrap();
    end.duration_since(start)
}

fn run_mutex_vecdeque(args: &Args, slot_size: u16, default_message: &str) -> Duration {
    let queue: Arc<Mutex<VecDeque<Vec<u8>>>> = Arc::new(Mutex::new(VecDeque::with_capacity(
        args.queue_depth as usize,
    )));

    // Shared start/end times guarded by a Mutex
    let start_time: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
    let end_time: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));

    // Create barrier to synchronize both threads before starting benchmark
    let barrier = Arc::new(Barrier::new(2));

    thread::scope(|s| {
        // Consumer
        {
            let queue = Arc::clone(&queue);
            let args = args.clone();
            let end_time = Arc::clone(&end_time);
            let barrier = Arc::clone(&barrier);
            s.spawn(move || {
                // Wait for both threads to be ready
                barrier.wait();

                let mut messages_received = 0u32;
                while messages_received < args.msg_count {
                    let maybe_msg = {
                        let mut q = queue.lock().unwrap();
                        q.pop_front()
                    };

                    if let Some(buf) = maybe_msg {
                        if args.msg_check_len > 0 {
                            for (i, got) in buf.iter().enumerate().take(args.msg_check_len as usize) {
                                let expected = PATTERN.as_bytes()
                                    [(messages_received as usize + i) % PATTERN.len()];
                                if expected != *got {
                                    panic!(
                                        "Mutex+VecDeque mismatch at message {}, byte {}: expected '{}' got '{}'",
                                        messages_received, i, expected, got
                                    );
                                }
                            }
                        }

                        if args.verbose {
                            // Interpret up to first zero
                            let s = str_from_u8(&buf);
                            println!("Mutex+VecDeque recv: {s}");
                        }
                        messages_received += 1;
                    }
                }
                *end_time.lock().unwrap() = Some(Instant::now());
            });
        }

        // Producer
        {
            let queue = Arc::clone(&queue);
            let args = args.clone();
            let start_time = Arc::clone(&start_time);
            let barrier = Arc::clone(&barrier);
            s.spawn(move || {
                // Wait for both threads to be ready
                barrier.wait();

                // Producer thread sets the start time after barrier
                *start_time.lock().unwrap() = Some(Instant::now());

                let mut messages_sent = 0u32;
                while messages_sent < args.msg_count {
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
                        for (i, dst) in buf.iter_mut().enumerate().take(args.msg_check_len as usize)
                        {
                            let b =
                                PATTERN.as_bytes()[(messages_sent as usize + i) % PATTERN.len()];
                            *dst = b;
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
                            messages_sent += 1;
                        }
                    }
                }
            });
        }
    });

    let start = start_time.lock().unwrap().unwrap();
    let end = end_time.lock().unwrap().unwrap();
    end.duration_since(start)
}

fn run_flume(args: &Args, slot_size: u16, default_message: &str) -> Duration {
    let (sender, receiver) = flume::bounded::<Vec<u8>>(args.queue_depth as usize);

    let start_time: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
    let end_time: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));

    // Create barrier to synchronize both threads before starting benchmark
    let barrier = Arc::new(Barrier::new(2));

    thread::scope(|s| {
        {
            let receiver = receiver.clone();
            let args = args.clone();
            let end_time = Arc::clone(&end_time);
            let barrier = Arc::clone(&barrier);
            s.spawn(move || {
                // Wait for both threads to be ready
                barrier.wait();

                let mut messages_received = 0u32;
                while messages_received < args.msg_count {
                    let buf = receiver.recv().expect("Flume channel closed");

                    if args.msg_check_len > 0 {
                        for (i, got) in buf.iter().enumerate().take(args.msg_check_len as usize) {
                            let expected = PATTERN.as_bytes()
                                [(messages_received as usize + i) % PATTERN.len()];
                            if expected != *got {
                                panic!(
                                    "Flume mismatch at message {}, byte {}: expected '{}' got '{}'",
                                    messages_received, i, expected, got
                                );
                            }
                        }
                    }

                    if args.verbose {
                        let s = str_from_u8(&buf);
                        println!("Flume recv: {s}");
                    }

                    messages_received += 1;
                }
                *end_time.lock().unwrap() = Some(Instant::now());
            });
        }

        {
            let sender = sender.clone();
            let args = args.clone();
            let start_time = Arc::clone(&start_time);
            let barrier = Arc::clone(&barrier);
            s.spawn(move || {
                // Wait for both threads to be ready
                barrier.wait();

                // Producer thread sets the start time after barrier
                *start_time.lock().unwrap() = Some(Instant::now());

                let mut messages_sent = 0u32;
                while messages_sent < args.msg_count {
                    if sender.len() >= args.in_flight_count as usize {
                        thread::yield_now();
                        continue;
                    }

                    let mut buf = vec![0u8; slot_size as usize];
                    if args.msg_check_len > 0 {
                        for (i, dst) in buf.iter_mut().enumerate().take(args.msg_check_len as usize)
                        {
                            let b =
                                PATTERN.as_bytes()[(messages_sent as usize + i) % PATTERN.len()];
                            *dst = b;
                        }
                    } else {
                        copy_str_to_slice(default_message, &mut buf);
                    }

                    if args.verbose {
                        println!("Flume send: {}", str_from_u8(&buf));
                    }

                    sender.send(buf).expect("Flume channel closed");
                    messages_sent += 1;
                }
            });
        }
    });

    let start = start_time.lock().unwrap().unwrap();
    let end = end_time.lock().unwrap().unwrap();
    end.duration_since(start)
}

struct QueueResult {
    name: &'static str,
    duration: Duration,
    throughput: f64,
}

impl QueueResult {
    fn new(name: &'static str, duration: Duration, msg_count: u32) -> Self {
        let throughput = (msg_count as f64) / duration.as_secs_f64();
        Self {
            name,
            duration,
            throughput,
        }
    }
}

fn align_decimal_strings(values: &[f64], precision: usize) -> Vec<String> {
    if values.is_empty() {
        return Vec::new();
    }

    let formatted: Vec<String> = values
        .iter()
        .map(|value| format!("{:.*}", precision, value))
        .collect();

    let target_decimal = formatted
        .iter()
        .map(|s| s.find('.').unwrap_or(s.len()))
        .max()
        .unwrap_or(0);

    formatted
        .into_iter()
        .map(|s| {
            let decimal_pos = s.find('.').unwrap_or(s.len());
            let padding = target_decimal.saturating_sub(decimal_pos);
            format!("{}{}", " ".repeat(padding), s)
        })
        .collect()
}

fn choose_common_prefix(values: &[f64]) -> (f64, &'static str) {
    const PREFIXES: &[(f64, &str)] = &[(1e12, "T"), (1e9, "G"), (1e6, "M"), (1e3, "K"), (1.0, "")];

    if values.is_empty() {
        return (1.0, "");
    }

    let mut counts = [0usize; PREFIXES.len()];
    for &value in values {
        let mut matched_index = None;
        for (idx, (threshold, _)) in PREFIXES.iter().enumerate() {
            if value >= *threshold {
                matched_index = Some(idx);
                break;
            }
        }
        let idx = matched_index.unwrap_or(PREFIXES.len() - 1);
        counts[idx] += 1;
    }

    let mut best_idx = PREFIXES.len() - 1;
    let mut best_count = 0usize;
    for (idx, &count) in counts.iter().enumerate() {
        if count > best_count || (count == best_count && idx < best_idx) {
            best_idx = idx;
            best_count = count;
        }
    }

    PREFIXES[best_idx]
}

fn main() {
    let default_message = "hello there";
    let args = Args::parse();

    let msg_len = std::cmp::max(args.msg_check_len, default_message.len() as u16);
    let slot_size = align_to_cache_line(msg_len);

    println!("Comparison run with:");
    println!("  Queue depth: {}", args.queue_depth);
    println!("  Max in-flight messages: {}", args.in_flight_count);
    println!("  Message length: {msg_len}");
    println!("  Slot size (aligned): {slot_size} bytes");
    println!("  Total messages: {}", args.msg_count);

    if args.in_flight_count == 0 || args.queue_depth == 0 {
        panic!("in_flight_count and queue_depth must be greater than zero");
    }
    if args.in_flight_count > args.queue_depth {
        panic!("in_flight_count cannot be larger than queue_depth");
    }

    let yc_dur = run_ycqueue(&args, slot_size, default_message);
    #[cfg(feature = "futex")]
    let ycf_dur = run_ycfutexqueue(&args, slot_size, default_message);
    let flume_dur = run_flume(&args, slot_size, default_message);
    let mv_dur = run_mutex_vecdeque(&args, slot_size, default_message);

    let mut results = vec![
        QueueResult::new("YCQueue", yc_dur, args.msg_count),
        QueueResult::new("Flume (bounded)", flume_dur, args.msg_count),
        QueueResult::new("Mutex+VecDeque", mv_dur, args.msg_count),
    ];
    #[cfg(feature = "futex")]
    results.push(QueueResult::new("YCFutexQueue", ycf_dur, args.msg_count));

    let label_width = results
        .iter()
        .map(|res| res.name.len() + 1)
        .max()
        .unwrap_or(0);

    let mut latency_sorted: Vec<&QueueResult> = results.iter().collect();
    latency_sorted.sort_by(|a, b| a.duration.cmp(&b.duration));
    let latency_values: Vec<f64> = latency_sorted
        .iter()
        .map(|res| res.duration.as_secs_f64() * 1_000_000.0)
        .collect();
    let latency_strings = align_decimal_strings(&latency_values, 3);

    println!("\nResults (lower is better):");
    for (res, value_str) in latency_sorted.iter().zip(latency_strings.iter()) {
        println!(
            "  {:<width$} {} us",
            format!("{}:", res.name),
            value_str,
            width = label_width
        );
    }

    let mut throughput_sorted: Vec<&QueueResult> = results.iter().collect();
    throughput_sorted.sort_by(|a, b| {
        b.throughput
            .partial_cmp(&a.throughput)
            .expect("NaN throughput encountered")
    });
    let throughput_values: Vec<f64> = throughput_sorted.iter().map(|res| res.throughput).collect();
    let (prefix_factor, prefix_label) = choose_common_prefix(&throughput_values);
    let scaled_throughput: Vec<f64> = throughput_values
        .iter()
        .map(|value| value / prefix_factor)
        .collect();
    let throughput_strings = align_decimal_strings(&scaled_throughput, 2);
    let throughput_unit = if prefix_label.is_empty() {
        "msgs/s".to_string()
    } else {
        format!("{prefix_label}msgs/s")
    };

    println!("\nThroughput:");
    for (res, value_str) in throughput_sorted.iter().zip(throughput_strings.iter()) {
        println!(
            "  {:<width$} {} {}",
            format!("{}:", res.name),
            value_str,
            throughput_unit,
            width = label_width
        );
    }
}
