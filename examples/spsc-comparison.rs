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

#[cfg(feature = "blocking")]
use yep_coc::{
    YCBlockingQueue,
    queue_alloc_helpers::{YCBlockingQueueOwnedData, YCBlockingQueueSharedData},
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
                    match consumer_queue.get_consume_slot(timeout, Duration::ZERO) {
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

                    match producer_queue.get_produce_slot(timeout, Duration::ZERO) {
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

#[cfg(feature = "blocking")]
fn run_ycblockingqueue(args: &Args, slot_size: u16, default_message: &str) -> Duration {
    let owned_data = YCBlockingQueueOwnedData::new(args.queue_depth, slot_size);
    let consumer_data = YCBlockingQueueSharedData::from_owned_data(&owned_data);
    let producer_data = YCBlockingQueueSharedData::from_owned_data(&owned_data);

    let mut consumer_queue = YCBlockingQueue::new(
        YCQueue::new(consumer_data.data.meta, consumer_data.data.data).unwrap(),
        consumer_data.count,
        consumer_data.condvar,
    );
    let mut producer_queue = YCBlockingQueue::new(
        YCQueue::new(producer_data.data.meta, producer_data.data.data).unwrap(),
        producer_data.count,
        producer_data.condvar,
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
                                            "YCBlockingQueue mismatch at message {}, byte {}: expected '{}' got '{}'",
                                            messages_received, i, expected, got
                                        );
                                    }
                                }
                            }

                            if args.verbose {
                                let s = str_from_u8(consume_slot.data);
                                println!("YCBlockingQueue recv: {s}");
                            }

                            consumer_queue.mark_slot_consumed(consume_slot).unwrap();
                            messages_received += 1;
                        }
                        Err(YCQueueError::Timeout) => {
                            thread::yield_now();
                        }
                        Err(e) => panic!("YCBlockingQueue consumer error: {e:?}"),
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
                                println!(
                                    "YCBlockingQueue send: {}",
                                    str_from_u8(produce_slot.data)
                                );
                            }

                            producer_queue.mark_slot_produced(produce_slot).unwrap();
                            messages_sent += 1;
                        }
                        Err(YCQueueError::Timeout) => {
                            thread::yield_now();
                        }
                        Err(e) => panic!("YCBlockingQueue producer error: {e:?}"),
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
    #[cfg(feature = "blocking")]
    let ycb_dur = run_ycblockingqueue(&args, slot_size, default_message);
    let flume_dur = run_flume(&args, slot_size, default_message);
    let mv_dur = run_mutex_vecdeque(&args, slot_size, default_message);

    println!("\nResults (lower is better):");
    println!(
        "  YCQueue:          {:.3} us",
        yc_dur.as_nanos() as f64 / 1_000.0
    );
    #[cfg(feature = "futex")]
    println!(
        "  YCFutexQueue:     {:.3} us",
        ycf_dur.as_nanos() as f64 / 1_000.0
    );
    #[cfg(feature = "blocking")]
    println!(
        "  YCBlockingQueue:  {:.3} us",
        ycb_dur.as_nanos() as f64 / 1_000.0
    );
    println!(
        "  Flume (bounded):  {:.3} us",
        flume_dur.as_nanos() as f64 / 1_000.0
    );
    println!(
        "  Mutex+VecDeque:   {:.3} us",
        mv_dur.as_nanos() as f64 / 1_000.0
    );

    let yc_msgs_per_sec = (args.msg_count as f64) / yc_dur.as_secs_f64();
    #[cfg(feature = "futex")]
    let ycf_msgs_per_sec = (args.msg_count as f64) / ycf_dur.as_secs_f64();
    #[cfg(feature = "blocking")]
    let ycb_msgs_per_sec = (args.msg_count as f64) / ycb_dur.as_secs_f64();
    let flume_msgs_per_sec = (args.msg_count as f64) / flume_dur.as_secs_f64();
    let mv_msgs_per_sec = (args.msg_count as f64) / mv_dur.as_secs_f64();
    println!("\nThroughput:");
    println!("  YCQueue:          {:.2} msgs/s", yc_msgs_per_sec);
    #[cfg(feature = "futex")]
    println!("  YCFutexQueue:     {:.2} msgs/s", ycf_msgs_per_sec);
    #[cfg(feature = "blocking")]
    println!("  YCBlockingQueue:  {:.2} msgs/s", ycb_msgs_per_sec);
    println!("  Flume (bounded):  {:.2} msgs/s", flume_msgs_per_sec);
    println!("  Mutex+VecDeque:   {:.2} msgs/s", mv_msgs_per_sec);
}
