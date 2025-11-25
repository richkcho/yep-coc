use clap::Parser;
use std::{
    thread,
    time::{Duration, Instant},
};
use test_support::utils::{align_to_cache_line, copy_str_to_slice, str_from_u8};
use yep_coc::{
    YCQueue, YCQueueError, queue_alloc_helpers::YCQueueOwnedData,
    queue_alloc_helpers::YCQueueSharedData,
};

const PATTERN: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

/// A simple send-recv example using YCQueue
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

    /// Enable verbose logging
    #[arg(short = 'v', long, default_value_t = false)]
    verbose: bool,

    /// Timeout in seconds for sender/receiver loops
    #[arg(short = 't', long, default_value = "10")]
    timeout_secs: u64,
}

fn main() {
    let default_message = "hello there";
    let args = Args::parse();

    let msg_len = std::cmp::max(args.msg_check_len, default_message.len() as u16);

    // Align message length to cache line size
    let slot_size = align_to_cache_line(msg_len);

    println!("Starting simple send test with:");
    println!("  Queue depth: {}", args.queue_depth);
    println!("  Batch size: {}", args.batch_size);
    println!("  Max in-flight messages: {}", args.in_flight_count);
    println!("  Message length: {msg_len}");
    println!("  Queue slot size: {slot_size} bytes");
    println!("  Total messages: {}", args.msg_count);

    // Validate arguments
    if args.in_flight_count > args.queue_depth {
        panic!("in_flight_count cannot be larger than queue_depth");
    }

    if args.in_flight_count == 0 || args.queue_depth == 0 {
        panic!("in_flight_count and queue_depth must be greater than zero");
    }

    if args.batch_size == 0 {
        panic!("batch_size must be greater than zero");
    }

    if args.batch_size > args.queue_depth {
        panic!("batch_size cannot exceed queue_depth");
    }

    // Create the queue with shared data regions
    let owned_data = YCQueueOwnedData::new(args.queue_depth, slot_size);
    let consumer_data = YCQueueSharedData::from_owned_data(&owned_data);
    let producer_data = YCQueueSharedData::from_owned_data(&owned_data);

    // Set up consumer and producer queues
    let mut consumer_queue = YCQueue::new(consumer_data.meta, consumer_data.data).unwrap();
    let mut producer_queue = YCQueue::new(producer_data.meta, producer_data.data).unwrap();

    let timeout = Duration::from_secs(args.timeout_secs);
    let test_start = Instant::now();

    // time when producer thread starts
    let mut start_time = test_start;

    // time when consumer thread finishes
    let mut end_time = test_start;

    // Use thread scope to ensure all threads complete before program exit
    thread::scope(|s| {
        // Consumer thread
        s.spawn(|| {
            let mut messages_received = 0;
            while messages_received < args.msg_count {
                if test_start.elapsed() >= timeout {
                    panic!(
                        "Consumer timed out after {:?} while waiting for message {}",
                        timeout, messages_received
                    );
                }
                match consumer_queue.get_consume_slots(args.batch_size, true) {
                    Ok(slots) => {
                        for (offset, slot) in slots.iter().enumerate() {
                            let msg_index = messages_received + offset as u32;
                            let msg = str_from_u8(slot.data);

                            if args.verbose {
                                println!("Received: {msg}");
                            }

                            if args.msg_check_len > 0 {
                                for i in 0..args.msg_check_len as usize {
                                    let expected_char = PATTERN.as_bytes()
                                        [(msg_index as usize + i) % PATTERN.len()];
                                    let received_char = msg.as_bytes()[i];
                                    if expected_char != received_char {
                                        panic!("Message content mismatch at message {msg_index}, byte {i}:\nExpected: '{expected_char}'\nReceived: '{received_char}'");
                                    }
                                }
                            }
                        }

                        messages_received += slots.len() as u32;
                        consumer_queue.mark_slots_consumed(slots).unwrap();
                    }
                    Err(YCQueueError::EmptyQueue) | Err(YCQueueError::SlotNotReady) => {
                        thread::yield_now(); // Queue is empty, yield CPU
                    }
                    Err(e) => panic!("Consumer error: {e:?}"),
                }
            }

            end_time = Instant::now();
            println!("Consumer finished after receiving {messages_received} messages");
        });

        // Producer thread
        s.spawn(|| {
            start_time = Instant::now();
            let mut messages_sent = 0;
            while messages_sent < args.msg_count {
                if test_start.elapsed() >= timeout {
                    panic!(
                        "Producer timed out after {:?} while waiting to send message {}",
                        timeout, messages_sent
                    );
                }
                let in_flight = producer_queue.in_flight_count();
                if in_flight >= args.in_flight_count {
                    thread::yield_now(); // Too many in-flight messages, yield CPU
                    continue;
                }

                let available_in_flight = args.in_flight_count - in_flight;
                let request = args.batch_size.min(available_in_flight);

                if request == 0 {
                    thread::yield_now();
                    continue;
                }

                match producer_queue.get_produce_slots(request, true) {
                    Ok(mut slots) => {
                        for (offset, slot) in slots.iter_mut().enumerate() {
                            let msg_index = messages_sent + offset as u32;
                            if args.msg_check_len > 0 {
                                for i in 0..args.msg_check_len as usize {
                                    let b = PATTERN.as_bytes()
                                        [(msg_index as usize + i) % PATTERN.len()];
                                    slot.data[i] = b;
                                }
                            } else {
                                copy_str_to_slice(default_message, slot.data);
                            }

                            if args.verbose {
                                println!("Sent: {}", str_from_u8(slot.data));
                            }
                        }

                        messages_sent += slots.len() as u32;
                        producer_queue.mark_slots_produced(slots).unwrap();
                    }
                    Err(YCQueueError::OutOfSpace) | Err(YCQueueError::SlotNotReady) => {
                        thread::yield_now(); // Queue is full, yield CPU
                    }
                    Err(e) => panic!("Producer error: {e:?}"),
                }
            }
            println!("Producer finished after sending {messages_sent} messages");
        });
    });

    println!(
        "Simple send test finished in {:?} us!",
        (end_time - start_time).as_micros()
    );
}
