#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};

    use yep_coc::queue_alloc_helpers::{YCQueueOwnedData, YCQueueSharedData};
    use yep_coc::{YCQueue, YCQueueError};

    pub fn str_to_u8(s: &str) -> &[u8] {
        return s.as_bytes();
    }

    pub fn str_from_u8(buf: &[u8]) -> &str {
        let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
        match std::str::from_utf8(&buf[..len]) {
            Ok(s) => s,
            Err(e) => panic!(
                "couldn't parse as utf-8 string, err: {:?} buf: {:?}",
                e, buf
            ),
        }
    }

    pub fn copy_str_to_slice(s: &str, buf: &mut [u8]) {
        let s_bytes = str_to_u8(s);
        assert!(s_bytes.len() <= buf.len(), "dst buffer not large enough!");

        buf[..s_bytes.len()].copy_from_slice(s_bytes);
    }

    #[test]
    /**
     * Simple test that produces and consumes two items from the queue with the pattern
     * produce -> consume -> produce -> consume, checking queue state and data contents along the way.
     */
    fn simple_produce_consume_test() {
        let slot_count: u16 = 64;
        let slot_size: u16 = 64;
        let num_iterations: u16 = 10;

        // this is the "original" data - it owned the underlying allocations
        let owned_data = YCQueueOwnedData::new(slot_count, slot_size);

        // we have to make a shared version to use in the other thread
        let consume_data = YCQueueSharedData::from_owned_data(&owned_data);
        let produce_data = YCQueueSharedData::from_owned_data(&owned_data);

        // set up consumed thread to poll & consume data
        let mut consume_queue = YCQueue::new(consume_data.meta, consume_data.data).unwrap();
        let mut produce_queue = YCQueue::new(produce_data.meta, produce_data.data).unwrap();

        std::thread::scope(|s| {
            // consumer thread
            s.spawn(move || {
                let mut counter = 0;

                while counter < slot_count * num_iterations {
                    match consume_queue.get_consume_slot() {
                        Ok(consume_slot) => {
                            let expected_str = format!("hello-{}", counter);
                            let actual_str = str_from_u8(consume_slot.data);
                            assert_eq!(
                                expected_str, actual_str,
                                "consumed data does not match expected"
                            );
                            consume_queue.mark_slot_consumed(consume_slot).unwrap();
                            counter += 1;
                        }
                        Err(YCQueueError::EmptyQueue) | Err(YCQueueError::SlotNotReady) => {
                            // no data yet, just spin
                            std::thread::yield_now();
                        }
                        Err(e) => {
                            panic!("unexpected error when consuming: {:?}", e);
                        }
                    }
                }
            });

            // producer thread
            s.spawn(move || {
                let mut counter = 0;

                while counter < slot_count * num_iterations {
                    match produce_queue.get_produce_slot() {
                        Ok(mut produce_slot) => {
                            let produce_str = format!("hello-{}", counter);
                            copy_str_to_slice(&produce_str, &mut produce_slot.data);
                            produce_queue.mark_slot_produced(produce_slot).unwrap();
                            counter += 1;
                        }
                        Err(YCQueueError::OutOfSpace) | Err(YCQueueError::SlotNotReady) => {
                            // queue is full, just spin
                            std::thread::yield_now();
                        }
                        Err(e) => {
                            panic!("unexpected error when producing: {:?}", e);
                        }
                    }
                }
            });
        });
    }

    #[test]
    /**
     * Test with multiple producers and multiple consumers.
     */
    fn multi_produce_consume_test() {
        let slot_count: u16 = 64;
        let slot_size: u16 = 64;
        let num_iterations: u16 = 20;
        let num_producers: u16 = 4;
        let num_consumers: u16 = 4;

        // this is the "original" data - it owned the underlying allocations
        let owned_data = YCQueueOwnedData::new(slot_count, slot_size);

        // create consumer queues
        let mut consumer_queues = Vec::with_capacity(num_consumers as usize);
        for _ in 0..num_consumers {
            let data = YCQueueSharedData::from_owned_data(&owned_data);
            consumer_queues.push(YCQueue::new(data.meta, data.data).unwrap());
        }

        // create producer queues
        let mut producer_queues = Vec::with_capacity(num_producers as usize);
        for _ in 0..num_producers {
            let data = YCQueueSharedData::from_owned_data(&owned_data);
            producer_queues.push(YCQueue::new(data.meta, data.data).unwrap());
        }

        // atomic counter for sending message id
        let produce_counter = Arc::new(AtomicU32::new(0));
        let max_messages = (slot_count as u32) * (num_iterations as u32);

        // keep track of recieved message
        let recieved_messages = Arc::new(Mutex::new(HashSet::<String>::new()));
        let sent_messages = Arc::new(Mutex::new(HashSet::<String>::new()));

        std::thread::scope(|s| {
            // start consumers
            for _ in 0..num_consumers {
                let mut consume_queue = consumer_queues.pop().unwrap();
                let recieved_messages = Arc::clone(&recieved_messages);
                s.spawn(move || {
                    while recieved_messages.lock().unwrap().len() < max_messages as usize{
                        match consume_queue.get_consume_slot() {
                            Ok(consume_slot) => {
                                let message = str_from_u8(consume_slot.data);
                                // we should only ever insert unique values
                                assert!(recieved_messages.lock().unwrap().insert(message.to_string()), "duplicate message received: {}", message);
                                consume_queue.mark_slot_consumed(consume_slot).unwrap();
                            }
                            Err(YCQueueError::EmptyQueue) | Err(YCQueueError::SlotNotReady) => {
                                // no data yet, just spin
                                std::thread::yield_now();
                            }
                            Err(e) => {
                                panic!("unexpected error when consuming: {:?}", e);
                            }
                        }
                    }
                });
            }

            // start producers
            for _ in 0..num_producers {
                let sent_messages = Arc::clone(&sent_messages);
                let mut produce_queue = producer_queues.pop().unwrap();
                let counter = Arc::clone(&produce_counter);
                s.spawn(move || {
                    let mut id = counter.fetch_add(1, Ordering::AcqRel);
                    while id < max_messages {
                        match produce_queue.get_produce_slot() {
                            Ok(mut produce_slot) => {
                                let produce_str = format!("hello-{}", id);
                                copy_str_to_slice(&produce_str, &mut produce_slot.data);
                                produce_queue.mark_slot_produced(produce_slot).unwrap();

                                assert!(sent_messages.lock().unwrap().insert(produce_str.to_string()), "duplicate message sent: {}", produce_str);
                                id = counter.fetch_add(1, Ordering::AcqRel);
                            }
                            Err(YCQueueError::OutOfSpace) | Err(YCQueueError::SlotNotReady) => {
                                // queue is full, just spin
                                std::thread::yield_now();
                            }
                            Err(e) => {
                                panic!("unexpected error when producing: {:?}", e);
                            }
                        }
                    }
                });
            }
        });

        // produce counter should be at least max_messages, as we may have had multiple producers fetch the counter at the same time
        assert!(produce_counter.load(Ordering::Acquire) >= max_messages);

        // the sent messages map should have the exact same size
        assert_eq!(sent_messages.lock().unwrap().len(), max_messages as usize);

        // make sure we sent all the messages
        for i in 0..max_messages {
            let expected_str = format!("hello-{}", i);
            assert!(sent_messages.lock().unwrap().contains(&expected_str), "missing message: {}", expected_str);
        }

        // make sure we got all the messages
        let recieved_messages = recieved_messages.lock().unwrap();
        assert_eq!(recieved_messages.len(), max_messages as usize);

        for i in 0..max_messages {
            let expected_str = format!("hello-{}", i);
            assert!(recieved_messages.contains(&expected_str), "missing message: {}", expected_str);
        }
    }
}
