mod utils;

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};

    use yep_coc::queue_alloc_helpers::{YCQueueOwnedData, YCQueueSharedData};
    use yep_coc::{YCQueue, YCQueueError};

    use crate::utils::{copy_str_to_slice, str_from_u8};

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
        let num_iterations: u16 = 100;
        let num_producers: u16 = 4;
        let num_consumers: u16 = 4;
        let timeout = std::time::Duration::from_secs(10);

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

        // keep track of received message
        let received_ids = Arc::new(Mutex::new(HashSet::<u32>::new()));
        let sent_ids = Arc::new(Mutex::new(HashSet::<u32>::new()));

        let deadline = std::time::Instant::now() + timeout;
        std::thread::scope(|s| {
            // start consumers
            for i in 0..num_consumers {
                let builder = std::thread::Builder::new().name(format!("consumer_{}", i));
                let mut consume_queue = consumer_queues.pop().unwrap();
                let received_ids = Arc::clone(&received_ids);
                builder.spawn_scoped(s, move || {
                    while received_ids.lock().unwrap().len() < max_messages as usize{
                        match consume_queue.get_consume_slot() {
                            Ok(consume_slot) => {
                                let message = str_from_u8(consume_slot.data);
                                let id_str = message.strip_prefix("hello-").expect(format!("bad message: {}", message).as_str());
                                let id: u32 = id_str.parse().expect(format!("bad message: {}", message).as_str());
                                assert!(id < max_messages, "received id out of range: {}, message: {}, slot: {}", id, message, consume_slot.index);
                                // we should only ever insert unique values
                                assert!(received_ids.lock().unwrap().insert(id), "duplicate message received: {}", message);

                                // poison the block after consuming the message to help catch bugs
                                let poison_str = format!("poisoned-{}", consume_slot.index);
                                copy_str_to_slice(&poison_str, consume_slot.data);

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

                        if std::time::Instant::now() > deadline {
                            panic!("test timed out after {:?}", timeout);
                        }
                    }
                }).unwrap();
            }

            // start producers
            for i in 0..num_producers {
                let builder = std::thread::Builder::new().name(format!("producer_{}", i));
                let sent_ids = Arc::clone(&sent_ids);
                let mut produce_queue = producer_queues.pop().unwrap();
                let counter = Arc::clone(&produce_counter);
                builder.spawn_scoped(s, move || {
                    let mut id = counter.fetch_add(1, Ordering::AcqRel);
                    while id < max_messages {
                        match produce_queue.get_produce_slot() {
                            Ok(mut produce_slot) => {
                                let produce_str = format!("hello-{}", id);
                                copy_str_to_slice(&produce_str, &mut produce_slot.data);
                                print!("{}:{}\n", produce_slot.index, produce_str);
                                produce_queue.mark_slot_produced(produce_slot).unwrap();

                                assert!(sent_ids.lock().unwrap().insert(id), "duplicate message sent: {}", produce_str);
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

                        if std::time::Instant::now() > deadline {
                            panic!("test timed out after {:?}", timeout);
                        }
                    }
                }).unwrap();
            }
        });

        // produce counter should be at least max_messages, as we may have had multiple producers fetch the counter at the same time
        assert!(produce_counter.load(Ordering::Acquire) >= max_messages);

        // the sent messages map should have the exact same size
        assert_eq!(sent_ids.lock().unwrap().len(), max_messages as usize);

        // make sure we sent all the messages
        for i in 0..max_messages {
            assert!(sent_ids.lock().unwrap().contains(&i), "missing sent id: {}", i);
        }

        // make sure we got all the messages
        let received_ids = received_ids.lock().unwrap();
        assert_eq!(received_ids.len(), max_messages as usize);

        for i in 0..max_messages {
            assert!(received_ids.contains(&i), "missing received id: {}, ids: {:?}", i, received_ids);
        }
    }
}
