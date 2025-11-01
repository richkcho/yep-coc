#[cfg(feature = "futex")]
#[cfg(test)]
mod futex_queue_tests {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Mutex};
    use std::time::{Duration, Instant};

    use yep_coc::queue_alloc_helpers::{YCFutexQueueOwnedData, YCFutexQueueSharedData};
    use yep_coc::{YCFutexQueue, YCQueueError};

    use test_support::utils::{copy_str_to_slice, str_from_u8};

    const DEFAULT_SMALL_TIMEOUT: Duration = Duration::from_millis(1);
    const DEFAULT_LONG_TIMEOUT: Duration = Duration::from_secs(2);

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
        let owned_data = YCFutexQueueOwnedData::new(slot_count, slot_size);

        // we have to make a shared version to use in the other thread
        let consume_data = YCFutexQueueSharedData::from_owned_data(&owned_data);
        let produce_data = YCFutexQueueSharedData::from_owned_data(&owned_data);

        // set up consumed thread to poll & consume data
        let mut consume_queue = YCFutexQueue::from_shared_data(consume_data).unwrap();
        let mut produce_queue = YCFutexQueue::from_shared_data(produce_data).unwrap();

        let timeout = std::time::Duration::from_secs(10);
        let deadline = std::time::Instant::now() + timeout;

        std::thread::scope(|s| {
            // consumer thread
            s.spawn(move || {
                let mut counter = 0;

                while counter < slot_count * num_iterations {
                    match consume_queue.get_consume_slot(DEFAULT_SMALL_TIMEOUT) {
                        Ok(consume_slot) => {
                            let expected_str = format!("hello-{counter}");
                            let actual_str = str_from_u8(consume_slot.data);
                            assert_eq!(
                                expected_str, actual_str,
                                "consumed data does not match expected"
                            );
                            consume_queue.mark_slot_consumed(consume_slot).unwrap();
                            counter += 1;
                        }
                        Err(YCQueueError::EmptyQueue)
                        | Err(YCQueueError::SlotNotReady)
                        | Err(YCQueueError::Timeout) => {
                            // no data yet, just spin
                            std::thread::yield_now();
                        }
                        Err(e) => {
                            panic!("unexpected error when consuming: {e:?}");
                        }
                    }

                    if std::time::Instant::now() > deadline {
                        panic!("test timed out after {timeout:?}");
                    }
                }
            });

            // producer thread
            s.spawn(move || {
                let mut counter = 0;

                while counter < slot_count * num_iterations {
                    match produce_queue.get_produce_slot(DEFAULT_SMALL_TIMEOUT) {
                        Ok(produce_slot) => {
                            let produce_str = format!("hello-{counter}");
                            copy_str_to_slice(&produce_str, &mut *produce_slot.data);
                            produce_queue.mark_slot_produced(produce_slot).unwrap();
                            counter += 1;
                        }
                        Err(YCQueueError::OutOfSpace)
                        | Err(YCQueueError::SlotNotReady)
                        | Err(YCQueueError::Timeout) => {
                            // queue is full, just spin
                            std::thread::yield_now();
                        }
                        Err(e) => {
                            panic!("unexpected error when producing: {e:?}");
                        }
                    }

                    if std::time::Instant::now() > deadline {
                        panic!("test timed out after {timeout:?}");
                    }
                }
            });
        });
    }

    #[test]
    fn multi_produce_consume_test() {
        let slot_count: u16 = 64;
        let slot_size: u16 = 64;
        let num_iterations: u16 = 100;
        let num_producers: u16 = 4;
        let num_consumers: u16 = 4;
        let timeout = std::time::Duration::from_secs(10);

        // this is the "original" data - it owned the underlying allocations
        let owned_data = YCFutexQueueOwnedData::new(slot_count, slot_size);

        // create consumer queues
        let mut consumer_queues = Vec::with_capacity(num_consumers as usize);
        for _ in 0..num_consumers {
            consumer_queues.push(YCFutexQueue::from_owned_data(&owned_data).unwrap());
        }

        // create producer queues
        let mut producer_queues = Vec::with_capacity(num_producers as usize);
        for _ in 0..num_producers {
            producer_queues.push(YCFutexQueue::from_owned_data(&owned_data).unwrap());
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
                let builder = std::thread::Builder::new().name(format!("consumer_{i}"));
                let mut consume_queue = consumer_queues.pop().unwrap();
                let received_ids = Arc::clone(&received_ids);
                builder
                    .spawn_scoped(s, move || {
                        while received_ids.lock().unwrap().len() < max_messages as usize {
                            match consume_queue.get_consume_slot(DEFAULT_SMALL_TIMEOUT) {
                                Ok(consume_slot) => {
                                    let message = str_from_u8(consume_slot.data);
                                    let id_str = message
                                        .strip_prefix("hello-")
                                        .unwrap_or_else(|| panic!("bad message: {message}"));
                                    let id: u32 = id_str
                                        .parse()
                                        .unwrap_or_else(|_| panic!("bad message: {message}"));
                                    assert!(
                                        id < max_messages,
                                        "received id out of range: {id}, message: {message}, slot: {}",
                                        consume_slot.index
                                    );
                                    // we should only ever insert unique values
                                    assert!(
                                        received_ids.lock().unwrap().insert(id),
                                        "duplicate message received: {message}",
                                    );

                                    // poison the block after consuming the message to help catch bugs
                                    let poison_str = format!("poisoned-{}", consume_slot.index);
                                    copy_str_to_slice(&poison_str, &mut *consume_slot.data);

                                    consume_queue.mark_slot_consumed(consume_slot).unwrap();
                                }
                                Err(YCQueueError::EmptyQueue) | Err(YCQueueError::SlotNotReady) | Err(YCQueueError::Timeout) => {
                                    // no data yet, just spin
                                    std::thread::yield_now();
                                }
                                Err(e) => {
                                panic!("unexpected error when consuming: {e:?}");
                                }
                            }

                            if std::time::Instant::now() > deadline {
                                panic!("test timed out after {timeout:?}");
                            }
                        }
                    })
                    .unwrap();
            }

            // start producers
            for i in 0..num_producers {
                let builder = std::thread::Builder::new().name(format!("producer_{i}"));
                let sent_ids = Arc::clone(&sent_ids);
                let mut produce_queue = producer_queues.pop().unwrap();
                let counter = Arc::clone(&produce_counter);
                builder
                    .spawn_scoped(s, move || {
                        let mut id = counter.fetch_add(1, Ordering::AcqRel);
                        while id < max_messages {
                            match produce_queue
                                .get_produce_slot(DEFAULT_SMALL_TIMEOUT)
                            {
                                Ok(produce_slot) => {
                                    let produce_str = format!("hello-{id}");
                                    copy_str_to_slice(&produce_str, &mut *produce_slot.data);
                                    produce_queue.mark_slot_produced(produce_slot).unwrap();

                                    assert!(
                                        sent_ids.lock().unwrap().insert(id),
                                        "duplicate message sent: {produce_str}",
                                    );
                                    id = counter.fetch_add(1, Ordering::AcqRel);
                                }
                                Err(YCQueueError::OutOfSpace)
                                | Err(YCQueueError::SlotNotReady)
                                | Err(YCQueueError::Timeout) => {
                                    // queue is full, just spin
                                    std::thread::yield_now();
                                }
                                Err(e) => {
                                    panic!("unexpected error when producing: {e:?}");
                                }
                            }

                            if std::time::Instant::now() > deadline {
                                panic!("test timed out after {timeout:?}");
                            }
                        }
                    })
                    .unwrap();
            }
        });

        // produce counter should be at least max_messages, as we may have had multiple producers fetch the counter at the same time
        assert!(produce_counter.load(Ordering::Acquire) >= max_messages);

        // the sent messages map should have the exact same size
        assert_eq!(sent_ids.lock().unwrap().len(), max_messages as usize);

        // make sure we sent all the messages
        for i in 0..max_messages {
            assert!(
                sent_ids.lock().unwrap().contains(&i),
                "missing sent id: {i}",
            );
        }

        // make sure we got all the messages
        let received_ids = received_ids.lock().unwrap();
        assert_eq!(received_ids.len(), max_messages as usize);

        for i in 0..max_messages {
            assert!(
                received_ids.contains(&i),
                "missing received id: {i}, ids: {received_ids:?}",
            );
        }
    }

    #[test]
    fn wake_forwarded_when_first_consumer_cannot_progress() {
        let slot_count: u16 = 4;
        let slot_size: u16 = 32;
        let owned = YCFutexQueueOwnedData::new(slot_count, slot_size);

        let barrier = Arc::new(Barrier::new(3));
        let single_consumed = Arc::new(AtomicBool::new(false));
        let batch_consumed = Arc::new(AtomicBool::new(false));

        std::thread::scope(|s| {
            let mut batch_queue = YCFutexQueue::from_owned_data(&owned).unwrap();
            let barrier_clone = Arc::clone(&barrier);
            let batch_flag = Arc::clone(&batch_consumed);
            s.spawn(move || {
                barrier_clone.wait();
                let slots = batch_queue
                    .get_consume_slots(2, false, DEFAULT_LONG_TIMEOUT)
                    .expect("batch consumer should eventually read two slots");
                batch_flag.store(true, Ordering::Release);
                batch_queue.mark_slots_consumed(slots).unwrap();
            });

            let mut single_queue = YCFutexQueue::from_owned_data(&owned).unwrap();
            let barrier_clone = Arc::clone(&barrier);
            let single_flag = Arc::clone(&single_consumed);
            s.spawn(move || {
                barrier_clone.wait();
                let slot = single_queue
                    .get_consume_slot(DEFAULT_LONG_TIMEOUT)
                    .expect("single consumer should receive a slot");
                single_flag.store(true, Ordering::Release);
                single_queue.mark_slot_consumed(slot).unwrap();
            });

            let mut produce_queue = YCFutexQueue::from_owned_data(&owned).unwrap();
            let barrier_clone = Arc::clone(&barrier);
            let single_flag = Arc::clone(&single_consumed);
            let batch_flag = Arc::clone(&batch_consumed);
            s.spawn(move || {
                let mut slots = produce_queue
                    .get_produce_slots(2, false, DEFAULT_LONG_TIMEOUT)
                    .expect("reserve two slots to produce");
                // Reserve an extra slot to leave one unproduced temporarily.
                let extra = produce_queue
                    .get_produce_slot(DEFAULT_LONG_TIMEOUT)
                    .expect("reserve third slot");

                let first = slots.remove(0);
                let second = slots.remove(0);

                barrier_clone.wait();
                std::thread::sleep(Duration::from_millis(5));

                second.data.fill(0xCD);
                produce_queue.mark_slot_produced(second).unwrap();
                extra.data.fill(0xEF);
                produce_queue.mark_slot_produced(extra).unwrap();

                std::thread::sleep(Duration::from_millis(5));

                first.data.fill(0xAB);
                produce_queue.mark_slot_produced(first).unwrap();

                let start = Instant::now();
                while !single_flag.load(Ordering::Acquire) || !batch_flag.load(Ordering::Acquire) {
                    assert!(
                        start.elapsed() < DEFAULT_LONG_TIMEOUT,
                        "consumers failed to make progress after all slots produced"
                    );
                    std::thread::yield_now();
                }
            });
        });

        assert!(single_consumed.load(Ordering::Acquire));
        assert!(batch_consumed.load(Ordering::Acquire));
    }

    #[test]
    fn batch_production_wakes_multiple_consumers() {
        let slot_count: u16 = 4;
        let slot_size: u16 = 32;
        let owned = YCFutexQueueOwnedData::new(slot_count, slot_size);

        let barrier = Arc::new(Barrier::new(3));
        let completion = Arc::new(AtomicUsize::new(0));

        std::thread::scope(|s| {
            for _ in 0..2 {
                let barrier_clone = Arc::clone(&barrier);
                let completion_clone = Arc::clone(&completion);
                let mut consume_queue = YCFutexQueue::from_owned_data(&owned).unwrap();
                s.spawn(move || {
                    barrier_clone.wait();
                    let slot = consume_queue
                        .get_consume_slot(DEFAULT_LONG_TIMEOUT)
                        .expect("consumer should receive data from batch");
                    consume_queue.mark_slot_consumed(slot).unwrap();
                    completion_clone.fetch_add(1, Ordering::AcqRel);
                });
            }

            let mut produce_queue = YCFutexQueue::from_owned_data(&owned).unwrap();
            let barrier_clone = Arc::clone(&barrier);
            s.spawn(move || {
                let mut slots = produce_queue
                    .get_produce_slots(2, false, DEFAULT_LONG_TIMEOUT)
                    .expect("reserve slots for batch produce");
                for (i, slot) in slots.iter_mut().enumerate() {
                    slot.data[0] = i as u8;
                }
                barrier_clone.wait();
                std::thread::sleep(Duration::from_millis(5));
                produce_queue.mark_slots_produced(slots).unwrap();
            });
        });

        assert_eq!(completion.load(Ordering::Acquire), 2);
    }
}
