#[cfg(test)]
mod tests {
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
}
