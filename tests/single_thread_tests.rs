#[cfg(test)]
mod tests {
    use yep_coc::{YCQueue, YCQueueError, YCQueueProduceSlot, YCQueueSharedMeta};
    use yep_coc::queue_alloc_helpers::YCQueueOwnedData;
    

    pub fn str_to_u8(s: &str) -> &[u8] {
        return s.as_bytes()
    }

    pub fn str_from_u8(buf: &[u8]) -> &str {
        let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
        match std::str::from_utf8(&buf[..len]) {
            Ok(s) => s,
            Err(e) => panic!("couldn't parse as utf-8 string, err: {:?} buf: {:?}", e, buf),
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
        let slot_count: u16 = 8;
        let slot_size: u16 = 256;

        /*
         * Set up the "shared metadata" for the queue
         */
        let mut owned_data = YCQueueOwnedData::new(slot_count, slot_size);
        let shared_meta = YCQueueSharedMeta::new(&owned_data.meta);

        // set up the queue
        let mut queue = YCQueue::new(shared_meta, owned_data.data.as_mut_slice()).unwrap();

        // consume on empty queue should fail
        assert_eq!(queue.get_consume_slot().unwrap_err(), YCQueueError::EmptyQueue);

        // get the first queue slot
        let queue_slot_0 = queue.get_produce_slot().unwrap();

        // check queue fields
        assert_eq!(queue_slot_0.index, 0);
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.produce_idx(), 1);

        // write the first message
        let first_test_msg = "hello there";
        copy_str_to_slice(first_test_msg, queue_slot_0.data);

        // get the second queue slot
        let queue_slot_1 = queue.get_produce_slot().unwrap();

        // check queue fields
        assert_eq!(queue_slot_1.index, 1);
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.produce_idx(), 2);

        // write the second message
        let second_test_msg = "general kenobi";
        copy_str_to_slice(second_test_msg, queue_slot_1.data);

        // attempt to consume from empty queue, which should fail
        assert_eq!(queue.get_consume_slot().unwrap_err(), YCQueueError::EmptyQueue);
        assert_eq!(queue.consume_idx(), 0);

        // produce into the first queue slot
        queue.mark_slot_produced(queue_slot_0).unwrap();
        assert_eq!(queue.in_flight_count(), 1);
        assert_eq!(queue.produce_idx(), 2);
        
        // make sure consume idx didn't change somehow
        assert_eq!(queue.consume_idx(), 0);

        let consume_slot_0 = queue.get_consume_slot().unwrap();

        // check queue fields
        assert_eq!(consume_slot_0.index, 0);
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.consume_idx(), 1);

        // check data
        assert_eq!(consume_slot_0.data.len(), slot_size as usize);
        assert_eq!(str_from_u8(consume_slot_0.data), first_test_msg);

        // attempt to consume from empty queue, which should fail
        assert_eq!(queue.get_consume_slot().unwrap_err(), YCQueueError::EmptyQueue);
        assert_eq!(queue.consume_idx(), 1);

        // produce second data item
        // produce into the first queue slot
        queue.mark_slot_produced(queue_slot_1).unwrap();
        assert_eq!(queue.in_flight_count(), 1);
        assert_eq!(queue.produce_idx(), 2);

        let consume_slot_1 = queue.get_consume_slot().unwrap();

        // check queue fields
        assert_eq!(consume_slot_1.index, 1);
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.consume_idx(), 2);

        // check data
        assert_eq!(consume_slot_1.data.len(), slot_size as usize);
        assert_eq!(str_from_u8(consume_slot_1.data), second_test_msg);

        // mark slots as consumed
        queue.mark_slot_consumed(consume_slot_0).unwrap();
        queue.mark_slot_consumed(consume_slot_1).unwrap();

        // check fields
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.produce_idx(), 2);
        assert_eq!(queue.consume_idx(), 2);

    }

    #[test]
    fn capacity_tests() {
        let slot_count: u16 = 8;
        let slot_size: u16 = 256;

        /*
         * Set up the "shared metadata" for the queue
         */
        let mut owned_data = YCQueueOwnedData::new(slot_count, slot_size);
        let shared_meta = YCQueueSharedMeta::new(&owned_data.meta);

        // set up the queue
        let mut queue = match YCQueue::new(shared_meta, owned_data.data.as_mut_slice()) {
            Ok(q) => q,
            Err(e) => panic!("Failed to create queue: err {:?}", e),
        };

        // reserve the entire queue for produce
        let mut slots = Vec::<YCQueueProduceSlot>::new();

        for _ in 0..slot_count {
            slots.push(queue.get_produce_slot().unwrap());
        }

        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.produce_idx(), 0);
        assert_eq!(queue.consume_idx(), 0);

        // queue is entirely reserved, shouldn't be able to reserve another produce slot
        assert_eq!(queue.get_produce_slot().unwrap_err(), YCQueueError::OutOfSpace);

        // produce entire queue
        for slot in slots.into_iter() {
            queue.mark_slot_produced(slot).unwrap();
        }

        // check queue stats
        assert_eq!(queue.in_flight_count(), slot_count);
        assert_eq!(queue.produce_idx(), 0);

        // queue is still reserved, shouldn't be able to reserve another produce slot
        assert_eq!(queue.get_produce_slot().unwrap_err(), YCQueueError::OutOfSpace);

        // consume a SINGLE element
        let consume_slot = queue.get_consume_slot().unwrap();
        assert_eq!(consume_slot.index, 0);
        assert_eq!(queue.consume_idx(), 1);

        queue.mark_slot_consumed(consume_slot).unwrap();
        assert_eq!(queue.consume_idx(), 1);

        // make sure we can produce exactly one more element
        let produce_slot = queue.get_produce_slot().unwrap();
        assert_eq!(produce_slot.index, 0);
        assert_eq!(queue.produce_idx(), 1);
        assert_eq!(queue.consume_idx(), 1);

        assert_eq!(queue.get_produce_slot().unwrap_err(), YCQueueError::OutOfSpace);

        queue.mark_slot_produced(produce_slot).unwrap();

        // consume the entire queue
        for _ in 0..slot_count {
            let consume_slot = queue.get_consume_slot().unwrap();
            queue.mark_slot_consumed(consume_slot).unwrap();
        }

    }

    #[test]
    fn out_of_order_produce_test() {
        let slot_count: u16 = 8;
        let slot_size: u16 = 256;

        /*
         * Set up the "shared metadata" for the queue
         */
        let mut owned_data = YCQueueOwnedData::new(slot_count, slot_size);
        let shared_meta = YCQueueSharedMeta::new(&owned_data.meta);

        // set up the queue
        let mut queue = YCQueue::new(shared_meta, owned_data.data.as_mut_slice()).unwrap();

        // get the queue slots
        let queue_slot_0 = queue.get_produce_slot().unwrap();
        let queue_slot_1 = queue.get_produce_slot().unwrap();

        assert_eq!(queue_slot_0.index, 0);
        assert_eq!(queue_slot_1.index, 1);

        // produce "second" queue slot first
        queue.mark_slot_produced(queue_slot_1).unwrap();

        // consume should fail
        assert_eq!(queue.get_consume_slot().unwrap_err(), YCQueueError::SlotNotReady);

        // until we produce the first slsot
        queue.mark_slot_produced(queue_slot_0).unwrap();

        assert_eq!(queue.get_consume_slot().unwrap().index, 0);
        assert_eq!(queue.get_consume_slot().unwrap().index, 1);
    }

    #[test]
    fn out_of_order_consume_test() {
        let slot_count: u16 = 8;
        let slot_size: u16 = 256;

        /*
         * Set up the "shared metadata" for the queue
         */
        let mut owned_data = YCQueueOwnedData::new(slot_count, slot_size);
        let shared_meta = YCQueueSharedMeta::new(&owned_data.meta);

        // set up the queue
        let mut queue = YCQueue::new(shared_meta, owned_data.data.as_mut_slice()).unwrap();

        // produce entire queue
        for _ in 0..slot_count {
            let queue_slot = queue.get_produce_slot().unwrap();
            queue.mark_slot_produced(queue_slot).unwrap();
        }
        assert_eq!(queue.produce_idx(), 0);
        assert_eq!(queue.consume_idx(), 0);
        assert_eq!(queue.in_flight_count(), slot_count);

        // get two consume slots
        let consume_slot_0 = queue.get_consume_slot().unwrap();
        let consume_slot_1= queue.get_consume_slot().unwrap();

        assert_eq!(consume_slot_0.index, 0);
        assert_eq!(consume_slot_1.index, 1);

        // mark second slot consumed
        queue.mark_slot_consumed(consume_slot_1).unwrap();

        // still can't get produce slot
        assert_eq!(queue.get_produce_slot().unwrap_err(), YCQueueError::SlotNotReady);

        // until we mark first slot consusmed
        queue.mark_slot_consumed(consume_slot_0).unwrap();

        // then both slots can be gotten again
        assert_eq!(queue.get_produce_slot().unwrap().index, 0);
        assert_eq!(queue.get_produce_slot().unwrap().index, 1);
    }

    #[test]
    fn multiple_queue_iterations() {
        let slot_count: u16 = 8;
        let slot_size: u16 = 64;
        const ITERATIONS: usize = 4; // Number of times to loop around the queue

        // Set up the queue
        let mut owned_data = YCQueueOwnedData::new(slot_count, slot_size);
        let shared_meta = YCQueueSharedMeta::new(&owned_data.meta);
        let mut queue = YCQueue::new(shared_meta, owned_data.data.as_mut_slice()).unwrap();

        let mut produce_slots = Vec::new();
        let mut all_messages = Vec::new();

        // Produce messages in batches to fill the queue multiple times
        for iter in 0..ITERATIONS {
            for slot_idx in 0..slot_count {
                let msg = format!("iter_{}_msg_{}", iter, slot_idx);
                all_messages.push(msg.clone());

                let slot = queue.get_produce_slot().unwrap();
                copy_str_to_slice(&msg, slot.data);
                produce_slots.push(slot);
            }

            // Mark all slots in this batch as produced
            for _ in 0..slot_count {
                let slot = produce_slots.remove(0);
                queue.mark_slot_produced(slot).unwrap();
            }

            // Consume all messages in this batch
            let mut consumed_messages = Vec::new();
            for _ in 0..slot_count {
                let slot = queue.get_consume_slot().unwrap();
                let msg = str_from_u8(slot.data).to_string();
                consumed_messages.push(msg);
                queue.mark_slot_consumed(slot).unwrap();
            }

            // Sort both lists since order doesn't matter within a batch
            let mut expected: Vec<_> = all_messages[iter*slot_count as usize..(iter+1)*slot_count as usize].to_vec();
            expected.sort();
            consumed_messages.sort();
            assert_eq!(consumed_messages, expected, "Messages in iteration {} don't match", iter);
        }

        // Verify queue is empty
        assert_eq!(queue.get_consume_slot().unwrap_err(), YCQueueError::EmptyQueue);
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.produce_idx(), queue.consume_idx());
    }
}
