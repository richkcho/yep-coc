#[cfg(test)]
mod tests {
    use yep_coc::{YCQueue, YCQueueError, YCQueueSharedMeta};
    use yep_coc::queue_alloc_helpers::YCQueueData;
    

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
        let mut owned_data = YCQueueData::new(slot_count, slot_size);
        let shared_meta = YCQueueSharedMeta::new(&owned_data.meta);

        // set up the queue
        let mut queue = match YCQueue::new(shared_meta, owned_data.data.as_mut_slice()) {
            Ok(q) => q,
            Err(e) => panic!("Failed to create queue: err {:?}", e),
        };

        // get the first queue slot
        let queue_slot_0 = match queue.get_produce_slot() {
            Ok(s) => s,
            Err(e) => panic!("Failed to get first queue slot, err {:?}", e),
        };

        // check queue fields
        assert_eq!(queue_slot_0.index, 0);
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.produce_idx(), 1);

        // write the first message
        let first_test_msg = "hello there";
        copy_str_to_slice(first_test_msg, queue_slot_0.data);

        // get the second queue slot
        let queue_slot_1 = match queue.get_produce_slot() {
            Ok(s) => s,
            Err(e) => panic!("Failed to get second queue slot, err {:?}", e),
        };

        // check queue fields
        assert_eq!(queue_slot_1.index, 1);
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.produce_idx(), 2);

        // write the second message
        let second_test_msg = "general kenobi";
        copy_str_to_slice(second_test_msg, queue_slot_1.data);

        // attempt to consume from empty queue
        match queue.get_consume_slot() {
            Ok(_) => assert!(false),
            Err(e) => assert_eq!(e, YCQueueError::EmptyQueue),
        };
        assert_eq!(queue.consume_idx(), 0);

        // produce into the first queue slot
        queue.mark_slot_produced(queue_slot_0);
        assert_eq!(queue.in_flight_count(), 1);
        assert_eq!(queue.produce_idx(), 2);
        
        // make sure consume idx didn't change somehow
        assert_eq!(queue.consume_idx(), 0);

        let consume_slot_0 = match queue.get_consume_slot() {
            Ok(slot) => slot,
            Err(e) => panic!("Failed to get consume slot 0, err {:?}", e),
        };

        // check queue fields
        assert_eq!(consume_slot_0.index, 0);
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.consume_idx(), 1);

        // check data
        assert_eq!(consume_slot_0.data.len(), slot_size as usize);
        assert_eq!(str_from_u8(consume_slot_0.data), first_test_msg);

        // attempt to consume from empty queue
        match queue.get_consume_slot() {
            Ok(_) => assert!(false),
            Err(e) => assert_eq!(e, YCQueueError::EmptyQueue),
        };
        assert_eq!(queue.consume_idx(), 1);

        // produce second data item
        // produce into the first queue slot
        queue.mark_slot_produced(queue_slot_1);
        assert_eq!(queue.in_flight_count(), 1);
        assert_eq!(queue.produce_idx(), 2);

        let consume_slot_1 = match queue.get_consume_slot() {
            Ok(slot) => slot,
            Err(e) => panic!("Failed to get consume slot 1, err {:?}", e),
        };

        // check queue fields
        assert_eq!(consume_slot_1.index, 1);
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.consume_idx(), 2);

        // check data
        assert_eq!(consume_slot_1.data.len(), slot_size as usize);
        assert_eq!(str_from_u8(consume_slot_1.data), second_test_msg);

        // mark slots as consumed
        queue.mark_slot_consumed(consume_slot_0);
        queue.mark_slot_consumed(consume_slot_1);

        // check fields
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.produce_idx(), 2);
        assert_eq!(queue.consume_idx(), 2);

    }
}
