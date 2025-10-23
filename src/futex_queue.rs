use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::{Duration, Instant},
};

use wait_on_address::AtomicWait;

use crate::{YCQueue, YCQueueConsumeSlot, YCQueueError, YCQueueProduceSlot};

/// Futex-backed convenience wrapper over [`YCQueue`].
///
/// Each instance maintains a shared atomic counter that tracks how many slots
/// have been produced and are ready for consumption. Producers increment this
/// counter when publishing data, and consumers decrement it after consuming
/// slots. Waiting operations leverage [`wait_on_address`] to block efficiently
/// until the counter changes or a timeout expires.
pub struct YCFutexQueue<'a> {
    pub queue: YCQueue<'a>,
    pub count: &'a AtomicU32,
}

impl<'a> YCFutexQueue<'a> {
    /// Wrap a queue together with a shared produced-counter.
    pub fn new(queue: YCQueue<'a>, count: &'a AtomicU32) -> Self {
        YCFutexQueue { queue, count }
    }

    /// Reserve up to `num_slots` slots for production, optionally accepting a partial batch.
    ///
    /// # Arguments
    /// * `num_slots` - Maximum contiguous slots to claim.
    /// * `best_effort` - When `true`, returns the currently available contiguous span even if it
    ///   is smaller than `num_slots`.
    /// * `timeout` - Maximum time to wait for space before returning `YCQueueError::Timeout`.
    /// * `retry_interval` - Minimum delay between retries when the queue is not ready yet.
    pub fn get_produce_slots(
        &mut self,
        num_slots: u16,
        best_effort: bool,
        timeout: Duration,
        retry_interval: Duration,
    ) -> Result<Vec<YCQueueProduceSlot<'a>>, YCQueueError> {
        let start_time = Instant::now();
        loop {
            let wait_start = Instant::now();

            match timeout.checked_sub(start_time.elapsed()) {
                Some(remaining_timeout) => self
                    .count
                    .wait_timeout(self.queue.capacity() as u32, remaining_timeout),
                None => return Err(YCQueueError::Timeout),
            }

            let ret = self.queue.get_produce_slots(num_slots, best_effort);
            match ret {
                Ok(slots) => return Ok(slots),
                Err(_) => {
                    if let Some(remaining_duration) =
                        wait_start.elapsed().checked_sub(retry_interval)
                    {
                        std::thread::sleep(remaining_duration)
                    }
                }
            }
        }
    }

    pub fn get_produce_slot(
        &mut self,
        timeout: Duration,
        retry_interval: Duration,
    ) -> Result<YCQueueProduceSlot<'a>, YCQueueError> {
        let mut slots = self.get_produce_slots(1, false, timeout, retry_interval)?;

        Ok(slots
            .pop()
            .expect("get_produce_slots(1, false) returned without a slot"))
    }

    /// Reserve up to `num_slots` slots for consumption, optionally accepting a partial batch.
    ///
    /// # Arguments
    /// * `num_slots` - Maximum contiguous slots to dequeue.
    /// * `best_effort` - When `true`, returns the currently published contiguous run even if it is
    ///   smaller than `num_slots`.
    /// * `timeout` - Maximum time to wait for data before returning `YCQueueError::Timeout`.
    /// * `retry_interval` - Minimum delay between retries when no slots are currently ready.
    pub fn get_consume_slots(
        &mut self,
        num_slots: u16,
        best_effort: bool,
        timeout: Duration,
        retry_interval: Duration,
    ) -> Result<Vec<YCQueueConsumeSlot<'a>>, YCQueueError> {
        debug_assert!(self.count.load(Ordering::Relaxed) <= self.queue.capacity() as u32);
        let start_time = Instant::now();
        loop {
            let wait_start = Instant::now();

            match timeout.checked_sub(start_time.elapsed()) {
                Some(remaining_timeout) => self.count.wait_timeout(0, remaining_timeout),
                None => return Err(YCQueueError::Timeout),
            }

            let ret = self.queue.get_consume_slots(num_slots, best_effort);
            match ret {
                Ok(slots) => {
                    self.count.fetch_sub(slots.len() as u32, Ordering::AcqRel);
                    debug_assert!(
                        self.count.load(Ordering::Relaxed) <= self.queue.capacity() as u32
                    );
                    return Ok(slots);
                }
                Err(_) => {
                    if let Some(remaining_duration) =
                        wait_start.elapsed().checked_sub(retry_interval)
                    {
                        std::thread::sleep(remaining_duration)
                    }
                }
            }
        }
    }

    pub fn get_consume_slot(
        &mut self,
        timeout: Duration,
        retry_interval: Duration,
    ) -> Result<YCQueueConsumeSlot<'a>, YCQueueError> {
        let mut slots = self.get_consume_slots(1, false, timeout, retry_interval)?;

        Ok(slots
            .pop()
            .expect("get_consume_slots(1, false) returned without a slot"))
    }

    /// Mark a single slot as produced and wake any waiting consumers.
    pub fn mark_slot_produced(&mut self, slot: YCQueueProduceSlot<'a>) -> Result<(), YCQueueError> {
        self.queue.mark_slot_produced(slot)?;
        self.count.fetch_add(1, Ordering::AcqRel);
        self.count.notify_all();
        Ok(())
    }

    /// Mark multiple slots as produced and wake waiting consumers.
    pub fn mark_slots_produced(
        &mut self,
        slots: Vec<YCQueueProduceSlot<'a>>,
    ) -> Result<(), YCQueueError> {
        if slots.is_empty() {
            return Ok(());
        }

        let num_produced = slots.len() as u32;
        self.queue.mark_slots_produced(slots)?;
        self.count.fetch_add(num_produced, Ordering::AcqRel);
        self.count.notify_all();
        Ok(())
    }

    /// Mark a single slot as consumed and notify waiting producers.
    pub fn mark_slot_consumed(&mut self, slot: YCQueueConsumeSlot<'a>) -> Result<(), YCQueueError> {
        debug_assert!(self.count.load(Ordering::Relaxed) <= self.queue.capacity() as u32);
        self.queue.mark_slot_consumed(slot)
    }

    /// Mark multiple slots as consumed and notify waiting producers.
    pub fn mark_slots_consumed(
        &mut self,
        slots: Vec<YCQueueConsumeSlot<'a>>,
    ) -> Result<(), YCQueueError> {
        debug_assert!(self.count.load(Ordering::Relaxed) <= self.queue.capacity() as u32);
        self.queue.mark_slots_consumed(slots)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue_alloc_helpers::YCFutexQueueOwnedData;
    use std::sync::atomic::Ordering;

    const DEFAULT_SMALL_TIMEOUT: Duration = Duration::from_millis(1);

    #[test]
    fn single_slot_roundtrip_and_best_effort_paths() {
        let owned = YCFutexQueueOwnedData::new(4, 32);
        let mut futex_queue = YCFutexQueue::from_owned_data(&owned).unwrap();

        let mut slots = futex_queue
            .get_produce_slots(1, false, DEFAULT_SMALL_TIMEOUT, Duration::ZERO)
            .expect("reserve slot");
        let slot = slots.pop().expect("slot");
        slot.data[0] = 0x11;
        futex_queue.mark_slot_produced(slot).expect("produce");
        assert_eq!(futex_queue.count.load(Ordering::Relaxed), 1);

        let mut consume_slots = futex_queue
            .get_consume_slots(1, false, DEFAULT_SMALL_TIMEOUT, Duration::ZERO)
            .expect("consume slot");
        let slot = consume_slots.pop().expect("consume slot");
        assert_eq!(slot.data[0], 0x11);
        futex_queue.mark_slot_consumed(slot).expect("consume");
        assert_eq!(futex_queue.count.load(Ordering::Relaxed), 0);

        let mut slots = futex_queue
            .get_produce_slots(1, true, DEFAULT_SMALL_TIMEOUT, Duration::ZERO)
            .expect("best-effort reserve slot");
        let slot = slots.pop().expect("slot");
        slot.data[0] = 0x22;
        futex_queue.mark_slot_produced(slot).expect("produce");
        assert_eq!(futex_queue.count.load(Ordering::Relaxed), 1);

        let mut consume_slots = futex_queue
            .get_consume_slots(1, true, DEFAULT_SMALL_TIMEOUT, Duration::ZERO)
            .expect("best-effort consume slot");
        let slot = consume_slots.pop().expect("consume slot");
        assert_eq!(slot.data[0], 0x22);
        futex_queue.mark_slot_consumed(slot).expect("consume");
        assert_eq!(futex_queue.count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn batched_produce_and_staggered_consume() {
        let owned = YCFutexQueueOwnedData::new(4, 32);
        let mut futex_queue = YCFutexQueue::from_owned_data(&owned).unwrap();

        let mut slots = futex_queue
            .get_produce_slots(4, false, DEFAULT_SMALL_TIMEOUT, Duration::ZERO)
            .expect("reserve batch");
        for (i, slot) in slots.iter_mut().enumerate() {
            slot.data[0] = i as u8;
        }
        futex_queue
            .mark_slots_produced(slots)
            .expect("mark batch produced");
        assert_eq!(futex_queue.count.load(Ordering::Relaxed), 4);

        let consume_first = futex_queue
            .get_consume_slots(2, false, DEFAULT_SMALL_TIMEOUT, Duration::ZERO)
            .expect("consume first half");
        assert_eq!(futex_queue.count.load(Ordering::Relaxed), 2);
        futex_queue
            .mark_slots_consumed(consume_first)
            .expect("consume first half");

        let consume_second = futex_queue
            .get_consume_slots(2, false, DEFAULT_SMALL_TIMEOUT, Duration::ZERO)
            .expect("consume second half");
        assert_eq!(futex_queue.count.load(Ordering::Relaxed), 0);
        for (i, slot) in consume_second.iter().enumerate() {
            assert_eq!(slot.data[0], (i + 2) as u8);
        }
        futex_queue
            .mark_slots_consumed(consume_second)
            .expect("consume second half");
    }

    #[test]
    fn timeout_when_queue_empty_or_full() {
        let slot_count: u16 = 2;
        let owned = YCFutexQueueOwnedData::new(slot_count, 32);
        let mut futex_queue = YCFutexQueue::from_owned_data(&owned).unwrap();

        assert_eq!(
            futex_queue
                .get_consume_slots(1, false, DEFAULT_SMALL_TIMEOUT, Duration::ZERO)
                .expect_err("empty queue should time out"),
            YCQueueError::Timeout
        );
        assert_eq!(
            futex_queue
                .get_consume_slots(1, true, DEFAULT_SMALL_TIMEOUT, Duration::ZERO)
                .expect_err("empty queue should time out"),
            YCQueueError::Timeout
        );

        let slots = futex_queue
            .get_produce_slots(slot_count, false, DEFAULT_SMALL_TIMEOUT, Duration::ZERO)
            .expect("reserve full queue");
        futex_queue
            .mark_slots_produced(slots)
            .expect("produce full queue");
        assert_eq!(futex_queue.count.load(Ordering::Relaxed), slot_count as u32);

        assert_eq!(
            futex_queue
                .get_produce_slots(1, false, DEFAULT_SMALL_TIMEOUT, Duration::ZERO)
                .expect_err("full queue should time out"),
            YCQueueError::Timeout
        );
        assert_eq!(
            futex_queue
                .get_produce_slots(1, true, DEFAULT_SMALL_TIMEOUT, Duration::ZERO)
                .expect_err("full queue should time out"),
            YCQueueError::Timeout
        );

        let pending = futex_queue
            .get_consume_slots(slot_count, false, DEFAULT_SMALL_TIMEOUT, Duration::ZERO)
            .expect("drain queue");
        futex_queue
            .mark_slots_consumed(pending)
            .expect("drain queue");
        assert_eq!(futex_queue.count.load(Ordering::Relaxed), 0);
    }
}
