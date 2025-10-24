use std::{
    sync::atomic::{AtomicI32, Ordering},
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
    pub count: &'a AtomicI32,
}

impl<'a> YCFutexQueue<'a> {
    /// Wrap a queue together with a shared produced-counter.
    ///
    /// # Arguments
    /// * `queue` - The underlying `YCQueue` that will provide slot storage.
    /// * `count` - Shared atomic counter tracking the number of published slots.
    ///
    /// # Returns
    /// A futex-backed queue wrapper that can block producers and consumers efficiently.
    ///
    /// # Errors
    /// This constructor does not return errors.
    ///
    /// # Examples
    /// ```
    /// # #[cfg(feature = "futex")] {
    /// use std::sync::atomic::Ordering;
    /// use yep_coc::queue_alloc_helpers::{YCFutexQueueOwnedData, YCFutexQueueSharedData};
    /// use yep_coc::{YCQueue, YCFutexQueue};
    ///
    /// let owned = YCFutexQueueOwnedData::new(2, 16);
    /// let shared = YCFutexQueueSharedData::from_owned_data(&owned);
    /// let queue = YCQueue::new(shared.data.meta, shared.data.data).unwrap();
    /// let futex = YCFutexQueue::new(queue, shared.count);
    ///
    /// assert_eq!(futex.count.load(Ordering::Relaxed), 0);
    /// # }
    /// ```
    pub fn new(queue: YCQueue<'a>, count: &'a AtomicI32) -> Self {
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
    ///
    /// # Returns
    /// `Ok` with one or more slots reserved for production.
    ///
    /// # Errors
    /// Returns `YCQueueError::InvalidArgs`, `YCQueueError::OutOfSpace`, `YCQueueError::SlotNotReady`,
    /// or `YCQueueError::Timeout` depending on the underlying queue state.
    ///
    /// # Examples
    /// ```
    /// # #[cfg(feature = "futex")] {
    /// use std::time::Duration;
    /// use yep_coc::queue_alloc_helpers::YCFutexQueueOwnedData;
    /// use yep_coc::YCFutexQueue;
    ///
    /// let owned = YCFutexQueueOwnedData::new(4, 32);
    /// let mut queue = YCFutexQueue::from_owned_data(&owned).unwrap();
    ///
    /// let timeout = Duration::from_millis(1);
    /// let slots = queue
    ///     .get_produce_slots(2, false, timeout, Duration::ZERO)
    ///     .unwrap();
    /// queue.mark_slots_produced(slots).unwrap();
    /// # }
    /// ```
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
                    .wait_timeout(self.queue.capacity() as i32, remaining_timeout),
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

    /// Reserve a single slot for production, blocking up to `timeout`.
    ///
    /// This is equivalent to calling [`get_produce_slots`](Self::get_produce_slots) with
    /// `num_slots == 1` and `best_effort == false`.
    ///
    /// # Arguments
    /// * `timeout` - Maximum time to wait before giving up with `YCQueueError::Timeout`.
    /// * `retry_interval` - Minimum delay between retries when capacity is not yet available.
    ///
    /// # Returns
    /// `Ok` with the next available `YCQueueProduceSlot`.
    ///
    /// # Errors
    /// Propagates the same errors as [`get_produce_slots`](Self::get_produce_slots).
    ///
    /// # Examples
    /// ```
    /// # #[cfg(feature = "futex")] {
    /// use std::time::Duration;
    /// use yep_coc::queue_alloc_helpers::YCFutexQueueOwnedData;
    /// use yep_coc::YCFutexQueue;
    ///
    /// let owned = YCFutexQueueOwnedData::new(2, 32);
    /// let mut queue = YCFutexQueue::from_owned_data(&owned).unwrap();
    ///
    /// let timeout = Duration::from_millis(1);
    /// let slot = queue
    ///     .get_produce_slot(timeout, Duration::ZERO)
    ///     .unwrap();
    /// queue.mark_slot_produced(slot).unwrap();
    /// # }
    /// ```
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
    ///
    /// # Returns
    /// `Ok` with one or more contiguous slots available for consumption.
    ///
    /// # Errors
    /// Returns `YCQueueError::InvalidArgs`, `YCQueueError::EmptyQueue`, `YCQueueError::SlotNotReady`,
    /// or `YCQueueError::Timeout` when the requested slots cannot be obtained.
    ///
    /// # Examples
    /// ```
    /// # #[cfg(feature = "futex")] {
    /// use std::time::Duration;
    /// use yep_coc::queue_alloc_helpers::YCFutexQueueOwnedData;
    /// use yep_coc::YCFutexQueue;
    ///
    /// let owned = YCFutexQueueOwnedData::new(4, 32);
    /// let mut queue = YCFutexQueue::from_owned_data(&owned).unwrap();
    /// let timeout = Duration::from_millis(1);
    /// let slots = queue
    ///     .get_produce_slots(2, false, timeout, Duration::ZERO)
    ///     .unwrap();
    /// queue.mark_slots_produced(slots).unwrap();
    ///
    /// let ready = queue
    ///     .get_consume_slots(2, false, timeout, Duration::ZERO)
    ///     .unwrap();
    /// queue.mark_slots_consumed(ready).unwrap();
    /// # }
    /// ```
    pub fn get_consume_slots(
        &mut self,
        num_slots: u16,
        best_effort: bool,
        timeout: Duration,
        retry_interval: Duration,
    ) -> Result<Vec<YCQueueConsumeSlot<'a>>, YCQueueError> {
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
                    /*
                     * Because we subntract after successfully getting slots, it's possible that the slots have *already*
                     * been produced by another thread between the time we woke up and the time we subtracted here.
                     * This is okay, because the queue itself ensures that we only get slots that are actually produced.
                     * This allows the count to go beyond the queue capacity temporarily, but we should always have
                     * a non-zero count when there are slots to consume.
                     */
                    self.count.fetch_sub(slots.len() as i32, Ordering::AcqRel);
                    debug_assert!(self.count.load(Ordering::Relaxed) >= 0);
                    // Wake any producers that might be waiting for capacity.
                    self.count.notify_all();
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

    /// Reserve a single slot for consumption, blocking up to `timeout`.
    ///
    /// This is equivalent to calling [`get_consume_slots`](Self::get_consume_slots) with
    /// `num_slots == 1` and `best_effort == false`.
    ///
    /// # Arguments
    /// * `timeout` - Maximum time to wait before giving up with `YCQueueError::Timeout`.
    /// * `retry_interval` - Minimum delay between retries when no slots are yet published.
    ///
    /// # Returns
    /// `Ok` with the next ready `YCQueueConsumeSlot`.
    ///
    /// # Errors
    /// Propagates the same errors as [`get_consume_slots`](Self::get_consume_slots).
    ///
    /// # Examples
    /// ```
    /// # #[cfg(feature = "futex")] {
    /// use std::time::Duration;
    /// use yep_coc::queue_alloc_helpers::YCFutexQueueOwnedData;
    /// use yep_coc::YCFutexQueue;
    ///
    /// let owned = YCFutexQueueOwnedData::new(2, 32);
    /// let mut queue = YCFutexQueue::from_owned_data(&owned).unwrap();
    ///
    /// let timeout = Duration::from_millis(1);
    /// let to_publish = queue
    ///     .get_produce_slots(1, false, timeout, Duration::ZERO)
    ///     .unwrap();
    /// queue.mark_slots_produced(to_publish).unwrap();
    ///
    /// let slot = queue
    ///     .get_consume_slot(timeout, Duration::ZERO)
    ///     .unwrap();
    /// queue.mark_slot_consumed(slot).unwrap();
    /// # }
    /// ```
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
    ///
    /// # Arguments
    /// * `slot` - Slot previously obtained from [`get_produce_slot`](Self::get_produce_slot) or
    ///   [`get_produce_slots`](Self::get_produce_slots).
    ///
    /// # Returns
    /// `Ok(())` when the slot is successfully marked as produced.
    ///
    /// # Errors
    /// Propagates `YCQueueError::InvalidArgs` when the slot metadata does not match the queue.
    ///
    /// # Examples
    /// ```
    /// # #[cfg(feature = "futex")] {
    /// use std::time::Duration;
    /// use yep_coc::queue_alloc_helpers::YCFutexQueueOwnedData;
    /// use yep_coc::YCFutexQueue;
    ///
    /// let owned = YCFutexQueueOwnedData::new(2, 32);
    /// let mut queue = YCFutexQueue::from_owned_data(&owned).unwrap();
    ///
    /// let slot = queue
    ///     .get_produce_slot(Duration::from_millis(1), Duration::ZERO)
    ///     .unwrap();
    /// queue.mark_slot_produced(slot).unwrap();
    /// # }
    /// ```
    pub fn mark_slot_produced(&mut self, slot: YCQueueProduceSlot<'a>) -> Result<(), YCQueueError> {
        debug_assert!(self.count.load(Ordering::Relaxed) >= 0);
        self.count.fetch_add(1, Ordering::AcqRel);
        self.queue.mark_slot_produced(slot)?;
        self.count.notify_all();
        Ok(())
    }

    /// Mark multiple slots as produced and wake waiting consumers.
    ///
    /// # Arguments
    /// * `slots` - Contiguous slots obtained via [`get_produce_slots`](Self::get_produce_slots).
    ///
    /// # Returns
    /// `Ok(())` when all slots are successfully published.
    ///
    /// # Errors
    /// Propagates `YCQueueError::InvalidArgs` when any slot does not belong to this queue.
    ///
    /// # Examples
    /// ```
    /// # #[cfg(feature = "futex")] {
    /// use std::time::Duration;
    /// use yep_coc::queue_alloc_helpers::YCFutexQueueOwnedData;
    /// use yep_coc::YCFutexQueue;
    ///
    /// let owned = YCFutexQueueOwnedData::new(4, 32);
    /// let mut queue = YCFutexQueue::from_owned_data(&owned).unwrap();
    ///
    /// let timeout = Duration::from_millis(1);
    /// let slots = queue
    ///     .get_produce_slots(3, false, timeout, Duration::ZERO)
    ///     .unwrap();
    /// queue.mark_slots_produced(slots).unwrap();
    /// # }
    /// ```
    pub fn mark_slots_produced(
        &mut self,
        slots: Vec<YCQueueProduceSlot<'a>>,
    ) -> Result<(), YCQueueError> {
        if slots.is_empty() {
            return Ok(());
        }

        let num_produced = slots.len() as i32;
        debug_assert!(self.count.load(Ordering::Relaxed) >= 0);
        self.count.fetch_add(num_produced, Ordering::AcqRel);
        self.queue.mark_slots_produced(slots)?;
        self.count.notify_all();
        Ok(())
    }

    /// Mark a single slot as consumed and notify waiting producers.
    ///
    /// Returns the slot to the underlying queue and keeps the produced counter in sync so that
    /// sleeping producers are woken as capacity becomes available.
    ///
    /// # Arguments
    /// * `slot` - Slot previously obtained from [`get_consume_slot`](Self::get_consume_slot) or
    ///   [`get_consume_slots`](Self::get_consume_slots).
    ///
    /// # Returns
    /// `Ok(())` when the slot is successfully returned to producers.
    ///
    /// # Errors
    /// Propagates `YCQueueError::InvalidArgs` when the slot metadata does not match the queue.
    ///
    /// # Examples
    /// ```
    /// # #[cfg(feature = "futex")] {
    /// use std::time::Duration;
    /// use yep_coc::queue_alloc_helpers::YCFutexQueueOwnedData;
    /// use yep_coc::YCFutexQueue;
    ///
    /// let owned = YCFutexQueueOwnedData::new(2, 32);
    /// let mut queue = YCFutexQueue::from_owned_data(&owned).unwrap();
    ///
    /// let timeout = Duration::from_millis(1);
    /// let slots = queue
    ///     .get_produce_slots(1, false, timeout, Duration::ZERO)
    ///     .unwrap();
    /// queue.mark_slots_produced(slots).unwrap();
    ///
    /// let slot = queue
    ///     .get_consume_slot(timeout, Duration::ZERO)
    ///     .unwrap();
    /// queue.mark_slot_consumed(slot).unwrap();
    /// # }
    /// ```
    pub fn mark_slot_consumed(&mut self, slot: YCQueueConsumeSlot<'a>) -> Result<(), YCQueueError> {
        self.queue.mark_slot_consumed(slot)
    }

    /// Mark multiple slots as consumed and notify waiting producers.
    ///
    /// Accepts the result of [`get_consume_slots`](Self::get_consume_slots) and returns the entire
    /// batch to the producer pool.
    ///
    /// # Arguments
    /// * `slots` - Contiguous slots obtained via [`get_consume_slots`](Self::get_consume_slots).
    ///
    /// # Returns
    /// `Ok(())` when all slots are successfully returned to the producer side.
    ///
    /// # Errors
    /// Propagates `YCQueueError::InvalidArgs` when any slot does not belong to this queue.
    ///
    /// # Examples
    /// ```
    /// # #[cfg(feature = "futex")] {
    /// use std::time::Duration;
    /// use yep_coc::queue_alloc_helpers::YCFutexQueueOwnedData;
    /// use yep_coc::YCFutexQueue;
    ///
    /// let owned = YCFutexQueueOwnedData::new(4, 32);
    /// let mut queue = YCFutexQueue::from_owned_data(&owned).unwrap();
    ///
    /// let timeout = Duration::from_millis(1);
    /// let slots = queue
    ///     .get_produce_slots(4, false, timeout, Duration::ZERO)
    ///     .unwrap();
    /// queue.mark_slots_produced(slots).unwrap();
    ///
    /// let ready = queue
    ///     .get_consume_slots(4, false, timeout, Duration::ZERO)
    ///     .unwrap();
    /// queue.mark_slots_consumed(ready).unwrap();
    /// # }
    /// ```
    pub fn mark_slots_consumed(
        &mut self,
        slots: Vec<YCQueueConsumeSlot<'a>>,
    ) -> Result<(), YCQueueError> {
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
        assert_eq!(futex_queue.count.load(Ordering::Relaxed), slot_count as i32);

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
