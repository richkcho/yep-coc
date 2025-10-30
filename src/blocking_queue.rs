use std::{
    sync::{Condvar, Mutex},
    time::{Duration, Instant},
};

use crate::{YCQueue, YCQueueConsumeSlot, YCQueueError, YCQueueProduceSlot};

/// Mutex and CondVar backed convenience wrapper over [`YCQueue`].
///
/// Each instance maintains a shared counter protected by a mutex that tracks how many slots
/// have been produced and are ready for consumption. Producers increment this
/// counter when publishing data, and consumers decrement it after consuming
/// slots. Waiting operations leverage condition variables to block efficiently
/// until the counter changes or a timeout expires.
pub struct YCBlockingQueue<'a> {
    pub queue: YCQueue<'a>,
    pub count: &'a Mutex<i32>,
    pub condvar: &'a Condvar,
}

impl<'a> YCBlockingQueue<'a> {
    /// Wrap a queue together with a shared produced-counter.
    ///
    /// # Arguments
    /// * `queue` - The underlying `YCQueue` that will provide slot storage.
    /// * `count` - Shared mutex-protected counter tracking the number of published slots.
    /// * `condvar` - Condition variable for efficient blocking/waking.
    ///
    /// # Returns
    /// A blocking queue wrapper that can block producers and consumers efficiently.
    ///
    /// # Errors
    /// This constructor does not return errors.
    ///
    /// # Examples
    /// ```
    /// # #[cfg(feature = "blocking")] {
    /// use std::sync::{Mutex, Condvar};
    /// use yep_coc::queue_alloc_helpers::{YCBlockingQueueOwnedData, YCBlockingQueueSharedData};
    /// use yep_coc::{YCQueue, YCBlockingQueue};
    ///
    /// let owned = YCBlockingQueueOwnedData::new(2, 16);
    /// let shared = YCBlockingQueueSharedData::from_owned_data(&owned);
    /// let queue = YCQueue::new(shared.data.meta, shared.data.data).unwrap();
    /// let blocking = YCBlockingQueue::new(queue, shared.count, shared.condvar);
    ///
    /// assert_eq!(*blocking.count.lock().unwrap(), 0);
    /// # }
    /// ```
    pub fn new(queue: YCQueue<'a>, count: &'a Mutex<i32>, condvar: &'a Condvar) -> Self {
        YCBlockingQueue {
            queue,
            count,
            condvar,
        }
    }

    /// Reserve up to `num_slots` slots for production, optionally accepting a partial batch.
    ///
    /// # Arguments
    /// * `num_slots` - Maximum contiguous slots to claim.
    /// * `best_effort` - When `true`, returns the currently available contiguous span even if it
    ///   is smaller than `num_slots`.
    /// * `timeout` - Maximum time to wait for space before returning `YCQueueError::Timeout`.
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
    /// # #[cfg(feature = "blocking")] {
    /// use std::time::Duration;
    /// use yep_coc::queue_alloc_helpers::YCBlockingQueueOwnedData;
    /// use yep_coc::YCBlockingQueue;
    ///
    /// let owned = YCBlockingQueueOwnedData::new(4, 32);
    /// let mut queue = YCBlockingQueue::from_owned_data(&owned).unwrap();
    ///
    /// let timeout = Duration::from_millis(1);
    /// let slots = queue
    ///     .get_produce_slots(2, false, timeout)
    ///     .unwrap();
    /// queue.mark_slots_produced(slots).unwrap();
    /// # }
    /// ```
    pub fn get_produce_slots(
        &mut self,
        num_slots: u16,
        best_effort: bool,
        timeout: Duration,
    ) -> Result<Vec<YCQueueProduceSlot<'a>>, YCQueueError> {
        let start_time = Instant::now();
        loop {
            // Wait until count < capacity (space available)
            let mut count_guard = self.count.lock().unwrap();
            let capacity = self.queue.capacity() as i32;
            while *count_guard >= capacity {
                // Recalculate remaining timeout before each wait
                let remaining_timeout = match timeout.checked_sub(start_time.elapsed()) {
                    Some(t) => t,
                    None => return Err(YCQueueError::Timeout),
                };
                
                let (new_guard, timeout_result) =
                    self.condvar.wait_timeout(count_guard, remaining_timeout).unwrap();
                count_guard = new_guard;
                
                if timeout_result.timed_out() {
                    return Err(YCQueueError::Timeout);
                }
            }
            drop(count_guard);

            let ret = self.queue.get_produce_slots(num_slots, best_effort);
            match ret {
                Ok(slots) => return Ok(slots),
                Err(_) => {
                    // Retry the loop - the condition variable will block if needed
                    continue;
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
    ///
    /// # Returns
    /// `Ok` with the next available `YCQueueProduceSlot`.
    ///
    /// # Errors
    /// Propagates the same errors as [`get_produce_slots`](Self::get_produce_slots).
    ///
    /// # Examples
    /// ```
    /// # #[cfg(feature = "blocking")] {
    /// use std::time::Duration;
    /// use yep_coc::queue_alloc_helpers::YCBlockingQueueOwnedData;
    /// use yep_coc::YCBlockingQueue;
    ///
    /// let owned = YCBlockingQueueOwnedData::new(2, 32);
    /// let mut queue = YCBlockingQueue::from_owned_data(&owned).unwrap();
    ///
    /// let timeout = Duration::from_millis(1);
    /// let slot = queue
    ///     .get_produce_slot(timeout)
    ///     .unwrap();
    /// queue.mark_slot_produced(slot).unwrap();
    /// # }
    /// ```
    pub fn get_produce_slot(
        &mut self,
        timeout: Duration,
    ) -> Result<YCQueueProduceSlot<'a>, YCQueueError> {
        let mut slots = self.get_produce_slots(1, false, timeout)?;

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
    /// # #[cfg(feature = "blocking")] {
    /// use std::time::Duration;
    /// use yep_coc::queue_alloc_helpers::YCBlockingQueueOwnedData;
    /// use yep_coc::YCBlockingQueue;
    ///
    /// let owned = YCBlockingQueueOwnedData::new(4, 32);
    /// let mut queue = YCBlockingQueue::from_owned_data(&owned).unwrap();
    /// let timeout = Duration::from_millis(1);
    /// let slots = queue
    ///     .get_produce_slots(2, false, timeout)
    ///     .unwrap();
    /// queue.mark_slots_produced(slots).unwrap();
    ///
    /// let ready = queue
    ///     .get_consume_slots(2, false, timeout)
    ///     .unwrap();
    /// queue.mark_slots_consumed(ready).unwrap();
    /// # }
    /// ```
    pub fn get_consume_slots(
        &mut self,
        num_slots: u16,
        best_effort: bool,
        timeout: Duration,
    ) -> Result<Vec<YCQueueConsumeSlot<'a>>, YCQueueError> {
        let start_time = Instant::now();
        loop {
            // Wait until count > 0 (data available)
            let mut count_guard = self.count.lock().unwrap();
            while *count_guard <= 0 {
                // Recalculate remaining timeout before each wait
                let remaining_timeout = match timeout.checked_sub(start_time.elapsed()) {
                    Some(t) => t,
                    None => return Err(YCQueueError::Timeout),
                };
                
                let (new_guard, timeout_result) =
                    self.condvar.wait_timeout(count_guard, remaining_timeout).unwrap();
                count_guard = new_guard;
                
                if timeout_result.timed_out() {
                    return Err(YCQueueError::Timeout);
                }
            }
            drop(count_guard);

            let ret = self.queue.get_consume_slots(num_slots, best_effort);
            match ret {
                Ok(slots) => {
                    /*
                     * Because we subtract after successfully getting slots, it's possible that the slots have *already*
                     * been produced by another thread between the time we woke up and the time we subtracted here.
                     * This is okay, because the queue itself ensures that we only get slots that are actually produced.
                     * This allows the count to go beyond the queue capacity temporarily, but we should always have
                     * a non-zero count when there are slots to consume. Note: in rare race conditions between multiple
                     * consumers, the count may temporarily go slightly negative, which is acceptable.
                     */
                    let mut count = self.count.lock().unwrap();
                    *count -= slots.len() as i32;
                    drop(count);
                    // Wake any producers that might be waiting for capacity.
                    self.condvar.notify_all();
                    return Ok(slots);
                }
                Err(_) => {
                    // Retry the loop - the condition variable will block if needed
                    continue;
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
    ///
    /// # Returns
    /// `Ok` with the next ready `YCQueueConsumeSlot`.
    ///
    /// # Errors
    /// Propagates the same errors as [`get_consume_slots`](Self::get_consume_slots).
    ///
    /// # Examples
    /// ```
    /// # #[cfg(feature = "blocking")] {
    /// use std::time::Duration;
    /// use yep_coc::queue_alloc_helpers::YCBlockingQueueOwnedData;
    /// use yep_coc::YCBlockingQueue;
    ///
    /// let owned = YCBlockingQueueOwnedData::new(2, 32);
    /// let mut queue = YCBlockingQueue::from_owned_data(&owned).unwrap();
    ///
    /// let timeout = Duration::from_millis(1);
    /// let to_publish = queue
    ///     .get_produce_slots(1, false, timeout)
    ///     .unwrap();
    /// queue.mark_slots_produced(to_publish).unwrap();
    ///
    /// let slot = queue
    ///     .get_consume_slot(timeout)
    ///     .unwrap();
    /// queue.mark_slot_consumed(slot).unwrap();
    /// # }
    /// ```
    pub fn get_consume_slot(
        &mut self,
        timeout: Duration,
    ) -> Result<YCQueueConsumeSlot<'a>, YCQueueError> {
        let mut slots = self.get_consume_slots(1, false, timeout)?;

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
    /// # #[cfg(feature = "blocking")] {
    /// use std::time::Duration;
    /// use yep_coc::queue_alloc_helpers::YCBlockingQueueOwnedData;
    /// use yep_coc::YCBlockingQueue;
    ///
    /// let owned = YCBlockingQueueOwnedData::new(2, 32);
    /// let mut queue = YCBlockingQueue::from_owned_data(&owned).unwrap();
    ///
    /// let slot = queue
    ///     .get_produce_slot(Duration::from_millis(1))
    ///     .unwrap();
    /// queue.mark_slot_produced(slot).unwrap();
    /// # }
    /// ```
    pub fn mark_slot_produced(&mut self, slot: YCQueueProduceSlot<'a>) -> Result<(), YCQueueError> {
        let mut count = self.count.lock().unwrap();
        debug_assert!(*count >= 0);
        *count += 1;
        drop(count);
        self.queue.mark_slot_produced(slot)?;
        self.condvar.notify_all();
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
    /// # #[cfg(feature = "blocking")] {
    /// use std::time::Duration;
    /// use yep_coc::queue_alloc_helpers::YCBlockingQueueOwnedData;
    /// use yep_coc::YCBlockingQueue;
    ///
    /// let owned = YCBlockingQueueOwnedData::new(4, 32);
    /// let mut queue = YCBlockingQueue::from_owned_data(&owned).unwrap();
    ///
    /// let timeout = Duration::from_millis(1);
    /// let slots = queue
    ///     .get_produce_slots(3, false, timeout)
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
        let mut count = self.count.lock().unwrap();
        debug_assert!(*count >= 0);
        *count += num_produced;
        drop(count);
        self.queue.mark_slots_produced(slots)?;
        self.condvar.notify_all();
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
    /// # #[cfg(feature = "blocking")] {
    /// use std::time::Duration;
    /// use yep_coc::queue_alloc_helpers::YCBlockingQueueOwnedData;
    /// use yep_coc::YCBlockingQueue;
    ///
    /// let owned = YCBlockingQueueOwnedData::new(2, 32);
    /// let mut queue = YCBlockingQueue::from_owned_data(&owned).unwrap();
    ///
    /// let timeout = Duration::from_millis(1);
    /// let slots = queue
    ///     .get_produce_slots(1, false, timeout)
    ///     .unwrap();
    /// queue.mark_slots_produced(slots).unwrap();
    ///
    /// let slot = queue
    ///     .get_consume_slot(timeout)
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
    /// # #[cfg(feature = "blocking")] {
    /// use std::time::Duration;
    /// use yep_coc::queue_alloc_helpers::YCBlockingQueueOwnedData;
    /// use yep_coc::YCBlockingQueue;
    ///
    /// let owned = YCBlockingQueueOwnedData::new(4, 32);
    /// let mut queue = YCBlockingQueue::from_owned_data(&owned).unwrap();
    ///
    /// let timeout = Duration::from_millis(1);
    /// let slots = queue
    ///     .get_produce_slots(4, false, timeout)
    ///     .unwrap();
    /// queue.mark_slots_produced(slots).unwrap();
    ///
    /// let ready = queue
    ///     .get_consume_slots(4, false, timeout)
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
    use crate::queue_alloc_helpers::YCBlockingQueueOwnedData;

    const DEFAULT_SMALL_TIMEOUT: Duration = Duration::from_millis(1);

    #[test]
    fn single_slot_roundtrip_and_best_effort_paths() {
        let owned = YCBlockingQueueOwnedData::new(4, 32);
        let mut blocking_queue = YCBlockingQueue::from_owned_data(&owned).unwrap();

        let mut slots = blocking_queue
            .get_produce_slots(1, false, DEFAULT_SMALL_TIMEOUT)
            .expect("reserve slot");
        let slot = slots.pop().expect("slot");
        slot.data[0] = 0x11;
        blocking_queue.mark_slot_produced(slot).expect("produce");
        assert_eq!(*blocking_queue.count.lock().unwrap(), 1);

        let mut consume_slots = blocking_queue
            .get_consume_slots(1, false, DEFAULT_SMALL_TIMEOUT)
            .expect("consume slot");
        let slot = consume_slots.pop().expect("consume slot");
        assert_eq!(slot.data[0], 0x11);
        blocking_queue.mark_slot_consumed(slot).expect("consume");
        assert_eq!(*blocking_queue.count.lock().unwrap(), 0);

        let mut slots = blocking_queue
            .get_produce_slots(1, true, DEFAULT_SMALL_TIMEOUT)
            .expect("best-effort reserve slot");
        let slot = slots.pop().expect("slot");
        slot.data[0] = 0x22;
        blocking_queue.mark_slot_produced(slot).expect("produce");
        assert_eq!(*blocking_queue.count.lock().unwrap(), 1);

        let mut consume_slots = blocking_queue
            .get_consume_slots(1, true, DEFAULT_SMALL_TIMEOUT)
            .expect("best-effort consume slot");
        let slot = consume_slots.pop().expect("consume slot");
        assert_eq!(slot.data[0], 0x22);
        blocking_queue.mark_slot_consumed(slot).expect("consume");
        assert_eq!(*blocking_queue.count.lock().unwrap(), 0);
    }

    #[test]
    fn batched_produce_and_staggered_consume() {
        let owned = YCBlockingQueueOwnedData::new(4, 32);
        let mut blocking_queue = YCBlockingQueue::from_owned_data(&owned).unwrap();

        let mut slots = blocking_queue
            .get_produce_slots(4, false, DEFAULT_SMALL_TIMEOUT)
            .expect("reserve batch");
        for (i, slot) in slots.iter_mut().enumerate() {
            slot.data[0] = i as u8;
        }
        blocking_queue
            .mark_slots_produced(slots)
            .expect("mark batch produced");
        assert_eq!(*blocking_queue.count.lock().unwrap(), 4);

        let consume_first = blocking_queue
            .get_consume_slots(2, false, DEFAULT_SMALL_TIMEOUT)
            .expect("consume first half");
        assert_eq!(*blocking_queue.count.lock().unwrap(), 2);
        blocking_queue
            .mark_slots_consumed(consume_first)
            .expect("consume first half");

        let consume_second = blocking_queue
            .get_consume_slots(2, false, DEFAULT_SMALL_TIMEOUT)
            .expect("consume second half");
        assert_eq!(*blocking_queue.count.lock().unwrap(), 0);
        for (i, slot) in consume_second.iter().enumerate() {
            assert_eq!(slot.data[0], (i + 2) as u8);
        }
        blocking_queue
            .mark_slots_consumed(consume_second)
            .expect("consume second half");
    }

    #[test]
    fn timeout_when_queue_empty_or_full() {
        let slot_count: u16 = 2;
        let owned = YCBlockingQueueOwnedData::new(slot_count, 32);
        let mut blocking_queue = YCBlockingQueue::from_owned_data(&owned).unwrap();

        assert_eq!(
            blocking_queue
                .get_consume_slots(1, false, DEFAULT_SMALL_TIMEOUT)
                .expect_err("empty queue should time out"),
            YCQueueError::Timeout
        );
        assert_eq!(
            blocking_queue
                .get_consume_slots(1, true, DEFAULT_SMALL_TIMEOUT)
                .expect_err("empty queue should time out"),
            YCQueueError::Timeout
        );

        let slots = blocking_queue
            .get_produce_slots(slot_count, false, DEFAULT_SMALL_TIMEOUT)
            .expect("reserve full queue");
        blocking_queue
            .mark_slots_produced(slots)
            .expect("produce full queue");
        assert_eq!(*blocking_queue.count.lock().unwrap(), slot_count as i32);

        assert_eq!(
            blocking_queue
                .get_produce_slots(1, false, DEFAULT_SMALL_TIMEOUT)
                .expect_err("full queue should time out"),
            YCQueueError::Timeout
        );
        assert_eq!(
            blocking_queue
                .get_produce_slots(1, true, DEFAULT_SMALL_TIMEOUT)
                .expect_err("full queue should time out"),
            YCQueueError::Timeout
        );

        let pending = blocking_queue
            .get_consume_slots(slot_count, false, DEFAULT_SMALL_TIMEOUT)
            .expect("drain queue");
        blocking_queue
            .mark_slots_consumed(pending)
            .expect("drain queue");
        assert_eq!(*blocking_queue.count.lock().unwrap(), 0);
    }
}
