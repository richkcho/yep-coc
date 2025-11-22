use std::sync::atomic::Ordering;

use crate::queue_meta::{YCQueueCursor, cursor_advance, cursor_index};

use crate::utils::get_bit;
use crate::{YCQueueError, YCQueueSharedMeta, utils};

#[derive(Debug)]
pub struct YCQueueProduceSlot<'a> {
    pub index: u16,
    pub data: &'a mut [u8],
}

#[derive(Debug)]
pub struct YCQueueConsumeSlot<'a> {
    pub index: u16,
    pub data: &'a mut [u8],
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum YCQueueOwner {
    Producer,
    Consumer,
}

pub struct YCQueue<'a> {
    shared_metadata: YCQueueSharedMeta<'a>,
    slots: Vec<Option<&'a mut [u8]>>,
    slot_count: u16,
    slot_size: u16,
    // slot count is a power of two, so we can store its exponent for fast calculations
    slot_count_exp: u16,
    cached_consumer_cursor: YCQueueCursor,
    cached_producer_cursor: YCQueueCursor,
}

impl<'a> YCQueue<'a> {
    /// Create a queue backed by shared metadata and a contiguous data region.
    ///
    /// # Arguments
    /// * `shared_metadata` - Shared ownership state that tracks slot usage across threads.
    /// * `data_region` - Contiguous slice that will be divided into fixed-size slots.
    ///
    /// # Returns
    /// `Ok(YCQueue)` when `data_region` aligns with the metadata configuration, or `Err(YCQueueError::InvalidArgs)` if the arguments disagree.
    ///
    /// # Examples
    /// ```
    /// use yep_coc::queue_alloc_helpers::YCQueueOwnedData;
    /// use yep_coc::{YCQueue, YCQueueSharedMeta};
    ///
    /// let mut owned = YCQueueOwnedData::new(4, 32);
    /// let shared = YCQueueSharedMeta::new(&owned.meta);
    /// let queue = YCQueue::new(shared, owned.data.as_mut_slice());
    /// assert!(queue.is_ok());
    /// ```
    pub fn new(
        shared_metadata: YCQueueSharedMeta<'a>,
        data_region: &'a mut [u8],
    ) -> Result<YCQueue<'a>, YCQueueError> {
        let slot_size = shared_metadata.slot_size.load(Ordering::Acquire) as usize;
        let slot_count = data_region.len() / slot_size;

        if !data_region.len().is_multiple_of(slot_size) {
            return Err(YCQueueError::InvalidArgs);
        }

        let mut slots = Vec::<Option<&'a mut [u8]>>::with_capacity(slot_count);
        for slot in data_region.chunks_exact_mut(slot_size) {
            slots.push(Some(slot));
        }

        if shared_metadata.slot_count.load(Ordering::Acquire) as usize != slot_count {
            return Err(YCQueueError::InvalidArgs);
        }

        if !slot_count.is_power_of_two() {
            return Err(YCQueueError::InvalidArgs);
        }
        let slot_count_exp = slot_count.trailing_zeros() as u16;

        Ok(YCQueue {
            shared_metadata,
            slots,
            slot_count: slot_count as u16,
            slot_size: slot_size as u16,
            slot_count_exp,
            cached_consumer_cursor: shared_metadata.consumer_cursor.load(Ordering::Acquire),
            cached_producer_cursor: shared_metadata.producer_cursor.load(Ordering::Acquire),
        })
    }

    /// Count how many slots starting at `idx` are currently owned by `owner`, up to `range`.
    ///
    /// Returns the number of consecutive slots that matched before encountering one owned by the
    /// opposite party, wrapping around the ring as needed.
    fn check_owner(&self, idx: u16, range: u16, owner: YCQueueOwner) -> u16 {
        if range == 0 || idx >= self.slot_count || range > self.slot_count {
            return 0;
        }

        let mut processed: u16 = 0;
        let mut remaining = range as u32;
        let mut current = idx as u32;
        let slot_count = self.slot_count as u32;

        while remaining > 0 {
            if current >= slot_count {
                current -= slot_count;
            }

            let chunk_idx = (current / u64::BITS) as usize;
            let bit_offset = (current % u64::BITS) as u8;
            let bits_available = u64::BITS - bit_offset as u32;
            let slots_until_end = slot_count - current;
            let span = remaining.min(bits_available).min(slots_until_end);
            debug_assert!(span > 0);

            let span_mask = if span == u64::BITS {
                !0u64
            } else {
                (1u64 << span) - 1
            };
            let value = self.shared_metadata.ownership[chunk_idx].load(Ordering::Acquire);
            let masked = (value >> bit_offset) & span_mask;

            match owner {
                YCQueueOwner::Producer => {
                    if masked != 0 {
                        let offset = masked.trailing_zeros() as u16;
                        return processed + offset;
                    }
                }
                YCQueueOwner::Consumer => {
                    if masked != span_mask {
                        let missing = (!masked) & span_mask;
                        let offset = missing.trailing_zeros() as u16;
                        return processed + offset;
                    }
                }
            }

            processed += span as u16;
            remaining -= span;
            current += span;
        }

        processed
    }

    /// Load the current owner bit for a single slot.
    ///
    /// Returns `YCQueueOwner::Producer` when the slot is available to producers and `Consumer`
    /// otherwise.
    fn get_owner(&self, idx: u16) -> YCQueueOwner {
        let atomic_idx = idx / u64::BITS as u16;
        let bit_idx = (idx % u64::BITS as u16) as u8;
        let atomic = &self.shared_metadata.ownership[atomic_idx as usize];
        let value = atomic.load(Ordering::Acquire);

        match utils::get_bit(&value, bit_idx) {
            false => YCQueueOwner::Producer,
            true => YCQueueOwner::Consumer,
        }
    }

    /// Atomically set the owner of a single slot, returning the previous owner.
    ///
    /// Used by producer/consumer transitions to flip the ownership bit while maintaining ordering.
    fn set_owner(&mut self, idx: u16, owner: YCQueueOwner) -> YCQueueOwner {
        loop {
            let atomic_idx = idx / u64::BITS as u16;
            let bit_idx = (idx % u64::BITS as u16) as u8;
            let atomic = &self.shared_metadata.ownership[atomic_idx as usize];
            let value = atomic.load(Ordering::Acquire);

            let new_value = match owner {
                YCQueueOwner::Producer => utils::clear_bit(&value, bit_idx),
                YCQueueOwner::Consumer => utils::set_bit(&value, bit_idx),
            };

            match atomic.compare_exchange(value, new_value, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => {
                    if get_bit(&value, bit_idx) {
                        return YCQueueOwner::Consumer;
                    } else {
                        return YCQueueOwner::Producer;
                    }
                }
                Err(_) => continue,
            }
        }
    }

    /// Atomically set the owner for a contiguous range of slots.
    ///
    /// Returns `Err(YCQueueError::InvalidArgs)` when the starting index is out of bounds or the
    /// range length exceeds the queue capacity.
    fn set_owner_range(
        &mut self,
        idx: u16,
        range: u16,
        owner: YCQueueOwner,
    ) -> Result<(), YCQueueError> {
        if range == 0 {
            return Ok(());
        }
        if idx >= self.slot_count || range > self.slot_count {
            return Err(YCQueueError::InvalidArgs);
        }
        let mut remaining = range as u32;
        let mut current = idx as u32;
        let slot_count = self.slot_count as u32;

        while remaining > 0 {
            // wrap around if needed
            if current >= slot_count {
                // TODO: tag as cold path
                current -= slot_count;
            }

            let chunk_idx = (current / u64::BITS) as usize;
            let bit_offset = (current % u64::BITS) as u8;
            let bits_available = u64::BITS - bit_offset as u32;
            let slots_until_end = slot_count - current;
            let span = remaining.min(bits_available).min(slots_until_end);
            debug_assert!(span > 0);

            let mask = if span == u64::BITS {
                !0u64
            } else {
                ((1u64 << span) - 1) << bit_offset
            };

            loop {
                let value = self.shared_metadata.ownership[chunk_idx].load(Ordering::Acquire);

                let new_value = match owner {
                    YCQueueOwner::Producer => {
                        debug_assert_eq!(value & mask, mask);
                        value & !mask
                    }
                    YCQueueOwner::Consumer => {
                        debug_assert_eq!(value & mask, 0);
                        value | mask
                    }
                };

                match self.shared_metadata.ownership[chunk_idx].compare_exchange(
                    value,
                    new_value,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(_) => continue,
                }
            }

            remaining -= span;
            current += span;
        }

        Ok(())
    }

    fn refresh_consumer_cursor(&mut self) -> YCQueueCursor {
        let cursor = self.shared_metadata.consumer_cursor.load(Ordering::Acquire);
        self.cached_consumer_cursor = cursor;
        cursor
    }

    fn refresh_producer_cursor(&mut self) -> YCQueueCursor {
        let cursor = self.shared_metadata.producer_cursor.load(Ordering::Acquire);
        self.cached_producer_cursor = cursor;
        cursor
    }

    fn cursor_index(&self, cursor: YCQueueCursor) -> u16 {
        cursor_index(cursor, self.slot_count_exp)
    }

    /// Returns the number of slots that have been produced (or are being produced into) but not yet consumed.
    ///
    /// # Returns
    /// Count of slots currently in flight between producers and consumers.
    ///
    /// # Examples
    /// ```
    /// use yep_coc::queue_alloc_helpers::YCQueueOwnedData;
    /// use yep_coc::{YCQueue, YCQueueSharedMeta};
    ///
    /// let mut owned = YCQueueOwnedData::new(2, 16);
    /// let shared = YCQueueSharedMeta::new(&owned.meta);
    /// let mut queue = YCQueue::new(shared, owned.data.as_mut_slice()).unwrap();
    ///
    /// assert_eq!(queue.in_flight_count(), 0);
    /// let slot = queue.get_produce_slot().unwrap();
    /// queue.mark_slot_produced(slot).unwrap();
    /// assert_eq!(queue.in_flight_count(), 1);
    /// ```
    #[inline]
    pub fn in_flight_count(&mut self) -> u16 {
        self.refresh_consumer_cursor();
        self.refresh_producer_cursor();
        self.slots_used_cached()
            .expect("producer cursor should not trail consumer")
    }

    /// Returns the circular index that will be reserved by the next producer call.
    ///
    /// # Returns
    /// The slot index measured modulo the queue capacity.
    #[inline]
    pub fn produce_idx(&self) -> u16 {
        self.cursor_index(self.cached_producer_cursor)
    }

    /// Returns the circular index that will be reserved by the next consumer call.
    ///
    /// # Returns
    /// The slot index measured modulo the queue capacity.
    #[inline]
    pub fn consume_idx(&self) -> u16 {
        self.cursor_index(self.cached_consumer_cursor)
    }

    /// Returns the total number of slots managed by this queue.
    #[inline]
    pub fn capacity(&self) -> u16 {
        self.slot_count
    }

    fn slots_used_cached(&self) -> Option<u16> {
        let producer_cursor = self.cached_producer_cursor;
        let consumer_cursor = self.cached_consumer_cursor;

        // is producer is behind consumer, it (producer) is stale
        if producer_cursor < consumer_cursor {
            return None;
        }

        // if producer is more than slot_count ahead, consumer is stale
        if producer_cursor > consumer_cursor + self.slot_count as u64 {
            return None;
        }

        let used = u16::try_from(producer_cursor - consumer_cursor).unwrap_or_else(|_| {
            panic!("distance should fit in u16: {producer_cursor} - {consumer_cursor}")
        });
        Some(used)
    }

    fn slots_available_cached(&self) -> Option<u16> {
        let used = self.slots_used_cached()?;
        Some(self.slot_count - used)
    }

    /// Reserve contiguous slots for producers, optionally in best-effort mode.
    ///
    /// When `best_effort` is `false`, the function succeeds only if all `num_slots` are available;
    /// otherwise it returns as many contiguous slots as currently ready.
    ///
    /// # Arguments
    /// * `num_slots` - Maximum number of contiguous slots to attempt to reserve.
    /// * `best_effort` - When `true`, grants a partial batch instead of requiring `num_slots`.
    ///
    /// # Returns
    /// `Ok` containing one or more slots when reservation succeeds.
    ///
    /// # Errors
    /// Returns `YCQueueError::InvalidArgs` when `num_slots` is zero or exceeds capacity,
    /// `YCQueueError::OutOfSpace` when there is no remaining capacity, and
    /// `YCQueueError::SlotNotReady` when the requested slots are not owned by the producer.
    ///
    /// # Examples
    /// ```
    /// use yep_coc::queue_alloc_helpers::YCQueueOwnedData;
    /// use yep_coc::{YCQueue, YCQueueSharedMeta};
    ///
    /// let mut owned = YCQueueOwnedData::new(4, 16);
    /// let shared = YCQueueSharedMeta::new(&owned.meta);
    /// let mut queue = YCQueue::new(shared, owned.data.as_mut_slice()).unwrap();
    ///
    /// let slots = queue.get_produce_slots(2, false).unwrap();
    /// assert_eq!(slots.len(), 2);
    /// queue.mark_slots_produced(slots).unwrap();
    ///
    /// let partial = queue.get_produce_slots(2, true).unwrap();
    /// assert!(!partial.is_empty());
    /// queue.mark_slots_produced(partial).unwrap();
    /// ```
    #[inline]
    pub fn get_produce_slots(
        &mut self,
        mut num_slots: u16,
        best_effort: bool,
    ) -> Result<Vec<YCQueueProduceSlot<'a>>, YCQueueError> {
        if num_slots == 0 || num_slots > self.slot_count {
            return Err(YCQueueError::InvalidArgs);
        }

        let requested_slots = num_slots;
        let mut refreshed = false;
        let old_producer = loop {
            let producer = self.refresh_producer_cursor();

            // first check available slots against cached consumer, if it's too stale we re-try
            let mut available_slots = match self.slots_available_cached() {
                Some(slots) => slots,
                None => {
                    if !refreshed {
                        self.refresh_consumer_cursor();
                        refreshed = true;
                        num_slots = requested_slots;
                        continue;
                    } else {
                        return Err(YCQueueError::OutOfSpace);
                    }
                }
            };

            if best_effort {
                num_slots = num_slots.min(available_slots).max(1);
            }

            if num_slots > available_slots {
                if !refreshed {
                    self.refresh_consumer_cursor();
                    refreshed = true;
                    num_slots = requested_slots;
                    continue;
                } else {
                    return Err(YCQueueError::OutOfSpace);
                }
            }

            available_slots = self.check_owner(
                self.cursor_index(producer),
                num_slots,
                YCQueueOwner::Producer,
            );

            if num_slots > available_slots {
                if best_effort && available_slots > 0 {
                    num_slots = available_slots;
                } else {
                    return Err(YCQueueError::SlotNotReady);
                }
            }

            debug_assert!(available_slots > 0);
            debug_assert!(available_slots <= num_slots);
            num_slots = available_slots;

            let new_producer = cursor_advance(producer, available_slots);

            match self.shared_metadata.producer_cursor.compare_exchange(
                producer,
                new_producer,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.cached_producer_cursor = new_producer;
                    break producer;
                }
                Err(_) => continue,
            }
        };

        let mut slots = Vec::with_capacity(num_slots as usize);
        let mut cursor = old_producer;
        for _ in 0..num_slots {
            let index = self.cursor_index(cursor);
            debug_assert_eq!(self.get_owner(index), YCQueueOwner::Producer);

            let slot_data = self.slots[index as usize].take();
            match slot_data {
                Some(data) => slots.push(YCQueueProduceSlot { index, data }),
                None => panic!("We double-loaned out produce index {index:?}"),
            }

            cursor = cursor_advance(cursor, 1);
        }

        Ok(slots)
    }

    /// Reserve a single slot to produce into.
    ///
    /// # Returns
    /// `Ok` with the next available `YCQueueProduceSlot` when space exists.
    ///
    /// # Errors
    /// Propagates the same errors as [`get_produce_slots`](Self::get_produce_slots) when no capacity is available.
    ///
    /// # Examples
    /// ```
    /// use yep_coc::queue_alloc_helpers::YCQueueOwnedData;
    /// use yep_coc::{YCQueue, YCQueueSharedMeta};
    ///
    /// let mut owned = YCQueueOwnedData::new(2, 16);
    /// let shared = YCQueueSharedMeta::new(&owned.meta);
    /// let mut queue = YCQueue::new(shared, owned.data.as_mut_slice()).unwrap();
    ///
    /// // Reserve a slot
    /// let slot = queue.get_produce_slot().unwrap();
    /// // Fill it with data
    /// slot.data.fill(0xAB);
    /// ```
    #[inline]
    pub fn get_produce_slot(&mut self) -> Result<YCQueueProduceSlot<'a>, YCQueueError> {
        let mut slots = self.get_produce_slots(1, false)?;

        Ok(slots
            .pop()
            .expect("get_produce_slots(1, false) returned without a slot"))
    }

    /// Mark a slot as produced into. This makes it available to consumers.
    ///
    /// # Arguments
    /// * `queue_slot` - Slot previously obtained from [`get_produce_slot`](Self::get_produce_slot) or [`get_produce_slots`](Self::get_produce_slots).
    ///
    /// # Returns
    /// `Ok(())` when the slot is successfully handed off to consumers.
    ///
    /// # Errors
    /// Returns `YCQueueError::InvalidArgs` when the slot has an unexpected size.
    ///
    /// # Examples
    /// ```
    /// use yep_coc::queue_alloc_helpers::YCQueueOwnedData;
    /// use yep_coc::{YCQueue, YCQueueSharedMeta};
    ///
    /// let mut owned = YCQueueOwnedData::new(2, 16);
    /// let shared = YCQueueSharedMeta::new(&owned.meta);
    /// let mut queue = YCQueue::new(shared, owned.data.as_mut_slice()).unwrap();
    ///
    /// let slot = queue.get_produce_slot().unwrap();
    /// queue.mark_slot_produced(slot).unwrap();
    /// assert_eq!(queue.in_flight_count(), 1);
    /// ```
    pub fn mark_slot_produced(
        &mut self,
        queue_slot: YCQueueProduceSlot<'a>,
    ) -> Result<(), YCQueueError> {
        /*
         * Marking a slot as produced gives it to the consumer to consume. These are not required
         * to happen in the same order the slots were reserved. This updates the in-flight count.
         */

        if queue_slot.data.len() != self.slot_size as usize {
            return Err(YCQueueError::InvalidArgs);
        }

        // yoink back the slot data
        let produce_idx = queue_slot.index;
        let old_data = self.slots[produce_idx as usize].replace(queue_slot.data);

        debug_assert_eq!(old_data, None);

        // update the bitfield.
        let old_owner = self.set_owner(produce_idx, YCQueueOwner::Consumer);
        debug_assert_eq!(old_owner, YCQueueOwner::Producer);

        Ok(())
    }

    /// Mark multiple slots as produced into. This makes them available to consumers.
    ///
    /// # Arguments
    /// * `queue_slots` - Contiguous slots previously obtained from [`get_produce_slots`](Self::get_produce_slots).
    ///
    /// # Returns
    /// `Ok(())` when ownership is returned to consumers.
    ///
    /// # Errors
    /// Returns `YCQueueError::InvalidArgs` when the slots are empty, non-contiguous, exceed capacity, or hold slices of the wrong length.
    ///
    /// # Examples
    /// ```
    /// use yep_coc::queue_alloc_helpers::YCQueueOwnedData;
    /// use yep_coc::{YCQueue, YCQueueSharedMeta};
    ///
    /// let mut owned = YCQueueOwnedData::new(4, 16);
    /// let shared = YCQueueSharedMeta::new(&owned.meta);
    /// let mut queue = YCQueue::new(shared, owned.data.as_mut_slice()).unwrap();
    ///
    /// let slots = queue.get_produce_slots(4, false).unwrap();
    /// queue.mark_slots_produced(slots).unwrap();
    /// assert_eq!(queue.in_flight_count(), 4);
    /// ```
    pub fn mark_slots_produced(
        &mut self,
        queue_slots: Vec<YCQueueProduceSlot<'a>>,
    ) -> Result<(), YCQueueError> {
        if queue_slots.is_empty() {
            return Ok(());
        }

        let slot_size = self.slot_size as usize;
        let count = queue_slots.len();
        if count > self.slot_count as usize {
            return Err(YCQueueError::InvalidArgs);
        }

        let start_index = queue_slots[0].index;
        let slot_count = self.slot_count as usize;

        for (offset, slot) in queue_slots.iter().enumerate() {
            if slot.data.len() != slot_size {
                return Err(YCQueueError::InvalidArgs);
            }

            let expected = ((start_index as usize + offset) % slot_count) as u16;
            if slot.index != expected {
                return Err(YCQueueError::InvalidArgs);
            }
        }

        for slot in queue_slots.into_iter() {
            let old_data = self.slots[slot.index as usize].replace(slot.data);
            debug_assert!(old_data.is_none());
        }

        self.set_owner_range(start_index, count as u16, YCQueueOwner::Consumer)
    }

    /// Reserve contiguous slots for consumers, optionally in best-effort mode.
    ///
    /// When `best_effort` is `false`, the function succeeds only if all `num_slots` are ready;
    /// otherwise it returns as many contiguous slots as currently published.
    ///
    /// # Arguments
    /// * `num_slots` - Maximum number of contiguous slots to attempt to dequeue.
    /// * `best_effort` - When `true`, grants a partial batch instead of requiring `num_slots`.
    ///
    /// # Returns
    /// `Ok` containing one or more ready slots when the reservation succeeds.
    ///
    /// # Errors
    /// Returns `YCQueueError::InvalidArgs` when `num_slots` is zero or exceeds capacity,
    /// `YCQueueError::EmptyQueue` when nothing has been produced yet, and
    /// `YCQueueError::SlotNotReady` when the requested slots have not all been published.
    ///
    /// # Examples
    /// ```
    /// use yep_coc::queue_alloc_helpers::YCQueueOwnedData;
    /// use yep_coc::{YCQueue, YCQueueSharedMeta};
    ///
    /// let mut owned = YCQueueOwnedData::new(4, 16);
    /// let shared = YCQueueSharedMeta::new(&owned.meta);
    /// let mut queue = YCQueue::new(shared, owned.data.as_mut_slice()).unwrap();
    ///
    /// let produce = queue.get_produce_slots(4, false).unwrap();
    /// queue.mark_slots_produced(produce).unwrap();
    ///
    /// let consume = queue.get_consume_slots(2, false).unwrap();
    /// assert_eq!(consume.len(), 2);
    ///
    /// let partial = queue.get_consume_slots(2, true).unwrap();
    /// assert!(!partial.is_empty());
    /// ```
    #[inline]
    pub fn get_consume_slots(
        &mut self,
        mut num_slots: u16,
        best_effort: bool,
    ) -> Result<Vec<YCQueueConsumeSlot<'a>>, YCQueueError> {
        if num_slots == 0 || num_slots > self.slot_count {
            return Err(YCQueueError::InvalidArgs);
        }

        let requested_slots = num_slots;
        let mut refreshed = false;
        let old_consumer = loop {
            let consumer = self.refresh_consumer_cursor();

            // first check available slots against cached producer, if it's too stale we re-try
            let mut available_slots = match self.slots_used_cached() {
                Some(slots) => slots,
                None => {
                    if !refreshed {
                        self.refresh_producer_cursor();
                        refreshed = true;
                        num_slots = requested_slots;
                        continue;
                    } else {
                        return Err(YCQueueError::EmptyQueue);
                    }
                }
            };

            if best_effort {
                num_slots = num_slots.min(available_slots).max(1);
            }

            if num_slots > available_slots {
                if !refreshed {
                    self.refresh_producer_cursor();
                    refreshed = true;
                    num_slots = requested_slots;
                    continue;
                } else {
                    return Err(YCQueueError::EmptyQueue);
                }
            }

            available_slots = self.check_owner(
                self.cursor_index(consumer),
                num_slots,
                YCQueueOwner::Consumer,
            );
            if (!best_effort && available_slots != num_slots)
                || (best_effort && available_slots == 0)
            {
                return Err(YCQueueError::SlotNotReady);
            }
            if num_slots > available_slots {
                debug_assert!(best_effort);
                num_slots = available_slots;
            }

            debug_assert!(available_slots > 0);
            debug_assert!(available_slots <= num_slots);

            let next_consumer = cursor_advance(consumer, available_slots);

            match self.shared_metadata.consumer_cursor.compare_exchange(
                consumer,
                next_consumer,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.cached_consumer_cursor = next_consumer;
                    break consumer;
                }
                Err(_) => continue,
            }
        };

        let mut slots = Vec::with_capacity(num_slots as usize);
        let mut cursor = old_consumer;
        for _ in 0..num_slots {
            let index = self.cursor_index(cursor);
            debug_assert_eq!(self.get_owner(index), YCQueueOwner::Consumer);

            let slot_data = self.slots[index as usize].take();
            match slot_data {
                Some(data) => slots.push(YCQueueConsumeSlot { index, data }),
                None => panic!("We double-loaned out consume index {index:?}"),
            }

            cursor = cursor_advance(cursor, 1);
        }

        Ok(slots)
    }

    /// Reserve a single slot for consumption.
    ///
    /// # Returns
    /// `Ok` with the next ready `YCQueueConsumeSlot` when data is available.
    ///
    /// # Errors
    /// Propagates the same errors as [`get_consume_slots`](Self::get_consume_slots) when no slots are ready.
    ///
    /// # Examples
    /// ```
    /// use yep_coc::queue_alloc_helpers::YCQueueOwnedData;
    /// use yep_coc::{YCQueue, YCQueueSharedMeta};
    ///
    /// let mut owned = YCQueueOwnedData::new(2, 16);
    /// let shared = YCQueueSharedMeta::new(&owned.meta);
    /// let mut queue = YCQueue::new(shared, owned.data.as_mut_slice()).unwrap();
    ///
    /// let slot = queue.get_produce_slot().unwrap();
    /// queue.mark_slot_produced(slot).unwrap();
    ///
    /// let consume = queue.get_consume_slot().unwrap();
    /// assert_eq!(consume.index, 0);
    /// ```
    pub fn get_consume_slot(&mut self) -> Result<YCQueueConsumeSlot<'a>, YCQueueError> {
        let mut slots = self.get_consume_slots(1, false)?;

        Ok(slots
            .pop()
            .expect("get_consume_slots(1, false) returned without a slot"))
    }

    /// Return an individual consumption slot back to the producer pool.
    ///
    /// # Arguments
    /// * `queue_slot` - Slot previously obtained from [`get_consume_slot`](Self::get_consume_slot) or [`get_consume_slots`](Self::get_consume_slots).
    ///
    /// # Returns
    /// `Ok(())` when the slot is successfully reclaimed for producers.
    ///
    /// # Errors
    /// Returns `YCQueueError::InvalidArgs` when the slot data length is unexpected.
    ///
    /// # Examples
    /// ```
    /// use yep_coc::queue_alloc_helpers::YCQueueOwnedData;
    /// use yep_coc::{YCQueue, YCQueueSharedMeta};
    ///
    /// let mut owned = YCQueueOwnedData::new(2, 16);
    /// let shared = YCQueueSharedMeta::new(&owned.meta);
    /// let mut queue = YCQueue::new(shared, owned.data.as_mut_slice()).unwrap();
    ///
    /// let slot = queue.get_produce_slot().unwrap();
    /// queue.mark_slot_produced(slot).unwrap();
    /// let consume = queue.get_consume_slot().unwrap();
    /// queue.mark_slot_consumed(consume).unwrap();
    /// assert_eq!(queue.in_flight_count(), 0);
    /// ```
    pub fn mark_slot_consumed(
        &mut self,
        queue_slot: YCQueueConsumeSlot<'a>,
    ) -> Result<(), YCQueueError> {
        if queue_slot.data.len() != self.slot_size as usize {
            return Err(YCQueueError::InvalidArgs);
        }

        // yoink back the slot data
        let consume_idx = queue_slot.index;
        let old_data = self.slots[consume_idx as usize].replace(queue_slot.data);

        debug_assert_eq!(old_data, None);

        // update the bitfield now
        let old_owner = self.set_owner(consume_idx, YCQueueOwner::Producer);
        debug_assert_eq!(old_owner, YCQueueOwner::Consumer);

        Ok(())
    }

    /// Return multiple consumption slots back to the producer pool at once.
    ///
    /// # Arguments
    /// * `queue_slots` - Contiguous slots previously obtained from [`get_consume_slots`](Self::get_consume_slots).
    ///
    /// # Returns
    /// `Ok(())` when all slots are reclaimed for producers.
    ///
    /// # Errors
    /// Returns `YCQueueError::InvalidArgs` when any slot has an unexpected length, is out of order, or when the batch length exceeds the queue capacity.
    ///
    /// # Examples
    /// ```
    /// use yep_coc::queue_alloc_helpers::YCQueueOwnedData;
    /// use yep_coc::{YCQueue, YCQueueSharedMeta};
    ///
    /// let mut owned = YCQueueOwnedData::new(4, 16);
    /// let shared = YCQueueSharedMeta::new(&owned.meta);
    /// let mut queue = YCQueue::new(shared, owned.data.as_mut_slice()).unwrap();
    ///
    /// let produce = queue.get_produce_slots(4, false).unwrap();
    /// queue.mark_slots_produced(produce).unwrap();
    /// let consume = queue.get_consume_slots(4, false).unwrap();
    /// queue.mark_slots_consumed(consume).unwrap();
    /// assert_eq!(queue.in_flight_count(), 0);
    /// ```
    pub fn mark_slots_consumed(
        &mut self,
        queue_slots: Vec<YCQueueConsumeSlot<'a>>,
    ) -> Result<(), YCQueueError> {
        if queue_slots.is_empty() {
            return Ok(());
        }

        let slot_size = self.slot_size as usize;
        let count = queue_slots.len();
        if count > self.slot_count as usize {
            return Err(YCQueueError::InvalidArgs);
        }

        let start_index = queue_slots[0].index;
        let slot_count = self.slot_count as usize;

        for (offset, slot) in queue_slots.iter().enumerate() {
            if slot.data.len() != slot_size {
                return Err(YCQueueError::InvalidArgs);
            }

            let expected = ((start_index as usize + offset) % slot_count) as u16;
            if slot.index != expected {
                return Err(YCQueueError::InvalidArgs);
            }
        }

        for slot in queue_slots.into_iter() {
            let old_data = self.slots[slot.index as usize].replace(slot.data);
            debug_assert!(old_data.is_none());
        }

        self.set_owner_range(start_index, count as u16, YCQueueOwner::Producer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::YCQueueError;
    use crate::queue_alloc_helpers::YCQueueOwnedData;

    #[test]
    fn simple_produce_consume_test() {
        let slot_count: u16 = 4;
        let slot_size: u16 = 32;

        let owned = YCQueueOwnedData::new(slot_count, slot_size);
        let mut queue = YCQueue::from_owned_data(&owned).unwrap();

        assert_eq!(
            queue.check_owner(0, slot_count, YCQueueOwner::Producer),
            slot_count
        );
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.produce_idx(), 0);
        assert_eq!(queue.consume_idx(), 0);

        let slot = queue.get_produce_slot().unwrap();
        assert_eq!(slot.index, 0);
        assert_eq!(queue.produce_idx(), 1);
        assert_eq!(queue.in_flight_count(), 1);
        assert_eq!(queue.check_owner(0, 1, YCQueueOwner::Producer), 1u16);

        slot.data.fill(0xAB);
        queue.mark_slot_produced(slot).unwrap();
        assert_eq!(queue.in_flight_count(), 1);
        assert_eq!(queue.check_owner(0, 1, YCQueueOwner::Consumer), 1u16);

        let consume_slot = queue.get_consume_slot().unwrap();
        assert_eq!(consume_slot.index, 0);
        assert_eq!(queue.consume_idx(), 1);
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.check_owner(0, 1, YCQueueOwner::Consumer), 1u16);

        queue.mark_slot_consumed(consume_slot).unwrap();
        assert_eq!(
            queue.check_owner(0, slot_count, YCQueueOwner::Producer),
            slot_count
        );
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.produce_idx(), 1);
        assert_eq!(queue.consume_idx(), 1);
    }

    #[test]
    fn batched_produce_consume_test() {
        let slot_count: u16 = 8;
        let slot_size: u16 = 64;

        let owned = YCQueueOwnedData::new(slot_count, slot_size);
        let mut queue = YCQueue::from_owned_data(&owned).unwrap();

        assert_eq!(
            queue.check_owner(0, slot_count, YCQueueOwner::Producer),
            slot_count
        );
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.produce_idx(), 0);
        assert_eq!(queue.consume_idx(), 0);

        let produce_batch = 3;
        let produce_slots = queue.get_produce_slots(produce_batch, false).unwrap();
        let produced_indices: Vec<_> = produce_slots.iter().map(|slot| slot.index).collect();
        assert_eq!(produced_indices, vec![0, 1, 2]);
        assert_eq!(queue.in_flight_count(), produce_batch);
        assert_eq!(queue.produce_idx(), produce_batch);
        assert_eq!(
            queue.check_owner(0, produce_batch, YCQueueOwner::Producer),
            produce_batch
        );

        queue.mark_slots_produced(produce_slots).unwrap();
        assert_eq!(
            queue.check_owner(0, produce_batch, YCQueueOwner::Consumer),
            produce_batch
        );
        assert_eq!(queue.in_flight_count(), produce_batch);

        let consume_slots_first = queue.get_consume_slots(2, false).unwrap();
        let consumed_indices: Vec<_> = consume_slots_first.iter().map(|slot| slot.index).collect();
        assert_eq!(consumed_indices, vec![0, 1]);
        assert_eq!(queue.consume_idx(), 2);
        assert_eq!(queue.in_flight_count(), 1);
        assert_eq!(queue.check_owner(0, 2, YCQueueOwner::Consumer), 2u16);
        assert_eq!(queue.check_owner(2, 1, YCQueueOwner::Consumer), 1u16);

        queue.mark_slots_consumed(consume_slots_first).unwrap();
        assert_eq!(queue.check_owner(0, 2, YCQueueOwner::Producer), 2u16);
        assert_eq!(queue.check_owner(2, 1, YCQueueOwner::Consumer), 1u16);
        assert_eq!(queue.in_flight_count(), 1);

        let final_slot = queue.get_consume_slots(1, false).unwrap();
        assert_eq!(final_slot[0].index, 2);
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.consume_idx(), 3);

        queue.mark_slots_consumed(final_slot).unwrap();
        assert_eq!(
            queue.check_owner(0, slot_count, YCQueueOwner::Producer),
            slot_count
        );
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.produce_idx(), 3);
        assert_eq!(queue.consume_idx(), 3);
    }

    #[test]
    fn in_flight_count_tracks_other_writers() {
        use std::sync::atomic::Ordering;

        let slot_count: u16 = 8;
        let slot_size: u16 = 16;

        let owned = YCQueueOwnedData::new(slot_count, slot_size);
        let mut queue = YCQueue::from_owned_data(&owned).unwrap();

        assert_eq!(queue.in_flight_count(), 0);

        queue
            .shared_metadata
            .producer_cursor
            .store(3, Ordering::Release);
        queue
            .shared_metadata
            .consumer_cursor
            .store(1, Ordering::Release);

        assert_eq!(queue.in_flight_count(), 2);
    }

    #[test]
    fn best_effort_produce_partial_batch() {
        let slot_count: u16 = 4;
        let slot_size: u16 = 32;

        let owned = YCQueueOwnedData::new(slot_count, slot_size);
        let mut queue = YCQueue::from_owned_data(&owned).unwrap();

        // Publish three slots so the consumer can hold one and block the wrap-around slot.
        let produce_slots = queue.get_produce_slots(3, false).unwrap();
        queue.mark_slots_produced(produce_slots).unwrap();

        // Hold one consumed slot to keep ownership on the far side of the ring.
        let mut consume_slots = queue.get_consume_slots(2, false).unwrap();
        let first = consume_slots.remove(0);
        queue.mark_slot_consumed(first).unwrap();
        let pending = consume_slots
            .pop()
            .expect("expect a remaining slot to stay in consumer hands");

        // Best-effort reservation should only hand out the contiguous run before the pending slot.
        let partial = queue.get_produce_slots(3, true).unwrap();
        assert_eq!(partial.len(), 2);
        assert_eq!(partial[0].index, 3);
        assert_eq!(partial[1].index, 0);

        // Return the slots to keep the queue consistent for the rest of the test harness.
        queue.mark_slots_produced(partial).unwrap();
        queue.mark_slot_consumed(pending).unwrap();

        let remaining = queue.get_consume_slots(3, false).unwrap();
        queue.mark_slots_consumed(remaining).unwrap();
    }

    #[test]
    fn best_effort_consume_partial_batch() {
        let slot_count: u16 = 4;
        let slot_size: u16 = 32;

        let owned = YCQueueOwnedData::new(slot_count, slot_size);
        let mut queue = YCQueue::from_owned_data(&owned).unwrap();

        let mut produce = queue.get_produce_slots(2, false).unwrap();
        let first_ready = produce.remove(0);
        let second_in_progress = produce
            .pop()
            .expect("second slot should be available for deferred publish");

        // No slots have been published yet, so best-effort should report a temporary stall.
        assert_eq!(
            queue.get_consume_slots(1, true).unwrap_err(),
            YCQueueError::SlotNotReady
        );

        queue.mark_slot_produced(first_ready).unwrap();

        // Only the published slot should be returned even though two were reserved.
        let mut consume = queue.get_consume_slots(2, true).unwrap();
        assert_eq!(consume.len(), 1);
        assert_eq!(consume[0].index, 0);

        // Clean up by finishing both outstanding slots.
        let ready = consume.pop().unwrap();
        queue.mark_slot_consumed(ready).unwrap();

        queue.mark_slot_produced(second_in_progress).unwrap();
        let leftover = queue.get_consume_slot().unwrap();
        queue.mark_slot_consumed(leftover).unwrap();
    }

    #[test]
    fn wrap_test() {
        let slot_count: u16 = 4;
        let slot_size: u16 = 32;

        let owned = YCQueueOwnedData::new(slot_count, slot_size);
        let mut queue = YCQueue::from_owned_data(&owned).unwrap();

        let initial_slots = queue.get_produce_slots(slot_count, false).unwrap();
        assert_eq!(queue.in_flight_count(), slot_count);
        assert_eq!(queue.produce_idx(), 0);

        queue.mark_slots_produced(initial_slots).unwrap();
        assert_eq!(
            queue.check_owner(0, slot_count, YCQueueOwner::Consumer),
            slot_count
        );

        let first_consumed = queue.get_consume_slots(slot_count, false).unwrap();
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.consume_idx(), 0);

        queue.mark_slots_consumed(first_consumed).unwrap();
        assert_eq!(
            queue.check_owner(0, slot_count, YCQueueOwner::Producer),
            slot_count
        );
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.produce_idx(), 0);
        assert_eq!(queue.consume_idx(), 0);

        let mut wrap_slots = queue.get_produce_slots(3, false).unwrap();
        let start_idx = wrap_slots[0].index;
        assert!(start_idx <= slot_count - 3 || start_idx == slot_count - 3);

        wrap_slots[0].data.fill(0xAA);
        wrap_slots[1].data.fill(0xBB);
        wrap_slots[2].data.fill(0xCC);

        queue.mark_slots_produced(wrap_slots).unwrap();
        assert_eq!(queue.in_flight_count(), 3);

        let consumed = queue.get_consume_slots(3, false).unwrap();
        let values: Vec<u8> = consumed.iter().map(|slot| slot.data[0]).collect();
        assert_eq!(values, vec![0xAA, 0xBB, 0xCC]);
        assert_eq!(queue.consume_idx(), (start_idx + 3) % slot_count);

        queue.mark_slots_consumed(consumed).unwrap();
        assert_eq!(
            queue.check_owner(0, slot_count, YCQueueOwner::Producer),
            slot_count
        );
        assert_eq!(queue.in_flight_count(), 0);
    }

    #[test]
    fn batched_produce_consume_crossing_word_boundaries() {
        let slot_count: u16 = 128;
        let slot_size: u16 = 16;
        let batch_size: u16 = 67;
        let iterations = 5;

        let owned = YCQueueOwnedData::new(slot_count, slot_size);
        let mut queue = YCQueue::from_owned_data(&owned).unwrap();

        assert_eq!(
            queue.check_owner(0, slot_count, YCQueueOwner::Producer),
            slot_count
        );
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.produce_idx(), 0);
        assert_eq!(queue.consume_idx(), 0);

        for iteration in 0..iterations {
            let expected_start = ((batch_size as usize * iteration) % slot_count as usize) as u16;
            assert_eq!(queue.produce_idx(), expected_start);

            let produce_slots = queue.get_produce_slots(batch_size, false).unwrap();
            assert_eq!(produce_slots.len(), batch_size as usize);
            assert_eq!(produce_slots[0].index, expected_start);

            for (offset, slot) in produce_slots.iter().enumerate() {
                let expected_index =
                    ((expected_start as usize + offset) % slot_count as usize) as u16;
                assert_eq!(slot.index, expected_index);
            }

            queue.mark_slots_produced(produce_slots).unwrap();
            assert_eq!(queue.in_flight_count(), batch_size);

            let consume_slots = queue.get_consume_slots(batch_size, false).unwrap();
            assert_eq!(consume_slots.len(), batch_size as usize);

            for (offset, slot) in consume_slots.iter().enumerate() {
                let expected_index =
                    ((expected_start as usize + offset) % slot_count as usize) as u16;
                assert_eq!(slot.index, expected_index);
            }

            queue.mark_slots_consumed(consume_slots).unwrap();

            let expected_idx =
                ((batch_size as usize * (iteration + 1)) % slot_count as usize) as u16;
            assert_eq!(queue.in_flight_count(), 0);
            assert_eq!(queue.produce_idx(), expected_idx);
            assert_eq!(queue.consume_idx(), expected_idx);
            assert_eq!(
                queue.check_owner(0, slot_count, YCQueueOwner::Producer),
                slot_count
            );
        }
    }
}
