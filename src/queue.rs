use std::{cell::Cell, sync::atomic::Ordering};

use crate::queue_meta::YCQueueU64Meta;

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
    slots: Vec<Cell<Option<&'a mut [u8]>>>,
    slot_count: u16,
    slot_size: u16,
}

impl<'a> YCQueue<'a> {
    pub fn new(
        shared_metadata: YCQueueSharedMeta<'a>,
        data_region: &'a mut [u8],
    ) -> Result<YCQueue<'a>, YCQueueError> {
        let slot_size = shared_metadata.slot_size.load(Ordering::Acquire) as usize;
        let slot_count = data_region.len() / slot_size;

        if !data_region.len().is_multiple_of(slot_size) {
            return Err(YCQueueError::InvalidArgs);
        }

        let mut slots = Vec::<Cell<Option<&'a mut [u8]>>>::with_capacity(slot_count);
        for slot in data_region.chunks_exact_mut(slot_size) {
            slots.push(Cell::new(Some(slot)));
        }

        if shared_metadata.slot_count.load(Ordering::Acquire) as usize != slot_count {
            return Err(YCQueueError::InvalidArgs);
        }

        Ok(YCQueue {
            shared_metadata,
            slots,
            slot_count: slot_count as u16,
            slot_size: slot_size as u16,
        })
    }

    fn check_owner(&self, idx: u16, range: u16, owner: YCQueueOwner) -> bool {
        if range == 0 {
            return true;
        }
        if idx >= self.slot_count || range > self.slot_count {
            return false;
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
            let span = remaining.min(bits_available);
            debug_assert!(span > 0);

            let mask = if span == u64::BITS {
                !0u64
            } else {
                ((1u64 << span) - 1) << bit_offset
            };
            let value = self.shared_metadata.ownership[chunk_idx].load(Ordering::Acquire);
            match owner {
                YCQueueOwner::Producer => {
                    if (value & mask) != 0 {
                        return false;
                    }
                }
                YCQueueOwner::Consumer => {
                    if (value & mask) != mask {
                        return false;
                    }
                }
            }
            remaining -= span;
            current += span;
        }
        true
    }

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

    fn get_u64_meta(&self) -> YCQueueU64Meta {
        YCQueueU64Meta::from_u64(self.shared_metadata.u64_meta.load(Ordering::Acquire))
    }

    pub fn in_flight_count(&self) -> u16 {
        self.get_u64_meta().in_flight
    }

    pub fn produce_idx(&self) -> u16 {
        self.get_u64_meta().produce_idx
    }

    pub fn consume_idx(&self) -> u16 {
        self.get_u64_meta().consume_idx
    }

    pub fn get_produce_slots(
        &mut self,
        num_slots: u16,
    ) -> Result<Vec<YCQueueProduceSlot<'a>>, YCQueueError> {
        if num_slots == 0 || num_slots > self.slot_count {
            return Err(YCQueueError::InvalidArgs);
        }

        let start_index = loop {
            let value = self.shared_metadata.u64_meta.load(Ordering::Acquire);
            let mut meta = YCQueueU64Meta::from_u64(value);

            if meta.in_flight as u32 + num_slots as u32 > self.slot_count as u32 {
                return Err(YCQueueError::OutOfSpace);
            }

            // make sure all the slots we want are owned by the producer
            if !self.check_owner(meta.produce_idx, num_slots, YCQueueOwner::Producer) {
                return Err(YCQueueError::SlotNotReady);
            }

            let produce_idx = meta.produce_idx;
            meta.in_flight += num_slots;
            meta.produce_idx += num_slots;
            // wrap around if needed
            if meta.produce_idx >= self.slot_count {
                // TODO: tag as cold path
                meta.produce_idx -= self.slot_count;
            }

            let new_value = meta.to_u64();
            match self.shared_metadata.u64_meta.compare_exchange(
                value,
                new_value,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break produce_idx,
                Err(_) => continue,
            }
        };

        let mut slots = Vec::with_capacity(num_slots as usize);
        let mut index = start_index;
        for _ in 0..num_slots {
            debug_assert_eq!(self.get_owner(index), YCQueueOwner::Producer);

            let slot_data = self.slots[index as usize].replace(None);
            match slot_data {
                Some(data) => slots.push(YCQueueProduceSlot { index, data }),
                None => panic!("We double-loaned out produce index {:?}", index),
            }

            index += 1;
            // wrap around if needed
            if index >= self.slot_count {
                // TODO: tag as cold path
                index -= self.slot_count;
            }
        }

        Ok(slots)
    }

    pub fn get_produce_slot(&mut self) -> Result<YCQueueProduceSlot<'a>, YCQueueError> {
        let mut slots = self.get_produce_slots(1)?;

        Ok(slots
            .pop()
            .expect("get_produce_slots(1) returned without a slot"))
    }

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
        let old_data = self.slots[produce_idx as usize].replace(Some(queue_slot.data));

        debug_assert_eq!(old_data, None);

        // update the bitfield.
        let old_owner = self.set_owner(produce_idx, YCQueueOwner::Consumer);
        debug_assert_eq!(old_owner, YCQueueOwner::Producer);

        Ok(())
    }

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
            let old_data = self.slots[slot.index as usize].replace(Some(slot.data));
            debug_assert!(old_data.is_none());
        }

        self.set_owner_range(start_index, count as u16, YCQueueOwner::Consumer)
    }

    pub fn get_consume_slots(
        &mut self,
        num_slots: u16,
    ) -> Result<Vec<YCQueueConsumeSlot<'a>>, YCQueueError> {
        if num_slots == 0 || num_slots > self.slot_count {
            return Err(YCQueueError::InvalidArgs);
        }

        let start_index = loop {
            let value = self.shared_metadata.u64_meta.load(Ordering::Acquire);
            let mut meta = YCQueueU64Meta::from_u64(value);

            if meta.in_flight < num_slots {
                return Err(YCQueueError::EmptyQueue);
            }

            if !self.check_owner(meta.consume_idx, num_slots, YCQueueOwner::Consumer) {
                return Err(YCQueueError::SlotNotReady);
            }

            let consume_idx = meta.consume_idx;
            let mut next_idx = consume_idx as u32 + num_slots as u32;
            if next_idx >= self.slot_count as u32 {
                next_idx -= self.slot_count as u32;
            }
            meta.consume_idx = next_idx as u16;
            meta.in_flight -= num_slots;

            let new_value = meta.to_u64();
            match self.shared_metadata.u64_meta.compare_exchange(
                value,
                new_value,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break consume_idx,
                Err(_) => continue,
            }
        };

        let mut slots = Vec::with_capacity(num_slots as usize);
        let mut index = start_index;
        for _ in 0..num_slots {
            debug_assert_eq!(self.get_owner(index), YCQueueOwner::Consumer);

            let slot_data = self.slots[index as usize].replace(None);
            match slot_data {
                Some(data) => slots.push(YCQueueConsumeSlot { index, data }),
                None => panic!("We double-loaned out consume index {:?}", index),
            }

            index += 1;
            if index >= self.slot_count {
                index -= self.slot_count;
            }
        }

        Ok(slots)
    }

    pub fn get_consume_slot(&mut self) -> Result<YCQueueConsumeSlot<'a>, YCQueueError> {
        let mut slots = self.get_consume_slots(1)?;

        Ok(slots
            .pop()
            .expect("get_consume_slots(1) returned without a slot"))
    }

    pub fn mark_slot_consumed(
        &mut self,
        queue_slot: YCQueueConsumeSlot<'a>,
    ) -> Result<(), YCQueueError> {
        if queue_slot.data.len() != self.slot_size as usize {
            return Err(YCQueueError::InvalidArgs);
        }

        // yoink back the slot data
        let consume_idx = queue_slot.index;
        let old_data = self.slots[consume_idx as usize].replace(Some(queue_slot.data));

        debug_assert_eq!(old_data, None);

        // update the bitfield now
        let old_owner = self.set_owner(consume_idx, YCQueueOwner::Producer);
        debug_assert_eq!(old_owner, YCQueueOwner::Consumer);

        Ok(())
    }

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
            let old_data = self.slots[slot.index as usize].replace(Some(slot.data));
            debug_assert!(old_data.is_none());
        }

        self.set_owner_range(start_index, count as u16, YCQueueOwner::Producer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::YCQueueSharedMeta;
    use crate::queue_alloc_helpers::YCQueueOwnedData;

    #[test]
    fn simple_produce_consume_test() {
        let slot_count: u16 = 4;
        let slot_size: u16 = 32;

        let mut owned = YCQueueOwnedData::new(slot_count, slot_size);
        let shared_meta = YCQueueSharedMeta::new(&owned.meta);
        let mut queue = YCQueue::new(shared_meta, owned.data.as_mut_slice()).unwrap();

        assert!(queue.check_owner(0, slot_count, YCQueueOwner::Producer));
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.produce_idx(), 0);
        assert_eq!(queue.consume_idx(), 0);

        let slot = queue.get_produce_slot().unwrap();
        assert_eq!(slot.index, 0);
        assert_eq!(queue.produce_idx(), 1);
        assert_eq!(queue.in_flight_count(), 1);
        assert!(queue.check_owner(0, 1, YCQueueOwner::Producer));

        slot.data.fill(0xAB);
        queue.mark_slot_produced(slot).unwrap();
        assert_eq!(queue.in_flight_count(), 1);
        assert!(queue.check_owner(0, 1, YCQueueOwner::Consumer));

        let consume_slot = queue.get_consume_slot().unwrap();
        assert_eq!(consume_slot.index, 0);
        assert_eq!(queue.consume_idx(), 1);
        assert_eq!(queue.in_flight_count(), 0);
        assert!(queue.check_owner(0, 1, YCQueueOwner::Consumer));

        queue.mark_slot_consumed(consume_slot).unwrap();
        assert!(queue.check_owner(0, slot_count, YCQueueOwner::Producer));
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.produce_idx(), 1);
        assert_eq!(queue.consume_idx(), 1);
    }

    #[test]
    fn batched_produce_consume_test() {
        let slot_count: u16 = 8;
        let slot_size: u16 = 64;

        let mut owned = YCQueueOwnedData::new(slot_count, slot_size);
        let shared_meta = YCQueueSharedMeta::new(&owned.meta);
        let mut queue = YCQueue::new(shared_meta, owned.data.as_mut_slice()).unwrap();

        assert!(queue.check_owner(0, slot_count, YCQueueOwner::Producer));
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.produce_idx(), 0);
        assert_eq!(queue.consume_idx(), 0);

        let produce_batch = 3;
        let produce_slots = queue.get_produce_slots(produce_batch).unwrap();
        let produced_indices: Vec<_> = produce_slots.iter().map(|slot| slot.index).collect();
        assert_eq!(produced_indices, vec![0, 1, 2]);
        assert_eq!(queue.in_flight_count(), produce_batch);
        assert_eq!(queue.produce_idx(), produce_batch);
        assert!(queue.check_owner(0, produce_batch, YCQueueOwner::Producer));

        queue.mark_slots_produced(produce_slots).unwrap();
        assert!(queue.check_owner(0, produce_batch, YCQueueOwner::Consumer));
        assert_eq!(queue.in_flight_count(), produce_batch);

        let consume_slots_first = queue.get_consume_slots(2).unwrap();
        let consumed_indices: Vec<_> = consume_slots_first.iter().map(|slot| slot.index).collect();
        assert_eq!(consumed_indices, vec![0, 1]);
        assert_eq!(queue.consume_idx(), 2);
        assert_eq!(queue.in_flight_count(), 1);
        assert!(queue.check_owner(0, 2, YCQueueOwner::Consumer));
        assert!(queue.check_owner(2, 1, YCQueueOwner::Consumer));

        queue.mark_slots_consumed(consume_slots_first).unwrap();
        assert!(queue.check_owner(0, 2, YCQueueOwner::Producer));
        assert!(queue.check_owner(2, 1, YCQueueOwner::Consumer));
        assert_eq!(queue.in_flight_count(), 1);

        let final_slot = queue.get_consume_slots(1).unwrap();
        assert_eq!(final_slot[0].index, 2);
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.consume_idx(), 3);

        queue.mark_slots_consumed(final_slot).unwrap();
        assert!(queue.check_owner(0, slot_count, YCQueueOwner::Producer));
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.produce_idx(), 3);
        assert_eq!(queue.consume_idx(), 3);
    }

    #[test]
    fn wrap_test() {
        let slot_count: u16 = 4;
        let slot_size: u16 = 32;

        let mut owned = YCQueueOwnedData::new(slot_count, slot_size);
        let shared_meta = YCQueueSharedMeta::new(&owned.meta);
        let mut queue = YCQueue::new(shared_meta, owned.data.as_mut_slice()).unwrap();

        let initial_slots = queue.get_produce_slots(slot_count).unwrap();
        assert_eq!(queue.in_flight_count(), slot_count);
        assert_eq!(queue.produce_idx(), 0);

        queue.mark_slots_produced(initial_slots).unwrap();
        assert!(queue.check_owner(0, slot_count, YCQueueOwner::Consumer));

        let first_consumed = queue.get_consume_slots(slot_count).unwrap();
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.consume_idx(), 0);

        queue.mark_slots_consumed(first_consumed).unwrap();
        assert!(queue.check_owner(0, slot_count, YCQueueOwner::Producer));
        assert_eq!(queue.in_flight_count(), 0);
        assert_eq!(queue.produce_idx(), 0);
        assert_eq!(queue.consume_idx(), 0);

        let mut wrap_slots = queue.get_produce_slots(3).unwrap();
        let start_idx = wrap_slots[0].index;
        assert!(start_idx <= slot_count - 3 || start_idx == slot_count - 3);

        wrap_slots[0].data.fill(0xAA);
        wrap_slots[1].data.fill(0xBB);
        wrap_slots[2].data.fill(0xCC);

        queue.mark_slots_produced(wrap_slots).unwrap();
        assert_eq!(queue.in_flight_count(), 3);

        let consumed = queue.get_consume_slots(3).unwrap();
        let values: Vec<u8> = consumed.iter().map(|slot| slot.data[0]).collect();
        assert_eq!(values, vec![0xAA, 0xBB, 0xCC]);
        assert_eq!(queue.consume_idx(), (start_idx + 3) % slot_count);

        queue.mark_slots_consumed(consumed).unwrap();
        assert!(queue.check_owner(0, slot_count, YCQueueOwner::Producer));
        assert_eq!(queue.in_flight_count(), 0);
    }
}
