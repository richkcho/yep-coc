use std::{cell::Cell, sync::atomic::Ordering};

use crate::queue_meta::YCQueueU64Meta;

use crate::utils::get_bit;
use crate::{utils, YCQueueError, YCQueueSharedMeta};

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

#[derive(Eq, PartialEq, Debug)]
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

impl<'a> YCQueue <'a> {
    pub fn new(
        shared_metadata: YCQueueSharedMeta<'a>,
        data_region: &'a mut [u8],
    ) -> Result<YCQueue<'a>, YCQueueError> {
        let slot_size = shared_metadata.slot_size.load(Ordering::Acquire) as usize;
        let slot_count = data_region.len() / slot_size;

        if data_region.len() % slot_size != 0 {
            return Err(YCQueueError::InvalidArgs);
        }

        let mut slots = Vec::<Cell<Option<&'a mut [u8]>>>::with_capacity(slot_count);
        for slot in data_region.chunks_exact_mut(slot_size).into_iter() {
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
                Ok(_) => if get_bit(&value, bit_idx) {
                    return YCQueueOwner::Consumer;
                } else {
                    return YCQueueOwner::Producer;
                },
                Err(_) => continue,
            }
        }
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

    pub fn get_produce_slot(&mut self) -> Result<YCQueueProduceSlot<'a>, YCQueueError> {
        /*
         * In order to get a produce slot, we need to make sure it is marked as owned by producer 
         * (consumer has finished using it). Queue slots are reserved in-order but may be marked 
         * as ready for consumption out of order. 
         */

        // find out what produce idx we should be using
        let produce_idx: u16;
        loop {
            let value = self.shared_metadata.u64_meta.load(Ordering::Acquire);
            let mut meta = YCQueueU64Meta::from_u64(value);

            // quick check for full queue
            if meta.produce_idx == meta.consume_idx && (meta.in_flight > 0 || meta.produce_pending > 0) {
                return Err(YCQueueError::OutOfSpace);
            }

            // if the consumer still owns the current produce idx, this means the queue is full as well
            if self.get_owner(meta.produce_idx) != YCQueueOwner::Producer {
                return Err(YCQueueError::OutOfSpace);
            }

            let temp_produce_idx: u16 = meta.produce_idx;

            meta.produce_idx = (meta.produce_idx + 1) % self.slot_count;
            meta.produce_pending += 1;

            // update shared memory with new produce_idx
            let new_value = meta.to_u64();
            match self.shared_metadata.u64_meta.compare_exchange(value, new_value, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => (),
                Err(_) => continue,
            }

            produce_idx = temp_produce_idx;
            break;
        }

        debug_assert!(self.get_owner(produce_idx) == YCQueueOwner::Producer);

        // hand out the slice that corresponds to this slot
        let slot_data = self.slots[produce_idx as usize].replace(None);
        match slot_data {
            Some(data) => return Ok(YCQueueProduceSlot {index: produce_idx, data}),
            None => panic!("We double-loaned out produce index {:?}", produce_idx),
        }
    }

    pub fn mark_slot_produced(&mut self, queue_slot: YCQueueProduceSlot<'a>) -> Option<YCQueueError> {
        /*
         * Marking a slot as produced gives it to the consumer to consume. These are not required 
         * to happen in the same order the slots were reserved. This updates the in-flight count.
         */
        
        if queue_slot.data.len() != self.slot_size as usize {
            return Some(YCQueueError::InvalidArgs);
        }

        // yoink back the slot data
        let produce_idx = queue_slot.index;
        let old_data = self.slots[produce_idx as usize].replace(Some(queue_slot.data));

        assert!(old_data == None);
        
        // update the bitfield. 
        let old_owner = self.set_owner(produce_idx, YCQueueOwner::Consumer);
        assert_eq!(old_owner, YCQueueOwner::Producer);

        // update the in-flight count
        loop {
            let value = self.shared_metadata.u64_meta.load(Ordering::Acquire);
            let mut meta = YCQueueU64Meta::from_u64(value);

            meta.in_flight += 1;

            assert!(meta.produce_pending >= 1);
            meta.produce_pending -= 1;

            assert!(meta.in_flight <= self.slot_count);

            let new_value = meta.to_u64();
            match self.shared_metadata.u64_meta.compare_exchange(value, new_value, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => (),
                Err(_) => continue,
            }

            break;
        }

        None
    }

    pub fn get_consume_slot(&mut self) -> Result<YCQueueConsumeSlot<'a>, YCQueueError> {
        // find out what consume idx we should be using
        let consume_idx: u16;
        loop {
            let value = self.shared_metadata.u64_meta.load(Ordering::Acquire);
            let mut meta = YCQueueU64Meta::from_u64(value);

            if meta.in_flight == 0 {
                return Err(YCQueueError::EmptyQueue);
            }

            let temp_consume_idx: u16 = meta.consume_idx;

            meta.consume_idx = (meta.consume_idx + 1) % self.slot_count;
            meta.in_flight -= 1;

            let new_value = meta.to_u64();
            match self.shared_metadata.u64_meta.compare_exchange(value, new_value, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => (),
                Err(_) => continue,
            }

            consume_idx = temp_consume_idx;
            break;
        }

        let slot_data = self.slots[consume_idx as usize].replace(None);
        match slot_data {
            Some(data) => return Ok(YCQueueConsumeSlot {index: consume_idx, data}),
            None => panic!("We double-loaned out consume index {:?}", consume_idx),
        }
    }

    pub fn mark_slot_consumed(&mut self, queue_slot: YCQueueConsumeSlot<'a>) -> Option<YCQueueError> {
        if queue_slot.data.len() != self.slot_size as usize {
            return Some(YCQueueError::InvalidArgs);
        }

        // yoink back the slot data
        let consume_idx = queue_slot.index;
        let old_data = self.slots[consume_idx as usize].replace(Some(queue_slot.data));

        assert!(old_data == None);

        // update the bitfield now
        let old_owner = self.set_owner(consume_idx, YCQueueOwner::Producer);
        assert_eq!(old_owner, YCQueueOwner::Consumer);

        None
    }

}