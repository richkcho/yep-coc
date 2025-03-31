use std::sync::atomic::{AtomicU16, AtomicU64};

use crate::queue_alloc_helpers::YCQueueOwnedMeta;


/// shared data associated with the metadata portion of the circular queue. All of these types are references, 
/// as they should point to some shared memory region. 
pub struct YCQueueSharedMeta<'a> {
    pub(crate) slot_count: &'a AtomicU16,
    pub(crate) slot_size: &'a AtomicU16,
    pub(crate) u64_meta: &'a AtomicU64,
    /// bitmap representing who owns which slot in the queue. 0 -> producer, 1 -> consumer.
    pub(crate) ownership: &'a [AtomicU64],
    // TODO: implement EXPAND
    // "busy bit" to implement locking for segment growth
    // busy: &'a AtomicBool,
}

impl<'a> YCQueueSharedMeta<'a> {
    pub fn new(meta_ref: &YCQueueOwnedMeta) -> YCQueueSharedMeta {
        YCQueueSharedMeta {slot_count: &meta_ref.slot_count, slot_size: &meta_ref.slot_size, u64_meta: &meta_ref.produce_meta, ownership: &meta_ref.ownership}
    }
}

pub(crate) struct YCQueueU64Meta {
    /// where to currently produce into
    pub(crate) produce_idx: u16,
    // how many in-flight messages there are
    pub(crate) in_flight: u16,
    /// where to consume from
    pub(crate) consume_idx: u16,
}

impl YCQueueU64Meta {
    pub(crate) fn from_u64(value: u64) -> YCQueueU64Meta {
        let produce_idx = value as u16;
        let in_flight = (value >> u16::BITS) as u16;
        let consume_idx = (value >> (2 * u16::BITS)) as u16;

        YCQueueU64Meta {produce_idx, in_flight, consume_idx}
    }

    pub(crate) fn to_u64(&self) -> u64 {
        let mut value: u64 = self.consume_idx as u64;
        value <<= u16::BITS;
        value |= self.in_flight as u64;
        value <<= u16::BITS;
        value |= self.produce_idx as u64;

        value
    }
}

#[cfg(test)]
mod tests {
    use super::YCQueueU64Meta;

    #[test]
    fn test_produce_meta() {
        let mut meta: YCQueueU64Meta = YCQueueU64Meta::from_u64(0);

        assert_eq!(meta.produce_idx, 0);
        assert_eq!(meta.consume_idx, 0);
        assert_eq!(meta.in_flight, 0);
        assert_eq!(meta.to_u64(), 0);

        let test_produce_idx: u16 = 1;
        let test_consume_idx: u16 = 2;
        let test_in_flight: u16 = 3;

        meta.produce_idx = test_produce_idx;
        meta.consume_idx = test_consume_idx;
        meta.in_flight = test_in_flight;

        // create new meta as copy from u64
        let new_meta = YCQueueU64Meta::from_u64(meta.to_u64());

        // validate u64 rep is same
        assert_eq!(meta.to_u64(), new_meta.to_u64());

        // validate fields are also the same
        assert_eq!(meta.produce_idx, new_meta.produce_idx);
        assert_eq!(meta.consume_idx, new_meta.consume_idx);
        assert_eq!(meta.in_flight, new_meta.in_flight);
    }
}