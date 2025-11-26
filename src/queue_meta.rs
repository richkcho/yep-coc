use std::sync::atomic::{AtomicU16, AtomicU64};

/// shared data associated with the metadata portion of the circular queue. All of these types are references,
/// as they should point to some shared memory region.
#[derive(Copy, Clone, Debug)]
pub struct YCQueueSharedMeta<'a> {
    pub(crate) slot_count: &'a AtomicU16,
    pub(crate) slot_size: &'a AtomicU16,
    pub(crate) u64_meta: &'a AtomicU64,
    /// bitmap representing who owns which slot in the queue. 0 -> producer, 1 -> consumer.
    pub(crate) ownership: &'a [AtomicU64],
}

/// Producer and consumer counters live here. This is all in one atomic u64 to allow for atomic updates of all
/// fields at once. This has the drawback that the fields occupy the same cache line. However, for a produce +
/// consume operation, both producer and consumer will need to synchronize on both producer and consumer counters,
/// so splitting it into separate cache lines may not be beneficial. It's likely hw dependent, but I don't know
/// without data first.
/// TODO: revisit this decision once I have the benchmarks and data.
#[derive(Copy, Clone, Debug)]
pub(crate) struct YCQueueU64Meta {
    /// where to currently produce into
    pub(crate) produce_idx: u16,
    // how many in-flight messages there are
    pub(crate) in_flight: u16,
    // slot_count is needed to compute consume_idx
    slot_count: u16,
}

impl YCQueueU64Meta {
    pub(crate) fn from_u64(value: u64, slot_count: u16) -> YCQueueU64Meta {
        let produce_idx = value as u16;
        let in_flight = (value >> u16::BITS) as u16;

        YCQueueU64Meta {
            produce_idx,
            in_flight,
            slot_count,
        }
    }

    pub(crate) fn to_u64(self) -> u64 {
        let mut value: u64 = self.in_flight as u64;
        value <<= u16::BITS;
        value |= self.produce_idx as u64;

        value
    }

    /// Compute the consumer index from producer index and in-flight count.
    /// This is computed as: consume_idx = (produce_idx - in_flight) % slot_count
    /// with proper handling of wrapping arithmetic.
    pub(crate) fn consume_idx(&self) -> u16 {
        let produce_idx = self.produce_idx as u32;
        let in_flight = self.in_flight as u32;
        let slot_count = self.slot_count as u32;

        ((produce_idx + slot_count - in_flight) % slot_count) as u16
    }
}

#[cfg(test)]
mod tests {
    use super::YCQueueU64Meta;

    #[test]
    fn test_u64_meta() {
        let slot_count: u16 = 10;
        let mut meta: YCQueueU64Meta = YCQueueU64Meta::from_u64(0, slot_count);

        assert_eq!(meta.produce_idx, 0);
        assert_eq!(meta.consume_idx(), 0);
        assert_eq!(meta.in_flight, 0);
        assert_eq!(meta.to_u64(), 0);

        let test_produce_idx: u16 = 5;
        let test_in_flight: u16 = 3;

        meta.produce_idx = test_produce_idx;
        meta.in_flight = test_in_flight;

        // consume_idx should be computed as (5 - 3) = 2
        assert_eq!(meta.consume_idx(), 2);

        // create new meta as copy from u64
        let new_meta = YCQueueU64Meta::from_u64(meta.to_u64(), slot_count);

        // validate u64 rep is same
        assert_eq!(meta.to_u64(), new_meta.to_u64());

        // validate fields are also the same
        assert_eq!(meta.produce_idx, new_meta.produce_idx);
        assert_eq!(meta.consume_idx(), new_meta.consume_idx());
        assert_eq!(meta.in_flight, new_meta.in_flight);

        // Test wrapping case: produce_idx < in_flight
        meta.produce_idx = 2;
        meta.in_flight = 5;
        // consume_idx should be (10 - (5 - 2)) = 10 - 3 = 7
        assert_eq!(meta.consume_idx(), 7);
    }
}
