use std::sync::atomic::{AtomicU16, AtomicU64};

/// shared data associated with the metadata portion of the circular queue. All of these types are references,
/// as they should point to some shared memory region.
#[derive(Copy, Clone, Debug)]
pub struct YCQueueSharedMeta<'a> {
    pub(crate) slot_count: &'a AtomicU16,
    pub(crate) slot_size: &'a AtomicU16,
    pub(crate) producer_cursor: &'a AtomicU64,
    pub(crate) consumer_cursor: &'a AtomicU64,
    /// bitmap representing who owns which slot in the queue. 0 -> producer, 1 -> consumer.
    pub(crate) ownership: &'a [AtomicU64],
}

pub(crate) type YCQueueCursor = u64;

#[inline]
pub(crate) fn cursor_advance(cursor: YCQueueCursor, delta: u16) -> YCQueueCursor {
    cursor + u64::from(delta)
}

#[inline]
pub(crate) fn cursor_index(cursor: YCQueueCursor, slot_size_exp: u16) -> u16 {
    (cursor as u16) & ((1 << slot_size_exp) - 1)
}

#[cfg(test)]
mod tests {
    use super::{YCQueueCursor, cursor_advance, cursor_index};

    #[test]
    fn cursor_round_trip() {
        let meta: YCQueueCursor = 7;
        let encoded = meta;
        let decoded = encoded;

        assert_eq!(decoded, meta);
    }

    #[test]
    fn cursor_advances() {
        let meta: YCQueueCursor = 15;
        let advanced = cursor_advance(meta, 5);

        assert_eq!(advanced, 20);
        assert_eq!(meta, 15);
    }

    #[test]
    fn cursor_indices_masked() {
        let meta: YCQueueCursor = 0b1011;
        assert_eq!(cursor_index(meta, 3), 0b011);
    }
}
