use std::sync::atomic::{AtomicU16, AtomicU64};

pub struct YCQueueOwnedMeta {
    pub slot_count: AtomicU16,
    pub slot_size: AtomicU16,
    pub produce_meta: AtomicU64,
    pub ownership: Vec<AtomicU64>,
}

impl YCQueueOwnedMeta {
    pub fn new(slot_count_u16: u16, slot_size_u16: u16) -> YCQueueOwnedMeta {
        let slot_count = AtomicU16::new(slot_count_u16);
        let slot_size = AtomicU16::new(slot_size_u16);
        let produce_meta = AtomicU64::new(0);
        let mut ownership = Vec::<AtomicU64>::with_capacity((slot_count_u16 as usize + u64::BITS as usize - 1) / u64::BITS as usize);

        for _i in 0..ownership.capacity() {
            ownership.push(AtomicU64::new(0));
        }

        YCQueueOwnedMeta {slot_count, slot_size, produce_meta, ownership}
    }
}

/// A way to hold a YCQueueSharedMeta to share between threads of a particular rust program. Not designed for IPC. 
pub struct YCQueueData {
    pub meta: YCQueueOwnedMeta,
    pub data: Vec<u8>,
}

impl YCQueueData {
    pub fn new(slot_count_u16: u16, slot_size_u16: u16) -> YCQueueData {
        let meta = YCQueueOwnedMeta::new(slot_count_u16, slot_size_u16);
        let data = vec![0 as u8; (slot_count_u16 * slot_size_u16) as usize];

        YCQueueData {meta, data}
    }
}