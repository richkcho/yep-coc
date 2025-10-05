use std::sync::atomic::{AtomicU16, AtomicU64};

use crate::{YCQueueError, YCQueueSharedMeta};

#[derive(Debug)]
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

impl<'a> YCQueueSharedMeta<'a> {
    pub fn new(meta_ref: &YCQueueOwnedMeta) -> YCQueueSharedMeta {
        YCQueueSharedMeta {slot_count: &meta_ref.slot_count, slot_size: &meta_ref.slot_size, u64_meta: &meta_ref.produce_meta, ownership: &meta_ref.ownership}
    }
    
    pub fn new_from_mut_ptr(ptr: *mut u8) -> Result<YCQueueSharedMeta<'a>, YCQueueError> {
        if ptr.is_null() {
            return Err(YCQueueError::InvalidArgs);
        }

        let slot_count_ptr = ptr as *mut AtomicU16;
        let slot_size_ptr = unsafe { slot_count_ptr.add(1) };
        let u64_meta_ptr = unsafe { slot_size_ptr.add(1) as *mut AtomicU64 };

        let slot_count = unsafe { &*slot_count_ptr };
        let slot_size = unsafe { &*slot_size_ptr };
        let produce_meta = unsafe { &*u64_meta_ptr };

        let slot_count_u16 = slot_count.load(std::sync::atomic::Ordering::Acquire);
        let ownership_count = (slot_count_u16 as usize + u64::BITS as usize - 1) / u64::BITS as usize;
        let ownership_ptr = unsafe { u64_meta_ptr.add(1) as *mut AtomicU64 };

        let ownership_slice = unsafe { std::slice::from_raw_parts_mut(ownership_ptr, ownership_count) };

        Ok(YCQueueSharedMeta {slot_count, slot_size, u64_meta: produce_meta, ownership: ownership_slice})
    }
}

/// A way to hold a YCQueueSharedMeta to share between threads of a particular rust program. Not designed for IPC. 
#[derive(Debug)]
pub struct YCQueueOwnedData {
    pub meta: YCQueueOwnedMeta,
    pub data: Vec<u8>,
    pub raw_ptr: *mut u8,
}

impl YCQueueOwnedData {
    pub fn new(slot_count_u16: u16, slot_size_u16: u16) -> YCQueueOwnedData {
        let meta = YCQueueOwnedMeta::new(slot_count_u16, slot_size_u16);
        let mut data = vec![0 as u8; (slot_count_u16 * slot_size_u16) as usize];
        let raw_ptr = data.as_mut_ptr();

        YCQueueOwnedData {meta, data, raw_ptr}
    }
}

#[derive(Debug)]
pub struct YCQueueSharedData<'a> {
    pub meta: YCQueueSharedMeta<'a>,
    pub data: &'a mut [u8],
}

impl<'a> YCQueueSharedData<'a> {
    pub fn from_owned_data(owned: &'a YCQueueOwnedData) -> YCQueueSharedData<'a> {
        let meta = YCQueueSharedMeta::new(&owned.meta);
        let data = owned.raw_ptr;

        YCQueueSharedData {meta, data: unsafe { std::slice::from_raw_parts_mut(data, owned.data.len()) }}
    }
}