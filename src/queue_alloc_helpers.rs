use std::sync::atomic::{AtomicU16, AtomicU64};

use crate::{YCQueueError, YCQueueSharedMeta};

#[derive(Debug)]
pub struct YCQueueOwnedMeta {
    pub slot_count: AtomicU16,
    pub slot_size: AtomicU16,
    pub u64_meta: AtomicU64,
    pub ownership: Vec<AtomicU64>,
}

impl YCQueueOwnedMeta {
    pub fn new(slot_count_u16: u16, slot_size_u16: u16) -> YCQueueOwnedMeta {
        let slot_count = AtomicU16::new(slot_count_u16);
        let slot_size = AtomicU16::new(slot_size_u16);
        let u64_meta = AtomicU64::new(0);
        let mut ownership =
            Vec::<AtomicU64>::with_capacity((slot_count_u16 as usize).div_ceil(u64::BITS as usize));

        for _i in 0..ownership.capacity() {
            ownership.push(AtomicU64::new(0));
        }

        YCQueueOwnedMeta {
            slot_count,
            slot_size,
            u64_meta,
            ownership,
        }
    }
}

impl<'a> YCQueueSharedMeta<'a> {
    pub fn new(meta_ref: &'a YCQueueOwnedMeta) -> YCQueueSharedMeta<'a> {
        YCQueueSharedMeta {
            slot_count: &meta_ref.slot_count,
            slot_size: &meta_ref.slot_size,
            u64_meta: &meta_ref.u64_meta,
            ownership: &meta_ref.ownership,
        }
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
        let u64_meta = unsafe { &*u64_meta_ptr };

        let slot_count_u16 = slot_count.load(std::sync::atomic::Ordering::Acquire);
        let ownership_count = (slot_count_u16 as usize).div_ceil(u64::BITS as usize);
        let ownership_ptr = unsafe { u64_meta_ptr.add(1) };

        let ownership_slice =
            unsafe { std::slice::from_raw_parts_mut(ownership_ptr, ownership_count) };

        Ok(YCQueueSharedMeta {
            slot_count,
            slot_size,
            u64_meta,
            ownership: ownership_slice,
        })
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
        let mut data = vec![0_u8; (slot_count_u16 * slot_size_u16) as usize];
        let raw_ptr = data.as_mut_ptr();

        YCQueueOwnedData {
            meta,
            data,
            raw_ptr,
        }
    }
}

#[derive(Debug)]
pub struct YCQueueSharedData<'a> {
    pub meta: YCQueueSharedMeta<'a>,
    pub data: &'a mut [u8],
}

/**
 * Create a YC queue view from non-owned data. Since This relies on referencing
 * memory we do not own but need to mutably borrow (the queue slots) that is shared with
 * other threads that will be also mutably borrowing the same slices (though not at the
 * same time), we allow data access through `unsafe`. It's not clear to me if this *can*
 * be safe, for inter-process comms you would need to map this memory with unsafe too
 * since it could "change underneath you". (of course, gated by the atomics and proper
 * memory barriers, since that is the point of this library.)
 */
impl<'a> YCQueueSharedData<'a> {
    pub fn from_owned_data(owned: &'a YCQueueOwnedData) -> YCQueueSharedData<'a> {
        let meta = YCQueueSharedMeta::new(&owned.meta);
        let data = owned.raw_ptr;

        YCQueueSharedData {
            meta,
            data: unsafe { std::slice::from_raw_parts_mut(data, owned.data.len()) },
        }
    }
}

#[cfg(test)]
mod queue_alloc_helpers_tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn test_shared_meta() {
        let slot_count: u16 = 128;
        let slot_size: u16 = 64;
        let owned_meta = YCQueueOwnedMeta::new(slot_count, slot_size);

        let shared_meta: YCQueueSharedMeta<'_> = YCQueueSharedMeta::new(&owned_meta);

        assert_eq!(
            owned_meta.u64_meta.load(Ordering::Acquire),
            shared_meta.u64_meta.load(Ordering::Acquire)
        );

        // validate initial memory
        let num_atomics = owned_meta.ownership.len();
        for i in 0..num_atomics {
            assert_eq!(
                owned_meta.ownership[i].load(Ordering::Acquire),
                shared_meta.ownership[i].load(Ordering::Acquire)
            );
        }

        // write to owned and see it reflect in shared
        let new_value: u64 = 12345;
        owned_meta.u64_meta.store(new_value, Ordering::Release);
        assert_eq!(shared_meta.u64_meta.load(Ordering::Acquire), new_value);
        for i in 0..num_atomics {
            owned_meta.ownership[i].store(i as u64, Ordering::Release);
        }
        for i in 0..num_atomics {
            assert_eq!(i as u64, shared_meta.ownership[i].load(Ordering::Acquire));
        }

        // write to shared and see it reflect in owned
        let new_new_value = 54321;
        shared_meta.u64_meta.store(new_new_value, Ordering::Release);
        assert_eq!(owned_meta.u64_meta.load(Ordering::Acquire), new_new_value);
        for i in 0..num_atomics {
            shared_meta.ownership[i].store(new_new_value + (i as u64), Ordering::Release);
        }
        for i in 0..num_atomics {
            assert_eq!(
                new_new_value + (i as u64),
                owned_meta.ownership[i].load(Ordering::Acquire)
            );
        }
    }

    #[test]
    fn test_shared_data() {
        let slot_count: u16 = 4;
        let slot_size: u16 = 8;
        let mut owned_data = YCQueueOwnedData::new(slot_count, slot_size);

        for (idx, byte) in owned_data.data.iter_mut().enumerate() {
            *byte = idx as u8;
        }

        let data_len = owned_data.data.len();

        // mutate using transient shared data
        {
            let shared = YCQueueSharedData::from_owned_data(&owned_data);
            let data = shared.data;
            assert_eq!(data, owned_data.data.as_slice());

            data[0] = 0xAA;
            data[data_len - 1] = 0xBB;
        }

        // check is OK
        assert_eq!(owned_data.data[0], 0xAA);
        assert_eq!(owned_data.data[data_len - 1], 0xBB);

        owned_data.data[1] = 0xCC;
        let mid = data_len / 2;
        owned_data.data[mid] = 0xDD;

        // should show up in shared data
        let shared = YCQueueSharedData::from_owned_data(&owned_data);
        let data = shared.data;
        assert_eq!(data[1], 0xCC);
        assert_eq!(data[mid], 0xDD);
    }
}
