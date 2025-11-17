cfg_if::cfg_if! {
    if #[cfg(feature = "futex")] {
        use std::sync::atomic::AtomicI32;
        use crate::YCFutexQueue;
    }
}

cfg_if::cfg_if! {
    if #[cfg(feature = "mutex")] {
        use std::sync::{Condvar, Mutex};
        use crate::YCMutexQueue;
    }
}

use crate::{YCQueue, YCQueueError, YCQueueSharedMeta};
use std::sync::atomic::{AtomicU16, AtomicU64};
use yep_cache_line_size::{CacheLevel, CacheType, get_cache_line_size};

const DEFAULT_CACHE_LINE_SIZE: usize = 64;

fn cache_line_size() -> usize {
    get_cache_line_size(CacheLevel::L1, CacheType::Data)
        .ok()
        .filter(|size| size.is_power_of_two())
        .unwrap_or(DEFAULT_CACHE_LINE_SIZE)
}

/// Cache-line padded atomic wrapper to keep heavily contended values isolated for owned metadata.
#[derive(Debug)]
#[repr(align(64))]
pub struct CachePaddedAtomicU64(AtomicU64);

impl CachePaddedAtomicU64 {
    pub(crate) const fn new(value: u64) -> Self {
        CachePaddedAtomicU64(AtomicU64::new(value))
    }
}

impl std::ops::Deref for CachePaddedAtomicU64 {
    type Target = AtomicU64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for CachePaddedAtomicU64 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

fn align_to(ptr: *mut u8, alignment: usize) -> *mut u8 {
    let addr = ptr as usize;
    let aligned = (addr + alignment - 1) & !(alignment - 1);
    aligned as *mut u8
}

#[derive(Debug)]
pub struct YCQueueOwnedMeta {
    pub slot_count: AtomicU16,
    pub slot_size: AtomicU16,
    pub producer_meta: CachePaddedAtomicU64,
    pub consumer_meta: CachePaddedAtomicU64,
    pub ownership: Vec<AtomicU64>,
}

impl YCQueueOwnedMeta {
    pub fn new(slot_count_u16: u16, slot_size_u16: u16) -> YCQueueOwnedMeta {
        let slot_count = AtomicU16::new(slot_count_u16);
        let slot_size = AtomicU16::new(slot_size_u16);
        let producer_meta = CachePaddedAtomicU64::new(0);
        let consumer_meta = CachePaddedAtomicU64::new(0);
        let mut ownership =
            Vec::<AtomicU64>::with_capacity((slot_count_u16 as usize).div_ceil(u64::BITS as usize));

        for _i in 0..ownership.capacity() {
            ownership.push(AtomicU64::new(0));
        }

        YCQueueOwnedMeta {
            slot_count,
            slot_size,
            producer_meta,
            consumer_meta,
            ownership,
        }
    }
}

impl<'a> YCQueueSharedMeta<'a> {
    pub fn new(meta_ref: &'a YCQueueOwnedMeta) -> YCQueueSharedMeta<'a> {
        YCQueueSharedMeta {
            slot_count: &meta_ref.slot_count,
            slot_size: &meta_ref.slot_size,
            producer_cursor: &meta_ref.producer_meta,
            consumer_cursor: &meta_ref.consumer_meta,
            ownership: &meta_ref.ownership,
        }
    }

    pub fn new_from_mut_ptr(ptr: *mut u8) -> Result<YCQueueSharedMeta<'a>, YCQueueError> {
        if ptr.is_null() {
            return Err(YCQueueError::InvalidArgs);
        }

        let slot_count_ptr = ptr as *mut AtomicU16;
        let slot_size_ptr = unsafe { slot_count_ptr.add(1) };
        let mut byte_cursor = unsafe { slot_size_ptr.add(1) as *mut u8 };

        let line_size = cache_line_size();
        let producer_meta_ptr = align_to(byte_cursor, line_size) as *mut CachePaddedAtomicU64;
        let consumer_meta_ptr = unsafe { producer_meta_ptr.add(1) };
        byte_cursor = unsafe { (consumer_meta_ptr.add(1)) as *mut u8 };

        let ownership_ptr =
            align_to(byte_cursor, std::mem::align_of::<AtomicU64>()) as *mut AtomicU64;

        let slot_count = unsafe { &*slot_count_ptr };
        let slot_size = unsafe { &*slot_size_ptr };
        let producer_meta: &AtomicU64 = unsafe { &*producer_meta_ptr };
        let consumer_meta: &AtomicU64 = unsafe { &*consumer_meta_ptr };

        let slot_count_u16 = slot_count.load(std::sync::atomic::Ordering::Acquire);
        let ownership_count = (slot_count_u16 as usize).div_ceil(u64::BITS as usize);

        let ownership_slice =
            unsafe { std::slice::from_raw_parts_mut(ownership_ptr, ownership_count) };

        Ok(YCQueueSharedMeta {
            slot_count,
            slot_size,
            producer_cursor: producer_meta,
            consumer_cursor: consumer_meta,
            ownership: ownership_slice,
        })
    }
}

/// A way to hold a YCQueueSharedMeta to share between threads of a particular rust program.
#[derive(Debug)]
pub struct YCQueueOwnedData {
    pub meta: YCQueueOwnedMeta,
    pub data: Vec<u8>,
    pub raw_ptr: *mut u8,
}

impl YCQueueOwnedData {
    pub fn new(slot_count_u16: u16, slot_size_u16: u16) -> YCQueueOwnedData {
        let meta = YCQueueOwnedMeta::new(slot_count_u16, slot_size_u16);
        let mut data = vec![0_u8; slot_count_u16 as usize * slot_size_u16 as usize];
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

impl<'a> YCQueue<'a> {
    pub fn from_owned_data(owned: &'a YCQueueOwnedData) -> Result<YCQueue<'a>, YCQueueError> {
        let shared_view = YCQueueSharedData::from_owned_data(owned);
        YCQueue::new(shared_view.meta, shared_view.data)
    }

    pub fn from_shared_data(shared: YCQueueSharedData<'a>) -> Result<YCQueue<'a>, YCQueueError> {
        YCQueue::new(shared.meta, shared.data)
    }
}

#[cfg(feature = "futex")]
#[derive(Debug)]
pub struct YCFutexQueueOwnedData {
    pub data: YCQueueOwnedData,
    pub count: AtomicI32,
}

#[cfg(feature = "futex")]
impl YCFutexQueueOwnedData {
    pub fn new(slot_count_u16: u16, slot_size_u16: u16) -> YCFutexQueueOwnedData {
        let data = YCQueueOwnedData::new(slot_count_u16, slot_size_u16);
        let count = AtomicI32::new(0);

        YCFutexQueueOwnedData { data, count }
    }
}

#[cfg(feature = "futex")]
#[derive(Debug)]
pub struct YCFutexQueueSharedData<'a> {
    pub data: YCQueueSharedData<'a>,
    pub count: &'a AtomicI32,
}

#[cfg(feature = "futex")]
impl<'a> YCFutexQueueSharedData<'a> {
    pub fn from_owned_data(futex_queue: &'a YCFutexQueueOwnedData) -> YCFutexQueueSharedData<'a> {
        let data = YCQueueSharedData::from_owned_data(&futex_queue.data);
        let count = &futex_queue.count;

        YCFutexQueueSharedData { data, count }
    }
}

#[cfg(feature = "futex")]
impl<'a> YCFutexQueue<'a> {
    pub fn from_shared_data(
        shared: YCFutexQueueSharedData<'a>,
    ) -> Result<YCFutexQueue<'a>, YCQueueError> {
        let queue = YCQueue::new(shared.data.meta, shared.data.data)?;

        Ok(YCFutexQueue {
            queue,
            count: shared.count,
        })
    }

    pub fn from_owned_data(
        owned: &'a YCFutexQueueOwnedData,
    ) -> Result<YCFutexQueue<'a>, YCQueueError> {
        let queue = YCQueue::from_owned_data(&owned.data)?;

        Ok(YCFutexQueue {
            queue,
            count: &owned.count,
        })
    }
}

#[cfg(feature = "mutex")]
#[derive(Debug)]
pub struct YCMutexQueueOwnedData {
    pub data: YCQueueOwnedData,
    pub count: Mutex<i32>,
    pub condvar: Condvar,
}

#[cfg(feature = "mutex")]
impl YCMutexQueueOwnedData {
    pub fn new(slot_count_u16: u16, slot_size_u16: u16) -> YCMutexQueueOwnedData {
        let data = YCQueueOwnedData::new(slot_count_u16, slot_size_u16);
        let count = Mutex::new(0);
        let condvar = Condvar::new();

        YCMutexQueueOwnedData {
            data,
            count,
            condvar,
        }
    }
}

#[cfg(feature = "mutex")]
#[derive(Debug)]
pub struct YCMutexQueueSharedData<'a> {
    pub data: YCQueueSharedData<'a>,
    pub count: &'a Mutex<i32>,
    pub condvar: &'a Condvar,
}

#[cfg(feature = "mutex")]
impl<'a> YCMutexQueueSharedData<'a> {
    pub fn from_owned_data(
        blocking_queue: &'a YCMutexQueueOwnedData,
    ) -> YCMutexQueueSharedData<'a> {
        let data = YCQueueSharedData::from_owned_data(&blocking_queue.data);
        let count = &blocking_queue.count;
        let condvar = &blocking_queue.condvar;

        YCMutexQueueSharedData {
            data,
            count,
            condvar,
        }
    }
}

#[cfg(feature = "mutex")]
impl<'a> YCMutexQueue<'a> {
    pub fn from_shared_data(
        shared: YCMutexQueueSharedData<'a>,
    ) -> Result<YCMutexQueue<'a>, YCQueueError> {
        let queue = YCQueue::new(shared.data.meta, shared.data.data)?;

        Ok(YCMutexQueue {
            queue,
            count: shared.count,
            condvar: shared.condvar,
        })
    }

    pub fn from_owned_data(
        owned: &'a YCMutexQueueOwnedData,
    ) -> Result<YCMutexQueue<'a>, YCQueueError> {
        let queue = YCQueue::from_owned_data(&owned.data)?;

        Ok(YCMutexQueue {
            queue,
            count: &owned.count,
            condvar: &owned.condvar,
        })
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
            owned_meta.producer_meta.load(Ordering::Acquire),
            shared_meta.producer_cursor.load(Ordering::Acquire)
        );
        assert_eq!(
            owned_meta.consumer_meta.load(Ordering::Acquire),
            shared_meta.consumer_cursor.load(Ordering::Acquire)
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
        owned_meta.producer_meta.store(new_value, Ordering::Release);
        assert_eq!(
            shared_meta.producer_cursor.load(Ordering::Acquire),
            new_value
        );
        owned_meta
            .consumer_meta
            .store(new_value + 1, Ordering::Release);
        assert_eq!(
            shared_meta.consumer_cursor.load(Ordering::Acquire),
            new_value + 1
        );
        for i in 0..num_atomics {
            owned_meta.ownership[i].store(i as u64, Ordering::Release);
        }
        for i in 0..num_atomics {
            assert_eq!(i as u64, shared_meta.ownership[i].load(Ordering::Acquire));
        }

        // write to shared and see it reflect in owned
        let new_new_value: u64 = 54321;
        shared_meta
            .producer_cursor
            .store(new_new_value, Ordering::Release);
        assert_eq!(
            owned_meta.producer_meta.load(Ordering::Acquire),
            new_new_value
        );
        shared_meta
            .consumer_cursor
            .store(new_new_value + 1, Ordering::Release);
        assert_eq!(
            owned_meta.consumer_meta.load(Ordering::Acquire),
            new_new_value + 1
        );
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

    #[cfg(feature = "futex")]
    #[test]
    fn test_futex_shared_queue_and_counter() {
        let slot_count: u16 = 4;
        let slot_size: u16 = 16;

        let mut owned = YCFutexQueueOwnedData::new(slot_count, slot_size);

        // Seed underlying queue data through owned handle.
        for (idx, byte) in owned.data.data.iter_mut().enumerate() {
            *byte = idx as u8;
        }

        let data_len = owned.data.data.len();
        {
            let shared = YCFutexQueueSharedData::from_owned_data(&owned);
            let data = shared.data.data;
            assert_eq!(data, owned.data.data.as_slice());

            // Mutate through the shared handle and ensure the owned buffer sees it.
            data[0] = 0xAA;
            data[data.len() - 1] = 0xBB;
        }
        assert_eq!(owned.data.data[0], 0xAA);
        assert_eq!(owned.data.data[data_len - 1], 0xBB);

        // Mutate through owned handle and verify the shared slice reflects changes.
        owned.data.data[1] = 0xCC;
        owned.data.data[data_len / 2] = 0xDD;
        {
            let shared_again = YCFutexQueueSharedData::from_owned_data(&owned);
            let data_again = shared_again.data.data;
            assert_eq!(data_again[1], 0xCC);
            assert_eq!(data_again[data_again.len() / 2], 0xDD);
        }

        // Verify the atomic counter is shared.
        owned.count.store(123, Ordering::Release);
        {
            let shared = YCFutexQueueSharedData::from_owned_data(&owned);
            assert_eq!(shared.count.load(Ordering::Acquire), 123);

            shared.count.store(77, Ordering::Release);
        }
        assert_eq!(owned.count.load(Ordering::Acquire), 77);
    }

    #[cfg(feature = "mutex")]
    #[test]
    fn test_blocking_shared_queue_and_counter() {
        let slot_count: u16 = 4;
        let slot_size: u16 = 16;

        let mut owned = YCMutexQueueOwnedData::new(slot_count, slot_size);

        // Seed underlying queue data through owned handle.
        for (idx, byte) in owned.data.data.iter_mut().enumerate() {
            *byte = idx as u8;
        }

        let data_len = owned.data.data.len();
        {
            let shared = YCMutexQueueSharedData::from_owned_data(&owned);
            let data = shared.data.data;
            assert_eq!(data, owned.data.data.as_slice());

            // Mutate through the shared handle and ensure the owned buffer sees it.
            data[0] = 0xAA;
            data[data.len() - 1] = 0xBB;
        }
        assert_eq!(owned.data.data[0], 0xAA);
        assert_eq!(owned.data.data[data_len - 1], 0xBB);

        // Mutate through owned handle and verify the shared slice reflects changes.
        owned.data.data[1] = 0xCC;
        owned.data.data[data_len / 2] = 0xDD;
        {
            let shared_again = YCMutexQueueSharedData::from_owned_data(&owned);
            let data_again = shared_again.data.data;
            assert_eq!(data_again[1], 0xCC);
            assert_eq!(data_again[data_again.len() / 2], 0xDD);
        }

        // Verify the mutex counter is shared.
        *owned.count.lock().unwrap() = 123;
        {
            let shared = YCMutexQueueSharedData::from_owned_data(&owned);
            assert_eq!(*shared.count.lock().unwrap(), 123);

            *shared.count.lock().unwrap() = 77;
        }
        assert_eq!(*owned.count.lock().unwrap(), 77);
    }
}
