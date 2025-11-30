cfg_if::cfg_if! {
    if #[cfg(feature = "futex")] {
        use std::sync::atomic::AtomicI32;
        use crate::YCFutexQueue;
    }
}

use crate::{YCQueue, YCQueueError, YCQueueSharedMeta};
use std::mem::size_of;
use std::sync::atomic::{AtomicU16, AtomicU64};
use yep_cache_line_size::{CacheLevel, CacheType, get_cache_line_size};

const DEFAULT_CACHE_LINE_SIZE: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorCacheLines {
    /// Place producer/consumer cursors in the same cache line.
    Single,
    /// Place producer/consumer cursors in separate cache lines.
    Split,
}

fn cache_line_size() -> usize {
    get_cache_line_size(CacheLevel::L1, CacheType::Data)
        .ok()
        .filter(|size| size.is_power_of_two())
        .unwrap_or(DEFAULT_CACHE_LINE_SIZE)
}

/// Cache-line padded atomic wrapper to keep heavily contended values isolated for owned metadata.
#[derive(Debug)]
pub struct CachePaddedAtomicU64 {
    storage: Box<[AtomicU64]>,
    aligned_index: usize,
}

impl CachePaddedAtomicU64 {
    pub(crate) fn new(value: u64) -> Self {
        let line_size = cache_line_size();
        let atoms_per_line = (line_size / size_of::<u64>()).max(1);

        let mut storage = Vec::with_capacity(atoms_per_line);
        for _ in 0..atoms_per_line {
            storage.push(AtomicU64::new(value));
        }
        let storage = storage.into_boxed_slice();

        let aligned_index = (0..storage.len())
            .find(|i| (unsafe { storage.as_ptr().add(*i) } as usize).is_multiple_of(line_size))
            .unwrap_or(0);

        debug_assert!(
            (unsafe { storage.as_ptr().add(aligned_index) } as usize).is_multiple_of(line_size)
        );

        CachePaddedAtomicU64 {
            storage,
            aligned_index,
        }
    }
}

impl std::ops::Deref for CachePaddedAtomicU64 {
    type Target = AtomicU64;

    fn deref(&self) -> &Self::Target {
        &self.storage[self.aligned_index]
    }
}

impl std::ops::DerefMut for CachePaddedAtomicU64 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.storage[self.aligned_index]
    }
}

#[derive(Debug)]
enum CursorStorage {
    Split {
        producer: CachePaddedAtomicU64,
        consumer: CachePaddedAtomicU64,
    },
    SingleLine {
        storage: Box<[AtomicU64]>,
        producer_index: usize,
        consumer_index: usize,
    },
}

#[derive(Debug)]
struct OwnedCursors {
    layout: CursorCacheLines,
    storage: CursorStorage,
}

impl OwnedCursors {
    fn new(initial_value: u64, layout: CursorCacheLines) -> Self {
        match layout {
            CursorCacheLines::Split => OwnedCursors {
                layout,
                storage: CursorStorage::Split {
                    producer: CachePaddedAtomicU64::new(initial_value),
                    consumer: CachePaddedAtomicU64::new(initial_value),
                },
            },
            CursorCacheLines::Single => {
                let line_size = cache_line_size();
                let atoms_per_line = (line_size / size_of::<u64>()).max(1);
                let mut storage = Vec::with_capacity(atoms_per_line + 1);
                for _ in 0..storage.capacity() {
                    storage.push(AtomicU64::new(initial_value));
                }
                let storage = storage.into_boxed_slice();
                let search_len = storage.len().saturating_sub(1);

                let producer_index = (0..search_len)
                    .find(|i| {
                        let base = unsafe { storage.as_ptr().add(*i) } as usize;
                        let consumer = unsafe { storage.as_ptr().add(*i + 1) } as usize;
                        base.is_multiple_of(line_size) && base / line_size == consumer / line_size
                    })
                    .unwrap_or(0);
                let consumer_index = producer_index + 1;

                debug_assert!(consumer_index < storage.len());
                debug_assert!({
                    let base = unsafe { storage.as_ptr().add(producer_index) } as usize;
                    let consumer = unsafe { storage.as_ptr().add(consumer_index) } as usize;
                    base.is_multiple_of(line_size) && base / line_size == consumer / line_size
                });

                OwnedCursors {
                    layout,
                    storage: CursorStorage::SingleLine {
                        storage,
                        producer_index,
                        consumer_index,
                    },
                }
            }
        }
    }

    fn producer(&self) -> &AtomicU64 {
        match &self.storage {
            CursorStorage::Split { producer, .. } => producer,
            CursorStorage::SingleLine {
                storage,
                producer_index,
                ..
            } => &storage[*producer_index],
        }
    }

    fn consumer(&self) -> &AtomicU64 {
        match &self.storage {
            CursorStorage::Split { consumer, .. } => consumer,
            CursorStorage::SingleLine {
                storage,
                consumer_index,
                ..
            } => &storage[*consumer_index],
        }
    }

    fn layout(&self) -> CursorCacheLines {
        self.layout
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
    cursors: OwnedCursors,
    pub ownership: Vec<AtomicU64>,
}

impl YCQueueOwnedMeta {
    pub fn new(slot_count_u16: u16, slot_size_u16: u16) -> YCQueueOwnedMeta {
        YCQueueOwnedMeta::new_with_cursor_layout(
            slot_count_u16,
            slot_size_u16,
            CursorCacheLines::Split,
        )
    }

    pub fn new_with_cursor_layout(
        slot_count_u16: u16,
        slot_size_u16: u16,
        cursor_layout: CursorCacheLines,
    ) -> YCQueueOwnedMeta {
        let slot_count = AtomicU16::new(slot_count_u16);
        let slot_size = AtomicU16::new(slot_size_u16);
        let cursors = OwnedCursors::new(0, cursor_layout);
        let mut ownership =
            Vec::<AtomicU64>::with_capacity((slot_count_u16 as usize).div_ceil(u64::BITS as usize));

        for _i in 0..ownership.capacity() {
            ownership.push(AtomicU64::new(0));
        }

        YCQueueOwnedMeta {
            slot_count,
            slot_size,
            cursors,
            ownership,
        }
    }

    pub fn producer_cursor(&self) -> &AtomicU64 {
        self.cursors.producer()
    }

    pub fn consumer_cursor(&self) -> &AtomicU64 {
        self.cursors.consumer()
    }

    pub fn cursor_layout(&self) -> CursorCacheLines {
        self.cursors.layout()
    }
}

impl<'a> YCQueueSharedMeta<'a> {
    pub fn new(meta_ref: &'a YCQueueOwnedMeta) -> YCQueueSharedMeta<'a> {
        YCQueueSharedMeta {
            slot_count: &meta_ref.slot_count,
            slot_size: &meta_ref.slot_size,
            producer_cursor: meta_ref.producer_cursor(),
            consumer_cursor: meta_ref.consumer_cursor(),
            ownership: &meta_ref.ownership,
        }
    }

    pub fn new_from_mut_ptr(ptr: *mut u8) -> Result<YCQueueSharedMeta<'a>, YCQueueError> {
        YCQueueSharedMeta::new_from_mut_ptr_with_layout(ptr, CursorCacheLines::Split)
    }

    pub fn new_from_mut_ptr_with_layout(
        ptr: *mut u8,
        cursor_layout: CursorCacheLines,
    ) -> Result<YCQueueSharedMeta<'a>, YCQueueError> {
        if ptr.is_null() {
            return Err(YCQueueError::InvalidArgs);
        }

        let slot_count_ptr = ptr as *mut AtomicU16;
        let slot_size_ptr = unsafe { slot_count_ptr.add(1) };
        let mut byte_cursor = unsafe { slot_size_ptr.add(1) as *mut u8 };

        let line_size = cache_line_size();
        let (producer_meta, consumer_meta) = match cursor_layout {
            CursorCacheLines::Split => {
                let producer_meta_ptr =
                    align_to(byte_cursor, line_size) as *mut CachePaddedAtomicU64;
                let consumer_meta_ptr = unsafe { producer_meta_ptr.add(1) };
                byte_cursor = unsafe { consumer_meta_ptr.add(1) as *mut u8 };

                let producer_meta: &AtomicU64 = unsafe { &*producer_meta_ptr };
                let consumer_meta: &AtomicU64 = unsafe { &*consumer_meta_ptr };
                (producer_meta, consumer_meta)
            }
            CursorCacheLines::Single => {
                let producer_meta_ptr = align_to(byte_cursor, line_size) as *mut AtomicU64;
                let consumer_meta_ptr = unsafe { producer_meta_ptr.add(1) };
                byte_cursor = unsafe { consumer_meta_ptr.add(1) as *mut u8 };

                let producer_meta: &AtomicU64 = unsafe { &*producer_meta_ptr };
                let consumer_meta: &AtomicU64 = unsafe { &*consumer_meta_ptr };
                (producer_meta, consumer_meta)
            }
        };

        let ownership_ptr =
            align_to(byte_cursor, std::mem::align_of::<AtomicU64>()) as *mut AtomicU64;

        let slot_count = unsafe { &*slot_count_ptr };
        let slot_size = unsafe { &*slot_size_ptr };

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
        YCQueueOwnedData::new_with_cursor_layout(
            slot_count_u16,
            slot_size_u16,
            CursorCacheLines::Split,
        )
    }

    pub fn new_with_cursor_layout(
        slot_count_u16: u16,
        slot_size_u16: u16,
        cursor_layout: CursorCacheLines,
    ) -> YCQueueOwnedData {
        let meta =
            YCQueueOwnedMeta::new_with_cursor_layout(slot_count_u16, slot_size_u16, cursor_layout);
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
        let data = YCQueueOwnedData::new_with_cursor_layout(
            slot_count_u16,
            slot_size_u16,
            CursorCacheLines::Split,
        );
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

#[cfg(test)]
mod queue_alloc_helpers_tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn cache_padded_atomic_is_aligned() {
        let atomic = CachePaddedAtomicU64::new(7);
        let ptr = (&*atomic as *const AtomicU64) as usize;
        let line_size = cache_line_size();

        assert_eq!(ptr % line_size, 0);
        assert_eq!(atomic.load(Ordering::Relaxed), 7);
    }

    #[test]
    fn test_shared_meta_split_layout() {
        shared_meta_round_trip(CursorCacheLines::Split);
    }

    #[test]
    fn test_shared_meta_single_layout() {
        shared_meta_round_trip(CursorCacheLines::Single);
    }

    fn shared_meta_round_trip(cursor_layout: CursorCacheLines) {
        let slot_count: u16 = 128;
        let slot_size: u16 = 64;
        let owned_meta =
            YCQueueOwnedMeta::new_with_cursor_layout(slot_count, slot_size, cursor_layout);

        let shared_meta: YCQueueSharedMeta<'_> = YCQueueSharedMeta::new(&owned_meta);

        assert_eq!(owned_meta.cursor_layout(), cursor_layout);

        assert_eq!(
            owned_meta.producer_cursor().load(Ordering::Acquire),
            shared_meta.producer_cursor.load(Ordering::Acquire)
        );
        assert_eq!(
            owned_meta.consumer_cursor().load(Ordering::Acquire),
            shared_meta.consumer_cursor.load(Ordering::Acquire)
        );

        if matches!(cursor_layout, CursorCacheLines::Single) {
            let line_size = cache_line_size();
            let producer_ptr = owned_meta.producer_cursor() as *const AtomicU64 as usize;
            let consumer_ptr = owned_meta.consumer_cursor() as *const AtomicU64 as usize;
            assert_eq!(producer_ptr / line_size, consumer_ptr / line_size);
        }

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
        owned_meta
            .producer_cursor()
            .store(new_value, Ordering::Release);
        assert_eq!(
            shared_meta.producer_cursor.load(Ordering::Acquire),
            new_value
        );
        owned_meta
            .consumer_cursor()
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
            owned_meta.producer_cursor().load(Ordering::Acquire),
            new_new_value
        );
        shared_meta
            .consumer_cursor
            .store(new_new_value + 1, Ordering::Release);
        assert_eq!(
            owned_meta.consumer_cursor().load(Ordering::Acquire),
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
        let mut owned_data = YCQueueOwnedData::new_with_cursor_layout(
            slot_count,
            slot_size,
            CursorCacheLines::Split,
        );

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
}
