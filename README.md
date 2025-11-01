# yep-coc

[![Crates.io](https://img.shields.io/crates/v/yep-coc.svg)](https://crates.io/crates/yep-coc) [![Documentation](https://docs.rs/yep-coc/badge.svg)](https://docs.rs/yep-coc) [![CI](https://github.com/richkcho/yep-coc/actions/workflows/ci.yml/badge.svg)](https://github.com/richkcho/yep-coc/actions/workflows/ci.yml)

## Overview
- Multi-producer, multi-consumer lock-free queue built for zero-copy data paths.
- Reserve/submit workflow lets producers publish buffers without blocking each other, while consumers reclaim slots independently.
- Designed for high-performance IPC scenarios

## Design Highlights
- Queue metadata is stored separately from the contiguous data region so corruption in payload does not affect queue state.
- Ownership of each slot is tracked with atomics, enabling concurrent producers and consumers without coarse locking.
- Producers and consumers operate on `YCQueueProduceSlot` and `YCQueueConsumeSlot` handles, making it explicit when data is ready.
- The API supports batching for reserve/submit/consume operations; single-slot helpers are layered on top for convenience.

## Quick Start
```rust
use yep_coc::queue_alloc_helpers::YCQueueOwnedData;
use yep_coc::{YCQueue, YCQueueSharedMeta};

fn main() {
    // Allocate backing storage for 4 slots of 128 bytes each.
    let mut owned = YCQueueOwnedData::new(4, 128);
    let shared = YCQueueSharedMeta::new(&owned.meta);
    let mut queue = YCQueue::new(shared, owned.data.as_mut_slice()).expect("queue");

    // Producer: reserve a slot, write data, then publish it.
    let mut produce_slot = queue.get_produce_slot().expect("produce slot");
    produce_slot.data[..5].copy_from_slice(b"hello");
    queue.mark_slot_produced(produce_slot).expect("publish");

    // Consumer: take the slot, read the data, and return it to producers.
    let consume_slot = queue.get_consume_slot().expect("consume slot");
    assert_eq!(&consume_slot.data[..5], b"hello");
    queue.mark_slot_consumed(consume_slot).expect("reclaim");
}
```

For more complete examples, check `examples/spsc-send-recv.rs` and `examples/mpmc-send-recv.rs`.

## YCFutexQueue Example

The `YCFutexQueue` provides a blocking queue using futex-based synchronization (enabled with the `futex` feature). This allows producers and consumers to efficiently wait for space or data without busy-waiting:

```rust
use std::time::Duration;
use yep_coc::queue_alloc_helpers::YCFutexQueueOwnedData;
use yep_coc::YCFutexQueue;

fn main() {
    // Allocate backing storage for 4 slots of 128 bytes each.
    let owned = YCFutexQueueOwnedData::new(4, 128);
    let mut queue = YCFutexQueue::from_owned_data(&owned).expect("queue");
    
    let timeout = Duration::from_millis(100);
    
    // Producer: reserve a slot, write data, then publish it.
    let mut slot = queue.get_produce_slot(timeout).expect("produce slot");
    slot.data[..5].copy_from_slice(b"hello");
    queue.mark_slot_produced(slot).expect("publish");
    
    // Consumer: wait for and take a slot, read the data, and return it.
    let slot = queue.get_consume_slot(timeout).expect("consume slot");
    assert_eq!(&slot.data[..5], b"hello");
    queue.mark_slot_consumed(slot).expect("reclaim");
}
```

## YCMutexQueue Example

The `YCMutexQueue` provides similar blocking capabilities using standard library primitives (Mutex and CondVar) and is enabled with the `mutex` feature:

```rust
use std::time::Duration;
use yep_coc::queue_alloc_helpers::YCMutexQueueOwnedData;
use yep_coc::YCMutexQueue;

fn main() {
    // Allocate backing storage for 4 slots of 128 bytes each.
    let owned = YCMutexQueueOwnedData::new(4, 128);
    let mut queue = YCMutexQueue::from_owned_data(&owned).expect("queue");
    
    let timeout = Duration::from_millis(100);
    
    // Producer: reserve a slot, write data, then publish it.
    let mut slot = queue.get_produce_slot(timeout).expect("produce slot");
    slot.data[..5].copy_from_slice(b"hello");
    queue.mark_slot_produced(slot).expect("publish");
    
    // Consumer: wait for and take a slot, read the data, and return it.
    let slot = queue.get_consume_slot(timeout).expect("consume slot");
    assert_eq!(&slot.data[..5], b"hello");
    queue.mark_slot_consumed(slot).expect("reclaim");
}
```

## Benchmarks

The repository includes Criterion-based benchmarks for measuring SPSC queue throughput:

```bash
# Run all SPSC throughput benchmarks
cargo bench --bench spsc_throughput

# Run a specific benchmark
cargo bench --bench spsc_throughput -- cap512_payload64_batch8

# Generate HTML reports (located in target/criterion/)
cargo bench --bench spsc_throughput -- --save-baseline my-baseline
```

The SPSC throughput benchmark measures steady-state performance with:
- Thread pinning to separate CPU cores (avoiding SMT siblings)
- NUMA-aware allocation (queue created after thread pinning)
- Synchronized start with barrier synchronization
- Support for single-slot and batched operations
- Parameter sweeps across capacities (256, 512, 2048), payload sizes (8, 64, 128 bytes), and batch sizes (1, 8, 32)
