# yep-coc

[![Crates.io](https://img.shields.io/crates/v/yep-coc.svg)](https://crates.io/crates/yep-coc) [![Documentation](https://docs.rs/yep-coc/badge.svg)](https://docs.rs/yep-coc) [![Rust](https://github.com/richkcho/yep-coc/actions/workflows/rust.yml/badge.svg)](https://github.com/richkcho/yep-coc/actions/workflows/rust.yml)

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

For more complete examples, check `examples/simple-send-recv.rs` and `examples/multi-send-recv.rs`.
