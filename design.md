## Preface
This document serves to describe a high level design of the yanother circular queue (YCQueue). 
My expected users of this queue are users who want to implement high performance zero copy IPC, between processes or processors. The latter would require part of this code to exist in a driver, so the design should support a way of doing this reasonably. These notions aren't new, and the API will look similar to those who have seen io_uring, RDMA, or other things that have similar performance constraints. Because we like extra ????, this YCQueue will hopefully support expansion and contraction.

## Goals
1. Provide zero-copy API for producers and consumers of the queue. Zero copy in the context of using the queue does not incur an additional copy.
2. Producers shouldn't block each other when accessing queue data, neither should consumers. Multiple producers should be able to "reserve" a slot, copy / DMA into it, then publish the data. Consumers should also be able to do the same. 
3. Metadata region should live in a separate memory region than the queue data. The data section should be "corruptible" without breaking queue state - only transferred data should be corrupted. 
4. No unreasonable assumptions about memory allocator or scheduler that would prevent this library from living in driver code or FW code. Currently, the state of the code *does* depend on `std`, but only on `Cell` and atomics, which I feel is reasonable. 

## Later Goals
1. Perf benchmark, worry about whether I should store queue info in one atomic vs two. (cache line separation could increase perf if there's high contention)
2. Allow growth... 
3. and shrinkage.

## Maybe Goals
1. Provide a simpler, type-rich interface.
2. Provide async blocking methods or something. This would have to be a wrapper, since the core queue should not make assumptions about schedulers or what OS is being used. 

## Yep Coc API
YCQueue (yanother circular queue) strives to accomplish this via a reserve and submit model for producing / consuming slots in the queue. Producer(s) reserve slots to publish into, which can be asynchronously written into and submitted (marked as ready for consumption). Consumer(s) reserve slots to consume, which can be asynchronously processed, then marked ready for re-use. 

See YCQueue in queue.rs for details. (TODO: add documentation with examples.)

It will also have a disjoint metadata region, which can be kept separate from the data region. 