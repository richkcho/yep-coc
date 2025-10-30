/// the circular queue code
pub mod queue;
pub use queue::YCQueue;
pub use queue::YCQueueConsumeSlot;
pub use queue::YCQueueOwner;
pub use queue::YCQueueProduceSlot;

/// Futex-backed helpers (optional feature)
#[cfg(feature = "futex")]
pub mod futex_queue;
#[cfg(feature = "futex")]
pub use futex_queue::YCFutexQueue;

/// Blocking queue backed by Mutex and CondVar (optional feature)
#[cfg(feature = "blocking")]
pub mod blocking_queue;
#[cfg(feature = "blocking")]
pub use blocking_queue::YCBlockingQueue;

/// dependencies for the circular queue code
pub mod queue_meta;
pub use queue_meta::YCQueueSharedMeta;

/// the errors
pub mod error;
pub use error::YCQueueError;

/// A way to allocate data for the queue
pub mod queue_alloc_helpers;

/// utils for internal usage
mod utils;
