/// the circular queue code
pub mod queue;
pub use queue::YCQueue;
pub use queue::YCQueueConsumeSlot;
pub use queue::YCQueueOwner;
pub use queue::YCQueueProduceSlot;

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
