#[derive(Eq, PartialEq, Debug)]
pub enum YCQueueError {
    InvalidArgs,
    OutOfSpace,
    EmptyQueue,
    SlotNotReady,
    #[cfg(feature = "futex")]
    Timeout,
}
