#[derive(Eq, PartialEq, Debug)]
pub enum YCQueueError {
    InvalidArgs,
    OutOfSpace,
    EmptyQueue,
    SlotNotReady,
    #[cfg(any(feature = "futex", feature = "blocking"))]
    Timeout,
}
