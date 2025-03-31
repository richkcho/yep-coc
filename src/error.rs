
#[derive(Eq, PartialEq, Debug)]
pub enum YCQueueError {
    /// Arg was invalid - it will never be valid
    InvalidArgsError,
    /// Mismatch between arg (assumed to be potentially valid) and internal book-keeping
    UnexpectedArgError,
    /// Something went wrong in internal state - check for memory corruption.
    InternalStateError,
    /// No more space in queue
    OutOfSpace,
    /// Empty queue
    EmptyQueue,
}