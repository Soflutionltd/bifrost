use crate::error::Error;

/// Error returned by [`Pipeline::execute`](super::Pipeline::execute) and
/// [`Pipeline::execute_dynamic`](super::Pipeline::execute_dynamic).
///
/// Distinguished from [`Error`] because pipeline execution distinguishes
/// failure classes: per-command errors (embedded in the result tuple as
/// `Result<T, Error>`, including a failure that ended the batch after its
/// first byte), pipeline-level refusals before any byte (the whole batch
/// refused), deadline expiry, and type-mismatch bugs (internal only).
#[derive(Debug)]
#[non_exhaustive]
pub enum PipelineError {
    /// The driver task has exited  -  the command channel is closed. Carries
    /// the observed `DriverGone` / `DriverPanicked`, stamped `Unsent` when
    /// the driver never claimed the batch (the channel was closed at
    /// admission, or the batch was dropped still queued) and `InFlight` when
    /// the driver had claimed it and died before answering, so a batch that
    /// may have been written is not reported as one that certainly was not.
    Disconnected(Error),
    /// The batch deadline expired. `Unsent` means withdrawal won before the
    /// first write; `InFlight` means the batch may have partially executed and
    /// the connection is abandoned. No per-command results are available.
    Timeout(Error),
    /// A pipeline-level refusal of the whole batch before anything was
    /// written (a command the session state does not permit, an encoding
    /// failure). Nothing ran. A failure after the first byte is never this:
    /// it is reported in each unanswered command's own result.
    Driver(Error),
    /// Internal error: a consumer returned a type that does not match
    /// the expected downcast target. This indicates a bug in the
    /// pipeline builder's command-to-consumer mapping.
    TypeMismatch {
        /// Zero-based index of the command whose output could not be
        /// downcast.
        index: usize,
    },
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disconnected(e) => write!(f, "pipeline: driver task disconnected: {e}"),
            Self::Timeout(e) => write!(f, "pipeline: deadline expired: {e}"),
            Self::Driver(e) => write!(f, "pipeline: driver error: {e}"),
            Self::TypeMismatch { index } => {
                write!(f, "pipeline: type mismatch at command index {index}")
            }
        }
    }
}

impl std::error::Error for PipelineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Driver(e) | Self::Disconnected(e) | Self::Timeout(e) => Some(e),
            Self::TypeMismatch { .. } => None,
        }
    }
}
