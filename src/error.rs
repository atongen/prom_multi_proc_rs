use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid metric name: {0}")]
    InvalidName(String),

    #[error("invalid label name: {0}")]
    InvalidLabel(String),

    #[error("invalid prefix: {0}")]
    InvalidPrefix(String),

    #[error("metric already exists: {0}")]
    DuplicateMetric(String),

    #[error("metric conflicts with existing method: {0}")]
    MethodConflict(String),

    #[error("missing help for metric: {0}")]
    MissingHelp(String),

    #[error("unknown metric type: {0}")]
    UnknownType(String),

    #[error("invalid batch size: {0}")]
    InvalidBatchSize(usize),

    #[error("invalid batch timeout: {0}")]
    InvalidBatchTimeout(u64),

    #[error("sync mode is mutually exclusive with batch_size and batch_timeout")]
    SyncModeConflict,

    #[error("metric spec file not found: {0}")]
    SpecFileNotFound(String),

    #[error("metric spec file is not valid JSON: {0}")]
    SpecFileInvalidJson(String),

    #[error("label count mismatch: expected {expected}, got {got}")]
    LabelCountMismatch { expected: usize, got: usize },

    #[error("invalid method {method} for metric type {metric_type}")]
    InvalidMethod { method: String, metric_type: String },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
