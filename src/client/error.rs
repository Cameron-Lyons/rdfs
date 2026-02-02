use thiserror::Error;

#[derive(Error, Debug)]
pub enum DfsError {
    #[error("network error: {0}")]
    Network(String),

    #[error("file not found: {0}")]
    NotFound(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("unknown error")]
    Unknown,
}
