use crate::protocol::{FileHandle, RpcError, RpcErrorCode};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DfsError {
    #[error("network error: {0}")]
    Network(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("rpc error [{code:?}]: {message}")]
    Rpc {
        code: RpcErrorCode,
        message: String,
        retryable: bool,
        current_handle: Option<FileHandle>,
    },

    #[error("protocol error: {0}")]
    Protocol(String),
}

impl DfsError {
    pub fn current_handle(&self) -> Option<FileHandle> {
        match self {
            Self::Rpc { current_handle, .. } => current_handle.clone(),
            _ => None,
        }
    }

    pub fn is_stale_handle(&self) -> bool {
        matches!(
            self,
            Self::Rpc {
                code: RpcErrorCode::StaleHandle,
                ..
            }
        )
    }
}

impl From<RpcError> for DfsError {
    fn from(value: RpcError) -> Self {
        Self::Rpc {
            code: value.code,
            message: value.message,
            retryable: value.retryable,
            current_handle: value.current_handle,
        }
    }
}
