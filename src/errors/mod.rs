//! Shared error definitions for the compiler.
//!
//! These variants provide a stable mapping from external failures to a
//! cohesive error surface. Keep descriptions concise and convert third-party
//! errors at module boundaries.
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CompilerError {
    /// Redis connection pool failed to initialize.
    #[error("Redis initialization failed: {0}")]
    RedisInit(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Errors arising from commands executed through the Redis pool.
    #[error("Redis pool error: {0}")]
    RedisPoolError(String),

    /// Application-level failures during job handling or emission.
    #[error("Job processing error: {0}")]
    JobProcessingError(String),

    /// Issues decoding inbound messages such as protobuf payloads.
    #[error("Queue message decode error: {0}")]
    Decode(String),

    #[error("Unknown error: {0}")]
    Unknown(#[from] Box<dyn std::error::Error + Send + Sync>),
}
