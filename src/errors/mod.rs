//! Error types for playback-compiler
//!
//! Overview
//! --------
//! Canonical error enumeration used across ingestion, transform, and emit
//! layers. Keep variants stable and descriptive; prefer mapping external
//! libraries into these variants at module boundaries.
//!
//! Usage
//! -----
//! - Convert low-level errors at the edge (e.g., Redis/S3/Arrow/Protobuf).
//! - Avoid leaking third-party error types across crate boundaries.
//!
//! Concurrency / Logging
//! ---------------------
//! Errors are `Send + Sync` and implement Display via `thiserror`.
//! Use `tracing` for context at call sites (`error!(...);`).
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CompilerError {
    /// Failure to initialize external services (pool creation, client wiring).
    #[error("Redis initialization failed: {0}")]
    RedisInit(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Pooled command/IO failures or protocol-level errors from Redis.
    #[error("Redis pool error: {0}")]
    RedisPoolError(String),

    /// Higher-level processing failure during job handling or emission.
    #[error("Job processing error: {0}")]
    JobProcessingError(String),

    /// Failed to parse/deserialize inbound payloads (e.g., protobuf).
    #[error("Queue message decode error: {0}")]
    Decode(String),

    #[error("Unknown error: {0}")]
    Unknown(#[from] Box<dyn std::error::Error + Send + Sync>),
}
