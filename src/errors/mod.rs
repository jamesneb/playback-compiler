use thiserror::Error;

#[derive(Error, Debug)]
pub enum CompilerError {
    #[error("Redis initialization failed: {0}")]
    RedisInit(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Redis pool error: {0}")]
    RedisPoolError(String),

    #[error("Job processing error: {0}")]
    JobProcessingError(String),

    #[error("Queue message decode error: {0}")]
    Decode(String),

    #[error("Unknown error: {0}")]
    Unknown(#[from] Box<dyn std::error::Error + Send + Sync>),
}
