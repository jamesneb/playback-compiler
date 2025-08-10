//! Ingest abstraction
//!
//! Overview
//! --------
//! Minimal trait representing a source of messages for the compiler. Concrete
//! implementations include Redis Streams.

use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct QueueMessage {
    pub id: String,
    pub payload: Bytes,
}

#[async_trait::async_trait]
pub trait Queue {
    type Error;

    async fn pop(&self) -> Result<Option<QueueMessage>, Self::Error>;

    /// Acknowledge successful processing so the backend can drop/redeliver accordingly.
    async fn ack(&self, id: &str) -> Result<(), Self::Error>;
}
