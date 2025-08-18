//! Abstractions for message ingestion.
//!
//! Defines a minimal interface for sources that supply work items to the
//! compiler. Implementations include adapters for Redis Streams and others.

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

    /// Confirm that a message has been processed so the backend can
    /// perform its retention or redelivery policy.
    async fn ack(&self, id: &str) -> Result<(), Self::Error>;
}
