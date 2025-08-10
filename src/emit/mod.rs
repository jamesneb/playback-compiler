use crate::{errors::CompilerError, proto::Job};
use bytes::Bytes;

pub mod dlq;
pub mod uploader;

use async_trait::async_trait;

#[async_trait]
pub trait ReplaySink {
    type Error;
    async fn put_delta(&self, key: &str, bytes: Bytes) -> Result<(), Self::Error>;
}

pub trait DeltaKeyBuilder {
    fn replay_delta_key(&self, job: &Job, now_unix_secs: u64, now_nanos: u32) -> String;
}

#[derive(Clone)]
pub struct SimpleKeyBuilder {
    pub prefix: String,
}

impl SimpleKeyBuilder {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }
}

impl DeltaKeyBuilder for SimpleKeyBuilder {
    fn replay_delta_key(&self, job: &Job, s: u64, n: u32) -> String {
        format!(
            "{}/replay-deltas/{}/{}-{:09}.arrow",
            self.prefix, job.id, s, n
        )
    }
}

pub async fn emit_replay_delta<S: ReplaySink + Sync>(
    sink: &S,
    key_builder: &impl DeltaKeyBuilder,
    job: &Job,
    bytes: Bytes,
) -> Result<(), CompilerError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let key = key_builder.replay_delta_key(job, now.as_secs(), now.subsec_nanos());
    sink.put_delta(&key, bytes)
        .await
        .map_err(|_| CompilerError::JobProcessingError("replay sink put failed".into()))
}
