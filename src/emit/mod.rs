//! Emit module: traits and sink exports

use bytes::Bytes;

pub mod sink;
pub mod uploader;

#[async_trait::async_trait]
pub trait ReplaySink {
    type Error;
    async fn put_delta(&self, key: &str, bytes: Bytes) -> Result<(), Self::Error>;
}

/// Optional: key builder trait if you want to compute keys outside the sink.
/// With the coalescer, we mostly compute a window key inside the sink,
/// but we keep this here if other sinks need it.
pub trait DeltaKeyBuilder {
    fn replay_delta_key(
        &self,
        _job: &crate::proto::Job,
        now_unix_secs: u64,
        now_nanos: u32,
    ) -> String;
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
    fn replay_delta_key(
        &self,
        job: &crate::proto::Job,
        now_unix_secs: u64,
        now_nanos: u32,
    ) -> String {
        format!(
            "{}/replay-deltas/{}/{}-{:09}.arrow",
            self.prefix, job.id, now_unix_secs, now_nanos
        )
    }
}
