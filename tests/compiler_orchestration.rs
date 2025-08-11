use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use prost::Message;
use tokio::sync::Mutex as AsyncMutex;

use playback_compiler::emit::{DeltaKeyBuilder, ReplaySink, SimpleKeyBuilder};
use playback_compiler::errors::CompilerError;
use playback_compiler::proto::Job;
use playback_compiler::transform::{decode::decode_job, encode::encode_many_ids_arrow_bytes};

/// Test doubles for downstream services.

#[derive(Clone, Default)]
struct FakeDLQ {
    /// Stored tuples of `(reason, job_id, payload)`.
    pub items: Arc<Mutex<Vec<(String, Option<String>, Bytes)>>>,
}
impl FakeDLQ {
    fn publish(&self, reason: &str, job_id: Option<&str>, payload: &Bytes) {
        self.items.lock().unwrap().push((
            reason.to_string(),
            job_id.map(|s| s.to_string()),
            payload.clone(),
        ));
    }
}

#[derive(Clone, Default)]
struct FakeIdem {
    /// Simulates Redis SETNX TTL via a set of processed job identifiers.
    set: Arc<AsyncMutex<HashSet<String>>>,
}
impl FakeIdem {
    async fn claim(&self, job_id: &str) -> Claim {
        let key = format!("processed:{job_id}");
        let mut s = self.set.lock().await;
        if s.contains(&key) {
            Claim::Duplicate
        } else {
            s.insert(key.clone());
            Claim::Fresh(key)
        }
    }
    async fn release(&self, key: String) {
        let mut s = self.set.lock().await;
        s.remove(&key);
    }
}

enum Claim {
    Fresh(String),
    Duplicate,
}

#[derive(Clone, Default)]
struct FakeSink {
    /// Recorded `(key, bytes)` pairs; can be configured to fail once.
    puts: Arc<Mutex<Vec<(String, Bytes)>>>,
    fail_once: Arc<Mutex<bool>>,
}
impl FakeSink {
    fn new_fail_once() -> Self {
        Self {
            puts: Default::default(),
            fail_once: Arc::new(Mutex::new(true)),
        }
    }
}
#[async_trait]
impl ReplaySink for FakeSink {
    type Error = CompilerError;
    async fn put_delta(&self, key: &str, bytes: Bytes) -> Result<(), Self::Error> {
        let mut fail = self.fail_once.lock().unwrap();
        if *fail {
            *fail = false;
            return Err(CompilerError::JobProcessingError("sink fail".into()));
        }
        self.puts.lock().unwrap().push((key.to_string(), bytes));
        Ok(())
    }
}

/// Minimal orchestrator used in tests.
/// Returns `(acknowledged, optional dlq_reason)`.
async fn process_one(
    payload: Bytes,
    // Added +Sync to satisfy ReplaySink bound used in production.
    sink: &(impl ReplaySink<Error = CompilerError> + Sync),
    key_builder: &SimpleKeyBuilder,
    idem: &FakeIdem,
    dlq: &FakeDLQ,
) -> (bool, Option<String>) {
    // Decode the incoming payload.
    let job = match decode_job(&payload) {
        Ok(j) => j,
        Err(_) => {
            dlq.publish("decode_error", None, &payload);
            // Acknowledge so the message is not retried.
            return (true, Some("decode_error".into()));
        }
    };

    // Ensure idempotency.
    match idem.claim(&job.id).await {
        Claim::Duplicate => (true, None),
        Claim::Fresh(idem_key) => {
            // Encode the job identifier.
            let id_bytes = Bytes::from(job.id.clone().into_bytes());
            let bytes = match encode_many_ids_arrow_bytes(&[id_bytes], false) {
                Ok(b) => b,
                Err(e) => {
                    idem.release(idem_key).await; // release idempotency key
                    dlq.publish("encode_error", Some(&job.id), &payload);
                    return (false, Some(format!("encode_error:{e:?}")));
                }
            };

            // Build the S3 object key.
            let s3_key = key_builder.replay_delta_key(&job, 1_712_345_678, 123);

            // Emit to the sink; on failure, release idempotency and DLQ.
            match sink.put_delta(&s3_key, Bytes::from(bytes)).await {
                Ok(_) => (true, None),
                Err(e) => {
                    idem.release(idem_key).await; // release idempotency key
                    dlq.publish("process_error", Some(&job.id), &payload);
                    (false, Some(format!("process_error:{e:?}")))
                }
            }
        }
    }
}

/// Helper to build a valid protobuf payload.
fn encode_job(id: &str) -> Bytes {
    let job = Job { id: id.to_string() };
    let mut buf = Vec::new();
    job.encode(&mut buf).unwrap();
    Bytes::from(buf)
}

/// Test cases.

#[tokio::test]
async fn decode_error_goes_to_dlq_and_acks() {
    let sink = FakeSink::default();
    let key_builder = SimpleKeyBuilder::new("tenants/default");
    let idem = FakeIdem::default();
    let dlq = FakeDLQ::default();

    // Provide invalid bytes to force a decode error.
    let (acked, reason) = process_one(
        Bytes::from_static(b"\x00\x01\x02"),
        &sink,
        &key_builder,
        &idem,
        &dlq,
    )
    .await;

    assert!(acked);
    assert_eq!(reason.as_deref(), Some("decode_error"));
    let dlq_items = dlq.items.lock().unwrap();
    assert_eq!(dlq_items.len(), 1);
    assert_eq!(dlq_items[0].0, "decode_error");
    assert!(dlq_items[0].1.is_none());
}

#[tokio::test]
async fn duplicate_job_is_acked_no_emit() {
    let sink = FakeSink::default();
    let key_builder = SimpleKeyBuilder::new("t/default");
    let idem = FakeIdem::default();
    let dlq = FakeDLQ::default();

    let (acked1, reason1) = process_one(encode_job("A"), &sink, &key_builder, &idem, &dlq).await;
    assert!(acked1);
    assert!(reason1.is_none());
    assert_eq!(sink.puts.lock().unwrap().len(), 1);

    // Second send should be treated as a duplicate and produce no new output.
    let (acked2, reason2) = process_one(encode_job("A"), &sink, &key_builder, &idem, &dlq).await;
    assert!(acked2);
    assert!(reason2.is_none());
    assert_eq!(
        sink.puts.lock().unwrap().len(),
        1,
        "no new puts for duplicate"
    );
}

#[tokio::test]
async fn sink_failure_releases_idem_and_goes_to_dlq() {
    let sink = FakeSink::new_fail_once();
    let key_builder = SimpleKeyBuilder::new("pfx");
    let idem = FakeIdem::default();
    let dlq = FakeDLQ::default();

    // First attempt fails; it is sent to DLQ and idempotency is released.
    let (acked1, reason1) = process_one(encode_job("B"), &sink, &key_builder, &idem, &dlq).await;
    assert!(!acked1);
    assert!(matches!(reason1, Some(r) if r.starts_with("process_error")));
    assert_eq!(dlq.items.lock().unwrap().len(), 1);

    // Second attempt succeeds after the one-shot failure.
    let (acked2, reason2) = process_one(encode_job("B"), &sink, &key_builder, &idem, &dlq).await;
    assert!(acked2);
    assert!(reason2.is_none());
    assert_eq!(sink.puts.lock().unwrap().len(), 1);
}
