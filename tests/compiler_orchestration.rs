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

/// ---- Fakes -----

#[derive(Clone, Default)]
struct FakeDLQ {
    // (reason, job_id, payload)
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
    // simulates Redis SETNX TTL; HashSet holds "processed:<job_id>"
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
    // records (key, bytes); can be told to fail once
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

/// ---- A tiny, pure “one message” orchestrator we drive in tests ----
/// Returns: ("ack" flag, maybe dlq_reason)
async fn process_one(
    payload: Bytes,
    // NOTE: add + Sync to satisfy ReplaySink bound used in prod
    sink: &(impl ReplaySink<Error = CompilerError> + Sync),
    key_builder: &SimpleKeyBuilder,
    idem: &FakeIdem,
    dlq: &FakeDLQ,
) -> (bool, Option<String>) {
    // decode
    let job = match decode_job(&payload) {
        Ok(j) => j,
        Err(_) => {
            dlq.publish("decode_error", None, &payload);
            return (true, Some("decode_error".into())); // ack on decode error
        }
    };

    // idempotency
    match idem.claim(&job.id).await {
        Claim::Duplicate => (true, None), // ack, no emit
        Claim::Fresh(idem_key) => {
            // encode (new API)
            let id_bytes = Bytes::from(job.id.clone().into_bytes());
            let bytes = match encode_many_ids_arrow_bytes(&[id_bytes], false) {
                Ok(b) => b,
                Err(e) => {
                    idem.release(idem_key).await; // release the *idempotency* key
                    dlq.publish("encode_error", Some(&job.id), &payload);
                    return (false, Some(format!("encode_error:{e:?}")));
                }
            };

            // build S3 key (separate name to avoid shadowing)
            let s3_key = key_builder.replay_delta_key(&job, 1_712_345_678, 123);

            // emit to sink; on failure, release idempotency and DLQ
            match sink.put_delta(&s3_key, Bytes::from(bytes)).await {
                Ok(_) => (true, None),
                Err(e) => {
                    idem.release(idem_key).await; // release the *idempotency* key
                    dlq.publish("process_error", Some(&job.id), &payload);
                    (false, Some(format!("process_error:{e:?}")))
                }
            }
        }
    }
}

/// ---- Helpers to build a valid protobuf payload ----
fn encode_job(id: &str) -> Bytes {
    let job = Job { id: id.to_string() };
    let mut buf = Vec::new();
    job.encode(&mut buf).unwrap();
    Bytes::from(buf)
}

/// ---- Tests ----

#[tokio::test]
async fn decode_error_goes_to_dlq_and_acks() {
    let sink = FakeSink::default();
    let key_builder = SimpleKeyBuilder::new("tenants/default");
    let idem = FakeIdem::default();
    let dlq = FakeDLQ::default();

    // garbage bytes
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

    // second time: duplicate → ack without emitting
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

    // first attempt fails in sink → not acked (we return false), DLQ + idempotency released
    let (acked1, reason1) = process_one(encode_job("B"), &sink, &key_builder, &idem, &dlq).await;
    assert!(!acked1);
    assert!(matches!(reason1, Some(r) if r.starts_with("process_error")));
    assert_eq!(dlq.items.lock().unwrap().len(), 1);

    // second attempt now succeeds (fail-once was consumed)
    let (acked2, reason2) = process_one(encode_job("B"), &sink, &key_builder, &idem, &dlq).await;
    assert!(acked2);
    assert!(reason2.is_none());
    assert_eq!(sink.puts.lock().unwrap().len(), 1);
}
