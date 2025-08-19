use anyhow::anyhow;
use bytes::Bytes;
use playback_compiler::emit::BlobStore;
use playback_compiler::proto::Job;
use playback_compiler::transform::decode::decode_job;
use prost::Message;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as AsyncMutex;
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
struct FakeStore {
    buf: Arc<Mutex<Vec<u8>>>,
    fail_once: Arc<Mutex<bool>>,
}
impl FakeStore {
    fn new() -> Self {
        Self {
            buf: Arc::new(Mutex::new(Vec::new())),
            fail_once: Arc::new(Mutex::new(false)),
        }
    }

    fn new_fail_once() -> Self {
        Self {
            buf: Default::default(),
            fail_once: Arc::new(Mutex::new(true)),
        }
    }
}

impl BlobStore for FakeStore {
    fn put_bytes<'a>(
        &'a self,
        _bucket: &'a str,
        _key: &'a str,
        data: Vec<u8>,
        _content_type: Option<&'a str>,
        _content_encoding: Option<&'a str>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send + 'a {
        async move {
            let mut fail = self.fail_once.lock().unwrap();
            if *fail {
                *fail = false;
                return Err(anyhow!("failure"));
            }
            let mut guard = self.buf.lock().unwrap();
            guard.extend_from_slice(&data);
            Ok(())
        }
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

/// Minimal orchestrator used in tests.
/// Returns `(acknowledged, optional dlq_reason)`.
async fn process_one(
    payload: Bytes,
    store: &impl BlobStore,
    idem: &FakeIdem,
    dlq: &FakeDLQ,
) -> (bool, Option<String>) {
    // Decode the incoming payload.
    let job = match decode_job(&payload) {
        Ok(j) => j,
        Err(_) => {
            dlq.publish("decode_error", None, &payload);
            return (true, Some("decode_error".into()));
        }
    };

    // Ensure idempotency.
    match idem.claim(&job.id).await {
        Claim::Duplicate => (true, None),
        Claim::Fresh(idem_key) => {
            // Prepare data as Vec<u8>
            let id_bytes = Bytes::from(job.id.clone().into_bytes());
            let data: Vec<u8> = id_bytes.to_vec();
            let bucket = "test-bucket";
            let key = format!("jobs/{}", job.id);

            // Call BlobStore::put_bytes correctly: data Vec<u8>, options are Option<&str>
            if let Err(e) = store.put_bytes(bucket, &key, data, None, None).await {
                // release idempotency key so a retry can succeed
                idem.release(idem_key).await;
                dlq.publish("encode_error", Some(&job.id), &payload);
                return (false, Some(format!("encode_error:{e:?}")));
            }

            // success keeps the idempotency claim
            (true, None)
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
    let store = FakeStore::new();
    let idem = FakeIdem::default();
    let dlq = FakeDLQ::default();

    // Provide invalid bytes to force a decode error.
    let (acked, reason) =
        process_one(Bytes::from_static(b"\x00\x01\x02"), &store, &idem, &dlq).await;

    assert!(acked);
    assert_eq!(reason.as_deref(), Some("decode_error"));
    let dlq_items = dlq.items.lock().unwrap();
    assert_eq!(dlq_items.len(), 1);
    assert_eq!(dlq_items[0].0, "decode_error");
    assert!(dlq_items[0].1.is_none());
}

#[tokio::test]
async fn duplicate_job_is_acked_no_emit() {
    let store = FakeStore::new();
    let idem = FakeIdem::default();
    let dlq = FakeDLQ::default();

    let (acked1, reason1) = process_one(encode_job("A"), &store, &idem, &dlq).await;
    assert!(acked1);
    assert!(reason1.is_none());
    assert_eq!(store.buf.lock().unwrap().len(), 1);

    // Second send should be treated as a duplicate and produce no new output.
    let (acked2, reason2) = process_one(encode_job("A"), &store, &idem, &dlq).await;
    assert!(acked2);
    assert!(reason2.is_none());
    assert_eq!(
        store.buf.lock().unwrap().len(),
        1,
        "no new puts for duplicate"
    );
}

#[tokio::test]
async fn sink_failure_releases_idem_and_goes_to_dlq() {
    let store = FakeStore::new_fail_once();
    let idem = FakeIdem::default();
    let dlq = FakeDLQ::default();

    // First attempt fails; it is sent to DLQ and idempotency is released.
    let (acked1, reason1) = process_one(encode_job("B"), &store, &idem, &dlq).await;
    assert!(!acked1);
    assert!(matches!(reason1, Some(r) if r.starts_with("process_error")));
    assert_eq!(dlq.items.lock().unwrap().len(), 1);

    // Second attempt succeeds after the one-shot failure.
    let (acked2, reason2) = process_one(encode_job("B"), &store, &idem, &dlq).await;
    assert!(acked2);
    assert!(reason2.is_none());
    assert_eq!(store.buf.lock().unwrap().len(), 1);
}
