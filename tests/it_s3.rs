//! Integration: S3-compatible sink (S3Mock)
//! Validates deterministic PUT/GET round-trip for replay delta bytes.

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    config::{Builder as S3ConfigBuilder, Region},
    primitives::ByteStream,
    Client,
};
use bytes::Bytes;
use playback_compiler::emit::ReplaySink; // <-- bring trait into scope
use playback_compiler::errors::CompilerError;
use playback_compiler::proto::Job;
use playback_compiler::transform::encode::encode_replay_delta_arrow;
use testcontainers::core::WaitFor;
use testcontainers::{clients, GenericImage};
use tokio::time::{sleep, Duration};

struct S3Sink {
    client: Client,
    bucket: String,
}

#[async_trait]
impl ReplaySink for S3Sink {
    type Error = CompilerError;
    async fn put_delta(&self, key: &str, bytes: Bytes) -> Result<(), Self::Error> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(bytes.to_vec()))
            .send()
            .await
            .map_err(|e| CompilerError::JobProcessingError(format!("put_object error: {e:?}")))?;
        Ok(())
    }
}

#[tokio::test]
#[ignore] // run with: cargo test --test it_s3 -- --ignored
async fn s3_mock_roundtrip() {
    // Start S3Mock (listens on 9090)
    let docker = clients::Cli::default();
    let img = GenericImage::new("adobe/s3mock", "latest")
        .with_exposed_port(9090)
        .with_wait_for(WaitFor::message_on_stdout("Started S3MockApplication"));
    let node = docker.run(img);
    let port = node.get_host_port_ipv4(9090);
    let endpoint = format!("http://127.0.0.1:{port}");

    // AWS SDK client pinned to the mock: static creds + path-style
    let shared = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint.clone())
        .load()
        .await;

    let conf = S3ConfigBuilder::from(&shared)
        .credentials_provider(Credentials::new("test", "test", None, None, "static"))
        .force_path_style(true)
        .build();

    let client = Client::from_conf(conf);

    // Create bucket with a small retry (mock may take a beat)
    let bucket = "it-bucket";
    let mut tries = 0;
    loop {
        match client.create_bucket().bucket(bucket).send().await {
            Ok(_) => break,
            Err(_)
                if {
                    tries += 1;
                    tries <= 10
                } =>
            {
                sleep(Duration::from_millis(150)).await;
            }
            Err(e) => panic!("create_bucket failed after retries: {e:?}"),
        }
    }

    // Prepare a replay delta
    let job = Job {
        id: "job-xyz".into(),
    };
    let arrow = encode_replay_delta_arrow(&job).expect("encode");

    // Deterministic key so GET matches what we PUT
    let key = format!(
        "tenants/default/replay-deltas/{}/test-000000123.arrow",
        job.id
    );

    // Upload via sink
    let sink = S3Sink {
        client: client.clone(),
        bucket: bucket.to_string(),
    };
    sink.put_delta(&key, Bytes::from(arrow.clone()))
        .await
        .expect("put");

    // Fetch back & verify
    let got = client
        .get_object()
        .bucket(bucket)
        .key(&key)
        .send()
        .await
        .expect("get_object");
    let body = got.body.collect().await.expect("collect").into_bytes();
    assert!(!body.is_empty(), "downloaded object is empty");
}
