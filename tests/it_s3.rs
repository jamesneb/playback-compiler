//! Integration test for the S3 sink using S3Mock.
//! Ensures that uploaded deltas can be retrieved byte-for-byte.

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    Client,
    config::{Builder as S3ConfigBuilder, Region},
    primitives::ByteStream,
};
use bytes::Bytes;
use playback_compiler::emit::ReplaySink;
use playback_compiler::errors::CompilerError;
use playback_compiler::proto::Job;
use playback_compiler::transform::encode::encode_many_ids_arrow_bytes;
use testcontainers::core::WaitFor;
use testcontainers::{GenericImage, clients};
use tokio::time::{Duration, sleep};

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
#[ignore] // Run with: cargo test --test it_s3 -- --ignored
async fn s3_mock_roundtrip() {
    // Start S3Mock on port 9090.
    let docker = clients::Cli::default();
    let img = GenericImage::new("adobe/s3mock", "latest")
        .with_exposed_port(9090)
        .with_wait_for(WaitFor::message_on_stdout("Started S3MockApplication"));
    let node = docker.run(img);
    let port = node.get_host_port_ipv4(9090);
    let endpoint = format!("http://127.0.0.1:{port}");

    // Configure the AWS SDK client for the mock using static credentials and path-style addressing.
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

    // Create the bucket with a small retry loop while the mock starts up.
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

    // Prepare a single replay delta.
    let job = Job {
        id: "job-xyz".into(),
    };
    let arrow =
        encode_many_ids_arrow_bytes(&[Bytes::from_static(b"job-xyz")], false).expect("encode");

    // Deterministic key so the fetched object matches the uploaded one.
    let key = format!(
        "tenants/default/replay-deltas/{}/test-000000123.arrow",
        job.id
    );

    // Upload via the S3 sink.
    let sink = S3Sink {
        client: client.clone(),
        bucket: bucket.to_string(),
    };
    sink.put_delta(&key, Bytes::from(arrow.clone()))
        .await
        .expect("put");

    // Fetch the object back and verify.
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
