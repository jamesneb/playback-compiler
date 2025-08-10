use aws_config::BehaviorVersion;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use bytes::Bytes;
use std::env;
use tracing::{error, info};

use super::ReplaySink;
use crate::errors::CompilerError;

pub async fn create_s3_client_from_env() -> Client {
    let mut loader = aws_config::defaults(BehaviorVersion::latest());
    if let Ok(endpoint_url) = env::var("S3_URL") {
        loader = loader.endpoint_url(endpoint_url);
    }
    let shared = loader.load().await;
    Client::new(&shared)
}

#[derive(Clone)]
pub struct S3ReplaySink {
    client: Client,
    bucket: String,
}

impl S3ReplaySink {
    pub fn new(client: Client, bucket: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
        }
    }
}

#[async_trait::async_trait]
impl ReplaySink for S3ReplaySink {
    type Error = CompilerError;

    async fn put_delta(&self, key: &str, bytes: Bytes) -> Result<(), Self::Error> {
        let body = ByteStream::from(bytes);
        let resp = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type("application/vnd.apache.arrow.file")
            .body(body)
            .send()
            .await;

        match resp {
            Ok(_) => {
                info!(bucket=%self.bucket, key=%key, "delta uploaded");
                Ok(())
            }
            Err(e) => {
                error!(bucket=%self.bucket, key=%key, err=?e, "delta upload failed");
                Err(CompilerError::JobProcessingError(e.to_string()))
            }
        }
    }
}
