//! S3 client utilities
//!
//! Overview
//! --------
//! Constructs an AWS S3 client from environment (optionally pointing to local
//! stacks) and provides simple upload helpers. Higher-level Arrow encoding lives
//! in the transform layer.
//! S3 client utilities
//!
//! Constructs an AWS S3 client from environment (optionally pointing to local
//! stacks) and provides simple upload helpers.

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

pub async fn create_s3_client_from_env() -> Client {
    dotenvy::dotenv().ok();

    let shared = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(
            std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into()),
        ))
        .load()
        .await;

    Client::new(&shared)
}

pub async fn upload_to_s3(
    client: &Client,
    bucket: &str,
    key: &str,
    data: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let len = data.len() as i64;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .content_length(len)
        .body(ByteStream::from(data))
        .send()
        .await?;
    Ok(())
}
