//! Helper functions for uploading objects to S3.

use anyhow::Result;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use futures::StreamExt;
use std::sync::Arc;

/// Construct an S3 client using environment configuration.
pub async fn create_s3_client_from_env() -> Client {
    dotenvy::dotenv().ok();
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
    let cfg = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(region))
        .load()
        .await;
    Client::new(&cfg)
}

/// Upload a single object to S3.
pub async fn upload_bytes_to_s3(
    client: &Client,
    bucket: &str,
    key: &str,
    data: Vec<u8>,
    content_type: Option<&str>,
    content_encoding: Option<&str>,
) -> Result<()> {
    let len = data.len() as i64;
    let mut req = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .content_length(len)
        .body(ByteStream::from(data));

    if let Some(ct) = content_type {
        req = req.content_type(ct);
    }
    if let Some(ce) = content_encoding {
        req = req.content_encoding(ce);
    }

    req.send().await?;
    Ok(())
}

/// Upload pre-chunked parts using S3 multipart semantics.
/// - `parts`: `(part_number, bytes)` tuples.
/// - `parallel_parts`: number of concurrent part uploads.
/// - `content_type` / `content_encoding`: object metadata applied at completion.
pub async fn multipart_upload(
    client: &Client,
    bucket: &str,
    key: &str,
    parts: Vec<(i32, Vec<u8>)>,
    parallel_parts: usize,
    content_type: Option<&str>,
    content_encoding: Option<&str>,
) -> Result<()> {
    // Initiate the multipart upload and set object metadata.
    let mut init = client.create_multipart_upload().bucket(bucket).key(key);
    if let Some(ct) = content_type {
        init = init.content_type(ct);
    }
    if let Some(ce) = content_encoding {
        init = init.content_encoding(ce);
    }
    let init = init.send().await?;
    let upload_id_master = Arc::new(init.upload_id().unwrap_or_default().to_string());

    // Upload each part concurrently; every task clones the upload ID.
    let completed_parts: Result<Vec<CompletedPart>> = {
        let futs = parts.into_iter().map(|(part_number, buf)| {
            let client = client.clone();
            let bucket = bucket.to_string();
            let key = key.to_string();
            let upload_id_master = upload_id_master.clone();

            async move {
                let len = buf.len() as i64;
                let etag = client
                    .upload_part()
                    .bucket(&bucket)
                    .key(&key)
                    .upload_id((*upload_id_master).clone()) // Clone upload ID for each request
                    .part_number(part_number)
                    .content_length(len)
                    .body(ByteStream::from(buf))
                    .send()
                    .await?
                    .e_tag
                    .unwrap_or_default();

                Ok::<_, anyhow::Error>(
                    CompletedPart::builder()
                        .part_number(part_number)
                        .e_tag(etag)
                        .build(),
                )
            }
        });

        let mut stream = tokio_stream::iter(futs).buffer_unordered(parallel_parts.max(1));
        let mut acc = Vec::new();
        while let Some(res) = stream.next().await {
            let part = res?;
            acc.push(part);
        }
        acc.sort_by_key(|p| p.part_number().unwrap_or_default());
        Ok(acc)
    };

    // Finalize the upload or abort on failure.
    match completed_parts {
        Ok(parts) => {
            let completed = CompletedMultipartUpload::builder()
                .set_parts(Some(parts))
                .build();

            client
                .complete_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id_master.as_str())
                .multipart_upload(completed)
                .send()
                .await?;
            Ok(())
        }
        Err(e) => {
            let _ = client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id_master.as_str())
                .send()
                .await;
            Err(e)
        }
    }
}
