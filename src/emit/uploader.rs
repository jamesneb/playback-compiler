use aws_config::BehaviorVersion;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use std::env;
use std::error::Error;

pub async fn create_s3_client_from_env() -> Client {
    dotenvy::dotenv().ok();

    println!("Loading AWS configuration from environment...");
    
    // Set environment variables for AWS SDK to pick up
    unsafe {
        let access_key = env::var("AWS_ACCESS_KEY_ID").expect("Missing AWS_ACCESS_KEY_ID");
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY").expect("Missing AWS_SECRET_ACCESS_KEY");
        let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        
        println!("AWS_ACCESS_KEY_ID: {}***", &access_key[..access_key.len().min(4)]);
        println!("AWS_SECRET_ACCESS_KEY: [REDACTED]");
        println!("AWS_REGION: {}", region);
        
        env::set_var("AWS_ACCESS_KEY_ID", access_key);
        env::set_var("AWS_SECRET_ACCESS_KEY", secret_key);
        env::set_var("AWS_DEFAULT_REGION", region);
    }

    let mut config_loader = aws_config::defaults(BehaviorVersion::latest());

    if let Ok(endpoint_url) = env::var("S3_URL") {
        println!("Using custom S3 endpoint: {}", endpoint_url);
        config_loader = config_loader.endpoint_url(endpoint_url);
    }

    let shared_config = config_loader.load().await;
    println!("AWS SDK configuration loaded successfully");
    Client::new(&shared_config)
}

pub async fn append_to_s3_arrow(
    client: &Client,
    bucket: &str,
    key: &str,
    new_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Attempting to append job ID '{}' to s3://{}/{}", new_id, bucket, key);
    
    // Try to get existing file size/metadata first
    let existing_data = match client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
    {
        Ok(_) => {
            println!("File exists, will download and append");
            // File exists, need to download and merge
            match client
                .get_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
            {
                Ok(response) => {
                    let data = response.body.collect().await?.into_bytes().to_vec();
                    Some(data)
                },
                Err(e) => {
                    eprintln!("Failed to download existing file: {:?}", e);
                    None
                }
            }
        },
        Err(_) => {
            println!("File doesn't exist, creating new one");
            None
        }
    };
    
    // Use our append function to merge the data
    let merged_data = crate::emit::append_to_arrow(new_id, existing_data.as_deref())?;
    
    // Upload the merged file
    let body = ByteStream::from(merged_data);
    
    match client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .send()
        .await 
    {
        Ok(_) => {
            println!("Successfully appended and uploaded to S3");
            Ok(())
        },
        Err(e) => {
            eprintln!("S3 upload failed: {:?}", e);
            Err(e.into())
        }
    }
}

pub async fn upload_to_s3(
    client: &Client,
    bucket: &str,
    key: &str,
    data: Vec<u8>,
) -> Result<(), aws_sdk_s3::Error> {
    println!("Attempting to upload {} bytes to s3://{}/{}", data.len(), bucket, key);
    
    let body = ByteStream::from(data);
    
    match client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .send()
        .await 
    {
        Ok(_) => {
            println!("Successfully uploaded to S3");
            Ok(())
        },
        Err(e) => {
            eprintln!("S3 upload failed - Error details: {:?}", e);
            eprintln!("S3 upload failed - Error source: {:?}", e.source());
            eprintln!("S3 upload failed - Error metadata: {:?}", e.meta());
            Err(e.into())
        }
    }
}
