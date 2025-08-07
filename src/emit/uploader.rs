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
