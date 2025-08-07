use aws_sdk_s3::Client;
use deadpool_redis::redis::AsyncCommands;
use deadpool_redis::Pool;
use playback_compiler::config::load_config;
use playback_compiler::emit::uploader::{create_s3_client_from_env, upload_to_s3};
use playback_compiler::emit::write_to_arrow;
use playback_compiler::errors::CompilerError;
use playback_compiler::redis::get_pool;
use playback_compiler::redis::init_redis_pool;
use prost::Message;
use tokio::signal;

pub mod proto {
    include!("../proto/job_proto.rs");
}

use proto::Job;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Compiler starting...");

    let config = load_config().expect("Failed to load config");
    init_redis_pool(&config.redis_url)
        .await
        .map_err(|e| CompilerError::RedisInit(e.to_string()))?;

    let pool = get_pool();
    let store = create_s3_client_from_env().await;
    tokio::select! {
        _ = run_compiler_loop(pool, store, &config) => {},
        _ = signal::ctrl_c() => {
            println!("Received Ctrl+C, shutting down.");
        },
    }

    Ok(())
}
async fn run_compiler_loop(pool: &Pool, store: Client, cfg: &playback_compiler::config::Config) {
    let mut conn = match pool.get().await {
        Ok(conn) => conn,
        Err(err) => {
            eprintln!("Failed to get Redis connection: {}", err);
            return;
        }
    };

    let queue_name = &cfg.job_queue_name;
    let bucket = &cfg.s3_bucket_name;
    let key = &cfg.s3_key;

    loop {
        println!("Waiting for job...");

        let result: Option<(String, Vec<u8>)> = match conn.blpop(queue_name, 0.0).await {
            Ok(job) => job,
            Err(err) => {
                eprintln!("Redis error: {}", err);
                continue;
            }
        };

        if let Some((_key, bytes)) = result {
            match Job::decode(&*bytes) {
                Ok(job) => {
                    match compile(&job.id) {
                        Ok(arrow_bytes) => {
                            match upload_to_s3(&store, bucket, key, arrow_bytes).await {
                                Ok(_) => {
                                    println!("Successfully uploaded arrow file to S3: s3://{}/{}", bucket, key);
                                },
                                Err(e) => {
                                    eprintln!("Failed to upload to S3: {}", e);
                                    eprintln!("Upload failed for job ID: {}", job.id);
                                    eprintln!("Target bucket: {}, key: {}", bucket, key);
                                }
                            }
                        }
                        Err(e) => eprintln!("Failed to compile job: {}", e),
                    }
                }
                Err(err) => eprintln!("Failed to decode protobuf: {}", err),
            }
        }
    }
}

fn compile(id: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    println!("Got job with ID: {}", id);
    
    // Write arrow data to memory
    let arrow_bytes = write_to_arrow(id)?;
    println!("Generated {} bytes of arrow data", arrow_bytes.len());
    
    Ok(arrow_bytes)
}
