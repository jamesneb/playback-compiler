use dotenvy::dotenv;
use serde::Deserialize;
use std::env;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub redis_url: String,
    pub job_queue_name: String,
    pub file_out_path: String,
    pub s3_bucket_name: String,
    pub s3_key: String,
}

pub fn load_config() -> Result<Config, Box<dyn std::error::Error>> {
    dotenv().ok();
    let redis_url = env::var("REDIS_URL")?;
    let job_queue_name = env::var("JOB_QUEUE_NAME")?;
    let file_out_path = env::var("FILE_OUT_PATH")?;
    let s3_bucket_name = env::var("S3_BUCKET_NAME")?;
    let s3_key = env::var("S3_KEY")?;
    Ok(Config {
        redis_url,
        job_queue_name,
        file_out_path,
        s3_bucket_name,
        s3_key,
    })
}
