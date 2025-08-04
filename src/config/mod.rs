use dotenvy::dotenv;
use serde::Deserialize;
use std::env;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub redis_url: String,
}

pub fn load_config() -> Result<Config, Box<dyn std::error::Error>> {
    dotenv().ok();
    let redis_url = env::var("REDIS_URL")?;
    Ok(Config { redis_url })
}
