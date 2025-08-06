use deadpool_redis::redis::AsyncCommands;
use deadpool_redis::Pool;
use playback_compiler::config::load_config;
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
    tokio::select! {
        _ = run_compiler_loop(pool) => {},
        _ = signal::ctrl_c() => {
            println!("Received Ctrl+C, shutting down.");
        },
    }

    Ok(())
}
async fn run_compiler_loop(pool: &Pool) {
    let mut conn = match pool.get().await {
        Ok(conn) => conn,
        Err(err) => {
            eprintln!("Failed to get Redis connection: {}", err);
            return;
        }
    };

    let queue_name = "job_queue"; // replace with your actual Redis key

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
                Ok(job) => println!("Got job with ID: {}", job.id),
                Err(err) => eprintln!("Failed to decode protobuf: {}", err),
            }
        }
    }
}
