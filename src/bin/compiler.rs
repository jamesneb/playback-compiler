use playback_compiler::config::load_config;
use playback_compiler::errors::CompilerError;
use playback_compiler::redis::init_redis_pool;
use tokio::signal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Compiler starting...");

    let config = load_config().expect("Failed to load config");
    let redis_pool = init_redis_pool(&config.redis_url)
        .await
        .map_err(|e| CompilerError::RedisInit(e.to_string()))?;

    tokio::select! {
        _ = run_compiler_loop() => {},
        _ = signal::ctrl_c() => {
            println!("Received Ctrl+C, shutting down.");
        },
    }

    Ok(())
}

async fn run_compiler_loop() {
    loop {
        // TODO: Your job handling logic here
        println!("Waiting for job...");
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}
