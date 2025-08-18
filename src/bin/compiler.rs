//! Command-line entry point for the compiler service.

use std::sync::Arc;

use playback_compiler::app::run;
use playback_compiler::config::load_config;
use playback_compiler::redis::{RedisControlPlane, RedisStreamQueue};
use playback_compiler::s3::S3Client;
use playback_compiler::util::ids::unique_consumer;

use tracing::{error, info};
use tracing_error::ErrorLayer;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();

    let cfg = Arc::new(load_config()?);
    info!("compiler starting; queue={}", cfg.job_queue_name);

    // Backends
    let store = S3Client::from_env().await;
    let queue = RedisStreamQueue::from_url(
        &cfg.job_queue_name,
        "compilers",
        &unique_consumer(),
        &cfg.redis_url,
    )?;
    queue.ensure_stream_group().await?;

    // Control-plane (idempotency + DLQ) shares the same pool
    let cp = Arc::new(RedisControlPlane::new(queue.pool_clone(), "dlq:compiler"));

    // Run (monomorphized over Queue + BlobStore)
    let permits = cfg.pipeline_parallelism.max(1);
    if let Err(e) = run(queue, store, cp, cfg.clone(), permits).await {
        error!(error=?e, "fatal error");
        return Err(e);
    }

    info!("compiler stopped");
    Ok(())
}

fn init_logging() {
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(fmt::layer().compact())
        .with(ErrorLayer::default())
        .init();
}
