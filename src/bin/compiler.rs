//! playback-compiler: worker entrypoint
//!
//! Overview
//! --------
//! Orchestrates the compiler worker: connects to Redis Streams, receives jobs,
//! decodes them, emits replay deltas to the storage sink, and acknowledges
//! successfully processed messages. Failures are logged and can be routed to a
//! dead-letter stream by the caller if desired.
//!
//! Responsibilities
//! ----------------
//! - Initialize logging, configuration, Redis pool, and S3 client.
//! - Drive the receive → process → ack loop with graceful shutdown.
//!
//! Error Model
//! -----------
//! - Initialization failures are fatal.
//! - Per-message failures are logged and do not terminate the loop.
//!
//! Concurrency / Performance
//! -------------------------
//! - Single async loop; Redis Stream consumer groups provide backpressure.
//! - S3 uploads are awaited; batching can be introduced within the sink layer.

//! playback-compiler: worker entrypoint (batched, concurrency-limited)

//! playback-compiler: worker entrypoint (batched, concurrency-limited)

//! playback-compiler: worker entrypoint (batched, concurrency-limited)

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_s3::Client;
use tokio::signal;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{error, info};
use tracing_error::ErrorLayer;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use playback_compiler::config::load_config;
use playback_compiler::emit::uploader::{create_s3_client_from_env, upload_to_s3};
use playback_compiler::emit::{DeltaKeyBuilder, SimpleKeyBuilder};
use playback_compiler::errors::CompilerError;
use playback_compiler::proto::Job;
use playback_compiler::redis::{init_redis_pool, pool, RedisStreamQueue};
use playback_compiler::transform::decode::decode_job;
use playback_compiler::transform::encode::encode_replay_delta_arrow;

pub fn init_logging() {
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(fmt::layer().compact())
        .with(ErrorLayer::default())
        .init();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();
    info!("compiler starting");

    let config = load_config().expect("failed to load config");

    init_redis_pool(&config.redis_url)
        .await
        .map_err(|e| CompilerError::RedisInit(e.to_string()))?;

    let s3 = create_s3_client_from_env().await;

    let queue = RedisStreamQueue::new(pool().clone(), &config.job_queue_name, "compilers", "c1");
    queue.ensure_stream_group().await?;

    tokio::select! {
        _ = run_loop(queue, s3, &config) => {},
        _ = signal::ctrl_c() => {
            info!("shutdown requested");
        }
    }

    Ok(())
}

async fn run_loop(queue: RedisStreamQueue, s3: Client, cfg: &playback_compiler::config::Config) {
    let permits = std::thread::available_parallelism()
        .map(|n| n.get() * 2)
        .unwrap_or(8);
    let semaphore = Arc::new(Semaphore::new(permits));

    const BATCH: usize = 256;
    let key_builder = SimpleKeyBuilder::new("tenants/default");

    loop {
        let batch = match queue.pop_batch(BATCH).await {
            Ok(v) => v,
            Err(e) => {
                error!(err = %e, "batch pop failed");
                continue;
            }
        };

        if batch.is_empty() {
            continue;
        }

        let mut join = JoinSet::new();
        for msg in batch {
            let s3 = s3.clone();
            let bucket = cfg.s3_bucket_name.clone();
            let kb = key_builder.clone();
            let sem = semaphore.clone();

            join.spawn(async move {
                let _permit = sem.acquire_owned().await.unwrap();

                let job: Job = match decode_job(&msg.payload) {
                    Ok(j) => j,
                    Err(e) => {
                        return (
                            msg.id,
                            Err(CompilerError::JobProcessingError(format!("decode: {e}"))),
                        )
                    }
                };

                let arrow = match encode_replay_delta_arrow(&job) {
                    Ok(b) => b,
                    Err(e) => {
                        return (
                            msg.id,
                            Err(CompilerError::JobProcessingError(format!("encode: {e}"))),
                        )
                    }
                };

                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                let key = kb.replay_delta_key(&job, now.as_secs() as u64, now.subsec_nanos());

                let res = upload_to_s3(&s3, &bucket, &key, arrow)
                    .await
                    .map_err(|e| CompilerError::JobProcessingError(format!("s3 put: {e}")));

                (msg.id, res)
            });
        }

        let mut success_ids: Vec<String> = Vec::new();
        while let Some(res) = join.join_next().await {
            match res {
                Ok((id, Ok(()))) => success_ids.push(id),
                Ok((id, Err(e))) => error!(id = %id, err = %e, "job failed"),
                Err(e) => error!(err = %e, "task join error"),
            }
        }

        if let Err(e) = queue.ack_many(&success_ids).await {
            error!(count = success_ids.len(), err = %e, "ack_many failed");
        }
    }
}
