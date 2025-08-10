use deadpool_redis::Pool;
use playback_compiler::config::load_config;
use playback_compiler::errors::CompilerError;
use playback_compiler::ingest::Queue;
use playback_compiler::redis::{init_redis_pool, pool as redis_pool, RedisStreamQueue};

use playback_compiler::emit::uploader::{create_s3_client_from_env, S3ReplaySink};
use playback_compiler::emit::{emit_replay_delta, SimpleKeyBuilder};

use playback_compiler::transform::decode::decode_job;
use playback_compiler::transform::encode::encode_replay_delta_arrow;

use playback_compiler::types::fp::{idempotency_claim, idempotency_release, publish_dlq, Idem};

use tokio::signal;
use tracing::{error, info, instrument};
use tracing_error::ErrorLayer;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

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
    let cfg = load_config()?;
    init_redis_pool(&cfg.redis_url).await?;

    let pool: Pool = redis_pool().clone();
    let queue = {
        let stream = cfg.job_queue_name.clone();
        let group = "compilers".to_string();
        let consumer = format!("compiler-{}", std::process::id());
        RedisStreamQueue::new(pool.clone(), stream, group, consumer)
    };

    let s3 = create_s3_client_from_env().await;
    let sink = S3ReplaySink::new(s3, cfg.s3_bucket_name.clone());
    let key_builder = SimpleKeyBuilder::new(&cfg.s3_key);
    let dlq_stream = format!("{}:dlq", cfg.job_queue_name);
    let ttl = cfg.idempotency_ttl_secs;

    tokio::select! {
        res = run(queue, pool.clone(), sink, key_builder, dlq_stream, ttl) => { if let Err(e) = res { error!(err=%e, "compiler loop error"); } }
        _ = signal::ctrl_c() => { info!("ctrl+c received, shutting down"); }
    }
    Ok(())
}

#[instrument(skip(queue, pool, sink, key_builder))]
async fn run(
    queue: RedisStreamQueue,
    pool: Pool,
    sink: S3ReplaySink,
    key_builder: SimpleKeyBuilder,
    dlq_stream: String,
    ttl: usize,
) -> Result<(), CompilerError> {
    queue.ensure_stream_group().await?;
    loop {
        let Some(msg) = queue.pop().await? else { continue };

        let job = match decode_job(&msg.payload) {
            Ok(j) => j,
            Err(_) => {
                publish_dlq(&pool, &dlq_stream, "decode_error", None, &msg.payload).await?;
                queue.ack(&msg.id).await?;
                continue;
            }
        };

        match idempotency_claim(&pool, &job.id, ttl).await? {
            Idem::Duplicate => {
                queue.ack(&msg.id).await?;
                continue;
            }
            Idem::Fresh(key) => {
                let result = (|| async {
                    let bytes = encode_replay_delta_arrow(&job)?;
                    emit_replay_delta(&sink, &key_builder, &job, bytes).await
                })()
                .await;

                match result {
                    Ok(_) => {
                        queue.ack(&msg.id).await?;
                    }
                    Err(e) => {
                        idempotency_release(&pool, key).await;
                        publish_dlq(
                            &pool,
                            &dlq_stream,
                            "process_error",
                            Some(&job.id),
                            &msg.payload,
                        )
                        .await?;
                        return Err(e);
                    }
                }
            }
        }
    }
}
