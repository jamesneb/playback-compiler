// src/bin/compiler.rs

use bytes::Bytes;
use playback_compiler::config::load_config;
use playback_compiler::emit::uploader::{create_s3_client_from_env, upload_bytes_to_s3};
use playback_compiler::ingest::Queue;
use playback_compiler::proto::Job;
use playback_compiler::redis::{init_redis_pool, pool, RedisStreamQueue};
use playback_compiler::transform::decode::decode_job;
use playback_compiler::transform::encode::encode_many_ids_arrow_bytes;
use playback_compiler::types::fp::{idempotency_claim, idempotency_release, publish_dlq, Idem};

use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::signal;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{error, info};
use tracing_error::ErrorLayer;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();

    let cfg = load_config()?;
    info!("compiler starting; queue={}", cfg.job_queue_name);

    init_redis_pool(&cfg.redis_url).await?;
    let s3 = create_s3_client_from_env().await;

    let consumer = unique_consumer();
    let queue = RedisStreamQueue::new(pool().clone(), &cfg.job_queue_name, "compilers", &consumer);
    queue.ensure_stream_group().await?;

    let permits = cfg.pipeline_parallelism.max(1);
    let semaphore = std::sync::Arc::new(Semaphore::new(permits));
    let mut join = JoinSet::new();

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                info!("ctrl-c: draining");
                break;
            }
            popped = queue.pop() => {
                match popped {
                    Ok(Some(msg)) => {
                        let sem = semaphore.clone();
                        let s3_cl = s3.clone();
                        let cfg_cl = cfg.clone();
                        let q_cl = queue.clone();

                        join.spawn(async move {
                            let _permit = sem.acquire_owned().await.expect("semaphore");
                            if let Err(e) = handle_one(msg.payload.to_vec(), &q_cl, &s3_cl, &cfg_cl).await {
                                error!(error=?e, "job failed");
                            } else if let Err(e) = q_cl.ack(&msg.id).await {
                                error!(error=?e, "ack failed");
                            }
                        });
                    }
                    Ok(None) => { /* timed out; loop again */ }
                    Err(e) => {
                        error!(error=?e, "pop failed");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
            Some(res) = join.join_next() => {
                if let Err(e) = res {
                    error!(error=?e, "task join error");
                }
            }
        }
    }

    while let Some(res) = join.join_next().await {
        if let Err(e) = res {
            error!(error=?e, "task join error during drain");
        }
    }

    info!("compiler stopped");
    Ok(())
}

async fn handle_one(
    raw: Vec<u8>,
    _queue: &RedisStreamQueue,
    s3: &aws_sdk_s3::Client,
    cfg: &playback_compiler::config::Config,
) -> Result<(), Box<dyn std::error::Error>> {
    // decode protobuf
    let raw_b = Bytes::from(raw);
    let job: Job = match decode_job(&raw_b) {
        Ok(j) => j,
        Err(e) => {
            let _ = publish_dlq(pool(), "dlq:compiler", "decode_error", None, &raw_b).await;
            return Err(e.into());
        }
    };

    // idempotency
    let idem_key = match idempotency_claim(pool(), &job.id, cfg.idempotency_ttl_secs).await? {
        Idem::Duplicate => return Ok(()),
        Idem::Fresh(k) => k,
    };

    // encode Arrow delta (expects &[Bytes])
    let id_bytes: Vec<Bytes> = vec![Bytes::from(job.id.clone())];
    let use_zstd = cfg.ipc_compression.to_lowercase() == "zstd";
    let arrow_bytes = encode_many_ids_arrow_bytes(&id_bytes, use_zstd)?;

    // S3 key: prefix/replay-deltas/<job.id>/<ts>.arrow
    let (secs, nanos) = now_unix();
    let key = format!(
        "{}/replay-deltas/{}/{}-{:09}.arrow",
        cfg.s3_prefix, job.id, secs, nanos
    );

    // Content headers
    let content_type = Some("application/vnd.apache.arrow.file");
    let content_encoding = if use_zstd { Some("zstd") } else { None };

    // Upload
    upload_bytes_to_s3(
        s3,
        &cfg.s3_bucket_name,
        &key,
        arrow_bytes,
        content_type,
        content_encoding,
    )
    .await?;

    // release idempotency
    idempotency_release(pool(), idem_key).await;
    Ok(())
}

fn init_logging() {
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env()) // e.g. RUST_LOG=info,playback_compiler=debug
        .with(fmt::layer().compact())
        .with(ErrorLayer::default())
        .init();
}

fn unique_consumer() -> String {
    format!("c-{}", std::process::id())
}

fn now_unix() -> (u64, u32) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0));
    (now.as_secs(), now.subsec_nanos())
}
