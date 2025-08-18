//! App runtime: queue loop + request handler (hot path).

use bytes::Bytes;
use std::{error::Error, sync::Arc};

use crate::config::Config;
use crate::emit::BlobStore;
use crate::errors::CompilerError;
use crate::ingest::Queue;
use crate::proto::Job;
use crate::redis::RedisControlPlane;
use crate::transform::{decode::decode_job, encode::encode_many_ids_arrow_bytes};
use crate::util::time::now_unix;

pub async fn run<Q, B>(
    queue: Q,
    store: B,
    cp: Arc<RedisControlPlane>, // ← DI Redis control-plane
    cfg: Arc<Config>,
    permits: usize,
) -> Result<(), Box<dyn Error>>
where
    Q: Queue<Error = CompilerError> + Clone + Send + Sync + 'static,
    B: BlobStore, // Clone + Send + Sync + 'static via trait bounds
{
    use tokio::sync::Semaphore;
    use tokio::{signal, task::JoinSet};
    use tracing::{error, info};

    let semaphore = Arc::new(Semaphore::new(permits));
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
                        let sem   = semaphore.clone();
                        let store = store.clone();   // cheap handle
                        let cp    = cp.clone();
                        let cfg   = cfg.clone();
                        let q_cl  = queue.clone();

                        join.spawn(async move {
                            let _permit = sem.acquire_owned().await.expect("semaphore");
                            if let Err(e) = handle_one(msg.payload.to_vec(), &store, &cp, &cfg).await {
                                error!(error=?e, "job failed");
                            } else if let Err(e) = q_cl.ack(&msg.id).await {
                                error!(error=?e, "ack failed");
                            }
                        });
                    }
                    Ok(None) => { /* idle poll */ }
                    Err(e) => {
                        error!(error=?e, "pop failed");
                        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
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
            tracing::error!(error=?e, "task join error during drain");
        }
    }

    Ok(())
}

async fn handle_one<B: BlobStore>(
    raw: Vec<u8>,
    store: &B,
    cp: &RedisControlPlane,
    cfg: &Config,
) -> Result<(), Box<dyn Error>> {
    // Decode
    let raw_b = Bytes::from(raw);
    let job: Job = match decode_job(&raw_b) {
        Ok(j) => j,
        Err(e) => {
            let _ = cp.dlq("decode_error", &raw_b).await;
            return Err(e.into());
        }
    };

    // Idempotency
    let idem_key = match cp.claim(&job.id, cfg.idempotency_ttl_secs).await? {
        crate::types::fp::Idem::Duplicate => return Ok(()),
        crate::types::fp::Idem::Fresh(k) => k,
    };

    // Encode Arrow
    let use_zstd = cfg.ipc_compression.eq_ignore_ascii_case("zstd");
    let id_bytes = [Bytes::from(job.id.clone())];
    let arrow_bytes = encode_many_ids_arrow_bytes(&id_bytes, use_zstd)?;

    // Key + metadata
    let (secs, nanos) = now_unix();
    let key = format!(
        "{}/replay-deltas/{}/{}-{:09}.arrow",
        cfg.s3_prefix, job.id, secs, nanos
    );
    let content_type = Some("application/vnd.apache.arrow.file");
    let content_encoding = if use_zstd { Some("zstd") } else { None };

    // Upload via injected store (static dispatch)
    store
        .put_bytes(
            &cfg.s3_bucket_name,
            &key,
            arrow_bytes,
            content_type,
            content_encoding,
        )
        .await?;

    // Release lock
    cp.release(idem_key).await;
    Ok(())
}
