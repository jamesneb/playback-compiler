//! Utility helpers for lightweight error handling and Redis interactions.
//!
//! Provides small building blocks such as connection helpers, dead-letter
//! publishing, and simple idempotency guards.

use crate::errors::CompilerError;
use bytes::Bytes;
use deadpool_redis::{
    Pool,
    redis::{AsyncCommands, cmd},
};
use tracing::info;

// --- Connection helper ---

/// Acquire a Redis connection from the pool and execute the given async action.
/// Pool errors are converted to `CompilerError` values.
pub async fn with_conn<T, F, Fut>(pool: &Pool, f: F) -> Result<T, CompilerError>
where
    F: FnOnce(deadpool_redis::Connection) -> Fut,
    Fut: std::future::Future<Output = Result<T, CompilerError>>,
{
    let conn = pool
        .get()
        .await
        .map_err(|e| CompilerError::RedisInit(e.to_string()))?;
    f(conn).await
}

// --- DLQ publishing ---

/// Publish a failure event to a Redis stream acting as a DLQ.
/// Includes the reason, payload bytes, and optional job identifier.
#[tracing::instrument(skip(pool, payload))]
pub async fn publish_dlq(
    pool: &Pool,
    stream: &str,
    reason: &str,
    job_id: Option<&str>,
    payload: &Bytes,
) -> Result<(), CompilerError> {
    with_conn(pool, |mut c| async move {
        let mut x = cmd("XADD");
        x.arg(stream)
            .arg("*")
            .arg("reason")
            .arg(reason)
            .arg("payload")
            .arg(&**payload);
        if let Some(jid) = job_id {
            x.arg("job_id").arg(jid);
        }
        x.query_async::<_, String>(&mut c)
            .await
            .map_err(|e| CompilerError::RedisPoolError(e.to_string()))?;
        info!(stream = %stream, reason = %reason, "published to dlq");
        Ok(())
    })
    .await
}

// --- Idempotency guard ---

/// Outcome of an idempotency claim attempt.
pub enum Idem {
    /// Claim succeeded; caller receives the redis key for later release.
    Fresh(String),
    /// Marker already exists, indicating duplicate work.
    Duplicate,
}

/// Attempt to set a TTL-bound idempotency marker for `job_id`.
/// Returns `Fresh(key)` on success or `Duplicate` if already claimed.
#[tracing::instrument(skip(pool, job_id, ttl_secs))]

pub async fn idempotency_claim(
    pool: &Pool,
    job_id: &str,
    ttl_secs: usize,
) -> Result<Idem, CompilerError> {
    let key = format!("processed:{job_id}");
    with_conn(pool, |mut c| async move {
        // SETNX: set only if the key does not already exist.
        let first: bool = c
            .set_nx(&key, 1)
            .await
            .map_err(|e| CompilerError::RedisPoolError(e.to_string()))?;

        if !first {
            return Ok(Idem::Duplicate);
        }

        // EXPIRE: clamp TTL to `i64::MAX` to satisfy the redis-rs signature.
        let ttl = i64::try_from(ttl_secs).unwrap_or(i64::MAX);
        let _: () = c
            .expire(&key, ttl)
            .await
            .map_err(|e| CompilerError::RedisPoolError(e.to_string()))?;

        Ok(Idem::Fresh(key))
    })
    .await
}

/// Remove the idempotency marker without propagating errors.
/// The operation is fire-and-forget.
#[tracing::instrument(skip(pool, key))]
pub async fn idempotency_release(pool: &Pool, key: String) {
    let _ = with_conn(pool, |mut c| async move {
        let _: () = c.del(&key).await.unwrap_or(());
        Ok(())
    })
    .await;
}
