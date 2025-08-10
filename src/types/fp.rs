//! Small functional utilities for ergonomic error/result handling
//!
//! Overview
//! --------
//! Helpers to reduce boilerplate around pooled-connection usage, DLQ publishing,
//! and simple idempotency guards. These are intentionally minimal and crate-
//! local building blocks used by higher-level modules.
//!
//! Notes
//! -----
//! - Keep this module dependency-light.
//! - Use at module edges to keep orchestration code concise.

use crate::errors::CompilerError;
use bytes::Bytes;
use deadpool_redis::{
    redis::{cmd, AsyncCommands},
    Pool,
};
use tracing::info;

// ========= Connection helper =========

/// Borrow a pooled Redis connection and run the provided async action.
/// Maps pool errors into `CompilerError` at the edge.
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

// ========= DLQ publishing =========

/// Publish a failure event to a Redis Stream (DLQ).
/// Fields: `reason`, binary `payload`, optional `job_id`.
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

// ========= Idempotency guard =========

/// Result of an idempotency claim attempt.
pub enum Idem {
    /// First claimant; returns the stored key so the caller can release if needed.
    Fresh(String),
    /// Claim already exists (duplicate work should be skipped).
    Duplicate,
}

/// Create a time-bounded idempotency marker for `job_id`.
/// Returns `Fresh(key)` if this caller owns the claim, otherwise `Duplicate`.
#[tracing::instrument(skip(pool, job_id, ttl_secs))]

pub async fn idempotency_claim(
    pool: &Pool,
    job_id: &str,
    ttl_secs: usize,
) -> Result<Idem, CompilerError> {
    let key = format!("processed:{job_id}");
    with_conn(pool, |mut c| async move {
        // SETNX: set only if not exists
        let first: bool = c
            .set_nx(&key, 1)
            .await
            .map_err(|e| CompilerError::RedisPoolError(e.to_string()))?;

        if !first {
            return Ok(Idem::Duplicate);
        }

        // EXPIRE: clamp TTL to i64::MAX to satisfy redis-rs signature
        let ttl = i64::try_from(ttl_secs).unwrap_or(i64::MAX);
        let _: () = c
            .expire(&key, ttl)
            .await
            .map_err(|e| CompilerError::RedisPoolError(e.to_string()))?;

        Ok(Idem::Fresh(key))
    })
    .await
}

/// Best-effort release of an idempotency marker.
/// Fire-and-forget semantics: errors are swallowed by design.
#[tracing::instrument(skip(pool, key))]
pub async fn idempotency_release(pool: &Pool, key: String) {
    let _ = with_conn(pool, |mut c| async move {
        let _: () = c.del(&key).await.unwrap_or(());
        Ok(())
    })
    .await;
}
