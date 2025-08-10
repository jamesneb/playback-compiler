//! Redis Streams integration
//!
//! Overview
//! --------
//! Provides a queue backed by Redis Streams (XREADGROUP / XACK) with consumer
//! groups and idle-claim support for failover.
//!
//! Responsibilities
//! ----------------
//! - Pool initialization and accessors.
//! - Stream group bootstrap.
//! - Pop (bounded block), ack, and idle autoclaim.
//!
//! Error Model
//! -----------
//! - Pool/command errors map to `CompilerError`.
//! - Unexpected reply shapes are treated as empty results.
//!
//! Concurrency / Performance
//! -------------------------
//! - Uses `deadpool-redis` for connection pooling.
//! - Bounded block reads (configurable) to avoid indefinite hangs.

//! Redis Streams integration
//!
//! Provides a queue backed by Redis Streams (XREADGROUP / XACK) with consumer
//! groups and idle-claim support for failover. Uses `deadpool-redis` for
//! pooling and parses replies from raw `redis::Value` (avoids gated APIs).

use crate::errors::CompilerError;
use crate::ingest::{Queue, QueueMessage};
use bytes::Bytes;
use deadpool_redis::redis::{self, AsyncCommands};
use deadpool_redis::{Config, Pool, Runtime};
use once_cell::sync::OnceCell;
use std::time::Duration;

// ====== POOL ========
static REDIS_POOL: OnceCell<Pool> = OnceCell::new();

pub async fn init_redis_pool(redis_url: &str) -> Result<(), CompilerError> {
    let cfg = Config::from_url(redis_url);
    let pool = cfg
        .create_pool(Some(Runtime::Tokio1))
        .map_err(|e| CompilerError::RedisInit(e.to_string()))?;
    REDIS_POOL
        .set(pool)
        .map_err(|_| CompilerError::RedisPoolError("pool already initialized".into()))?;
    Ok(())
}

pub fn pool() -> &'static Pool {
    REDIS_POOL.get().expect("pool not initialized")
}

// ========= Types =========

#[derive(Clone)]
pub struct RedisStreamQueue {
    pool: Pool,
    stream: String,
    group: String,
    consumer: String,
    read_timeout_ms: usize,
}

// ========= Public API =========

impl RedisStreamQueue {
    pub fn new(pool: Pool, stream: &str, group: &str, consumer: &str) -> Self {
        Self {
            pool,
            stream: stream.to_string(),
            group: group.to_string(),
            consumer: consumer.to_string(),
            read_timeout_ms: 5_000,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout_ms = timeout.as_millis() as usize;
        self
    }

    pub async fn ensure_stream_group(&self) -> Result<(), CompilerError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CompilerError::RedisInit(e.to_string()))?;
        let r: Result<String, _> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&self.stream)
            .arg(&self.group)
            .arg("$")
            .arg("MKSTREAM")
            .query_async(&mut *conn)
            .await;

        match r {
            Ok(_) => Ok(()),
            Err(e) if e.to_string().contains("BUSYGROUP") => Ok(()),
            Err(e) => Err(CompilerError::RedisPoolError(e.to_string())),
        }
    }

    /// Batched pop using XREADGROUP with COUNT; parsed from raw `redis::Value`.
    pub async fn pop_batch(&self, count: usize) -> Result<Vec<QueueMessage>, CompilerError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CompilerError::RedisInit(e.to_string()))?;

        let reply: redis::Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&self.group)
            .arg(&self.consumer)
            .arg("COUNT")
            .arg(count)
            .arg("BLOCK")
            .arg(self.read_timeout_ms)
            .arg("STREAMS")
            .arg(&self.stream)
            .arg(">")
            .query_async(&mut *conn)
            .await
            .map_err(|e| CompilerError::RedisPoolError(e.to_string()))?;

        Ok(parse_xread_value(reply))
    }

    /// Pipeline XACK for many message IDs.
    pub async fn ack_many(&self, ids: &[String]) -> Result<(), CompilerError> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CompilerError::RedisInit(e.to_string()))?;
        let mut cmd = redis::cmd("XACK");
        cmd.arg(&self.stream).arg(&self.group);
        for id in ids {
            cmd.arg(id);
        }
        let _: i64 = cmd
            .query_async(&mut *conn)
            .await
            .map_err(|e| CompilerError::RedisPoolError(e.to_string()))?;
        Ok(())
    }

    /// Single ack for compatibility.
    pub async fn ack(&self, id: &str) -> Result<(), CompilerError> {
        self.ack_many(std::slice::from_ref(&id.to_string())).await
    }
}

#[async_trait::async_trait]
impl Queue for RedisStreamQueue {
    type Error = CompilerError;

    async fn pop(&self) -> Result<Option<QueueMessage>, Self::Error> {
        let mut batch = self.pop_batch(1).await?;
        Ok(batch.pop())
    }

    async fn ack(&self, id: &str) -> Result<(), Self::Error> {
        self.ack(id).await
    }
}

// ========= Parsers =========

/// Parse XREAD* reply (as `redis::Value`) into messages.
///
/// Shape:
/// Bulk[
///   Bulk[ stream_name(Data), Bulk[
///     Bulk[ id(Data), Bulk[ k(Data), v(Data), ... ] ],
///     ...
///   ]],
///   ...
/// ]
pub(crate) fn parse_xread_value(val: redis::Value) -> Vec<QueueMessage> {
    use deadpool_redis::redis::Value;
    let mut out = Vec::new();

    let Value::Bulk(streams) = val else { return out; };

    for s in streams {
        let Value::Bulk(stream_pair) = s else { continue; };
        if stream_pair.len() != 2 {
            continue;
        }

        let Value::Bulk(msgs) = &stream_pair[1] else { continue; };

        for m in msgs {
            let Value::Bulk(pair) = m else { continue; };
            if pair.len() != 2 {
                continue;
            }

            let id = match &pair[0] {
                Value::Data(b) => String::from_utf8_lossy(b).to_string(),
                _ => continue,
            };

            let Value::Bulk(kv) = &pair[1] else { continue; };

            let mut payload: Option<Bytes> = None;
            let mut i = 0;
            while i + 1 < kv.len() {
                match (&kv[i], &kv[i + 1]) {
                    (Value::Data(k), Value::Data(v)) if k == b"payload" => {
                        payload = Some(Bytes::from(v.clone()));
                        break;
                    }
                    _ => {}
                }
                i += 2;
            }

            if let Some(p) = payload {
                out.push(QueueMessage { id, payload: p });
            }
        }
    }

    out
}

#[allow(dead_code)]
pub(crate) fn parse_xautoclaim_value(val: redis::Value) -> Vec<QueueMessage> {
    use deadpool_redis::redis::Value;
    let mut out = Vec::new();

    let Value::Bulk(root) = val else { return out };
    if root.len() != 2 {
        return out;
    }

    let Value::Bulk(items) = &root[1] else { return out };

    for entry in items {
        let Value::Bulk(x) = entry else { continue };
        if x.len() != 2 {
            continue;
        }

        let id = match &x[0] {
            Value::Data(b) => String::from_utf8_lossy(b).to_string(),
            _ => continue,
        };
        let Value::Bulk(kv) = &x[1] else { continue };

        let mut payload: Option<Bytes> = None;
        let mut i = 0;
        while i + 1 < kv.len() {
            if let (Value::Data(k), Value::Data(v)) = (&kv[i], &kv[i + 1]) {
                if k == b"payload" {
                    payload = Some(Bytes::from(v.clone()));
                    break;
                }
            }
            i += 2;
        }
        if let Some(p) = payload {
            out.push(QueueMessage { id, payload: p });
        }
    }
    out
}
