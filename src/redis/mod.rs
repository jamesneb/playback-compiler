//! Redis Streams integration that leverages `deadpool-redis` for a single `redis` dependency.

use crate::errors::CompilerError;
use crate::ingest::Queue;
pub use crate::ingest::QueueMessage;
use crate::types::fp::{idempotency_claim, idempotency_release, publish_dlq, Idem};
use bytes::Bytes;
use deadpool_redis::redis::{self};
use deadpool_redis::{Config, Pool, Runtime};
use once_cell::sync::OnceCell;
use std::time::Duration;

static REDIS_POOL: OnceCell<Pool> = OnceCell::new();

pub fn init_redis_pool(redis_url: &str) -> Result<(), CompilerError> {
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

#[derive(Clone)]
pub struct RedisStreamQueue {
    pool: Pool,
    stream: String,
    group: String,
    consumer: String,
    read_timeout_ms: usize,
}

impl RedisStreamQueue {
    pub fn pool_clone(&self) -> Pool {
        self.pool.clone()
    }
    pub fn from_url(
        stream: &str,
        group: &str,
        consumer: &str,
        url: &str,
    ) -> Result<Self, CompilerError> {
        init_redis_pool(url)?;

        let p = pool().clone();
        Ok(Self {
            pool: p,
            stream: stream.to_string(),
            group: group.to_string(),
            consumer: consumer.to_string(),
            read_timeout_ms: 5_000,
        })
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

    pub async fn pop_batch(&self, count: usize) -> Result<Vec<QueueMessage>, CompilerError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CompilerError::RedisInit(e.to_string()))?;

        #[cfg(debug_assertions)]
        {
            use tracing::debug;
            debug!(stream = %self.stream, group = %self.group, consumer = %self.consumer, count = count, "executing XREADGROUP");
        }
        
        // Request a concrete redis::Value to keep version differences isolated.
        let val: redis::Value = redis::cmd("XREADGROUP")
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

        let messages = parse_xread_value(val);
        
        #[cfg(debug_assertions)]
        {
            use tracing::info;
            info!(message_count = messages.len(), "parsed messages from Redis stream");
        }
        
        Ok(messages)
    }

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

/// Convert the `XREADGROUP` response into a list of `QueueMessage` instances.
/// Uses the `redis` crate types exclusively to sidestep version mismatches.
pub fn parse_xread_value(val: redis::Value) -> Vec<QueueMessage> {
    use deadpool_redis::redis::Value;
    let mut out = Vec::new();

    // Response layout:
    // Array[
    //   Array[ stream_name, Array[ Array[ id, Array[ k1, v1, ... ] ], ... ] ],
    //   ...
    // ]
    let Value::Bulk(streams) = val else {
        return out;
    };

    for s in streams {
        let Value::Bulk(stream_pair) = s else {
            continue;
        };
        if stream_pair.len() != 2 {
            continue;
        }
        let Value::Bulk(msgs) = &stream_pair[1] else {
            continue;
        };

        for m in msgs {
            let Value::Bulk(pair) = m else { continue };
            if pair.len() != 2 {
                continue;
            }
            let id = match &pair[0] {
                Value::Data(b) => String::from_utf8_lossy(b).to_string(),
                _ => continue,
            };
            let Value::Bulk(kv) = &pair[1] else { continue };

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

#[derive(Clone)]
pub struct RedisControlPlane {
    pool: Pool,
    dlq_stream: String,
}

impl RedisControlPlane {
    pub fn new(pool: Pool, dlq_stream: impl Into<String>) -> Self {
        Self {
            pool,
            dlq_stream: dlq_stream.into(),
        }
    }

    pub fn from_url(url: &str, dlq_stream: impl Into<String>) -> Result<Self, CompilerError> {
        init_redis_pool(url)?;
        Ok(Self::new(pool().clone(), dlq_stream))
    }

    pub async fn claim(&self, job_id: &str, ttl_secs: u64) -> Result<Idem, CompilerError> {
        idempotency_claim(&self.pool, job_id, ttl_secs).await
    }

    pub async fn release(&self, key: String) {
        idempotency_release(&self.pool, key).await;
    }

    pub async fn dlq(&self, reason: &str, payload: &bytes::Bytes) -> Result<(), CompilerError> {
        publish_dlq(&self.pool, &self.dlq_stream, reason, None, payload).await
    }

    pub fn pool_clone(&self) -> Pool {
        self.pool.clone()
    }
}
