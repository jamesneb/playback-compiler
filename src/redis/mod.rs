//! Redis Streams integration (single version of `redis` via deadpool-redis)

use crate::errors::CompilerError;
use crate::ingest::Queue;
pub use crate::ingest::QueueMessage;
use bytes::Bytes;
use deadpool_redis::redis::{self};
use deadpool_redis::{Config, Pool, Runtime};
use once_cell::sync::OnceCell;
use std::time::Duration;

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

#[derive(Clone)]
pub struct RedisStreamQueue {
    pool: Pool,
    stream: String,
    group: String,
    consumer: String,
    read_timeout_ms: usize,
}

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

    pub async fn pop_batch(&self, count: usize) -> Result<Vec<QueueMessage>, CompilerError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CompilerError::RedisInit(e.to_string()))?;

        // Ask for a typed Value to avoid cross-crate type mismatch
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

        Ok(parse_xread_value(val))
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

/// Parse the `XREADGROUP` redis::Value reply into QueueMessage list.
/// We stay purely on the deadpool-redis `redis` crate to avoid type/version conflicts.
pub fn parse_xread_value(val: redis::Value) -> Vec<QueueMessage> {
    use deadpool_redis::redis::Value;
    let mut out = Vec::new();

    // Expected shape:
    // Array[
    //   Array[ stream_name, Array[ Array[ id, Array[ k1, v1, ... ] ], ... ] ],
    //   ...
    // ]
    let Value::Bulk(streams) = val else { return out };

    for s in streams {
        let Value::Bulk(stream_pair) = s else { continue };
        if stream_pair.len() != 2 {
            continue;
        }
        let Value::Bulk(msgs) = &stream_pair[1] else { continue };

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
