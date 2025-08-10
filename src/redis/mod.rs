use bytes::Bytes;
use deadpool_redis::{redis, Config, Pool, Runtime};
use once_cell::sync::OnceCell;
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};

use crate::errors::CompilerError;
use crate::ingest::{Queue, QueueMessage};
static REDIS_POOL: OnceCell<Pool> = OnceCell::new();

pub async fn init_redis_pool(redis_url: &str) -> Result<(), CompilerError> {
    let cfg = Config::from_url(redis_url);
    let pool = cfg
        .create_pool(Some(Runtime::Tokio1))
        .map_err(|e| CompilerError::RedisInit(e.to_string()))?;
    REDIS_POOL
        .set(pool)
        .map_err(|_| CompilerError::RedisPoolError("Pool already initialized".into()))?;
    Ok(())
}

pub fn pool() -> &'static Pool {
    REDIS_POOL.get().expect("Redis pool not initialized")
}

#[derive(Clone)]
pub struct RedisStreamQueue {
    stream: String,
    group: String,
    consumer: String,
    pool: Pool,
    block_ms: usize,
    count: usize,
}

impl RedisStreamQueue {
    pub fn new(
        pool: Pool,
        stream: impl Into<String>,
        group: impl Into<String>,
        consumer: impl Into<String>,
    ) -> Self {
        Self {
            pool,
            stream: stream.into(),
            group: group.into(),
            consumer: consumer.into(),
            block_ms: 5_000,
            count: 1,
        }
    }

    pub fn with_block(mut self, d: Duration) -> Self {
        self.block_ms = d.as_millis() as usize;
        self
    }

    pub fn with_count(mut self, n: usize) -> Self {
        self.count = n.max(1);
        self
    }

    #[instrument(skip(self))]
    pub async fn ensure_stream_group(&self) -> Result<(), CompilerError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CompilerError::RedisInit(e.to_string()))?;

        let res: redis::RedisResult<()> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&self.stream)
            .arg(&self.group)
            .arg("$")
            .arg("MKSTREAM")
            .query_async(&mut *conn)
            .await;

        match res {
            Ok(_) => {
                info!(stream=%self.stream, group=%self.group, "created consumer group");
                Ok(())
            }
            Err(err) => {
                let s = err.to_string();
                if s.contains("BUSYGROUP") {
                    debug!(stream=%self.stream, group=%self.group, "group exists");
                    Ok(())
                } else {
                    error!(error=%s, "XGROUP CREATE failed");
                    Err(CompilerError::RedisPoolError(s))
                }
            }
        }
    }

    #[instrument(skip(self), fields(stream=%self.stream, group=%self.group, consumer=%self.consumer))]
    async fn pop_once(&self) -> Result<Option<QueueMessage>, CompilerError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CompilerError::RedisInit(e.to_string()))?;

        let reply: Option<redis::Value> = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&self.group)
            .arg(&self.consumer)
            .arg("BLOCK")
            .arg(self.block_ms)
            .arg("COUNT")
            .arg(self.count)
            .arg("STREAMS")
            .arg(&self.stream)
            .arg(">")
            .query_async(&mut *conn)
            .await
            .map_err(|e| CompilerError::RedisPoolError(e.to_string()))?;

        let Some(val) = reply else { return Ok(None) };
        Ok(parse_xread_value(val))
    }

    pub async fn autoclaim_idle_over(
        &self,
        min_idle: Duration,
        count: usize,
    ) -> Result<Vec<QueueMessage>, CompilerError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CompilerError::RedisInit(e.to_string()))?;

        let res: redis::Value = redis::cmd("XAUTOCLAIM")
            .arg(&self.stream)
            .arg(&self.group)
            .arg(&self.consumer)
            .arg(min_idle.as_millis() as usize)
            .arg("0-0")
            .arg("COUNT")
            .arg(count)
            .query_async(&mut *conn)
            .await
            .map_err(|e| CompilerError::RedisPoolError(e.to_string()))?;

        Ok(parse_xautoclaim_value(res))
    }
}

pub fn parse_xread_value(val: deadpool_redis::redis::Value) -> Option<QueueMessage> {
    use deadpool_redis::redis::Value;
    let mut msg_id: Option<String> = None;
    let mut payload: Option<Vec<u8>> = None;

    if let Value::Bulk(streams) = val {
        if let Some(Value::Bulk(stream_entry)) = streams.get(0) {
            if let Some(Value::Bulk(entries)) = stream_entry.get(1) {
                if let Some(Value::Bulk(entry)) = entries.get(0) {
                    if let Some(Value::Data(idb)) = entry.get(0) {
                        msg_id = Some(String::from_utf8_lossy(idb).to_string());
                    }
                    if let Some(Value::Bulk(kvs)) = entry.get(1) {
                        let mut i = 0;
                        while i + 1 < kvs.len() {
                            let k = &kvs[i];
                            let v = &kvs[i + 1];
                            let k_str = match k {
                                Value::Data(b) => String::from_utf8_lossy(b),
                                _ => "".into(),
                            };
                            if k_str == "payload" {
                                if let Value::Data(b) = v {
                                    payload = Some(b.clone());
                                }
                            }
                            i += 2;
                        }
                    }
                }
            }
        }
    }

    let id = msg_id?;
    let bytes = payload?;
    Some(QueueMessage {
        id,
        payload: Bytes::from(bytes),
    })
}

/// Parse XAUTOCLAIM reply → many QueueMessage.
fn parse_xautoclaim_value(val: redis::Value) -> Vec<QueueMessage> {
    use redis::Value;
    let mut out = Vec::new();

    if let Value::Bulk(outer) = val {
        // Shape: [ <next-id>, [ [id, [kvs...]], ... ] , ... ]
        if outer.len() >= 2 {
            if let Value::Bulk(entries) = &outer[1] {
                for e in entries {
                    if let Value::Bulk(parts) = e {
                        let id = match &parts[0] {
                            Value::Data(b) => String::from_utf8_lossy(b).to_string(),
                            _ => continue,
                        };
                        let mut payload: Option<Vec<u8>> = None;
                        if let Value::Bulk(kvs) = &parts[1] {
                            let mut i = 0;
                            while i + 1 < kvs.len() {
                                let k = &kvs[i];
                                let v = &kvs[i + 1];
                                let k_str = match k {
                                    Value::Data(b) => String::from_utf8_lossy(b),
                                    _ => "".into(),
                                };
                                if k_str == "payload" {
                                    if let Value::Data(b) = v {
                                        payload = Some(b.clone());
                                    }
                                }
                                i += 2;
                            }
                        }
                        if let Some(p) = payload {
                            out.push(QueueMessage {
                                id,
                                payload: Bytes::from(p),
                            });
                        }
                    }
                }
            }
        }
    }

    out
}

#[async_trait::async_trait]
impl Queue for RedisStreamQueue {
    type Error = CompilerError;

    async fn pop(&self) -> Result<Option<QueueMessage>, Self::Error> {
        // Try to ensure group; if it races with another worker, we’ll recover below.
        if let Err(e) = self.ensure_stream_group().await {
            warn!(err=%e, "ensure_stream_group failed; will retry");
        }

        // Simple exponential backoff on transport errors.
        let mut backoff = Duration::from_millis(200);
        loop {
            match self.pop_once().await {
                Ok(msg) => return Ok(msg),
                Err(e) => {
                    let s = e.to_string();
                    if s.contains("NOGROUP") {
                        warn!("consumer group missing; recreating");
                        self.ensure_stream_group().await?;
                        continue;
                    }
                    error!(err=%s, "redis pop failed; backing off");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
            }
        }
    }

    async fn ack(&self, id: &str) -> Result<(), Self::Error> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CompilerError::RedisInit(e.to_string()))?;
        redis::cmd("XACK")
            .arg(&self.stream)
            .arg(&self.group)
            .arg(id)
            .query_async::<_, i64>(&mut *conn)
            .await
            .map_err(|e| CompilerError::RedisPoolError(e.to_string()))?;
        Ok(())
    }
}
