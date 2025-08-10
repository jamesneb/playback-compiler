use crate::errors::CompilerError;
use bytes::Bytes;
use deadpool_redis::{
    redis::{cmd, AsyncCommands},
    Pool,
};
use tracing::info;

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
        info!(stream=%stream, reason=%reason, "published to dlq");
        Ok(())
    })
    .await
}

pub enum Idem {
    Fresh(String),
    Duplicate,
}

pub async fn idempotency_claim(
    pool: &Pool,
    job_id: &str,
    ttl_secs: usize,
) -> Result<Idem, CompilerError> {
    let key = format!("processed:{job_id}");
    with_conn(pool, |mut c| async move {
        let first: bool = c
            .set_nx(&key, 1)
            .await
            .map_err(|e| CompilerError::RedisPoolError(e.to_string()))?;
        if !first {
            return Ok(Idem::Duplicate);
        }
        let _: () = c
            .expire(&key, i64::try_from(ttl_secs).unwrap_or(i64::MAX))
            .await
            .map_err(|e| CompilerError::RedisPoolError(e.to_string()))?;
        Ok(Idem::Fresh(key))
    })
    .await
}

pub async fn idempotency_release(pool: &Pool, key: String) {
    let _ = with_conn(pool, |mut c| async move {
        let _: () = c.del(&key).await.unwrap_or(());
        Ok(())
    })
    .await;
}
