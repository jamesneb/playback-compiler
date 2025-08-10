use crate::errors::CompilerError;
use bytes::Bytes;
use deadpool_redis::{redis::cmd, Pool};
use tracing::info;

pub async fn publish(
    pool: &Pool,
    stream: &str,
    reason: &str,
    job_id: Option<&str>,
    payload: &Bytes,
) -> Result<(), CompilerError> {
    let mut conn = pool
        .get()
        .await
        .map_err(|e| CompilerError::RedisInit(e.to_string()))?;
    let mut c = cmd("XADD");
    c.arg(stream)
        .arg("*")
        .arg("reason")
        .arg(reason)
        .arg("payload")
        .arg(&**payload);
    if let Some(jid) = job_id {
        c.arg("job_id").arg(jid);
    }
    c.query_async::<_, String>(&mut *conn)
        .await
        .map_err(|e| CompilerError::RedisPoolError(e.to_string()))?;
    info!(stream=%stream, reason=%reason, "published to dlq");
    Ok(())
}
