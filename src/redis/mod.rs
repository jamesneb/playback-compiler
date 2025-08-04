use deadpool_redis::{Config, Pool, Runtime};
use once_cell::sync::OnceCell;
use std::sync::Arc;

use crate::errors::CompilerError;

static REDIS_CONNECTION_POOL: OnceCell<Arc<Pool>> = OnceCell::new();

pub async fn init_redis_pool(redis_url: &str) -> Result<(), CompilerError> {
    let cfg = Config::from_url(redis_url);
    let pool = cfg
        .create_pool(Some(Runtime::Tokio1))
        .map_err(|e| CompilerError::RedisInit(e.to_string()))?;

    REDIS_CONNECTION_POOL
        .set(Arc::new(pool))
        .map_err(|_| CompilerError::RedisPoolError("Pool already initialized".to_string()))?;

    Ok(())
}

pub fn get_pool() -> &'static Pool {
    REDIS_CONNECTION_POOL
        .get()
        .expect("Redis pool not initialized")
}
