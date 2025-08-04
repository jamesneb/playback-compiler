use deadpool_redis::{Config, Pool, Runtime};
use once_cell::sync::OnceCell;
use std::sync::Arc;

static REDIS_CONNECTION_POOL: OnceCell<Arc<Pool>> = OnceCell::new();

pub async fn init_redis_pool(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = Config::from_url(url);
    let pool = cfg.create_pool(Some(Runtime::Tokio1))?;
    REDIS_CONNECTION_POOL
        .set(Arc::new(pool))
        .map_err(|_| "Pool already set")?;
    Ok(())
}

pub fn get_pool() -> &'static Pool {
    REDIS_CONNECTION_POOL
        .get()
        .expect("Redis pool not initialized")
}
