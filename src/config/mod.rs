use dotenvy::dotenv;
use serde::Deserialize;
use std::env;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    /// Connection string for Redis.
    pub redis_url: String,
    /// Name of the job queue.
    pub job_queue_name: String,
    /// Destination bucket for emitted objects.
    pub s3_bucket_name: String,
    /// Prefix within the bucket.
    pub s3_prefix: String,

    /// TTL in seconds for idempotency claims.
    pub idempotency_ttl_secs: u64,

    /// Maximum bytes per coalescing window.
    pub window_max_bytes: usize,
    /// Maximum message count per window.
    pub window_max_count: usize,
    /// Maximum age in milliseconds before a window is flushed.
    pub window_max_age_ms: u64,
    /// Interval in milliseconds to check for flush conditions.
    pub flush_tick_ms: u64,
    /// Capacity of the channel feeding the sink.
    pub sink_channel_capacity: usize,

    /// Threshold in bytes to switch to multipart uploads.
    pub multipart_threshold_bytes: usize,
    /// Size in bytes for each multipart chunk.
    pub multipart_part_size_bytes: usize,
    /// Number of multipart sections uploaded concurrently.
    pub multipart_parallel_parts: usize,

    /// Arrow IPC compression method (`none` or `zstd`).
    pub ipc_compression: String,
    /// Enable object-level Zstd compression.
    pub zstd_object: bool,
    /// Zstd compression level.
    pub zstd_level: i32,
    /// MIME type set on uploads.
    pub content_type: String,
    /// Whether to advertise Content-Encoding: zstd.
    pub content_encoding_zstd: bool,

    /// Maximum retry attempts for S3 operations.
    pub retry_max_attempts: usize,
    /// Base delay in milliseconds for retries.
    pub retry_base_delay_ms: u64,
    /// Degree of parallelism in the processing pipeline.
    pub pipeline_parallelism: usize,
}

fn parse_bool(name: &str, default: bool) -> bool {
    match env::var(name) {
        Ok(v) => matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"),
        Err(_) => default,
    }
}

fn parse_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
pub fn load_config() -> Result<Config, Box<dyn std::error::Error>> {
    dotenv().ok();

    // Mandatory settings.
    let redis_url = env::var("REDIS_URL")?;
    let job_queue_name = env::var("JOB_QUEUE_NAME")?;
    let s3_bucket_name = env::var("S3_BUCKET_NAME")?;
    let s3_prefix = env::var("S3_PREFIX")?;
    let idempotency_ttl_secs = env::var("IDEMPOTENCY_TTL_SECS")?.parse()?;

    // Windowing parameters tuned for low latency.
    let window_max_bytes = parse_or("WINDOW_MAX_BYTES", 1_048_576usize); // default 1 MiB
    let window_max_count = parse_or("WINDOW_MAX_COUNT", 2048usize);
    let window_max_age_ms = parse_or("WINDOW_MAX_AGE_MS", 200u64);
    let flush_tick_ms = parse_or("FLUSH_TICK_MS", 50u64);
    let sink_channel_capacity = parse_or("SINK_CHANNEL_CAPACITY", 8192usize);

    // Multipart upload thresholds.
    let multipart_threshold_bytes = parse_or("MULTIPART_THRESHOLD_BYTES", 5 * 1024 * 1024usize); // minimum 5 MiB
    let multipart_part_size_bytes = parse_or("MULTIPART_PART_SIZE_BYTES", 8 * 1024 * 1024usize); // part size 8 MiB
    let multipart_parallel_parts = parse_or("MULTIPART_PARALLEL_PARTS", 4usize);

    // Compression options.
    let ipc_compression = env::var("IPC_COMPRESSION").unwrap_or_else(|_| "none".into()); // accepts "none" or "zstd"
    let zstd_object = parse_bool("ZSTD_OBJECT", true);
    let zstd_level = parse_or("ZSTD_LEVEL", 6i32);
    let content_type =
        env::var("CONTENT_TYPE").unwrap_or_else(|_| "application/vnd.apache.arrow.file".into());
    let content_encoding_zstd = parse_bool("CONTENT_ENCODING_ZSTD", true);

    // Retry and pipeline behaviour.
    let retry_max_attempts = parse_or("RETRY_MAX_ATTEMPTS", 5usize);
    let retry_base_delay_ms = parse_or("RETRY_BASE_DELAY_MS", 50u64);
    let pipeline_parallelism = parse_or("PIPELINE_PARALLELISM", 4usize);

    Ok(Config {
        redis_url,
        job_queue_name,
        s3_bucket_name,
        s3_prefix,
        idempotency_ttl_secs,
        window_max_bytes,
        window_max_count,
        window_max_age_ms,
        flush_tick_ms,
        sink_channel_capacity,
        multipart_threshold_bytes,
        multipart_part_size_bytes,
        multipart_parallel_parts,
        ipc_compression,
        zstd_object,
        zstd_level,
        content_type,
        content_encoding_zstd,
        retry_max_attempts,
        retry_base_delay_ms,
        pipeline_parallelism,
    })
}
