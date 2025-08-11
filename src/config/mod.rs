use dotenvy::dotenv;
use serde::Deserialize;
use std::env;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    // --- Required ---
    pub redis_url: String,
    pub job_queue_name: String,
    pub s3_bucket_name: String,
    pub s3_prefix: String,

    pub idempotency_ttl_secs: usize,

    // --- Windowing / coalescing ---
    pub window_max_bytes: usize,
    pub window_max_count: usize,
    pub window_max_age_ms: u64,
    pub flush_tick_ms: u64,
    pub sink_channel_capacity: usize,

    // --- Multipart S3 ---
    // Use *_BYTES to be explicit about units
    pub multipart_threshold_bytes: usize,
    pub multipart_part_size_bytes: usize,
    pub multipart_parallel_parts: usize,

    // --- Compression knobs ---
    // ipc_compression is kept for forward-compat; currently "none" or "zstd"
    pub ipc_compression: String, // "none" | "zstd" (Arrow IPC-level; not used on Arrow 56)
    pub zstd_object: bool,       // object-level zstd (we use this now)
    pub zstd_level: i32,         // 1..=21 typical; 3-6 good tradeoff
    pub content_type: String,    // e.g. "application/vnd.apache.arrow.file"
    pub content_encoding_zstd: bool, // set Content-Encoding: zstd on uploads

    // --- Retry / pipeline ---
    pub retry_max_attempts: usize,
    pub retry_base_delay_ms: u64,
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

    // Required (fail fast if missing)
    let redis_url = env::var("REDIS_URL")?;
    let job_queue_name = env::var("JOB_QUEUE_NAME")?;
    let s3_bucket_name = env::var("S3_BUCKET_NAME")?;
    let s3_prefix = env::var("S3_PREFIX")?;
    let idempotency_ttl_secs = env::var("IDEMPOTENCY_TTL_SECS")?.parse()?;

    // Windowing / coalescing (defaults chosen for low-latency)
    let window_max_bytes = parse_or("WINDOW_MAX_BYTES", 1_048_576usize); // 1 MiB
    let window_max_count = parse_or("WINDOW_MAX_COUNT", 2048usize);
    let window_max_age_ms = parse_or("WINDOW_MAX_AGE_MS", 200u64);
    let flush_tick_ms = parse_or("FLUSH_TICK_MS", 50u64);
    let sink_channel_capacity = parse_or("SINK_CHANNEL_CAPACITY", 8192usize);

    // Multipart S3
    let multipart_threshold_bytes = parse_or("MULTIPART_THRESHOLD_BYTES", 5 * 1024 * 1024usize); // 5 MiB min
    let multipart_part_size_bytes = parse_or("MULTIPART_PART_SIZE_BYTES", 8 * 1024 * 1024usize); // 8 MiB
    let multipart_parallel_parts = parse_or("MULTIPART_PARALLEL_PARTS", 4usize);

    // Compression
    let ipc_compression = env::var("IPC_COMPRESSION").unwrap_or_else(|_| "none".into()); // "none" or "zstd"
    let zstd_object = parse_bool("ZSTD_OBJECT", true);
    let zstd_level = parse_or("ZSTD_LEVEL", 6i32);
    let content_type =
        env::var("CONTENT_TYPE").unwrap_or_else(|_| "application/vnd.apache.arrow.file".into());
    let content_encoding_zstd = parse_bool("CONTENT_ENCODING_ZSTD", true);

    // Retry / pipeline
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
