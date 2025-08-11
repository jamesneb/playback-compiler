//! Sink that batches deltas and uploads them to S3.
//!
//! Data is encoded as Arrow IPC files and optionally compressed with Zstd
//! before being stored as either single PUTs or multipart uploads.

use crate::transform::encode::encode_many_ids_arrow_bytes;
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use rustc_hash::FxHashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::{Interval, interval};
use tracing::{debug, error, info};

use crate::emit::uploader::{multipart_upload, upload_bytes_to_s3};

#[derive(Clone, Debug)]
pub struct CoalescingConfig {
    /// Target S3 bucket.
    pub bucket: String,
    /// Prefix within the bucket where objects are written.
    pub prefix: String,
    /// Maximum aggregate bytes per window.
    pub max_bytes: usize,
    /// Maximum number of IDs per window.
    pub max_count: usize,
    /// Maximum time a window may remain open.
    pub max_age: Duration,
    /// Threshold at which multipart upload is used.
    pub multipart_threshold: usize,
    /// Size of each multipart chunk.
    pub multipart_part_size: usize,
    /// Number of parts uploaded concurrently.
    pub multipart_parallel_parts: usize,
    /// Capacity for the input channel.
    pub channel_capacity: usize,
    /// Placeholder for future IPC-level compression support.
    pub use_zstd_ipc: bool,
    /// Enable object-level Zstd compression.
    pub zstd_object: bool,
    /// Zstd compression level.
    pub zstd_level: i32,
    /// Max retry attempts for uploads.
    pub retry_max_attempts: usize,
    /// Base delay in milliseconds between retries.
    pub retry_base_delay_ms: u64,
    /// Interval in milliseconds for background flush checks.
    pub flush_tick_ms: u64,
    /// MIME type to set on uploaded objects.
    pub content_type: String,
    /// Whether to advertise `Content-Encoding: zstd`.
    pub content_encoding_zstd: bool,
}

#[derive(Clone, Debug)]
pub struct DeltaItem {
    pub tenant: String,
    pub id: Bytes,
    pub ts_ns: i64,
}

#[derive(Clone)]
pub struct CoalescingS3Sink {
    tx: Sender<DeltaItem>,
    stop_tx: Sender<()>,
}

impl CoalescingS3Sink {
    pub fn start(cfg: CoalescingConfig, s3: S3Client) -> Self {
        let (tx, rx) = mpsc::channel::<DeltaItem>(cfg.channel_capacity);
        let (stop_tx, stop_rx) = mpsc::channel::<()>(1);

        tokio::spawn(async move {
            if let Err(e) = run_sink(rx, stop_rx, &cfg, &s3).await {
                error!(error=%e, "sink task exited with error");
            }
        });

        Self { tx, stop_tx }
    }

    pub fn sender(&self) -> Sender<DeltaItem> {
        self.tx.clone()
    }

    pub async fn shutdown(self) {
        let _ = self.stop_tx.send(()).await;
    }
}

struct Window {
    ids: Vec<Bytes>,
    total_bytes: usize,
    count: usize,
    started_at: Instant,
}

impl Window {
    fn new() -> Self {
        Self {
            ids: Vec::new(),
            total_bytes: 0,
            count: 0,
            started_at: Instant::now(),
        }
    }
    fn push(&mut self, id: Bytes) {
        self.total_bytes += id.len();
        self.count += 1;
        self.ids.push(id);
    }
    fn should_flush(&self, cfg: &CoalescingConfig, now: Instant) -> bool {
        self.count >= cfg.max_count
            || self.total_bytes >= cfg.max_bytes
            || now.duration_since(self.started_at) >= cfg.max_age
    }
    fn reset(&mut self) {
        self.ids.clear();
        self.total_bytes = 0;
        self.count = 0;
        self.started_at = Instant::now();
    }
}

async fn run_sink(
    mut rx: Receiver<DeltaItem>,
    mut stop_rx: Receiver<()>,
    cfg: &CoalescingConfig,
    s3: &S3Client,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut by_tenant: FxHashMap<String, Window> = FxHashMap::default();
    let mut ticker: Interval = interval(Duration::from_millis(cfg.flush_tick_ms));

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                flush_due(&mut by_tenant, cfg, s3).await?;
            }
            _ = stop_rx.recv() => {
                info!("sink: shutdown signal received; flushing all");
                flush_all(&mut by_tenant, cfg, s3).await?;
                break;
            }
            maybe_item = rx.recv() => {
                match maybe_item {
                    Some(item) => {
                        let win = by_tenant.entry(item.tenant).or_insert_with(Window::new);
                        win.push(item.id);
                        if win.should_flush(cfg, Instant::now()) {
                            flush_tenant(win, cfg, s3, &cfg.prefix).await?;
                            win.reset();
                        }
                    }
                    None => {
                        info!("sink: channel closed; flushing all");
                        flush_all(&mut by_tenant, cfg, s3).await?;
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

async fn flush_due(
    map: &mut FxHashMap<String, Window>,
    cfg: &CoalescingConfig,
    s3: &S3Client,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let now = Instant::now();
    for (tenant, win) in map.iter_mut() {
        if win.count > 0 && win.should_flush(cfg, now) {
            let prefix = format!("{}/{}", cfg.prefix, tenant);
            flush_tenant(win, cfg, s3, &prefix).await?;
            win.reset();
        }
    }
    Ok(())
}

async fn flush_all(
    map: &mut FxHashMap<String, Window>,
    cfg: &CoalescingConfig,
    s3: &S3Client,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for (tenant, win) in map.iter_mut() {
        if win.count > 0 {
            let prefix = format!("{}/{}", cfg.prefix, tenant);
            flush_tenant(win, cfg, s3, &prefix).await?;
            win.reset();
        }
    }
    Ok(())
}

async fn flush_tenant(
    win: &Window,
    cfg: &CoalescingConfig,
    s3: &S3Client,
    prefix: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if win.count == 0 {
        return Ok(());
    }

    // Encode ids to Arrow IPC format.
    let encoded =
        encode_many_ids_arrow_bytes(&win.ids, cfg.use_zstd_ipc /* unused on Arrow 56 */)?;

    // Optionally compress the entire object with Zstd.
    let (object_bytes, key, content_type, content_encoding) = if cfg.zstd_object {
        let compressed = zstd::encode_all(&*encoded, cfg.zstd_level)?;
        let ts_ns = now_ns();
        (
            compressed,
            format!("{}/replay-deltas/{:013}.arrow.zst", prefix, ts_ns),
            Some(cfg.content_type.as_str()),
            if cfg.content_encoding_zstd {
                Some("zstd")
            } else {
                None
            },
        )
    } else {
        let ts_ns = now_ns();
        (
            encoded,
            format!("{}/replay-deltas/{:013}.arrow", prefix, ts_ns),
            Some(cfg.content_type.as_str()),
            None,
        )
    };

    // Choose single or multipart upload based on object size.
    if object_bytes.len() >= cfg.multipart_threshold {
        let parts = split_parts(&object_bytes, cfg.multipart_part_size);
        multipart_upload(
            s3,
            &cfg.bucket,
            &key,
            parts,
            cfg.multipart_parallel_parts,
            content_type,
            content_encoding,
        )
        .await?;
    } else {
        upload_bytes_to_s3(
            s3,
            &cfg.bucket,
            &key,
            object_bytes,
            content_type,
            content_encoding,
        )
        .await?;
    }

    debug!(%key, bytes=%win.total_bytes, count=%win.count, "flushed window");
    Ok(())
}

fn split_parts(buf: &[u8], part_size: usize) -> Vec<(i32, Vec<u8>)> {
    let mut out = Vec::new();
    if buf.is_empty() {
        return out;
    }
    let mut idx: i32 = 1;
    let mut start = 0usize;
    while start < buf.len() {
        let end = (start + part_size).min(buf.len());
        out.push((idx, buf[start..end].to_vec()));
        idx += 1;
        start = end;
    }
    out
}

fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}
