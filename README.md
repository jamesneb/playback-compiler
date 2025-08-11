# Playback Compiler

`playback-compiler` transforms stored telemetry slices into compact replay files that a WebAssembly/WebGPU frontend can play back. It runs as an asynchronous Rust service that consumes jobs from a queue, performs idempotent compilation, and uploads results to object storage.

## Architecture

1. **Ingest** – Jobs are emitted by `playback-orchestrator` into a Redis Stream. Each job is a minimal protobuf payload containing a slice identifier.
2. **Idempotency** – The compiler uses Redis keys to ensure that each job is processed only once and publishes dead-letter entries for decode failures.
3. **Transform** – `decode_job` parses the protobuf payload and `encode_many_ids_arrow_bytes` converts identifiers into Arrow IPC bytes, optionally Zstd-compressed.
4. **Emit** – The resulting replay chunk is uploaded to S3 using multipart uploads when necessary. Objects are stored under `s3://<bucket>/<prefix>/replay-deltas/<job-id>/<timestamp>.arrow` with appropriate content headers.
5. **Ack** – Successful uploads acknowledge the Redis message; failures are retried with exponential backoff.

```text
┌────────────┐       ┌────────────────┐       ┌─────────────┐       ┌──────────────┐
│ Orchestrator├────▶│Redis job queue│────▶│Compiler loop├────▶│S3 replay files│
└────────────┘       └────────────────┘       └─────────────┘       └──────────────┘
```

## Configuration

Configuration is supplied via environment variables (loaded through `.env` when present):

| Variable | Description |
|----------|-------------|
| `REDIS_URL` | Redis connection URL |
| `JOB_QUEUE_NAME` | Redis Stream containing jobs |
| `S3_BUCKET_NAME` | Target bucket for replay files |
| `S3_PREFIX` | Prefix path within the bucket |
| `IDEMPOTENCY_TTL_SECS` | TTL for idempotency keys |
| `IPC_COMPRESSION` | `none` or `zstd` for Arrow IPC stream |
| `ZSTD_OBJECT` | Whether to compress the whole object with Zstd |
| `ZSTD_LEVEL` | Compression level for object-level Zstd |
| `CONTENT_TYPE` | Upload content-type (default `application/vnd.apache.arrow.file`) |
| `CONTENT_ENCODING_ZSTD` | Adds `Content-Encoding: zstd` when true |
| `RETRY_MAX_ATTEMPTS` | Max upload retries |
| `RETRY_BASE_DELAY_MS` | Base delay for exponential backoff |
| `PIPELINE_PARALLELISM` | Maximum concurrent jobs |
| `WINDOW_MAX_BYTES`, `WINDOW_MAX_COUNT`, `WINDOW_MAX_AGE_MS` | Controls job coalescing |
| `FLUSH_TICK_MS`, `SINK_CHANNEL_CAPACITY` | Internal buffering knobs |
| `MULTIPART_THRESHOLD_BYTES`, `MULTIPART_PART_SIZE_BYTES`, `MULTIPART_PARALLEL_PARTS` | Multipart upload tuning |

## Building & Running

```bash
# Build
cargo build --bin compiler

# Run with environment variables configured
REDIS_URL=redis://localhost:6379 \
JOB_QUEUE_NAME=replay-jobs \
S3_BUCKET_NAME=my-bucket \
S3_PREFIX=my-prefix \
IDEMPOTENCY_TTL_SECS=300 \
cargo run --bin compiler
```

## Testing

Unit and integration tests can be executed with:

```bash
cargo test
```

Integration tests use [testcontainers](https://docs.rs/testcontainers) and require a local Docker daemon. If Docker is unavailable, those tests will fail.

## License

This project is licensed under the MIT License.

