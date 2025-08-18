pub mod app;
pub mod config;
pub mod emit;
pub mod errors;
pub mod ingest;
pub mod proto;
pub mod redis;
pub mod s3;
pub mod transform;
pub mod types;
pub mod util;
// Configure a global allocator optimized for throughput.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
