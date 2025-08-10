pub mod config;
pub mod emit;
pub mod errors;
pub mod ingest;
pub mod proto;
pub mod redis;
pub mod transform;
pub mod types;
// Use mimalloc as the global allocator for lower alloc overhead in hot paths.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
