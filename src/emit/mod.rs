//! Facilities for emitting compiled replay deltas to storage.
use anyhow::Result;
use core::future::Future;

#[allow(clippy::manual_async_fn)]
pub trait BlobStore: Clone + Send + Sync + 'static {
    fn put_bytes<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        data: Vec<u8>,
        content_type: Option<&'a str>,
        content_encoding: Option<&'a str>,
    ) -> impl Future<Output = Result<()>> + Send + 'a;
}
