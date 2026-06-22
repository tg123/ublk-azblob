//! `BlobBackend` trait and implementations.
//!
//! The trait is the only interface between the ublk I/O loop and the Azure SDK.
//! No Azure SDK types cross this boundary, so SDK upgrades are isolated here.

pub mod azure;
pub mod buffered;
pub mod cache_budget;
pub mod cache_index;
pub mod cache_lru;
pub mod file;
pub mod mem;

use async_trait::async_trait;
use bytes::Bytes;

/// Maximum bytes per Azure Put Page / Put Page From URL request (4 MiB).
pub const MAX_PAGE_REQUEST_BYTES: u64 = 4 * 1024 * 1024;

/// Per-request chunk size used by the template copy paths, in bytes.
///
/// Overridable via `UBLK_COPY_CHUNK_BYTES`; the value is 512-aligned and clamped
/// to `[512, MAX_PAGE_REQUEST_BYTES]` (Azure caps Put Page / Put Page From URL
/// at 4 MiB). Defaults to the 4 MiB maximum.
pub fn copy_chunk_bytes() -> u64 {
    std::env::var("UBLK_COPY_CHUNK_BYTES")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|n| (n / 512 * 512).clamp(512, MAX_PAGE_REQUEST_BYTES))
        .unwrap_or(MAX_PAGE_REQUEST_BYTES)
}

/// Number of logical CPUs, used to size default concurrency for the parallel
/// copy / warm-up paths. Falls back to 8 when the count can't be determined.
pub fn cpu_count() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8)
}

/// Abstraction over a page-blob–like byte store.
///
/// All offsets and lengths **must** be multiples of 512 bytes (Azure Page Blob
/// constraint).  Callers are responsible for alignment; implementations may
/// return an error on mis-aligned requests.
///
/// This trait is the *only* interface the ublk I/O loop uses.  No Azure SDK
/// types appear here, so upgrading the SDK (which is 0.x / preview) only
/// requires changing the `azure` module.
#[async_trait]
#[allow(dead_code)]
pub trait BlobBackend: Send + Sync {
    /// Create (or overwrite) the backing blob with the given size in bytes.
    ///
    /// `size` must be a non-zero multiple of 512.
    async fn create(&self, size: u64) -> anyhow::Result<()>;

    /// Grow the backing store to `new_size` bytes (volume expansion).
    ///
    /// `new_size` must be a non-zero multiple of 512 and must not be smaller
    /// than the current size — shrinking is rejected, matching CSI and Azure
    /// Page Blob semantics. Growing to the current size is a no-op success
    /// (idempotent), so the CSI resize flow can safely retry.
    async fn resize(&self, new_size: u64) -> anyhow::Result<()>;

    /// Read `len` bytes starting at `offset`.
    ///
    /// Both `offset` and `len` must be multiples of 512.
    async fn read(&self, offset: u64, len: u64) -> anyhow::Result<Bytes>;

    /// Write `data` starting at `offset`.
    ///
    /// `offset` must be a multiple of 512 and `data.len()` must be a multiple of 512.
    async fn write(&self, offset: u64, data: Bytes) -> anyhow::Result<()>;

    /// Zero-fill the region `[offset, offset+len)`.
    ///
    /// Both `offset` and `len` must be multiples of 512.
    async fn clear(&self, offset: u64, len: u64) -> anyhow::Result<()>;

    /// Warm the region `[offset, offset+len)` so it is resident locally.
    ///
    /// For cache-backed backends this fetches the region from the underlying
    /// store and stores it in the local cache as *clean* (non-dirty) pages, so
    /// it is safe in read-only mode and does not schedule any write-back.
    /// Backends without a local cache fall back to a plain read so the region
    /// is at least fetched once.  Used by the background warm-up.
    ///
    /// Both `offset` and `len` must be multiples of 512.
    async fn prefetch(&self, offset: u64, len: u64) -> anyhow::Result<()> {
        self.read(offset, len).await.map(|_| ())
    }

    /// Warm `[0, limit_bytes)` into the local cache (if any), best-effort.
    ///
    /// `page_size` is the fetch granularity and `concurrency` bounds the number
    /// of in-flight page fetches. The default implementation is a sequential
    /// `prefetch` scan (no concurrency) that stops on the first read error;
    /// cache-backed backends override it to fetch pages from the blob in
    /// parallel (bandwidth- rather than latency-bound) and, being best-effort,
    /// log and skip individual failed pages while continuing the warm-up — the
    /// device keeps serving any missed regions on demand.
    async fn warmup(&self, dev_size: u64, page_size: u64, limit_bytes: u64, concurrency: usize) {
        let _ = concurrency; // honoured only by cache-backed backends
        let limit = limit_bytes.min(dev_size);
        let mut offset = 0u64;
        let mut warmed = 0u64;
        while offset < limit {
            let len = page_size.min(dev_size - offset);
            if let Err(err) = self.prefetch(offset, len).await {
                tracing::warn!(offset, %err, "cache warm-up read failed; stopping early");
                break;
            }
            warmed += len;
            offset += len;
            tokio::task::yield_now().await;
        }
        tracing::info!(
            warmed_bytes = warmed,
            limit_bytes = limit,
            "cache warm-up complete"
        );
    }

    /// Flush any pending writes to durable storage.
    ///
    /// For write-through backends this is a no-op; for write-back caches it
    /// drains the dirty buffer.
    async fn flush(&self) -> anyhow::Result<()>;

    /// Delete the backing blob entirely.
    ///
    /// Used by the CSI controller when a PersistentVolume is removed.  Deleting
    /// a blob that does not exist is treated as success (idempotent).
    async fn delete(&self) -> anyhow::Result<()>;

    /// Return the current size of the backing store in bytes.
    async fn size(&self) -> anyhow::Result<u64>;
}
