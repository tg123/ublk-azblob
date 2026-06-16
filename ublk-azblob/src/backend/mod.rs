//! `BlobBackend` trait and implementations.
//!
//! The trait is the only interface between the ublk I/O loop and the Azure SDK.
//! No Azure SDK types cross this boundary, so SDK upgrades are isolated here.

pub mod azure;
pub mod buffered;
pub mod file;
pub mod mem;

use async_trait::async_trait;
use bytes::Bytes;

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

    /// Flush any pending writes to durable storage.
    ///
    /// For write-through backends this is a no-op; for write-back caches it
    /// drains the dirty buffer.
    async fn flush(&self) -> anyhow::Result<()>;

    /// Return the current size of the backing store in bytes.
    async fn size(&self) -> anyhow::Result<u64>;
}
