//! Write-back buffered backend.
//!
//! `BufferedBackend` wraps any `BlobBackend` and accumulates writes in memory,
//! flushing dirty "pages" (fixed-size regions) to the inner backend in batches.
//! Reads are served from the buffer when the requested region overlaps dirty
//! data, falling back to the inner backend for clean regions.
//!
//! # Parameters
//! - `page_size`: Size of each buffer page (e.g. 4 MiB).  Must be a multiple
//!   of 512 bytes.
//! - `max_dirty_pages`: When the number of dirty pages exceeds this limit,
//!   the oldest dirty pages are auto-flushed before accepting new writes.

use super::BlobBackend;
use anyhow::{bail, Context as _};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::stream::{StreamExt as _, TryStreamExt as _};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{info, trace, warn};

/// Configuration for the write-back buffer.
#[derive(Debug, Clone)]
pub struct BufferedConfig {
    /// Size of each buffer page in bytes (must be a multiple of 512).
    pub page_size: u64,
    /// Maximum number of dirty (unflushed) pages kept in the in-memory
    /// write-back buffer. Exceeding it triggers an immediate flush of the
    /// oldest dirty pages back down to the limit.
    pub max_dirty_pages: usize,
    /// Idle flush timeout in seconds: flush dirty pages after N seconds of write inactivity.
    /// Set to 0 to disable idle flushing. When triggered, resets the force flush timer.
    pub idle_flush_secs: u64,
    /// Force flush timeout in seconds: maximum time since last successful flush before
    /// forcing a flush regardless of activity. Set to 0 for no timeout. Reset by idle flushes.
    ///
    /// This is purely a *scheduling interval* for the background task — it does
    /// not bound how long an individual flush may take (see `flush_io_timeout_secs`).
    pub force_flush_timeout_secs: u64,
    /// Optional hard timeout (in seconds) on a single flush I/O operation.
    /// Set to 0 (the default) for no cap, so explicit and shutdown flushes can
    /// run to completion even when there are many dirty pages or a slow link.
    pub flush_io_timeout_secs: u64,
    /// Maximum number of dirty pages flushed concurrently to the inner backend.
    ///
    /// Flushing is latency-bound when the inner backend is remote (e.g. Azure in
    /// a distant region), so issuing several page writes in flight at once is the
    /// key throughput lever. Each in-flight write holds its own snapshot of the
    /// page, so the transient extra memory is bounded by
    /// `page_size × flush_concurrency`. A value of `1` restores fully sequential
    /// flushing. Values are clamped to at least `1`.
    pub flush_concurrency: usize,
}

impl Default for BufferedConfig {
    fn default() -> Self {
        Self {
            page_size: 4 * 1024 * 1024,    // 4 MiB
            max_dirty_pages: 64,           // 256 MiB total
            idle_flush_secs: 15,           // flush after 15s idle
            force_flush_timeout_secs: 600, // force flush after 10 minutes
            flush_io_timeout_secs: 0,      // no per-flush I/O cap by default
            flush_concurrency: 16,         // up to 16 pages in flight per flush
        }
    }
}

/// A single buffer page: either fully in-memory or partially written.
///
/// Pages are always `page_size` bytes.  Regions that have not been written are
/// `None` (meaning "read from inner backend on demand" or "zero for new blobs").
struct Page {
    /// The actual data buffer — always `page_size` bytes.
    data: BytesMut,
    /// Whether this page has been modified since the last flush.
    dirty: bool,
    /// Sequence number for LRU eviction.
    seq: u64,
}

/// Write-back buffered wrapper around any `BlobBackend`.
pub struct BufferedBackend {
    inner: Arc<dyn BlobBackend>,
    config: BufferedConfig,
    state: Mutex<BufferState>,
}

struct BufferState {
    /// Map from page index → page data.
    pages: BTreeMap<u64, Page>,
    /// Monotonic counter for LRU ordering.
    seq_counter: u64,
    /// Total device size (set after create/size call).
    dev_size: u64,
    /// Timestamp of last write operation (for idle flush detection).
    last_write: Option<Instant>,
    /// Timestamp of last successful flush (for force flush timer).
    last_flush: Option<Instant>,
}

impl BufferedBackend {
    pub fn new(inner: Arc<dyn BlobBackend>, config: BufferedConfig) -> anyhow::Result<Arc<Self>> {
        if config.page_size < 512 || !config.page_size.is_multiple_of(512) {
            bail!(
                "page_size ({}) must be a non-zero multiple of 512",
                config.page_size
            );
        }
        if config.max_dirty_pages == 0 {
            bail!("max_dirty_pages must be greater than 0");
        }
        let backend = Arc::new(Self {
            inner,
            config: config.clone(),
            state: Mutex::new(BufferState {
                pages: BTreeMap::new(),
                seq_counter: 0,
                dev_size: 0,
                last_write: None,
                last_flush: None,
            }),
        });

        // Spawn idle flush and force flush task if configured
        if config.idle_flush_secs > 0 || config.force_flush_timeout_secs > 0 {
            // Hold a Weak reference so this task does not keep the backend alive:
            // when the last owner drops the `Arc`, `upgrade()` returns `None` and
            // the task exits instead of leaking the backend in an infinite loop.
            let backend_weak = Arc::downgrade(&backend);
            // Check at least twice as often as the smallest configured timeout,
            // but never faster than once per second (the `/2` and `/4` divisions
            // can otherwise round down to a 0s interval and busy-loop).
            let check_interval = if config.idle_flush_secs > 0 {
                Duration::from_secs((config.idle_flush_secs / 2).max(1))
            } else {
                Duration::from_secs((config.force_flush_timeout_secs / 4).max(1))
            };
            let idle_timeout = Duration::from_secs(config.idle_flush_secs);
            let force_timeout = Duration::from_secs(config.force_flush_timeout_secs);

            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(check_interval).await;

                    // Stop once the backend has been dropped.
                    let Some(backend_clone) = backend_weak.upgrade() else {
                        break;
                    };

                    let (should_idle_flush, should_force_flush) = {
                        let state = backend_clone.state.lock().await;
                        let has_dirty = state.pages.values().any(|p| p.dirty);

                        let idle_trigger = if config.idle_flush_secs > 0 {
                            if let Some(last_write) = state.last_write {
                                last_write.elapsed() >= idle_timeout && has_dirty
                            } else {
                                false
                            }
                        } else {
                            false
                        };

                        let force_trigger = if config.force_flush_timeout_secs > 0 {
                            // Base the timer on the last flush, or — if we have
                            // never flushed — on the first write, so a force flush
                            // only fires once `force_flush_timeout_secs` has
                            // actually elapsed (not immediately on the first write).
                            match (state.last_flush, state.last_write) {
                                (Some(last_flush), _) => {
                                    last_flush.elapsed() >= force_timeout && has_dirty
                                }
                                (None, Some(last_write)) => {
                                    last_write.elapsed() >= force_timeout && has_dirty
                                }
                                (None, None) => false,
                            }
                        } else {
                            false
                        };

                        (idle_trigger, force_trigger)
                    };

                    if should_idle_flush {
                        info!(
                            "idle flush triggered after {}s of inactivity",
                            config.idle_flush_secs
                        );
                        if let Err(e) = backend_clone.flush().await {
                            warn!("idle flush failed: {:#}", e);
                        }
                    } else if should_force_flush {
                        info!(
                            "force flush triggered after {}s since last flush",
                            config.force_flush_timeout_secs
                        );
                        if let Err(e) = backend_clone.flush().await {
                            warn!("force flush failed: {:#}", e);
                        }
                    }
                }
            });
        }

        Ok(backend)
    }

    /// Return the device size, lazily initialising it from the inner backend.
    ///
    /// The state lock is never held across the inner `.await`.
    async fn dev_size(&self) -> anyhow::Result<u64> {
        {
            let state = self.state.lock().await;
            if state.dev_size != 0 {
                return Ok(state.dev_size);
            }
        }
        let sz = self.inner.size().await?;
        let mut state = self.state.lock().await;
        if state.dev_size == 0 {
            state.dev_size = sz;
        }
        Ok(state.dev_size)
    }

    /// Ensure `page_idx` is resident, loading it from the inner backend if
    /// necessary.  The inner read happens **without** the state lock held; the
    /// lock is only taken briefly to check residency and to insert the page.
    ///
    /// Pages are never removed once resident, so after this returns the page is
    /// guaranteed to stay present for subsequent `get_mut` access.
    async fn ensure_resident(&self, page_idx: u64, dev_size: u64) -> anyhow::Result<()> {
        {
            let state = self.state.lock().await;
            if state.pages.contains_key(&page_idx) {
                return Ok(());
            }
        }

        let page_size = self.config.page_size;
        let offset = page_idx * page_size;
        let read_len = if dev_size > 0 {
            dev_size.saturating_sub(offset).min(page_size)
        } else {
            0
        };

        let mut buf = BytesMut::zeroed(page_size as usize);
        if read_len > 0 {
            let data = self
                .inner
                .read(offset, read_len)
                .await
                .with_context(|| format!("load page {page_idx} from backend"))?;
            if data.len() as u64 != read_len {
                bail!(
                    "backend returned {} bytes loading page {page_idx} (expected {read_len})",
                    data.len()
                );
            }
            buf[..data.len()].copy_from_slice(&data);
        }

        let mut state = self.state.lock().await;
        // Another task may have loaded the page while we were reading; if so,
        // keep theirs (never clobber a resident page) and drop our load.
        if !state.pages.contains_key(&page_idx) {
            state.seq_counter += 1;
            let seq = state.seq_counter;
            state.pages.insert(
                page_idx,
                Page {
                    data: buf,
                    dirty: false,
                    seq,
                },
            );
        }
        Ok(())
    }

    /// Flush the given pages to the inner backend.
    ///
    /// Pages are flushed with bounded concurrency (`flush_concurrency`): up to
    /// that many page writes are in flight at once, which is the key throughput
    /// lever when the inner backend is latency-bound (e.g. a remote blob store).
    ///
    /// Each page's bytes are snapshotted under a brief lock (so at most
    /// `flush_concurrency` snapshots exist at once), written to the inner backend
    /// with the lock released, then marked clean under the lock — but only if the
    /// page was not modified again during the write (detected via its sequence
    /// number). This guarantees no dirty data is lost.
    ///
    /// If `flush_io_timeout_secs` is configured, the entire flush operation
    /// must complete within that timeout or it will be aborted.
    async fn flush_indices(&self, indices: &[u64]) -> anyhow::Result<()> {
        let page_size = self.config.page_size;
        let concurrency = self.config.flush_concurrency.max(1);
        let timeout = if self.config.flush_io_timeout_secs > 0 {
            Some(Duration::from_secs(self.config.flush_io_timeout_secs))
        } else {
            None
        };

        let flush_task = async {
            futures::stream::iter(indices.iter().copied().map(|page_idx| async move {
                // Snapshot the dirty page under a brief lock (no await held).
                let snapshot = {
                    let state = self.state.lock().await;
                    match state.pages.get(&page_idx) {
                        Some(p) if p.dirty => {
                            let offset = page_idx * page_size;
                            let write_len = if state.dev_size > 0 {
                                (p.data.len() as u64).min(state.dev_size.saturating_sub(offset))
                            } else {
                                p.data.len() as u64
                            };
                            Some((
                                p.seq,
                                offset,
                                write_len,
                                Bytes::copy_from_slice(&p.data[..]),
                            ))
                        }
                        _ => None,
                    }
                };

                let Some((seq, offset, write_len, data)) = snapshot else {
                    return Ok(());
                };

                if write_len > 0 {
                    trace!(
                        page_idx,
                        offset,
                        len = write_len,
                        "flushing page to backend"
                    );
                    self.inner
                        .write(offset, data.slice(..write_len as usize))
                        .await
                        .with_context(|| format!("flush page {page_idx} at offset {offset}"))?;
                }

                // Mark clean only if the page wasn't re-dirtied during the flush.
                let mut state = self.state.lock().await;
                if let Some(p) = state.pages.get_mut(&page_idx) {
                    if p.seq == seq {
                        p.dirty = false;
                    }
                }
                Ok::<(), anyhow::Error>(())
            }))
            .buffer_unordered(concurrency)
            .try_collect::<()>()
            .await
        };

        if let Some(timeout_duration) = timeout {
            match tokio::time::timeout(timeout_duration, flush_task).await {
                Ok(result) => result,
                Err(_) => {
                    bail!(
                        "flush timed out after {}s (flush_io_timeout_secs exceeded)",
                        self.config.flush_io_timeout_secs
                    );
                }
            }
        } else {
            flush_task.await
        }
    }

    /// If the number of dirty pages exceeds `max_dirty_pages`, flush the oldest
    /// dirty pages until the limit is satisfied.
    async fn enforce_dirty_limit(&self) -> anyhow::Result<()> {
        let victims: Vec<u64> = {
            let state = self.state.lock().await;
            let dirty = state.pages.values().filter(|p| p.dirty).count();
            if dirty <= self.config.max_dirty_pages {
                return Ok(());
            }
            let over = dirty - self.config.max_dirty_pages;
            let mut by_age: Vec<(u64, u64)> = state
                .pages
                .iter()
                .filter(|(_, p)| p.dirty)
                .map(|(&idx, p)| (p.seq, idx))
                .collect();
            by_age.sort_unstable();
            by_age.iter().take(over).map(|&(_, idx)| idx).collect()
        };

        if !victims.is_empty() {
            info!(
                evicting = victims.len(),
                "auto-flushing dirty pages over limit"
            );
            self.flush_indices(&victims).await?;
        }
        Ok(())
    }
}

/// Reject requests whose `[offset, offset+len)` range falls outside the device.
fn check_in_bounds(op: &str, offset: u64, len: u64, dev_size: u64) -> anyhow::Result<()> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| anyhow::anyhow!("{op}: offset ({offset}) + len ({len}) overflows u64"))?;
    if end > dev_size {
        bail!("{op} out of bounds: offset={offset} len={len} dev_size={dev_size}");
    }
    Ok(())
}

#[async_trait]
impl BlobBackend for BufferedBackend {
    async fn create(&self, size: u64) -> anyhow::Result<()> {
        self.inner.create(size).await?;
        let mut state = self.state.lock().await;
        state.dev_size = size;
        state.pages.clear();
        state.seq_counter = 0;
        Ok(())
    }

    async fn resize(&self, new_size: u64) -> anyhow::Result<()> {
        // Grow the underlying store first, then update the cached device size so
        // reads/writes past the old end pass the in-bounds check. Buffered dirty
        // pages stay valid (the grown region is appended after them).
        self.inner.resize(new_size).await?;
        let mut state = self.state.lock().await;
        state.dev_size = new_size;
        Ok(())
    }

    async fn read(&self, offset: u64, len: u64) -> anyhow::Result<Bytes> {
        if len == 0 {
            return Ok(Bytes::new());
        }
        if !offset.is_multiple_of(512) || !len.is_multiple_of(512) {
            bail!("read: offset ({offset}) and len ({len}) must be 512-byte aligned");
        }

        let dev_size = self.dev_size().await?;
        check_in_bounds("read", offset, len, dev_size)?;

        let page_size = self.config.page_size;
        let mut result = BytesMut::zeroed(len as usize);
        let mut pos: u64 = 0;

        while pos < len {
            let abs_offset = offset + pos;
            let page_idx = abs_offset / page_size;
            let page_offset = abs_offset % page_size;
            let chunk_len = (page_size - page_offset).min(len - pos) as usize;

            // Try to serve from the buffer under a brief lock (no await held).
            let served = {
                let state = self.state.lock().await;
                if let Some(page) = state.pages.get(&page_idx) {
                    result[pos as usize..pos as usize + chunk_len].copy_from_slice(
                        &page.data[page_offset as usize..page_offset as usize + chunk_len],
                    );
                    true
                } else {
                    false
                }
            };

            if !served {
                // Read directly from inner with no lock held; don't pollute the
                // cache for pure reads.
                let data = self
                    .inner
                    .read(abs_offset, chunk_len as u64)
                    .await
                    .with_context(|| format!("read offset={abs_offset} len={chunk_len}"))?;
                if data.len() != chunk_len {
                    bail!(
                        "backend returned {} bytes for read offset={abs_offset} len={chunk_len}",
                        data.len()
                    );
                }
                result[pos as usize..pos as usize + chunk_len].copy_from_slice(&data);
            }

            pos += chunk_len as u64;
        }

        Ok(result.freeze())
    }

    async fn write(&self, offset: u64, data: Bytes) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        if !offset.is_multiple_of(512) || !(data.len() as u64).is_multiple_of(512) {
            bail!(
                "write: offset ({offset}) and data.len() ({}) must be 512-byte aligned",
                data.len()
            );
        }

        let dev_size = self.dev_size().await?;
        let len = data.len() as u64;
        check_in_bounds("write", offset, len, dev_size)?;

        let page_size = self.config.page_size;
        let mut pos: u64 = 0;

        while pos < len {
            let abs_offset = offset + pos;
            let page_idx = abs_offset / page_size;
            let page_offset = (abs_offset % page_size) as usize;
            let chunk_len = (page_size - page_offset as u64).min(len - pos) as usize;

            // Load the page (inner I/O happens without the state lock held).
            self.ensure_resident(page_idx, dev_size).await?;

            // Apply the modification under a brief lock (no await held).
            {
                let mut state = self.state.lock().await;
                state.seq_counter += 1;
                let seq = state.seq_counter;
                let page = state
                    .pages
                    .get_mut(&page_idx)
                    .expect("page resident after ensure_resident");
                page.data[page_offset..page_offset + chunk_len]
                    .copy_from_slice(&data[pos as usize..pos as usize + chunk_len]);
                page.dirty = true;
                page.seq = seq;
                // Track last write time for idle flush
                state.last_write = Some(Instant::now());
            }

            // Keep the dirty-page count bounded (flushes oldest, lock released
            // across the inner writes).
            self.enforce_dirty_limit().await?;

            pos += chunk_len as u64;
        }

        Ok(())
    }

    async fn clear(&self, offset: u64, len: u64) -> anyhow::Result<()> {
        if len == 0 {
            return Ok(());
        }
        if !offset.is_multiple_of(512) || !len.is_multiple_of(512) {
            bail!("clear: offset ({offset}) and len ({len}) must be 512-byte aligned");
        }

        let dev_size = self.dev_size().await?;
        check_in_bounds("clear", offset, len, dev_size)?;

        let page_size = self.config.page_size;
        let mut pos: u64 = 0;

        while pos < len {
            let abs_offset = offset + pos;
            let page_idx = abs_offset / page_size;
            let page_offset = (abs_offset % page_size) as usize;
            let chunk_len = (page_size - page_offset as u64).min(len - pos) as usize;

            self.ensure_resident(page_idx, dev_size).await?;

            {
                let mut state = self.state.lock().await;
                state.seq_counter += 1;
                let seq = state.seq_counter;
                let page = state
                    .pages
                    .get_mut(&page_idx)
                    .expect("page resident after ensure_resident");
                page.data[page_offset..page_offset + chunk_len].fill(0);
                page.dirty = true;
                page.seq = seq;
                // Track last write time for idle flush
                state.last_write = Some(Instant::now());
            }

            self.enforce_dirty_limit().await?;

            pos += chunk_len as u64;
        }

        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        let dirty_indices: Vec<u64> = {
            let state = self.state.lock().await;
            state
                .pages
                .iter()
                .filter(|(_, p)| p.dirty)
                .map(|(&idx, _)| idx)
                .collect()
        };

        if !dirty_indices.is_empty() {
            info!(
                dirty_pages = dirty_indices.len(),
                "flushing all dirty pages"
            );
            self.flush_indices(&dirty_indices).await?;

            // Update last_flush timestamp after successful flush
            let mut state = self.state.lock().await;
            state.last_flush = Some(Instant::now());
        }

        // Also flush the inner backend in case it has its own buffering.
        self.inner.flush().await
    }

    async fn delete(&self) -> anyhow::Result<()> {
        // Drop any buffered dirty pages and delegate to the inner backend.
        let mut state = self.state.lock().await;
        state.pages.clear();
        drop(state);
        self.inner.delete().await
    }

    async fn size(&self) -> anyhow::Result<u64> {
        self.dev_size().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::mem::MemBackend;

    fn make_backend(dev_size: u64, page_size: u64, max_dirty: usize) -> Arc<BufferedBackend> {
        let inner = Arc::new(MemBackend::new(dev_size).unwrap());
        BufferedBackend::new(
            inner,
            BufferedConfig {
                page_size,
                max_dirty_pages: max_dirty,
                idle_flush_secs: 0,
                force_flush_timeout_secs: 0,
                flush_io_timeout_secs: 0,
                flush_concurrency: 16,
            },
        )
        .unwrap()
    }

    #[tokio::test]
    async fn write_read_roundtrip() {
        let b = make_backend(4096, 1024, 4);
        let data = Bytes::from(vec![0xAB; 512]);
        b.write(0, data.clone()).await.unwrap();
        let read = b.read(0, 512).await.unwrap();
        assert_eq!(read, data);
    }

    #[tokio::test]
    async fn resize_grows_device_and_inner() {
        let b = make_backend(4096, 1024, 4);
        let data = Bytes::from(vec![0xAB; 512]);
        b.write(0, data.clone()).await.unwrap();
        // Grow the device.
        b.resize(8192).await.unwrap();
        assert_eq!(b.size().await.unwrap(), 8192);
        // Existing buffered data survives.
        assert_eq!(b.read(0, 512).await.unwrap(), data);
        // The newly-available region is now in bounds and reads back zeroed.
        let tail = b.read(4096, 512).await.unwrap();
        assert!(tail.iter().all(|&x| x == 0));
        // A write into the grown region round-trips and persists on flush.
        let grown = Bytes::from(vec![0xCD; 512]);
        b.write(8192 - 512, grown.clone()).await.unwrap();
        b.flush().await.unwrap();
        assert_eq!(b.read(8192 - 512, 512).await.unwrap(), grown);
        // Shrink is rejected.
        assert!(b.resize(4096).await.is_err(), "shrink rejected");
    }

    #[tokio::test]
    async fn write_spanning_pages() {
        let b = make_backend(4096, 1024, 4);
        // Write 2048 bytes starting at offset 512 — spans pages 0 and 1.
        let data = Bytes::from(vec![0xCD; 2048]);
        b.write(512, data.clone()).await.unwrap();
        let read = b.read(512, 2048).await.unwrap();
        assert_eq!(read, data);
    }

    #[tokio::test]
    async fn flush_persists_to_inner() {
        let inner = Arc::new(MemBackend::new(4096).unwrap());
        let b = BufferedBackend::new(
            inner.clone(),
            BufferedConfig {
                page_size: 1024,
                max_dirty_pages: 4,
                idle_flush_secs: 0,
                force_flush_timeout_secs: 0,
                flush_io_timeout_secs: 0,
                flush_concurrency: 16,
            },
        )
        .unwrap();
        let data = Bytes::from(vec![0xEF; 512]);
        b.write(0, data.clone()).await.unwrap();

        // Before flush, inner should still be zeros.
        let inner_read = inner.read(0, 512).await.unwrap();
        assert!(inner_read.iter().all(|&x| x == 0));

        b.flush().await.unwrap();

        // After flush, inner should have our data.
        let inner_read = inner.read(0, 512).await.unwrap();
        assert_eq!(inner_read, data);
    }

    #[tokio::test]
    async fn auto_flush_on_limit() {
        let inner = Arc::new(MemBackend::new(4096).unwrap());
        let b = BufferedBackend::new(
            inner.clone(),
            BufferedConfig {
                page_size: 1024,
                max_dirty_pages: 2,
                idle_flush_secs: 0,
                force_flush_timeout_secs: 0,
                flush_io_timeout_secs: 0,
                flush_concurrency: 16,
            },
        )
        .unwrap();

        // Write to 3 different pages — should trigger auto-flush of the oldest.
        b.write(0, Bytes::from(vec![0x11; 512])).await.unwrap();
        b.write(1024, Bytes::from(vec![0x22; 512])).await.unwrap();
        b.write(2048, Bytes::from(vec![0x33; 512])).await.unwrap();

        // The first page (oldest) should have been flushed to inner.
        let inner_read = inner.read(0, 512).await.unwrap();
        assert_eq!(inner_read, Bytes::from(vec![0x11; 512]));
    }

    #[tokio::test]
    async fn concurrent_flush_persists_all_dirty_pages() {
        // Many dirty pages flushed at once must all reach the inner backend,
        // regardless of the (bounded, out-of-order) flush concurrency.
        const PAGES: u64 = 40;
        let page_size = 1024u64;
        let dev_size = PAGES * page_size;
        let inner = Arc::new(MemBackend::new(dev_size).unwrap());
        let b = BufferedBackend::new(
            inner.clone(),
            BufferedConfig {
                page_size,
                max_dirty_pages: PAGES as usize, // keep them all dirty until flush()
                idle_flush_secs: 0,
                force_flush_timeout_secs: 0,
                flush_io_timeout_secs: 0,
                flush_concurrency: 8,
            },
        )
        .unwrap();

        for p in 0..PAGES {
            let byte = (p & 0xFF) as u8;
            b.write(p * page_size, Bytes::from(vec![byte; page_size as usize]))
                .await
                .unwrap();
        }

        b.flush().await.unwrap();

        for p in 0..PAGES {
            let byte = (p & 0xFF) as u8;
            let got = inner.read(p * page_size, page_size).await.unwrap();
            assert_eq!(got, Bytes::from(vec![byte; page_size as usize]), "page {p}");
        }
    }

    #[tokio::test]
    async fn flush_concurrency_clamped_to_at_least_one() {
        // A configured concurrency of 0 must not deadlock or skip pages; it is
        // clamped up to 1 (fully sequential).
        let inner = Arc::new(MemBackend::new(4096).unwrap());
        let b = BufferedBackend::new(
            inner.clone(),
            BufferedConfig {
                page_size: 1024,
                max_dirty_pages: 8,
                idle_flush_secs: 0,
                force_flush_timeout_secs: 0,
                flush_io_timeout_secs: 0,
                flush_concurrency: 0,
            },
        )
        .unwrap();

        let data = Bytes::from(vec![0x5A; 512]);
        b.write(0, data.clone()).await.unwrap();
        b.flush().await.unwrap();
        assert_eq!(inner.read(0, 512).await.unwrap(), data);
    }

    #[tokio::test]
    async fn clear_marks_dirty() {
        let b = make_backend(4096, 1024, 4);
        let data = Bytes::from(vec![0xFF; 512]);
        b.write(0, data).await.unwrap();
        b.clear(0, 512).await.unwrap();
        let read = b.read(0, 512).await.unwrap();
        assert!(read.iter().all(|&x| x == 0));
    }

    #[tokio::test]
    async fn out_of_bounds_is_rejected() {
        let b = make_backend(2048, 1024, 4);
        // Reads, writes and clears past dev_size must fail rather than silently
        // succeed or drop data.
        assert!(b.read(1536, 1024).await.is_err(), "read past end");
        assert!(
            b.write(1536, Bytes::from(vec![0u8; 1024])).await.is_err(),
            "write past end"
        );
        assert!(b.clear(1536, 1024).await.is_err(), "clear past end");
    }

    // Concurrent writes to distinct pages, interleaved with a flush, must not
    // deadlock and every write must survive — exercising the path where the
    // state lock is released across inner I/O.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writes_are_consistent() {
        const PAGES: u64 = 32;
        let page_size = 1024u64;
        let dev_size = PAGES * page_size;
        let inner = Arc::new(MemBackend::new(dev_size).unwrap());
        let b = BufferedBackend::new(
            inner.clone(),
            BufferedConfig {
                page_size,
                max_dirty_pages: 4,
                idle_flush_secs: 0,
                force_flush_timeout_secs: 0,
                flush_io_timeout_secs: 0,
                flush_concurrency: 16,
            },
        )
        .unwrap();

        let mut handles = Vec::new();
        for p in 0..PAGES {
            let b = b.clone();
            handles.push(tokio::spawn(async move {
                let byte = (p & 0xFF) as u8;
                b.write(p * page_size, Bytes::from(vec![byte; 512]))
                    .await
                    .unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        b.flush().await.unwrap();

        // Every page's write must be readable both through the buffer and from
        // the inner backend after flush.
        for p in 0..PAGES {
            let byte = (p & 0xFF) as u8;
            let via_buffer = b.read(p * page_size, 512).await.unwrap();
            assert!(
                via_buffer.iter().all(|&x| x == byte),
                "page {p} wrong via buffer"
            );
            let via_inner = inner.read(p * page_size, 512).await.unwrap();
            assert!(
                via_inner.iter().all(|&x| x == byte),
                "page {p} not persisted to inner"
            );
        }
    }
}
