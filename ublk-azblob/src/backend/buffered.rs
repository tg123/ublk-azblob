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
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, trace};

/// Configuration for the write-back buffer.
#[derive(Debug, Clone)]
pub struct BufferedConfig {
    /// Size of each buffer page in bytes (must be a multiple of 512).
    pub page_size: u64,
    /// Maximum number of dirty pages held in memory before auto-flush.
    pub max_dirty_pages: usize,
}

impl Default for BufferedConfig {
    fn default() -> Self {
        Self {
            page_size: 4 * 1024 * 1024, // 4 MiB
            max_dirty_pages: 64,        // 256 MiB total
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
}

impl BufferedBackend {
    pub fn new(inner: Arc<dyn BlobBackend>, config: BufferedConfig) -> anyhow::Result<Self> {
        if config.page_size < 512 || !config.page_size.is_multiple_of(512) {
            bail!(
                "page_size ({}) must be a non-zero multiple of 512",
                config.page_size
            );
        }
        if config.max_dirty_pages == 0 {
            bail!("max_dirty_pages must be greater than 0");
        }
        Ok(Self {
            inner,
            config,
            state: Mutex::new(BufferState {
                pages: BTreeMap::new(),
                seq_counter: 0,
                dev_size: 0,
            }),
        })
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
    /// Each page's bytes are snapshotted under a brief lock, written to the
    /// inner backend with the lock released, then marked clean under the lock —
    /// but only if the page was not modified again during the write (detected
    /// via its sequence number).  This guarantees no dirty data is lost.
    async fn flush_indices(&self, indices: &[u64]) -> anyhow::Result<()> {
        let page_size = self.config.page_size;
        for &page_idx in indices {
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
                continue;
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
        }
        Ok(())
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
        }

        // Also flush the inner backend in case it has its own buffering.
        self.inner.flush().await
    }

    async fn size(&self) -> anyhow::Result<u64> {
        self.dev_size().await
    }

    async fn snapshot(&self) -> anyhow::Result<String> {
        // Flush buffered writes so the snapshot reflects the latest data.
        self.flush().await?;
        self.inner.snapshot().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::mem::MemBackend;

    fn make_backend(dev_size: u64, page_size: u64, max_dirty: usize) -> BufferedBackend {
        let inner = Arc::new(MemBackend::new(dev_size).unwrap());
        BufferedBackend::new(
            inner,
            BufferedConfig {
                page_size,
                max_dirty_pages: max_dirty,
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
        let b = Arc::new(
            BufferedBackend::new(
                inner.clone(),
                BufferedConfig {
                    page_size,
                    max_dirty_pages: 4,
                },
            )
            .unwrap(),
        );

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
