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
    pub fn new(inner: Arc<dyn BlobBackend>, config: BufferedConfig) -> Self {
        assert!(config.page_size >= 512 && config.page_size.is_multiple_of(512));
        Self {
            inner,
            config,
            state: Mutex::new(BufferState {
                pages: BTreeMap::new(),
                seq_counter: 0,
                dev_size: 0,
            }),
        }
    }

    /// Flush the N oldest dirty pages to make room.
    async fn evict_oldest(&self, state: &mut BufferState, count: usize) -> anyhow::Result<()> {
        // Collect dirty page indices sorted by sequence number (oldest first).
        let mut dirty: Vec<(u64, u64)> = state
            .pages
            .iter()
            .filter(|(_, p)| p.dirty)
            .map(|(&idx, p)| (p.seq, idx))
            .collect();
        dirty.sort_unstable();

        let to_flush: Vec<u64> = dirty.iter().take(count).map(|&(_, idx)| idx).collect();

        for &page_idx in &to_flush {
            self.flush_page(state, page_idx).await?;
        }
        Ok(())
    }

    /// Flush a single page to the inner backend.
    async fn flush_page(&self, state: &mut BufferState, page_idx: u64) -> anyhow::Result<()> {
        let page = match state.pages.get_mut(&page_idx) {
            Some(p) if p.dirty => p,
            _ => return Ok(()),
        };

        let offset = page_idx * self.config.page_size;
        let data = Bytes::copy_from_slice(&page.data[..]);

        // Determine actual write length (don't write past dev_size).
        let write_len = if state.dev_size > 0 {
            let remaining = state.dev_size.saturating_sub(offset);
            (data.len() as u64).min(remaining)
        } else {
            data.len() as u64
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

        // Mark clean after successful write.
        let page = state.pages.get_mut(&page_idx).unwrap();
        page.dirty = false;
        Ok(())
    }

    /// Ensure a page exists in the buffer, loading from inner if needed.
    async fn ensure_page(&self, state: &mut BufferState, page_idx: u64) -> anyhow::Result<()> {
        if state.pages.contains_key(&page_idx) {
            return Ok(());
        }

        let offset = page_idx * self.config.page_size;
        let read_len = if state.dev_size > 0 {
            let remaining = state.dev_size.saturating_sub(offset);
            remaining.min(self.config.page_size)
        } else {
            self.config.page_size
        };

        let data = if read_len > 0 && state.dev_size > 0 {
            // Read existing content from inner backend.
            self.inner
                .read(offset, read_len)
                .await
                .with_context(|| format!("load page {page_idx} from backend"))?
        } else {
            Bytes::from(vec![0u8; self.config.page_size as usize])
        };

        let mut buf = BytesMut::zeroed(self.config.page_size as usize);
        let copy_len = data.len().min(buf.len());
        buf[..copy_len].copy_from_slice(&data[..copy_len]);

        state.seq_counter += 1;
        state.pages.insert(
            page_idx,
            Page {
                data: buf,
                dirty: false,
                seq: state.seq_counter,
            },
        );
        Ok(())
    }

    /// Count dirty pages.
    fn dirty_count(state: &BufferState) -> usize {
        state.pages.values().filter(|p| p.dirty).count()
    }
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

        let mut state = self.state.lock().await;
        if state.dev_size == 0 {
            state.dev_size = self.inner.size().await?;
        }

        let page_size = self.config.page_size;
        let mut result = BytesMut::zeroed(len as usize);
        let mut pos: u64 = 0;

        while pos < len {
            let abs_offset = offset + pos;
            let page_idx = abs_offset / page_size;
            let page_offset = abs_offset % page_size;
            let chunk_len = (page_size - page_offset).min(len - pos) as usize;

            if state.pages.contains_key(&page_idx) {
                // Serve from buffer.
                let page = state.pages.get(&page_idx).unwrap();
                result[pos as usize..pos as usize + chunk_len].copy_from_slice(
                    &page.data[page_offset as usize..page_offset as usize + chunk_len],
                );
            } else {
                // Read directly from inner (don't pollute cache for pure reads).
                let data = self
                    .inner
                    .read(abs_offset, chunk_len as u64)
                    .await
                    .with_context(|| format!("read offset={abs_offset} len={chunk_len}"))?;
                let copy_len = data.len().min(chunk_len);
                result[pos as usize..pos as usize + copy_len].copy_from_slice(&data[..copy_len]);
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

        let mut state = self.state.lock().await;
        if state.dev_size == 0 {
            state.dev_size = self.inner.size().await?;
        }

        let page_size = self.config.page_size;
        let mut pos: u64 = 0;
        let len = data.len() as u64;

        while pos < len {
            let abs_offset = offset + pos;
            let page_idx = abs_offset / page_size;
            let page_offset = (abs_offset % page_size) as usize;
            let chunk_len = (page_size - page_offset as u64).min(len - pos) as usize;

            // Auto-flush if we're about to exceed the dirty page limit and this
            // page isn't already in the buffer.
            if !state.pages.contains_key(&page_idx) {
                let dirty = Self::dirty_count(&state);
                if dirty >= self.config.max_dirty_pages {
                    let to_evict = dirty - self.config.max_dirty_pages + 1;
                    info!(dirty, evicting = to_evict, "auto-flushing dirty pages");
                    self.evict_oldest(&mut state, to_evict).await?;
                }
            }

            self.ensure_page(&mut state, page_idx).await?;

            state.seq_counter += 1;
            let seq = state.seq_counter;
            let page = state.pages.get_mut(&page_idx).unwrap();
            page.data[page_offset..page_offset + chunk_len]
                .copy_from_slice(&data[pos as usize..pos as usize + chunk_len]);
            page.dirty = true;
            page.seq = seq;

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

        let mut state = self.state.lock().await;
        if state.dev_size == 0 {
            state.dev_size = self.inner.size().await?;
        }

        let page_size = self.config.page_size;
        let mut pos: u64 = 0;

        while pos < len {
            let abs_offset = offset + pos;
            let page_idx = abs_offset / page_size;
            let page_offset = (abs_offset % page_size) as usize;
            let chunk_len = (page_size - page_offset as u64).min(len - pos) as usize;

            if !state.pages.contains_key(&page_idx) {
                let dirty = Self::dirty_count(&state);
                if dirty >= self.config.max_dirty_pages {
                    let to_evict = dirty - self.config.max_dirty_pages + 1;
                    self.evict_oldest(&mut state, to_evict).await?;
                }
            }

            self.ensure_page(&mut state, page_idx).await?;

            state.seq_counter += 1;
            let seq = state.seq_counter;
            let page = state.pages.get_mut(&page_idx).unwrap();
            page.data[page_offset..page_offset + chunk_len].fill(0);
            page.dirty = true;
            page.seq = seq;

            pos += chunk_len as u64;
        }

        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;
        let dirty_indices: Vec<u64> = state
            .pages
            .iter()
            .filter(|(_, p)| p.dirty)
            .map(|(&idx, _)| idx)
            .collect();

        if dirty_indices.is_empty() {
            return Ok(());
        }

        info!(
            dirty_pages = dirty_indices.len(),
            "flushing all dirty pages"
        );
        for page_idx in dirty_indices {
            self.flush_page(&mut state, page_idx).await?;
        }

        // Also call inner flush in case it has its own buffering.
        // Release lock first to avoid holding it across the inner flush.
        drop(state);
        self.inner.flush().await
    }

    async fn size(&self) -> anyhow::Result<u64> {
        let mut state = self.state.lock().await;
        if state.dev_size == 0 {
            state.dev_size = self.inner.size().await?;
        }
        Ok(state.dev_size)
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
        );
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
        );

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
}
