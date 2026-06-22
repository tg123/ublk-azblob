//! Write-back buffered backend that doubles as an in-memory page cache.
//!
//! `BufferedBackend` wraps any `BlobBackend` and accumulates writes in memory,
//! flushing dirty "pages" (fixed-size regions) to the inner backend in batches.
//! It is also a read cache: a page fetched to satisfy a read (or left behind
//! after a flush) stays resident so later accesses are served from memory
//! instead of the inner backend. The resident set is bounded by an LRU budget
//! (`max_cached_pages`) that evicts the least-recently-used **clean** pages;
//! dirty pages are pinned until flushed.
//!
//! # Parameters
//! - `page_size`: Size of each buffer page (e.g. 4 MiB).  Must be a multiple
//!   of 512 bytes.
//! - `max_dirty_pages`: When the number of dirty pages exceeds this limit,
//!   the oldest dirty pages are auto-flushed before accepting new writes.
//! - `max_cached_pages`: Upper bound on resident pages (clean + dirty). When
//!   exceeded, the least-recently-used clean pages are evicted from memory.
//!   `0` means unlimited (grow-only, no clean-page eviction).

use super::cache_lru::Lru;
use super::io_gateway::{with_class, IoClass};
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
    /// Maximum number of *resident* pages (clean **and** dirty) kept in memory.
    ///
    /// The buffer doubles as an in-memory read cache: clean pages fetched to
    /// satisfy reads (or left behind after a flush) stay resident so later
    /// accesses are served from memory. When the resident page count exceeds
    /// this limit, the least-recently-used **clean** pages are evicted from
    /// memory (dirty pages are pinned — they are bounded separately by
    /// `max_dirty_pages`, which flushes them). `0` means unlimited (grow-only,
    /// no clean-page eviction). Should be `>= max_dirty_pages` to leave room for
    /// the pinned dirty working set.
    pub max_cached_pages: usize,
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
            max_cached_pages: 256,         // 1 GiB resident cap (clean + dirty)
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
    /// Modification sequence number. Bumped on every write/clear so a flush can
    /// detect whether the page was re-dirtied while its bytes were in flight
    /// (and must therefore stay dirty). Distinct from the LRU recency clock.
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
    /// LRU bookkeeping for clean-page eviction. Mirrors the `pages` map: every
    /// resident page has an entry, and only *clean* pages are evictable. Used to
    /// keep the resident set within `max_cached_pages`.
    lru: Lru,
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
        if config.max_cached_pages != 0 && config.max_cached_pages < config.max_dirty_pages {
            bail!(
                "max_cached_pages ({}) must be 0 (unlimited) or >= max_dirty_pages ({})",
                config.max_cached_pages,
                config.max_dirty_pages
            );
        }
        let backend = Arc::new(Self {
            inner,
            config: config.clone(),
            state: Mutex::new(BufferState {
                pages: BTreeMap::new(),
                lru: Lru::default(),
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
    /// A freshly loaded page is inserted clean and recorded in the LRU, and the
    /// resident set is trimmed back within `max_cached_pages` (the just-loaded
    /// page is protected from that trim).  Because clean pages may later be
    /// evicted to honour the cache budget, callers must **not** assume the page
    /// is still resident after re-acquiring the lock — they re-check and retry
    /// `ensure_resident` if it was evicted in the meantime.
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
            // The newly resident page is clean and therefore evictable; record it
            // and trim the resident set back within the cache budget, protecting
            // the page we just loaded.
            state.lru.touch(page_idx, true);
            self.enforce_cache_limit(&mut state, Some(page_idx));
        }
        Ok(())
    }

    /// Evict least-recently-used **clean** pages from memory until the resident
    /// page count is within `max_cached_pages` (a no-op when it is `0`,
    /// i.e. unlimited).
    ///
    /// Dirty pages are pinned and never evicted (their unflushed bytes would be
    /// lost); they are bounded separately by `max_dirty_pages`, which flushes
    /// them.  `protect` is never evicted, so a caller can keep a page it is
    /// actively serving resident.  Runs entirely under the state lock with no
    /// `await`, so it never drops a page another task is mid-fetch on.
    fn enforce_cache_limit(&self, state: &mut BufferState, protect: Option<u64>) {
        let max = self.config.max_cached_pages;
        if max == 0 {
            return;
        }
        while state.pages.len() > max {
            let Some(victim) = state.lru.pop_lru(protect) else {
                break; // nothing else is evictable (the rest are dirty/protected)
            };
            state.pages.remove(&victim);
        }
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

        let flush_task = with_class(IoClass::Flush, async {
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
                let cleaned = match state.pages.get_mut(&page_idx) {
                    Some(p) if p.seq == seq => {
                        p.dirty = false;
                        true
                    }
                    _ => false,
                };
                if cleaned {
                    // Now clean again: the page becomes an eviction candidate.
                    state.lru.touch(page_idx, true);
                }
                Ok::<(), anyhow::Error>(())
            }))
            .buffer_unordered(concurrency)
            .try_collect::<()>()
            .await
        });

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
        state.lru.clear();
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

            // The buffer doubles as an in-memory read cache: make the page
            // resident (fetching from the inner backend without the lock held)
            // and serve the chunk from memory, refreshing its LRU recency.  A
            // clean page may be evicted between `ensure_resident` and the lock,
            // so re-check residency and reload on the rare miss.
            loop {
                self.ensure_resident(page_idx, dev_size).await?;

                let mut state = self.state.lock().await;
                let Some(page) = state.pages.get(&page_idx) else {
                    // Evicted in the race window; reload and retry.
                    continue;
                };
                result[pos as usize..pos as usize + chunk_len].copy_from_slice(
                    &page.data[page_offset as usize..page_offset as usize + chunk_len],
                );
                let clean = !page.dirty;
                state.lru.touch(page_idx, clean);
                self.enforce_cache_limit(&mut state, Some(page_idx));
                break;
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

            // Load the page (inner I/O happens without the state lock held),
            // then apply the modification under a brief lock.  A clean page can
            // be evicted in the window between the two, so re-check residency and
            // reload on the rare miss instead of asserting it is still present.
            loop {
                self.ensure_resident(page_idx, dev_size).await?;

                let mut state = self.state.lock().await;
                if !state.pages.contains_key(&page_idx) {
                    // Evicted in the race window; reload and retry.
                    continue;
                }
                state.seq_counter += 1;
                let seq = state.seq_counter;
                let page = state
                    .pages
                    .get_mut(&page_idx)
                    .expect("page resident: contains_key checked above");
                page.data[page_offset..page_offset + chunk_len]
                    .copy_from_slice(&data[pos as usize..pos as usize + chunk_len]);
                page.dirty = true;
                page.seq = seq;
                // Dirty pages are pinned (not evictable) until flushed.
                state.lru.touch(page_idx, false);
                // Track last write time for idle flush
                state.last_write = Some(Instant::now());
                break;
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

            loop {
                self.ensure_resident(page_idx, dev_size).await?;

                let mut state = self.state.lock().await;
                if !state.pages.contains_key(&page_idx) {
                    // Evicted in the race window; reload and retry.
                    continue;
                }
                state.seq_counter += 1;
                let seq = state.seq_counter;
                let page = state
                    .pages
                    .get_mut(&page_idx)
                    .expect("page resident: contains_key checked above");
                page.data[page_offset..page_offset + chunk_len].fill(0);
                page.dirty = true;
                page.seq = seq;
                // Dirty pages are pinned (not evictable) until flushed.
                state.lru.touch(page_idx, false);
                // Track last write time for idle flush
                state.last_write = Some(Instant::now());
                break;
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
        state.lru.clear();
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
                max_cached_pages: 0,
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
                max_cached_pages: 0,
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
                max_cached_pages: 0,
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
                max_cached_pages: 0,
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
                max_cached_pages: 0,
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
                max_cached_pages: 0,
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

    // ---- In-memory cache (read population + LRU eviction) ----

    /// Inner backend that counts the number of `read` calls reaching it, so a
    /// test can assert that a cache hit was served from memory (no inner read).
    struct CountingBackend {
        inner: MemBackend,
        reads: std::sync::atomic::AtomicU64,
    }

    impl CountingBackend {
        fn new(size: u64) -> Self {
            Self {
                inner: MemBackend::new(size).unwrap(),
                reads: std::sync::atomic::AtomicU64::new(0),
            }
        }
        fn read_count(&self) -> u64 {
            self.reads.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl BlobBackend for CountingBackend {
        async fn create(&self, size: u64) -> anyhow::Result<()> {
            self.inner.create(size).await
        }
        async fn read(&self, offset: u64, len: u64) -> anyhow::Result<Bytes> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.read(offset, len).await
        }
        async fn write(&self, offset: u64, data: Bytes) -> anyhow::Result<()> {
            self.inner.write(offset, data).await
        }
        async fn clear(&self, offset: u64, len: u64) -> anyhow::Result<()> {
            self.inner.clear(offset, len).await
        }
        async fn flush(&self) -> anyhow::Result<()> {
            self.inner.flush().await
        }
        async fn delete(&self) -> anyhow::Result<()> {
            self.inner.delete().await
        }
        async fn size(&self) -> anyhow::Result<u64> {
            self.inner.size().await
        }
    }

    fn make_cached_backend(
        inner: Arc<dyn BlobBackend>,
        page_size: u64,
        max_dirty: usize,
        max_cached: usize,
    ) -> Arc<BufferedBackend> {
        BufferedBackend::new(
            inner,
            BufferedConfig {
                page_size,
                max_dirty_pages: max_dirty,
                max_cached_pages: max_cached,
                idle_flush_secs: 0,
                force_flush_timeout_secs: 0,
                flush_io_timeout_secs: 0,
                flush_concurrency: 16,
            },
        )
        .unwrap()
    }

    async fn resident_pages(b: &BufferedBackend) -> usize {
        b.state.lock().await.pages.len()
    }

    #[tokio::test]
    async fn read_populates_cache_and_serves_from_memory() {
        // Seed the inner backend with a known pattern.
        let page_size = 1024u64;
        let inner = Arc::new(CountingBackend::new(4 * page_size));
        inner
            .write(0, Bytes::from(vec![0x7E; page_size as usize]))
            .await
            .unwrap();
        let counter = inner.clone();
        let b = make_cached_backend(inner, page_size, 4, 16);

        // First read misses the cache and fetches the page from the inner backend.
        let r1 = b.read(0, 512).await.unwrap();
        assert!(r1.iter().all(|&x| x == 0x7E));
        assert_eq!(resident_pages(&b).await, 1, "page should now be cached");
        let after_first = counter.read_count();
        assert!(after_first >= 1, "first read must reach inner backend");

        // Subsequent reads of the same page are served from memory: no new inner
        // reads.
        let r2 = b.read(0, 512).await.unwrap();
        assert!(r2.iter().all(|&x| x == 0x7E));
        assert_eq!(
            counter.read_count(),
            after_first,
            "cache hit must not reach inner backend"
        );
    }

    #[tokio::test]
    async fn clean_pages_evicted_over_cache_limit() {
        const PAGES: u64 = 8;
        let page_size = 1024u64;
        let inner = Arc::new(MemBackend::new(PAGES * page_size).unwrap());
        // Cache at most 3 pages; all pages are clean reads, so the resident set
        // must never exceed the limit.
        let b = make_cached_backend(inner, page_size, 3, 3);

        for p in 0..PAGES {
            let _ = b.read(p * page_size, 512).await.unwrap();
            assert!(
                resident_pages(&b).await <= 3,
                "resident set exceeded the cache limit at page {p}"
            );
        }
        assert_eq!(resident_pages(&b).await, 3, "cache should be full");
    }

    #[tokio::test]
    async fn dirty_pages_are_pinned_against_eviction() {
        const PAGES: u64 = 8;
        let page_size = 1024u64;
        let inner = Arc::new(MemBackend::new(PAGES * page_size).unwrap());
        // Keep up to 4 dirty pages (no auto-flush) and cap the resident set at 4
        // total, so admitting clean read pages must evict *clean* pages only —
        // never the pinned dirty ones.
        let b = make_cached_backend(inner.clone(), page_size, 4, 4);

        // Dirty pages 0..4 (kept dirty: max_dirty_pages == 4).
        for p in 0..4u64 {
            let byte = 0xA0 + p as u8;
            b.write(p * page_size, Bytes::from(vec![byte; page_size as usize]))
                .await
                .unwrap();
        }
        assert_eq!(resident_pages(&b).await, 4);

        // Now read clean pages 4..8; each admission is over the cap and must
        // evict a clean page — but there are no clean pages, so the dirty ones
        // survive and the resident set may transiently exceed the cap.
        for p in 4..PAGES {
            let _ = b.read(p * page_size, 512).await.unwrap();
        }

        // The four dirty pages must still be resident with their unflushed data.
        for p in 0..4u64 {
            let byte = 0xA0 + p as u8;
            let got = b.read(p * page_size, page_size).await.unwrap();
            assert!(
                got.iter().all(|&x| x == byte),
                "dirty page {p} lost its unflushed data"
            );
        }
        // And none of it reached the inner backend (never flushed).
        for p in 0..4u64 {
            let got = inner.read(p * page_size, page_size).await.unwrap();
            assert!(
                got.iter().all(|&x| x == 0),
                "dirty page {p} must not have been flushed/evicted"
            );
        }

        // After flushing, the now-clean pages become evictable and the resident
        // set settles within the cap on the next admission.
        b.flush().await.unwrap();
        let _ = b.read(0, 512).await.unwrap();
        assert!(
            resident_pages(&b).await <= 4,
            "resident set must settle within the cap once pages are clean"
        );
    }

    #[tokio::test]
    async fn eviction_preserves_read_correctness() {
        // With a tiny cache, a full sweep that constantly evicts must still
        // return correct bytes for every page.
        const PAGES: u64 = 16;
        let page_size = 1024u64;
        let inner = Arc::new(MemBackend::new(PAGES * page_size).unwrap());
        for p in 0..PAGES {
            let byte = (p as u8).wrapping_mul(17);
            inner
                .write(p * page_size, Bytes::from(vec![byte; page_size as usize]))
                .await
                .unwrap();
        }
        let b = make_cached_backend(inner, page_size, 2, 2);

        // Two passes so the second pass hits a mix of cached and evicted pages.
        for _ in 0..2 {
            for p in 0..PAGES {
                let byte = (p as u8).wrapping_mul(17);
                let got = b.read(p * page_size, page_size).await.unwrap();
                assert!(got.iter().all(|&x| x == byte), "wrong bytes for page {p}");
                assert!(resident_pages(&b).await <= 2);
            }
        }
    }

    #[tokio::test]
    async fn invalid_max_cached_pages_rejected() {
        let inner = Arc::new(MemBackend::new(4096).unwrap());
        // max_cached_pages must be 0 (unlimited) or >= max_dirty_pages.
        let res = BufferedBackend::new(
            inner,
            BufferedConfig {
                page_size: 1024,
                max_dirty_pages: 8,
                max_cached_pages: 4,
                idle_flush_secs: 0,
                force_flush_timeout_secs: 0,
                flush_io_timeout_secs: 0,
                flush_concurrency: 16,
            },
        );
        assert!(
            res.is_err(),
            "max_cached_pages < max_dirty_pages must error"
        );
    }
}
