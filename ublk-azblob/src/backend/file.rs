//! Persistent local-disk cache backend.
//!
//! `FileCacheBackend` wraps any [`BlobBackend`] and uses a local disk file as a
//! durable page cache.  It is designed to be one level of a *multi-level* cache:
//!
//! ```text
//! BufferedBackend (memory) ──► FileCacheBackend (local disk) ──► AzurePageBlobBackend (blob)
//! ```
//!
//! # On-disk layout
//!
//! Two files live in the cache directory:
//!
//! - `<name>.dat` — a sparse file the size of the device.  Page `i` is stored at
//!   byte offset `i * page_size`.
//! - `<name>.meta` — a small header followed by two bitmaps (`present` and
//!   `dirty`), one bit per page.
//!
//! # Crash recovery
//!
//! The `dirty` bitmap is persisted (with `fsync`) whenever a page is dirtied or
//! cleaned, and the page data is `fsync`ed before the dirty bit is set.  This
//! means that after a crash or restart the cache can be re-opened, the dirty
//! metadata recovered, and any pages that had not yet reached the blob can still
//! be flushed — see [`FileCacheBackend::open`] and [`BlobBackend::flush`].
//!
//! `present` (clean) pages are an optimization only; losing a `present` bit just
//! forces a re-read from the inner backend, so those updates are not `fsync`ed.

use super::cache_budget::CacheBudget;
use super::BlobBackend;
use anyhow::{bail, Context as _};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd as _;
use std::path::PathBuf;
use tokio::sync::Mutex;
use tracing::{info, trace, warn};

/// On-disk metadata magic ("UBLK file cache").
const MAGIC: &[u8; 8] = b"UBLKFCAC";
/// On-disk metadata format version.
const VERSION: u32 = 1;
/// Fixed size of the metadata header in bytes.  The two bitmaps follow it.
const HEADER_SIZE: u64 = 64;

/// Configuration for the local-disk cache.
#[derive(Debug, Clone)]
pub struct FileCacheConfig {
    /// Directory that holds the cache data and metadata files.
    pub dir: PathBuf,
    /// Base file name (without extension) for the cache files.
    pub name: String,
    /// Size of each cache page in bytes (must be a non-zero multiple of 512).
    pub page_size: u64,
    /// Maximum total bytes of cached page data on local disk, **shared across
    /// all processes** using the same `dir` (see [`CacheBudget`]).  `0` means
    /// unlimited (no eviction), preserving the original grow-only behaviour.
    pub max_bytes: u64,
}

impl Default for FileCacheConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("."),
            name: "ublk-azblob-cache".to_string(),
            page_size: 1024 * 1024, // 1 MiB
            max_bytes: 0,           // unlimited
        }
    }
}

/// In-memory LRU bookkeeping for resident (present) cache pages.
///
/// Tracks the recency of every present page and which of them are *evictable*
/// (present **and** clean — dirty pages must be flushed, never dropped).  Only
/// maintained when a [`CacheBudget`] is active.
#[derive(Default)]
struct Lru {
    /// Monotonic access clock; higher = more recently used.
    seq: u64,
    /// Recency stamp of every present page.
    by_page: HashMap<u64, u64>,
    /// Evictable (clean, present) pages, ordered by recency stamp so the
    /// smallest key is the least-recently-used eviction candidate.
    evictable: BTreeMap<u64, u64>,
}

impl Lru {
    /// Whether `page` is currently accounted as present.
    fn contains(&self, page: u64) -> bool {
        self.by_page.contains_key(&page)
    }

    /// Record an access to `page`, (re)classifying it as evictable iff `clean`.
    fn touch(&mut self, page: u64, clean: bool) {
        if let Some(old) = self.by_page.get(&page) {
            self.evictable.remove(old);
        }
        self.seq += 1;
        let s = self.seq;
        self.by_page.insert(page, s);
        if clean {
            self.evictable.insert(s, page);
        }
    }

    /// Pop the least-recently-used evictable page, skipping `protect`.
    fn pop_lru(&mut self, protect: Option<u64>) -> Option<u64> {
        let key = self
            .evictable
            .iter()
            .find(|(_, &page)| Some(page) != protect)
            .map(|(&seq, _)| seq)?;
        let page = self.evictable.remove(&key)?;
        self.by_page.remove(&page);
        Some(page)
    }

    /// Reset to empty (used on create/delete).
    fn clear(&mut self) {
        self.seq = 0;
        self.by_page.clear();
        self.evictable.clear();
    }
}

/// Persistent local-disk page cache wrapping any [`BlobBackend`].
pub struct FileCacheBackend {
    inner: std::sync::Arc<dyn BlobBackend>,
    page_size: u64,
    /// Shared cross-process byte budget; `None` when unlimited.
    budget: Option<CacheBudget>,
    state: Mutex<CacheState>,
}

struct CacheState {
    /// Open handle to the page data file.
    data: File,
    /// Open handle to the metadata file.
    meta: File,
    /// Total device size in bytes.
    dev_size: u64,
    /// Number of cache pages (`ceil(dev_size / page_size)`).
    num_pages: u64,
    /// Bitmap: page is present (cached) in the data file.
    present: Vec<u8>,
    /// Bitmap: page is dirty (cached but not yet flushed to the inner backend).
    dirty: Vec<u8>,
    /// Bytes of resident (present) page data this backend holds on disk.  Only
    /// meaningful when a budget is active.
    resident_bytes: u64,
    /// LRU bookkeeping for eviction.  Only populated when a budget is active.
    lru: Lru,
}

#[inline]
fn bitmap_bytes(num_pages: u64) -> usize {
    num_pages.div_ceil(8) as usize
}

#[inline]
fn bit_get(bitmap: &[u8], idx: u64) -> bool {
    let byte = (idx / 8) as usize;
    let bit = (idx % 8) as u8;
    byte < bitmap.len() && (bitmap[byte] & (1 << bit)) != 0
}

#[inline]
fn bit_set(bitmap: &mut [u8], idx: u64, value: bool) {
    let byte = (idx / 8) as usize;
    let bit = (idx % 8) as u8;
    if value {
        bitmap[byte] |= 1 << bit;
    } else {
        bitmap[byte] &= !(1 << bit);
    }
}

impl FileCacheBackend {
    /// Open (or create) a cache in `cfg.dir`, recovering any persisted state.
    ///
    /// `dev_size` is the size of the backing device (typically obtained from the
    /// inner backend via [`BlobBackend::size`]).  If an existing cache is found
    /// with a different `page_size` or `dev_size`, it is treated as incompatible
    /// and re-initialized.
    ///
    /// Returns the cache plus the number of *dirty* pages recovered from disk so
    /// the caller can decide whether to flush them to the inner backend on start.
    pub fn open(
        inner: std::sync::Arc<dyn BlobBackend>,
        cfg: FileCacheConfig,
        dev_size: u64,
    ) -> anyhow::Result<(Self, u64)> {
        if cfg.page_size == 0 || !cfg.page_size.is_multiple_of(512) {
            bail!(
                "page_size ({}) must be a non-zero multiple of 512",
                cfg.page_size
            );
        }
        if dev_size == 0 {
            bail!("dev_size must be greater than 0");
        }

        std::fs::create_dir_all(&cfg.dir)
            .with_context(|| format!("create cache dir {}", cfg.dir.display()))?;

        let data_path = cfg.dir.join(format!("{}.dat", cfg.name));
        let meta_path = cfg.dir.join(format!("{}.meta", cfg.name));

        let num_pages = dev_size.div_ceil(cfg.page_size);

        let data = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&data_path)
            .with_context(|| format!("open cache data file {}", data_path.display()))?;
        data.set_len(dev_size)
            .with_context(|| format!("size cache data file to {dev_size}"))?;

        let meta = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&meta_path)
            .with_context(|| format!("open cache meta file {}", meta_path.display()))?;

        let (present, dirty, recovered_dirty) =
            load_or_init_meta(&meta, cfg.page_size, dev_size, num_pages)?;

        if recovered_dirty > 0 {
            info!(
                dir = %cfg.dir.display(),
                recovered_dirty,
                "recovered dirty pages from local disk cache"
            );
        }

        // Optional shared cross-process byte budget.  When active, seed the LRU
        // and resident-byte accounting from the pages recovered off disk.
        let budget = CacheBudget::open(&cfg.dir, &cfg.name, cfg.max_bytes)
            .context("open shared cache budget")?;

        let mut lru = Lru::default();
        let mut resident_bytes: u64 = 0;
        if budget.is_some() {
            for page_idx in 0..num_pages {
                if bit_get(&present, page_idx) {
                    let offset = page_idx * cfg.page_size;
                    let page_len = cfg.page_size.min(dev_size.saturating_sub(offset));
                    resident_bytes = resident_bytes.saturating_add(page_len);
                    // Dirty pages are present but not evictable until flushed.
                    lru.touch(page_idx, !bit_get(&dirty, page_idx));
                }
            }
            if let Some(b) = &budget {
                let over = b.reset(resident_bytes).context("seed cache budget")?;
                if over > 0 {
                    info!(
                        over_bytes = over,
                        max_bytes = cfg.max_bytes,
                        "local disk cache over shared budget after recovery; \
                         clean pages will be evicted on demand"
                    );
                }
            }
        }

        let state = CacheState {
            data,
            meta,
            dev_size,
            num_pages,
            present,
            dirty,
            resident_bytes,
            lru,
        };

        Ok((
            Self {
                inner,
                page_size: cfg.page_size,
                budget,
                state: Mutex::new(state),
            },
            recovered_dirty,
        ))
    }

    /// Re-initialize the cache for a brand new device of `size` bytes, discarding
    /// every cached and dirty page.
    fn reinit(&self, state: &mut CacheState, size: u64) -> anyhow::Result<()> {
        let num_pages = size.div_ceil(self.page_size);
        state.data.set_len(size).context("resize cache data file")?;
        state.dev_size = size;
        state.num_pages = num_pages;
        state.present = vec![0u8; bitmap_bytes(num_pages)];
        state.dirty = vec![0u8; bitmap_bytes(num_pages)];
        write_full_meta(
            &state.meta,
            self.page_size,
            size,
            num_pages,
            &state.present,
            &state.dirty,
        )?;
        // All pages were dropped, so reset eviction accounting too.
        state.resident_bytes = 0;
        state.lru.clear();
        if let Some(b) = &self.budget {
            b.reset(0).context("reset cache budget on reinit")?;
        }
        Ok(())
    }

    /// Valid byte length of `page_idx` (the final page may be short).
    #[inline]
    fn page_len(&self, state: &CacheState, page_idx: u64) -> u64 {
        let offset = page_idx * self.page_size;
        self.page_size.min(state.dev_size.saturating_sub(offset))
    }

    /// Account `page_idx` as a present page (updating LRU recency and, for newly
    /// resident pages, the shared budget) then evict our own clean pages if the
    /// admission pushed the shared total over its limit.  `clean` says whether
    /// the page is currently evictable (present and not dirty).
    ///
    /// A no-op when no budget is configured.
    fn account_present(
        &self,
        state: &mut CacheState,
        page_idx: u64,
        clean: bool,
    ) -> anyhow::Result<()> {
        let Some(budget) = &self.budget else {
            return Ok(());
        };

        let already = state.lru.contains(page_idx);
        state.lru.touch(page_idx, clean);
        if already {
            // Recency/evictability updated; no new disk bytes admitted.
            return Ok(());
        }

        let len = self.page_len(state, page_idx);
        state.resident_bytes = state.resident_bytes.saturating_add(len);
        let over = budget.admit(len).context("admit cache budget")?;
        if over > 0 {
            // Protect the page we just admitted from being evicted immediately.
            self.evict_clean(state, over, Some(page_idx))?;
        }
        Ok(())
    }

    /// Drop up to `over` bytes of our least-recently-used *clean* pages, punching
    /// holes to actually reclaim disk space and clearing their `present` bits.
    /// Dirty pages are never evicted (they would lose unflushed data).
    fn evict_clean(
        &self,
        state: &mut CacheState,
        mut over: u64,
        protect: Option<u64>,
    ) -> anyhow::Result<()> {
        let Some(budget) = &self.budget else {
            return Ok(());
        };

        let mut evicted = 0u64;
        while over > 0 {
            let Some(page_idx) = state.lru.pop_lru(protect) else {
                break; // nothing else we own is evictable
            };
            let offset = page_idx * self.page_size;
            let len = self.page_len(state, page_idx);

            if len > 0 {
                punch_hole(&state.data, offset, len)
                    .with_context(|| format!("punch hole for evicted page {page_idx}"))?;
            }
            // Clear the present bit and persist it.  A clean bit needs no fsync:
            // if the cleared bit is lost on crash the worst case is a redundant
            // re-read from the inner backend.
            bit_set(&mut state.present, page_idx, false);
            let byte = page_idx / 8;
            let present_off = HEADER_SIZE + byte;
            if let Err(err) = state
                .meta
                .write_all_at(&[state.present[byte as usize]], present_off)
            {
                warn!(page_idx, %err, "failed to persist evicted present bit (non-fatal)");
            }

            state.resident_bytes = state.resident_bytes.saturating_sub(len);
            budget.release(len).context("release cache budget")?;
            over = over.saturating_sub(len);
            evicted += 1;
        }

        if evicted > 0 {
            trace!(
                evicted_pages = evicted,
                resident_bytes = state.resident_bytes,
                "evicted clean cache pages to honour shared budget"
            );
        }
        Ok(())
    }

    /// Persist a single page's `dirty` (and `present`) bit to the metadata file
    /// and `fsync` it so the change survives a crash.
    fn persist_meta_bit(state: &CacheState, page_idx: u64) -> anyhow::Result<()> {
        let byte_in_bitmap = page_idx / 8;
        let bitmap_len = bitmap_bytes(state.num_pages) as u64;

        // present bitmap byte
        let present_off = HEADER_SIZE + byte_in_bitmap;
        state
            .meta
            .write_all_at(&[state.present[byte_in_bitmap as usize]], present_off)
            .context("persist present bit")?;
        // dirty bitmap byte
        let dirty_off = HEADER_SIZE + bitmap_len + byte_in_bitmap;
        state
            .meta
            .write_all_at(&[state.dirty[byte_in_bitmap as usize]], dirty_off)
            .context("persist dirty bit")?;
        state.meta.sync_data().context("fsync cache metadata")?;
        Ok(())
    }

    /// Ensure page `page_idx` is fully present in the data file, loading it from
    /// the inner backend if necessary.  Returns the page's valid length (the last
    /// page may be shorter than `page_size`).
    async fn ensure_page(&self, state: &mut CacheState, page_idx: u64) -> anyhow::Result<u64> {
        let offset = page_idx * self.page_size;
        let page_len = self.page_size.min(state.dev_size.saturating_sub(offset));

        if bit_get(&state.present, page_idx) {
            return Ok(page_len);
        }

        // Load existing content for this page from the inner backend.
        let data = self
            .inner
            .read(offset, page_len)
            .await
            .with_context(|| format!("load page {page_idx} from inner backend"))?;
        if data.len() as u64 != page_len {
            bail!(
                "inner backend returned {} bytes for page {page_idx} (expected {page_len})",
                data.len()
            );
        }
        state
            .data
            .write_all_at(&data, offset)
            .with_context(|| format!("write page {page_idx} to cache file"))?;

        bit_set(&mut state.present, page_idx, true);
        // A freshly loaded clean page does not need an fsync: if the present bit
        // is lost on crash it is simply re-read from the inner backend.
        let byte = page_idx / 8;
        let present_off = HEADER_SIZE + byte;
        if let Err(err) = state
            .meta
            .write_all_at(&[state.present[byte as usize]], present_off)
        {
            warn!(page_idx, %err, "failed to persist clean present bit (non-fatal)");
        }

        // Account the newly-resident clean page and evict if over budget.  The
        // page just loaded is protected from immediate eviction.
        self.account_present(state, page_idx, true)?;

        Ok(page_len)
    }

    /// Flush a single dirty page to the inner backend and clear its dirty bit.
    async fn flush_page(&self, state: &mut CacheState, page_idx: u64) -> anyhow::Result<()> {
        if !bit_get(&state.dirty, page_idx) {
            return Ok(());
        }
        let offset = page_idx * self.page_size;
        let page_len = self.page_size.min(state.dev_size.saturating_sub(offset));
        if page_len == 0 {
            bit_set(&mut state.dirty, page_idx, false);
            return Ok(());
        }

        let mut buf = vec![0u8; page_len as usize];
        state
            .data
            .read_exact_at(&mut buf, offset)
            .with_context(|| format!("read page {page_idx} from cache file"))?;

        trace!(
            page_idx,
            offset,
            len = page_len,
            "flushing cached page to inner backend"
        );
        self.inner
            .write(offset, Bytes::from(buf))
            .await
            .with_context(|| format!("flush page {page_idx} to inner backend"))?;

        bit_set(&mut state.dirty, page_idx, false);
        Self::persist_meta_bit(state, page_idx)?;
        // Now clean again: the page becomes an eviction candidate.
        if self.budget.is_some() {
            state.lru.touch(page_idx, true);
        }
        Ok(())
    }

    /// Mark a region of a page dirty after writing `payload` into the data file.
    ///
    /// The caller must have ensured the page is present.
    fn dirty_page_region(
        &self,
        state: &mut CacheState,
        page_idx: u64,
        page_offset: u64,
        payload: &[u8],
    ) -> anyhow::Result<()> {
        let offset = page_idx * self.page_size + page_offset;
        state
            .data
            .write_all_at(payload, offset)
            .with_context(|| format!("write page {page_idx} to cache file"))?;
        // Durably persist the page data before advertising it as dirty so a crash
        // can never leave a dirty bit pointing at stale/garbage page contents.
        state.data.sync_data().context("fsync cache data")?;

        bit_set(&mut state.present, page_idx, true);
        bit_set(&mut state.dirty, page_idx, true);
        Self::persist_meta_bit(state, page_idx)?;
        // Account the page as resident but *not* evictable (it is dirty).  For a
        // page that was already present this just reclassifies it.
        self.account_present(state, page_idx, false)?;
        Ok(())
    }

    /// Count of pages currently marked dirty (used by tests).
    #[cfg(test)]
    async fn dirty_count(&self) -> u64 {
        let state = self.state.lock().await;
        (0..state.num_pages)
            .filter(|&i| bit_get(&state.dirty, i))
            .count() as u64
    }

    /// Count of pages currently marked present (used by tests).
    #[cfg(test)]
    async fn present_count(&self) -> u64 {
        let state = self.state.lock().await;
        (0..state.num_pages)
            .filter(|&i| bit_get(&state.present, i))
            .count() as u64
    }

    /// This backend's resident byte accounting (used by tests).
    #[cfg(test)]
    async fn resident_bytes(&self) -> u64 {
        self.state.lock().await.resident_bytes
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

/// Punch a hole in `file` over `[offset, offset+len)`, deallocating those blocks
/// so the disk space is actually reclaimed while the file keeps its logical size
/// (the region reads back as zeros).  Used when evicting a clean cache page.
fn punch_hole(file: &File, offset: u64, len: u64) -> anyhow::Result<()> {
    let ret = unsafe {
        libc::fallocate(
            file.as_raw_fd(),
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            offset as libc::off_t,
            len as libc::off_t,
        )
    };
    if ret != 0 {
        return Err(std::io::Error::last_os_error()).context("fallocate punch hole");
    }
    Ok(())
}

#[async_trait]
impl BlobBackend for FileCacheBackend {
    async fn create(&self, size: u64) -> anyhow::Result<()> {
        if size == 0 || !size.is_multiple_of(512) {
            bail!("size must be a non-zero multiple of 512 bytes, got {size}");
        }
        self.inner.create(size).await?;
        let mut state = self.state.lock().await;
        self.reinit(&mut state, size)?;
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
        check_in_bounds("read", offset, len, state.dev_size)?;

        let page_size = self.page_size;
        let track_lru = self.budget.is_some();
        let mut result = BytesMut::zeroed(len as usize);
        let mut pos: u64 = 0;

        while pos < len {
            let abs_offset = offset + pos;
            let page_idx = abs_offset / page_size;
            let page_offset = abs_offset % page_size;
            let chunk_len = (page_size - page_offset).min(len - pos);

            if bit_get(&state.present, page_idx) {
                // Serve from the local cache file.
                state
                    .data
                    .read_exact_at(
                        &mut result[pos as usize..(pos + chunk_len) as usize],
                        abs_offset,
                    )
                    .with_context(|| {
                        format!("read cache file offset={abs_offset} len={chunk_len}")
                    })?;
                // A cache hit refreshes the page's LRU recency (keeping its
                // current clean/dirty evictability classification).
                if track_lru {
                    let clean = !bit_get(&state.dirty, page_idx);
                    state.lru.touch(page_idx, clean);
                }
            } else {
                // Cache miss: read straight from the inner backend.  Pure reads do
                // not populate the cache to avoid evicting/overwriting dirty data
                // and to keep read latency predictable.
                let data = self
                    .inner
                    .read(abs_offset, chunk_len)
                    .await
                    .with_context(|| format!("read offset={abs_offset} len={chunk_len}"))?;
                if data.len() as u64 != chunk_len {
                    bail!(
                        "inner backend returned {} bytes for read offset={abs_offset} len={chunk_len}",
                        data.len()
                    );
                }
                result[pos as usize..(pos + chunk_len) as usize].copy_from_slice(&data);
            }

            pos += chunk_len;
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
        let len = data.len() as u64;
        check_in_bounds("write", offset, len, state.dev_size)?;

        let page_size = self.page_size;
        let mut pos: u64 = 0;
        while pos < len {
            let abs_offset = offset + pos;
            let page_idx = abs_offset / page_size;
            let page_offset = abs_offset % page_size;
            let chunk_len = (page_size - page_offset).min(len - pos);

            // For partial-page writes we must have the rest of the page populated
            // so a later full-page flush does not clobber neighbouring data.
            if chunk_len != page_size {
                self.ensure_page(&mut state, page_idx).await?;
            }

            let chunk = data.slice(pos as usize..(pos + chunk_len) as usize);
            self.dirty_page_region(&mut state, page_idx, page_offset, &chunk)?;

            pos += chunk_len;
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
        check_in_bounds("clear", offset, len, state.dev_size)?;

        let page_size = self.page_size;
        let mut pos: u64 = 0;
        while pos < len {
            let abs_offset = offset + pos;
            let page_idx = abs_offset / page_size;
            let page_offset = abs_offset % page_size;
            let chunk_len = (page_size - page_offset).min(len - pos);

            if chunk_len != page_size {
                self.ensure_page(&mut state, page_idx).await?;
            }

            let zeros = vec![0u8; chunk_len as usize];
            self.dirty_page_region(&mut state, page_idx, page_offset, &zeros)?;

            pos += chunk_len;
        }

        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;
        let dirty: Vec<u64> = (0..state.num_pages)
            .filter(|&i| bit_get(&state.dirty, i))
            .collect();

        if !dirty.is_empty() {
            info!(
                dirty_pages = dirty.len(),
                "flushing dirty cache pages to inner backend"
            );
            for page_idx in dirty {
                self.flush_page(&mut state, page_idx).await?;
            }
        }

        // Propagate to the inner backend in case it buffers too.
        drop(state);
        self.inner.flush().await
    }

    async fn delete(&self) -> anyhow::Result<()> {
        // Drop all cached/dirty state and delegate to the inner backend.
        let mut state = self.state.lock().await;
        let num_pages = state.num_pages;
        state.present = vec![0u8; bitmap_bytes(num_pages)];
        state.dirty = vec![0u8; bitmap_bytes(num_pages)];
        // Forget all resident pages for budget/eviction accounting too.
        state.resident_bytes = 0;
        state.lru.clear();
        if let Some(b) = &self.budget {
            b.reset(0).context("reset cache budget on delete")?;
        }
        drop(state);
        self.inner.delete().await
    }

    async fn size(&self) -> anyhow::Result<u64> {
        let state = self.state.lock().await;
        Ok(state.dev_size)
    }
}

/// Load the metadata bitmaps from `meta`, validating the header.  If the file is
/// empty, too short, or incompatible (different magic/version/page_size/dev_size)
/// it is (re)initialized to all-clean.  Returns `(present, dirty, dirty_count)`.
fn load_or_init_meta(
    meta: &File,
    page_size: u64,
    dev_size: u64,
    num_pages: u64,
) -> anyhow::Result<(Vec<u8>, Vec<u8>, u64)> {
    let bm_len = bitmap_bytes(num_pages);
    let expected_len = HEADER_SIZE + 2 * bm_len as u64;
    let actual_len = meta.metadata().context("stat cache metadata")?.len();

    let init = |reason: &str| -> anyhow::Result<(Vec<u8>, Vec<u8>, u64)> {
        if !reason.is_empty() {
            warn!(reason, "re-initializing local disk cache metadata");
        }
        let present = vec![0u8; bm_len];
        let dirty = vec![0u8; bm_len];
        write_full_meta(meta, page_size, dev_size, num_pages, &present, &dirty)?;
        Ok((present, dirty, 0))
    };

    if actual_len < expected_len {
        return init(if actual_len == 0 {
            ""
        } else {
            "metadata truncated"
        });
    }

    let mut header = [0u8; HEADER_SIZE as usize];
    meta.read_exact_at(&mut header, 0)
        .context("read cache metadata header")?;

    if &header[0..8] != MAGIC {
        return init("bad metadata magic");
    }
    let version = u32::from_le_bytes(header[8..12].try_into().unwrap());
    let stored_page_size = u64::from_le_bytes(header[16..24].try_into().unwrap());
    let stored_dev_size = u64::from_le_bytes(header[24..32].try_into().unwrap());
    if version != VERSION || stored_page_size != page_size || stored_dev_size != dev_size {
        return init("metadata header mismatch (version/page_size/dev_size)");
    }

    let mut present = vec![0u8; bm_len];
    let mut dirty = vec![0u8; bm_len];
    meta.read_exact_at(&mut present, HEADER_SIZE)
        .context("read present bitmap")?;
    meta.read_exact_at(&mut dirty, HEADER_SIZE + bm_len as u64)
        .context("read dirty bitmap")?;

    let dirty_count = (0..num_pages).filter(|&i| bit_get(&dirty, i)).count() as u64;
    Ok((present, dirty, dirty_count))
}

/// Write the full metadata header and both bitmaps, then `fsync`.
fn write_full_meta(
    meta: &File,
    page_size: u64,
    dev_size: u64,
    num_pages: u64,
    present: &[u8],
    dirty: &[u8],
) -> anyhow::Result<()> {
    let mut header = [0u8; HEADER_SIZE as usize];
    header[0..8].copy_from_slice(MAGIC);
    header[8..12].copy_from_slice(&VERSION.to_le_bytes());
    header[16..24].copy_from_slice(&page_size.to_le_bytes());
    header[24..32].copy_from_slice(&dev_size.to_le_bytes());
    header[32..40].copy_from_slice(&num_pages.to_le_bytes());

    let total = HEADER_SIZE + present.len() as u64 + dirty.len() as u64;
    meta.set_len(total).context("size cache metadata file")?;
    meta.write_all_at(&header, 0)
        .context("write metadata header")?;
    meta.write_all_at(present, HEADER_SIZE)
        .context("write present bitmap")?;
    meta.write_all_at(dirty, HEADER_SIZE + present.len() as u64)
        .context("write dirty bitmap")?;
    meta.sync_data().context("fsync cache metadata")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::mem::MemBackend;
    use std::path::Path;
    use std::sync::Arc;

    fn tmp_dir(tag: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let uniq = format!(
            "ublk-fcache-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        p.push(uniq);
        p
    }

    fn open(
        inner: Arc<dyn BlobBackend>,
        dir: &Path,
        page_size: u64,
        dev_size: u64,
    ) -> (FileCacheBackend, u64) {
        open_budgeted(inner, dir, page_size, dev_size, 0)
    }

    fn open_budgeted(
        inner: Arc<dyn BlobBackend>,
        dir: &Path,
        page_size: u64,
        dev_size: u64,
        max_bytes: u64,
    ) -> (FileCacheBackend, u64) {
        FileCacheBackend::open(
            inner,
            FileCacheConfig {
                dir: dir.to_path_buf(),
                name: "test".to_string(),
                page_size,
                max_bytes,
            },
            dev_size,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn write_read_roundtrip() {
        let dir = tmp_dir("roundtrip");
        let inner = Arc::new(MemBackend::new(4096).unwrap());
        let (b, _) = open(inner, &dir, 1024, 4096);
        let data = Bytes::from(vec![0xAB; 512]);
        b.write(0, data.clone()).await.unwrap();
        let read = b.read(0, 512).await.unwrap();
        assert_eq!(read, data);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn write_spanning_pages() {
        let dir = tmp_dir("spanning");
        let inner = Arc::new(MemBackend::new(4096).unwrap());
        let (b, _) = open(inner, &dir, 1024, 4096);
        let data = Bytes::from(vec![0xCD; 2048]);
        b.write(512, data.clone()).await.unwrap();
        let read = b.read(512, 2048).await.unwrap();
        assert_eq!(read, data);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn flush_persists_to_inner() {
        let dir = tmp_dir("flush");
        let inner = Arc::new(MemBackend::new(4096).unwrap());
        let (b, _) = open(inner.clone(), &dir, 1024, 4096);
        let data = Bytes::from(vec![0xEF; 512]);
        b.write(0, data.clone()).await.unwrap();

        // Before flush the inner backend is untouched.
        let inner_read = inner.read(0, 512).await.unwrap();
        assert!(inner_read.iter().all(|&x| x == 0));

        b.flush().await.unwrap();

        let inner_read = inner.read(0, 512).await.unwrap();
        assert_eq!(inner_read, data);
        assert_eq!(b.dirty_count().await, 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn clear_marks_dirty_and_zeroes() {
        let dir = tmp_dir("clear");
        let inner = Arc::new(MemBackend::new(4096).unwrap());
        let (b, _) = open(inner, &dir, 1024, 4096);
        b.write(0, Bytes::from(vec![0xFF; 512])).await.unwrap();
        b.clear(0, 512).await.unwrap();
        let read = b.read(0, 512).await.unwrap();
        assert!(read.iter().all(|&x| x == 0));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn out_of_bounds_is_rejected() {
        let dir = tmp_dir("oob");
        let inner = Arc::new(MemBackend::new(2048).unwrap());
        let (b, _) = open(inner, &dir, 1024, 2048);
        assert!(b.read(1536, 1024).await.is_err());
        assert!(b.write(1536, Bytes::from(vec![0u8; 1024])).await.is_err());
        assert!(b.clear(1536, 1024).await.is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn recovers_dirty_pages_after_restart() {
        let dir = tmp_dir("recover");
        let payload = Bytes::from(vec![0x5A; 512]);

        // First "boot": write into the cache but never flush to the blob.
        {
            let inner = Arc::new(MemBackend::new(4096).unwrap());
            let (b, recovered) = open(inner.clone(), &dir, 1024, 4096);
            assert_eq!(recovered, 0);
            b.write(1024, payload.clone()).await.unwrap();
            // Inner backend still empty — data lives only in the disk cache.
            assert!(inner.read(1024, 512).await.unwrap().iter().all(|&x| x == 0));
            // Drop b (simulating process exit) without flushing.
        }

        // Second "boot": brand new (empty) inner backend, same cache dir.
        {
            let inner = Arc::new(MemBackend::new(4096).unwrap());
            let (b, recovered) = open(inner.clone(), &dir, 1024, 4096);
            // Dirty page must have been recovered from disk metadata.
            assert_eq!(recovered, 1);
            // The cached data is still readable straight from the local disk.
            let read = b.read(1024, 512).await.unwrap();
            assert_eq!(read, payload);
            // And it can still be flushed to the (fresh) blob after restart.
            b.flush().await.unwrap();
            let inner_read = inner.read(1024, 512).await.unwrap();
            assert_eq!(inner_read, payload);
            assert_eq!(b.dirty_count().await, 0);
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn partial_page_write_preserves_neighbouring_data_on_flush() {
        // A sub-page write must read-modify-write the whole page so a later
        // full-page flush does not clobber the bytes it did not touch.
        let dir = tmp_dir("rmw");
        let inner = Arc::new(MemBackend::new(4096).unwrap());
        // Seed the inner backend so page 0 already has known content.
        inner.write(0, Bytes::from(vec![0x11; 1024])).await.unwrap();

        let (b, _) = open(inner.clone(), &dir, 1024, 4096);
        // Overwrite only the first 512 bytes of page 0.
        b.write(0, Bytes::from(vec![0x22; 512])).await.unwrap();
        b.flush().await.unwrap();

        // First half updated, second half (loaded from inner) preserved.
        let page = inner.read(0, 1024).await.unwrap();
        assert!(page[..512].iter().all(|&x| x == 0x22));
        assert!(page[512..].iter().all(|&x| x == 0x11));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn multi_page_write_flushes_all_pages() {
        let dir = tmp_dir("multipage");
        let inner = Arc::new(MemBackend::new(4096).unwrap());
        let (b, _) = open(inner.clone(), &dir, 1024, 4096);

        // Write across all four 1 KiB pages in one call.
        let data = Bytes::from(vec![0x9C; 4096]);
        b.write(0, data.clone()).await.unwrap();
        assert_eq!(b.dirty_count().await, 4);

        b.flush().await.unwrap();
        assert_eq!(b.dirty_count().await, 0);
        assert_eq!(inner.read(0, 4096).await.unwrap(), data);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn overwrite_dirty_page_flushes_latest_value() {
        let dir = tmp_dir("overwrite");
        let inner = Arc::new(MemBackend::new(4096).unwrap());
        let (b, _) = open(inner.clone(), &dir, 1024, 4096);

        // Two writes to the same page before any flush.
        b.write(0, Bytes::from(vec![0x01; 512])).await.unwrap();
        b.write(0, Bytes::from(vec![0x02; 512])).await.unwrap();
        // Still a single dirty page.
        assert_eq!(b.dirty_count().await, 1);

        b.flush().await.unwrap();
        // The most recent write wins on the inner backend.
        assert_eq!(
            inner.read(0, 512).await.unwrap(),
            Bytes::from(vec![0x02; 512])
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn write_to_last_short_page() {
        // dev_size (2560) is not a multiple of page_size (1024): the last page
        // is a short 512-byte page.  Writing it must flush only its valid bytes.
        let dir = tmp_dir("shortpage");
        let inner = Arc::new(MemBackend::new(2560).unwrap());
        let (b, _) = open(inner.clone(), &dir, 1024, 2560);

        let payload = Bytes::from(vec![0x77; 512]);
        b.write(2048, payload.clone()).await.unwrap();
        b.flush().await.unwrap();

        assert_eq!(inner.read(2048, 512).await.unwrap(), payload);
        let read = b.read(2048, 512).await.unwrap();
        assert_eq!(read, payload);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn read_after_write_hits_cache_without_touching_inner() {
        // After a cached write, a read of that page is served from the local
        // disk file, not the (still-empty) inner backend.
        let dir = tmp_dir("cachehit");
        let inner = Arc::new(MemBackend::new(4096).unwrap());
        let (b, _) = open(inner.clone(), &dir, 1024, 4096);

        let payload = Bytes::from(vec![0x3D; 512]);
        b.write(512, payload.clone()).await.unwrap();

        // Inner is still untouched (no flush yet) but the cache serves the data.
        assert!(inner.read(512, 512).await.unwrap().iter().all(|&x| x == 0));
        assert_eq!(b.read(512, 512).await.unwrap(), payload);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn read_miss_is_served_from_inner_without_caching() {
        // A pure read of a never-written page comes straight from the inner
        // backend and does not populate the cache (page stays absent).
        let dir = tmp_dir("readmiss");
        let inner = Arc::new(MemBackend::new(4096).unwrap());
        inner.write(0, Bytes::from(vec![0x6E; 512])).await.unwrap();

        let (b, _) = open(inner.clone(), &dir, 1024, 4096);
        assert_eq!(b.read(0, 512).await.unwrap(), Bytes::from(vec![0x6E; 512]));
        // Read does not dirty anything.
        assert_eq!(b.dirty_count().await, 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn dirty_writes_survive_reopen_and_flush_once() {
        // Writes that are persisted to the cache but never flushed are recovered
        // as dirty after reopening the cache against the same directory, and the
        // recovered data flushes correctly to a fresh inner backend.
        let dir = tmp_dir("reopen");
        let payload = Bytes::from(vec![0x4F; 2048]);
        {
            let inner = Arc::new(MemBackend::new(4096).unwrap());
            let (b, recovered) = open(inner, &dir, 1024, 4096);
            assert_eq!(recovered, 0);
            b.write(0, payload.clone()).await.unwrap();
            // Drop without flushing.
        }
        {
            let inner = Arc::new(MemBackend::new(4096).unwrap());
            let (b, recovered) = open(inner.clone(), &dir, 1024, 4096);
            assert_eq!(recovered, 2);
            // Cached data still readable from disk after reopen.
            assert_eq!(b.read(0, 2048).await.unwrap(), payload);
            b.flush().await.unwrap();
            assert_eq!(inner.read(0, 2048).await.unwrap(), payload);
            assert_eq!(b.dirty_count().await, 0);
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn create_resets_cache() {
        let dir = tmp_dir("create");
        let inner = Arc::new(MemBackend::new(4096).unwrap());
        let (b, _) = open(inner, &dir, 1024, 4096);
        b.write(0, Bytes::from(vec![0x7E; 512])).await.unwrap();
        assert_eq!(b.dirty_count().await, 1);
        b.create(4096).await.unwrap();
        assert_eq!(b.dirty_count().await, 0);
        // After reset the page reads back as zero from the fresh inner backend.
        let read = b.read(0, 512).await.unwrap();
        assert!(read.iter().all(|&x| x == 0));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn budget_evicts_lru_clean_pages() {
        // Budget of two 1 KiB pages.  Reading three different pages must keep the
        // cache at two resident pages, evicting the least-recently-used one.
        let dir = tmp_dir("evict");
        let inner = Arc::new(MemBackend::new(4096).unwrap());
        for (i, byte) in [0x10u8, 0x20, 0x30, 0x40].into_iter().enumerate() {
            inner
                .write(i as u64 * 1024, Bytes::from(vec![byte; 1024]))
                .await
                .unwrap();
        }
        // Populate pages by writing them (full-page writes mark them present).
        let (b, _) = open_budgeted(inner.clone(), &dir, 1024, 4096, 2048);

        // Make pages clean+resident by writing then flushing each.
        b.write(0, Bytes::from(vec![0xA0; 1024])).await.unwrap();
        b.write(1024, Bytes::from(vec![0xA1; 1024])).await.unwrap();
        b.flush().await.unwrap();
        assert_eq!(b.present_count().await, 2);
        assert_eq!(b.resident_bytes().await, 2048);

        // Touch page 0 so page 1 becomes the LRU victim.
        let _ = b.read(0, 512).await.unwrap();

        // A third clean page must evict exactly one page to stay within budget.
        b.write(2048, Bytes::from(vec![0xA2; 1024])).await.unwrap();
        b.flush().await.unwrap();
        assert_eq!(b.present_count().await, 2);
        assert_eq!(b.resident_bytes().await, 2048);

        // Page 1 (LRU) was evicted: it now re-reads from the inner backend value.
        assert_eq!(
            b.read(1024, 1024).await.unwrap(),
            Bytes::from(vec![0xA1; 1024])
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn budget_never_evicts_dirty_pages() {
        // Even when way over budget, unflushed (dirty) pages must never be
        // dropped — that would lose data.
        let dir = tmp_dir("evictdirty");
        let inner = Arc::new(MemBackend::new(4096).unwrap());
        // Tiny budget: one page.
        let (b, _) = open_budgeted(inner.clone(), &dir, 1024, 4096, 1024);

        // Dirty three pages without flushing.
        b.write(0, Bytes::from(vec![0xB0; 1024])).await.unwrap();
        b.write(1024, Bytes::from(vec![0xB1; 1024])).await.unwrap();
        b.write(2048, Bytes::from(vec![0xB2; 1024])).await.unwrap();

        // All three remain present (dirty, non-evictable) despite the 1-page budget.
        assert_eq!(b.dirty_count().await, 3);
        assert_eq!(b.present_count().await, 3);

        // They all flush correctly (no data lost to eviction).
        b.flush().await.unwrap();
        assert_eq!(inner.read(0, 1024).await.unwrap(), Bytes::from(vec![0xB0; 1024]));
        assert_eq!(inner.read(1024, 1024).await.unwrap(), Bytes::from(vec![0xB1; 1024]));
        assert_eq!(inner.read(2048, 1024).await.unwrap(), Bytes::from(vec![0xB2; 1024]));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn unlimited_budget_never_evicts() {
        // max_bytes == 0 preserves the original grow-only behaviour.
        let dir = tmp_dir("unlimited");
        let inner = Arc::new(MemBackend::new(4096).unwrap());
        let (b, _) = open_budgeted(inner.clone(), &dir, 1024, 4096, 0);
        for i in 0..4u64 {
            b.write(i * 1024, Bytes::from(vec![0xC0 + i as u8; 1024]))
                .await
                .unwrap();
        }
        b.flush().await.unwrap();
        assert_eq!(b.present_count().await, 4);
        std::fs::remove_dir_all(&dir).ok();
    }
}
