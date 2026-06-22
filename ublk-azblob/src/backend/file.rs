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
use super::cache_index::CacheIndex;
use super::BlobBackend;
use anyhow::{bail, Context as _};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::Path;
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
    /// Base file name (without extension) for the cache files.  Must be unique
    /// per cache instance within `dir`; when [`share_pages`](Self::share_pages)
    /// is set it is also the *owner* key peers use to locate this cache's data
    /// file for cross-process page sharing.
    pub name: String,
    /// Size of each cache page in bytes (must be a non-zero multiple of 512).
    pub page_size: u64,
    /// Maximum total bytes of cached page data on local disk, **shared across
    /// all processes** using the same `dir` (see [`CacheBudget`]).  `0` means
    /// unlimited (no eviction), preserving the original grow-only behaviour.
    pub max_bytes: u64,
    /// Identity of the backing blob this cache mirrors.  Caches in the same
    /// `dir` that share a `blob_identity` may serve each other's clean pages
    /// (cross-process page sharing); only used when
    /// [`share_pages`](Self::share_pages) is set.  Typically the container/blob
    /// (or golden-image) identity.
    pub blob_identity: String,
    /// Enable cross-process clean-page sharing via a shared `.cache-index`
    /// (see [`CacheIndex`]).  `false` (default) preserves the original
    /// behaviour with zero overhead — each cache only ever reads its own pages
    /// and the blob.
    pub share_pages: bool,
}

impl Default for FileCacheConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("."),
            name: "ublk-azblob-cache".to_string(),
            page_size: 1024 * 1024, // 1 MiB
            max_bytes: 0,           // unlimited
            blob_identity: String::new(),
            share_pages: false,
        }
    }
}

/// In-memory LRU bookkeeping for resident (present) cache pages.
///
/// Tracks the recency of every present page and which of them are *evictable*
/// (present **and** clean — dirty pages must be flushed, never dropped).  Only
/// maintained when a [`CacheBudget`] is active.
///
/// # Why a purpose-built structure instead of an off-the-shelf LRU crate
///
/// Off-the-shelf caches (`lru`, `clru`, `quick_cache`, `moka`, `foyer`, …) own
/// the cached *values* and evict purely by recency (optionally by a byte
/// weight).  This cache is structurally different on three axes that none of
/// them model:
///
/// - **Values live on disk, not in the map.**  Page bytes are stored in a
///   sparse data file addressed by page index; eviction is a `PUNCH_HOLE` plus
///   a bitmap-bit clear, so the LRU only needs to order page *indices*.
/// - **A pinned, non-evictable subset.**  Dirty (unflushed) pages must never be
///   dropped or data is lost; generic LRUs have no notion of "evict by recency,
///   but only among the clean pages".  Here `evictable` is a second index that
///   excludes dirty pages, and pages move in/out of it as they are
///   dirtied/flushed.
/// - **A cross-process byte budget.**  The real limit is shared between
///   processes via the `flock`-coordinated [`CacheBudget`]; an in-process cache
///   crate cannot enforce it.  `foyer` is the closest "disk block cache" but it
///   owns its own on-disk format and recovery, which would replace this whole
///   backend and still not provide the cross-process budget.
///
/// The structure is intentionally small: a monotonic recency clock plus two
/// indexes giving O(log n) insert/touch and O(1)-amortised LRU selection.
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
    /// Shared cross-process clean-page index; `None` when page sharing is off.
    index: Option<CacheIndex>,
    /// Whether the cache dir's filesystem supports `FALLOC_FL_PUNCH_HOLE`.
    /// Eviction reclaims disk by punching holes; on a filesystem that lacks it
    /// (NFS, some overlay / virtio-fs) we degrade to grow-only instead of
    /// failing live writes (see [`FileCacheBackend::open`]).
    eviction_supported: bool,
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

        // Optional shared cross-process clean-page index.  When active, publish
        // every page recovered as present **and clean** so peers caching the
        // same blob can read it from our data file instead of the blob.
        let index = if cfg.share_pages {
            let idx = CacheIndex::open(&cfg.dir, &cfg.name, &cfg.blob_identity, cfg.page_size)
                .context("open shared cache index")?;
            for page_idx in 0..state.num_pages {
                if bit_get(&state.present, page_idx) && !bit_get(&state.dirty, page_idx) {
                    let offset = page_idx * cfg.page_size;
                    let page_len = cfg.page_size.min(dev_size.saturating_sub(offset));
                    idx.publish(page_idx, page_len)
                        .context("publish recovered clean page")?;
                }
            }
            Some(idx)
        } else {
            None
        };

        // Eviction reclaims disk by punching holes in the data file. Probe
        // support once (only matters when a budget is set); if the cache dir's
        // filesystem lacks `FALLOC_FL_PUNCH_HOLE` (NFS, some overlay/virtio-fs),
        // degrade to grow-only rather than turning the first over-budget write
        // into a fatal I/O error.
        let eviction_supported = if budget.is_some() {
            let ok = probe_punch_hole(&cfg.dir);
            if !ok {
                warn!(
                    dir = %cfg.dir.display(),
                    "cache filesystem does not support FALLOC_FL_PUNCH_HOLE; \
                     disabling eviction (cache grows without reclaiming disk and \
                     the byte budget is not enforced)"
                );
            }
            ok
        } else {
            // No budget → eviction never runs, so support is irrelevant.
            true
        };

        Ok((
            Self {
                inner,
                page_size: cfg.page_size,
                budget,
                index,
                eviction_supported,
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
        if let Some(idx) = &self.index {
            idx.unpublish_all().context("clear cache index on reinit")?;
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
        if !self.eviction_supported {
            // Grow-only: this filesystem can't reclaim disk via hole-punching, so
            // we keep all pages rather than failing the live write (warned once at
            // open). The byte budget is effectively unenforced.
            return Ok(());
        }

        let mut evicted = 0u64;
        while over > 0 {
            let Some(page_idx) = state.lru.pop_lru(protect) else {
                break; // nothing else we own is evictable
            };
            let offset = page_idx * self.page_size;
            let len = self.page_len(state, page_idx);

            // Withdraw the page from the shared index *before* punching its hole
            // so no peer can be mid-read of bytes we are about to deallocate
            // (`read_peer_page` and `unpublish` are mutually exclusive under the
            // index lock).
            if let Some(idx) = &self.index {
                idx.unpublish(page_idx)
                    .with_context(|| format!("unpublish evicted page {page_idx}"))?;
            }

            if len > 0 {
                if let Err(err) = punch_hole(&state.data, offset, len) {
                    // Couldn't reclaim this page's disk (e.g. a transient
                    // fallocate failure on an otherwise-supported FS). Restore it
                    // to a consistent state — back into the LRU and re-published —
                    // and stop evicting rather than failing the live write or
                    // leaving the LRU/budget out of sync. We may briefly run over
                    // budget until the next eviction pass.
                    warn!(page_idx, %err, "punch hole failed; skipping eviction this pass");
                    state.lru.touch(page_idx, true);
                    if let Some(idx) = &self.index {
                        let _ = idx.publish(page_idx, len);
                    }
                    break;
                }
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
    /// a peer cache (cross-process sharing) or the inner backend if necessary.
    /// Returns the page's valid length (the last page may be shorter than
    /// `page_size`).
    async fn ensure_page(&self, state: &mut CacheState, page_idx: u64) -> anyhow::Result<u64> {
        let offset = page_idx * self.page_size;
        let page_len = self.page_size.min(state.dev_size.saturating_sub(offset));

        if bit_get(&state.present, page_idx) {
            return Ok(page_len);
        }

        // Prefer a live peer's clean copy of this page (copy-on-write: cheaper
        // than a blob round-trip); fall back to the inner backend.
        let data: Vec<u8> = match self.try_peer_page(page_idx, page_len)? {
            Some(buf) => buf,
            None => {
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
                data.to_vec()
            }
        };
        self.store_clean_page(state, page_idx, offset, &data)?;
        Ok(page_len)
    }

    /// Write a freshly-fetched **clean** page into the cache file, mark it
    /// present (persisting the bit; a lost bit just causes a re-read, so the
    /// write is non-fatal), and account it against the byte budget. Shared by
    /// [`ensure_page`](Self::ensure_page) and the concurrent warm-up path.
    fn store_clean_page(
        &self,
        state: &mut CacheState,
        page_idx: u64,
        offset: u64,
        data: &[u8],
    ) -> anyhow::Result<()> {
        // An all-zero page is left as a sparse hole in the data file
        // (which reads back as zeros) instead of being written, so zero regions
        // of the blob — e.g. an ext4 image's free space — consume no local disk.
        // This is always safe here: this function is called only for a
        // not-yet-present page whose data-file region is still a hole.
        if data.iter().any(|&b| b != 0) {
            state
                .data
                .write_all_at(data, offset)
                .with_context(|| format!("write page {page_idx} to cache file"))?;
        }

        bit_set(&mut state.present, page_idx, true);
        let byte = page_idx / 8;
        let present_off = HEADER_SIZE + byte;
        if let Err(err) = state
            .meta
            .write_all_at(&[state.present[byte as usize]], present_off)
        {
            warn!(page_idx, %err, "failed to persist clean present bit (non-fatal)");
        }

        // Account the newly-resident clean page and evict if over budget. The
        // page just loaded is protected from immediate eviction.
        self.account_present(state, page_idx, true)?;
        Ok(())
    }

    /// Make `page_idx` resident as a clean page, fetching it from a peer or the
    /// inner backend **without holding the state lock** during the fetch — so
    /// warm-up can run many fetches concurrently (the serial `ensure_page` holds
    /// the lock across the blob read, which would serialize them). The lock is
    /// taken only to check presence and to store the bytes. Returns the number
    /// of newly-warmed bytes (0 if the page was already present / lost a race).
    async fn warm_one_page(&self, page_idx: u64) -> anyhow::Result<u64> {
        let (offset, page_len) = {
            let state = self.state.lock().await;
            if bit_get(&state.present, page_idx) {
                return Ok(0);
            }
            let offset = page_idx * self.page_size;
            (
                offset,
                self.page_size.min(state.dev_size.saturating_sub(offset)),
            )
        };
        if page_len == 0 {
            return Ok(0);
        }

        // Fetch with no state lock held, so concurrent warm-up pages overlap.
        let data: Vec<u8> = match self.try_peer_page(page_idx, page_len)? {
            Some(buf) => buf,
            None => {
                let d = self
                    .inner
                    .read(offset, page_len)
                    .await
                    .with_context(|| format!("warm-up read page {page_idx}"))?;
                if d.len() as u64 != page_len {
                    bail!(
                        "inner backend returned {} bytes for page {page_idx} (expected {page_len})",
                        d.len()
                    );
                }
                d.to_vec()
            }
        };

        // Store under a brief lock; another warm-up task may have raced us here.
        let mut state = self.state.lock().await;
        if bit_get(&state.present, page_idx) {
            return Ok(0);
        }
        self.store_clean_page(&mut state, page_idx, offset, &data)?;
        if let Some(idx) = &self.index {
            if !bit_get(&state.dirty, page_idx) {
                idx.publish(page_idx, page_len)
                    .with_context(|| format!("publish warmed page {page_idx}"))?;
            }
        }
        Ok(page_len)
    }

    /// Try to fetch a full page from a live peer's cache via the shared index.
    /// Returns the page bytes on a sharing hit, `None` when no peer has it (so
    /// the caller fetches from the blob).  A no-op when sharing is disabled.
    fn try_peer_page(&self, page_idx: u64, page_len: u64) -> anyhow::Result<Option<Vec<u8>>> {
        let Some(idx) = &self.index else {
            return Ok(None);
        };
        let mut buf = vec![0u8; page_len as usize];
        if idx
            .read_peer_page(page_idx, &mut buf)
            .with_context(|| format!("read peer copy of page {page_idx}"))?
        {
            trace!(page_idx, len = page_len, "served page from peer cache");
            Ok(Some(buf))
        } else {
            Ok(None)
        }
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
        // A freshly-flushed page is clean and resident, so peers caching the
        // same blob may now read it from our data file (cross-process sharing).
        if let Some(idx) = &self.index {
            idx.publish(page_idx, page_len)
                .with_context(|| format!("publish flushed page {page_idx}"))?;
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
        // Copy-on-write: withdraw this page from the shared index *before*
        // mutating it so peers stop reading our (now diverging) copy and never
        // observe a torn write.  The page becomes private to us until it is
        // flushed clean again (and re-published).
        if let Some(idx) = &self.index {
            idx.unpublish(page_idx)
                .with_context(|| format!("unpublish dirtied page {page_idx}"))?;
        }

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
///
/// `FALLOC_FL_PUNCH_HOLE` is a Linux-only syscall, so this is the Linux impl.
#[cfg(target_os = "linux")]
fn punch_hole(file: &File, offset: u64, len: u64) -> anyhow::Result<()> {
    use std::os::unix::io::AsRawFd as _;
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

/// Non-Linux fallback: hole punching (`FALLOC_FL_PUNCH_HOLE`) is a Linux-only
/// syscall. Report it as unsupported so [`probe_punch_hole`] returns `false` and
/// the cache degrades to grow-only (no eviction), exactly like a filesystem that
/// doesn't support hole punching. Keeps the portable core build compiling on
/// non-Linux targets.
#[cfg(not(target_os = "linux"))]
fn punch_hole(_file: &File, _offset: u64, _len: u64) -> anyhow::Result<()> {
    anyhow::bail!("hole punching (FALLOC_FL_PUNCH_HOLE) is only supported on Linux")
}

/// Probe whether `dir`'s filesystem supports `FALLOC_FL_PUNCH_HOLE` by punching a
/// hole in a throwaway temp file. Returns `false` on `EOPNOTSUPP` (or any other
/// failure), in which case eviction is disabled and the cache runs grow-only.
fn probe_punch_hole(dir: &Path) -> bool {
    let path = dir.join(format!(".punch-probe.{}", std::process::id()));
    let ok = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .and_then(|f| {
            f.set_len(4096)?;
            // 4096 is a safe, alignment-agnostic probe length.
            punch_hole(&f, 0, 4096).map_err(std::io::Error::other)
        })
        .is_ok();
    let _ = std::fs::remove_file(&path);
    ok
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
                // Cache miss.  When cross-process sharing is on, try to serve
                // the page from a live peer's local copy (a peer that already
                // fetched this blob page) before falling back to the inner
                // backend.  Pure reads do not populate our own cache — keeping
                // read latency predictable and the byte budget single-owner:
                // the page stays resident only in the peer that owns it.
                let page_len = self.page_len(&state, page_idx);
                let mut served = false;
                if let Some(page) = self.try_peer_page(page_idx, page_len)? {
                    let start = page_offset as usize;
                    result[pos as usize..(pos + chunk_len) as usize]
                        .copy_from_slice(&page[start..start + chunk_len as usize]);
                    served = true;
                }
                if !served {
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
            }

            pos += chunk_len;
        }

        Ok(result.freeze())
    }

    async fn prefetch(&self, offset: u64, len: u64) -> anyhow::Result<()> {
        if len == 0 {
            return Ok(());
        }
        if !offset.is_multiple_of(512) || !len.is_multiple_of(512) {
            bail!("prefetch: offset ({offset}) and len ({len}) must be 512-byte aligned");
        }

        let page_size = self.page_size;
        let dev_size = {
            let state = self.state.lock().await;
            state.dev_size
        };
        check_in_bounds("prefetch", offset, len, dev_size)?;

        // Align the start down to a page boundary and walk page-by-page, taking
        // the lock for one page at a time so live I/O can interleave during a
        // long warm-up.  `ensure_page` fetches the page (from a peer or the
        // inner backend) and stores it locally as a clean page; when cross-process
        // sharing is on we also publish it so peers caching the same blob can read
        // it from our data file (this is what makes warm-up populate the *shared*
        // cache, not just our own — otherwise clean warmed pages stay unpublished).
        let end = offset + len;
        let mut page_off = offset - (offset % page_size);
        while page_off < end {
            let page_idx = page_off / page_size;
            {
                let mut state = self.state.lock().await;
                let page_len = self.ensure_page(&mut state, page_idx).await?;
                if let Some(idx) = &self.index {
                    // Only publish genuinely clean pages: a page already dirtied
                    // by a concurrent write must not be advertised as a clean copy.
                    if page_len > 0 && !bit_get(&state.dirty, page_idx) {
                        idx.publish(page_idx, page_len)
                            .with_context(|| format!("publish warmed page {page_idx}"))?;
                    }
                }
            }
            page_off += page_size;
        }
        Ok(())
    }

    async fn warmup(&self, dev_size: u64, _page_size: u64, limit_bytes: u64, concurrency: usize) {
        use futures::stream::{self, StreamExt};
        use std::sync::atomic::{AtomicU64, Ordering};

        // Warm in this cache's own page units, regardless of the caller's
        // `page_size` hint (the cache can only make whole pages resident).
        let page_size = self.page_size;
        let limit = limit_bytes.min(dev_size);
        if limit == 0 {
            return;
        }
        let n_pages = limit.div_ceil(page_size);
        let conc = concurrency.max(1);

        // Query sparseness map to skip zero regions; best-effort, so a failure
        // falls back to warming everything.
        let data_ranges = match self.data_ranges().await {
            Ok(ranges) => ranges,
            Err(err) => {
                warn!(%err, "data-ranges query failed; warming the whole device");
                None
            }
        };
        if let Some(ranges) = &data_ranges {
            let data_bytes: u64 = ranges.iter().map(|&(_, len)| len).sum();
            info!(
                data_ranges = ranges.len(),
                data_bytes, "warm-up using blob sparseness map (skipping zero regions)"
            );
        }

        let warmed = AtomicU64::new(0);
        let skipped = AtomicU64::new(0);

        // Fetch up to `conc` pages from the blob at once (each `warm_one_page`
        // does its blob read with no state lock held), turning warm-up from a
        // latency-bound serial scan into a bandwidth-bound parallel one.
        stream::iter(0..n_pages)
            .for_each_concurrent(conc, |page_idx| {
                let warmed = &warmed;
                let skipped = &skipped;
                let data_ranges = &data_ranges;
                async move {
                    let offset = page_idx * page_size;
                    let len = page_size.min(dev_size.saturating_sub(offset));
                    if let Some(ranges) = data_ranges {
                        if !super::range_intersects(ranges, offset, len) {
                            skipped.fetch_add(len, Ordering::Relaxed);
                            return;
                        }
                    }
                    match self.warm_one_page(page_idx).await {
                        Ok(n) => {
                            warmed.fetch_add(n, Ordering::Relaxed);
                        }
                        Err(err) => warn!(page_idx, %err, "warm-up page failed (continuing)"),
                    }
                }
            })
            .await;

        info!(
            warmed_bytes = warmed.load(Ordering::Relaxed),
            skipped_bytes = skipped.load(Ordering::Relaxed),
            limit_bytes = limit,
            concurrency = conc,
            "cache warm-up complete"
        );
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

    async fn data_ranges(&self) -> anyhow::Result<Option<Vec<(u64, u64)>>> {
        // Sparseness is a property of the backing blob, not the local cache.
        self.inner.data_ranges().await
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
        if let Some(idx) = &self.index {
            idx.unpublish_all().context("clear cache index on delete")?;
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
                blob_identity: String::new(),
                share_pages: false,
            },
            dev_size,
        )
        .unwrap()
    }

    /// Open a sharing-enabled cache with a given owner `name` and shared
    /// `blob_identity` so two instances in one dir can serve each other's pages.
    fn open_shared(
        inner: Arc<dyn BlobBackend>,
        dir: &Path,
        name: &str,
        blob_identity: &str,
        page_size: u64,
        dev_size: u64,
    ) -> (FileCacheBackend, u64) {
        FileCacheBackend::open(
            inner,
            FileCacheConfig {
                dir: dir.to_path_buf(),
                name: name.to_string(),
                page_size,
                max_bytes: 0,
                blob_identity: blob_identity.to_string(),
                share_pages: true,
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
        // Read does not dirty anything, and the page stays absent from the cache.
        assert_eq!(b.dirty_count().await, 0);
        assert_eq!(b.present_count().await, 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn prefetch_populates_cache_as_clean_pages() {
        // Warm-up's prefetch makes pages resident locally (clean, not dirty) so
        // later reads are served from the cache file even if the inner backend
        // changes, and no write-back is scheduled.
        let dir = tmp_dir("prefetch");
        let inner = Arc::new(MemBackend::new(8192).unwrap());
        inner.write(0, Bytes::from(vec![0xAB; 2048])).await.unwrap();

        let (b, _) = open(inner.clone(), &dir, 1024, 4096);
        // Prefetch the first two 1 KiB pages.
        b.prefetch(0, 2048).await.unwrap();

        // Both pages are now resident and clean (no write-back pending).
        assert_eq!(b.present_count().await, 2);
        assert_eq!(b.dirty_count().await, 0);

        // Mutate the inner backend; the cache must serve the warmed copy.
        inner.write(0, Bytes::from(vec![0x00; 2048])).await.unwrap();
        assert_eq!(
            b.read(0, 2048).await.unwrap(),
            Bytes::from(vec![0xAB; 2048])
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn prefetch_zero_page_leaves_sparse_hole() {
        // An all-zero page is marked present and reads back as zeros,
        // but is left as a sparse hole in the data file (no blocks allocated).
        use std::os::unix::fs::MetadataExt;
        let dir = tmp_dir("zerohole");
        // Inner backend is all zeros (MemBackend starts zero-filled).
        let inner = Arc::new(MemBackend::new(1 << 20).unwrap());
        let (b, _) = open(inner, &dir, 65536, 1 << 20);

        b.prefetch(0, 1 << 20).await.unwrap();

        // Every page is resident (so reads never hit the inner backend again)...
        assert_eq!(b.present_count().await, 16);
        // ...and reads back as zeros.
        assert_eq!(b.read(0, 4096).await.unwrap(), Bytes::from(vec![0u8; 4096]));

        // The data file is logically 1 MiB but allocates ~no blocks (a hole).
        let meta = std::fs::metadata(dir.join("test.dat")).unwrap();
        assert_eq!(meta.len(), 1 << 20);
        // 512-byte blocks; a fully written 1 MiB file would be ~2048 blocks.
        assert!(
            meta.blocks() < 64,
            "expected a sparse data file, got {} blocks",
            meta.blocks()
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn warmup_concurrent_populates_all_pages() {
        // The concurrent warm-up (`warmup` with concurrency > 1) fetches every
        // page from the inner backend and makes it resident as a clean page.
        let dir = tmp_dir("warmup-conc");
        let page = 1024u64;
        let dev = 16 * page; // 16 pages
        let inner = Arc::new(MemBackend::new(dev).unwrap());
        inner
            .write(0, Bytes::from(vec![0x5A; dev as usize]))
            .await
            .unwrap();

        let (b, _) = open(inner.clone(), &dir, page, dev);
        // Warm the whole device, 8 pages in flight at once.
        b.warmup(dev, page, dev, 8).await;

        assert_eq!(b.present_count().await, 16);
        assert_eq!(b.dirty_count().await, 0);

        // Mutate inner; every page must now be served from the warmed cache.
        inner
            .write(0, Bytes::from(vec![0x00; dev as usize]))
            .await
            .unwrap();
        assert_eq!(
            b.read(0, dev).await.unwrap(),
            Bytes::from(vec![0x5A; dev as usize])
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A wrapper that delegates to an inner backend but reports a fixed
    /// sparseness map via `data_ranges()` and counts reads, so the file-cache
    /// warm-up's zero-gap skip branch can be unit-tested (MemBackend reports
    /// `None`, which never exercises it).
    struct SparseBackend {
        inner: Arc<dyn BlobBackend>,
        ranges: Vec<(u64, u64)>,
        reads: std::sync::atomic::AtomicU64,
    }

    impl SparseBackend {
        fn new(inner: Arc<dyn BlobBackend>, ranges: Vec<(u64, u64)>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                ranges,
                reads: std::sync::atomic::AtomicU64::new(0),
            })
        }
        fn read_count(&self) -> u64 {
            self.reads.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl BlobBackend for SparseBackend {
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
        async fn data_ranges(&self) -> anyhow::Result<Option<Vec<(u64, u64)>>> {
            Ok(Some(self.ranges.clone()))
        }
    }

    #[tokio::test]
    async fn warmup_skips_zero_gap_pages_via_data_ranges() {
        // With a sparse `data_ranges()` map, warm-up must populate only the
        // pages overlapping a data range and never fetch (or make resident) the
        // pages lying entirely in a zero gap.
        let dir = tmp_dir("warmup-sparse");
        let page = 1024u64;
        let dev = 16 * page; // 16 pages

        // Data lives only in pages [4, 8); the rest of the device is a zero gap.
        let data_off = 4 * page;
        let data_len = 4 * page;
        let inner = Arc::new(MemBackend::new(dev).unwrap());
        inner
            .write(data_off, Bytes::from(vec![0x5A; data_len as usize]))
            .await
            .unwrap();
        let sparse = SparseBackend::new(inner, vec![(data_off, data_len)]);

        let (b, _) = open(sparse.clone(), &dir, page, dev);
        b.warmup(dev, page, dev, 8).await;

        // Only the 4 data pages are resident; the 12 zero-gap pages were skipped.
        assert_eq!(b.present_count().await, 4);
        // And only those 4 pages were ever fetched from the backend.
        assert_eq!(sparse.read_count(), 4);

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
        assert_eq!(
            inner.read(0, 1024).await.unwrap(),
            Bytes::from(vec![0xB0; 1024])
        );
        assert_eq!(
            inner.read(1024, 1024).await.unwrap(),
            Bytes::from(vec![0xB1; 1024])
        );
        assert_eq!(
            inner.read(2048, 1024).await.unwrap(),
            Bytes::from(vec![0xB2; 1024])
        );
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

    #[tokio::test]
    async fn evicted_pages_reload_correct_data() {
        // After a clean page is evicted to honour the budget, a later read of it
        // must transparently reload the exact bytes from the inner backend, while
        // the most-recently-written page stays resident in the cache.
        let dir = tmp_dir("reload");
        let inner = Arc::new(MemBackend::new(8192).unwrap());
        let (b, _) = open_budgeted(inner.clone(), &dir, 4096, 8192, 4096);

        // Page 0 becomes clean + resident, filling the whole one-page budget.
        b.write(0, Bytes::from(vec![0xD0; 4096])).await.unwrap();
        b.flush().await.unwrap();
        assert_eq!(b.present_count().await, 1);

        // Writing page 1 admits it over budget and evicts the older clean page 0.
        b.write(4096, Bytes::from(vec![0xD1; 4096])).await.unwrap();
        b.flush().await.unwrap();
        assert_eq!(b.present_count().await, 1);
        assert_eq!(b.resident_bytes().await, 4096);

        // Both pages read back their correct values: page 1 from the resident
        // cache, page 0 reloaded from the blob after its eviction.
        assert_eq!(
            b.read(4096, 4096).await.unwrap(),
            Bytes::from(vec![0xD1; 4096])
        );
        assert_eq!(
            b.read(0, 4096).await.unwrap(),
            Bytes::from(vec![0xD0; 4096])
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn eviction_reclaims_disk_space() {
        use std::os::unix::fs::MetadataExt as _;

        // Eviction must physically reclaim disk via PUNCH_HOLE, not merely clear
        // a bit: with a 4-page budget, churning through 8 pages keeps the data
        // file's allocated block count near 4 pages, far below the 8 pages it
        // would reach if nothing were reclaimed.
        let dir = tmp_dir("reclaim");
        std::fs::create_dir_all(&dir).unwrap();
        let page = 4096u64;
        let dev_size = 8 * page;
        let inner = Arc::new(MemBackend::new(dev_size).unwrap());
        let (b, _) = open_budgeted(inner.clone(), &dir, page, dev_size, 4 * page);

        let data_file = dir.join("test.dat");
        let blocks = || std::fs::metadata(&data_file).unwrap().blocks();
        let page_blocks = page / 512;

        // Fill the budget with four clean pages.
        for i in 0..4u64 {
            b.write(i * page, Bytes::from(vec![0xE0 + i as u8; page as usize]))
                .await
                .unwrap();
        }
        b.flush().await.unwrap();
        let baseline = blocks();

        // Write four more pages; each admission evicts an LRU clean page.
        for i in 4..8u64 {
            b.write(i * page, Bytes::from(vec![0xE0 + i as u8; page as usize]))
                .await
                .unwrap();
            b.flush().await.unwrap();
        }

        assert_eq!(b.present_count().await, 4);
        assert_eq!(b.resident_bytes().await, 4 * page);
        // Without reclaiming, all eight pages would stay allocated (≈ 2×baseline).
        // Allow one page of slack for filesystem rounding.
        assert!(
            blocks() <= baseline + page_blocks,
            "data file kept {} blocks (baseline {baseline}); holes were not punched",
            blocks()
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A `BlobBackend` decorator that counts how many `read` calls reach it, so
    /// cross-process sharing tests can assert a peer served a page with **zero**
    /// blob reads.
    struct CountingBackend {
        inner: Arc<dyn BlobBackend>,
        reads: std::sync::atomic::AtomicU64,
    }

    impl CountingBackend {
        fn new(inner: Arc<dyn BlobBackend>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                reads: std::sync::atomic::AtomicU64::new(0),
            })
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

    #[tokio::test]
    async fn peer_serves_clean_page_without_blob_read() {
        // Stage 2: process A populates the cache for a blob; process B (a second
        // cache of the *same* blob_identity in the same dir) serves a read of
        // that page from A's data file without ever reading the blob.
        let dir = tmp_dir("share-read");
        let page = 4096u64;
        let dev = 2 * page;

        // A's own blob, seeded so page 0 has known content.
        let a_inner = Arc::new(MemBackend::new(dev).unwrap());
        a_inner
            .write(0, Bytes::from(vec![0xA7; page as usize]))
            .await
            .unwrap();
        let (a, _) = open_shared(a_inner.clone(), &dir, "volA", "golden", page, dev);

        // A writes page 0 and flushes → it is now clean, resident and published.
        a.write(0, Bytes::from(vec![0xA7; page as usize]))
            .await
            .unwrap();
        a.flush().await.unwrap();

        // B caches the same golden identity but its own (empty) blob whose reads
        // we count.  Its data file starts empty (page 0 absent).
        let b_blob = Arc::new(MemBackend::new(dev).unwrap());
        let b_counting = CountingBackend::new(b_blob);
        let (b, _) = open_shared(b_counting.clone(), &dir, "volB", "golden", page, dev);

        // B reads page 0: it is absent locally but A published it, so B copies
        // A's bytes — with zero reads against B's own blob.
        let got = b.read(0, page).await.unwrap();
        assert_eq!(got, Bytes::from(vec![0xA7; page as usize]));
        assert_eq!(
            b_counting.read_count(),
            0,
            "page should come from peer, not blob"
        );

        // A page no peer published still falls back to the blob (one read).
        let _ = b.read(page, page).await.unwrap();
        assert_eq!(b_counting.read_count(), 1);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn warmup_publishes_clean_pages_for_peers() {
        // Warm-up (prefetch) must publish the clean pages it makes resident so a
        // peer caching the same blob_identity can read them from the warmer's data
        // file — the read-only golden-image use case, where no process ever
        // writes+flushes. Without publishing, the peer would always miss.
        let dir = tmp_dir("share-warmup");
        let page = 4096u64;
        let dev = 2 * page;

        // A's blob, seeded so page 0 has known content; A only *prefetches* it.
        let a_inner = Arc::new(MemBackend::new(dev).unwrap());
        a_inner
            .write(0, Bytes::from(vec![0xC3; page as usize]))
            .await
            .unwrap();
        let (a, _) = open_shared(a_inner.clone(), &dir, "volA", "golden", page, dev);

        // Warm page 0 into A's cache (clean, resident, and — with the fix —
        // published). No write/flush happens.
        a.prefetch(0, page).await.unwrap();
        assert_eq!(a.present_count().await, 1);
        assert_eq!(a.dirty_count().await, 0);

        // B caches the same golden identity but its own (empty) blob whose reads
        // we count.
        let b_blob = Arc::new(MemBackend::new(dev).unwrap());
        let b_counting = CountingBackend::new(b_blob);
        let (b, _) = open_shared(b_counting.clone(), &dir, "volB", "golden", page, dev);

        // B reads page 0 from A's warmed copy — zero reads against B's own blob.
        let got = b.read(0, page).await.unwrap();
        assert_eq!(got, Bytes::from(vec![0xC3; page as usize]));
        assert_eq!(
            b_counting.read_count(),
            0,
            "warmed page should come from peer, not blob"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn cow_writers_do_not_corrupt_each_other() {
        // Stage 3: two caches share a blob_identity.  When each writes the same
        // page, it copies-on-write into its own data file and withdraws the page
        // from the index, so neither observes the other's bytes.
        let dir = tmp_dir("share-cow");
        let page = 4096u64;
        let dev = page;

        let a_inner = Arc::new(MemBackend::new(dev).unwrap());
        let (a, _) = open_shared(a_inner.clone(), &dir, "cowA", "golden", page, dev);
        let b_inner = Arc::new(MemBackend::new(dev).unwrap());
        let (b, _) = open_shared(b_inner.clone(), &dir, "cowB", "golden", page, dev);

        // A writes and flushes page 0, publishing it.
        a.write(0, Bytes::from(vec![0x11; page as usize]))
            .await
            .unwrap();
        a.flush().await.unwrap();

        // B writes its own value to page 0.  Because B dirties the page it must
        // withdraw any inherited copy and keep its write private to its file.
        b.write(0, Bytes::from(vec![0x22; page as usize]))
            .await
            .unwrap();
        b.flush().await.unwrap();

        // Each reads back its own value; neither is corrupted by the other.
        assert_eq!(
            a.read(0, page).await.unwrap(),
            Bytes::from(vec![0x11; page as usize])
        );
        assert_eq!(
            b.read(0, page).await.unwrap(),
            Bytes::from(vec![0x22; page as usize])
        );
        // And each backing blob holds its own value.
        assert_eq!(
            a_inner.read(0, page).await.unwrap(),
            Bytes::from(vec![0x11; page as usize])
        );
        assert_eq!(
            b_inner.read(0, page).await.unwrap(),
            Bytes::from(vec![0x22; page as usize])
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn dirtying_page_withdraws_it_from_peers() {
        // A page a peer is serving must stop being shared the moment its owner
        // dirties it (so peers never read a diverging/torn copy).
        let dir = tmp_dir("share-withdraw");
        let page = 4096u64;
        let dev = page;

        let a_inner = Arc::new(MemBackend::new(dev).unwrap());
        let (a, _) = open_shared(a_inner.clone(), &dir, "wA", "golden", page, dev);
        a.write(0, Bytes::from(vec![0x55; page as usize]))
            .await
            .unwrap();
        a.flush().await.unwrap();

        // B can see A's published page.
        let b_blob = Arc::new(MemBackend::new(dev).unwrap());
        let b_counting = CountingBackend::new(b_blob);
        let (b, _) = open_shared(b_counting.clone(), &dir, "wB", "golden", page, dev);
        assert_eq!(
            b.read(0, page).await.unwrap(),
            Bytes::from(vec![0x55; page as usize])
        );
        assert_eq!(b_counting.read_count(), 0);

        // A dirties the page (without flushing) → it is withdrawn from sharing.
        a.write(0, Bytes::from(vec![0x66; page as usize]))
            .await
            .unwrap();

        // B now misses the peer and falls back to its own blob.
        let _ = b.read(0, page).await.unwrap();
        assert_eq!(
            b_counting.read_count(),
            1,
            "withdrawn page must hit the blob"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn lru_orders_by_recency() {
        let mut lru = Lru::default();
        lru.touch(0, true);
        lru.touch(1, true);
        lru.touch(2, true);
        // Least-recently-used first.
        assert_eq!(lru.pop_lru(None), Some(0));
        assert_eq!(lru.pop_lru(None), Some(1));
        assert_eq!(lru.pop_lru(None), Some(2));
        assert_eq!(lru.pop_lru(None), None);
    }

    #[test]
    fn lru_touch_refreshes_recency() {
        let mut lru = Lru::default();
        lru.touch(0, true);
        lru.touch(1, true);
        lru.touch(2, true);
        // Re-touching page 0 makes it most-recently-used.
        lru.touch(0, true);
        assert_eq!(lru.pop_lru(None), Some(1));
        assert_eq!(lru.pop_lru(None), Some(2));
        assert_eq!(lru.pop_lru(None), Some(0));
    }

    #[test]
    fn lru_dirty_pages_are_not_evictable() {
        let mut lru = Lru::default();
        lru.touch(0, true); // clean
        lru.touch(1, false); // dirty
        lru.touch(2, true); // clean
                            // Page 1 is present but never an eviction candidate.
        assert!(lru.contains(1));
        assert_eq!(lru.pop_lru(None), Some(0));
        assert_eq!(lru.pop_lru(None), Some(2));
        assert_eq!(lru.pop_lru(None), None);
        assert!(lru.contains(1));
    }

    #[test]
    fn lru_reclassifies_between_clean_and_dirty() {
        let mut lru = Lru::default();
        lru.touch(0, true); // evictable
        lru.touch(0, false); // now dirty → no longer evictable
        assert_eq!(lru.pop_lru(None), None);
        lru.touch(0, true); // flushed → evictable again
        assert_eq!(lru.pop_lru(None), Some(0));
    }

    #[test]
    fn lru_pop_skips_protected_page() {
        let mut lru = Lru::default();
        lru.touch(0, true);
        lru.touch(1, true);
        // The LRU page (0) is protected, so page 1 is chosen instead.
        assert_eq!(lru.pop_lru(Some(0)), Some(1));
        // Page 0 is still present; with no protection it is now evictable.
        assert!(lru.contains(0));
        assert_eq!(lru.pop_lru(Some(0)), None);
        assert_eq!(lru.pop_lru(None), Some(0));
    }

    #[test]
    fn lru_clear_empties_all_state() {
        let mut lru = Lru::default();
        lru.touch(0, true);
        lru.touch(1, false);
        lru.clear();
        assert!(!lru.contains(0));
        assert!(!lru.contains(1));
        assert_eq!(lru.pop_lru(None), None);
    }

    #[test]
    fn lru_empty_has_nothing() {
        let mut lru = Lru::default();
        assert!(!lru.contains(0));
        assert!(!lru.contains(u64::MAX));
        assert_eq!(lru.pop_lru(None), None);
        assert_eq!(lru.pop_lru(Some(0)), None);
    }

    #[test]
    fn lru_pop_removes_from_contains() {
        // Popping a page must drop it from the resident set, not just the
        // evictable index (otherwise byte accounting would double-count).
        let mut lru = Lru::default();
        lru.touch(7, true);
        assert!(lru.contains(7));
        assert_eq!(lru.pop_lru(None), Some(7));
        assert!(!lru.contains(7));
        assert_eq!(lru.pop_lru(None), None);
    }

    #[test]
    fn lru_clean_page_is_popped_exactly_once() {
        // Repeated touches of the same clean page collapse to a single resident,
        // single-evictable entry.
        let mut lru = Lru::default();
        lru.touch(3, true);
        lru.touch(3, true);
        lru.touch(3, true);
        assert_eq!(lru.pop_lru(None), Some(3));
        assert_eq!(lru.pop_lru(None), None);
        assert!(!lru.contains(3));
    }

    #[test]
    fn lru_dirty_retouch_stays_non_evictable() {
        // Re-touching a dirty page (e.g. a second write before flush) updates its
        // recency but it must never become an eviction candidate.
        let mut lru = Lru::default();
        lru.touch(0, false);
        lru.touch(0, false);
        assert!(lru.contains(0));
        assert_eq!(lru.pop_lru(None), None);
    }

    #[test]
    fn lru_protecting_dirty_page_still_evicts_clean() {
        // Protecting a page that isn't evictable anyway is a no-op; the clean LRU
        // page is still chosen.
        let mut lru = Lru::default();
        lru.touch(0, false); // dirty
        lru.touch(1, true); // clean
        assert_eq!(lru.pop_lru(Some(0)), Some(1));
        assert_eq!(lru.pop_lru(None), None);
        assert!(lru.contains(0));
    }

    #[test]
    fn lru_clear_allows_fresh_reuse() {
        // After clear, the recency clock restarts and ordering is correct again.
        let mut lru = Lru::default();
        lru.touch(5, true);
        lru.touch(6, true);
        lru.clear();
        lru.touch(2, true);
        lru.touch(1, true);
        // Insertion order after clear: 2 then 1 → 2 is least-recently-used.
        assert_eq!(lru.pop_lru(None), Some(2));
        assert_eq!(lru.pop_lru(None), Some(1));
        assert_eq!(lru.pop_lru(None), None);
    }

    #[test]
    fn lru_mixed_workload_order() {
        // Comprehensive interleaving: clean inserts, a dirty page, a flush
        // (dirty→clean), a re-touch, and a protected pop.
        let mut lru = Lru::default();
        lru.touch(0, true); // clean, oldest
        lru.touch(1, false); // dirty (pinned)
        lru.touch(2, true); // clean
        lru.touch(3, true); // clean
        lru.touch(0, true); // re-touch 0 → now newest clean
        lru.touch(1, true); // flush page 1 → becomes evictable (newest)

        // Evictable clean pages by recency: 2, 3, 0, 1.
        assert_eq!(lru.pop_lru(Some(2)), Some(3)); // 2 protected → next LRU is 3
        assert_eq!(lru.pop_lru(None), Some(2));
        assert_eq!(lru.pop_lru(None), Some(0));
        assert_eq!(lru.pop_lru(None), Some(1));
        assert_eq!(lru.pop_lru(None), None);
        assert!(!lru.contains(0));
        assert!(!lru.contains(1));
    }
}
