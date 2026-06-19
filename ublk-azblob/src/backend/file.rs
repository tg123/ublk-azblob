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

use super::BlobBackend;
use anyhow::{bail, Context as _};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
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
}

impl Default for FileCacheConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("."),
            name: "ublk-azblob-cache".to_string(),
            page_size: 1024 * 1024, // 1 MiB
        }
    }
}

/// Persistent local-disk page cache wrapping any [`BlobBackend`].
pub struct FileCacheBackend {
    inner: std::sync::Arc<dyn BlobBackend>,
    page_size: u64,
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

        let state = CacheState {
            data,
            meta,
            dev_size,
            num_pages,
            present,
            dirty,
        };

        Ok((
            Self {
                inner,
                page_size: cfg.page_size,
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

        let state = self.state.lock().await;
        check_in_bounds("read", offset, len, state.dev_size)?;

        let page_size = self.page_size;
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

    async fn size(&self) -> anyhow::Result<u64> {
        let state = self.state.lock().await;
        Ok(state.dev_size)
    }

    async fn snapshot(&self) -> anyhow::Result<String> {
        // Flush cached dirty pages so the snapshot reflects the latest data.
        self.flush().await?;
        self.inner.snapshot().await
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
        FileCacheBackend::open(
            inner,
            FileCacheConfig {
                dir: dir.to_path_buf(),
                name: "test".to_string(),
                page_size,
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
}
