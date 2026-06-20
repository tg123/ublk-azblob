//! Cross-process, crash-safe page index for sharing clean cache pages.
//!
//! Several `ublk-azblob` processes (for example one CSI node serving many
//! volumes that are copy-on-write clones of the same golden image) can share a
//! single `--cache-dir`.  When two of them cache the *same logical blob* — the
//! same `blob_identity` — a page that one process has already fetched from the
//! blob and written clean to its own data file can be served to a peer by
//! copying it straight off local disk, avoiding a redundant network round-trip.
//!
//! `CacheIndex` is the shared directory that makes this possible.  It is the
//! read-path counterpart to [`CacheBudget`](super::cache_budget::CacheBudget)
//! and uses the exact same cross-process discipline (an advisory
//! `flock(LOCK_EX)` over a small state file plus an in-process mutex, with
//! dead-PID pruning for crash safety).
//!
//! # On-disk layout
//!
//! A single `.cache-index` file in the cache directory records, one line per
//! published page:
//!
//! ```text
//! <blob_identity>\t<page_idx>\t<owner>\t<pid>\t<len>
//! ```
//!
//! `owner` is the publishing cache's file base name, so the page's bytes live
//! at offset `page_idx * page_size` in `<dir>/<owner>.dat`.  Only **clean**
//! (already-flushed) pages are ever published — a dirty page is private to its
//! writer until it is flushed, preserving the single-writer-per-file invariant.
//!
//! # Crash safety
//!
//! Like the budget, the index never *leaks* across a crash: every entry records
//! the publisher's `pid`, and whenever the file is locked, entries whose process
//! is no longer alive (`kill(pid, 0)` → `ESRCH`) are pruned.  A clean shutdown
//! removes the owner's entries via [`Drop`].  The file is purely advisory:
//! losing it (or an individual entry) only forgoes a sharing opportunity and
//! falls back to fetching the page from the blob — it never affects correctness.
//!
//! # Race safety
//!
//! [`CacheIndex::read_peer_page`] reads a peer's data file **while holding the
//! `flock`**.  Eviction ([`CacheIndex::unpublish`]) and copy-on-write both take
//! the same lock before they punch a hole in, or overwrite, a published page, so
//! a peer can never observe a torn or hole-punched page mid-read.

use super::cache_budget::{pid_alive, FlockGuard};
use anyhow::{Context as _, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Name of the shared page-index state file inside the cache directory.
const STATE_FILE: &str = ".cache-index";

/// One published clean page.
struct Entry {
    blob_identity: String,
    page_idx: u64,
    owner: String,
    pid: u32,
    len: u64,
}

/// A shared, crash-safe directory of clean cache pages for one cache directory.
pub struct CacheIndex {
    dir: PathBuf,
    owner: String,
    blob_identity: String,
    pid: u32,
    page_size: u64,
    /// The state file, used both for its contents and as the `flock` target.
    /// The [`Mutex`] serializes access *within* this process; `flock` serializes
    /// it *across* processes.
    file: Mutex<File>,
}

impl CacheIndex {
    /// Open (or create) the shared page index for `dir`.
    ///
    /// `owner` is this cache's file base name (so its data file is
    /// `<dir>/<owner>.dat`) and must be unique per instance within the
    /// directory; `blob_identity` is the shared key under which clean pages are
    /// published and looked up.  Neither may contain a tab or newline.
    pub fn open(dir: &Path, owner: &str, blob_identity: &str, page_size: u64) -> Result<Self> {
        for (what, s) in [("owner", owner), ("blob_identity", blob_identity)] {
            if s.contains('\t') || s.contains('\n') {
                anyhow::bail!("cache index {what} must not contain tab or newline: {s:?}");
            }
        }

        let path = dir.join(STATE_FILE);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .with_context(|| format!("open cache index file {}", path.display()))?;

        let index = Self {
            dir: dir.to_path_buf(),
            owner: owner.to_string(),
            blob_identity: blob_identity.to_string(),
            pid: std::process::id(),
            page_size,
            file: Mutex::new(file),
        };

        // Prune any dead peers' leftovers on startup.
        index.with_locked_mut(|_| {})?;
        Ok(index)
    }

    /// Advertise that this owner holds `page_idx` clean on local disk, so peers
    /// caching the same `blob_identity` may read it instead of the blob.
    pub fn publish(&self, page_idx: u64, len: u64) -> Result<()> {
        self.with_locked_mut(|entries| {
            upsert(
                entries,
                &self.blob_identity,
                page_idx,
                &self.owner,
                self.pid,
                len,
            );
        })
    }

    /// Withdraw `page_idx` from the index (the page was evicted or is being
    /// dirtied by this owner and may no longer be read by peers).
    pub fn unpublish(&self, page_idx: u64) -> Result<()> {
        self.with_locked_mut(|entries| {
            entries.retain(|e| {
                !(e.owner == self.owner
                    && e.pid == self.pid
                    && e.blob_identity == self.blob_identity
                    && e.page_idx == page_idx)
            });
        })
    }

    /// Withdraw every page this owner has published (used on reinit/delete).
    pub fn unpublish_all(&self) -> Result<()> {
        self.with_locked_mut(|entries| {
            entries.retain(|e| !(e.owner == self.owner && e.pid == self.pid));
        })
    }

    /// Try to serve `page_idx` of this cache's `blob_identity` from a live
    /// **peer's** local data file into `buf` (which must be `page_len` long).
    ///
    /// Returns `Ok(true)` when a peer supplied the page, `Ok(false)` when no
    /// live peer has it (the caller must then fetch it from the blob).  The
    /// peer's file is read while the index lock is held so the peer cannot
    /// concurrently evict or overwrite the page (both of which take the lock).
    pub fn read_peer_page(&self, page_idx: u64, buf: &mut [u8]) -> Result<bool> {
        self.with_locked_read(|entries| -> Result<bool> {
            // Find a live peer (a different owner/pid) that published this page
            // with a matching length.  Self entries are skipped: a present page
            // is served from our own file, never via the index.
            let Some(peer) = entries.iter().find(|e| {
                e.blob_identity == self.blob_identity
                    && e.page_idx == page_idx
                    && !(e.owner == self.owner && e.pid == self.pid)
                    && e.len == buf.len() as u64
            }) else {
                return Ok(false);
            };

            let path = self.dir.join(format!("{}.dat", peer.owner));
            let file = match File::open(&path) {
                Ok(f) => f,
                // A missing peer file is not fatal: fall back to the blob.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
                Err(e) => {
                    return Err(e)
                        .with_context(|| format!("open peer cache file {}", path.display()))
                }
            };
            use std::os::unix::fs::FileExt as _;
            let offset = page_idx * self.page_size;
            match file.read_exact_at(buf, offset) {
                Ok(()) => Ok(true),
                // The peer's page is shorter than advertised (truncated, or the
                // blocks were reclaimed by an external hole-punch). Treat it as
                // "peer cannot supply this page" and fall back to the blob rather
                // than failing the read with stale/invalid bytes.
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
                Err(e) => Err(e)
                    .with_context(|| format!("read peer page {page_idx} from {}", path.display())),
            }
        })?
    }

    /// Number of entries this owner currently has published (test helper).
    #[cfg(test)]
    pub fn published_count(&self) -> Result<usize> {
        self.with_locked_read(|entries| {
            entries
                .iter()
                .filter(|e| e.owner == self.owner && e.pid == self.pid)
                .count()
        })
    }

    /// Run `f` against the parsed entries while holding both the in-process
    /// mutex and an exclusive `flock`, pruning dead owners first and persisting
    /// the (possibly mutated) entries afterwards. For **mutating** operations
    /// (publish/unpublish).
    fn with_locked_mut<R>(&self, f: impl FnOnce(&mut Vec<Entry>) -> R) -> Result<R> {
        let mut file = self.file.lock().expect("cache index mutex poisoned");
        let _guard = FlockGuard::acquire(&file)?;

        let mut entries = read_entries(&mut file)?;
        prune_dead(&mut entries, self.pid);
        let result = f(&mut entries);
        write_entries(&mut file, &entries)?;
        Ok(result)
    }

    /// Like [`with_locked_mut`] but for **read-only** operations
    /// (`read_peer_page`, `published_count`): the index file is **not** rewritten,
    /// which matters because it holds one line per published *page* and these run
    /// on every cross-process cache miss. Dead-owner pruning is applied in-memory
    /// only; it is persisted by the next mutating operation.
    fn with_locked_read<R>(&self, f: impl FnOnce(&[Entry]) -> R) -> Result<R> {
        let mut file = self.file.lock().expect("cache index mutex poisoned");
        let _guard = FlockGuard::acquire(&file)?;

        let mut entries = read_entries(&mut file)?;
        prune_dead(&mut entries, self.pid);
        Ok(f(&entries))
    }
}

impl Drop for CacheIndex {
    fn drop(&mut self) {
        // Best-effort: forget all our pages so peers stop trying to read our
        // (about-to-vanish) data file.  A crash is handled by dead-pid pruning.
        let _ = self.unpublish_all();
    }
}

/// Insert or update this owner's entry for `(blob_identity, page_idx)`.
fn upsert(
    entries: &mut Vec<Entry>,
    blob_identity: &str,
    page_idx: u64,
    owner: &str,
    pid: u32,
    len: u64,
) {
    if let Some(e) = entries.iter_mut().find(|e| {
        e.owner == owner
            && e.pid == pid
            && e.blob_identity == blob_identity
            && e.page_idx == page_idx
    }) {
        e.len = len;
        return;
    }
    entries.push(Entry {
        blob_identity: blob_identity.to_string(),
        page_idx,
        owner: owner.to_string(),
        pid,
        len,
    });
}

/// Drop entries whose process is no longer alive (keeping our own unconditionally).
fn prune_dead(entries: &mut Vec<Entry>, self_pid: u32) {
    entries.retain(|e| e.pid == self_pid || pid_alive(e.pid));
}

/// Parse the whole state file into entries, ignoring malformed lines.
fn read_entries(file: &mut File) -> Result<Vec<Entry>> {
    file.seek(SeekFrom::Start(0)).context("seek index file")?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).context("read index file")?;

    let mut entries = Vec::new();
    for line in buf.lines() {
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            continue;
        }
        let mut parts = line.split('\t');
        let (Some(blob), Some(page), Some(owner), Some(pid), Some(len)) = (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) else {
            continue;
        };
        let (Ok(page_idx), Ok(pid), Ok(len)) =
            (page.parse::<u64>(), pid.parse::<u32>(), len.parse::<u64>())
        else {
            continue;
        };
        entries.push(Entry {
            blob_identity: blob.to_string(),
            page_idx,
            owner: owner.to_string(),
            pid,
            len,
        });
    }
    Ok(entries)
}

/// Overwrite the state file with `entries` (truncating any previous content).
fn write_entries(file: &mut File, entries: &[Entry]) -> Result<()> {
    let mut out = String::new();
    for e in entries {
        out.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\n",
            e.blob_identity, e.page_idx, e.owner, e.pid, e.len
        ));
    }
    file.set_len(0).context("truncate index file")?;
    file.seek(SeekFrom::Start(0)).context("seek index file")?;
    file.write_all(out.as_bytes()).context("write index file")?;
    file.flush().context("flush index file")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::FileExt as _;

    fn tmp_dir(tag: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "ublk-index-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    /// Create a `<owner>.dat` of `pages` pages filled so page `i` is all `0xC0+i`.
    fn seed_data_file(dir: &Path, owner: &str, page_size: u64, pages: u64) {
        let path = dir.join(format!("{owner}.dat"));
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .unwrap();
        f.set_len(page_size * pages).unwrap();
        for i in 0..pages {
            let buf = vec![0xC0u8 + i as u8; page_size as usize];
            f.write_all_at(&buf, i * page_size).unwrap();
        }
    }

    #[test]
    fn peer_read_serves_published_clean_page() {
        let dir = tmp_dir("peerread");
        let page = 4096u64;
        seed_data_file(&dir, "a", page, 2);

        let a = CacheIndex::open(&dir, "a", "blob", page).unwrap();
        let b = CacheIndex::open(&dir, "b", "blob", page).unwrap();

        // No peer has published page 0 yet.
        let mut buf = vec![0u8; page as usize];
        assert!(!b.read_peer_page(0, &mut buf).unwrap());

        // a publishes page 0; b can now read a's copy.
        a.publish(0, page).unwrap();
        assert!(b.read_peer_page(0, &mut buf).unwrap());
        assert!(buf.iter().all(|&x| x == 0xC0));
    }

    #[test]
    fn different_blob_identity_does_not_share() {
        let dir = tmp_dir("isolate");
        let page = 4096u64;
        seed_data_file(&dir, "a", page, 1);
        let a = CacheIndex::open(&dir, "a", "blobA", page).unwrap();
        let b = CacheIndex::open(&dir, "b", "blobB", page).unwrap();
        a.publish(0, page).unwrap();
        // b caches a different blob, so a's page must not be visible to it.
        let mut buf = vec![0u8; page as usize];
        assert!(!b.read_peer_page(0, &mut buf).unwrap());
    }

    #[test]
    fn own_entries_are_not_served_to_self() {
        let dir = tmp_dir("self");
        let page = 4096u64;
        seed_data_file(&dir, "a", page, 1);
        let a = CacheIndex::open(&dir, "a", "blob", page).unwrap();
        a.publish(0, page).unwrap();
        // A present page is served from our own file, never via the index.
        let mut buf = vec![0u8; page as usize];
        assert!(!a.read_peer_page(0, &mut buf).unwrap());
    }

    #[test]
    fn unpublish_withdraws_page() {
        let dir = tmp_dir("unpub");
        let page = 4096u64;
        seed_data_file(&dir, "a", page, 1);
        let a = CacheIndex::open(&dir, "a", "blob", page).unwrap();
        let b = CacheIndex::open(&dir, "b", "blob", page).unwrap();
        a.publish(0, page).unwrap();
        let mut buf = vec![0u8; page as usize];
        assert!(b.read_peer_page(0, &mut buf).unwrap());
        a.unpublish(0).unwrap();
        assert!(!b.read_peer_page(0, &mut buf).unwrap());
    }

    #[test]
    fn length_mismatch_is_not_served() {
        // A short final page published with a different length must not satisfy
        // a full-page request.
        let dir = tmp_dir("len");
        let page = 4096u64;
        seed_data_file(&dir, "a", page, 1);
        let a = CacheIndex::open(&dir, "a", "blob", page).unwrap();
        let b = CacheIndex::open(&dir, "b", "blob", page).unwrap();
        a.publish(0, 512).unwrap();
        let mut buf = vec![0u8; page as usize];
        assert!(!b.read_peer_page(0, &mut buf).unwrap());
    }

    #[test]
    fn dead_peer_entries_are_pruned() {
        let dir = tmp_dir("deadpeer");
        let page = 4096u64;
        // Hand-write an entry for a definitely-dead pid.
        std::fs::write(dir.join(STATE_FILE), "blob\t0\tghost\t999999999\t4096\n").unwrap();
        let b = CacheIndex::open(&dir, "b", "blob", page).unwrap();
        let mut buf = vec![0u8; page as usize];
        // The ghost is dead, so its page is pruned and not served.
        assert!(!b.read_peer_page(0, &mut buf).unwrap());
    }

    #[test]
    fn drop_withdraws_all_pages() {
        let dir = tmp_dir("drop");
        let page = 4096u64;
        seed_data_file(&dir, "a", page, 2);
        let b = CacheIndex::open(&dir, "b", "blob", page).unwrap();
        {
            let a = CacheIndex::open(&dir, "a", "blob", page).unwrap();
            a.publish(0, page).unwrap();
            a.publish(1, page).unwrap();
            assert_eq!(a.published_count().unwrap(), 2);
        } // a dropped → its entries withdrawn
        let mut buf = vec![0u8; page as usize];
        assert!(!b.read_peer_page(0, &mut buf).unwrap());
        assert!(!b.read_peer_page(1, &mut buf).unwrap());
    }
}
