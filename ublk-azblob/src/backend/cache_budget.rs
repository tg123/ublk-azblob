//! Cross-process, crash-safe byte budget for the local-disk page cache.
//!
//! Several `ublk-azblob` processes (for example one CSI node serving many
//! volumes) can share a single `--cache-dir`.  Without a shared budget each
//! [`FileCacheBackend`](super::file::FileCacheBackend) would grow its cache file
//! independently, so a single noisy volume could fill the disk while nothing
//! evicts.  `CacheBudget` gives them one **node-wide byte limit** to share.
//!
//! # How it works
//!
//! A tiny state file (`.cache-budget`) in the cache directory records, one line
//! per owner, how many bytes of cached page data that owner currently holds:
//!
//! ```text
//! <owner>\t<pid>\t<bytes>
//! ```
//!
//! Every read-modify-write of the file is serialized across processes with an
//! advisory `flock(LOCK_EX)`.  The global resident total is just the sum of all
//! live owners' byte counts; [`CacheBudget::admit`] returns how far over the
//! limit the system now is so the caller can shed (evict) that many bytes of its
//! own clean pages.
//!
//! # Crash safety
//!
//! The budget never *leaks* across a crash: each owner records its `pid`, and
//! whenever the file is locked, entries whose process is no longer alive
//! (`kill(pid, 0)` → `ESRCH`) are pruned and their bytes reclaimed.  A clean
//! shutdown removes the owner's entry via [`Drop`].  The file is therefore
//! purely advisory — losing it (or any individual update) only loses accounting
//! accuracy, never correctness of the cached data itself.
//!
//! # Scope
//!
//! A process only ever evicts *its own* clean pages via this budget: it never
//! mutates another process's cache files, so each backend stays the sole
//! authority over its own `present` bitmap and data file.  The shared total
//! therefore bounds the aggregate footprint of *active* processes; a fully idle
//! peer keeps its resident pages until it does I/O again or exits.  Sharing the
//! actual cached page data between processes (so a read miss can be served from a
//! peer's data file) is layered on top by [`super::cache_index::CacheIndex`].

use anyhow::{Context as _, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::os::unix::io::AsRawFd as _;
use std::path::Path;
use std::sync::Mutex;

/// Name of the shared budget state file inside the cache directory.
const STATE_FILE: &str = ".cache-budget";

/// One owner's accounting record.
struct Entry {
    owner: String,
    pid: u32,
    bytes: u64,
}

/// A shared, crash-safe byte budget for one cache directory.
pub struct CacheBudget {
    max_bytes: u64,
    owner: String,
    pid: u32,
    /// The state file, used both for its contents and as the `flock` target.
    /// The [`Mutex`] serializes access *within* this process; `flock` serializes
    /// it *across* processes.
    file: Mutex<File>,
}

impl CacheBudget {
    /// Open (or create) the shared budget for `dir`, registering `owner` with a
    /// zero initial balance.  Returns `Ok(None)` when `max_bytes == 0`
    /// (unlimited), in which case the caller skips all budget/eviction logic.
    ///
    /// `owner` must be unique per cache within the directory (the cache file
    /// base name is used) and must not contain tab or newline characters.
    pub fn open(dir: &Path, owner: &str, max_bytes: u64) -> Result<Option<Self>> {
        if max_bytes == 0 {
            return Ok(None);
        }
        if owner.contains('\t') || owner.contains('\n') {
            anyhow::bail!("cache budget owner must not contain tab or newline: {owner:?}");
        }

        let path = dir.join(STATE_FILE);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .with_context(|| format!("open cache budget file {}", path.display()))?;

        let budget = Self {
            max_bytes,
            owner: owner.to_string(),
            pid: std::process::id(),
            file: Mutex::new(file),
        };

        // Register a fresh zero-balance entry, pruning any dead peers' leftovers.
        budget.with_locked(|entries| {
            let e = upsert(entries, &budget.owner, budget.pid);
            e.bytes = 0;
        })?;

        Ok(Some(budget))
    }

    /// Set this owner's resident balance to exactly `bytes` (used at startup
    /// after counting pages recovered from disk).  Returns how many bytes the
    /// system is now over the limit (0 if within budget).
    pub fn reset(&self, bytes: u64) -> Result<u64> {
        self.with_locked(|entries| {
            let e = upsert(entries, &self.owner, self.pid);
            e.bytes = bytes;
            total(entries).saturating_sub(self.max_bytes)
        })
    }

    /// Account `bytes` of newly resident page data for this owner and return how
    /// many bytes the system is now over the global limit (0 if within budget).
    /// A positive result asks the caller to evict that many bytes of its own
    /// clean pages and report them via [`CacheBudget::release`].
    pub fn admit(&self, bytes: u64) -> Result<u64> {
        self.with_locked(|entries| {
            let e = upsert(entries, &self.owner, self.pid);
            e.bytes = e.bytes.saturating_add(bytes);
            total(entries).saturating_sub(self.max_bytes)
        })
    }

    /// Account `bytes` of page data this owner has dropped (evicted) from disk.
    pub fn release(&self, bytes: u64) -> Result<()> {
        self.with_locked(|entries| {
            let e = upsert(entries, &self.owner, self.pid);
            e.bytes = e.bytes.saturating_sub(bytes);
        })
    }

    /// The configured global limit in bytes.
    #[cfg(test)]
    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// Read the current global resident total across all live owners.
    #[cfg(test)]
    pub fn global_total(&self) -> Result<u64> {
        self.with_locked(|entries| total(entries))
    }

    /// Run `f` against the parsed entries while holding both the in-process mutex
    /// and an exclusive `flock`, pruning dead owners first and persisting the
    /// (possibly mutated) entries afterwards.
    fn with_locked<R>(&self, f: impl FnOnce(&mut Vec<Entry>) -> R) -> Result<R> {
        let mut file = self.file.lock().expect("cache budget mutex poisoned");
        let _guard = FlockGuard::acquire(&file)?;

        let mut entries = read_entries(&mut file)?;
        prune_dead(&mut entries, self.pid);
        let result = f(&mut entries);
        write_entries(&mut file, &entries)?;
        Ok(result)
    }
}

impl Drop for CacheBudget {
    fn drop(&mut self) {
        // Best-effort: remove our entry so a clean shutdown frees our budget
        // immediately (a crash is handled by dead-pid pruning instead).
        let _ = self.with_locked(|entries| {
            entries.retain(|e| !(e.owner == self.owner && e.pid == self.pid));
        });
    }
}

/// Sum of all owners' resident byte counts.
fn total(entries: &[Entry]) -> u64 {
    entries
        .iter()
        .fold(0u64, |acc, e| acc.saturating_add(e.bytes))
}

/// Find this owner's entry, inserting a fresh zero-balance one if absent.
fn upsert<'a>(entries: &'a mut Vec<Entry>, owner: &str, pid: u32) -> &'a mut Entry {
    if let Some(idx) = entries
        .iter()
        .position(|e| e.owner == owner && e.pid == pid)
    {
        return &mut entries[idx];
    }
    entries.push(Entry {
        owner: owner.to_string(),
        pid,
        bytes: 0,
    });
    entries.last_mut().expect("just pushed")
}

/// Drop entries whose process is no longer alive (keeping our own unconditionally).
fn prune_dead(entries: &mut Vec<Entry>, self_pid: u32) {
    entries.retain(|e| e.pid == self_pid || pid_alive(e.pid));
}

/// Whether `pid` refers to a live process, via `kill(pid, 0)`.
///
/// `0`/`EPERM` mean the process exists; `ESRCH` means it does not.  Any other
/// error is treated conservatively as "alive" so we never reclaim a peer's
/// budget by mistake.
pub(super) fn pid_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    let ret = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if ret == 0 {
        return true;
    }
    // `ESRCH` means the process is gone; any other error (e.g. `EPERM`) means it
    // exists, so treat everything but `ESRCH` as alive.
    !matches!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    )
}

/// Parse the whole state file into entries, ignoring malformed lines.
fn read_entries(file: &mut File) -> Result<Vec<Entry>> {
    file.seek(SeekFrom::Start(0)).context("seek budget file")?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).context("read budget file")?;

    let mut entries = Vec::new();
    for line in buf.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut parts = line.split('\t');
        let (Some(owner), Some(pid), Some(bytes)) = (parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        let (Ok(pid), Ok(bytes)) = (pid.parse::<u32>(), bytes.parse::<u64>()) else {
            continue;
        };
        entries.push(Entry {
            owner: owner.to_string(),
            pid,
            bytes,
        });
    }
    Ok(entries)
}

/// Overwrite the state file with `entries` (truncating any previous content).
fn write_entries(file: &mut File, entries: &[Entry]) -> Result<()> {
    let mut out = String::new();
    for e in entries {
        out.push_str(&format!("{}\t{}\t{}\n", e.owner, e.pid, e.bytes));
    }
    file.set_len(0).context("truncate budget file")?;
    file.seek(SeekFrom::Start(0)).context("seek budget file")?;
    file.write_all(out.as_bytes())
        .context("write budget file")?;
    file.flush().context("flush budget file")?;
    Ok(())
}

/// RAII wrapper around an exclusive advisory `flock` on a state file.
///
/// Shared with [`super::cache_index`] so both the byte budget and the page
/// index serialize cross-process access the same way.
pub(super) struct FlockGuard {
    fd: libc::c_int,
}

impl FlockGuard {
    pub(super) fn acquire(file: &File) -> Result<Self> {
        let fd = file.as_raw_fd();
        let ret = unsafe { libc::flock(fd, libc::LOCK_EX) };
        if ret != 0 {
            return Err(std::io::Error::last_os_error()).context("flock cache state file");
        }
        Ok(Self { fd })
    }
}

impl Drop for FlockGuard {
    fn drop(&mut self) {
        unsafe {
            libc::flock(self.fd, libc::LOCK_UN);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(tag: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "ublk-budget-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn unlimited_returns_none() {
        let dir = tmp_dir("unlimited");
        assert!(CacheBudget::open(&dir, "a", 0).unwrap().is_none());
    }

    #[test]
    fn admit_and_release_track_over_budget() {
        let dir = tmp_dir("track");
        let b = CacheBudget::open(&dir, "vol", 1000).unwrap().unwrap();
        assert_eq!(b.admit(400).unwrap(), 0);
        assert_eq!(b.admit(400).unwrap(), 0);
        // 800 + 400 = 1200 → 200 over the 1000 limit.
        assert_eq!(b.admit(400).unwrap(), 200);
        assert_eq!(b.global_total().unwrap(), 1200);
        b.release(300).unwrap();
        assert_eq!(b.global_total().unwrap(), 900);
        assert_eq!(b.max_bytes(), 1000);
    }

    #[test]
    fn release_saturates_at_zero() {
        let dir = tmp_dir("sat");
        let b = CacheBudget::open(&dir, "vol", 1000).unwrap().unwrap();
        b.admit(100).unwrap();
        b.release(500).unwrap();
        assert_eq!(b.global_total().unwrap(), 0);
    }

    #[test]
    fn two_owners_share_one_budget() {
        let dir = tmp_dir("shared");
        let a = CacheBudget::open(&dir, "a", 1000).unwrap().unwrap();
        let c = CacheBudget::open(&dir, "c", 1000).unwrap().unwrap();
        assert_eq!(a.admit(600).unwrap(), 0);
        // c sees a's 600 already counted: 600 + 600 = 1200 → 200 over.
        assert_eq!(c.admit(600).unwrap(), 200);
        assert_eq!(a.global_total().unwrap(), 1200);
    }

    #[test]
    fn drop_releases_owner_balance() {
        let dir = tmp_dir("drop");
        let a = CacheBudget::open(&dir, "a", 1000).unwrap().unwrap();
        a.admit(500).unwrap();
        {
            let c = CacheBudget::open(&dir, "c", 1000).unwrap().unwrap();
            c.admit(300).unwrap();
            assert_eq!(a.global_total().unwrap(), 800);
        } // c dropped → its 300 released
        assert_eq!(a.global_total().unwrap(), 500);
    }

    #[test]
    fn reset_sets_absolute_balance() {
        let dir = tmp_dir("reset");
        let b = CacheBudget::open(&dir, "vol", 1000).unwrap().unwrap();
        b.admit(900).unwrap();
        assert_eq!(b.reset(200).unwrap(), 0);
        assert_eq!(b.global_total().unwrap(), 200);
        assert_eq!(b.reset(1500).unwrap(), 500);
    }

    #[test]
    fn dead_owner_entries_are_pruned() {
        let dir = tmp_dir("prune");
        // Hand-write an entry for a definitely-dead pid.
        let path = dir.join(STATE_FILE);
        std::fs::write(&path, "ghost\t999999999\t5000\n").unwrap();
        let b = CacheBudget::open(&dir, "vol", 1000).unwrap().unwrap();
        // The ghost's 5000 bytes must be reclaimed, leaving only our 0.
        assert_eq!(b.global_total().unwrap(), 0);
    }
}
