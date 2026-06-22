//! In-memory LRU bookkeeping for resident cache pages.
//!
//! This small structure is shared by every cache *level* in the backend stack:
//! the persistent local-disk cache ([`FileCacheBackend`](super::file::FileCacheBackend))
//! and the in-memory write-back buffer
//! ([`BufferedBackend`](super::buffered::BufferedBackend)). Each level stores
//! page *bytes* elsewhere (a sparse data file on disk, or a `BytesMut` per page
//! in memory) and uses an [`Lru`] only to order page *indices* for eviction.
//!
//! It tracks the recency of every present page and which of them are
//! *evictable* (present **and** clean — dirty pages must be flushed, never
//! dropped).
//!
//! # Why a purpose-built structure instead of an off-the-shelf LRU crate
//!
//! Off-the-shelf caches (`lru`, `clru`, `quick_cache`, `moka`, `foyer`, …) own
//! the cached *values* and evict purely by recency (optionally by a byte
//! weight).  This cache is structurally different on two axes that none of them
//! model:
//!
//! - **Values live outside the map.**  Page bytes are stored in a sparse data
//!   file (disk level) or a per-page buffer (memory level), addressed by page
//!   index; eviction is a `PUNCH_HOLE` plus a bitmap-bit clear (disk) or a map
//!   removal (memory), so the LRU only needs to order page *indices*.
//! - **A pinned, non-evictable subset.**  Dirty (unflushed) pages must never be
//!   dropped or data is lost; generic LRUs have no notion of "evict by recency,
//!   but only among the clean pages".  Here `evictable` is a second index that
//!   excludes dirty pages, and pages move in/out of it as they are
//!   dirtied/flushed.
//!
//! The structure is intentionally small: a monotonic recency clock plus two
//! indexes giving O(log n) insert/touch and O(1)-amortised LRU selection.

use std::collections::{BTreeMap, HashMap};

#[derive(Default)]
pub(crate) struct Lru {
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
    pub(crate) fn contains(&self, page: u64) -> bool {
        self.by_page.contains_key(&page)
    }

    /// Record an access to `page`, (re)classifying it as evictable iff `clean`.
    pub(crate) fn touch(&mut self, page: u64, clean: bool) {
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
    pub(crate) fn pop_lru(&mut self, protect: Option<u64>) -> Option<u64> {
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
    pub(crate) fn clear(&mut self) {
        self.seq = 0;
        self.by_page.clear();
        self.evictable.clear();
    }
}
