//! Cluster coordination: combine an **Azure blob lease** ("blob lock") with an
//! optional **Kubernetes `coordination.k8s.io` Lease** ("cluster lease") so that
//! at most one node mounts a given page blob at a time.
//!
//! The blob lease is the authoritative, storage-level lock and is used in **both**
//! modes:
//!
//! * **Single-process mode** (no cluster lease): the [`Coordinator`] acquires and
//!   renews only the blob lease.  If another process already holds it, mounting is
//!   refused — there is no liveness arbiter, so a held lease is never broken.
//! * **Cluster mode** (with a cluster lease): the blob lease is paired with the
//!   Kubernetes lease, which adds the liveness-based take-over protocol below.
//!
//! ## Why two locks?
//!
//! * The **blob lease** is the authoritative, storage-level lock: while it is
//!   held no other client can write to the page blob, so it prevents two live
//!   nodes from corrupting the blob even across a network partition.
//! * The **cluster lease** is a *liveness* signal.  Its `renewTime` is refreshed
//!   while the holder is alive and mounting/uploading/recovering.  If a node
//!   dies hard (kernel panic, power loss) its blob lease may still be "held"
//!   from Azure's point of view until it expires, but its cluster lease stops
//!   being renewed.  Another node can then observe that the holder is stale
//!   (older than the *recovery timeout*), take the cluster lease, **break** the
//!   stale blob lease and acquire it for itself.
//!
//! ## Multi-cluster note
//!
//! The Kubernetes cluster lease is namespaced/named within one cluster, so it
//! only coordinates nodes of the *same* cluster. If several clusters share one
//! storage account, give each cluster a dedicated blob path prefix (a "folder",
//! e.g. via the StorageClass blob-path template `mycluster/${pvc.namespace}/...`)
//! so their volumes — and therefore their blob leases — never collide. Two
//! clusters pointed at the *same* blob would only be mutually excluded by the
//! Azure blob lease (no cross-cluster liveness signal), which is far weaker.
//!
//! ## Startup algorithm ([`Coordinator::acquire`])
//!
//! 1. Acquire (or take over) the **cluster lease** for our holder identity.
//!    If another holder is still fresh (renewed within the recovery timeout) we
//!    refuse to mount — another machine is alive and owns the volume.
//! 2. Acquire the **blob lease**.  If it is already held (the previous holder
//!    died without releasing it) we — having already won the cluster lease and
//!    therefore established that the previous holder is past the recovery
//!    timeout — **break** the blob lease and acquire it for ourselves.
//! 3. Start a background **renewal loop** that keeps both leases fresh, and
//!    return a [`CoordinationGuard`].  On clean shutdown the guard releases both
//!    leases.
//!
//! The two backends are abstracted behind the [`BlobLock`] and [`ClusterLease`]
//! traits so the orchestration can be unit-tested with in-memory fakes (see the
//! tests at the bottom of this file) without a real cluster or Azure account.

#[cfg(feature = "coordination")]
pub mod k8s;

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context as _};
use async_trait::async_trait;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// Default Azure blob lease duration.  Azure caps a *finite* lease at 60s; the
/// renewal loop refreshes it well before it expires.
pub const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(60);

/// Default recovery timeout: how long a holder's cluster lease may go un-renewed
/// before another node is allowed to take the volume over.
pub const DEFAULT_RECOVERY_TIMEOUT: Duration = Duration::from_secs(120);

/// Configuration for the [`Coordinator`].
#[derive(Clone, Debug)]
pub struct CoordinationConfig {
    /// Identity recorded as the holder of the cluster lease (typically the node
    /// name / hostname).
    pub holder_identity: String,
    /// Finite Azure blob lease duration (clamped to Azure's 15..=60s window).
    pub lease_duration: Duration,
    /// How long a holder's cluster lease may be stale before take-over.
    pub recovery_timeout: Duration,
    /// How often the renewal loop refreshes both leases.
    pub renew_interval: Duration,
}

impl CoordinationConfig {
    /// Build a config, deriving a sensible renewal interval from the lease
    /// duration (a third of it, with a 5s floor) and clamping the blob lease
    /// duration to Azure's accepted 15..=60s range.
    pub fn new(
        holder_identity: impl Into<String>,
        lease_duration: Duration,
        recovery_timeout: Duration,
    ) -> Self {
        let lease_duration = Duration::from_secs(lease_duration.as_secs().clamp(15, 60));
        let renew_interval = std::cmp::max(lease_duration / 3, Duration::from_secs(5));
        Self {
            holder_identity: holder_identity.into(),
            lease_duration,
            recovery_timeout,
            renew_interval,
        }
    }

    /// The blob lease duration in seconds, as the `i32` the Azure SDK expects.
    fn lease_duration_secs(&self) -> i32 {
        self.lease_duration.as_secs() as i32
    }
}

// ── BlobLock ────────────────────────────────────────────────────────────────

/// Error returned by [`BlobLock::acquire`].
#[derive(Debug)]
pub enum LockError {
    /// The lease is currently held by someone else (HTTP 409/412).
    Held,
    /// Any other failure.
    Other(anyhow::Error),
}

impl fmt::Display for LockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LockError::Held => write!(f, "blob lease is already held"),
            LockError::Other(e) => write!(f, "{e:#}"),
        }
    }
}

impl std::error::Error for LockError {}

/// A storage-level exclusive lock on the backing blob (an Azure blob lease).
#[async_trait]
pub trait BlobLock: Send + Sync {
    /// Acquire a finite lease of `duration_secs` seconds, returning the lease id
    /// assigned by the server.  Returns [`LockError::Held`] if another client
    /// already holds the lease.
    async fn acquire(&self, duration_secs: i32) -> Result<String, LockError>;

    /// Renew the lease identified by `lease_id`.
    async fn renew(&self, lease_id: &str) -> anyhow::Result<()>;

    /// Release the lease identified by `lease_id`.
    async fn release(&self, lease_id: &str) -> anyhow::Result<()>;

    /// Break the current lease immediately, regardless of who holds it, so it
    /// can be re-acquired.  Used to take a volume over from a dead holder.
    async fn break_lock(&self) -> anyhow::Result<()>;
}

// ── ClusterLease ──────────────────────────────────────────────────────────────

/// Outcome of attempting to acquire the cluster lease.
#[derive(Debug)]
pub enum ClusterAcquire {
    /// We now hold the cluster lease.
    Acquired,
    /// Another holder is still alive (renewed within the recovery timeout).
    HeldByLiveHolder {
        /// The current holder identity.
        holder: String,
        /// How long ago the holder last renewed the lease.
        since_renew: Duration,
    },
}

/// A cluster-wide liveness lease (a Kubernetes `coordination.k8s.io` Lease).
#[async_trait]
pub trait ClusterLease: Send + Sync {
    /// Attempt to acquire — or take over a stale holder's — cluster lease for
    /// our holder identity.
    async fn try_acquire(&self) -> anyhow::Result<ClusterAcquire>;

    /// Refresh our hold on the cluster lease (`renewTime = now`).
    async fn renew(&self) -> anyhow::Result<()>;

    /// Release the cluster lease so another node can take the volume
    /// immediately, without waiting for the recovery timeout.
    async fn release(&self) -> anyhow::Result<()>;
}

// ── Coordinator ───────────────────────────────────────────────────────────────

/// Orchestrates acquisition of the blob lock — and, when a cluster lease is
/// provided, the cluster lease too — and keeps them renewed for as long as the
/// [`CoordinationGuard`] is held.
///
/// The cluster lease is **optional**: in single-process mode (no Kubernetes
/// cluster to coordinate with) the coordinator acquires and renews only the
/// authoritative blob lease.  Passing a cluster lease additionally layers the
/// liveness-based take-over protocol on top (see the module docs).
pub struct Coordinator {
    cluster: Option<Arc<dyn ClusterLease>>,
    blob: Arc<dyn BlobLock>,
    config: CoordinationConfig,
}

impl Coordinator {
    /// Create a coordinator over the given (optional) cluster lease and blob
    /// lock.  Pass `None` for the cluster lease to run in single-process,
    /// blob-lock-only mode.
    pub fn new(
        cluster: Option<Arc<dyn ClusterLease>>,
        blob: Arc<dyn BlobLock>,
        config: CoordinationConfig,
    ) -> Self {
        Self {
            cluster,
            blob,
            config,
        }
    }

    /// Run the startup algorithm: acquire (or take over) both locks and start
    /// the renewal loop.  On success the returned guard owns the renewal task
    /// and releases both leases when dropped/awaited.
    pub async fn acquire(self) -> anyhow::Result<CoordinationGuard> {
        // 1. Cluster lease (optional) — gates take-over and proves no live peer
        //    owns us.  Skipped entirely in single-process blob-lock-only mode.
        if let Some(cluster) = &self.cluster {
            match cluster
                .try_acquire()
                .await
                .context("acquire cluster lease")?
            {
                ClusterAcquire::Acquired => {
                    info!(holder = %self.config.holder_identity, "cluster lease acquired");
                }
                ClusterAcquire::HeldByLiveHolder {
                    holder,
                    since_renew,
                } => {
                    return Err(anyhow!(
                        "cluster lease is held by a live holder '{holder}' \
                         (renewed {since_renew:?} ago, within the {:?} recovery timeout); \
                         another node is still using this volume — refusing to mount",
                        self.config.recovery_timeout
                    ));
                }
            }
        }

        // 2. Blob lease — the authoritative storage lock.  If a dead holder left
        //    it held, break it (we already won the cluster lease, so the holder
        //    is past the recovery timeout) and re-acquire.
        let secs = self.config.lease_duration_secs();
        let lease_id = match self.blob.acquire(secs).await {
            Ok(id) => {
                info!("blob lease acquired");
                id
            }
            Err(LockError::Held) => {
                // Only break a held blob lease when a cluster lease arbitrates
                // liveness: winning it proves the previous holder is past the
                // recovery timeout.  In single-process blob-lock-only mode there
                // is no liveness arbiter, so refuse rather than risk corrupting a
                // blob that another live process may still be writing.
                let Some(cluster) = &self.cluster else {
                    return Err(anyhow!(
                        "blob lease is already held by another process — refusing to mount; \
                         if you are certain no other process is using this blob, \
                         pass --disable-blob-lock"
                    ));
                };
                warn!(
                    "blob lease is held but its holder is past the recovery timeout; \
                     breaking the stale blob lease to take the volume over"
                );
                self.blob
                    .break_lock()
                    .await
                    .context("break stale blob lease for take-over")?;
                match self.blob.acquire(secs).await {
                    Ok(id) => {
                        info!("blob lease re-acquired after break");
                        id
                    }
                    Err(LockError::Held) => {
                        // Roll back the cluster lease so we don't strand it.
                        let _ = cluster.release().await;
                        return Err(anyhow!(
                            "blob lease is still held after breaking it — another \
                             node raced us for the take-over"
                        ));
                    }
                    Err(LockError::Other(e)) => {
                        let _ = cluster.release().await;
                        return Err(e).context("re-acquire blob lease after break");
                    }
                }
            }
            Err(LockError::Other(e)) => {
                if let Some(cluster) = &self.cluster {
                    let _ = cluster.release().await;
                }
                return Err(e).context("acquire blob lease");
            }
        };

        // 3. Renewal loop keeps both leases fresh until the guard stops it.
        let stop = Arc::new(Notify::new());
        let handle = tokio::spawn(renew_loop(
            self.cluster.clone(),
            self.blob.clone(),
            lease_id.clone(),
            self.config.renew_interval,
            stop.clone(),
        ));

        Ok(CoordinationGuard {
            cluster: self.cluster,
            blob: self.blob,
            lease_id,
            stop,
            handle: Some(handle),
        })
    }
}

/// Background loop renewing the blob lease — and the cluster lease, when present
/// — at `interval` until `stop` is notified.
async fn renew_loop(
    cluster: Option<Arc<dyn ClusterLease>>,
    blob: Arc<dyn BlobLock>,
    lease_id: String,
    interval: Duration,
    stop: Arc<Notify>,
) {
    loop {
        tokio::select! {
            _ = stop.notified() => break,
            _ = tokio::time::sleep(interval) => {
                if let Err(e) = blob.renew(&lease_id).await {
                    error!(err = %format!("{e:#}"), "failed to renew blob lease");
                }
                if let Some(cluster) = &cluster {
                    if let Err(e) = cluster.renew().await {
                        error!(err = %format!("{e:#}"), "failed to renew cluster lease");
                    }
                }
            }
        }
    }
}

/// Holds the blob lease (and optional cluster lease) for the lifetime of a
/// mounted device.  Stops renewal and releases the leases on
/// [`CoordinationGuard::release`] (or, best-effort, on drop).
pub struct CoordinationGuard {
    cluster: Option<Arc<dyn ClusterLease>>,
    blob: Arc<dyn BlobLock>,
    lease_id: String,
    stop: Arc<Notify>,
    handle: Option<JoinHandle<()>>,
}

impl CoordinationGuard {
    /// The blob-lease id (`x-ms-lease-id`) held for this mount.
    ///
    /// Once coordination is enabled, every mutating request to the leased page
    /// blob must carry this id or Azure rejects it with HTTP 412; the data-path
    /// backend is told about it via `AzurePageBlobBackend::set_lease_id`.
    pub fn lease_id(&self) -> &str {
        &self.lease_id
    }

    /// Stop the renewal loop and release the blob lease (and cluster lease, when
    /// present) so another node can take the volume over immediately.
    pub async fn release(mut self) {
        self.stop.notify_one();
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
        if let Err(e) = self.blob.release(&self.lease_id).await {
            warn!(err = %format!("{e:#}"), "failed to release blob lease on shutdown");
        }
        if let Some(cluster) = &self.cluster {
            if let Err(e) = cluster.release().await {
                warn!(err = %format!("{e:#}"), "failed to release cluster lease on shutdown");
            }
            info!("released cluster lease and blob lease");
        } else {
            info!("released blob lease");
        }
    }
}

impl fmt::Debug for CoordinationGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoordinationGuard")
            .field("renewing", &self.handle.is_some())
            .finish()
    }
}

impl Drop for CoordinationGuard {
    fn drop(&mut self) {
        // If `release` was not called, at least stop the renewal task.  We
        // cannot await an async release from `drop`, so the leases will be left
        // to expire on their own (blob lease) / go stale (cluster lease).
        if self.handle.is_some() {
            self.stop.notify_one();
            warn!("CoordinationGuard dropped without release(); leases will expire on their own");
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// In-memory blob lock: tracks the current lease id (if any).
    #[derive(Default)]
    struct MemBlobLock {
        held: Mutex<Option<String>>,
        next: Mutex<u64>,
    }

    impl MemBlobLock {
        fn preheld() -> Self {
            Self {
                held: Mutex::new(Some("foreign-lease".to_string())),
                next: Mutex::new(0),
            }
        }
        fn current(&self) -> Option<String> {
            self.held.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl BlobLock for MemBlobLock {
        async fn acquire(&self, _duration_secs: i32) -> Result<String, LockError> {
            let mut held = self.held.lock().unwrap();
            if held.is_some() {
                return Err(LockError::Held);
            }
            let mut n = self.next.lock().unwrap();
            *n += 1;
            let id = format!("lease-{n}");
            *held = Some(id.clone());
            Ok(id)
        }
        async fn renew(&self, lease_id: &str) -> anyhow::Result<()> {
            let held = self.held.lock().unwrap();
            match held.as_deref() {
                Some(id) if id == lease_id => Ok(()),
                _ => Err(anyhow!("not the lease holder")),
            }
        }
        async fn release(&self, lease_id: &str) -> anyhow::Result<()> {
            let mut held = self.held.lock().unwrap();
            if held.as_deref() == Some(lease_id) {
                *held = None;
            }
            Ok(())
        }
        async fn break_lock(&self) -> anyhow::Result<()> {
            *self.held.lock().unwrap() = None;
            Ok(())
        }
    }

    /// In-memory cluster lease with a configurable acquire outcome.
    struct MemClusterLease {
        outcome_live: bool,
        released: Mutex<bool>,
    }

    impl MemClusterLease {
        fn free() -> Self {
            Self {
                outcome_live: false,
                released: Mutex::new(false),
            }
        }
        fn live() -> Self {
            Self {
                outcome_live: true,
                released: Mutex::new(false),
            }
        }
    }

    #[async_trait]
    impl ClusterLease for MemClusterLease {
        async fn try_acquire(&self) -> anyhow::Result<ClusterAcquire> {
            if self.outcome_live {
                Ok(ClusterAcquire::HeldByLiveHolder {
                    holder: "other-node".to_string(),
                    since_renew: Duration::from_secs(1),
                })
            } else {
                Ok(ClusterAcquire::Acquired)
            }
        }
        async fn renew(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn release(&self) -> anyhow::Result<()> {
            *self.released.lock().unwrap() = true;
            Ok(())
        }
    }

    fn config() -> CoordinationConfig {
        CoordinationConfig::new(
            "test-node",
            DEFAULT_LEASE_DURATION,
            DEFAULT_RECOVERY_TIMEOUT,
        )
    }

    #[tokio::test]
    async fn acquires_both_when_free() {
        let cluster = Arc::new(MemClusterLease::free());
        let blob = Arc::new(MemBlobLock::default());
        let coord = Coordinator::new(Some(cluster.clone()), blob.clone(), config());
        let guard = coord.acquire().await.expect("should acquire");
        assert!(blob.current().is_some(), "blob lease should be held");
        guard.release().await;
        assert!(blob.current().is_none(), "blob lease released on shutdown");
        assert!(*cluster.released.lock().unwrap(), "cluster lease released");
    }

    #[tokio::test]
    async fn refuses_when_peer_is_live() {
        let cluster = Arc::new(MemClusterLease::live());
        let blob = Arc::new(MemBlobLock::default());
        let coord = Coordinator::new(Some(cluster), blob.clone(), config());
        let err = coord.acquire().await.expect_err("should refuse");
        assert!(
            err.to_string().contains("live holder"),
            "unexpected error: {err:#}"
        );
        assert!(blob.current().is_none(), "blob lease must not be taken");
    }

    #[tokio::test]
    async fn breaks_stale_blob_lease_to_take_over() {
        // Cluster lease is free (prior holder is past the recovery timeout), but
        // the blob lease is still held by the dead holder.  We should break it
        // and acquire it for ourselves.
        let cluster = Arc::new(MemClusterLease::free());
        let blob = Arc::new(MemBlobLock::preheld());
        let coord = Coordinator::new(Some(cluster), blob.clone(), config());
        let guard = coord.acquire().await.expect("should take over");
        let held = blob.current().expect("blob lease held after take-over");
        assert_ne!(held, "foreign-lease", "should be our fresh lease id");
        guard.release().await;
    }

    #[tokio::test]
    async fn blob_only_acquires_and_releases_without_cluster_lease() {
        // Single-process mode: no cluster lease, just the authoritative blob lock.
        let blob = Arc::new(MemBlobLock::default());
        let coord = Coordinator::new(None, blob.clone(), config());
        let guard = coord.acquire().await.expect("should acquire blob lock");
        assert!(blob.current().is_some(), "blob lease should be held");
        guard.release().await;
        assert!(blob.current().is_none(), "blob lease released on shutdown");
    }

    #[tokio::test]
    async fn blob_only_refuses_held_lease_without_breaking() {
        // Without a cluster lease there is no liveness arbiter, so a held blob
        // lease must be respected (not broken) — another live process may own it.
        let blob = Arc::new(MemBlobLock::preheld());
        let coord = Coordinator::new(None, blob.clone(), config());
        let err = coord.acquire().await.expect_err("should refuse");
        assert!(
            err.to_string().contains("already held"),
            "unexpected error: {err:#}"
        );
        assert_eq!(
            blob.current().as_deref(),
            Some("foreign-lease"),
            "foreign blob lease must not be broken"
        );
    }

    #[test]
    fn config_clamps_and_derives_interval() {
        let cfg = CoordinationConfig::new("n", Duration::from_secs(600), Duration::from_secs(90));
        assert_eq!(cfg.lease_duration, Duration::from_secs(60));
        assert_eq!(cfg.renew_interval, Duration::from_secs(20));
        let cfg = CoordinationConfig::new("n", Duration::from_secs(1), Duration::from_secs(90));
        assert_eq!(cfg.lease_duration, Duration::from_secs(15));
        assert_eq!(cfg.renew_interval, Duration::from_secs(5));
    }
}
