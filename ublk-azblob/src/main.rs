//! `ublk-azblob` — Linux ublk block device backed by an Azure Page Blob.
//!
//! # Usage
//!
//! ```text
//! ublk-azblob [GLOBAL OPTIONS] --blob-url <BLOB_URL> <run|test> --size <SIZE>
//! ```
//!
//! `--blob-url` is a full Azure blob URL (e.g.
//! `https://acct.blob.core.windows.net/container/blob.vhd`, or for Azurite
//! `http://127.0.0.1:10000/devstoreaccount1/container/blob`) selecting the
//! account, container and blob in one argument.  `--size` (and other per-command
//! options) belong to the `run`/`test` subcommands; auth options are global.
//!
//! See `--help` and `README.md` for full documentation.

mod auth;
mod backend;
mod bloburl;
#[cfg_attr(not(feature = "coordination"), allow(dead_code))]
mod coordination;
#[cfg(feature = "csi")]
mod csi;
mod nbd_target;
mod ublk_target;

use anyhow::Context as _;
use auth::{AuthConfig, UserAssignedIdentity};
use backend::{
    azure::AzurePageBlobBackend,
    buffered::{BufferedBackend, BufferedConfig},
    file::{FileCacheBackend, FileCacheConfig},
    BlobBackend,
};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info, warn};

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "ublk-azblob",
    about = "Expose an Azure Page Blob as a Linux ublk block device (/dev/ublkbN)",
    version
)]
struct Cli {
    /// Full Azure blob URL selecting the account, container and blob in one
    /// argument.
    ///
    /// Examples:
    /// `https://mystorageaccount.blob.core.windows.net/mycontainer/myblob.vhd`
    /// or, for Azurite, `http://127.0.0.1:10000/devstoreaccount1/mycontainer/myblob`.
    /// The URL may carry a `?snapshot=<timestamp>` and/or a SAS query.
    ///
    /// Required by the `run`, `test` and `copy` subcommands.  Not used in `csi`
    /// mode (values come from StorageClass parameters / secrets).
    #[arg(long, env = "AZURE_STORAGE_BLOB_URL")]
    blob_url: Option<String>,

    /// Target a specific blob *snapshot* (the `x-ms-snapshot` timestamp).
    ///
    /// A snapshot is an immutable, point-in-time view of the blob.  Because a
    /// snapshot can never change, selecting one is what makes the device
    /// **read-only** (all write/discard operations are rejected) *and* makes the
    /// local cache safe to reuse: there is no separate `--read-only` flag.
    /// May also be supplied as a `?snapshot=` query on `--blob-url`; `--snapshot`
    /// takes precedence when both are provided.
    #[arg(long)]
    snapshot: Option<String>,

    /// Storage account key (base64).  Enables SharedKey auth mode.
    ///
    /// Mutually exclusive with --msi / --msi-* / --workload-identity.  Use for
    /// Azurite and local dev.
    #[arg(long, env = "AZURE_STORAGE_KEY", conflicts_with_all = ["msi", "msi_client_id", "msi_object_id", "msi_resource_id", "workload_identity"])]
    account_key: Option<String>,

    /// Shared Access Signature (SAS) token authenticating the blob.
    ///
    /// The query string of a SAS URL (with or without a leading `?`). Used to
    /// read a `templateBlobUrl` golden image that carries its own SAS (possibly
    /// in a different storage account). Takes precedence over other auth modes.
    #[arg(long, env = "AZURE_STORAGE_SAS")]
    sas_token: Option<String>,

    /// Enable system-assigned Managed Identity.
    #[arg(long, env = "AZURE_USE_MSI")]
    msi: bool,

    /// User-assigned Managed Identity — client ID.
    #[arg(long, env = "AZURE_MSI_CLIENT_ID", conflicts_with_all = ["msi_object_id", "msi_resource_id"])]
    msi_client_id: Option<String>,

    /// User-assigned Managed Identity — object ID.
    #[arg(long, env = "AZURE_MSI_OBJECT_ID", conflicts_with_all = ["msi_resource_id"])]
    msi_object_id: Option<String>,

    /// User-assigned Managed Identity — resource ID.
    #[arg(long, env = "AZURE_MSI_RESOURCE_ID")]
    msi_resource_id: Option<String>,

    /// Enable Microsoft Entra Workload Identity (federated Kubernetes token).
    ///
    /// The recommended way to access Azure Storage from AKS pods. The client
    /// id, tenant id and projected token file default to the standard
    /// `AZURE_CLIENT_ID` / `AZURE_TENANT_ID` / `AZURE_FEDERATED_TOKEN_FILE`
    /// environment variables injected by the workload-identity webhook.
    /// Mutually exclusive with --account-key and the --msi* flags.
    #[arg(long, env = "AZURE_USE_WORKLOAD_IDENTITY", conflicts_with_all = ["account_key", "msi", "msi_client_id", "msi_object_id", "msi_resource_id"])]
    workload_identity: bool,

    /// Workload Identity / service principal client ID (overrides `AZURE_CLIENT_ID`).
    #[arg(long, env = "AZURE_CLIENT_ID")]
    azure_client_id: Option<String>,

    /// Workload Identity / service principal tenant ID (overrides `AZURE_TENANT_ID`).
    #[arg(long, env = "AZURE_TENANT_ID")]
    azure_tenant_id: Option<String>,

    /// Path to the projected federated token file (overrides
    /// `AZURE_FEDERATED_TOKEN_FILE`).
    #[arg(long, env = "AZURE_FEDERATED_TOKEN_FILE")]
    azure_federated_token_file: Option<String>,

    /// Service principal client secret.
    ///
    /// When set (together with `AZURE_CLIENT_ID` / `AZURE_TENANT_ID`), the
    /// driver authenticates as an Entra ID application (client-secret flow)
    /// rather than a managed/workload identity. Mutually exclusive with
    /// --account-key and the --msi* flags.
    #[arg(long, env = "AZURE_CLIENT_SECRET", conflicts_with_all = ["account_key", "msi", "msi_client_id", "msi_object_id", "msi_resource_id"])]
    azure_client_secret: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Provision a new page blob and start the ublk device.
    Run {
        /// Device size in bytes (must be a multiple of 512).
        #[arg(long, env = "UBLK_DEV_SIZE")]
        size: u64,

        /// Create (or overwrite) the page blob before starting the device.
        #[arg(long)]
        create: bool,

        /// Number of io_uring queues.
        #[arg(long, default_value = "1")]
        nr_queues: u16,

        /// Queue depth (concurrent in-flight I/Os per queue).
        #[arg(long, default_value = "64")]
        queue_depth: u16,

        /// ublk device id to request (`-1` lets the kernel auto-allocate the
        /// next free `/dev/ublkbN`).
        #[arg(long, default_value = "-1")]
        id: i32,

        /// Write-back buffer page size in bytes.
        ///
        /// Writes are buffered in pages of this size and flushed to Azure in
        /// batches.  Must be a multiple of 512.  Set to 0 to disable buffering
        /// (write-through mode).
        #[arg(long, default_value = "4194304", env = "UBLK_PAGE_SIZE")]
        page_size: u64,

        /// Maximum number of dirty pages held in memory before auto-flush.
        ///
        /// When the dirty page count exceeds this limit the oldest pages are
        /// flushed to Azure automatically.  Total memory cap ≈ page_size × max_dirty_pages.
        #[arg(long, default_value = "64", env = "UBLK_MAX_DIRTY_PAGES")]
        max_dirty_pages: usize,

        /// Enable cluster coordination: acquire both the Azure blob lease
        /// ("blob lock") and a Kubernetes `coordination.k8s.io` Lease ("cluster
        /// lease") before mounting, so at most one node serves the volume.
        /// Requires the `coordination` build feature.
        #[arg(
            long,
            env = "UBLK_COORDINATION",
            num_args = 0..=1,
            default_value_t = false,
            default_missing_value = "true",
            value_parser = clap::builder::BoolishValueParser::new(),
        )]
        coordination: bool,

        /// Recovery timeout (seconds): how long a holder's cluster lease may go
        /// un-renewed before another node may break its blob lease and take the
        /// volume over.
        #[arg(long, default_value_t = coordination::DEFAULT_RECOVERY_TIMEOUT.as_secs(), env = "UBLK_RECOVERY_TIMEOUT_SECS")]
        recovery_timeout_secs: u64,

        /// Blob lease duration in seconds (clamped to Azure's 15..=60s window).
        #[arg(long, default_value_t = coordination::DEFAULT_LEASE_DURATION.as_secs(), env = "UBLK_LEASE_DURATION_SECS")]
        lease_duration_secs: u64,

        /// Kubernetes namespace for the cluster lease (defaults to
        /// `POD_NAMESPACE`, then `default`).
        #[arg(long, env = "UBLK_LEASE_NAMESPACE")]
        lease_namespace: Option<String>,

        /// Cluster lease object name (defaults to a sanitized
        /// `<container>-<blob>`).
        #[arg(long, env = "UBLK_LEASE_NAME")]
        lease_name: Option<String>,

        /// Holder identity recorded in the cluster lease (defaults to
        /// `HOSTNAME`, then `unknown-node`).
        #[arg(long, env = "UBLK_LEASE_HOLDER")]
        lease_holder: Option<String>,

        /// Directory for the persistent local-disk cache.
        ///
        /// When set, a local-disk cache layer is inserted between the in-memory
        /// write-back buffer and Azure, forming a multi-level cache
        /// (memory → local disk → blob).  Cached pages — including unflushed
        /// *dirty* pages — survive a restart: on startup the cache is recovered
        /// from disk and any recovered dirty pages are flushed to the blob.
        #[arg(long, env = "UBLK_CACHE_DIR")]
        cache_dir: Option<PathBuf>,

        /// Local-disk cache page size in bytes (must be a multiple of 512).
        ///
        /// Only used when `--cache-dir` is set.
        #[arg(long, default_value = "1048576", env = "UBLK_CACHE_PAGE_SIZE")]
        cache_page_size: u64,

        /// Maximum total bytes of cached page data on local disk, **shared
        /// across all processes** using the same `--cache-dir` (0 = unlimited).
        ///
        /// When set, processes sharing the cache directory enforce one node-wide
        /// LRU byte budget: clean (already-flushed) pages are evicted to stay
        /// within the limit, so a single noisy volume cannot fill the disk.
        /// Dirty (unflushed) pages are never evicted.  Only used when
        /// `--cache-dir` is set.
        #[arg(long, default_value = "0", env = "UBLK_CACHE_MAX_BYTES")]
        cache_max_bytes: u64,

        /// Enable cross-process clean-page sharing in the local-disk cache.
        ///
        /// **Currently disabled / no-op:** accepted but ignored (a warning is
        /// logged) — every cache is single-process for now. When re-enabled,
        /// processes that cache the **same blob** (same
        /// `--cache-blob-identity`) in a shared `--cache-dir` can serve each
        /// other's already-fetched *clean* pages directly off local disk via a
        /// `flock`-coordinated `.cache-index`, avoiding a redundant blob read.
        /// Each cache still writes only its own data file (copy-on-write on a
        /// dirtying write), so the single-writer-per-file invariant holds.
        /// Requires a per-instance `--cache-instance` so peers have distinct
        /// data files.  Only used when `--cache-dir` is set.
        #[arg(
            long,
            env = "UBLK_CACHE_SHARE_PAGES",
            num_args = 0..=1,
            default_value_t = false,
            default_missing_value = "true",
            value_parser = clap::builder::BoolishValueParser::new(),
        )]
        cache_share_pages: bool,

        /// Shared identity of the blob this cache mirrors, used to match peers
        /// for `--cache-share-pages`.  Defaults to the container/blob.  Set it to
        /// a common value (e.g. a golden-image id) across volumes that should
        /// share pages.  Only used when `--cache-dir` is set.
        #[arg(long, env = "UBLK_CACHE_BLOB_IDENTITY")]
        cache_blob_identity: Option<String>,

        /// Per-instance cache file base name, making each cache's data file
        /// unique within a shared `--cache-dir`.  Defaults to the container/blob.
        /// Must be stable across restarts of the same volume (so dirty pages are
        /// recovered) and unique per volume when `--cache-share-pages` is set.
        /// Only used when `--cache-dir` is set.
        #[arg(long, env = "UBLK_CACHE_INSTANCE")]
        cache_instance: Option<String>,

        /// Warm the local-disk cache on start by sequentially prefetching the
        /// blob into it. Runs in the background (does not delay the device coming
        /// online) and is sharing-aware (pages a live peer already caches are
        /// fetched from the peer, not Azure). Only used when `--cache-dir` is set;
        /// best for read-only / read-mostly datasets that fit the cache budget.
        #[arg(
            long,
            env = "UBLK_CACHE_WARMUP",
            num_args = 0..=1,
            default_value_t = false,
            default_missing_value = "true",
            value_parser = clap::builder::BoolishValueParser::new(),
        )]
        cache_warmup: bool,

        /// Cap in bytes for `--cache-warmup`. `0` = auto: the cache byte budget
        /// (`--cache-max-bytes`) when set, otherwise the whole device. Prefetch
        /// stops once this many bytes from offset 0 have been scanned.
        #[arg(long, default_value = "0", env = "UBLK_CACHE_WARMUP_BYTES")]
        cache_warmup_bytes: u64,

        /// Idle flush timeout in seconds: automatically flush dirty pages after N
        /// seconds of write inactivity.  Set to 0 to disable idle flushing.
        ///
        /// This helps ensure data is periodically persisted to Azure even when
        /// there's no explicit flush call or dirty-page limit trigger.
        /// When idle flush is triggered, the force flush timer is reset.
        #[arg(long, default_value = "15", env = "UBLK_IDLE_FLUSH_SECS")]
        idle_flush_secs: u64,

        /// Force flush timeout in seconds: maximum time since the last successful
        /// flush before forcing a flush regardless of idle state.  This acts as a
        /// hard deadline to ensure data is persisted even if writes are continuous.
        /// Set to 0 for no timeout. Idle flushes reset this timer.
        ///
        /// This prevents data from staying dirty for too long during continuous writes.
        #[arg(long, default_value = "600", env = "UBLK_FORCE_FLUSH_TIMEOUT_SECS")]
        force_flush_timeout_secs: u64,

        /// Optional hard timeout (seconds) on a single flush I/O operation.
        /// Set to 0 (default) for no cap, so explicit/shutdown flushes can finish
        /// even with many dirty pages or a slow link. This is independent of
        /// `--force-flush-timeout-secs` (which only schedules background flushes).
        #[arg(long, default_value = "0", env = "UBLK_FLUSH_IO_TIMEOUT_SECS")]
        flush_io_timeout_secs: u64,

        /// Serve over the NBD protocol instead of ublk (compatibility mode).
        ///
        /// When set, the device is exposed as an NBD server bound to this
        /// `host:port` (e.g. `0.0.0.0:10809`) rather than a `/dev/ublkbN`
        /// device.  Use this on kernels/platforms without `ublk_drv`: connect
        /// with the standard NBD client, e.g.
        /// `nbd-client <host> <port> /dev/nbd0`.  The ublk-specific options
        /// (`--nr-queues`, `--queue-depth`, `--id`) are ignored in this mode.
        #[arg(long, env = "NBD_LISTEN")]
        nbd: Option<String>,
    },

    /// Just test the BlobBackend connection (write → read → clear → verify).
    Test {
        /// Device size to use for the test blob.
        #[arg(long, default_value = "4096")]
        size: u64,
    },

    /// Copy a golden-image *template* blob into the target blob selected by
    /// `--blob-url`, then exit.
    ///
    /// This is the same server-side-style streamed copy the CSI controller uses
    /// to provision a read-write volume from `templateBlobUrl`. The target blob
    /// is created sized to hold the template (and at least `--size`). Requires
    /// the `csi` build feature.
    #[cfg(feature = "csi")]
    Copy {
        /// Full Azure blob URL of the template (may carry a SAS and/or
        /// `snapshot=` query).
        #[arg(long, env = "TEMPLATE_BLOB_URL")]
        template_url: String,

        /// Minimum target size in bytes (the target is grown to the template
        /// size when larger). Rounded up to 512.
        #[arg(long, default_value = "0")]
        size: u64,
    },

    /// Run the Kubernetes CSI driver (Container Storage Interface gRPC server).
    ///
    /// Lets a Kubernetes PersistentVolumeClaim be backed by an Azure Page Blob
    /// exposed through a ublk device.  Requires the `csi` build feature (and the
    /// `ublk` feature on the node side).
    #[cfg(feature = "csi")]
    Csi {
        /// Default Azure Storage account name for provisioned volumes.
        ///
        /// Used when a StorageClass does not set `storageAccount`.  May be empty
        /// when every StorageClass supplies its own account.
        #[arg(long, env = "AZURE_STORAGE_ACCOUNT", default_value = "")]
        account: String,

        /// Default blob container used when a StorageClass does not set one.
        #[arg(long, env = "AZURE_STORAGE_CONTAINER", default_value = "")]
        container: String,

        /// Azure Storage service endpoint URL template.
        ///
        /// Defaults to `https://<account>.blob.core.windows.net/`.  A `%s`
        /// placeholder is substituted with the per-volume account.  For Azurite
        /// use `http://127.0.0.1:10000/<account>`.
        #[arg(long, env = "AZURE_STORAGE_ENDPOINT")]
        endpoint: Option<String>,

        /// CSI endpoint to listen on (`unix:///csi/csi.sock` or `tcp://addr:port`).
        #[arg(long, env = "CSI_ENDPOINT", default_value = "unix:///csi/csi.sock")]
        csi_endpoint: String,

        /// Node identifier reported to Kubernetes (typically the node name).
        #[arg(long, env = "CSI_NODE_ID", default_value = "")]
        node_id: String,

        /// Which CSI services to serve: `controller`, `node`, or `all`.
        #[arg(long, env = "CSI_ROLE", default_value = "all")]
        role: csi::Role,

        /// Use NBD instead of ublk for node devices (compatibility mode).
        #[arg(long, env = "CSI_USE_NBD")]
        use_nbd: bool,

        /// NBD listen address prefix (e.g. `127.0.0.1`). Port is auto-assigned per volume.
        #[arg(long, env = "CSI_NBD_HOST", default_value = "127.0.0.1")]
        nbd_host: String,

        /// Starting port for NBD servers (each volume gets host:port+N).
        #[arg(long, env = "CSI_NBD_PORT_START", default_value = "10809")]
        nbd_port_start: u16,
    },
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("ublk_azblob=info".parse().unwrap()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Run {
            size,
            create,
            nr_queues,
            queue_depth,
            id,
            page_size,
            max_dirty_pages,
            coordination,
            recovery_timeout_secs,
            lease_duration_secs,
            ref lease_namespace,
            ref lease_name,
            ref lease_holder,
            ref cache_dir,
            cache_page_size,
            cache_max_bytes,
            cache_share_pages,
            ref cache_blob_identity,
            ref cache_instance,
            cache_warmup,
            cache_warmup_bytes,
            idle_flush_secs,
            force_flush_timeout_secs,
            flush_io_timeout_secs,
            ref nbd,
        } => {
            let loc = cli.location()?;
            let auth = build_auth(&cli, &loc.account, loc.sas.as_deref())?;
            // Read-only is derived solely from selecting a snapshot: a snapshot
            // is immutable, so the device is exposed read-only and its cache is
            // safe to reuse. There is no separate read-only flag.
            let read_only = loc.snapshot.is_some();
            if read_only {
                info!("snapshot selected: read-only mode (writes, discards, creation disabled)");
            }
            let azure_backend = build_azure_backend(&loc, &auth)?;
            if create {
                if read_only {
                    anyhow::bail!("--create cannot be used against a snapshot (read-only)");
                }
                info!(size, "creating page blob");
                azure_backend
                    .create(size)
                    .await
                    .context("create page blob")?;
            }

            let actual_size = azure_backend.size().await.context("get blob size")?;
            info!(size = actual_size, "blob ready");

            // Acquire cluster + blob coordination locks before mounting (after
            // the blob exists, so its lease can be taken).
            let guard = if coordination {
                Some(
                    acquire_coordination(
                        &loc,
                        &auth,
                        recovery_timeout_secs,
                        lease_duration_secs,
                        lease_namespace.clone(),
                        lease_name.clone(),
                        lease_holder.clone(),
                    )
                    .await
                    .context("acquire coordination locks")?,
                )
            } else {
                None
            };

            // Once the blob lease is held, every write/clear must carry the
            // matching `x-ms-lease-id` or Azure rejects it with HTTP 412. Hand
            // the lease id to the data-path backend before any I/O is served.
            if let Some(g) = &guard {
                azure_backend.set_lease_id(Some(g.lease_id().to_string()));
            }
            let backend: Arc<dyn BlobBackend> = azure_backend;

            // Optional local-disk cache layer (memory → local disk → blob).
            let backend: Arc<dyn BlobBackend> = if let Some(dir) = cache_dir.clone() {
                // Default the blob identity and per-instance name to the
                // container/blob.
                let default_name = cache_file_name(&loc.container, &loc.blob);
                let blob_identity = cache_blob_identity
                    .clone()
                    .map(|s| sanitize_cache_component(&s))
                    .unwrap_or_else(|| default_name.clone());
                let name = cache_instance
                    .clone()
                    .map(|s| sanitize_cache_component(&s))
                    .unwrap_or_else(|| default_name.clone());
                // Cross-process page sharing is forced OFF in the shipped binary
                // for now: a snapshot read cache and a pre-upload write cache are
                // both single-owner, so the cache never publishes to or reads
                // from peers. The `.cache-index` sharing machinery (including the
                // copy-on-write write path) is fully implemented but gated off
                // here; `--cache-share-pages` is accepted but ignored, pending
                // further correctness review before it is re-enabled.
                if cache_share_pages {
                    warn!(
                        "--cache-share-pages is currently disabled (single-process cache only); ignoring"
                    );
                }
                let share_pages = false;
                info!(
                    cache_dir = %dir.display(),
                    cache_page_size,
                    cache_max_bytes,
                    cache_share_pages = share_pages,
                    blob_identity = %blob_identity,
                    name = %name,
                    "local-disk cache enabled"
                );
                let (cache, recovered_dirty) = FileCacheBackend::open(
                    backend,
                    FileCacheConfig {
                        dir,
                        name,
                        page_size: cache_page_size,
                        max_bytes: cache_max_bytes,
                        blob_identity,
                        share_pages,
                    },
                    actual_size,
                )
                .context("open local-disk cache")?;
                let cache: Arc<dyn BlobBackend> = Arc::new(cache);
                // Recovered dirty pages (written before a previous restart but
                // never flushed) are pushed to the blob now that we are back up.
                if recovered_dirty > 0 {
                    info!(
                        recovered_dirty,
                        "flushing recovered dirty cache pages to blob"
                    );
                    cache
                        .flush()
                        .await
                        .context("flush recovered dirty cache pages")?;
                }
                // Optional background cache warm-up: sequentially prefetch the
                // blob into the cache so reads are served locally. Spawned
                // detached so the device comes online immediately. (Sharing is
                // disabled, so warm-up populates only this process's own cache.)
                if cache_warmup {
                    let limit = if cache_warmup_bytes > 0 {
                        cache_warmup_bytes
                    } else if cache_max_bytes > 0 {
                        cache_max_bytes
                    } else {
                        actual_size
                    }
                    .min(actual_size);
                    let warm_backend = cache.clone();
                    info!(
                        warmup_limit_bytes = limit,
                        "cache warm-up started (background)"
                    );
                    tokio::spawn(async move {
                        warmup_cache(warm_backend, actual_size, cache_page_size, limit).await;
                    });
                }
                cache
            } else {
                if cache_warmup {
                    warn!("--cache-warmup ignored: requires --cache-dir");
                }
                backend
            };

            // Wrap with write-back buffer if page_size > 0.  The buffer only
            // serves to batch writes, so it is skipped entirely in read-only
            // mode where no writes ever reach the backend.
            let backend: Arc<dyn BlobBackend> = if read_only {
                info!("write-through mode (read-only)");
                backend
            } else if page_size > 0 {
                info!(
                    page_size,
                    max_dirty_pages,
                    idle_flush_secs,
                    force_flush_timeout_secs,
                    flush_io_timeout_secs,
                    "write-back buffer enabled"
                );
                BufferedBackend::new(
                    backend,
                    BufferedConfig {
                        page_size,
                        max_dirty_pages,
                        idle_flush_secs,
                        force_flush_timeout_secs,
                        flush_io_timeout_secs,
                    },
                )
                .context("configure write-back buffer")?
            } else {
                info!("write-through mode (no buffering)");
                backend
            };

            let cfg = ublk_target::UblkConfig {
                block_size: 512,
                dev_size: actual_size,
                nr_queues,
                queue_depth,
                id,
                read_only,
            };
            let result = if let Some(addr) = nbd {
                info!(addr = %addr, "starting NBD compatibility server");
                nbd_target::run_nbd_target(backend, addr, actual_size, read_only)
                    .await
                    .context("nbd target")
            } else {
                ublk_target::run_ublk_target(backend, cfg)
                    .await
                    .context("ublk target")
            };

            // Release the leases so another node can take over immediately.
            if let Some(guard) = guard {
                guard.release().await;
            }
            result?;
        }

        Command::Test { size } => {
            let loc = cli.location()?;
            let auth = build_auth(&cli, &loc.account, loc.sas.as_deref())?;
            let backend: Arc<dyn BlobBackend> = build_azure_backend(&loc, &auth)?;
            run_smoke_test(backend, size).await?;
        }

        #[cfg(feature = "csi")]
        Command::Copy {
            ref template_url,
            size,
        } => {
            let loc = cli.location()?;
            run_template_copy(&cli, &loc, template_url, size).await?;
        }

        #[cfg(feature = "csi")]
        Command::Csi {
            account,
            container,
            endpoint,
            csi_endpoint,
            node_id,
            role,
            use_nbd,
            nbd_host,
            nbd_port_start,
        } => {
            // The endpoint *template* may contain a `%s` placeholder for the
            // account (subdomain-style, e.g. `http://%s.blob.localhost:10000/`).
            // The CSI driver keeps the template verbatim and substitutes `%s`
            // per volume in `csi::build_backend` (each volume can target a
            // different account).
            let endpoint_template = endpoint.unwrap_or_else(|| {
                // With an empty default account, use a generic endpoint (the
                // account is substituted per volume later).
                if account.is_empty() {
                    "https://blob.core.windows.net/".to_string()
                } else {
                    format!("https://{account}.blob.core.windows.net/")
                }
            });
            let config = csi::DriverConfig {
                account: account.clone(),
                endpoint: endpoint_template,
                default_container: container.clone(),
                account_key: cli.account_key.clone(),
                use_msi: cli.msi
                    || cli.msi_client_id.is_some()
                    || cli.msi_object_id.is_some()
                    || cli.msi_resource_id.is_some(),
                msi_client_id: cli.msi_client_id.clone(),
                use_workload_identity: cli.workload_identity,
                workload_identity_client_id: cli.azure_client_id.clone(),
                workload_identity_tenant_id: cli.azure_tenant_id.clone(),
                workload_identity_token_file: cli.azure_federated_token_file.clone(),
                sp_tenant_id: cli.azure_tenant_id.clone(),
                sp_client_id: cli.azure_client_id.clone(),
                sp_client_secret: cli.azure_client_secret.clone(),
                use_nbd,
                nbd_host: nbd_host.clone(),
                nbd_port_start,
            };
            let node_id = if node_id.is_empty() {
                hostname()
            } else {
                node_id
            };
            csi::run_csi(&csi_endpoint, role, node_id, config)
                .await
                .context("CSI driver")?;
        }
    }

    Ok(())
}

/// A blob location parsed from `--blob-url`, used by the single-device
/// `run` / `test` / `copy` subcommands.
struct Location {
    /// Blob service endpoint URL with a trailing `/` (what
    /// `build_container_client` expects to append `/container` to).
    endpoint: String,
    /// Storage account name (derived from the URL host/path).
    account: String,
    /// Container name.
    container: String,
    /// Blob name (may contain `/`).
    blob: String,
    /// Effective snapshot timestamp (CLI `--snapshot` overrides the URL's
    /// `?snapshot=`).
    snapshot: Option<String>,
    /// Effective SAS token (CLI `--sas-token` overrides the URL's SAS query).
    sas: Option<String>,
}

impl Cli {
    fn snapshot_from_env() -> Option<String> {
        std::env::var("AZURE_STORAGE_SNAPSHOT")
            .ok()
            .filter(|s| !s.is_empty())
    }

    /// Resolve the target blob for the single-device subcommands.
    ///
    /// The user-facing path parses the global `--blob-url` into its components,
    /// applying the `--snapshot` / `--sas-token` overrides.  When `--blob-url`
    /// is absent the explicit `AZURE_STORAGE_{ACCOUNT,CONTAINER,BLOB,ENDPOINT}`
    /// environment is used instead: this is the internal contract the CSI node
    /// plugin relies on, which spawns this `run` child with those variables (a
    /// per-volume `%s` subdomain endpoint template cannot be encoded in a single
    /// parseable `--blob-url`).
    fn location(&self) -> anyhow::Result<Location> {
        if let Some(url) = self.blob_url.as_deref() {
            let parsed = bloburl::parse_blob_url(url).context("parse --blob-url")?;
            return Ok(Location {
                endpoint: format!("{}/", parsed.service_url.trim_end_matches('/')),
                account: parsed.account,
                container: parsed.container,
                blob: parsed.blob,
                snapshot: self
                    .snapshot
                    .clone()
                    .or(parsed.snapshot)
                    .or_else(Self::snapshot_from_env),
                sas: self.sas_token.clone().or(parsed.sas),
            });
        }
        self.location_from_env()
    }

    /// Build a [`Location`] from the explicit `AZURE_STORAGE_*` selectors set by
    /// the CSI node plugin on the spawned `run` child.
    fn location_from_env(&self) -> anyhow::Result<Location> {
        let blob = std::env::var("AZURE_STORAGE_BLOB")
            .ok()
            .filter(|s| !s.is_empty())
            .context("AZURE_STORAGE_BLOB is required when --blob-url is not set")?;
        let account = std::env::var("AZURE_STORAGE_ACCOUNT")
            .ok()
            .filter(|s| !s.is_empty())
            .context("AZURE_STORAGE_ACCOUNT is required when --blob-url is not set")?;
        let container = std::env::var("AZURE_STORAGE_CONTAINER")
            .ok()
            .filter(|s| !s.is_empty())
            .context("AZURE_STORAGE_CONTAINER is required when --blob-url is not set")?;
        // The endpoint template may carry a `%s` placeholder for the account
        // (subdomain style, e.g. `http://%s.blob.localhost:10000/`); the
        // single-device child knows its account up-front and substitutes it.
        let endpoint_template = std::env::var("AZURE_STORAGE_ENDPOINT")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| format!("https://{account}.blob.core.windows.net/"));
        let endpoint = endpoint_template.replace("%s", &account);
        Ok(Location {
            endpoint: format!("{}/", endpoint.trim_end_matches('/')),
            account,
            container,
            blob,
            snapshot: self.snapshot.clone().or_else(Self::snapshot_from_env),
            sas: self.sas_token.clone(),
        })
    }
}

/// Build an `AzurePageBlobBackend` for the blob selected by `--blob-url`.  Used
/// by the `run` and `test` subcommands (which target a single, explicitly-named
/// blob).
fn build_azure_backend(
    loc: &Location,
    auth: &AuthConfig,
) -> anyhow::Result<Arc<AzurePageBlobBackend>> {
    let container_client = auth::build_container_client(&loc.endpoint, &loc.container, auth)
        .context("build container client")?;
    let backend = AzurePageBlobBackend::new(container_client, loc.blob.clone());
    let backend = match &loc.snapshot {
        Some(snapshot) => {
            info!(snapshot = %snapshot, "targeting blob snapshot (read-only)");
            backend.with_snapshot(snapshot.clone())
        }
        None => backend,
    };
    Ok(Arc::new(backend))
}

/// Copy a `templateBlobUrl` golden image into the target blob selected by
/// `--blob-url` using a server-side copy. Mirrors what the CSI controller does
/// when provisioning a read-write volume from a template.
#[cfg(feature = "csi")]
async fn run_template_copy(
    cli: &Cli,
    loc: &Location,
    template_url: &str,
    min_size: u64,
) -> anyhow::Result<()> {
    use backend::azure::AzurePageBlobBackend;
    use csi::{copy_template, parse_blob_url, round_up_512};

    let tmpl = parse_blob_url(template_url).context("parse --template-url")?;

    // Authenticate the source with its own SAS when present; otherwise reuse the
    // CLI credentials (the template must then be reachable with them). The source
    // service URL is taken from the template URL's own host so a non-SAS template
    // in a different account/host than the target is read from the right place.
    let src_service_url = format!("{}/", tmpl.service_url.trim_end_matches('/'));
    let src_auth = match &tmpl.sas {
        Some(sas) => AuthConfig::Sas {
            sas_token: sas.clone(),
        },
        None => build_auth(cli, &tmpl.account, None)?,
    };
    let src_container = auth::build_container_client(&src_service_url, &tmpl.container, &src_auth)
        .context("build template container client")?;
    let mut source = AzurePageBlobBackend::new(src_container, tmpl.blob.clone());
    if let Some(snapshot) = &tmpl.snapshot {
        source = source.with_snapshot(snapshot.clone());
    }
    let source_size = source.size().await.context("stat template blob")?;
    let size = round_up_512(source_size.max(min_size));

    let dest_auth = build_auth(cli, &loc.account, loc.sas.as_deref())?;
    let dest = build_azure_backend(loc, &dest_auth)?;
    info!(
        template = %template_url, source_size, target_size = size,
        container = %loc.container, "server-side copy of template into target blob"
    );
    dest.create(size).await.context("create target blob")?;
    copy_template(
        dest.as_ref(),
        &source,
        template_url,
        tmpl.sas.is_some(),
        &dest_auth,
        source_size,
    )
    .await
    .context("copy template blob")?;
    info!("template copy complete");
    Ok(())
}

/// Best-effort node hostname for the CSI `--node-id` default.
#[cfg(feature = "csi")]
fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "unknown-node".to_string())
}

/// Acquire the cluster lease + blob lock for the selected blob and return the
/// guard that keeps them renewed.  Requires the `coordination` build feature.
#[cfg(feature = "coordination")]
#[allow(clippy::too_many_arguments)]
async fn acquire_coordination(
    loc: &Location,
    auth: &AuthConfig,
    recovery_timeout_secs: u64,
    lease_duration_secs: u64,
    lease_namespace: Option<String>,
    lease_name: Option<String>,
    lease_holder: Option<String>,
) -> anyhow::Result<coordination::CoordinationGuard> {
    use coordination::{
        k8s::{sanitize_lease_name, K8sClusterLease},
        CoordinationConfig, Coordinator,
    };
    use std::time::Duration;

    let blob = loc.blob.clone();
    let container_client = auth::build_container_client(&loc.endpoint, &loc.container, auth)
        .context("build container client for blob lock")?;
    let blob_lock = Arc::new(backend::azure::AzureBlobLock::new(
        container_client,
        blob.clone(),
    ));

    let holder = lease_holder
        .filter(|h| !h.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok().filter(|h| !h.is_empty()))
        .unwrap_or_else(|| "unknown-node".to_string());
    let namespace = lease_namespace
        .filter(|n| !n.is_empty())
        .or_else(|| {
            std::env::var("POD_NAMESPACE")
                .ok()
                .filter(|n| !n.is_empty())
        })
        .unwrap_or_else(|| "default".to_string());
    let name = lease_name
        .filter(|n| !n.is_empty())
        .unwrap_or_else(|| sanitize_lease_name(&format!("{}-{}", loc.container, blob)));

    let config = CoordinationConfig::new(
        holder.clone(),
        Duration::from_secs(lease_duration_secs),
        Duration::from_secs(recovery_timeout_secs),
    );

    info!(
        %namespace, %name, %holder,
        recovery_timeout_secs, "connecting to cluster lease"
    );
    let cluster = Arc::new(
        K8sClusterLease::connect(
            &namespace,
            name,
            holder,
            config.lease_duration,
            config.recovery_timeout,
        )
        .await
        .context("connect kubernetes cluster lease")?,
    );

    Coordinator::new(cluster, blob_lock, config).acquire().await
}

/// Stub used when the `coordination` feature is not compiled in: fail loudly so
/// `--coordination` is never silently ignored.
#[cfg(not(feature = "coordination"))]
#[allow(clippy::too_many_arguments)]
async fn acquire_coordination(
    _loc: &Location,
    _auth: &AuthConfig,
    _recovery_timeout_secs: u64,
    _lease_duration_secs: u64,
    _lease_namespace: Option<String>,
    _lease_name: Option<String>,
    _lease_holder: Option<String>,
) -> anyhow::Result<coordination::CoordinationGuard> {
    anyhow::bail!(
        "--coordination requires the `coordination` build feature; \
         rebuild with `--features coordination` (or `--features csi`)"
    )
}

// ── Auth builder ─────────────────────────────────────────────────────────────

/// Build a filesystem-safe cache file base name from the container and blob.
///
/// Container/blob names may contain `/` and other characters; sanitizing to a
/// fixed alphabet (alphanumerics plus `-` and `_`) keeps the cache files inside
/// the chosen `--cache-dir` and prevents path traversal (e.g. `..`).
fn cache_file_name(container: &str, blob: &str) -> String {
    sanitize_cache_component(&format!("{container}-{blob}"))
}

/// Background cache warm-up: sequentially populate `[0, limit_bytes)` of the
/// device through `backend` (the cache layer) so those pages become resident
/// locally — served from a peer when one already holds them, else fetched from
/// the blob and stored as clean pages in the local cache.
///
/// Best-effort: a read error stops the warm-up (the device keeps serving on
/// demand). Yields between pages so it doesn't starve live I/O.
async fn warmup_cache(
    backend: Arc<dyn BlobBackend>,
    dev_size: u64,
    page_size: u64,
    limit_bytes: u64,
) {
    let limit = limit_bytes.min(dev_size);
    let mut offset = 0u64;
    let mut warmed = 0u64;
    while offset < limit {
        let len = page_size.min(dev_size - offset);
        match backend.prefetch(offset, len).await {
            Ok(()) => warmed += len,
            Err(err) => {
                warn!(offset, %err, "cache warm-up read failed; stopping early");
                break;
            }
        }
        offset += len;
        tokio::task::yield_now().await;
    }
    info!(
        warmed_bytes = warmed,
        limit_bytes = limit,
        "cache warm-up complete"
    );
}

/// Sanitize a single string to the cache file-name alphabet (alphanumerics plus
/// `-` and `_`), so an operator-supplied `--cache-instance` / blob identity
/// stays inside `--cache-dir` and cannot traverse paths.
fn sanitize_cache_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    out
}

fn build_auth(cli: &Cli, account: &str, sas: Option<&str>) -> anyhow::Result<AuthConfig> {
    if let Some(sas) = sas {
        return Ok(AuthConfig::Sas {
            sas_token: sas.to_string(),
        });
    }

    if let Some(key) = &cli.account_key {
        return Ok(AuthConfig::SharedKey {
            account_name: account.to_string(),
            account_key: key.clone(),
        });
    }

    if cli.workload_identity {
        return Ok(AuthConfig::WorkloadIdentity {
            client_id: cli.azure_client_id.clone(),
            tenant_id: cli.azure_tenant_id.clone(),
            token_file: cli.azure_federated_token_file.clone(),
        });
    }

    // Prefer user-assigned identities if given, fall back to system-assigned.
    let user_assigned = cli
        .msi_client_id
        .as_ref()
        .map(|s| UserAssignedIdentity::ClientId(s.clone()))
        .or_else(|| {
            cli.msi_object_id
                .as_ref()
                .map(|s| UserAssignedIdentity::ObjectId(s.clone()))
        })
        .or_else(|| {
            cli.msi_resource_id
                .as_ref()
                .map(|s| UserAssignedIdentity::ResourceId(s.clone()))
        });

    if user_assigned.is_some() || cli.msi {
        return Ok(AuthConfig::Msi(user_assigned));
    }

    if let (Some(tenant_id), Some(client_id), Some(client_secret)) = (
        cli.azure_tenant_id.as_ref(),
        cli.azure_client_id.as_ref(),
        cli.azure_client_secret.as_ref(),
    ) {
        return Ok(AuthConfig::ServicePrincipal {
            tenant_id: tenant_id.clone(),
            client_id: client_id.clone(),
            client_secret: client_secret.clone(),
        });
    }

    anyhow::bail!(
        "No auth method specified. Use --account-key for Azurite/dev, \
         --workload-identity for AKS (federated token), \
         --msi / --msi-client-id for Managed Identity, \
         or AZURE_CLIENT_ID + AZURE_TENANT_ID + AZURE_CLIENT_SECRET for a service principal."
    );
}

// ── Smoke test ────────────────────────────────────────────────────────────────

/// Quick write → read-back → clear → read-zero cycle to verify connectivity.
async fn run_smoke_test(backend: Arc<dyn BlobBackend>, size: u64) -> anyhow::Result<()> {
    info!(size, "provisioning test blob");
    backend.create(size).await.context("create")?;

    let pattern = bytes::Bytes::from(vec![0xABu8; 512]);
    info!("writing pattern to first page");
    backend.write(0, pattern.clone()).await.context("write")?;

    let read_back = backend.read(0, 512).await.context("read")?;
    if read_back != pattern {
        error!("read-back mismatch!");
        anyhow::bail!("smoke test FAILED: read-back mismatch");
    }

    info!("clearing first page");
    backend.clear(0, 512).await.context("clear")?;

    let zeroed = backend.read(0, 512).await.context("read after clear")?;
    if !zeroed.iter().all(|&b| b == 0) {
        error!("page not zeroed after clear!");
        anyhow::bail!("smoke test FAILED: page not zeroed after clear");
    }

    info!("smoke test PASSED ✓");
    Ok(())
}

#[cfg(test)]
mod warmup_tests {
    use super::*;
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::sync::Mutex;

    /// Records every `read(offset, len)` so a test can assert the warm-up's
    /// access pattern.
    #[derive(Default)]
    struct RecordingBackend {
        size: u64,
        reads: Mutex<Vec<(u64, u64)>>,
    }

    #[async_trait]
    impl BlobBackend for RecordingBackend {
        async fn create(&self, _size: u64) -> anyhow::Result<()> {
            Ok(())
        }
        async fn read(&self, offset: u64, len: u64) -> anyhow::Result<Bytes> {
            self.reads.lock().unwrap().push((offset, len));
            Ok(Bytes::from(vec![0u8; len as usize]))
        }
        async fn write(&self, _offset: u64, _data: Bytes) -> anyhow::Result<()> {
            Ok(())
        }
        async fn clear(&self, _offset: u64, _len: u64) -> anyhow::Result<()> {
            Ok(())
        }
        async fn flush(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn delete(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn size(&self) -> anyhow::Result<u64> {
            Ok(self.size)
        }
    }

    #[tokio::test]
    async fn warmup_scans_whole_device_in_page_chunks() {
        let b = Arc::new(RecordingBackend {
            size: 8192,
            ..Default::default()
        });
        warmup_cache(b.clone(), 8192, 4096, 8192).await;
        assert_eq!(*b.reads.lock().unwrap(), vec![(0, 4096), (4096, 4096)]);
    }

    #[tokio::test]
    async fn warmup_stops_at_limit() {
        let b = Arc::new(RecordingBackend {
            size: 8192,
            ..Default::default()
        });
        // limit = one page
        warmup_cache(b.clone(), 8192, 4096, 4096).await;
        assert_eq!(*b.reads.lock().unwrap(), vec![(0, 4096)]);
    }

    #[tokio::test]
    async fn warmup_last_chunk_is_clamped_to_device() {
        let b = Arc::new(RecordingBackend {
            size: 6144,
            ..Default::default()
        });
        // limit > dev_size is clamped; last chunk is the partial tail.
        warmup_cache(b.clone(), 6144, 4096, u64::MAX).await;
        assert_eq!(*b.reads.lock().unwrap(), vec![(0, 4096), (4096, 2048)]);
    }
}
