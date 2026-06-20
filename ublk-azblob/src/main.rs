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
mod blob_url;
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
use tracing::{error, info};

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
    /// A snapshot is an immutable, point-in-time view of the blob.  Selecting a
    /// snapshot implies read-only mode: the device is exposed read-only and all
    /// write/discard operations are rejected.  May also be supplied as a
    /// `?snapshot=` query on `--blob-url`.
    #[arg(long, env = "AZURE_STORAGE_SNAPSHOT")]
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

        /// Expose the device read-only.
        ///
        /// The ublk device / NBD export is advertised read-only and every
        /// write, discard, and write-zeroes request is rejected.  Implied when
        /// `--snapshot` is set.
        #[arg(
            long,
            env = "UBLK_READ_ONLY",
            num_args = 0..=1,
            default_value_t = false,
            default_missing_value = "true",
            value_parser = clap::builder::BoolishValueParser::new(),
        )]
        read_only: bool,

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
            read_only,
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
            idle_flush_secs,
            force_flush_timeout_secs,
            flush_io_timeout_secs,
            ref nbd,
        } => {
            let loc = cli.location()?;
            let auth = build_auth(&cli, &loc.account, loc.sas.as_deref())?;
            // A snapshot is immutable, so selecting one forces read-only mode.
            let read_only = read_only || loc.snapshot.is_some();
            if read_only {
                info!("read-only mode: writes, discards, and creation are disabled");
            }
            let azure_backend = build_azure_backend(&loc, &auth)?;
            if create {
                if read_only {
                    anyhow::bail!(
                        "--create cannot be used in read-only mode (--read-only/--snapshot)"
                    );
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
                info!(
                    cache_dir = %dir.display(),
                    cache_page_size,
                    "local-disk cache enabled"
                );
                let (cache, recovered_dirty) = FileCacheBackend::open(
                    backend,
                    FileCacheConfig {
                        dir,
                        name: cache_file_name(&loc.container, &loc.blob),
                        page_size: cache_page_size,
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
                cache
            } else {
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
    /// Parse the global `--blob-url` into its components for the single-device
    /// subcommands, applying the `--snapshot` / `--sas-token` overrides.
    fn location(&self) -> anyhow::Result<Location> {
        let url = self
            .blob_url
            .as_deref()
            .context("--blob-url (AZURE_STORAGE_BLOB_URL) is required for this subcommand")?;
        let parsed = blob_url::parse_blob_url(url).context("parse --blob-url")?;
        Ok(Location {
            endpoint: format!("{}/", parsed.service_url.trim_end_matches('/')),
            account: parsed.account,
            container: parsed.container,
            blob: parsed.blob,
            snapshot: self.snapshot.clone().or(parsed.snapshot),
            sas: self.sas_token.clone().or(parsed.sas),
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
    let mut name = String::with_capacity(container.len() + blob.len() + 1);
    for ch in format!("{container}-{blob}").chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            name.push(ch);
        } else {
            name.push('_');
        }
    }
    name
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
