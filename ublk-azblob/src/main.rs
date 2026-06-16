//! `ublk-azblob` — Linux ublk block device backed by an Azure Page Blob.
//!
//! # Usage
//!
//! ```text
//! ublk-azblob [GLOBAL OPTIONS] --account <ACCOUNT> --container <CONTAINER> --blob <BLOB> \
//!     <run|test> --size <SIZE>
//! ```
//!
//! `--size` (and other per-command options) belong to the `run`/`test`
//! subcommands; auth and storage selectors are global options.
//!
//! See `--help` and `README.md` for full documentation.

mod auth;
mod backend;
mod coordination;
#[cfg(feature = "csi")]
mod csi;
mod ublk_target;

use anyhow::Context as _;
use auth::{AuthConfig, UserAssignedIdentity};
use backend::{
    azure::AzurePageBlobBackend,
    buffered::{BufferedBackend, BufferedConfig},
    BlobBackend,
};
use clap::{Parser, Subcommand};
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
    /// Azure Storage account name (e.g. `mystorageaccount`).
    #[arg(long, env = "AZURE_STORAGE_ACCOUNT")]
    account: String,

    /// Blob container name.
    #[arg(long, env = "AZURE_STORAGE_CONTAINER")]
    container: String,

    /// Page blob name (path within the container).
    ///
    /// Required by the `run` and `test` subcommands.  The `csi` subcommand picks
    /// a per-volume blob name from the Kubernetes volume id, so it is optional
    /// there.
    #[arg(long, env = "AZURE_STORAGE_BLOB")]
    blob: Option<String>,

    /// Azure Storage service endpoint URL.
    ///
    /// Defaults to `https://<account>.blob.core.windows.net/`.
    /// For Azurite use `http://127.0.0.1:10000/<account>`.
    #[arg(long, env = "AZURE_STORAGE_ENDPOINT")]
    endpoint: Option<String>,

    /// Storage account key (base64).  Enables SharedKey auth mode.
    ///
    /// Mutually exclusive with --msi / --msi-*.  Use for Azurite and local dev.
    #[arg(long, env = "AZURE_STORAGE_KEY", conflicts_with_all = ["msi", "msi_client_id", "msi_object_id", "msi_resource_id"])]
    account_key: Option<String>,

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
        #[arg(long, env = "UBLK_COORDINATION")]
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
    },

    /// Just test the BlobBackend connection (write → read → clear → verify).
    Test {
        /// Device size to use for the test blob.
        #[arg(long, default_value = "4096")]
        size: u64,
    },

    /// Run the Kubernetes CSI driver (Container Storage Interface gRPC server).
    ///
    /// Lets a Kubernetes PersistentVolumeClaim be backed by an Azure Page Blob
    /// exposed through a ublk device.  Requires the `csi` build feature (and the
    /// `ublk` feature on the node side).
    #[cfg(feature = "csi")]
    Csi {
        /// CSI endpoint to listen on (`unix:///csi/csi.sock` or `tcp://addr:port`).
        #[arg(long, env = "CSI_ENDPOINT", default_value = "unix:///csi/csi.sock")]
        csi_endpoint: String,

        /// Node identifier reported to Kubernetes (typically the node name).
        #[arg(long, env = "CSI_NODE_ID", default_value = "")]
        node_id: String,

        /// Which CSI services to serve: `controller`, `node`, or `all`.
        #[arg(long, env = "CSI_ROLE", default_value = "all")]
        role: csi::Role,
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
    let endpoint = cli
        .endpoint
        .clone()
        .unwrap_or_else(|| format!("https://{}.blob.core.windows.net/", cli.account));

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
        } => {
            let backend = build_azure_backend(&cli, &endpoint)?;
            if create {
                info!(size, "creating page blob");
                backend.create(size).await.context("create page blob")?;
            }

            let actual_size = backend.size().await.context("get blob size")?;
            info!(size = actual_size, "blob ready");

            // Acquire cluster + blob coordination locks before mounting (after
            // the blob exists, so its lease can be taken).
            let guard = if coordination {
                Some(
                    acquire_coordination(
                        &cli,
                        &endpoint,
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

            // Wrap with write-back buffer if page_size > 0.
            let backend: Arc<dyn BlobBackend> = if page_size > 0 {
                info!(page_size, max_dirty_pages, "write-back buffer enabled");
                Arc::new(
                    BufferedBackend::new(
                        backend,
                        BufferedConfig {
                            page_size,
                            max_dirty_pages,
                        },
                    )
                    .context("configure write-back buffer")?,
                )
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
            };
            let result = ublk_target::run_ublk_target(backend, cfg)
                .await
                .context("ublk target");

            // Release the leases so another node can take over immediately.
            if let Some(guard) = guard {
                guard.release().await;
            }
            result?;
        }

        Command::Test { size } => {
            let backend = build_azure_backend(&cli, &endpoint)?;
            run_smoke_test(backend, size).await?;
        }

        #[cfg(feature = "csi")]
        Command::Csi {
            csi_endpoint,
            node_id,
            role,
        } => {
            let config = csi::DriverConfig {
                account: cli.account.clone(),
                endpoint: endpoint.clone(),
                default_container: cli.container.clone(),
                account_key: cli.account_key.clone(),
                use_msi: cli.msi
                    || cli.msi_client_id.is_some()
                    || cli.msi_object_id.is_some()
                    || cli.msi_resource_id.is_some(),
                msi_client_id: cli.msi_client_id.clone(),
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

/// Build an `AzurePageBlobBackend` for the blob selected by the global CLI
/// options.  Used by the `run` and `test` subcommands (which target a single,
/// explicitly-named blob).
fn build_azure_backend(cli: &Cli, endpoint: &str) -> anyhow::Result<Arc<dyn BlobBackend>> {
    let blob = cli
        .blob
        .clone()
        .context("--blob (AZURE_STORAGE_BLOB) is required for this subcommand")?;
    let auth = build_auth(cli)?;
    let container_client = auth::build_container_client(endpoint, &cli.container, &auth)
        .context("build container client")?;
    Ok(Arc::new(AzurePageBlobBackend::new(container_client, blob)))
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
    cli: &Cli,
    endpoint: &str,
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

    let blob = cli
        .blob
        .clone()
        .context("--blob (AZURE_STORAGE_BLOB) is required for coordination")?;
    let auth = build_auth(cli)?;
    let container_client = auth::build_container_client(endpoint, &cli.container, &auth)
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
        .unwrap_or_else(|| sanitize_lease_name(&format!("{}-{}", cli.container, blob)));

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
    _cli: &Cli,
    _endpoint: &str,
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

fn build_auth(cli: &Cli) -> anyhow::Result<AuthConfig> {
    if let Some(key) = &cli.account_key {
        return Ok(AuthConfig::SharedKey {
            account_name: cli.account.clone(),
            account_key: key.clone(),
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

    anyhow::bail!(
        "No auth method specified. Use --account-key for Azurite/dev, \
         or --msi / --msi-client-id for production (Managed Identity)."
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
