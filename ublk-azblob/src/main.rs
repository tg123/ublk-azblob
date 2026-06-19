//! `ublk-azblob` — Linux ublk block device backed by an Azure Page Blob.
//!
//! # Usage
//!
//! ```text
//! ublk-azblob [STORAGE/AUTH OPTIONS] <run|test> --size <SIZE>
//! ```
//!
//! `--size` (and other per-command options) belong to the `run`/`test`
//! subcommands; auth and storage selectors are global options.
//!
//! Folder import and snapshot creation are provided by the standalone
//! `ublk-azblob-import` and `ublk-azblob-snapshot` tools.
//!
//! See `--help` and `README.md` for full documentation.

use anyhow::Context as _;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;
use ublk_azblob::backend::{
    buffered::{BufferedBackend, BufferedConfig},
    file::{FileCacheBackend, FileCacheConfig},
    BlobBackend,
};
use ublk_azblob::cli::{init_tracing, StorageArgs};
use ublk_azblob::{nbd_target, ublk_target};

use bytes::Bytes;
use tracing::{error, info};

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "ublk-azblob",
    about = "Expose an Azure Page Blob as a Linux ublk block device (/dev/ublkbN)",
    version
)]
struct Cli {
    #[command(flatten)]
    storage: StorageArgs,

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
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let cli = Cli::parse();
    let backend: Arc<dyn BlobBackend> = cli.storage.build_backend()?;

    match cli.command {
        Command::Run {
            size,
            create,
            nr_queues,
            queue_depth,
            id,
            page_size,
            max_dirty_pages,
            cache_dir,
            cache_page_size,
            nbd,
        } => {
            if create {
                info!(size, blob = %cli.storage.blob, "creating page blob");
                backend.create(size).await.context("create page blob")?;
            }

            let actual_size = backend.size().await.context("get blob size")?;
            info!(size = actual_size, blob = %cli.storage.blob, "blob ready");

            // Optional local-disk cache layer (memory → local disk → blob).
            let backend: Arc<dyn BlobBackend> = if let Some(dir) = cache_dir {
                info!(
                    cache_dir = %dir.display(),
                    cache_page_size,
                    "local-disk cache enabled"
                );
                let (cache, recovered_dirty) = FileCacheBackend::open(
                    backend,
                    FileCacheConfig {
                        dir,
                        name: cache_file_name(&cli.storage.container, &cli.storage.blob),
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
            if let Some(addr) = nbd {
                info!(addr = %addr, "starting NBD compatibility server");
                nbd_target::run_nbd_target(backend, &addr, actual_size)
                    .await
                    .context("nbd target")?;
            } else {
                ublk_target::run_ublk_target(backend, cfg)
                    .await
                    .context("ublk target")?;
            }
        }

        Command::Test { size } => {
            run_smoke_test(backend, size).await?;
        }
    }

    Ok(())
}

// ── Helpers ──────────────────────────────────────────────────────────────────

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

// ── Smoke test ────────────────────────────────────────────────────────────────

/// Quick write → read-back → clear → read-zero cycle to verify connectivity.
async fn run_smoke_test(backend: Arc<dyn BlobBackend>, size: u64) -> anyhow::Result<()> {
    info!(size, "provisioning test blob");
    backend.create(size).await.context("create")?;

    let pattern = Bytes::from(vec![0xABu8; 512]);
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
