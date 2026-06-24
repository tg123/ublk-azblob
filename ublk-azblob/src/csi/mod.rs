//! Kubernetes **CSI** (Container Storage Interface) driver for `ublk-azblob`.
//!
//! This module lets a Kubernetes `PersistentVolumeClaim` be backed by an Azure
//! Page Blob exposed through a ublk block device.  It implements the three CSI
//! gRPC services:
//!
//! * **Identity** — plugin name / capabilities / health probe.
//! * **Controller** — `CreateVolume` / `DeleteVolume` (provisions and removes the
//!   backing page blob).  Runs as a Kubernetes `Deployment` alongside the
//!   `external-provisioner` sidecar.
//! * **Node** — `NodePublishVolume` / `NodeUnpublishVolume` (starts a ublk
//!   device over the page blob, makes a filesystem and mounts it at the path the
//!   kubelet requests).  Runs as a `DaemonSet` alongside `node-driver-registrar`.
//!
//! The driver is gated behind the `csi` Cargo feature.  The node side also needs
//! the `ublk` feature to attach real block devices — build the node image with
//! `--features "ublk csi"`.
//!
//! ## Volume model
//!
//! One PVC maps to exactly one page blob.  The CSI *volume id* encodes the
//! storage account, container and blob name as `"{account}/{container}/{blob}"`.
//! The account is deliberately encoded so that `DeleteVolume` (which only
//! receives the volume id and secrets, never the `StorageClass` parameters) can
//! recover the per-volume account chosen at create time — the account may be
//! overridden per volume via the `storageAccount` `StorageClass` parameter, not
//! just the driver-level default.  The service endpoint is driver-level
//! configuration supplied to every replica via environment variables / flags.

pub mod controller;
pub mod identity;
pub mod mount;
pub mod node;

/// Generated CSI v1 protobuf / gRPC bindings (`csi.v1` package).
#[allow(
    clippy::all,
    clippy::pedantic,
    missing_docs,
    non_camel_case_types,
    rustdoc::all
)]
pub mod proto {
    tonic::include_proto!("csi.v1");
}

use anyhow::Context as _;
use std::path::Path;
use tonic::transport::Server;
use tracing::{info, warn};

use crate::auth::{self, AuthConfig, UserAssignedIdentity};
use crate::backend::{azure::AzurePageBlobBackend, BlobBackend};
use std::collections::HashMap;
use std::sync::Arc;

use proto::controller_server::ControllerServer;
use proto::identity_server::IdentityServer;
use proto::node_server::NodeServer;

/// CSI driver name advertised to Kubernetes.  Must match the `CSIDriver`
/// object's `metadata.name` and the StorageClass `provisioner`.
pub const DRIVER_NAME: &str = "azblob.ublk.csi.tg123.github.io";

/// Driver version reported by `GetPluginInfo` (the crate version).
pub const DRIVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Which CSI services this process should serve.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Role {
    /// Controller service only (provisioning Deployment).
    Controller,
    /// Node service only (per-node DaemonSet).
    Node,
    /// Both controller and node — handy for single-binary / local testing.
    All,
}

impl std::str::FromStr for Role {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "controller" => Ok(Role::Controller),
            "node" => Ok(Role::Node),
            "all" => Ok(Role::All),
            other => anyhow::bail!("invalid CSI role '{other}' (expected controller|node|all)"),
        }
    }
}

/// Driver-level storage configuration shared by every CSI request.
///
/// Per-volume selectors (the blob name, and optionally the container) are
/// carried in the CSI volume id / volume context; this struct holds the
/// account-wide settings that are the same for the whole driver deployment.
#[derive(Debug, Clone)]
pub struct DriverConfig {
    /// Azure Storage account name.
    pub account: String,
    /// Service endpoint URL (defaults to `https://<account>.blob.core.windows.net/`).
    pub endpoint: String,
    /// Default container used when a StorageClass does not set one.
    pub default_container: String,
    /// Account key for SharedKey auth (Azurite / local dev), if configured.
    pub account_key: Option<String>,
    /// Use Managed Identity when no account key is provided.
    pub use_msi: bool,
    /// Optional user-assigned Managed Identity client id.
    pub msi_client_id: Option<String>,
    /// Use Microsoft Entra Workload Identity (federated Kubernetes token).
    pub use_workload_identity: bool,
    /// Optional Workload Identity client id (defaults to `AZURE_CLIENT_ID`).
    pub workload_identity_client_id: Option<String>,
    /// Optional Workload Identity tenant id (defaults to `AZURE_TENANT_ID`).
    pub workload_identity_tenant_id: Option<String>,
    /// Optional federated token file path (defaults to `AZURE_FEDERATED_TOKEN_FILE`).
    pub workload_identity_token_file: Option<String>,
    /// Service principal Entra ID tenant id (client-secret auth).
    pub sp_tenant_id: Option<String>,
    /// Service principal application (client) id (client-secret auth).
    pub sp_client_id: Option<String>,
    /// Service principal client secret (client-secret auth).
    pub sp_client_secret: Option<String>,
    /// Use NBD instead of ublk for node devices (compatibility mode).
    pub use_nbd: bool,
    /// NBD listen address prefix (e.g. `127.0.0.1`).
    pub nbd_host: String,
    /// Starting port for NBD servers (each volume gets host:port+N).
    pub nbd_port_start: u16,
}

/// Run the CSI gRPC server, listening on `endpoint` until the process is
/// signalled.  `endpoint` is a CSI endpoint URL: either `unix:///path/to.sock`
/// or `tcp://host:port`.
pub async fn run_csi(
    endpoint: &str,
    role: Role,
    node_id: String,
    config: DriverConfig,
) -> anyhow::Result<()> {
    let identity = identity::IdentityService::new(role);
    let mut builder = Server::builder().add_service(IdentityServer::new(identity));

    if matches!(role, Role::Controller | Role::All) {
        info!("enabling CSI controller service");
        let controller = controller::ControllerService::new(config.clone());
        builder = builder.add_service(ControllerServer::new(controller));
    }
    if matches!(role, Role::Node | Role::All) {
        info!(node_id = %node_id, "enabling CSI node service");
        let node = node::NodeService::new(node_id, config);
        builder = builder.add_service(NodeServer::new(node));
    }

    info!(endpoint, ?role, "CSI driver listening");
    serve(builder, endpoint).await
}

/// Bind the configured transport and serve until shutdown.
async fn serve(router: tonic::transport::server::Router, endpoint: &str) -> anyhow::Result<()> {
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
        info!("shutdown signal received, stopping CSI server");
    };

    if let Some(path) = endpoint.strip_prefix("unix://") {
        // A stale socket from a previous run would make bind() fail.
        if Path::new(path).exists() {
            let _ = std::fs::remove_file(path);
        }
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create socket directory {parent:?}"))?;
        }
        let listener = tokio::net::UnixListener::bind(path)
            .with_context(|| format!("bind unix socket {path}"))?;
        let stream = tokio_stream::wrappers::UnixListenerStream::new(listener);
        router
            .serve_with_incoming_shutdown(stream, shutdown)
            .await
            .context("CSI server (unix) terminated")?;
    } else {
        let addr = endpoint
            .strip_prefix("tcp://")
            .unwrap_or(endpoint)
            .parse()
            .with_context(|| format!("parse tcp endpoint {endpoint}"))?;
        router
            .serve_with_shutdown(addr, shutdown)
            .await
            .context("CSI server (tcp) terminated")?;
    }
    Ok(())
}

/// Encode an `(account, container, blob)` triple as a CSI volume id.
///
/// The account is encoded so that `DeleteVolume` — which CSI only hands the
/// volume id and secrets, never the StorageClass parameters — can recover the
/// per-volume storage account chosen at create time. Account and container
/// names cannot contain `/`, so splitting on the first two `/` is unambiguous
/// even though blob names may themselves contain slashes.
pub fn make_volume_id(account: &str, container: &str, blob: &str) -> String {
    format!("{account}/{container}/{blob}")
}

/// Prefix marking a read-only *template* volume id (see [`make_volume_id_ro`]).
const RO_VOLUME_ID_PREFIX: &str = "ro:";

/// Encode a read-only **template** volume id.
///
/// Read-only/snapshot volumes provisioned from a `templateBlobUrl` point many
/// PVCs at one shared golden-image blob. The `ro:` marker lets `DeleteVolume`
/// recognise them and skip deletion, so removing a PVC never deletes the shared
/// template.
pub fn make_volume_id_ro(account: &str, container: &str, blob: &str) -> String {
    format!("{RO_VOLUME_ID_PREFIX}{account}/{container}/{blob}")
}

/// Decode a CSI volume id produced by [`make_volume_id`] / [`make_volume_id_ro`]
/// into `(read_only, account, container, blob)`.
pub fn parse_volume_id(volume_id: &str) -> anyhow::Result<(bool, String, String, String)> {
    let (read_only, rest) = match volume_id.strip_prefix(RO_VOLUME_ID_PREFIX) {
        Some(rest) => (true, rest),
        None => (false, volume_id),
    };
    let mut parts = rest.splitn(3, '/');
    let account = parts.next().unwrap_or("");
    let container = parts.next().unwrap_or("");
    let blob = parts.next().unwrap_or("");
    if account.is_empty() || container.is_empty() || blob.is_empty() {
        anyhow::bail!("malformed volume id '{volume_id}' (expected '[ro:]account/container/blob')");
    }
    Ok((
        read_only,
        account.to_string(),
        container.to_string(),
        blob.to_string(),
    ))
}

/// Select the driver's auth config for `account`, honouring per-request secrets
/// (e.g. a SharedKey `accountKey`) the same way [`build_backend`] does.
pub fn select_driver_auth(
    config: &DriverConfig,
    account: &str,
    secrets: &HashMap<String, String>,
) -> anyhow::Result<AuthConfig> {
    let account_key = secrets
        .get("accountKey")
        .cloned()
        .or_else(|| config.account_key.clone());

    if let Some(key) = account_key {
        Ok(AuthConfig::SharedKey {
            account_name: account.to_string(),
            account_key: key,
        })
    } else if config.use_workload_identity {
        Ok(AuthConfig::WorkloadIdentity {
            client_id: config.workload_identity_client_id.clone(),
            tenant_id: config.workload_identity_tenant_id.clone(),
            token_file: config.workload_identity_token_file.clone(),
        })
    } else if config.use_msi || config.msi_client_id.is_some() {
        Ok(AuthConfig::Msi(
            config
                .msi_client_id
                .clone()
                .map(UserAssignedIdentity::ClientId),
        ))
    } else if let (Some(tenant_id), Some(client_id), Some(client_secret)) = (
        secrets
            .get("AZURE_TENANT_ID")
            .or(config.sp_tenant_id.as_ref()),
        secrets
            .get("AZURE_CLIENT_ID")
            .or(config.sp_client_id.as_ref()),
        secrets
            .get("AZURE_CLIENT_SECRET")
            .or(config.sp_client_secret.as_ref()),
    ) {
        Ok(AuthConfig::ServicePrincipal {
            tenant_id: tenant_id.clone(),
            client_id: client_id.clone(),
            client_secret: client_secret.clone(),
        })
    } else {
        anyhow::bail!(
            "no auth configured: provide an account key (secret 'accountKey' or \
             --account-key), enable Workload Identity (--workload-identity), \
             enable Managed Identity (--msi / --msi-client-id), \
             or a service principal (AZURE_CLIENT_ID / AZURE_TENANT_ID / AZURE_CLIENT_SECRET)"
        );
    }
}

/// The subdomain-style endpoint URL for `account` under the driver config.
fn account_endpoint(config: &DriverConfig, account: &str) -> String {
    if config.endpoint.contains("%s") {
        config.endpoint.replace("%s", account)
    } else {
        format!("https://{account}.blob.core.windows.net/")
    }
}

/// Build an Azure Page Blob backend for `container`/`blob` using the driver
/// configuration, allowing per-request `secrets` (e.g. a SharedKey
/// `accountKey`) to override the account key.
pub fn build_backend(
    config: &DriverConfig,
    account: &str,
    container: &str,
    blob: &str,
    secrets: &HashMap<String, String>,
) -> anyhow::Result<Arc<dyn BlobBackend>> {
    let auth = select_driver_auth(config, account, secrets)?;
    let endpoint = account_endpoint(config, account);
    let container_client = auth::build_container_client(&endpoint, container, &auth)
        .context("build container client")?;
    Ok(Arc::new(AzurePageBlobBackend::new(
        container_client,
        blob.to_string(),
    )))
}

/// Build a read backend for a parsed `templateBlobUrl` source.
///
/// Authenticates with the URL's SAS token when present; otherwise falls back to
/// the driver's own credentials (the source must then live in an account the
/// driver can reach). The source snapshot, if any, is applied so the copy/mount
/// reads the immutable point-in-time view.
pub fn build_template_backend(
    config: &DriverConfig,
    tmpl: &TemplateBlobRef,
    secrets: &HashMap<String, String>,
) -> anyhow::Result<Arc<dyn BlobBackend>> {
    let (service_url, auth) = if let Some(sas) = &tmpl.sas {
        (
            format!("{}/", tmpl.service_url.trim_end_matches('/')),
            AuthConfig::Sas {
                sas_token: sas.clone(),
            },
        )
    } else {
        let auth = select_driver_auth(config, &tmpl.account, secrets)?;
        (account_endpoint(config, &tmpl.account), auth)
    };
    let container_client = auth::build_container_client(&service_url, &tmpl.container, &auth)
        .context("build template container client")?;
    let mut backend = AzurePageBlobBackend::new(container_client, tmpl.blob.clone());
    // Auth-wired pipeline for `Get Page Ranges` so the copy can query the
    // source's sparseness map and skip its zero ranges; best-effort.
    match auth::build_pipeline(&auth) {
        Ok(pipeline) => backend = backend.with_page_list(pipeline),
        Err(err) => {
            warn!(%err, "source page-ranges query disabled (could not build auth pipeline)")
        }
    }
    if let Some(snapshot) = &tmpl.snapshot {
        backend = backend.with_snapshot(snapshot.clone());
    }
    Ok(Arc::new(backend))
}

/// Like [`build_backend`] but returns the concrete [`AzurePageBlobBackend`] and
/// the [`AuthConfig`] used, so the caller can issue server-side operations
/// (e.g. [`copy_template`]) that need both.
pub fn build_backend_concrete(
    config: &DriverConfig,
    account: &str,
    container: &str,
    blob: &str,
    secrets: &HashMap<String, String>,
) -> anyhow::Result<(AzurePageBlobBackend, AuthConfig)> {
    let auth = select_driver_auth(config, account, secrets)?;
    let endpoint = account_endpoint(config, account);
    let container_client = auth::build_container_client(&endpoint, container, &auth)
        .context("build container client")?;
    Ok((
        AzurePageBlobBackend::new(container_client, blob.to_string()),
        auth,
    ))
}

/// Copy a `templateBlobUrl` golden image into `dest`, preferring a true
/// server-side copy.
///
/// - When the source carries a SAS, or the driver uses Entra auth (so a
///   copy-source bearer token can be minted), the copy is done with
///   `Put Page From URL` — the storage service fetches each range directly from
///   the source, so **no bytes flow through this process** and it scales to any
///   size (incl. cross-account sources).
/// - Otherwise (SharedKey / account-key auth with no source SAS) the storage
///   service can't authenticate the source read, so the copy falls back to a
///   streamed copy through `source` (download → upload).
///
/// `source_url` is the raw `templateBlobUrl` (already carrying any `snapshot=` /
/// SAS query). `dest` must already exist and be at least `total_size`; it need
/// **not** be freshly zeroed — zero ranges are cleared on the destination (see
/// below), so a retry against an existing same-size blob is safe.
///
/// When the `source` can report its sparseness map (via
/// [`BlobBackend::data_ranges`]), ranges that the source never wrote are
/// **cleared** on the destination rather than copied: neither the server-side
/// nor the streamed path reads them from the source, but both issue
/// `Clear Pages` / `clear` so the destination reads back as zero there even if
/// it previously held data. A source that cannot report ranges degrades to
/// copying every byte.
pub async fn copy_template(
    dest: &AzurePageBlobBackend,
    source: &dyn BlobBackend,
    source_url: &str,
    source_has_sas: bool,
    dest_auth: &AuthConfig,
    total_size: u64,
) -> anyhow::Result<()> {
    // Best-effort source sparseness map: lets both copy paths clear (instead of
    // copying) the source's unwritten free space on the destination, so a
    // non-empty/retried target is still zeroed there. A missing or errored map
    // copies in full.
    let data_ranges = match source.data_ranges().await {
        Ok(ranges) => ranges,
        Err(err) => {
            warn!(%err, "source data-ranges query failed; copying the whole blob");
            None
        }
    };
    if let Some(ranges) = &data_ranges {
        let data_bytes: u64 = ranges.iter().map(|&(_, len)| len).sum();
        info!(
            data_ranges = ranges.len(),
            data_bytes, "copy using source sparseness map (skipping zero ranges)"
        );
    }
    let data_ranges = data_ranges.as_deref();

    // A SAS in the URL authenticates the source itself; otherwise the storage
    // service needs an Entra copy-source authorization (minted per batch inside
    // `copy_pages_from_url`). SharedKey with no SAS can't authenticate a
    // server-side source read, so we probe for a token and fall back to streaming.
    let entra_token = if source_has_sas {
        None
    } else {
        auth::storage_bearer_token(dest_auth)
            .await
            .context("mint copy-source authorization token")?
    };
    if source_has_sas {
        info!(
            total_size,
            "server-side copy (Put Page From URL, SAS source)"
        );
        dest.copy_pages_from_url(source_url, total_size, None, data_ranges)
            .await
    } else if entra_token.is_some() {
        info!(
            total_size,
            "server-side copy (Put Page From URL, Entra source)"
        );
        // Pass the auth (not the probe token) so the token is re-minted per batch.
        dest.copy_pages_from_url(source_url, total_size, Some(dest_auth.clone()), data_ranges)
            .await
    } else {
        info!(
            total_size,
            "no copy-source authorization (SharedKey, no SAS); streaming the copy"
        );
        copy_blob_streamed(source, dest, total_size, data_ranges).await
    }
}

/// Streamed, chunked copy of `total_size` bytes from `source` into `dest` (the
/// fallback used by [`copy_template`] when a server-side copy isn't possible).
///
/// Copies in 4 MiB page-aligned chunks; sparse source ranges read back as zeros.
///
/// `source_data_ranges` is the source sparseness map: when `Some`, chunks lying
/// entirely in a zero gap are **cleared** on `dest` (via `clear`) rather than
/// copied — neither read from the source nor written from it — so the
/// destination reads back as zero there even when it is not a freshly-created
/// blob (e.g. a retry against an existing same-size target).
async fn copy_blob_streamed(
    source: &dyn BlobBackend,
    dest: &dyn BlobBackend,
    total_size: u64,
    source_data_ranges: Option<&[(u64, u64)]>,
) -> anyhow::Result<()> {
    use crate::backend::io_gateway::{with_class, IoClass};
    // Tag both the source reads (downloads) and destination writes (uploads) as
    // copy traffic so the I/O gateway prioritizes foreground reads and flushes
    // ahead of this bulk copy.
    with_class(IoClass::Copy, async move {
        let chunk = crate::backend::copy_chunk_bytes();
        let mut offset = 0u64;
        let mut copied_bytes = 0u64;
        let mut cleared_bytes = 0u64;
        while offset < total_size {
            let len = chunk.min(total_size - offset);
            if let Some(ranges) = source_data_ranges {
                if !crate::backend::range_intersects(ranges, offset, len) {
                    // Zero gap in the source: clear the destination range rather
                    // than copying zeros. This avoids the source read but still
                    // guarantees the destination reads back as zero there even
                    // when it is not a freshly-created blob (create() is
                    // idempotent and does not zero an existing same-size target,
                    // so a retry could otherwise retain stale data and corrupt
                    // the clone).
                    dest.clear(offset, len)
                        .await
                        .with_context(|| format!("clear copy offset={offset} len={len}"))?;
                    cleared_bytes += len;
                    offset += len;
                    continue;
                }
            }
            let data = source
                .read(offset, len)
                .await
                .with_context(|| format!("read template offset={offset} len={len}"))?;
            dest.write(offset, data)
                .await
                .with_context(|| format!("write copy offset={offset} len={len}"))?;
            copied_bytes += len;
            offset += len;
        }
        dest.flush().await.context("flush copied blob")?;
        if source_data_ranges.is_some() {
            info!(
                copied_bytes,
                cleared_bytes, total_size, "streamed copy cleared source zero ranges"
            );
        }
        Ok(())
    })
    .await
}

/// Round `n` up to the next multiple of 512 (the page-blob alignment), with a
/// floor of 512 bytes.
pub fn round_up_512(n: u64) -> u64 {
    let aligned = n.div_ceil(512) * 512;
    aligned.max(512)
}

/// Re-exported from [`crate::bloburl`]: a parsed Azure blob URL (used here for
/// the StorageClass `templateBlobUrl` golden-image source).
pub use crate::bloburl::{parse_blob_url, TemplateBlobRef};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn volume_id_roundtrip() {
        let id = make_volume_id("myaccount", "mycontainer", "pvc-abc/data.vhd");
        assert_eq!(id, "myaccount/mycontainer/pvc-abc/data.vhd");
        let (ro, a, c, b) = parse_volume_id(&id).unwrap();
        assert!(!ro);
        assert_eq!(a, "myaccount");
        assert_eq!(c, "mycontainer");
        assert_eq!(b, "pvc-abc/data.vhd");
    }

    #[test]
    fn volume_id_ro_roundtrip() {
        let id = make_volume_id_ro("acct", "cont", "golden/img.vhd");
        assert_eq!(id, "ro:acct/cont/golden/img.vhd");
        let (ro, a, c, b) = parse_volume_id(&id).unwrap();
        assert!(ro);
        assert_eq!(a, "acct");
        assert_eq!(c, "cont");
        assert_eq!(b, "golden/img.vhd");
    }

    #[test]
    fn volume_id_rejects_malformed() {
        assert!(parse_volume_id("nocontainer").is_err());
        assert!(parse_volume_id("account/container").is_err());
        assert!(parse_volume_id("/container/blob").is_err());
        assert!(parse_volume_id("account//blob").is_err());
        assert!(parse_volume_id("account/container/").is_err());
    }

    #[test]
    fn role_parsing() {
        assert_eq!("controller".parse::<Role>().unwrap(), Role::Controller);
        assert_eq!("NODE".parse::<Role>().unwrap(), Role::Node);
        assert_eq!("all".parse::<Role>().unwrap(), Role::All);
        assert!("bogus".parse::<Role>().is_err());
    }
}
