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
use tracing::info;

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
    if let Some(snapshot) = &tmpl.snapshot {
        backend = backend.with_snapshot(snapshot.clone());
    }
    Ok(Arc::new(backend))
}

/// Streamed, chunked copy of `total_size` bytes from `source` into `dest`.
///
/// Used to clone a `templateBlobUrl` golden image into a fresh per-PVC blob.
/// Copies in 4 MiB page-aligned chunks; sparse/never-written source ranges read
/// back as zeros, so the whole logical size is materialised on the destination.
/// `dest` must already exist (call `create` first) and be at least `total_size`.
pub async fn copy_blob(
    source: &dyn BlobBackend,
    dest: &dyn BlobBackend,
    total_size: u64,
) -> anyhow::Result<()> {
    const CHUNK: u64 = 4 * 1024 * 1024;
    let mut offset = 0u64;
    while offset < total_size {
        let len = CHUNK.min(total_size - offset);
        let data = source
            .read(offset, len)
            .await
            .with_context(|| format!("read template offset={offset} len={len}"))?;
        dest.write(offset, data)
            .await
            .with_context(|| format!("write copy offset={offset} len={len}"))?;
        offset += len;
    }
    dest.flush().await.context("flush copied blob")?;
    Ok(())
}

/// Round `n` up to the next multiple of 512 (the page-blob alignment), with a
/// floor of 512 bytes.
pub fn round_up_512(n: u64) -> u64 {
    let aligned = n.div_ceil(512) * 512;
    aligned.max(512)
}

/// A parsed `templateBlobUrl` (the StorageClass golden-image source).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplateBlobRef {
    /// Blob *service* URL the rest of the code expects (`build_container_client`
    /// appends `/container`): subdomain-style `https://acct.blob.core.windows.net`
    /// for Azure, or `http://host:port/account` for Azurite/path-style.
    pub service_url: String,
    /// Storage account name.
    pub account: String,
    /// Container name.
    pub container: String,
    /// Blob name (may contain `/`).
    pub blob: String,
    /// Optional `snapshot=` timestamp from the URL.
    pub snapshot: Option<String>,
    /// Optional SAS query string (everything except `snapshot`, present only when
    /// the URL carries a `sig=` signature).
    pub sas: Option<String>,
}

/// Parse a full Azure blob URL (`templateBlobUrl`) into its components.
///
/// Supports both Azure subdomain hosts (`<account>.blob.core.windows.net`) and
/// path-style/Azurite hosts (`host:port/<account>/...`). Any `snapshot=` query
/// is split out; the remaining query (when it carries a `sig=`) is returned as
/// the SAS token.
pub fn parse_blob_url(url: &str) -> anyhow::Result<TemplateBlobRef> {
    let parsed = azure_core::http::Url::parse(url)
        .with_context(|| format!("parse templateBlobUrl: {url}"))?;
    let scheme = parsed.scheme();
    let host = parsed
        .host_str()
        .context("templateBlobUrl has no host")?
        .to_string();

    // Split query into snapshot vs the rest (SAS).
    let mut snapshot = None;
    let mut sas_pairs: Vec<(String, String)> = Vec::new();
    let mut has_sig = false;
    for (k, v) in parsed.query_pairs() {
        if k == "snapshot" {
            snapshot = Some(v.into_owned());
        } else {
            if k == "sig" {
                has_sig = true;
            }
            sas_pairs.push((k.into_owned(), v.into_owned()));
        }
    }
    let sas = if has_sig {
        let mut tmp = azure_core::http::Url::parse("https://x/").unwrap();
        tmp.query_pairs_mut().extend_pairs(&sas_pairs);
        tmp.query().map(|q| q.to_string())
    } else {
        None
    };

    let segments: Vec<String> = parsed
        .path_segments()
        .map(|it| {
            it.filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default();

    // Azure subdomain style: `<account>.blob.<suffix>` → account is the first
    // host label, the path is `<container>/<blob...>`.
    let azure_subdomain = host.contains(".blob.");
    let (service_url, account, container, blob) = if azure_subdomain {
        let account = host.split('.').next().unwrap_or("").to_string();
        if segments.len() < 2 {
            anyhow::bail!("templateBlobUrl missing container/blob path: {url}");
        }
        let container = segments[0].clone();
        let blob = segments[1..].join("/");
        let port = parsed.port().map(|p| format!(":{p}")).unwrap_or_default();
        (format!("{scheme}://{host}{port}"), account, container, blob)
    } else {
        // Path-style / Azurite: `host:port/<account>/<container>/<blob...>`.
        if segments.len() < 3 {
            anyhow::bail!("templateBlobUrl missing account/container/blob path: {url}");
        }
        let account = segments[0].clone();
        let container = segments[1].clone();
        let blob = segments[2..].join("/");
        let port = parsed.port().map(|p| format!(":{p}")).unwrap_or_default();
        (
            format!("{scheme}://{host}{port}/{account}"),
            account,
            container,
            blob,
        )
    };

    if container.is_empty() || blob.is_empty() {
        anyhow::bail!("templateBlobUrl missing container or blob: {url}");
    }
    Ok(TemplateBlobRef {
        service_url,
        account,
        container,
        blob,
        snapshot,
        sas,
    })
}

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

    #[test]
    fn parse_blob_url_azure_subdomain() {
        let r =
            parse_blob_url("https://myacct.blob.core.windows.net/images/golden/disk.vhd").unwrap();
        assert_eq!(r.service_url, "https://myacct.blob.core.windows.net");
        assert_eq!(r.account, "myacct");
        assert_eq!(r.container, "images");
        assert_eq!(r.blob, "golden/disk.vhd");
        assert_eq!(r.snapshot, None);
        assert_eq!(r.sas, None);
    }

    #[test]
    fn parse_blob_url_azurite_path_style() {
        let r = parse_blob_url("http://127.0.0.1:10000/devstoreaccount1/images/golden/disk.vhd")
            .unwrap();
        assert_eq!(r.service_url, "http://127.0.0.1:10000/devstoreaccount1");
        assert_eq!(r.account, "devstoreaccount1");
        assert_eq!(r.container, "images");
        assert_eq!(r.blob, "golden/disk.vhd");
    }

    #[test]
    fn parse_blob_url_with_sas_and_snapshot() {
        let r = parse_blob_url(
            "https://myacct.blob.core.windows.net/c/b?snapshot=2024-01-02T03:04:05.0Z&sv=2022-11-02&sig=ABC%2Bdef&se=2030-01-01",
        )
        .unwrap();
        assert_eq!(r.account, "myacct");
        assert_eq!(r.container, "c");
        assert_eq!(r.blob, "b");
        assert_eq!(r.snapshot.as_deref(), Some("2024-01-02T03:04:05.0Z"));
        let sas = r.sas.expect("sas present");
        assert!(sas.contains("sig=ABC%2Bdef"));
        assert!(sas.contains("sv=2022-11-02"));
        assert!(!sas.contains("snapshot"));
    }

    #[test]
    fn parse_blob_url_no_sig_means_no_sas() {
        // A bare query without a signature is not treated as a SAS token.
        let r = parse_blob_url("https://myacct.blob.core.windows.net/c/b?foo=bar").unwrap();
        assert_eq!(r.sas, None);
    }

    #[test]
    fn parse_blob_url_rejects_incomplete() {
        assert!(parse_blob_url("https://myacct.blob.core.windows.net/onlycontainer").is_err());
        assert!(parse_blob_url("http://127.0.0.1:10000/acct/onlycontainer").is_err());
        assert!(parse_blob_url("not a url").is_err());
    }
}
