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
//! container and blob name as `"{container}/{blob}"`, which is all
//! `DeleteVolume` (which only receives the volume id and secrets) needs to find
//! the blob again.  The storage account and service endpoint are driver-level
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

/// Encode a `(container, blob)` pair as a CSI volume id.
///
/// Container names cannot contain `/`, so splitting on the first `/` is
/// unambiguous even though blob names may themselves contain slashes.
pub fn make_volume_id(container: &str, blob: &str) -> String {
    format!("{container}/{blob}")
}

/// Decode a CSI volume id produced by [`make_volume_id`] into `(container, blob)`.
pub fn parse_volume_id(volume_id: &str) -> anyhow::Result<(String, String)> {
    let (container, blob) = volume_id.split_once('/').with_context(|| {
        format!("malformed volume id '{volume_id}' (expected 'container/blob')")
    })?;
    if container.is_empty() || blob.is_empty() {
        anyhow::bail!("malformed volume id '{volume_id}': empty container or blob");
    }
    Ok((container.to_string(), blob.to_string()))
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
    let account_key = secrets
        .get("accountKey")
        .cloned()
        .or_else(|| config.account_key.clone());

    let auth = if let Some(key) = account_key {
        AuthConfig::SharedKey {
            account_name: account.to_string(),
            account_key: key,
        }
    } else if config.use_workload_identity {
        AuthConfig::WorkloadIdentity {
            client_id: config.workload_identity_client_id.clone(),
            tenant_id: config.workload_identity_tenant_id.clone(),
            token_file: config.workload_identity_token_file.clone(),
        }
    } else if config.use_msi || config.msi_client_id.is_some() {
        AuthConfig::Msi(
            config
                .msi_client_id
                .clone()
                .map(UserAssignedIdentity::ClientId),
        )
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
        AuthConfig::ServicePrincipal {
            tenant_id: tenant_id.clone(),
            client_id: client_id.clone(),
            client_secret: client_secret.clone(),
        }
    } else {
        anyhow::bail!(
            "no auth configured: provide an account key (secret 'accountKey' or \
             --account-key), enable Workload Identity (--workload-identity), \
             enable Managed Identity (--msi / --msi-client-id), \
             or a service principal (AZURE_CLIENT_ID / AZURE_TENANT_ID / AZURE_CLIENT_SECRET)"
        );
    };

    // Build the account-specific endpoint URL
    // For standard Azure, construct from account: https://{account}.blob.core.windows.net/
    // For custom endpoints (Azurite, sovereign clouds), use config endpoint if it contains the account
    let endpoint = if config.endpoint.contains(&account) {
        // Custom endpoint already has account name (e.g., Azurite: http://127.0.0.1:10000/devstoreaccount1)
        config.endpoint.clone()
    } else {
        // Standard Azure or generic endpoint - construct account-specific URL
        format!("https://{account}.blob.core.windows.net/")
    };

    let container_client = auth::build_container_client(&endpoint, container, &auth)
        .context("build container client")?;
    Ok(Arc::new(AzurePageBlobBackend::new(
        container_client,
        blob.to_string(),
    )))
}

/// Round `n` up to the next multiple of 512 (the page-blob alignment), with a
/// floor of 512 bytes.
pub fn round_up_512(n: u64) -> u64 {
    let aligned = n.div_ceil(512) * 512;
    aligned.max(512)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn volume_id_roundtrip() {
        let id = make_volume_id("mycontainer", "pvc-abc/data.vhd");
        assert_eq!(id, "mycontainer/pvc-abc/data.vhd");
        let (c, b) = parse_volume_id(&id).unwrap();
        assert_eq!(c, "mycontainer");
        assert_eq!(b, "pvc-abc/data.vhd");
    }

    #[test]
    fn volume_id_rejects_malformed() {
        assert!(parse_volume_id("nocontainer").is_err());
        assert!(parse_volume_id("/blob").is_err());
        assert!(parse_volume_id("container/").is_err());
    }

    #[test]
    fn role_parsing() {
        assert_eq!("controller".parse::<Role>().unwrap(), Role::Controller);
        assert_eq!("NODE".parse::<Role>().unwrap(), Role::Node);
        assert_eq!("all".parse::<Role>().unwrap(), Role::All);
        assert!("bogus".parse::<Role>().is_err());
    }
}
