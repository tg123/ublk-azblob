//! CSI **Node** service: attach a ublk Azure Page Blob device on the node and
//! mount it where the kubelet asks (`NodePublishVolume`), then unmount and tear
//! the device down on `NodeUnpublishVolume`.
//!
//! Each published volume owns a child `ublk-azblob run` process that keeps the
//! `/dev/ublkbN` device alive.  The child is tracked in an in-memory registry
//! and signalled (`SIGINT`) to shut down cleanly on unpublish.

use std::collections::HashMap;
use std::process::Child;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tonic::{Request, Response, Status};
use tracing::{error, info, instrument, warn};

use super::mount;
use super::proto::node_server::Node;
use super::proto::{
    volume_capability::AccessType, NodeExpandVolumeRequest, NodeExpandVolumeResponse,
    NodeGetCapabilitiesRequest, NodeGetCapabilitiesResponse, NodeGetInfoRequest,
    NodeGetInfoResponse, NodeGetVolumeStatsRequest, NodeGetVolumeStatsResponse,
    NodePublishVolumeRequest, NodePublishVolumeResponse, NodeStageVolumeRequest,
    NodeStageVolumeResponse, NodeUnpublishVolumeRequest, NodeUnpublishVolumeResponse,
    NodeUnstageVolumeRequest, NodeUnstageVolumeResponse,
};
use super::DriverConfig;

/// Default filesystem created on a freshly-provisioned volume.
const DEFAULT_FS_TYPE: &str = "ext4";
/// How long to wait for the ublk device node to appear after spawning the child.
const DEVICE_TIMEOUT: Duration = Duration::from_secs(60);

/// A currently-published volume and the resources backing it.
struct Published {
    child: Child,
    device: String,
    target: String,
}

/// Node service implementation.
pub struct NodeService {
    node_id: String,
    config: DriverConfig,
    /// volume_id → published volume.
    volumes: Arc<Mutex<HashMap<String, Published>>>,
    /// Serialises device discovery so concurrent publishes don't race over
    /// "which new `/dev/ublkbN` just appeared".
    publish_lock: Arc<Mutex<()>>,
}

impl NodeService {
    /// Build a node service for `node_id` using the driver configuration.
    pub fn new(node_id: String, config: DriverConfig) -> Self {
        Self {
            node_id,
            config,
            volumes: Arc::new(Mutex::new(HashMap::new())),
            publish_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Build the `AZURE_STORAGE_*` environment for the child `run` process from
    /// the volume context, request secrets and driver config.
    fn child_env(
        &self,
        ctx: &HashMap<String, String>,
        secrets: &HashMap<String, String>,
        volume_id: &str,
    ) -> anyhow::Result<Vec<(String, String)>> {
        let get = |k: &str| ctx.get(k).cloned();
        let account = get("account").unwrap_or_else(|| self.config.account.clone());
        let endpoint = get("endpoint").unwrap_or_else(|| self.config.endpoint.clone());
        let container = get("container").unwrap_or_else(|| self.config.default_container.clone());
        let blob = get("blob").ok_or_else(|| anyhow::anyhow!("volume context missing 'blob'"))?;

        // Collapse the account / endpoint / container / blob (+ optional
        // read-only snapshot) into a single `AZURE_STORAGE_BLOB_URL` for the
        // child `run` process. A volume is read-only exactly when it targets a
        // blob snapshot (a `templateBlobUrl` with `?snapshot=<timestamp>`); the
        // child derives read-only from the URL's `?snapshot=` alone — there is
        // no separate readOnly flag. The endpoint template's `%s` account
        // placeholder is substituted here so the child sees a resolved URL.
        let snapshot = get("snapshot").filter(|s| !s.is_empty());
        let blob_url = child_blob_url(&endpoint, &account, &container, &blob, snapshot.as_deref());
        let mut env = vec![("AZURE_STORAGE_BLOB_URL".to_string(), blob_url)];

        let account_key = secrets
            .get("accountKey")
            .cloned()
            .or_else(|| self.config.account_key.clone());
        if let Some(key) = account_key {
            env.push(("AZURE_STORAGE_KEY".to_string(), key));
        } else if self.config.use_workload_identity {
            env.push(("AZURE_USE_WORKLOAD_IDENTITY".to_string(), "1".to_string()));
            if let Some(id) = &self.config.workload_identity_client_id {
                env.push(("AZURE_CLIENT_ID".to_string(), id.clone()));
            }
            if let Some(t) = &self.config.workload_identity_tenant_id {
                env.push(("AZURE_TENANT_ID".to_string(), t.clone()));
            }
            if let Some(f) = &self.config.workload_identity_token_file {
                env.push(("AZURE_FEDERATED_TOKEN_FILE".to_string(), f.clone()));
            }
        } else if self.config.use_msi || self.config.msi_client_id.is_some() {
            env.push(("AZURE_USE_MSI".to_string(), "1".to_string()));
            if let Some(id) = &self.config.msi_client_id {
                env.push(("AZURE_MSI_CLIENT_ID".to_string(), id.clone()));
            }
        } else if let (Some(tenant), Some(client), Some(secret)) = (
            secrets
                .get("AZURE_TENANT_ID")
                .or(self.config.sp_tenant_id.as_ref()),
            secrets
                .get("AZURE_CLIENT_ID")
                .or(self.config.sp_client_id.as_ref()),
            secrets
                .get("AZURE_CLIENT_SECRET")
                .or(self.config.sp_client_secret.as_ref()),
        ) {
            env.push(("AZURE_TENANT_ID".to_string(), tenant.clone()));
            env.push(("AZURE_CLIENT_ID".to_string(), client.clone()));
            env.push(("AZURE_CLIENT_SECRET".to_string(), secret.clone()));
        } else {
            anyhow::bail!(
                "no auth configured for node publish: provide secret 'accountKey', \
                 enable Workload Identity, enable MSI, or a service principal \
                 (AZURE_CLIENT_ID / AZURE_TENANT_ID / AZURE_CLIENT_SECRET)"
            );
        }

        // Cluster coordination: when the StorageClass opts in (volume context
        // `coordination: "true"`), enable the cluster lease + blob lock in the
        // child `run` process so at most one node serves the volume.  The holder
        // identity is this node; the lease namespace defaults to the driver
        // pod's namespace (POD_NAMESPACE) unless the context overrides it.
        let coordination = get("coordination")
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
            .unwrap_or(false);
        if coordination {
            env.push(("UBLK_COORDINATION".to_string(), "1".to_string()));
            env.push(("UBLK_LEASE_HOLDER".to_string(), self.node_id.clone()));
            let namespace = get("leaseNamespace")
                .or_else(|| std::env::var("POD_NAMESPACE").ok())
                .filter(|n| !n.is_empty());
            if let Some(ns) = namespace {
                env.push(("UBLK_LEASE_NAMESPACE".to_string(), ns));
            }
            if let Some(secs) = get("recoveryTimeoutSecs").filter(|s| !s.is_empty()) {
                env.push(("UBLK_RECOVERY_TIMEOUT_SECS".to_string(), secs));
            }
            if let Some(secs) = get("leaseDurationSecs").filter(|s| !s.is_empty()) {
                env.push(("UBLK_LEASE_DURATION_SECS".to_string(), secs));
            }
        }

        // Read-only / snapshot is carried by `AZURE_STORAGE_BLOB_URL`'s
        // `?snapshot=` query (assembled above), so nothing to add here.
        // SAS token from a `templateBlobUrl` that carries its own signature; the
        // child `run` process authenticates the (possibly cross-account) template
        // blob with it instead of the driver credentials.
        if let Some(sas) = get("sasToken").filter(|s| !s.is_empty()) {
            env.push(("AZURE_STORAGE_SAS".to_string(), sas));
        }

        // Cross-process page sharing: when the node enables a shared cache with
        // `UBLK_CACHE_SHARE_PAGES` (inherited from the DaemonSet), give each
        // volume a stable, unique cache instance name (its volume id) so peers
        // caching the same blob get distinct data files and can share each
        // other's clean pages off local disk.  The blob identity defaults to the
        // container/blob, so concurrent mounts of the *same* blob share pages.
        let share_pages = std::env::var("UBLK_CACHE_SHARE_PAGES")
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
            .unwrap_or(false);
        if share_pages {
            env.push(("UBLK_CACHE_INSTANCE".to_string(), volume_id.to_string()));
        }
        Ok(env)
    }
}

/// Assemble a single `AZURE_STORAGE_BLOB_URL` for the child `run` process from
/// the per-volume endpoint template, account, container, blob and optional
/// read-only snapshot.
///
/// The endpoint's `%s` account placeholder (subdomain style) is substituted
/// here so the child receives a fully-resolved, `parse_blob_url`-parseable URL:
/// when the endpoint host already encodes the account (`*.blob.*`) the path is
/// just `/<container>/<blob>`; otherwise (path-style / Azurite) the account is
/// the leading path segment so the account round-trips.
fn child_blob_url(
    endpoint: &str,
    account: &str,
    container: &str,
    blob: &str,
    snapshot: Option<&str>,
) -> String {
    let resolved = endpoint.replace("%s", account);
    let base = resolved.trim_end_matches('/');
    let host_has_account = base.contains(".blob.");
    let account_in_path = base.split('/').any(|seg| seg == account);
    let mut url = if host_has_account || account_in_path {
        format!("{base}/{container}/{blob}")
    } else {
        format!("{base}/{account}/{container}/{blob}")
    };
    if let Some(s) = snapshot {
        url.push_str("?snapshot=");
        url.push_str(s);
    }
    url
}

#[tonic::async_trait]
impl Node for NodeService {
    #[instrument(skip(self, request))]
    async fn node_publish_volume(
        &self,
        request: Request<NodePublishVolumeRequest>,
    ) -> Result<Response<NodePublishVolumeResponse>, Status> {
        let req = request.into_inner();
        if req.volume_id.is_empty() {
            return Err(Status::invalid_argument("volume id is required"));
        }
        if req.target_path.is_empty() {
            return Err(Status::invalid_argument("target path is required"));
        }

        // Already published to this target? Treat as success (idempotent).
        if let Some(existing) = self.volumes.lock().unwrap().get(&req.volume_id) {
            if existing.target == req.target_path {
                info!(volume_id = %req.volume_id, "already published; idempotent success");
                return Ok(Response::new(NodePublishVolumeResponse {}));
            }
            return Err(Status::failed_precondition(
                "volume already published at a different target path",
            ));
        }

        // Resolve filesystem type and mount flags from the volume capability.
        let mut fs_type = req
            .volume_context
            .get("fsType")
            .cloned()
            .unwrap_or_else(|| DEFAULT_FS_TYPE.to_string());
        let mut mount_flags: Vec<String> = Vec::new();
        if let Some(cap) = &req.volume_capability {
            match &cap.access_type {
                Some(AccessType::Mount(m)) => {
                    if !m.fs_type.is_empty() {
                        fs_type = m.fs_type.clone();
                    }
                    mount_flags = m.mount_flags.clone();
                }
                Some(AccessType::Block(_)) => {
                    return Err(Status::invalid_argument(
                        "raw block volumes are not supported; request a filesystem (Mount) volume",
                    ));
                }
                None => {}
            }
        }

        let size: u64 = req
            .volume_context
            .get("size")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let env = self
            .child_env(&req.volume_context, &req.secrets, &req.volume_id)
            .map_err(|e| Status::invalid_argument(format!("{e:#}")))?;

        let volume_id = req.volume_id.clone();
        let target = req.target_path.clone();
        // The device is read-only when the CSI request asks for it, or when the
        // StorageClass selects a snapshot.  A snapshot is immutable, so mounting
        // read-write over it would fail on first write; force a read-only mount
        // (and skip mkfs) in that case.
        let device_read_only = req
            .volume_context
            .get("snapshot")
            .is_some_and(|s| !s.is_empty());
        let readonly = req.readonly || device_read_only;
        // A volume copied from a `templateBlobUrl` already carries a filesystem;
        // never reformat it (the copy is the user's golden image).
        let from_template = req
            .volume_context
            .get("fromTemplate")
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
            .unwrap_or(false);
        // Optional `fsck` pass before mounting an already-formatted, writable
        // device. Defaults to off; `true`/`preen` preens, `force` runs a full
        // check. A read-only device can't be repaired in place, so fsck is
        // skipped there (see below).
        let fsck_mode = mount::FsckMode::parse(
            req.volume_context
                .get("fsck")
                .map(String::as_str)
                .unwrap_or(""),
        )
        .map_err(|e| Status::invalid_argument(format!("{e:#}")))?;
        let volumes = self.volumes.clone();
        let publish_lock = self.publish_lock.clone();
        let use_nbd = self.config.use_nbd;
        let nbd_host = self.config.nbd_host.clone();
        let nbd_port_start = self.config.nbd_port_start;

        info!(use_nbd = use_nbd, nbd_host = %nbd_host, nbd_port_start = nbd_port_start, "NodePublishVolume: NBD config");

        // The device + filesystem work is blocking; run it off the async runtime.
        let result = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            // Snapshot existing devices, spawn the child, and discover the new
            // node under a global lock so concurrent publishes don't collide.
            let (mut child, device) = {
                let _guard = publish_lock.lock().unwrap();

                // Build NBD listen address if NBD mode is enabled
                let nbd_listen = if use_nbd {
                    // Allocate a unique free port per volume so multiple volumes
                    // (or a remount racing a still-flushing previous server) on
                    // the same node don't collide on a single shared port.
                    let port = mount::find_free_port(&nbd_host, nbd_port_start, 1024)?;
                    let listen = format!("{}:{}", nbd_host, port);
                    info!(listen = %listen, "NBD mode enabled, will connect to NBD server");
                    Some(listen)
                } else {
                    info!("ublk mode enabled, will wait for /dev/ublkbN");
                    None
                };

                // Snapshot existing ublk devices *before* spawning the child so a
                // fast child that creates /dev/ublkbN before we look isn't already
                // in the "before" set (which would make it invisible as "new").
                let ublk_before = if nbd_listen.is_none() {
                    mount::list_ublk_devices()
                } else {
                    Default::default()
                };

                let mut child = mount::spawn_device(size, &env, nbd_listen.clone())?;

                let device = if let Some(listen_addr) = nbd_listen {
                    // NBD mode: wait for server and connect with nbd-client
                    info!(listen_addr = %listen_addr, "Calling wait_and_connect_nbd");
                    match mount::wait_and_connect_nbd(&listen_addr, &mut child, DEVICE_TIMEOUT) {
                        Ok(dev) => dev,
                        Err(e) => {
                            mount::signal_pid(child.id(), libc::SIGINT);
                            let _ = child.wait();
                            return Err(e);
                        }
                    }
                } else {
                    // ublk mode: wait for /dev/ublkbN to appear
                    info!("Calling wait_for_new_device for ublk");
                    match mount::wait_for_new_device(&ublk_before, &mut child, DEVICE_TIMEOUT) {
                        Ok(dev) => dev,
                        Err(e) => {
                            mount::signal_pid(child.id(), libc::SIGINT);
                            let _ = child.wait();
                            return Err(e);
                        }
                    }
                };

                (child, device)
            };

            // Make a filesystem only on a blank device, then mount.
            let outcome = (|| -> anyhow::Result<()> {
                let mut formatted = false;
                if mount::has_filesystem(&device) {
                    info!(device = %device, "existing filesystem detected; skipping mkfs");
                } else if from_template {
                    // The blob was copied from a golden-image template, which is
                    // already formatted; never reformat it.
                    info!(device = %device, "volume copied from template; skipping mkfs");
                } else if readonly {
                    // A read-only device has no filesystem we can create; the
                    // blob must already contain one.
                    anyhow::bail!(
                        "device {device} is read-only and has no filesystem; \
                         a read-only/snapshot volume must already be formatted"
                    );
                } else {
                    mount::mkfs(&device, &fs_type)?;
                    formatted = true;
                }
                // Optionally fsck an existing filesystem before mounting. A
                // freshly `mkfs`'d device is already clean, and a read-only
                // device can't be repaired in place, so skip fsck in both cases.
                if fsck_mode != mount::FsckMode::Off && !formatted && !readonly {
                    mount::fsck(&device, &fs_type, fsck_mode)?;
                }
                mount::mount(&device, &target, &fs_type, &mount_flags, readonly)?;
                Ok(())
            })();

            if let Err(e) = outcome {
                // Roll back the device on failure.
                mount::signal_pid(child.id(), libc::SIGINT);
                let _ = child.wait();
                return Err(e);
            }

            // Keep the long-lived child's pipes drained so it can't block.
            mount::drain_child_output(&mut child);
            volumes.lock().unwrap().insert(
                volume_id,
                Published {
                    child,
                    device,
                    target,
                },
            );
            Ok(())
        })
        .await
        .map_err(|e| Status::internal(format!("publish task panicked: {e}")))?;

        result.map_err(|e| {
            error!(error = %format!("{e:#}"), "NodePublishVolume failed");
            Status::internal(format!("{e:#}"))
        })?;

        info!(volume_id = %req.volume_id, target = %req.target_path, "NodePublishVolume done");
        Ok(Response::new(NodePublishVolumeResponse {}))
    }

    #[instrument(skip(self, request))]
    async fn node_unpublish_volume(
        &self,
        request: Request<NodeUnpublishVolumeRequest>,
    ) -> Result<Response<NodeUnpublishVolumeResponse>, Status> {
        let req = request.into_inner();
        if req.volume_id.is_empty() {
            return Err(Status::invalid_argument("volume id is required"));
        }
        if req.target_path.is_empty() {
            return Err(Status::invalid_argument("target path is required"));
        }

        let published = self.volumes.lock().unwrap().remove(&req.volume_id);
        let target = req.target_path.clone();
        let volume_id = req.volume_id.clone();

        let result = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            // Always attempt to unmount the requested target (idempotent).
            mount::umount(&target)?;

            if let Some(p) = published {
                info!(device = %p.device, "stopping ublk device");
                mount::signal_pid(p.child.id(), libc::SIGINT);
                let mut child = p.child;
                match child.wait() {
                    Ok(status) if !status.success() => {
                        warn!(%status, "ublk-azblob exited non-zero on shutdown");
                    }
                    Ok(_) => {}
                    Err(e) => warn!(error = %e, "wait on ublk-azblob child failed"),
                }
            } else {
                warn!(%volume_id, "no tracked device for volume on unpublish");
            }
            Ok(())
        })
        .await
        .map_err(|e| Status::internal(format!("unpublish task panicked: {e}")))?;

        result.map_err(|e| {
            error!(error = %format!("{e:#}"), "NodeUnpublishVolume failed");
            Status::internal(format!("{e:#}"))
        })?;

        info!(volume_id = %req.volume_id, "NodeUnpublishVolume done");
        Ok(Response::new(NodeUnpublishVolumeResponse {}))
    }

    async fn node_get_capabilities(
        &self,
        _request: Request<NodeGetCapabilitiesRequest>,
    ) -> Result<Response<NodeGetCapabilitiesResponse>, Status> {
        // No staging and no online expansion: publish/unpublish do all the work.
        Ok(Response::new(NodeGetCapabilitiesResponse {
            capabilities: Vec::new(),
        }))
    }

    async fn node_get_info(
        &self,
        _request: Request<NodeGetInfoRequest>,
    ) -> Result<Response<NodeGetInfoResponse>, Status> {
        Ok(Response::new(NodeGetInfoResponse {
            node_id: self.node_id.clone(),
            max_volumes_per_node: 0,
            accessible_topology: None,
        }))
    }

    // ── Unimplemented node RPCs ───────────────────────────────────────────────

    async fn node_stage_volume(
        &self,
        _request: Request<NodeStageVolumeRequest>,
    ) -> Result<Response<NodeStageVolumeResponse>, Status> {
        Err(Status::unimplemented("NodeStageVolume"))
    }

    async fn node_unstage_volume(
        &self,
        _request: Request<NodeUnstageVolumeRequest>,
    ) -> Result<Response<NodeUnstageVolumeResponse>, Status> {
        Err(Status::unimplemented("NodeUnstageVolume"))
    }

    async fn node_get_volume_stats(
        &self,
        _request: Request<NodeGetVolumeStatsRequest>,
    ) -> Result<Response<NodeGetVolumeStatsResponse>, Status> {
        Err(Status::unimplemented("NodeGetVolumeStats"))
    }

    async fn node_expand_volume(
        &self,
        _request: Request<NodeExpandVolumeRequest>,
    ) -> Result<Response<NodeExpandVolumeResponse>, Status> {
        Err(Status::unimplemented("NodeExpandVolume"))
    }
}

#[cfg(test)]
mod tests {
    use super::child_blob_url;
    use crate::bloburl::parse_blob_url;

    #[test]
    fn child_blob_url_subdomain_template_roundtrips() {
        let url = child_blob_url(
            "https://%s.blob.core.windows.net/",
            "myacct",
            "images",
            "golden/disk.vhd",
            None,
        );
        assert_eq!(
            url,
            "https://myacct.blob.core.windows.net/images/golden/disk.vhd"
        );
        let r = parse_blob_url(&url).unwrap();
        assert_eq!(r.account, "myacct");
        assert_eq!(r.container, "images");
        assert_eq!(r.blob, "golden/disk.vhd");
        assert_eq!(r.snapshot, None);
    }

    #[test]
    fn child_blob_url_appends_snapshot() {
        let url = child_blob_url(
            "http://%s.blob.localhost:10000/",
            "devstoreaccount1",
            "c",
            "b",
            Some("2026-06-20T20:06:28.7995412Z"),
        );
        let r = parse_blob_url(&url).unwrap();
        assert_eq!(r.account, "devstoreaccount1");
        assert_eq!(r.container, "c");
        assert_eq!(r.blob, "b");
        assert_eq!(r.snapshot.as_deref(), Some("2026-06-20T20:06:28.7995412Z"));
    }

    #[test]
    fn child_blob_url_path_style_account_in_path() {
        // Azurite path-style endpoint already carrying the account segment.
        let url = child_blob_url(
            "http://127.0.0.1:10000/devstoreaccount1",
            "devstoreaccount1",
            "c",
            "b",
            None,
        );
        assert_eq!(url, "http://127.0.0.1:10000/devstoreaccount1/c/b");
        let r = parse_blob_url(&url).unwrap();
        assert_eq!(r.account, "devstoreaccount1");
        assert_eq!(r.container, "c");
        assert_eq!(r.blob, "b");
    }

    #[test]
    fn child_blob_url_path_style_account_prepended() {
        // Bare path-style endpoint (no account); account becomes the leading segment.
        let url = child_blob_url("http://127.0.0.1:10000/", "acct", "c", "b", None);
        assert_eq!(url, "http://127.0.0.1:10000/acct/c/b");
        let r = parse_blob_url(&url).unwrap();
        assert_eq!(r.account, "acct");
    }

    #[test]
    fn child_blob_url_k8s_e2e_path_style_endpoint() {
        // Mirrors the k8s e2e endpoint: a `%s` account placeholder in the path
        // of a literal-IP host (Azurite path-style).
        let url = child_blob_url(
            "http://172.30.0.10:10000/%s",
            "devstoreaccount1",
            "ublk-azblob-volumes",
            "default/volumes/pvc-abc",
            None,
        );
        assert_eq!(
            url,
            "http://172.30.0.10:10000/devstoreaccount1/ublk-azblob-volumes/default/volumes/pvc-abc"
        );
        let r = parse_blob_url(&url).unwrap();
        assert_eq!(r.account, "devstoreaccount1");
        assert_eq!(r.container, "ublk-azblob-volumes");
        assert_eq!(r.blob, "default/volumes/pvc-abc");
    }
}
