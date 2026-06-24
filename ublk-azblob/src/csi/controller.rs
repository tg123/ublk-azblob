//! CSI **Controller** service: provisioning (`CreateVolume`) and de-provisioning
//! (`DeleteVolume`) of the Azure Page Blob that backs a PersistentVolume.
//!
//! Only the create/delete RPCs are implemented; all other controller RPCs
//! return `UNIMPLEMENTED`, matching the advertised `CREATE_DELETE_VOLUME`
//! capability.

use std::collections::HashMap;

use tonic::{Request, Response, Status};
use tracing::{error, info, instrument};

use super::proto::controller_server::Controller;
use super::proto::{
    controller_service_capability::{rpc::Type as RpcType, Rpc, Type as CapType},
    validate_volume_capabilities_response::Confirmed,
    ControllerExpandVolumeRequest, ControllerExpandVolumeResponse,
    ControllerGetCapabilitiesRequest, ControllerGetCapabilitiesResponse,
    ControllerGetVolumeRequest, ControllerGetVolumeResponse, ControllerModifyVolumeRequest,
    ControllerModifyVolumeResponse, ControllerPublishVolumeRequest,
    ControllerPublishVolumeResponse, ControllerServiceCapability, ControllerUnpublishVolumeRequest,
    ControllerUnpublishVolumeResponse, CreateSnapshotRequest, CreateSnapshotResponse,
    CreateVolumeRequest, CreateVolumeResponse, DeleteSnapshotRequest, DeleteSnapshotResponse,
    DeleteVolumeRequest, DeleteVolumeResponse, GetCapacityRequest, GetCapacityResponse,
    ListSnapshotsRequest, ListSnapshotsResponse, ListVolumesRequest, ListVolumesResponse,
    ValidateVolumeCapabilitiesRequest, ValidateVolumeCapabilitiesResponse, Volume,
};
use super::{
    build_backend, build_backend_concrete, build_template_backend, copy_template, make_volume_id,
    make_volume_id_ro, parse_blob_url, parse_volume_id, round_up_512, DriverConfig,
};
use crate::backend::BlobBackend;

/// Parameter key (StorageClass `parameters`) for storage account.
const PARAM_STORAGE_ACCOUNT: &str = "storageAccount";
/// Parameter key (StorageClass `parameters`) selecting the blob container.
const PARAM_CONTAINER: &str = "container";
/// Parameter key for blob path template.
const PARAM_BLOB_PATH_TEMPLATE: &str = "blobPathTemplate";
/// Parameter key (StorageClass `parameters`) selecting the on-disk filesystem
/// the node should create when formatting a freshly-provisioned (non-template)
/// blob.
const PARAM_NEW_BLOB_FS_TYPE: &str = "newBlobFsType";
/// Parameter key (StorageClass `parameters`) selecting the filesystem the node
/// should mount when provisioning from a golden-image template (`templateBlobUrl`).
/// The template is never reformatted, so this is the type it already carries.
/// Only meaningful when `templateBlobUrl` is set.
const PARAM_TEMPLATE_BLOB_FS_TYPE: &str = "templateBlobFsType";
/// Advanced parameter key (StorageClass `parameters`) overriding the built-in
/// mount options that the `templateBlobFsType` profile would otherwise apply.
/// Only meaningful when `templateBlobUrl` is set.
const PARAM_TEMPLATE_BLOB_MOUNT_ARGS: &str = "templateBlobMountArgsOverwrite";
/// Parameter key (StorageClass `parameters`) opting a read-only snapshot volume
/// into an **ephemeral overlay**: the node presents a writable merged view over
/// the immutable snapshot whose writes land in a node-local upper layer that is
/// discarded on unpublish (pod-local changes never reach the blob). Only
/// meaningful for a `templateBlobUrl` that targets a snapshot (`?snapshot=`).
const PARAM_OVERLAY: &str = "overlay";
/// Parameter keys for the optional cluster-lease coordination, forwarded to the
/// node via the volume context (the node's `child_env` reads them).
const PARAM_COORDINATION: &str = "coordination";
const PARAM_LEASE_NAMESPACE: &str = "leaseNamespace";
const PARAM_RECOVERY_TIMEOUT_SECS: &str = "recoveryTimeoutSecs";
const PARAM_LEASE_DURATION_SECS: &str = "leaseDurationSecs";
/// Volume-context key carrying a blob snapshot timestamp.
///
/// This is **not** a StorageClass parameter — it is only populated from a
/// `templateBlobUrl` that includes a `?snapshot=<timestamp>` query, and read by
/// the node to mount the immutable snapshot read-only.
const PARAM_SNAPSHOT: &str = "snapshot";
/// Parameter key: a full Azure blob URL to use as a golden-image template.
/// A template URL that targets a **snapshot** (`?snapshot=`) is mounted directly
/// read-only (no lock/lease); a non-snapshot template is copied into a fresh
/// per-PVC blob (read-write) and formatting is skipped.
const PARAM_TEMPLATE_BLOB_URL: &str = "templateBlobUrl";

/// Default blob path template
const DEFAULT_BLOB_PATH_TEMPLATE: &str = "ublk-azblob-disk/${pv.name}";

/// Controller service implementation.
pub struct ControllerService {
    config: DriverConfig,
}

/// Expand variables in a template string
fn expand_template(template: &str, pvc_name: &str, pvc_namespace: &str, pv_name: &str) -> String {
    template
        .replace("${pvc.name}", pvc_name)
        .replace("${pvc.namespace}", pvc_namespace)
        .replace("${pv.name}", pv_name)
}

/// Sanitize path by removing leading/trailing slashes and collapsing doubles
fn sanitize_path(path: &str) -> String {
    path.trim()
        .trim_start_matches('/')
        .trim_end_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("/")
}

impl ControllerService {
    /// Build a controller service from the driver configuration.
    pub fn new(config: DriverConfig) -> Self {
        Self { config }
    }

    /// Get storage account with variable expansion and secret fallback
    #[allow(clippy::result_large_err)]
    fn storage_account_for(
        &self,
        parameters: &HashMap<String, String>,
        secrets: &HashMap<String, String>,
        pvc_name: &str,
        pvc_namespace: &str,
        pv_name: &str,
    ) -> Result<String, Status> {
        // 1. Try StorageClass parameter (with expansion)
        if let Some(template) = parameters.get(PARAM_STORAGE_ACCOUNT) {
            return Ok(expand_template(template, pvc_name, pvc_namespace, pv_name));
        }

        // 2. Try secret's AZURE_STORAGE_ACCOUNT
        if let Some(account) = secrets.get("AZURE_STORAGE_ACCOUNT") {
            return Ok(account.clone());
        }

        // 3. Fall back to config default
        Ok(self.config.account.clone())
    }

    /// Get container with variable expansion and secret fallback
    #[allow(clippy::result_large_err)]
    fn container_for(
        &self,
        parameters: &HashMap<String, String>,
        secrets: &HashMap<String, String>,
        pvc_name: &str,
        pvc_namespace: &str,
        pv_name: &str,
    ) -> Result<String, Status> {
        // 1. Try StorageClass parameter (with expansion)
        if let Some(template) = parameters.get(PARAM_CONTAINER) {
            return Ok(expand_template(template, pvc_name, pvc_namespace, pv_name));
        }

        // 2. Try secret's AZURE_STORAGE_CONTAINER
        if let Some(container) = secrets.get("AZURE_STORAGE_CONTAINER") {
            return Ok(container.clone());
        }

        // 3. Fall back to config default
        Ok(self.config.default_container.clone())
    }
}

#[tonic::async_trait]
impl Controller for ControllerService {
    #[instrument(skip(self, request))]
    async fn create_volume(
        &self,
        request: Request<CreateVolumeRequest>,
    ) -> Result<Response<CreateVolumeResponse>, Status> {
        let req = request.into_inner();
        if req.name.is_empty() {
            return Err(Status::invalid_argument("volume name is required"));
        }

        // Size: use required_bytes (minimum size) and round up to 512-byte page-blob alignment.
        // Per CSI spec, required_bytes is the minimum and limit_bytes is the maximum.
        // Using required_bytes is more cost-efficient for Azure Page Blobs (billed per allocated byte).
        let requested = req
            .capacity_range
            .as_ref()
            .map(|c| c.required_bytes)
            .unwrap_or(0);
        let size = round_up_512(requested.max(0) as u64);

        // The external-provisioner injects PVC/PV metadata as parameters.
        let pvc_name = req
            .parameters
            .get("csi.storage.k8s.io/pvc/name")
            .map(|s| s.as_str())
            .unwrap_or("");
        let pvc_namespace = req
            .parameters
            .get("csi.storage.k8s.io/pvc/namespace")
            .map(|s| s.as_str())
            .unwrap_or("");
        let pv_name = &req.name;

        let account = self.storage_account_for(
            &req.parameters,
            &req.secrets,
            pvc_name,
            pvc_namespace,
            pv_name,
        )?;
        info!(endpoint = %self.config.endpoint, account = %account, "Building backend for CreateVolume");
        let container = self.container_for(
            &req.parameters,
            &req.secrets,
            pvc_name,
            pvc_namespace,
            pv_name,
        )?;
        // Determine blob path from template (StorageClass parameter or default).
        let blob_template = req
            .parameters
            .get(PARAM_BLOB_PATH_TEMPLATE)
            .map(|s| s.as_str())
            .unwrap_or(DEFAULT_BLOB_PATH_TEMPLATE);
        let blob = sanitize_path(&expand_template(
            blob_template,
            pvc_name,
            pvc_namespace,
            pv_name,
        ));

        // `templateBlobUrl` provisions the volume from a golden-image blob.
        // Read-only exactly when the template URL targets a snapshot — such
        // volumes mount the template directly with no copy, no lock and no lease;
        // a non-snapshot template is copied into the per-PVC blob (read-write).

        // Tracks state when provisioning from a template (see below).
        let mut size = size;
        let mut already_created = false;
        let mut from_template = false;

        if let Some(template_url) = req
            .parameters
            .get(PARAM_TEMPLATE_BLOB_URL)
            .filter(|s| !s.is_empty())
        {
            let tmpl = parse_blob_url(template_url)
                .map_err(|e| Status::invalid_argument(format!("templateBlobUrl: {e:#}")))?;
            let read_only_mode = tmpl.snapshot.is_some();
            let source = build_template_backend(&self.config, &tmpl, &req.secrets)
                .map_err(|e| Status::internal(format!("build template backend: {e:#}")))?;
            let source_size = source
                .size()
                .await
                .map_err(|e| Status::internal(format!("stat template blob: {e:#}")))?;

            if read_only_mode {
                // Mount the shared golden-image blob directly, read-only, with no
                // copy and (crucially) no coordination — many PVCs may mount it.
                info!(
                    template = %template_url, source_size,
                    "CreateVolume: read-only template mount (no copy, no lease)"
                );
                let mut volume_context: HashMap<String, String> = HashMap::new();
                volume_context.insert("account".to_string(), tmpl.account.clone());
                volume_context.insert("container".to_string(), tmpl.container.clone());
                volume_context.insert("blob".to_string(), tmpl.blob.clone());
                volume_context.insert(
                    "endpoint".to_string(),
                    format!("{}/", tmpl.service_url.trim_end_matches('/')),
                );
                volume_context.insert("size".to_string(), source_size.to_string());
                if let Some(snapshot) = &tmpl.snapshot {
                    volume_context.insert(PARAM_SNAPSHOT.to_string(), snapshot.clone());
                }
                if let Some(sas) = &tmpl.sas {
                    volume_context.insert("sasToken".to_string(), sas.clone());
                }
                if let Some(fs) = req.parameters.get(PARAM_NEW_BLOB_FS_TYPE) {
                    volume_context.insert(PARAM_NEW_BLOB_FS_TYPE.to_string(), fs.clone());
                }
                // A read-only template is mounted (never formatted), so the node
                // mounts it as `templateBlobFsType`; `templateBlobMountArgsOverwrite` lets
                // an advanced user override the profile's built-in mount options.
                if let Some(fs) = req.parameters.get(PARAM_TEMPLATE_BLOB_FS_TYPE) {
                    volume_context.insert(PARAM_TEMPLATE_BLOB_FS_TYPE.to_string(), fs.clone());
                }
                if let Some(args) = req.parameters.get(PARAM_TEMPLATE_BLOB_MOUNT_ARGS) {
                    volume_context.insert(PARAM_TEMPLATE_BLOB_MOUNT_ARGS.to_string(), args.clone());
                }
                // Opt-in ephemeral overlay: the node stacks a writable node-local
                // layer over the immutable snapshot so pods can write locally
                // without mutating (or copying) the shared golden image.
                if let Some(v) = req.parameters.get(PARAM_OVERLAY) {
                    volume_context.insert(PARAM_OVERLAY.to_string(), v.clone());
                }
                return Ok(Response::new(CreateVolumeResponse {
                    volume: Some(Volume {
                        capacity_bytes: source_size as i64,
                        volume_id: make_volume_id_ro(&tmpl.account, &tmpl.container, &tmpl.blob),
                        volume_context,
                        content_source: None,
                        accessible_topology: Vec::new(),
                    }),
                }));
            }

            // Read-write: copy the template into a fresh per-PVC blob, sized to
            // hold the image (and at least the requested size), then skip mkfs on
            // the node since the copy is already formatted.
            size = round_up_512(source_size.max(size));
            info!(
                template = %template_url, source_size, size,
                "CreateVolume: server-side copy of template into per-PVC blob"
            );
            let (dest, dest_auth) =
                build_backend_concrete(&self.config, &account, &container, &blob, &req.secrets)
                    .map_err(|e| Status::internal(format!("build backend: {e:#}")))?;
            dest.create(size)
                .await
                .map_err(|e| Status::internal(format!("create page blob: {e:#}")))?;
            copy_template(
                &dest,
                source.as_ref(),
                template_url,
                tmpl.sas.is_some(),
                &dest_auth,
                source_size,
            )
            .await
            .map_err(|e| Status::internal(format!("copy template blob: {e:#}")))?;
            already_created = true;
            from_template = true;
        }

        info!(name = %req.name, account = %account, container = %container, size, "CreateVolume");

        let backend = build_backend(&self.config, &account, &container, &blob, &req.secrets)
            .map_err(|e| Status::internal(format!("build backend: {e:#}")))?;
        if !already_created {
            backend.create(size).await.map_err(|e| {
                error!(error = %format!("{e:#}"), "create page blob failed");
                Status::internal(format!("create page blob: {e:#}"))
            })?;
        }

        // Hand the node everything it needs to attach the device later.
        let mut volume_context: HashMap<String, String> = HashMap::new();
        volume_context.insert("container".to_string(), container.clone());
        volume_context.insert("blob".to_string(), blob.clone());
        volume_context.insert("account".to_string(), account.clone());

        // Build the account-specific endpoint for the node.
        // Template form `http://%s.host/` substitutes the account name;
        // otherwise build the standard Azure endpoint from the account.
        let endpoint = if self.config.endpoint.contains("%s") {
            self.config.endpoint.replace("%s", account.as_str())
        } else {
            format!("https://{account}.blob.core.windows.net/")
        };

        volume_context.insert("endpoint".to_string(), endpoint);
        volume_context.insert("size".to_string(), size.to_string());
        if from_template {
            // The copied blob already carries a filesystem; tell the node to skip
            // mkfs so it preserves the template's contents.
            volume_context.insert("fromTemplate".to_string(), "true".to_string());
        }
        if let Some(fs) = req.parameters.get(PARAM_NEW_BLOB_FS_TYPE) {
            volume_context.insert(PARAM_NEW_BLOB_FS_TYPE.to_string(), fs.clone());
        }
        // `templateBlobFsType` / `templateBlobMountArgsOverwrite` only apply when the
        // volume is provisioned from a golden-image template (it is copied, not
        // formatted, so the node mounts it as the template's existing filesystem).
        if from_template {
            if let Some(fs) = req.parameters.get(PARAM_TEMPLATE_BLOB_FS_TYPE) {
                volume_context.insert(PARAM_TEMPLATE_BLOB_FS_TYPE.to_string(), fs.clone());
            }
            if let Some(args) = req.parameters.get(PARAM_TEMPLATE_BLOB_MOUNT_ARGS) {
                volume_context.insert(PARAM_TEMPLATE_BLOB_MOUNT_ARGS.to_string(), args.clone());
            }
        }
        // Forward the coordination opt-in (and its tuning) from the StorageClass
        // parameters into the volume context, since CSI only hands the node the
        // volume context the controller returns — not the StorageClass parameters.
        // The node's `child_env` reads these keys to enable the cluster/blob lease.
        for key in [
            PARAM_COORDINATION,
            PARAM_LEASE_NAMESPACE,
            PARAM_RECOVERY_TIMEOUT_SECS,
            PARAM_LEASE_DURATION_SECS,
        ] {
            if let Some(v) = req.parameters.get(key) {
                volume_context.insert(key.to_string(), v.clone());
            }
        }

        Ok(Response::new(CreateVolumeResponse {
            volume: Some(Volume {
                capacity_bytes: size as i64,
                volume_id: make_volume_id(&account, &container, &blob),
                volume_context,
                content_source: None,
                accessible_topology: Vec::new(),
            }),
        }))
    }

    #[instrument(skip(self, request))]
    async fn delete_volume(
        &self,
        request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        let req = request.into_inner();
        if req.volume_id.is_empty() {
            return Err(Status::invalid_argument("volume id is required"));
        }
        let (read_only, account, container, blob) = parse_volume_id(&req.volume_id)
            .map_err(|e| Status::invalid_argument(format!("{e:#}")))?;

        info!(volume_id = %req.volume_id, account = %account, read_only, "DeleteVolume");

        // Read-only template volumes (`templateBlobUrl` + readOnly/snapshot) point
        // many PVCs at one shared golden-image blob, so deleting it when a single
        // PVC is removed would destroy the template for everyone. Treat delete as
        // a no-op for these — the template's lifecycle is managed out of band.
        if read_only {
            info!(volume_id = %req.volume_id, "read-only template volume; skipping blob deletion");
            return Ok(Response::new(DeleteVolumeResponse {}));
        }

        // The storage account is recovered from the volume id (encoded at create
        // time), so a per-volume `storageAccount` is targeted correctly rather
        // than falling back to the secret/config account and orphaning the blob.
        let backend = build_backend(&self.config, &account, &container, &blob, &req.secrets)
            .map_err(|e| Status::internal(format!("build backend: {e:#}")))?;
        backend.delete().await.map_err(|e| {
            error!(error = %format!("{e:#}"), "delete page blob failed");
            Status::internal(format!("delete page blob: {e:#}"))
        })?;

        Ok(Response::new(DeleteVolumeResponse {}))
    }

    async fn controller_get_capabilities(
        &self,
        _request: Request<ControllerGetCapabilitiesRequest>,
    ) -> Result<Response<ControllerGetCapabilitiesResponse>, Status> {
        let cap = ControllerServiceCapability {
            r#type: Some(CapType::Rpc(Rpc {
                r#type: RpcType::CreateDeleteVolume as i32,
            })),
        };
        Ok(Response::new(ControllerGetCapabilitiesResponse {
            capabilities: vec![cap],
        }))
    }

    async fn validate_volume_capabilities(
        &self,
        request: Request<ValidateVolumeCapabilitiesRequest>,
    ) -> Result<Response<ValidateVolumeCapabilitiesResponse>, Status> {
        let req = request.into_inner();
        // We support any access mode / mount capability the caller asks for.
        Ok(Response::new(ValidateVolumeCapabilitiesResponse {
            confirmed: Some(Confirmed {
                volume_context: req.volume_context,
                volume_capabilities: req.volume_capabilities,
                parameters: req.parameters,
                mutable_parameters: req.mutable_parameters,
            }),
            message: String::new(),
        }))
    }

    // ── Unimplemented controller RPCs ─────────────────────────────────────────

    async fn controller_publish_volume(
        &self,
        _request: Request<ControllerPublishVolumeRequest>,
    ) -> Result<Response<ControllerPublishVolumeResponse>, Status> {
        Err(Status::unimplemented("ControllerPublishVolume"))
    }

    async fn controller_unpublish_volume(
        &self,
        _request: Request<ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<ControllerUnpublishVolumeResponse>, Status> {
        Err(Status::unimplemented("ControllerUnpublishVolume"))
    }

    async fn list_volumes(
        &self,
        _request: Request<ListVolumesRequest>,
    ) -> Result<Response<ListVolumesResponse>, Status> {
        Err(Status::unimplemented("ListVolumes"))
    }

    async fn get_capacity(
        &self,
        _request: Request<GetCapacityRequest>,
    ) -> Result<Response<GetCapacityResponse>, Status> {
        Err(Status::unimplemented("GetCapacity"))
    }

    async fn create_snapshot(
        &self,
        _request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        Err(Status::unimplemented("CreateSnapshot"))
    }

    async fn delete_snapshot(
        &self,
        _request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        Err(Status::unimplemented("DeleteSnapshot"))
    }

    async fn list_snapshots(
        &self,
        _request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        Err(Status::unimplemented("ListSnapshots"))
    }

    async fn controller_expand_volume(
        &self,
        _request: Request<ControllerExpandVolumeRequest>,
    ) -> Result<Response<ControllerExpandVolumeResponse>, Status> {
        Err(Status::unimplemented("ControllerExpandVolume"))
    }

    async fn controller_get_volume(
        &self,
        _request: Request<ControllerGetVolumeRequest>,
    ) -> Result<Response<ControllerGetVolumeResponse>, Status> {
        Err(Status::unimplemented("ControllerGetVolume"))
    }

    async fn controller_modify_volume(
        &self,
        _request: Request<ControllerModifyVolumeRequest>,
    ) -> Result<Response<ControllerModifyVolumeResponse>, Status> {
        Err(Status::unimplemented("ControllerModifyVolume"))
    }
}
