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
use super::{build_backend, make_volume_id, parse_volume_id, round_up_512, DriverConfig};

/// Parameter key (StorageClass `parameters`) for storage account.
const PARAM_STORAGE_ACCOUNT: &str = "storageAccount";
/// Parameter key (StorageClass `parameters`) selecting the blob container.
const PARAM_CONTAINER: &str = "container";
/// Parameter key for blob path template.
const PARAM_BLOB_PATH_TEMPLATE: &str = "blobPathTemplate";
/// Parameter key selecting the on-disk filesystem the node should create.
const PARAM_FS_TYPE: &str = "fsType";

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

        // Size: round the requested bytes up to the 512-byte page-blob alignment.
        let requested = req
            .capacity_range
            .as_ref()
            .map(|c| c.required_bytes.max(c.limit_bytes))
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

        info!(name = %req.name, account = %account, container = %container, size, "CreateVolume");

        let backend = build_backend(&self.config, &account, &container, &blob, &req.secrets)
            .map_err(|e| Status::internal(format!("build backend: {e:#}")))?;
        backend.create(size).await.map_err(|e| {
            error!(error = %format!("{e:#}"), "create page blob failed");
            Status::internal(format!("create page blob: {e:#}"))
        })?;

        // Hand the node everything it needs to attach the device later.
        let mut volume_context: HashMap<String, String> = HashMap::new();
        volume_context.insert("container".to_string(), container.clone());
        volume_context.insert("blob".to_string(), blob.clone());
        volume_context.insert("account".to_string(), account.clone());

        // Build account-specific endpoint for the node
        // Standard Azure: construct from account; Custom (Azurite): use config if has account
        let endpoint = if self.config.endpoint.contains(&account) {
            self.config.endpoint.clone()
        } else {
            format!("https://{account}.blob.core.windows.net/")
        };

        volume_context.insert("endpoint".to_string(), endpoint);
        volume_context.insert("size".to_string(), size.to_string());
        if let Some(fs) = req.parameters.get(PARAM_FS_TYPE) {
            volume_context.insert(PARAM_FS_TYPE.to_string(), fs.clone());
        }

        Ok(Response::new(CreateVolumeResponse {
            volume: Some(Volume {
                capacity_bytes: size as i64,
                volume_id: make_volume_id(&container, &blob),
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
        let (container, blob) = parse_volume_id(&req.volume_id)
            .map_err(|e| Status::invalid_argument(format!("{e:#}")))?;

        info!(volume_id = %req.volume_id, "DeleteVolume");

        // For delete, try to get account from secrets or fall back to config
        let account = req
            .secrets
            .get(PARAM_STORAGE_ACCOUNT)
            .map(|s| s.as_str())
            .unwrap_or(&self.config.account);

        let backend = build_backend(&self.config, account, &container, &blob, &req.secrets)
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
