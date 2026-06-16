//! CSI **Identity** service: plugin name, capabilities and health probe.

use tonic::{Request, Response, Status};

use super::proto::identity_server::Identity;
use super::proto::{
    plugin_capability::{service::Type as ServiceType, Service, Type as CapabilityType},
    GetPluginCapabilitiesRequest, GetPluginCapabilitiesResponse, GetPluginInfoRequest,
    GetPluginInfoResponse, PluginCapability, ProbeRequest, ProbeResponse,
};
use super::{Role, DRIVER_NAME, DRIVER_VERSION};

/// Identity service implementation.
pub struct IdentityService {
    role: Role,
}

impl IdentityService {
    /// Build an identity service for the given driver role.
    pub fn new(role: Role) -> Self {
        Self { role }
    }
}

#[tonic::async_trait]
impl Identity for IdentityService {
    async fn get_plugin_info(
        &self,
        _request: Request<GetPluginInfoRequest>,
    ) -> Result<Response<GetPluginInfoResponse>, Status> {
        Ok(Response::new(GetPluginInfoResponse {
            name: DRIVER_NAME.to_string(),
            vendor_version: DRIVER_VERSION.to_string(),
            manifest: Default::default(),
        }))
    }

    async fn get_plugin_capabilities(
        &self,
        _request: Request<GetPluginCapabilitiesRequest>,
    ) -> Result<Response<GetPluginCapabilitiesResponse>, Status> {
        let mut capabilities = Vec::new();
        // Only advertise the controller service when this process serves it.
        if matches!(self.role, Role::Controller | Role::All) {
            capabilities.push(PluginCapability {
                r#type: Some(CapabilityType::Service(Service {
                    r#type: ServiceType::ControllerService as i32,
                })),
            });
        }
        Ok(Response::new(GetPluginCapabilitiesResponse {
            capabilities,
        }))
    }

    async fn probe(
        &self,
        _request: Request<ProbeRequest>,
    ) -> Result<Response<ProbeResponse>, Status> {
        Ok(Response::new(ProbeResponse { ready: Some(true) }))
    }
}
