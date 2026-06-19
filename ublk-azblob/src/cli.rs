//! Shared CLI options and construction helpers.
//!
//! Every binary in this crate (the `ublk-azblob` device server and the
//! standalone `ublk-azblob-import` / `ublk-azblob-snapshot` tools) accepts the
//! same Azure storage selectors and authentication flags.  Those options live
//! here in [`StorageArgs`] (flattened into each command's clap parser) together
//! with the helpers that turn them into an authenticated [`BlobBackend`].

use crate::auth::{self, AuthConfig, UserAssignedIdentity};
use crate::backend::{azure::AzurePageBlobBackend, BlobBackend};
use anyhow::Context as _;
use clap::Args;
use std::sync::Arc;

/// Azure storage selectors and authentication flags shared by all binaries.
#[derive(Args, Debug, Clone)]
pub struct StorageArgs {
    /// Azure Storage account name (e.g. `mystorageaccount`).
    #[arg(long, env = "AZURE_STORAGE_ACCOUNT")]
    pub account: String,

    /// Blob container name.
    #[arg(long, env = "AZURE_STORAGE_CONTAINER")]
    pub container: String,

    /// Page blob name (path within the container).
    #[arg(long, env = "AZURE_STORAGE_BLOB")]
    pub blob: String,

    /// Azure Storage service endpoint URL.
    ///
    /// Defaults to `https://<account>.blob.core.windows.net/`.
    /// For Azurite use `http://127.0.0.1:10000/<account>`.
    #[arg(long, env = "AZURE_STORAGE_ENDPOINT")]
    pub endpoint: Option<String>,

    /// Storage account key (base64).  Enables SharedKey auth mode.
    ///
    /// Mutually exclusive with --msi / --msi-*.  Use for Azurite and local dev.
    #[arg(long, env = "AZURE_STORAGE_KEY", conflicts_with_all = ["msi", "msi_client_id", "msi_object_id", "msi_resource_id"])]
    pub account_key: Option<String>,

    /// Enable system-assigned Managed Identity.
    #[arg(long, env = "AZURE_USE_MSI")]
    pub msi: bool,

    /// User-assigned Managed Identity — client ID.
    #[arg(long, env = "AZURE_MSI_CLIENT_ID", conflicts_with_all = ["msi_object_id", "msi_resource_id"])]
    pub msi_client_id: Option<String>,

    /// User-assigned Managed Identity — object ID.
    #[arg(long, env = "AZURE_MSI_OBJECT_ID", conflicts_with_all = ["msi_resource_id"])]
    pub msi_object_id: Option<String>,

    /// User-assigned Managed Identity — resource ID.
    #[arg(long, env = "AZURE_MSI_RESOURCE_ID")]
    pub msi_resource_id: Option<String>,
}

impl StorageArgs {
    /// Resolve the service endpoint, defaulting to the public Azure endpoint.
    pub fn endpoint(&self) -> String {
        self.endpoint
            .clone()
            .unwrap_or_else(|| format!("https://{}.blob.core.windows.net/", self.account))
    }

    /// Build the authentication configuration from the supplied flags.
    pub fn build_auth(&self) -> anyhow::Result<AuthConfig> {
        if let Some(key) = &self.account_key {
            return Ok(AuthConfig::SharedKey {
                account_name: self.account.clone(),
                account_key: key.clone(),
            });
        }

        // Prefer user-assigned identities if given, fall back to system-assigned.
        let user_assigned = self
            .msi_client_id
            .as_ref()
            .map(|s| UserAssignedIdentity::ClientId(s.clone()))
            .or_else(|| {
                self.msi_object_id
                    .as_ref()
                    .map(|s| UserAssignedIdentity::ObjectId(s.clone()))
            })
            .or_else(|| {
                self.msi_resource_id
                    .as_ref()
                    .map(|s| UserAssignedIdentity::ResourceId(s.clone()))
            });

        if user_assigned.is_some() || self.msi {
            return Ok(AuthConfig::Msi(user_assigned));
        }

        anyhow::bail!(
            "No auth method specified. Use --account-key for Azurite/dev, \
             or --msi / --msi-client-id for production (Managed Identity)."
        );
    }

    /// Build an authenticated [`AzurePageBlobBackend`] for the selected blob.
    pub fn build_backend(&self) -> anyhow::Result<Arc<dyn BlobBackend>> {
        let auth = self.build_auth()?;
        let container_client =
            auth::build_container_client(&self.endpoint(), &self.container, &auth)
                .context("build container client")?;
        Ok(Arc::new(AzurePageBlobBackend::new(
            container_client,
            self.blob.clone(),
        )))
    }
}

/// Initialize tracing with the crate's default `info` directive.
pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("ublk_azblob=info".parse().unwrap()),
        )
        .init();
}
