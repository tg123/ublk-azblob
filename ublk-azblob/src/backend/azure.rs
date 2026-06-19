//! Azure Page Blob implementation of `BlobBackend`.
//!
//! `AzurePageBlobBackend` wraps `azure_storage_blob::BlobContainerClient` and
//! exposes the read/write/clear/flush/size operations through the `BlobBackend`
//! trait.  The Azure SDK is **completely isolated** inside this module — no SDK
//! type crosses the `BlobBackend` boundary into the rest of the crate.

use super::BlobBackend;
use anyhow::{bail, Context as _};
use async_trait::async_trait;
use azure_storage_blob::{
    models::{
        BlobClientCreateSnapshotResultHeaders, BlobClientDownloadOptions,
        BlobClientGetPropertiesResultHeaders, HttpRange,
    },
    BlobContainerClient,
};
use bytes::Bytes;
use tracing::{instrument, trace};

/// Azure Page Blob backend.
///
/// Internally holds a `BlobContainerClient` (which carries the pipeline with
/// auth policies already wired in) and the target blob name.
///
/// All Azure SDK types stay inside this struct; none escape via the
/// `BlobBackend` trait.
pub struct AzurePageBlobBackend {
    container: BlobContainerClient,
    blob_name: String,
}

impl AzurePageBlobBackend {
    /// Construct a backend from an already-configured `BlobContainerClient`
    /// and a target blob name.
    ///
    /// The container does **not** need to exist yet; call [`BlobBackend::create`]
    /// to provision the blob.
    pub fn new(container: BlobContainerClient, blob_name: impl Into<String>) -> Self {
        Self {
            container,
            blob_name: blob_name.into(),
        }
    }
}

#[async_trait]
impl BlobBackend for AzurePageBlobBackend {
    #[instrument(skip(self), fields(blob = %self.blob_name))]
    async fn create(&self, size: u64) -> anyhow::Result<()> {
        if size == 0 || !size.is_multiple_of(512) {
            bail!("size must be a non-zero multiple of 512 bytes, got {size}");
        }
        // Ensure the target container exists before provisioning the blob.
        // A 409 Conflict means it already exists, which is fine.
        if let Err(err) = self.container.create(None).await {
            if err.http_status() != Some(azure_core::http::StatusCode::Conflict) {
                return Err(err).context("create container");
            }
        }
        let blob_client = self.container.blob_client(&self.blob_name);
        let page_client = blob_client.page_blob_client();
        trace!(size, "creating page blob");
        page_client
            .create(size, None)
            .await
            .with_context(|| format!("create page blob '{}' size={size}", self.blob_name))?;
        Ok(())
    }

    #[instrument(skip(self), fields(blob = %self.blob_name, offset, len))]
    async fn read(&self, offset: u64, len: u64) -> anyhow::Result<Bytes> {
        if !offset.is_multiple_of(512) {
            bail!("offset {offset} is not 512-byte aligned");
        }
        if !len.is_multiple_of(512) {
            bail!("len {len} is not 512-byte aligned");
        }
        let blob_client = self.container.blob_client(&self.blob_name);
        trace!(offset, len, "downloading range");
        let opts = BlobClientDownloadOptions {
            range: Some(HttpRange::new(offset, len)),
            ..Default::default()
        };
        let result = blob_client.download(Some(opts)).await.with_context(|| {
            format!(
                "download blob '{}' offset={offset} len={len}",
                self.blob_name
            )
        })?;
        let data = result
            .body
            .collect()
            .await
            .with_context(|| format!("collect body for blob '{}'", self.blob_name))?;
        Ok(data)
    }

    #[instrument(skip(self, data), fields(blob = %self.blob_name, offset, len = data.len()))]
    async fn write(&self, offset: u64, data: Bytes) -> anyhow::Result<()> {
        if !offset.is_multiple_of(512) {
            bail!("offset {offset} is not 512-byte aligned");
        }
        let len = data.len() as u64;
        if !len.is_multiple_of(512) {
            bail!("data length {len} is not 512-byte aligned");
        }
        let blob_client = self.container.blob_client(&self.blob_name);
        let page_client = blob_client.page_blob_client();
        let range = HttpRange::new(offset, len);
        trace!(offset, len, "uploading pages");
        page_client
            .upload_pages(
                azure_core::http::RequestContent::from(data.to_vec()),
                len,
                range,
                None,
            )
            .await
            .with_context(|| {
                format!(
                    "upload_pages blob '{}' offset={offset} len={len}",
                    self.blob_name
                )
            })?;
        Ok(())
    }

    #[instrument(skip(self), fields(blob = %self.blob_name, offset, len))]
    async fn clear(&self, offset: u64, len: u64) -> anyhow::Result<()> {
        if !offset.is_multiple_of(512) {
            bail!("offset {offset} is not 512-byte aligned");
        }
        if !len.is_multiple_of(512) {
            bail!("len {len} is not 512-byte aligned");
        }
        let blob_client = self.container.blob_client(&self.blob_name);
        let page_client = blob_client.page_blob_client();
        let range = HttpRange::new(offset, len);
        trace!(offset, len, "clearing pages");
        page_client
            .clear_pages(range, None)
            .await
            .with_context(|| {
                format!(
                    "clear_pages blob '{}' offset={offset} len={len}",
                    self.blob_name
                )
            })?;
        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        // Page blobs are write-through by design — every upload_pages call is
        // durable once it returns 201.  Nothing to do here for Phase 1.
        Ok(())
    }

    #[instrument(skip(self), fields(blob = %self.blob_name))]
    async fn size(&self) -> anyhow::Result<u64> {
        let blob_client = self.container.blob_client(&self.blob_name);
        let props = blob_client
            .get_properties(None)
            .await
            .with_context(|| format!("get_properties for blob '{}'", self.blob_name))?;
        let len = props.content_length()?.ok_or_else(|| {
            anyhow::anyhow!(
                "blob '{}': missing Content-Length in get_properties response",
                self.blob_name
            )
        })?;
        Ok(len)
    }

    #[instrument(skip(self), fields(blob = %self.blob_name))]
    async fn snapshot(&self) -> anyhow::Result<String> {
        let blob_client = self.container.blob_client(&self.blob_name);
        trace!("creating blob snapshot");
        let result = blob_client
            .create_snapshot(None)
            .await
            .with_context(|| format!("create_snapshot for blob '{}'", self.blob_name))?;
        let snapshot = result
            .snapshot()
            .with_context(|| format!("read snapshot header for blob '{}'", self.blob_name))?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "blob '{}': missing x-ms-snapshot in create_snapshot response",
                    self.blob_name
                )
            })?;
        Ok(snapshot)
    }
}
