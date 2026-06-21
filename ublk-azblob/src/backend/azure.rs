//! Azure Page Blob implementation of `BlobBackend`.
//!
//! `AzurePageBlobBackend` wraps `azure_storage_blob::BlobContainerClient` and
//! exposes the read/write/clear/flush/size operations through the `BlobBackend`
//! trait.  The Azure SDK is **completely isolated** inside this module — no SDK
//! type crosses the `BlobBackend` boundary into the rest of the crate.

use super::BlobBackend;
use crate::coordination::{BlobLock, LockError};
use anyhow::{bail, Context as _};
use async_trait::async_trait;
use azure_storage_blob::{
    models::{
        BlobClientAcquireLeaseResultHeaders, BlobClientDownloadOptions,
        BlobClientGetPropertiesResultHeaders, HttpRange, PageBlobClientClearPagesOptions,
        PageBlobClientCreateOptions, PageBlobClientUploadPagesFromUrlOptions,
        PageBlobClientUploadPagesOptions,
    },
    BlobClient, BlobContainerClient,
};
use bytes::Bytes;
use futures::stream::{StreamExt as _, TryStreamExt as _};
use std::sync::RwLock;
use tracing::{error, instrument, trace};

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
    /// Optional snapshot ID (`x-ms-snapshot` timestamp).  When set, every
    /// operation targets the immutable snapshot rather than the live blob, so
    /// the backend is effectively read-only.
    snapshot: Option<String>,
    /// Blob-lease id (`x-ms-lease-id`) to attach to every mutating request.
    ///
    /// When cluster coordination is enabled the holder takes an exclusive lease
    /// on the page blob; Azure then rejects any Put Page / Clear Pages that does
    /// not carry the matching lease id with HTTP 412. This is set (via
    /// [`AzurePageBlobBackend::set_lease_id`]) once the lease has been acquired,
    /// before any I/O is served. `None` means no lease is held (coordination
    /// disabled), and requests are sent without a lease condition.
    lease_id: RwLock<Option<String>>,
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
            snapshot: None,
            lease_id: RwLock::new(None),
        }
    }

    /// Target a specific blob snapshot.
    ///
    /// A blob snapshot is an immutable, read-only view of the blob taken at a
    /// point in time; mutating operations against it are rejected by Azure.
    /// Callers should pair this with a read-only mount.
    pub fn with_snapshot(mut self, snapshot: impl Into<String>) -> Self {
        self.snapshot = Some(snapshot.into());
        self
    }

    /// Build a `BlobClient` for the target blob, scoped to the configured
    /// snapshot when one is set.
    fn blob_client(&self) -> anyhow::Result<BlobClient> {
        let client = self.container.blob_client(&self.blob_name);
        match &self.snapshot {
            Some(snapshot) => client
                .with_snapshot(snapshot)
                .with_context(|| format!("target snapshot '{snapshot}'")),
            None => Ok(client),
        }
    }

    /// Attach (or clear) the blob-lease id carried on every mutating request.
    ///
    /// Call this with `Some(id)` after acquiring the coordination blob lease and
    /// before serving any I/O, so writes to the now-leased blob carry the
    /// matching `x-ms-lease-id` instead of being rejected with HTTP 412.
    pub fn set_lease_id(&self, lease_id: Option<String>) {
        *self.lease_id.write().unwrap() = lease_id;
    }

    /// Snapshot the current lease id, if any.
    fn lease_id(&self) -> Option<String> {
        self.lease_id.read().unwrap().clone()
    }

    /// Server-side copy `total_size` bytes from `source_url` into this page blob
    /// using concurrent `Put Page From URL` requests.
    ///
    /// This is a true server-to-server copy: the storage service fetches each
    /// 4 MiB range directly from `source_url` — no bytes flow through this
    /// process. The destination blob must already exist (call [`BlobBackend::create`]
    /// first) and be at least `total_size`.
    ///
    /// `copy_source_auth` is the driver credential used to authorize the service
    /// to read a source in a *different* storage account (`Some` for Entra auth);
    /// pass `None` when the source carries its own SAS or lives in the same
    /// account. The Entra token is re-minted per ~8 GiB batch so a copy whose
    /// wall-clock time exceeds the (~1 h) token lifetime does not start failing
    /// late chunks with HTTP 403. Ranges are 512-aligned; sparse source ranges
    /// copy as zeros.
    pub async fn copy_pages_from_url(
        &self,
        source_url: &str,
        total_size: u64,
        copy_source_auth: Option<crate::auth::AuthConfig>,
    ) -> anyhow::Result<()> {
        if !total_size.is_multiple_of(512) {
            bail!("copy size {total_size} is not 512-byte aligned");
        }
        /// Re-mint the copy-source token roughly every this many bytes.
        const BATCH_BYTES: u64 = 8 * 1024 * 1024 * 1024;
        // Per-request size (override with `UBLK_COPY_CHUNK_BYTES`); `Put Page From
        // URL` caps it at 4 MiB.
        let chunk = crate::backend::copy_chunk_bytes();
        // Default concurrency to the logical CPU count (same auto-sizing as the
        // cache warm-up path), overridable with `UBLK_COPY_CONCURRENCY`.
        let concurrency = std::env::var("UBLK_COPY_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or_else(crate::backend::cpu_count);

        let blob_client = self.container.blob_client(&self.blob_name);
        let page_client = blob_client.page_blob_client();
        let lease_id = self.lease_id();

        let n_chunks = total_size.div_ceil(chunk);
        let chunks_per_batch = (BATCH_BYTES / chunk).max(concurrency as u64);
        trace!(
            total_size,
            n_chunks,
            chunk,
            concurrency,
            "server-side copy via Put Page From URL"
        );

        let mut batch_start = 0u64;
        while batch_start < n_chunks {
            let batch_end = (batch_start + chunks_per_batch).min(n_chunks);
            // Fresh copy-source authorization for this batch (Entra tokens expire).
            let csa = match &copy_source_auth {
                Some(auth) => crate::auth::storage_bearer_token(auth)
                    .await
                    .context("mint copy-source authorization token")?,
                None => None,
            };
            let copies = (batch_start..batch_end).map(|i| {
                let offset = i * chunk;
                let len = chunk.min(total_size - offset);
                let src_range = HttpRange::new(offset, len);
                let dst_range = HttpRange::new(offset, len);
                let opts = PageBlobClientUploadPagesFromUrlOptions {
                    lease_id: lease_id.clone(),
                    copy_source_authorization: csa.clone(),
                    ..Default::default()
                };
                let page_client = &page_client;
                let source_url = source_url.to_string();
                async move {
                    page_client
                        .upload_pages_from_url(source_url, src_range, len, dst_range, Some(opts))
                        .await
                        .with_context(|| format!("put page from url offset={offset} len={len}"))?;
                    Ok::<(), anyhow::Error>(())
                }
            });
            futures::stream::iter(copies)
                .buffer_unordered(concurrency)
                .try_collect::<()>()
                .await?;
            batch_start = batch_end;
        }
        Ok(())
    }
}

#[async_trait]
impl BlobBackend for AzurePageBlobBackend {
    #[instrument(skip(self), fields(blob = %self.blob_name))]
    async fn create(&self, size: u64) -> anyhow::Result<()> {
        if self.snapshot.is_some() {
            bail!("cannot create: backend targets a read-only blob snapshot");
        }
        if size == 0 || !size.is_multiple_of(512) {
            bail!("size must be a non-zero multiple of 512 bytes, got {size}");
        }
        // Ensure the target container exists before provisioning the blob.
        // A 409 Conflict means it already exists, which is fine.
        if let Err(err) = self.container.create(None).await {
            if err.http_status() != Some(azure_core::http::StatusCode::Conflict) {
                error!(
                    "create container failed: status={:?}, error={:?}",
                    err.http_status(),
                    err
                );
                return Err(err).context("create container");
            }
        }
        let blob_client = self.container.blob_client(&self.blob_name);
        // Idempotency (CSI requires CreateVolume not to mutate an existing
        // volume): a plain Put Page Blob would overwrite and zero an existing
        // blob, so if the blob already exists, return success when the size
        // matches and fail when it differs, instead of recreating it.
        match blob_client.get_properties(None).await {
            Ok(props) => {
                let existing = props.content_length()?.unwrap_or(0);
                if existing == size {
                    trace!(size, "page blob already exists with the requested size");
                    return Ok(());
                }
                bail!(
                    "blob '{}' already exists with size {existing}, requested {size}",
                    self.blob_name
                );
            }
            Err(err) if err.http_status() == Some(azure_core::http::StatusCode::NotFound) => {
                // Does not exist yet — provision it below.
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("check existing blob '{}' before create", self.blob_name)
                });
            }
        }
        let page_client = blob_client.page_blob_client();
        trace!(size, "creating page blob");
        let opts = PageBlobClientCreateOptions {
            lease_id: self.lease_id(),
            ..Default::default()
        };
        page_client
            .create(size, Some(opts))
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
        let blob_client = self.blob_client()?;
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
        if self.snapshot.is_some() {
            bail!("cannot write: backend targets a read-only blob snapshot");
        }
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
        let opts = PageBlobClientUploadPagesOptions {
            lease_id: self.lease_id(),
            ..Default::default()
        };
        page_client
            .upload_pages(
                azure_core::http::RequestContent::from(data.to_vec()),
                len,
                range,
                Some(opts),
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
        if self.snapshot.is_some() {
            bail!("cannot clear: backend targets a read-only blob snapshot");
        }
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
        let opts = PageBlobClientClearPagesOptions {
            lease_id: self.lease_id(),
            ..Default::default()
        };
        page_client
            .clear_pages(range, Some(opts))
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
    async fn delete(&self) -> anyhow::Result<()> {
        let blob_client = self.container.blob_client(&self.blob_name);
        trace!("deleting blob");
        if let Err(err) = blob_client.delete(None).await {
            // A 404 means the blob is already gone — treat delete as idempotent.
            if err.http_status() != Some(azure_core::http::StatusCode::NotFound) {
                return Err(err).with_context(|| format!("delete blob '{}'", self.blob_name));
            }
        }
        Ok(())
    }

    #[instrument(skip(self), fields(blob = %self.blob_name))]
    async fn size(&self) -> anyhow::Result<u64> {
        let blob_client = self.blob_client()?;
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
    async fn etag(&self) -> anyhow::Result<Option<String>> {
        let blob_client = self.blob_client()?;
        let props = blob_client
            .get_properties(None)
            .await
            .with_context(|| format!("get_properties for blob '{}'", self.blob_name))?;
        Ok(props.etag()?.map(|e| e.to_string()))
    }
}

// ── Blob lease (cluster coordination "blob lock") ─────────────────────────────

/// Azure blob-lease implementation of [`BlobLock`].
///
/// Holds its own `BlobContainerClient` (with the auth pipeline wired in) and the
/// target blob name.  Kept next to [`AzurePageBlobBackend`] so all Azure SDK
/// types stay inside this module.
///
/// The lease is intentionally **finite**: Azure caps an explicit lease duration
/// at 60s, and the coordination layer ([`crate::coordination`]) keeps it alive
/// with a renewal loop (renewing roughly every `lease_duration / 3`). This is
/// deliberate — an infinite lease would never expire if the holder node died,
/// permanently blocking takeover. With a finite lease, a dead holder's lease
/// lapses within ≤60s, after which another node can break/acquire it (gated by
/// the cluster-lease recovery timeout). A clean shutdown releases it immediately.
#[cfg_attr(not(feature = "coordination"), allow(dead_code))]
pub struct AzureBlobLock {
    container: BlobContainerClient,
    blob_name: String,
}

#[cfg_attr(not(feature = "coordination"), allow(dead_code))]
impl AzureBlobLock {
    /// Construct a blob lock for `blob_name` from an already-configured
    /// `BlobContainerClient`.
    pub fn new(container: BlobContainerClient, blob_name: impl Into<String>) -> Self {
        Self {
            container,
            blob_name: blob_name.into(),
        }
    }

    /// Map an Azure SDK error to a [`LockError`], classifying lease conflicts
    /// (HTTP 409 Conflict / 412 Precondition Failed) as [`LockError::Held`].
    fn classify(err: azure_core::Error, what: &str) -> LockError {
        match err.http_status() {
            Some(azure_core::http::StatusCode::Conflict)
            | Some(azure_core::http::StatusCode::PreconditionFailed) => LockError::Held,
            _ => LockError::Other(anyhow::Error::new(err).context(what.to_string())),
        }
    }
}

#[async_trait]
impl BlobLock for AzureBlobLock {
    #[instrument(skip(self), fields(blob = %self.blob_name, duration_secs))]
    async fn acquire(&self, duration_secs: i32) -> Result<String, LockError> {
        let blob_client = self.container.blob_client(&self.blob_name);
        trace!(duration_secs, "acquiring blob lease");
        let result = blob_client
            .acquire_lease(duration_secs, None)
            .await
            .map_err(|e| Self::classify(e, "acquire blob lease"))?;
        let lease_id = result
            .lease_id()
            .map_err(|e| LockError::Other(anyhow::Error::new(e).context("read lease id")))?
            .ok_or_else(|| {
                LockError::Other(anyhow::anyhow!(
                    "acquire lease response missing x-ms-lease-id header"
                ))
            })?;
        Ok(lease_id)
    }

    #[instrument(skip(self), fields(blob = %self.blob_name))]
    async fn renew(&self, lease_id: &str) -> anyhow::Result<()> {
        let blob_client = self.container.blob_client(&self.blob_name);
        trace!("renewing blob lease");
        blob_client
            .renew_lease(lease_id.to_string(), None)
            .await
            .with_context(|| format!("renew blob lease '{}'", self.blob_name))?;
        Ok(())
    }

    #[instrument(skip(self), fields(blob = %self.blob_name))]
    async fn release(&self, lease_id: &str) -> anyhow::Result<()> {
        let blob_client = self.container.blob_client(&self.blob_name);
        trace!("releasing blob lease");
        blob_client
            .release_lease(lease_id.to_string(), None)
            .await
            .with_context(|| format!("release blob lease '{}'", self.blob_name))?;
        Ok(())
    }

    #[instrument(skip(self), fields(blob = %self.blob_name))]
    async fn break_lock(&self) -> anyhow::Result<()> {
        let blob_client = self.container.blob_client(&self.blob_name);
        trace!("breaking blob lease");
        // No options ⇒ default break period (the lease breaks at the end of its
        // remaining period).  We pass a 0 break period so the lease becomes
        // available immediately for take-over.
        let opts = azure_storage_blob::models::BlobClientBreakLeaseOptions {
            break_period: Some(0),
            ..Default::default()
        };
        blob_client
            .break_lease(Some(opts))
            .await
            .with_context(|| format!("break blob lease '{}'", self.blob_name))?;
        Ok(())
    }
}
