//! Azure Page Blob implementation of `BlobBackend`.
//!
//! `AzurePageBlobBackend` wraps `azure_storage_blob::BlobContainerClient` and
//! exposes the read/write/clear/flush/size operations through the `BlobBackend`
//! trait.  The Azure SDK is **completely isolated** inside this module — no SDK
//! type crosses the `BlobBackend` boundary into the rest of the crate.

use super::io_gateway::{current_class, AzureIoGateway, IoClass};
use super::BlobBackend;
use crate::coordination::{BlobLock, LockError};
use anyhow::{bail, Context as _};
use async_trait::async_trait;
use azure_core::http::{Context, Method, Pipeline, Request, Url};
use azure_storage_blob::{
    models::{
        BlobClientAcquireLeaseResultHeaders, BlobClientDownloadOptions,
        BlobClientGetPropertiesResultHeaders, HttpRange, PageBlobClientClearPagesOptions,
        PageBlobClientClearPagesResultHeaders, PageBlobClientCreateOptions,
        PageBlobClientUploadPagesFromUrlOptions, PageBlobClientUploadPagesOptions,
        PageBlobClientUploadPagesResultHeaders,
    },
    BlobClient, BlobContainerClient,
};
use bytes::Bytes;
use futures::stream::{StreamExt as _, TryStreamExt as _};
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tracing::{error, info, instrument, trace, warn};

/// Maximum attempts (initial try + retries) for a single Azure page-blob
/// operation before the error is surfaced to the block layer.
const AZURE_OP_ATTEMPTS: u32 = 5;
/// Backoff before the first retry; doubles each attempt up to [`AZURE_RETRY_MAX`].
const AZURE_RETRY_BASE: Duration = Duration::from_millis(200);
/// Upper bound on a single backoff sleep.
const AZURE_RETRY_MAX: Duration = Duration::from_secs(5);

/// Whether an Azure SDK error is worth retrying. Throttling and transient server
/// failures — HTTP 408 (Request Timeout), 429 (Too Many Requests) and any 5xx —
/// plus non-HTTP errors (transport timeouts, connection resets, DNS) are
/// retryable. Deterministic client errors (auth failures, 404, 416 invalid
/// range, precondition) are not: retrying them only delays the inevitable.
fn is_retryable(err: &azure_core::Error) -> bool {
    match err.kind() {
        azure_core::error::ErrorKind::HttpResponse { status, .. } => {
            let code = u16::from(*status);
            code == 408 || code == 429 || (500..=599).contains(&code)
        }
        // No HTTP status ⇒ a transport/IO/timeout error — worth retrying.
        _ => true,
    }
}

/// Run an idempotent Azure page-blob operation with bounded exponential backoff
/// (plus jitter) on transient failures.
///
/// Reads (`Get Blob`) and page writes (`Put Page` / `Clear Pages`) are
/// idempotent, so retrying is safe. Without this, a single throttling (503),
/// timeout, or mid-stream connection reset propagates straight to the ublk/NBD
/// block device as an `Input/output error`, which corrupts an in-flight read
/// (e.g. git's streaming pack read → "invalid index-pack / unresolved deltas").
async fn with_retry<T, F, Fut>(op: &str, blob: &str, mut f: F) -> azure_core::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = azure_core::Result<T>>,
{
    let mut delay = AZURE_RETRY_BASE;
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if attempt >= AZURE_OP_ATTEMPTS || !is_retryable(&e) {
                    return Err(e);
                }
                // Cheap jitter (no `rand` dependency): 0–127 ms off the clock, to
                // desynchronise retries when many pages are throttled at once
                // (e.g. during cache warm-up).
                let jitter = Duration::from_millis(
                    (std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.subsec_nanos())
                        .unwrap_or(0)
                        % 128) as u64,
                );
                let backoff = delay + jitter;
                warn!(
                    op,
                    blob,
                    attempt,
                    backoff_ms = backoff.as_millis() as u64,
                    error = %e,
                    "retryable Azure error; backing off"
                );
                tokio::time::sleep(backoff).await;
                delay = (delay * 2).min(AZURE_RETRY_MAX);
            }
        }
    }
}

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
    /// Shared, process-wide I/O gateway that all reads (downloads) and
    /// writes/clears/copies (uploads) funnel through, enforcing the per-direction
    /// bandwidth ceiling, concurrency cap and priority scheduling.
    gateway: Arc<AzureIoGateway>,
    /// ETag returned by the most recent mutating request (Put Page / Clear
    /// Pages).
    ///
    /// Every page-blob write response already carries the blob's new ETag, so we
    /// cache it here to let [`BlobBackend::etag`] return the post-write validity
    /// token without a separate `get_properties` round-trip. `None` until the
    /// first write of this process's lifetime, in which case `etag` falls back
    /// to `get_properties`.
    last_etag: RwLock<Option<String>>,
    /// Optional auth-wired pipeline for `Get Page Ranges` (`?comp=pagelist`),
    /// which the typed SDK 1.0 client no longer exposes.  `None` disables the
    /// [`BlobBackend::data_ranges`] sparseness query (callers then assume every
    /// byte may contain data).  Built by [`crate::auth::build_pipeline`].
    page_list_pipeline: Option<Pipeline>,
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
            gateway: AzureIoGateway::global(),
            last_etag: RwLock::new(None),
            page_list_pipeline: None,
        }
    }

    /// Attach an auth-wired pipeline used to issue `Get Page Ranges`
    /// (`?comp=pagelist`) requests, enabling [`BlobBackend::data_ranges`].
    ///
    /// Build the pipeline with [`crate::auth::build_pipeline`] so it carries the
    /// same credential as the backend's container client.
    pub fn with_page_list(mut self, pipeline: Pipeline) -> Self {
        self.page_list_pipeline = Some(pipeline);
        self
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

    /// Record the ETag carried by a mutating response so a later [`etag`] call
    /// can avoid a separate `get_properties` round-trip.
    ///
    /// [`etag`]: BlobBackend::etag
    fn record_etag(&self, etag: Option<String>) {
        *self.last_etag.write().unwrap() = etag;
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
    ///
    /// `source_data_ranges` is the source blob's sparseness map (from
    /// [`BlobBackend::data_ranges`]); when `Some`, any chunk lying entirely in a
    /// zero gap is **cleared** on the destination (`Clear Pages`) instead of
    /// being copied — no `Put Page From URL` is issued for it, so the storage
    /// service never copies the source's unwritten free space, yet the
    /// destination is still guaranteed to read back as zero there. This is safe
    /// even when copying into a blob that already holds data (e.g. a retry
    /// against an idempotently-created same-size target). `None` copies every
    /// chunk.
    pub async fn copy_pages_from_url(
        &self,
        source_url: &str,
        total_size: u64,
        copy_source_auth: Option<crate::auth::AuthConfig>,
        source_data_ranges: Option<&[(u64, u64)]>,
    ) -> anyhow::Result<()> {
        if !total_size.is_multiple_of(512) {
            bail!("copy size {total_size} is not 512-byte aligned");
        }
        /// Re-mint the copy-source token roughly every this many bytes.
        const BATCH_BYTES: u64 = 8 * 1024 * 1024 * 1024;
        // Per-request size (override with `UBLK_COPY_CHUNK_BYTES`); `Put Page From
        // URL` caps it at 4 MiB.
        let chunk = crate::backend::copy_chunk_bytes();
        // How many copy chunks to pipeline into the I/O gateway at once. This
        // only bounds in-flight *enqueued* work (and thus memory); the actual
        // server-side upload concurrency and bandwidth are enforced centrally by
        // the gateway's upload pipeline. Overridable with `UBLK_COPY_CONCURRENCY`.
        let pipeline_depth = std::env::var("UBLK_COPY_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or_else(crate::backend::cpu_count);

        let blob_client = self.container.blob_client(&self.blob_name);
        let page_client = Arc::new(blob_client.page_blob_client());
        let lease_id = self.lease_id();
        let gateway = self.gateway.clone();

        let n_chunks = total_size.div_ceil(chunk);
        let chunks_per_batch = (BATCH_BYTES / chunk).max(pipeline_depth as u64);
        trace!(
            total_size,
            n_chunks,
            chunk,
            pipeline_depth,
            clear_zero_ranges = source_data_ranges.is_some(),
            "server-side copy via Put Page From URL (zero gaps cleared, not copied)"
        );

        let mut copied_bytes = 0u64;
        let mut cleared_bytes = 0u64;
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
            // Each chunk either carries source data — copied via Put Page From
            // URL — or lies entirely in a source zero gap. For a zero gap we
            // *clear* the destination range rather than skipping it: this still
            // avoids the source round-trip, but also guarantees the destination
            // reads back as zero there even when it is not a freshly-created
            // blob. `create()` is idempotent and does not zero an existing
            // same-size target, so on a retry/re-run a skipped range could
            // otherwise retain stale data and corrupt the clone.
            let ops = (batch_start..batch_end).map(|i| {
                let offset = i * chunk;
                let len = chunk.min(total_size - offset);
                let is_data = source_data_ranges
                    .map(|ranges| super::range_intersects(ranges, offset, len))
                    .unwrap_or(true);
                if is_data {
                    copied_bytes += len;
                } else {
                    cleared_bytes += len;
                }
                let page_client = page_client.clone();
                let lease_id = lease_id.clone();
                let csa = csa.clone();
                let source_url = source_url.to_string();
                let gateway = gateway.clone();
                async move {
                    gateway
                        .upload(IoClass::Copy, len, async move {
                            if is_data {
                                let opts = PageBlobClientUploadPagesFromUrlOptions {
                                    lease_id,
                                    copy_source_authorization: csa,
                                    ..Default::default()
                                };
                                page_client
                                    .upload_pages_from_url(
                                        source_url,
                                        HttpRange::new(offset, len),
                                        len,
                                        HttpRange::new(offset, len),
                                        Some(opts),
                                    )
                                    .await
                                    .with_context(|| {
                                        format!("put page from url offset={offset} len={len}")
                                    })?;
                            } else {
                                let opts = PageBlobClientClearPagesOptions {
                                    lease_id,
                                    ..Default::default()
                                };
                                page_client
                                    .clear_pages(HttpRange::new(offset, len), Some(opts))
                                    .await
                                    .with_context(|| {
                                        format!("clear pages offset={offset} len={len}")
                                    })?;
                            }
                            Ok::<(), anyhow::Error>(())
                        })
                        .await
                }
            });
            futures::stream::iter(ops)
                .buffer_unordered(pipeline_depth)
                .try_collect::<()>()
                .await?;
            batch_start = batch_end;
        }
        if source_data_ranges.is_some() {
            info!(
                copied_bytes,
                cleared_bytes, total_size, "server-side copy cleared source zero ranges"
            );
        }
        // The server-side copy mutated the blob via many concurrent requests, so
        // any previously cached write ETag is now stale; drop it so a later
        // `etag` call re-reads the authoritative value.
        self.record_etag(None);
        Ok(())
    }

    /// Query Azure `Get Page Ranges` (`?comp=pagelist`) for the byte ranges of
    /// the blob that actually contain data.
    ///
    /// The typed `azure_storage_blob` 1.0 client no longer exposes this
    /// operation, so it is issued as a raw GET through the auth-wired
    /// [`page_list_pipeline`](Self::page_list_pipeline).  Returns `Ok(None)` when
    /// no pipeline was attached (capability disabled).  Returned ranges are
    /// `(offset, len)` pairs, 512-byte aligned, sorted by offset; every byte not
    /// covered reads back as zero.
    async fn list_page_ranges(&self) -> anyhow::Result<Option<Vec<(u64, u64)>>> {
        let Some(pipeline) = &self.page_list_pipeline else {
            return Ok(None);
        };
        let ctx = Context::new();
        let mut ranges: Vec<(u64, u64)> = Vec::new();
        let mut marker: Option<String> = None;
        // `Get Page Ranges` is paginated: a heavily-fragmented blob returns a
        // subset plus a `<NextMarker>`; re-issue with `&marker=` until it is
        // empty so no data ranges are dropped (and warmed regions mistaken for
        // zero gaps).
        loop {
            // Blob URL, encoded identically to every other request this backend makes.
            let mut url: Url = self.container.blob_client(&self.blob_name).url().clone();
            {
                let mut q = url.query_pairs_mut();
                q.append_pair("comp", "pagelist");
                if let Some(snapshot) = &self.snapshot {
                    q.append_pair("snapshot", snapshot);
                }
                if let Some(m) = &marker {
                    q.append_pair("marker", m);
                }
            }
            let mut request = Request::new(url, Method::Get);
            request.insert_header("x-ms-version", PAGE_LIST_API_VERSION);
            trace!(marker = ?marker, "querying page ranges");
            let response = pipeline
                .send(&ctx, &mut request, None)
                .await
                .with_context(|| format!("Get Page Ranges for blob '{}'", self.blob_name))?;
            let status = response.status();
            if !status.is_success() {
                let body = response.into_body().into_string().unwrap_or_default();
                anyhow::bail!(
                    "Get Page Ranges for blob '{}' returned HTTP {status}: {body}",
                    self.blob_name
                );
            }
            let body = response.into_body().into_string().with_context(|| {
                format!("read Get Page Ranges body for blob '{}'", self.blob_name)
            })?;
            let batch = parse_page_ranges(&body).with_context(|| {
                format!("parse Get Page Ranges body for blob '{}'", self.blob_name)
            })?;
            ranges.extend(batch);
            match parse_next_marker(&body) {
                Some(m) => marker = Some(m),
                None => break,
            }
        }
        // Batches arrive ordered, but re-sort defensively across pages.
        ranges.sort_by_key(|&(start, _)| start);
        Ok(Some(ranges))
    }
}

/// Azure Storage REST API version used for the raw `Get Page Ranges` request.
/// Kept in sync with the `azure_storage_blob` SDK's default service version.
const PAGE_LIST_API_VERSION: &str = "2026-04-06";

/// Parse a `Get Page Ranges` XML body into `(offset, len)` data ranges.
///
/// The body looks like
/// `<PageList><PageRange><Start>0</Start><End>511</End></PageRange>...</PageList>`,
/// where `Start`/`End` are **inclusive** 512-aligned byte offsets.  `ClearRange`
/// elements (present only in a diff response) are ignored.  Ranges are returned
/// sorted by offset.
fn parse_page_ranges(body: &str) -> anyhow::Result<Vec<(u64, u64)>> {
    let mut ranges = Vec::new();
    let mut rest = body;
    while let Some(open) = rest.find("<PageRange>") {
        let after = &rest[open + "<PageRange>".len()..];
        let Some(close) = after.find("</PageRange>") else {
            bail!("unterminated <PageRange> element in Get Page Ranges response");
        };
        let segment = &after[..close];
        let start = extract_tag_u64(segment, "Start").context("missing <Start> in <PageRange>")?;
        let end = extract_tag_u64(segment, "End").context("missing <End> in <PageRange>")?;
        if end < start {
            bail!("invalid page range: End ({end}) < Start ({start})");
        }
        ranges.push((start, end - start + 1));
        rest = &after[close + "</PageRange>".len()..];
    }
    ranges.sort_by_key(|&(start, _)| start);
    Ok(ranges)
}

/// Extract a non-empty `<NextMarker>` continuation token from a `Get Page
/// Ranges` response body, if present (an empty `<NextMarker />` means the last
/// page).
fn parse_next_marker(body: &str) -> Option<String> {
    let open = "<NextMarker>";
    let close = "</NextMarker>";
    let start = body.find(open)? + open.len();
    let end = body[start..].find(close)? + start;
    let marker = body[start..end].trim();
    if marker.is_empty() {
        None
    } else {
        Some(marker.to_string())
    }
}

/// Extract the unsigned integer value of `<tag>NNN</tag>` from `segment`.
fn extract_tag_u64(segment: &str, tag: &str) -> anyhow::Result<u64> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = segment
        .find(&open)
        .with_context(|| format!("missing <{tag}>"))?
        + open.len();
    let end = segment[start..]
        .find(&close)
        .with_context(|| format!("missing </{tag}>"))?
        + start;
    segment[start..end]
        .trim()
        .parse::<u64>()
        .with_context(|| format!("parse <{tag}> value"))
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
        let blob_client = Arc::new(self.blob_client()?);
        let blob_name = self.blob_name.clone();
        let class = current_class().unwrap_or(IoClass::ForegroundRead);
        trace!(offset, len, "downloading range");
        self.gateway
            .download(class, len, async move {
                with_retry("download", &blob_name, || {
                    let blob_client = blob_client.clone();
                    async move {
                        let opts = BlobClientDownloadOptions {
                            range: Some(HttpRange::new(offset, len)),
                            ..Default::default()
                        };
                        let result = blob_client.download(Some(opts)).await?;
                        let data = result.body.collect().await?;
                        Ok(data)
                    }
                })
                .await
                .with_context(|| format!("download blob '{blob_name}' offset={offset} len={len}"))
            })
            .await
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
        let page_client = Arc::new(blob_client.page_blob_client());
        let blob_name = self.blob_name.clone();
        let class = current_class().unwrap_or(IoClass::Flush);
        let lease_id = self.lease_id();
        trace!(offset, len, "uploading pages");
        let etag = self
            .gateway
            .upload(class, len, async move {
                with_retry("upload_pages", &blob_name, || {
                    let page_client = page_client.clone();
                    let content = azure_core::http::RequestContent::from(data.to_vec());
                    let range = HttpRange::new(offset, len);
                    let opts = PageBlobClientUploadPagesOptions {
                        lease_id: lease_id.clone(),
                        ..Default::default()
                    };
                    async move {
                        let resp = page_client
                            .upload_pages(content, len, range, Some(opts))
                            .await?;
                        Ok(resp.etag().ok().flatten().map(|e| e.to_string()))
                    }
                })
                .await
                .with_context(|| {
                    format!("upload_pages blob '{blob_name}' offset={offset} len={len}")
                })
            })
            .await?;
        // The Put Page response already carries the blob's new ETag; cache it so
        // a follow-up flush can read the validity token without an extra
        // get_properties round-trip.
        self.record_etag(etag);
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
        let page_client = Arc::new(blob_client.page_blob_client());
        let blob_name = self.blob_name.clone();
        let class = current_class().unwrap_or(IoClass::Flush);
        let lease_id = self.lease_id();
        trace!(offset, len, "clearing pages");
        let etag = self
            .gateway
            .upload(class, 0, async move {
                with_retry("clear_pages", &blob_name, || {
                    let page_client = page_client.clone();
                    let range = HttpRange::new(offset, len);
                    let opts = PageBlobClientClearPagesOptions {
                        lease_id: lease_id.clone(),
                        ..Default::default()
                    };
                    async move {
                        let resp = page_client.clear_pages(range, Some(opts)).await?;
                        Ok(resp.etag().ok().flatten().map(|e| e.to_string()))
                    }
                })
                .await
                .with_context(|| {
                    format!("clear_pages blob '{blob_name}' offset={offset} len={len}")
                })
            })
            .await?;
        // Clear Pages also mutates the blob and returns the new ETag; keep the
        // cached validity token current.
        self.record_etag(etag);
        Ok(())
    }

    async fn data_ranges(&self) -> anyhow::Result<Option<Vec<(u64, u64)>>> {
        self.list_page_ranges().await
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
        // A prior write/clear in this process already learned the blob's current
        // ETag from its response, so reuse it instead of issuing a separate
        // get_properties round-trip on the (latency-bound) flush completion path.
        if let Some(etag) = self.last_etag.read().unwrap().clone() {
            return Ok(Some(etag));
        }
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

#[cfg(test)]
mod tests {
    use super::{extract_tag_u64, parse_next_marker, parse_page_ranges};

    #[test]
    fn parse_next_marker_present_and_absent() {
        // A non-empty marker is returned for continuation.
        let with = "<PageList><NextMarker>2!abc=</NextMarker></PageList>";
        assert_eq!(parse_next_marker(with).as_deref(), Some("2!abc="));
        // An empty (or self-closing) marker means the last page.
        assert_eq!(
            parse_next_marker("<PageList><NextMarker></NextMarker></PageList>"),
            None
        );
        assert_eq!(
            parse_next_marker("<PageList><NextMarker /></PageList>"),
            None
        );
        assert_eq!(parse_next_marker("<PageList />"), None);
    }

    #[test]
    fn parse_empty_page_list() {
        let body = r#"<?xml version="1.0" encoding="utf-8"?><PageList />"#;
        assert!(parse_page_ranges(body).unwrap().is_empty());
    }

    #[test]
    fn parse_single_range_inclusive_to_len() {
        let body = "<PageList><PageRange><Start>0</Start><End>511</End></PageRange></PageList>";
        // Inclusive [0, 511] => offset 0, len 512.
        assert_eq!(parse_page_ranges(body).unwrap(), vec![(0, 512)]);
    }

    #[test]
    fn parse_multiple_ranges_sorted() {
        let body = "<PageList>\
            <PageRange><Start>1024</Start><End>2047</End></PageRange>\
            <PageRange><Start>0</Start><End>511</End></PageRange>\
            </PageList>";
        assert_eq!(
            parse_page_ranges(body).unwrap(),
            vec![(0, 512), (1024, 1024)]
        );
    }

    #[test]
    fn parse_ignores_clear_ranges() {
        let body = "<PageList>\
            <PageRange><Start>0</Start><End>511</End></PageRange>\
            <ClearRange><Start>512</Start><End>1023</End></ClearRange>\
            </PageList>";
        assert_eq!(parse_page_ranges(body).unwrap(), vec![(0, 512)]);
    }

    #[test]
    fn parse_rejects_inverted_range() {
        let body = "<PageList><PageRange><Start>512</Start><End>0</End></PageRange></PageList>";
        assert!(parse_page_ranges(body).is_err());
    }

    #[test]
    fn extract_tag_handles_whitespace() {
        assert_eq!(extract_tag_u64("<Start> 42 </Start>", "Start").unwrap(), 42);
        assert!(extract_tag_u64("<Start>42</Start>", "End").is_err());
    }

    // ── retry classification + backoff behaviour ───────────────────────────

    use super::{is_retryable, with_retry, AZURE_OP_ATTEMPTS};
    use azure_core::error::{Error, ErrorKind};
    use azure_core::http::StatusCode;

    fn http_err(status: StatusCode) -> Error {
        Error::with_message(
            ErrorKind::HttpResponse {
                status,
                error_code: None,
                raw_response: None,
            },
            "synthetic",
        )
    }

    #[test]
    fn retryable_covers_throttling_5xx_and_transport_only() {
        for s in [
            StatusCode::RequestTimeout,      // 408
            StatusCode::TooManyRequests,     // 429
            StatusCode::InternalServerError, // 500
            StatusCode::BadGateway,          // 502
            StatusCode::ServiceUnavailable,  // 503
            StatusCode::GatewayTimeout,      // 504
        ] {
            assert!(is_retryable(&http_err(s)), "{s:?} must be retryable");
        }
        for s in [
            StatusCode::Ok,                           // 200
            StatusCode::Unauthorized,                 // 401
            StatusCode::Forbidden,                    // 403
            StatusCode::NotFound,                     // 404
            StatusCode::PreconditionFailed,           // 412
            StatusCode::RequestedRangeNotSatisfiable, // 416
        ] {
            assert!(!is_retryable(&http_err(s)), "{s:?} must NOT be retryable");
        }
        // No HTTP status ⇒ transport/IO error ⇒ retryable.
        assert!(is_retryable(&Error::with_message(
            ErrorKind::Connection,
            "reset"
        )));
        assert!(is_retryable(&Error::with_message(ErrorKind::Io, "timeout")));
    }

    #[tokio::test]
    async fn with_retry_returns_first_success() {
        let mut calls = 0u32;
        let out: azure_core::Result<u32> = with_retry("op", "blob", || {
            calls += 1;
            async { Ok(7u32) }
        })
        .await;
        assert_eq!(out.unwrap(), 7);
        assert_eq!(calls, 1, "success must not retry");
    }

    #[tokio::test]
    async fn with_retry_recovers_after_transient_failures() {
        let mut calls = 0u32;
        let out: azure_core::Result<u32> = with_retry("op", "blob", || {
            calls += 1;
            let n = calls;
            async move {
                if n < 3 {
                    Err(http_err(StatusCode::ServiceUnavailable))
                } else {
                    Ok(42u32)
                }
            }
        })
        .await;
        assert_eq!(out.unwrap(), 42);
        assert_eq!(calls, 3, "should succeed on the 3rd attempt");
    }

    #[tokio::test]
    async fn with_retry_gives_up_after_max_attempts() {
        let mut calls = 0u32;
        let out: azure_core::Result<u32> = with_retry("op", "blob", || {
            calls += 1;
            async { Err::<u32, _>(http_err(StatusCode::ServiceUnavailable)) }
        })
        .await;
        assert!(out.is_err());
        assert_eq!(calls, AZURE_OP_ATTEMPTS, "must stop at the attempt cap");
    }

    #[tokio::test]
    async fn with_retry_does_not_retry_client_errors() {
        let mut calls = 0u32;
        let out: azure_core::Result<u32> = with_retry("op", "blob", || {
            calls += 1;
            async { Err::<u32, _>(http_err(StatusCode::NotFound)) }
        })
        .await;
        assert!(out.is_err());
        assert_eq!(calls, 1, "a 404 must fail fast without retrying");
    }
}
