//! Authentication helpers for Azure Storage.
//!
//! Supports five auth modes:
//!
//! 1. **Managed Identity (MSI)** — uses [`azure_identity::ManagedIdentityCredential`].
//!    Suitable for production workloads running on Azure VMs / AKS / App Service.
//!    System-assigned and user-assigned (client/object/resource ID) identities are
//!    both supported.
//!
//! 2. **Workload Identity** — uses [`azure_identity::WorkloadIdentityCredential`],
//!    the recommended way to access Azure from AKS pods via a federated
//!    Kubernetes service-account token (Microsoft Entra Workload ID). The
//!    client id, tenant id and projected token file are taken from the standard
//!    `AZURE_CLIENT_ID` / `AZURE_TENANT_ID` / `AZURE_FEDERATED_TOKEN_FILE`
//!    environment variables injected by the workload-identity webhook, or from
//!    explicit overrides.
//!
//! 3. **Shared Key (account key)** — implements the Azure Storage
//!    `SharedKey` HMAC-SHA256 signing scheme as a pipeline [`Policy`].
//!    Used for Azurite (the local emulator) and any environment where you have
//!    the raw storage account key.  Azurite does **not** support Entra ID / MSI,
//!    so this path is the only option for local dev and the docker-compose e2e test.
//!
//! 4. **Service Principal** — uses [`azure_identity::ClientSecretCredential`],
//!    authenticating with a Microsoft Entra application client id, tenant id and
//!    client secret.
//!
//! 5. **Shared Access Signature (SAS)** — appends a SAS query string to every
//!    request via [`SasPolicy`]. Used to read a `templateBlobUrl` golden image
//!    that carries its own SAS, possibly from a different storage account.
//!
//! ## Note on SDK preview status
//! `azure_identity` and `azure_storage_blob` are 0.x / preview crates.  All
//! auth construction lives here so a breaking SDK change only requires editing
//! this file.

use anyhow::Context as _;
use azure_core::credentials::{Secret, TokenCredential};
use azure_core::http::{
    policies::{auth::BearerTokenAuthorizationPolicy, Policy, PolicyResult},
    ClientOptions, Context, Pipeline, Request,
};
use azure_identity::{
    ClientSecretCredential, ManagedIdentityCredential, ManagedIdentityCredentialOptions,
    UserAssignedId, WorkloadIdentityCredential, WorkloadIdentityCredentialOptions,
};
use azure_storage_blob::{BlobContainerClient, BlobContainerClientOptions, BlobServiceClient};
use base64::{engine::general_purpose::STANDARD as BASE64_STD, Engine as _};
use std::path::PathBuf;
use std::sync::Arc;
use time::{macros::format_description, OffsetDateTime};
use tracing::debug;

// ── Credential types ──────────────────────────────────────────────────────────

/// User-assigned identity selector for MSI.
#[derive(Clone, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum UserAssignedIdentity {
    ClientId(String),
    ObjectId(String),
    ResourceId(String),
}

/// How to authenticate against Azure Blob Storage.
#[derive(Clone, Debug)]
pub enum AuthConfig {
    /// Managed Identity (system-assigned or user-assigned).
    Msi(Option<UserAssignedIdentity>),

    /// Microsoft Entra Workload Identity (federated Kubernetes token).
    ///
    /// Each field falls back to its standard environment variable when `None`:
    /// `client_id` → `AZURE_CLIENT_ID`, `tenant_id` → `AZURE_TENANT_ID`,
    /// `token_file` → `AZURE_FEDERATED_TOKEN_FILE`.
    WorkloadIdentity {
        client_id: Option<String>,
        tenant_id: Option<String>,
        token_file: Option<String>,
    },

    /// Service principal (Entra ID application) with a client secret.
    ///
    /// Suitable when the workload authenticates as an app registration rather
    /// than a managed identity (e.g. an AKS pod handed an SP client id/secret).
    ServicePrincipal {
        tenant_id: String,
        client_id: String,
        client_secret: String,
    },

    /// Storage account shared key (HMAC-SHA256).
    ///
    /// Required for Azurite and environments where an Entra ID credential is
    /// unavailable.
    SharedKey {
        account_name: String,
        account_key: String,
    },

    /// Shared Access Signature (SAS) token.
    ///
    /// The token is the query string of a SAS URL (with or without a leading
    /// `?`).  Used to read a `templateBlobUrl` that carries its own SAS, possibly
    /// from a different storage account than the driver's own credentials.
    Sas { sas_token: String },
}

// ── BlobContainerClient factory ───────────────────────────────────────────────

/// Build a `BlobContainerClient` from a service URL, container name, and auth config.
///
/// The returned client has the appropriate auth policy already wired into its
/// pipeline.  No Azure SDK types escape this function — callers only receive
/// the opaque `BlobContainerClient`.
pub fn build_container_client(
    service_url: &str,
    container_name: &str,
    auth: &AuthConfig,
) -> anyhow::Result<BlobContainerClient> {
    let url = azure_core::http::Url::parse(service_url)
        .with_context(|| format!("parse service URL: {service_url}"))?;

    match auth {
        AuthConfig::Msi(user_assigned) => {
            let opts = user_assigned.as_ref().map(|id| {
                let uid = match id {
                    UserAssignedIdentity::ClientId(s) => UserAssignedId::ClientId(s.clone()),
                    UserAssignedIdentity::ObjectId(s) => UserAssignedId::ObjectId(s.clone()),
                    UserAssignedIdentity::ResourceId(s) => UserAssignedId::ResourceId(s.clone()),
                };
                ManagedIdentityCredentialOptions {
                    user_assigned_id: Some(uid),
                    ..Default::default()
                }
            });
            debug!(user_assigned = ?user_assigned, "using Managed Identity credential");
            let cred =
                ManagedIdentityCredential::new(opts).context("create ManagedIdentityCredential")?;
            let svc = BlobServiceClient::new(url, Some(cred), None)
                .context("create BlobServiceClient (MSI)")?;
            Ok(svc.blob_container_client(container_name))
        }

        AuthConfig::WorkloadIdentity {
            client_id,
            tenant_id,
            token_file,
        } => {
            debug!(
                client_id = ?client_id,
                tenant_id = ?tenant_id,
                "using Workload Identity credential"
            );
            let opts = WorkloadIdentityCredentialOptions {
                client_id: client_id.clone(),
                tenant_id: tenant_id.clone(),
                token_file_path: token_file.clone().map(PathBuf::from),
                ..Default::default()
            };
            let cred = WorkloadIdentityCredential::new(Some(opts))
                .context("create WorkloadIdentityCredential")?;
            let svc = BlobServiceClient::new(url, Some(cred), None)
                .context("create BlobServiceClient (Workload Identity)")?;
            Ok(svc.blob_container_client(container_name))
        }

        AuthConfig::ServicePrincipal {
            tenant_id,
            client_id,
            client_secret,
        } => {
            debug!(client_id = %client_id, "using ServicePrincipal (client secret) credential");
            let cred = ClientSecretCredential::new(
                tenant_id,
                client_id.clone(),
                Secret::from(client_secret.clone()),
                None,
            )
            .context("create ClientSecretCredential")?;
            let svc = BlobServiceClient::new(url, Some(cred), None)
                .context("create BlobServiceClient (ServicePrincipal)")?;
            Ok(svc.blob_container_client(container_name))
        }

        AuthConfig::SharedKey {
            account_name,
            account_key,
        } => {
            debug!(account = %account_name, "using SharedKey credential");
            let policy = SharedKeyPolicy::new(account_name.clone(), account_key.clone())
                .context("create SharedKeyPolicy")?;
            // Use `None` credential so the SDK doesn't add a Bearer-token Authorization header;
            // our SharedKeyPolicy is injected via `client_options.per_try_policies`.
            let mut container_opts = BlobContainerClientOptions::default();
            container_opts
                .client_options
                .per_try_policies
                .push(Arc::new(policy));
            let container_url = format!("{}/{container_name}", service_url.trim_end_matches('/'));
            let container_url = azure_core::http::Url::parse(&container_url)
                .with_context(|| format!("parse container URL: {container_url}"))?;
            BlobContainerClient::new(container_url, None, Some(container_opts))
                .context("create BlobContainerClient (SharedKey)")
        }

        AuthConfig::Sas { sas_token } => {
            debug!("using SAS token credential");
            let policy = SasPolicy::new(sas_token);
            let mut container_opts = BlobContainerClientOptions::default();
            container_opts
                .client_options
                .per_try_policies
                .push(Arc::new(policy));
            let container_url = format!("{}/{container_name}", service_url.trim_end_matches('/'));
            let container_url = azure_core::http::Url::parse(&container_url)
                .with_context(|| format!("parse container URL: {container_url}"))?;
            // `None` credential: the SasPolicy appends the SAS query string to
            // every request, so the SDK must not also add an auth header.
            BlobContainerClient::new(container_url, None, Some(container_opts))
                .context("create BlobContainerClient (SAS)")
        }
    }
}

/// Build a bare [`Pipeline`] wired with the same authentication as
/// [`build_container_client`], for issuing storage REST requests the typed SDK
/// client does not expose (currently `Get Page Ranges`, i.e. `?comp=pagelist`).
///
/// SharedKey/SAS auth is injected as a per-try policy (so it re-signs on every
/// retry, exactly like the SDK client); Entra auth (MSI / Workload Identity /
/// Service Principal) is injected as a bearer-token policy scoped to
/// `https://storage.azure.com/.default`, mirroring `BlobServiceClient::new`.
pub fn build_pipeline(auth: &AuthConfig) -> anyhow::Result<Pipeline> {
    let mut per_try_policies: Vec<Arc<dyn Policy>> = Vec::new();
    match auth {
        AuthConfig::SharedKey {
            account_name,
            account_key,
        } => {
            let policy = SharedKeyPolicy::new(account_name.clone(), account_key.clone())
                .context("create SharedKeyPolicy")?;
            per_try_policies.push(Arc::new(policy));
        }
        AuthConfig::Sas { sas_token } => {
            per_try_policies.push(Arc::new(SasPolicy::new(sas_token)));
        }
        _ => {
            let cred = build_token_credential(auth)?
                .context("Entra auth produced no token credential")?;
            per_try_policies.push(Arc::new(BearerTokenAuthorizationPolicy::new(
                cred,
                vec!["https://storage.azure.com/.default"],
            )));
        }
    }
    Ok(Pipeline::new(
        option_env!("CARGO_PKG_NAME"),
        option_env!("CARGO_PKG_VERSION"),
        ClientOptions::default(),
        Vec::default(),
        per_try_policies,
        None,
    ))
}

/// Build an Entra (Microsoft Entra ID) token credential for `auth`, or `None`
/// for SharedKey/SAS auth (which don't yield an OAuth token).
fn build_token_credential(auth: &AuthConfig) -> anyhow::Result<Option<Arc<dyn TokenCredential>>> {
    let cred: Arc<dyn TokenCredential> = match auth {
        AuthConfig::Msi(user_assigned) => {
            let opts = user_assigned.as_ref().map(|id| {
                let uid = match id {
                    UserAssignedIdentity::ClientId(s) => UserAssignedId::ClientId(s.clone()),
                    UserAssignedIdentity::ObjectId(s) => UserAssignedId::ObjectId(s.clone()),
                    UserAssignedIdentity::ResourceId(s) => UserAssignedId::ResourceId(s.clone()),
                };
                ManagedIdentityCredentialOptions {
                    user_assigned_id: Some(uid),
                    ..Default::default()
                }
            });
            ManagedIdentityCredential::new(opts).context("create ManagedIdentityCredential")?
        }
        AuthConfig::WorkloadIdentity {
            client_id,
            tenant_id,
            token_file,
        } => {
            let opts = WorkloadIdentityCredentialOptions {
                client_id: client_id.clone(),
                tenant_id: tenant_id.clone(),
                token_file_path: token_file.clone().map(PathBuf::from),
                ..Default::default()
            };
            WorkloadIdentityCredential::new(Some(opts))
                .context("create WorkloadIdentityCredential")?
        }
        AuthConfig::ServicePrincipal {
            tenant_id,
            client_id,
            client_secret,
        } => ClientSecretCredential::new(
            tenant_id,
            client_id.clone(),
            Secret::from(client_secret.clone()),
            None,
        )
        .context("create ClientSecretCredential")?,
        AuthConfig::SharedKey { .. } | AuthConfig::Sas { .. } => return Ok(None),
    };
    Ok(Some(cred))
}

/// Mint a `Bearer <token>` storage authorization header value for `auth`, or
/// `None` for SharedKey/SAS.
///
/// Used as the `x-ms-copy-source-authorization` header on a server-side
/// `Put Page From URL` copy so the storage service can read a source blob in a
/// *different* account (cross-account golden-image template) under the driver's
/// own Entra identity.
pub async fn storage_bearer_token(auth: &AuthConfig) -> anyhow::Result<Option<String>> {
    let Some(cred) = build_token_credential(auth)? else {
        return Ok(None);
    };
    let token = cred
        .get_token(&["https://storage.azure.com/.default"], None)
        .await
        .context("acquire storage OAuth token for copy-source-authorization")?;
    Ok(Some(format!("Bearer {}", token.token.secret())))
}

// ── SharedKeyPolicy ───────────────────────────────────────────────────────────

/// Azure Storage SharedKey HMAC-SHA256 signing policy.
///
/// This policy computes the `Authorization: SharedKey` header required by
/// Azurite and any endpoint that uses account-key authentication.
///
/// It is injected via `ClientOptions::per_try_policies` so it runs on every
/// attempt (including retries) and correctly handles the `x-ms-date` header.
#[derive(Debug)]
pub struct SharedKeyPolicy {
    account_name: String,
    /// Raw (decoded) 64-byte account key.
    account_key_bytes: Vec<u8>,
}

impl SharedKeyPolicy {
    pub fn new(account_name: String, account_key_b64: String) -> anyhow::Result<Self> {
        let account_key_bytes = BASE64_STD
            .decode(&account_key_b64)
            .context("decode account key (expected base64)")?;
        Ok(Self {
            account_name,
            account_key_bytes,
        })
    }

    /// Build the canonicalized `x-ms-*` headers string.
    fn canonicalized_headers(headers: &azure_core::http::headers::Headers) -> String {
        let mut ms_headers: Vec<(String, String)> = headers
            .iter()
            .filter_map(|(name, value)| {
                let n = name.as_str().to_lowercase();
                if n.starts_with("x-ms-") {
                    Some((n, value.as_str().trim().to_string()))
                } else {
                    None
                }
            })
            .collect();
        ms_headers.sort_by(|a, b| a.0.cmp(&b.0));
        ms_headers
            .iter()
            .map(|(k, v)| format!("{k}:{v}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Build the canonicalized resource string.
    ///
    /// Format: `/{account}{path}\n{sorted query params}`
    ///
    /// Note: for Azurite the account name appears as the first path segment, so
    /// the canonicalized resource intentionally contains it twice (e.g.
    /// `/devstoreaccount1/devstoreaccount1/container/blob`). Azurite's own
    /// SharedKey signing computes the resource the same way, so this matches.
    fn canonicalized_resource(account: &str, url: &azure_core::http::Url) -> String {
        let path = url.path();
        let mut result = format!("/{account}{path}");

        // Collect and sort query parameters
        let mut params: Vec<(String, String)> = url
            .query_pairs()
            .map(|(k, v)| (k.to_lowercase(), v.to_string()))
            .collect();
        params.sort_by(|a, b| a.0.cmp(&b.0));

        // Group values for the same key (comma-separated)
        let mut grouped: Vec<(String, Vec<String>)> = Vec::new();
        for (k, v) in params {
            if let Some(last) = grouped.last_mut() {
                if last.0 == k {
                    last.1.push(v);
                    continue;
                }
            }
            grouped.push((k, vec![v]));
        }

        for (k, vals) in &grouped {
            result.push('\n');
            result.push_str(k);
            result.push(':');
            result.push_str(&vals.join(","));
        }

        result
    }

    /// Compute the HMAC-SHA256 `Authorization` header value.
    fn sign(&self, request: &Request) -> String {
        let method = request.method().as_str().to_uppercase();
        let headers = request.headers();

        let get_header = |name: &str| -> String {
            headers
                .get_optional_str(&azure_core::http::headers::HeaderName::from(
                    name.to_string(),
                ))
                .unwrap_or_default()
                .to_string()
        };

        let content_length = get_header("content-length");
        // Per spec: use empty string (not "0") when content-length is absent or zero
        let content_length_str = if content_length == "0" || content_length.is_empty() {
            String::new()
        } else {
            content_length
        };

        let canonicalized_headers = Self::canonicalized_headers(headers);
        let canonicalized_resource =
            Self::canonicalized_resource(&self.account_name, request.url());

        let string_to_sign = format!(
            "{method}\n{}\n{}\n{content_length_str}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{canonicalized_headers}\n{canonicalized_resource}",
            get_header("content-encoding"),
            get_header("content-language"),
            get_header("content-md5"),
            get_header("content-type"),
            get_header("date"),
            get_header("if-modified-since"),
            get_header("if-match"),
            get_header("if-none-match"),
            get_header("if-unmodified-since"),
            get_header("range"),
        );

        let mac = hmac_sha256::HMAC::mac(string_to_sign.as_bytes(), &self.account_key_bytes);
        let sig = BASE64_STD.encode(mac);
        format!("SharedKey {}:{}", self.account_name, sig)
    }
}

#[async_trait::async_trait]
impl Policy for SharedKeyPolicy {
    async fn send(
        &self,
        ctx: &Context,
        request: &mut Request,
        next: &[Arc<dyn Policy>],
    ) -> PolicyResult {
        // Add x-ms-date if not already present (some SDK versions add x-ms-date automatically;
        // we add it here to ensure it's available for the signature computation).
        if request
            .headers()
            .get_optional_str(&azure_core::http::headers::HeaderName::from(
                "x-ms-date".to_string(),
            ))
            .is_none()
        {
            // RFC 1123 / HTTP-date format required by Azure Storage, e.g.
            // "Mon, 02 Jan 2006 15:04:05 GMT". `time`'s `Rfc2822` formatter
            // renders the zone as "+0000" rather than "GMT", which strict
            // endpoints/emulators reject, so format the HTTP-date explicitly.
            let now = OffsetDateTime::now_utc();
            let http_date = format_description!(
                "[weekday repr:short], [day] [month repr:short] [year] \
                 [hour]:[minute]:[second] GMT"
            );
            let date_str = now.format(http_date).unwrap_or_else(|_| now.to_string());
            request.insert_header("x-ms-date", date_str);
        }

        let auth = self.sign(request);
        request.insert_header("authorization", auth);

        // Continue down the pipeline
        next[0].send(ctx, request, &next[1..]).await
    }
}

// ── SasPolicy ─────────────────────────────────────────────────────────────────

/// Appends a SAS (Shared Access Signature) query string to every request URL.
///
/// A SAS URL authenticates the request entirely through its query parameters
/// (`sv`, `sig`, `se`, …), so this policy merges those parameters into each
/// outgoing request URL.  It is injected via `per_try_policies` (like
/// [`SharedKeyPolicy`]) and the client is built with a `None` credential so the
/// SDK does not add a conflicting `Authorization` header.
#[derive(Debug)]
pub struct SasPolicy {
    /// SAS query pairs, parsed once (leading `?` stripped).
    pairs: Vec<(String, String)>,
}

impl SasPolicy {
    pub fn new(sas_token: &str) -> Self {
        let trimmed = sas_token.trim_start_matches('?');
        let pairs = azure_core::http::Url::parse(&format!("https://x/?{trimmed}"))
            .map(|u| {
                u.query_pairs()
                    .map(|(k, v)| (k.into_owned(), v.into_owned()))
                    .collect()
            })
            .unwrap_or_default();
        Self { pairs }
    }
}

#[async_trait::async_trait]
impl Policy for SasPolicy {
    async fn send(
        &self,
        ctx: &Context,
        request: &mut Request,
        next: &[Arc<dyn Policy>],
    ) -> PolicyResult {
        // Merge the SAS pairs into the request URL, skipping any already present
        // so a snapshot/comp query the SDK added is preserved.
        let existing: std::collections::HashSet<String> = request
            .url()
            .query_pairs()
            .map(|(k, _)| k.into_owned())
            .collect();
        let to_add: Vec<(String, String)> = self
            .pairs
            .iter()
            .filter(|(k, _)| !existing.contains(k))
            .cloned()
            .collect();
        if !to_add.is_empty() {
            let mut qp = request.url_mut().query_pairs_mut();
            for (k, v) in &to_add {
                qp.append_pair(k, v);
            }
        }
        next[0].send(ctx, request, &next[1..]).await
    }
}

#[cfg(test)]
mod tests {
    use super::{build_container_client, AuthConfig, SharedKeyPolicy};

    #[test]
    fn canonicalized_resource_azurite_includes_account_in_path() {
        // Azurite URLs carry the account as the first path segment, so the
        // canonicalized resource contains it twice — this is what Azurite's own
        // SharedKey signing expects.
        let url = azure_core::http::Url::parse(
            "http://127.0.0.1:10000/devstoreaccount1/mycontainer/myblob",
        )
        .unwrap();

        let resource = SharedKeyPolicy::canonicalized_resource("devstoreaccount1", &url);

        assert_eq!(
            resource,
            "/devstoreaccount1/devstoreaccount1/mycontainer/myblob"
        );
    }

    #[test]
    fn canonicalized_resource_keeps_normal_storage_paths() {
        let url = azure_core::http::Url::parse(
            "https://devstoreaccount1.blob.core.windows.net/mycontainer/myblob",
        )
        .unwrap();

        let resource = SharedKeyPolicy::canonicalized_resource("devstoreaccount1", &url);

        assert_eq!(resource, "/devstoreaccount1/mycontainer/myblob");
    }

    #[test]
    fn build_container_client_workload_identity() {
        // The Workload Identity credential reads the federated token file at
        // construction time, so point it at a temp file and pass explicit
        // client/tenant ids (so the test does not depend on ambient env vars).
        let token_path = std::env::temp_dir().join(format!(
            "ublk-azblob-wi-{}-{}.tok",
            std::process::id(),
            line!()
        ));
        std::fs::write(&token_path, "fake.federated.jwt").unwrap();

        let auth = AuthConfig::WorkloadIdentity {
            client_id: Some("00000000-0000-0000-0000-000000000000".to_string()),
            tenant_id: Some("contoso.onmicrosoft.com".to_string()),
            token_file: Some(token_path.to_string_lossy().into_owned()),
        };

        let client = build_container_client(
            "https://devstoreaccount1.blob.core.windows.net/",
            "mycontainer",
            &auth,
        );

        let ok = client.is_ok();
        let err = client.err().map(|e| format!("{e:#}"));
        std::fs::remove_file(&token_path).ok();
        assert!(ok, "workload identity client failed: {err:?}");
    }

    #[tokio::test]
    async fn sas_policy_merges_query_and_preserves_existing() {
        use super::SasPolicy;
        use azure_core::error::{Error, ErrorKind};
        use azure_core::http::policies::{Policy, PolicyResult};
        use azure_core::http::{Context, Method, Request, Url};
        use std::sync::{Arc, Mutex};

        // Terminal policy that captures the final request URL after `SasPolicy`
        // has merged the SAS query in, then short-circuits the pipeline.
        #[derive(Debug)]
        struct Capture(Arc<Mutex<Option<Url>>>);

        #[async_trait::async_trait]
        impl Policy for Capture {
            async fn send(
                &self,
                _ctx: &Context,
                request: &mut Request,
                _next: &[Arc<dyn Policy>],
            ) -> PolicyResult {
                *self.0.lock().unwrap() = Some(request.url().clone());
                Err(Error::with_message(ErrorKind::Other, "terminal"))
            }
        }

        let captured = Arc::new(Mutex::new(None));
        let terminal: Arc<dyn Policy> = Arc::new(Capture(captured.clone()));

        let sas =
            SasPolicy::new("?sv=2021-08-06&ss=b&sig=abc%2Bdef%3D&se=2030-01-01T00%3A00%3A00Z");

        // The request URL already carries `comp`/`snapshot` (as the SDK would add
        // for a Put Page against a snapshot); those must survive the merge.
        let url = Url::parse(
            "https://acct.blob.core.windows.net/container/blob\
             ?comp=page&snapshot=2020-01-01T00%3A00%3A00Z",
        )
        .unwrap();
        let mut request = Request::new(url, Method::Put);

        let _ = sas
            .send(
                &Context::new(),
                &mut request,
                std::slice::from_ref(&terminal),
            )
            .await;

        let final_url = captured.lock().unwrap().clone().expect("url captured");
        let pairs: std::collections::HashMap<String, String> = final_url
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();

        // Pre-existing keys are preserved with their original values.
        assert_eq!(pairs.get("comp").map(String::as_str), Some("page"));
        assert_eq!(
            pairs.get("snapshot").map(String::as_str),
            Some("2020-01-01T00:00:00Z")
        );
        // SAS pairs are merged in.
        assert_eq!(pairs.get("sv").map(String::as_str), Some("2021-08-06"));
        assert_eq!(pairs.get("ss").map(String::as_str), Some("b"));
        assert_eq!(pairs.get("sig").map(String::as_str), Some("abc+def="));
        assert_eq!(
            pairs.get("se").map(String::as_str),
            Some("2030-01-01T00:00:00Z")
        );
    }
}
