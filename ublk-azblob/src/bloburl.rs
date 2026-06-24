//! Parsing of full Azure blob URLs into their component parts.
//!
//! A single blob URL such as
//! `https://acct.blob.core.windows.net/container/path/to/blob.vhd` (optionally
//! carrying a `?snapshot=` timestamp and/or a SAS query) is the user-facing way
//! to select the blob for the single-device `run` / `test` / `copy` commands
//! (`--blob-url`).  The CSI controller also parses `templateBlobUrl` golden-image
//! sources through [`parse_blob_url`].
//!
//! Both Azure subdomain hosts (`<account>.blob.core.windows.net`) and
//! path-style / Azurite hosts (`host:port/<account>/...`) are supported.

use anyhow::Context as _;

/// A parsed Azure blob URL (e.g. `--blob-url` or a `templateBlobUrl`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplateBlobRef {
    /// Blob *service* URL the rest of the code expects (`build_container_client`
    /// appends `/container`): subdomain-style `https://acct.blob.core.windows.net`
    /// for Azure, or `http://host:port/account` for Azurite/path-style.
    pub service_url: String,
    /// Storage account name.
    pub account: String,
    /// Container name.
    pub container: String,
    /// Blob name (may contain `/`).
    pub blob: String,
    /// Optional `snapshot=` timestamp from the URL.
    pub snapshot: Option<String>,
    /// Optional SAS query string (everything except `snapshot`, present only when
    /// the URL carries a `sig=` signature).
    pub sas: Option<String>,
}

/// Parse a full Azure blob URL into its components.
///
/// Supports both Azure subdomain hosts (`<account>.blob.core.windows.net`) and
/// path-style/Azurite hosts (`host:port/<account>/...`). Any `snapshot=` query
/// is split out; the remaining query (when it carries a `sig=`) is returned as
/// the SAS token.
pub fn parse_blob_url(url: &str) -> anyhow::Result<TemplateBlobRef> {
    let parsed =
        azure_core::http::Url::parse(url).with_context(|| format!("parse blob URL: {url}"))?;
    let scheme = parsed.scheme();
    let host = parsed
        .host_str()
        .context("blob URL has no host")?
        .to_string();

    // Split query into snapshot vs the rest (SAS).
    let mut snapshot = None;
    let mut sas_pairs: Vec<(String, String)> = Vec::new();
    let mut has_sig = false;
    for (k, v) in parsed.query_pairs() {
        if k == "snapshot" {
            snapshot = Some(v.into_owned());
        } else {
            if k == "sig" {
                has_sig = true;
            }
            sas_pairs.push((k.into_owned(), v.into_owned()));
        }
    }
    let sas = if has_sig {
        let mut tmp = azure_core::http::Url::parse("https://x/").unwrap();
        tmp.query_pairs_mut().extend_pairs(&sas_pairs);
        tmp.query().map(|q| q.to_string())
    } else {
        None
    };

    let segments: Vec<String> = parsed
        .path_segments()
        .map(|it| {
            it.filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default();

    // Subdomain/production style: `<account>.<host-suffix>` → account is the
    // first host label, the path is `<container>/<blob...>`.
    let azure_subdomain = is_subdomain_host(&host);
    let (service_url, account, container, blob) = if azure_subdomain {
        let account = host.split('.').next().unwrap_or("").to_string();
        if segments.len() < 2 {
            anyhow::bail!("blob URL missing container/blob path: {url}");
        }
        let container = segments[0].clone();
        let blob = segments[1..].join("/");
        let port = parsed.port().map(|p| format!(":{p}")).unwrap_or_default();
        (format!("{scheme}://{host}{port}"), account, container, blob)
    } else {
        // Path-style / Azurite: `host:port/<account>/<container>/<blob...>`.
        if segments.len() < 3 {
            anyhow::bail!("blob URL missing account/container/blob path: {url}");
        }
        let account = segments[0].clone();
        let container = segments[1].clone();
        let blob = segments[2..].join("/");
        let port = parsed.port().map(|p| format!(":{p}")).unwrap_or_default();
        (
            format!("{scheme}://{host}{port}/{account}"),
            account,
            container,
            blob,
        )
    };

    if container.is_empty() || blob.is_empty() {
        anyhow::bail!("blob URL missing container or blob: {url}");
    }
    Ok(TemplateBlobRef {
        service_url,
        account,
        container,
        blob,
        snapshot,
        sas,
    })
}

/// Whether `host` is subdomain / *production* style — i.e. the storage account
/// is the first host label (`<account>.blob.core.windows.net`, or a custom
/// `<account>.host...` such as an Azurite `<account>.azurite.<ns>...`).
///
/// Path-style hosts — where the account is instead the first URL *path* segment
/// — are IP literals and single-label hosts (e.g. Azurite's `127.0.0.1` or
/// `azurite`). This mirrors the Azure SDK / Azurite "product style URL"
/// detection (account from host unless the host is an IP/bare name).
pub fn is_subdomain_host(host: &str) -> bool {
    let is_ip =
        host.parse::<std::net::IpAddr>().is_ok() || (host.starts_with('[') && host.ends_with(']'));
    !is_ip && host.contains('.')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_blob_url_azure_subdomain() {
        let r =
            parse_blob_url("https://myacct.blob.core.windows.net/images/golden/disk.vhd").unwrap();
        assert_eq!(r.service_url, "https://myacct.blob.core.windows.net");
        assert_eq!(r.account, "myacct");
        assert_eq!(r.container, "images");
        assert_eq!(r.blob, "golden/disk.vhd");
        assert_eq!(r.snapshot, None);
        assert_eq!(r.sas, None);
    }

    #[test]
    fn parse_blob_url_azurite_path_style() {
        let r = parse_blob_url("http://127.0.0.1:10000/devstoreaccount1/images/golden/disk.vhd")
            .unwrap();
        assert_eq!(r.service_url, "http://127.0.0.1:10000/devstoreaccount1");
        assert_eq!(r.account, "devstoreaccount1");
        assert_eq!(r.container, "images");
        assert_eq!(r.blob, "golden/disk.vhd");
    }

    #[test]
    fn parse_blob_url_with_sas_and_snapshot() {
        let r = parse_blob_url(
            "https://myacct.blob.core.windows.net/c/b?snapshot=2024-01-02T03:04:05.0Z&sv=2022-11-02&sig=ABC%2Bdef&se=2030-01-01",
        )
        .unwrap();
        assert_eq!(r.account, "myacct");
        assert_eq!(r.container, "c");
        assert_eq!(r.blob, "b");
        assert_eq!(r.snapshot.as_deref(), Some("2024-01-02T03:04:05.0Z"));
        let sas = r.sas.expect("sas present");
        assert!(sas.contains("sig=ABC%2Bdef"));
        assert!(sas.contains("sv=2022-11-02"));
        assert!(!sas.contains("snapshot"));
    }

    #[test]
    fn parse_blob_url_no_sig_means_no_sas() {
        // A bare query without a signature is not treated as a SAS token.
        let r = parse_blob_url("https://myacct.blob.core.windows.net/c/b?foo=bar").unwrap();
        assert_eq!(r.sas, None);
    }

    #[test]
    fn parse_blob_url_custom_subdomain_host() {
        // A non-`.blob.` multi-label host (e.g. an Azurite custom subdomain) is
        // production style: the account is the first host label.
        let r = parse_blob_url(
            "http://devstoreaccount1.azurite.kube-system.svc.cluster.local:10000/c/blob/path",
        )
        .unwrap();
        assert_eq!(
            r.service_url,
            "http://devstoreaccount1.azurite.kube-system.svc.cluster.local:10000"
        );
        assert_eq!(r.account, "devstoreaccount1");
        assert_eq!(r.container, "c");
        assert_eq!(r.blob, "blob/path");
    }

    #[test]
    fn is_subdomain_host_classification() {
        assert!(is_subdomain_host("myacct.blob.core.windows.net"));
        assert!(is_subdomain_host(
            "devstoreaccount1.azurite.kube-system.svc.cluster.local"
        ));
        // IP literals and single-label hosts are path-style.
        assert!(!is_subdomain_host("127.0.0.1"));
        assert!(!is_subdomain_host("azurite"));
        assert!(!is_subdomain_host("localhost"));
        assert!(!is_subdomain_host("[::1]"));
    }

    #[test]
    fn parse_blob_url_rejects_incomplete() {
        assert!(parse_blob_url("https://myacct.blob.core.windows.net/onlycontainer").is_err());
        assert!(parse_blob_url("http://127.0.0.1:10000/acct/onlycontainer").is_err());
        assert!(parse_blob_url("not a url").is_err());
    }
}
