//! Kubernetes [`ClusterLease`] backend.
//!
//! Implements the cluster-wide liveness lease using a `coordination.k8s.io`
//! `Lease` object.  The lease's `renewTime` is used as a freshness signal: a
//! holder whose `renewTime` is older than the configured recovery timeout is
//! considered dead and may be taken over.
//!
//! This module is only compiled with the `coordination` feature, which pulls in
//! the `kube` and `k8s-openapi` crates.

use anyhow::{Context as _, Result};
use async_trait::async_trait;
use std::time::Duration;

use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use k8s_openapi::chrono::Utc;
use kube::api::{Api, ObjectMeta, PostParams};
use kube::Client;

use super::{ClusterAcquire, ClusterLease};

/// A Kubernetes `coordination.k8s.io` Lease used as the cluster liveness lock.
pub struct K8sClusterLease {
    api: Api<Lease>,
    name: String,
    holder: String,
    lease_duration: Duration,
    recovery_timeout: Duration,
}

impl K8sClusterLease {
    /// Build a cluster lease handle for `name` in `namespace`, using the
    /// in-cluster (or kubeconfig) credentials resolved by [`Client::try_default`].
    pub async fn connect(
        namespace: &str,
        name: impl Into<String>,
        holder: impl Into<String>,
        lease_duration: Duration,
        recovery_timeout: Duration,
    ) -> Result<Self> {
        let client = Client::try_default()
            .await
            .context("initialize kubernetes client")?;
        Ok(Self::with_client(
            client,
            namespace,
            name,
            holder,
            lease_duration,
            recovery_timeout,
        ))
    }

    /// Build a cluster lease handle from an existing [`Client`].
    pub fn with_client(
        client: Client,
        namespace: &str,
        name: impl Into<String>,
        holder: impl Into<String>,
        lease_duration: Duration,
        recovery_timeout: Duration,
    ) -> Self {
        Self {
            api: Api::namespaced(client, namespace),
            name: name.into(),
            holder: holder.into(),
            lease_duration,
            recovery_timeout,
        }
    }

    fn now_micro() -> MicroTime {
        MicroTime(Utc::now())
    }

    /// Build the LeaseSpec that records us as the current holder.
    fn owned_spec(&self) -> LeaseSpec {
        let now = Self::now_micro();
        LeaseSpec {
            holder_identity: Some(self.holder.clone()),
            lease_duration_seconds: Some(self.lease_duration.as_secs() as i32),
            acquire_time: Some(now.clone()),
            renew_time: Some(now),
            ..Default::default()
        }
    }

    /// Create the Lease object claiming it for ourselves.
    async fn create_owned(&self) -> Result<()> {
        let lease = Lease {
            metadata: ObjectMeta {
                name: Some(self.name.clone()),
                ..Default::default()
            },
            spec: Some(self.owned_spec()),
        };
        self.api
            .create(&PostParams::default(), &lease)
            .await
            .context("create cluster lease")?;
        Ok(())
    }

    /// Replace an existing Lease, claiming it for ourselves.  Uses the
    /// `resourceVersion` from `current` for optimistic concurrency so a
    /// concurrent take-over attempt is rejected with HTTP 409.
    async fn replace_owned(&self, current: &Lease) -> Result<()> {
        let mut next = current.clone();
        next.spec = Some(self.owned_spec());
        // Keep the resourceVersion for optimistic concurrency control.
        next.metadata = ObjectMeta {
            name: Some(self.name.clone()),
            resource_version: current.metadata.resource_version.clone(),
            ..Default::default()
        };
        self.api
            .replace(&self.name, &PostParams::default(), &next)
            .await
            .context("replace cluster lease")?;
        Ok(())
    }
}

/// True if `holder` matches our identity (treating an empty/absent holder as
/// "unowned", i.e. takeable).
fn held_by(spec: &LeaseSpec, me: &str) -> bool {
    matches!(spec.holder_identity.as_deref(), Some(h) if h == me)
}

/// How long ago `renew_time` was, or `None` if absent.
fn age_of(renew_time: &Option<MicroTime>) -> Option<Duration> {
    renew_time.as_ref().and_then(|t| {
        let elapsed = Utc::now().signed_duration_since(t.0);
        elapsed.to_std().ok()
    })
}

#[async_trait]
impl ClusterLease for K8sClusterLease {
    async fn try_acquire(&self) -> Result<ClusterAcquire> {
        match self.api.get_opt(&self.name).await.context("get lease")? {
            None => {
                // No lease yet — create one for ourselves.
                self.create_owned().await?;
                Ok(ClusterAcquire::Acquired)
            }
            Some(lease) => {
                let spec = lease.spec.clone().unwrap_or_default();
                let holder = spec.holder_identity.clone().unwrap_or_default();

                if held_by(&spec, &self.holder) || holder.is_empty() {
                    // We already hold it (or it is unowned) — refresh.
                    self.replace_owned(&lease).await?;
                    return Ok(ClusterAcquire::Acquired);
                }

                // Held by someone else — is it stale?
                match age_of(&spec.renew_time) {
                    Some(age) if age >= self.recovery_timeout => {
                        // Holder is dead: take over (optimistic concurrency
                        // guards against a competing take-over).
                        self.replace_owned(&lease).await?;
                        Ok(ClusterAcquire::Acquired)
                    }
                    Some(since_renew) => Ok(ClusterAcquire::HeldByLiveHolder {
                        holder,
                        since_renew,
                    }),
                    None => {
                        // No renewTime recorded — treat as takeable.
                        self.replace_owned(&lease).await?;
                        Ok(ClusterAcquire::Acquired)
                    }
                }
            }
        }
    }

    async fn renew(&self) -> Result<()> {
        let lease = self
            .api
            .get_opt(&self.name)
            .await
            .context("get lease for renew")?;
        match lease {
            Some(lease) => self.replace_owned(&lease).await,
            None => self.create_owned().await,
        }
    }

    async fn release(&self) -> Result<()> {
        let lease = self
            .api
            .get_opt(&self.name)
            .await
            .context("get lease for release")?;
        let Some(current) = lease else {
            return Ok(());
        };
        let spec = current.spec.clone().unwrap_or_default();
        // Only clear ownership if we still hold it.
        if !held_by(&spec, &self.holder) {
            return Ok(());
        }
        let mut next = current.clone();
        next.spec = Some(LeaseSpec {
            holder_identity: None,
            renew_time: None,
            lease_duration_seconds: spec.lease_duration_seconds,
            ..Default::default()
        });
        next.metadata = ObjectMeta {
            name: Some(self.name.clone()),
            resource_version: current.metadata.resource_version.clone(),
            ..Default::default()
        };
        self.api
            .replace(&self.name, &PostParams::default(), &next)
            .await
            .context("release cluster lease")?;
        Ok(())
    }
}

/// Sanitize an arbitrary string into a DNS-1123 subdomain suitable for a
/// Kubernetes object name (lowercase alphanumerics, `-` and `.`).
pub fn sanitize_lease_name(raw: &str) -> String {
    let mut out: String = raw
        .chars()
        .map(|c| {
            let c = c.to_ascii_lowercase();
            if c.is_ascii_alphanumeric() || c == '-' || c == '.' {
                c
            } else {
                '-'
            }
        })
        .collect();
    // Trim to a sane length and strip leading/trailing non-alphanumerics.
    out.truncate(200);
    let trimmed = out.trim_matches(|c: char| !c.is_ascii_alphanumeric());
    if trimmed.is_empty() {
        "ublk-azblob".to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_makes_valid_names() {
        assert_eq!(
            sanitize_lease_name("mycontainer/My_Blob.img"),
            "mycontainer-my-blob.img"
        );
        assert_eq!(sanitize_lease_name("___"), "ublk-azblob");
        assert_eq!(sanitize_lease_name("UPPER"), "upper");
    }

    #[test]
    fn held_by_matches_identity() {
        let spec = LeaseSpec {
            holder_identity: Some("node-a".into()),
            ..Default::default()
        };
        assert!(held_by(&spec, "node-a"));
        assert!(!held_by(&spec, "node-b"));
    }
}
