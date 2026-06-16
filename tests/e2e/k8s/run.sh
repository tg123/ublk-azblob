#!/usr/bin/env bash
#
# Kubernetes PVC e2e for the ublk-azblob CSI driver.
#
# Spins up a single-node kind cluster, deploys the CSI driver (controller +
# node), an in-cluster Azurite, and then exercises the full PVC lifecycle:
#
#   1. create a PVC backed by the `azblob-ublk` StorageClass
#   2. run a writer Job that writes 8 MiB of random data and records its SHA-256
#   3. delete the writer (NodeUnpublishVolume tears the ublk device down and
#      flushes the page blob)
#   4. run a reader Job that mounts the *same* PVC on a fresh ublk device over
#      the existing page blob and verifies the SHA-256 still matches
#
# This is the Kubernetes counterpart of tests/mount_e2e.rs and proves the data
# survives provision → write → unmount → remount through the page blob.
#
# Requirements (provided by the CI workflow): a Linux host with `ublk_drv`
# loaded, root, Docker, `kind`, and `kubectl`.  When any of these is missing the
# test SKIPS (exit 0) rather than failing, mirroring tests/mount_e2e.rs.
#
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$here/../../.." && pwd)"

CLUSTER="${KIND_CLUSTER:-azblob-e2e}"
IMAGE="${E2E_IMAGE:-ublk-azblob:e2e}"
NS=kube-system
# Azurite well-known development account + key (public, not a real secret).
ACCOUNT=devstoreaccount1
KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
ENDPOINT="http://azurite.${NS}.svc.cluster.local:10000/devstoreaccount1"
CONTAINER=pvc

log()  { echo "=== $* ==="; }
skip() { echo "SKIP: $*" >&2; exit 0; }

# ── Preflight: skip gracefully when the environment can't drive ublk ──────────
[[ "$(id -u)" -eq 0 ]]               || skip "must run as root"
[[ -e /dev/ublk-control ]]           || skip "ublk_drv not loaded (no /dev/ublk-control)"
command -v docker  >/dev/null 2>&1   || skip "docker not found"
command -v kind    >/dev/null 2>&1   || skip "kind not found"
command -v kubectl >/dev/null 2>&1   || skip "kubectl not found"

teardown() {
  log "tearing down kind cluster $CLUSTER"
  kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
}
trap teardown EXIT

# ── Build + load the driver image ─────────────────────────────────────────────
log "building driver image $IMAGE"
docker build -f "$repo_root/deploy/Dockerfile" -t "$IMAGE" "$repo_root"

log "creating kind cluster $CLUSTER"
kind create cluster --name "$CLUSTER" --config "$here/kind-config.yaml" --wait 120s

log "loading $IMAGE into the cluster"
kind load docker-image "$IMAGE" --name "$CLUSTER"

# ── Deploy Azurite + driver config ────────────────────────────────────────────
log "deploying Azurite"
kubectl apply -f "$here/azurite.yaml"
kubectl -n "$NS" rollout status deployment/azurite --timeout=120s

log "creating driver secret + config"
kubectl -n "$NS" create secret generic csi-azblob-secret \
  --from-literal=account="$ACCOUNT" \
  --from-literal=accountKey="$KEY" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" create configmap csi-azblob-config \
  --from-literal=endpoint="$ENDPOINT" \
  --from-literal=container="$CONTAINER" \
  --dry-run=client -o yaml | kubectl apply -f -

# ── Deploy the CSI driver, pinned to the locally-built image ──────────────────
manifests="$(mktemp -d)"
cp "$repo_root"/deploy/kubernetes/*.yaml "$manifests/"
sed -i "s#image: ghcr.io/tg123/ublk-azblob:latest#image: ${IMAGE}#g" "$manifests"/*.yaml
sed -i "s#imagePullPolicy: IfNotPresent#imagePullPolicy: Never#g" "$manifests"/*.yaml

log "deploying CSI driver"
kubectl apply -f "$manifests/csi-driver.yaml"
kubectl apply -f "$manifests/rbac.yaml"
kubectl apply -f "$manifests/storageclass.yaml"
kubectl apply -f "$manifests/controller.yaml"
kubectl apply -f "$manifests/node.yaml"

log "waiting for the driver to become ready"
kubectl -n "$NS" rollout status deployment/csi-azblob-controller --timeout=180s
kubectl -n "$NS" rollout status daemonset/csi-azblob-node --timeout=180s

# ── Run the PVC write/remount/verify cycle ────────────────────────────────────
log "creating PVC"
kubectl apply -f "$repo_root/deploy/example/pvc.yaml"

log "running writer Job"
kubectl apply -f "$here/writer.yaml"
if ! kubectl wait --for=condition=complete job/azblob-writer --timeout=240s; then
  echo "writer Job did not complete:" >&2
  kubectl describe job/azblob-writer || true
  kubectl logs -l app=azblob-writer --tail=200 || true
  kubectl -n "$NS" logs -l app=csi-azblob-node -c azblob --tail=200 || true
  exit 1
fi
kubectl logs -l app=azblob-writer --tail=50 || true

log "deleting writer (triggers NodeUnpublishVolume / device teardown)"
kubectl delete -f "$here/writer.yaml" --wait=true

log "running reader Job (remounts the same PVC, verifies checksum)"
kubectl apply -f "$here/reader.yaml"
if ! kubectl wait --for=condition=complete job/azblob-reader --timeout=240s; then
  echo "reader Job did not complete — data did not survive the remount:" >&2
  kubectl describe job/azblob-reader || true
  kubectl logs -l app=azblob-reader --tail=200 || true
  kubectl -n "$NS" logs -l app=csi-azblob-node -c azblob --tail=200 || true
  exit 1
fi
kubectl logs -l app=azblob-reader --tail=50

log "k8s PVC e2e PASSED ✓"
