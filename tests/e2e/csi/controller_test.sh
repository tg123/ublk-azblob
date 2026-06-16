#!/usr/bin/env bash
#
# CSI controller e2e for ublk-azblob.
#
# This test exercises the *controller* half of the CSI driver end-to-end without
# needing a kernel or the ublk_drv module: it starts the `ublk-azblob csi
# --role controller` gRPC server against a local Azurite and drives it with
# `grpcurl`, verifying the Identity and Controller RPCs that Kubernetes' external
# provisioner relies on (GetPluginInfo, ControllerGetCapabilities, CreateVolume,
# DeleteVolume — including DeleteVolume idempotency).
#
# Because it needs no block device it runs anywhere (CI, laptop) once Azurite is
# reachable.  The companion `tests/e2e/k8s/run.sh` covers the node/mount path on
# a real ublk-capable host.
#
# Environment:
#   BIN                     path to the ublk-azblob binary (default: built via cargo)
#   AZURE_STORAGE_ENDPOINT  Azurite blob endpoint
#                           (default: http://127.0.0.1:10000/devstoreaccount1)
#   AZURE_STORAGE_ACCOUNT   storage account   (default: devstoreaccount1)
#   AZURE_STORAGE_KEY       shared key        (default: Azurite well-known key)
#   AZURE_STORAGE_CONTAINER blob container    (default: pvc)
#
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$here/../../.." && pwd)"
proto_dir="$repo_root/ublk-azblob/proto/csi"

ACCOUNT="${AZURE_STORAGE_ACCOUNT:-devstoreaccount1}"
KEY="${AZURE_STORAGE_KEY:-Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==}"
ENDPOINT="${AZURE_STORAGE_ENDPOINT:-http://127.0.0.1:10000/devstoreaccount1}"
CONTAINER="${AZURE_STORAGE_CONTAINER:-pvc}"

log() { echo "=== $* ==="; }

if ! command -v grpcurl >/dev/null 2>&1; then
  echo "skipping controller e2e: grpcurl not found on PATH" >&2
  exit 0
fi

BIN="${BIN:-}"
if [[ -z "$BIN" ]]; then
  log "building ublk-azblob (--features csi)"
  ( cd "$repo_root" && cargo build --release --features csi -p ublk-azblob )
  BIN="$repo_root/ublk-azblob/target/release/ublk-azblob"
  [[ -x "$BIN" ]] || BIN="$repo_root/target/release/ublk-azblob"
fi
[[ -x "$BIN" ]] || { echo "binary not found: $BIN" >&2; exit 1; }

sock="$(mktemp -u /tmp/csi-ctrl.XXXXXX.sock)"
ctrl_pid=""

cleanup() {
  if [[ -n "$ctrl_pid" ]]; then
    kill "$ctrl_pid" >/dev/null 2>&1 || true
    wait "$ctrl_pid" 2>/dev/null || true
  fi
  rm -f "$sock"
}
trap cleanup EXIT

log "starting CSI controller on unix://$sock"
AZURE_STORAGE_ACCOUNT="$ACCOUNT" \
AZURE_STORAGE_KEY="$KEY" \
AZURE_STORAGE_ENDPOINT="$ENDPOINT" \
AZURE_STORAGE_CONTAINER="$CONTAINER" \
  "$BIN" csi --role controller --csi-endpoint "unix://$sock" &
ctrl_pid=$!

# Wait for the controller to create and bind the unix socket.
for _ in $(seq 1 30); do
  [[ -S "$sock" ]] && break
  if ! kill -0 "$ctrl_pid" 2>/dev/null; then
    echo "controller exited before binding the socket" >&2
    exit 1
  fi
  sleep 1
done
[[ -S "$sock" ]] || { echo "timed out waiting for $sock" >&2; exit 1; }

gc() { local method="$1"; shift; grpcurl -plaintext -unix -import-path "$proto_dir" -proto csi.proto "$@" "$sock" "$method"; }

vol_name="pvc-e2e-$$-$RANDOM"

log "Identity/GetPluginInfo"
info="$(gc csi.v1.Identity/GetPluginInfo)"
echo "$info"
echo "$info" | grep -q 'azblob.ublk.csi.tg123.github.io' \
  || { echo "GetPluginInfo: unexpected driver name" >&2; exit 1; }

log "Controller/ControllerGetCapabilities"
caps="$(gc csi.v1.Controller/ControllerGetCapabilities)"
echo "$caps"
echo "$caps" | grep -q 'CREATE_DELETE_VOLUME' \
  || { echo "missing CREATE_DELETE_VOLUME capability" >&2; exit 1; }

log "Controller/CreateVolume ($vol_name)"
create="$(gc csi.v1.Controller/CreateVolume -d "{\"name\":\"$vol_name\",\"capacity_range\":{\"required_bytes\":1048576},\"volume_capabilities\":[{\"mount\":{\"fs_type\":\"ext4\"},\"access_mode\":{\"mode\":1}}]}")"
echo "$create"
echo "$create" | grep -q "\"$CONTAINER/$vol_name\"" \
  || { echo "CreateVolume: unexpected volume id" >&2; exit 1; }

log "Controller/CreateVolume idempotency (same request)"
gc csi.v1.Controller/CreateVolume -d "{\"name\":\"$vol_name\",\"capacity_range\":{\"required_bytes\":1048576},\"volume_capabilities\":[{\"mount\":{\"fs_type\":\"ext4\"},\"access_mode\":{\"mode\":1}}]}" >/dev/null

log "Controller/DeleteVolume ($CONTAINER/$vol_name)"
gc csi.v1.Controller/DeleteVolume -d "{\"volume_id\":\"$CONTAINER/$vol_name\"}"

log "Controller/DeleteVolume idempotency (already deleted)"
gc csi.v1.Controller/DeleteVolume -d "{\"volume_id\":\"$CONTAINER/$vol_name\"}"

log "controller e2e PASSED ✓"
