#!/usr/bin/env bash
# mount_test.sh — filesystem-level e2e test for ublk-azblob.
#
# This exercises the *full* stack: a real ublk block device (/dev/ublkbN)
# backed by an Azure Page Blob (Azurite in CI), with an ext4 filesystem on top.
#
# Cycle:
#   1. create the page blob and start the ublk device
#   2. mkfs.ext4 + mount into a folder
#   3. write a handful of random files, record their SHA-256 checksums
#   4. send SIGUSR1 to force a backend flush
#   5. unmount and stop the device (tearing down /dev/ublkbN)
#   6. start the device again over the *same* page blob, remount
#   7. verify every file's SHA-256 matches — proving the data round-tripped
#      through Put Page / Get Page Ranges and survived the remount
#
# Requirements (CI provides these): Linux ≥6.0 with ublk_drv loaded, root /
# CAP_SYS_ADMIN, e2fsprogs (mkfs.ext4), and a running Azurite reachable at
# AZURE_STORAGE_ENDPOINT.
#
# Environment:
#   BIN                     path to the ublk-azblob binary (built --features ublk)
#   AZURE_STORAGE_ENDPOINT  Azurite blob endpoint (default: Azurite dev account)
#   BLOB_SIZE               page blob size in bytes (default: 256 MiB)
#   NUM_FILES               number of random files to write (default: 8)

set -euo pipefail

BIN="${BIN:-target/release/ublk-azblob}"
DEV_ID="${DEV_ID:-0}"
DEV="/dev/ublkb${DEV_ID}"
BLOB_SIZE="${BLOB_SIZE:-268435456}" # 256 MiB
NUM_FILES="${NUM_FILES:-8}"

# Azurite well-known development credentials.
export AZURE_STORAGE_ACCOUNT="${AZURE_STORAGE_ACCOUNT:-devstoreaccount1}"
export AZURE_STORAGE_KEY="${AZURE_STORAGE_KEY:-Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==}"
export AZURE_STORAGE_ENDPOINT="${AZURE_STORAGE_ENDPOINT:-http://127.0.0.1:10000/devstoreaccount1}"
export AZURE_STORAGE_CONTAINER="${AZURE_STORAGE_CONTAINER:-e2etest}"
export AZURE_STORAGE_BLOB="${AZURE_STORAGE_BLOB:-mounttest}"

MNT="$(mktemp -d)"
WORK="$(mktemp -d)"
CHECKSUMS="${WORK}/checksums.sha256"
DEV_PID=""

log() { echo "=== $* ==="; }

cleanup() {
    set +e
    mountpoint -q "${MNT}" && umount "${MNT}"
    if [ -n "${DEV_PID}" ] && kill -0 "${DEV_PID}" 2>/dev/null; then
        kill -INT "${DEV_PID}"
        wait "${DEV_PID}" 2>/dev/null
    fi
    rmdir "${MNT}" 2>/dev/null
    rm -rf "${WORK}" 2>/dev/null
}
trap cleanup EXIT

# Start the ublk device in the background and wait for /dev/ublkbN to appear.
# $1: extra args (e.g. "--create" on the first run to provision the blob).
start_device() {
    local extra="$1"
    log "starting ublk device ${DEV} (${extra:-reuse existing blob})"
    # shellcheck disable=SC2086
    "${BIN}" run --id "${DEV_ID}" --size "${BLOB_SIZE}" ${extra} &
    DEV_PID=$!

    for _ in $(seq 1 60); do
        if [ -b "${DEV}" ]; then
            log "device ${DEV} is up (pid ${DEV_PID})"
            return 0
        fi
        if ! kill -0 "${DEV_PID}" 2>/dev/null; then
            echo "ublk-azblob exited before ${DEV} appeared" >&2
            wait "${DEV_PID}" 2>/dev/null || true
            return 1
        fi
        sleep 1
    done
    echo "timed out waiting for ${DEV}" >&2
    return 1
}

# Stop the running device cleanly via SIGINT and wait for it to exit.
stop_device() {
    log "stopping ublk device ${DEV} (pid ${DEV_PID})"
    kill -INT "${DEV_PID}"
    wait "${DEV_PID}" 2>/dev/null || true
    DEV_PID=""
    # Give the kernel a moment to remove the device node.
    for _ in $(seq 1 30); do
        [ -b "${DEV}" ] || break
        sleep 1
    done
}

# ── Phase 1: provision device, make a filesystem, write random files ──────────

start_device "--create"

log "mkfs.ext4 on ${DEV}"
mkfs.ext4 -q -F "${DEV}"

log "mounting ${DEV} at ${MNT}"
mount "${DEV}" "${MNT}"

log "writing ${NUM_FILES} random files"
: > "${CHECKSUMS}"
for i in $(seq 1 "${NUM_FILES}"); do
    f="${MNT}/random_${i}.bin"
    # Random size between 1 and 4 MiB.
    blocks=$(( (RANDOM % 4096) + 1 ))
    dd if=/dev/urandom of="${f}" bs=1024 count="${blocks}" status=none
done
# Record checksums with paths relative to the mount point so they can be
# re-verified after a remount.
( cd "${MNT}" && sha256sum random_*.bin ) > "${CHECKSUMS}"
cat "${CHECKSUMS}"

log "sync + SIGUSR1 to force flush to the page blob"
sync
kill -USR1 "${DEV_PID}"
sleep 2

# ── Phase 2: unmount, tear the device down ────────────────────────────────────

log "unmounting ${MNT}"
umount "${MNT}"
stop_device

# ── Phase 3: remount over the same blob and verify checksums ──────────────────

start_device ""

log "remounting ${DEV} at ${MNT}"
mount "${DEV}" "${MNT}"

log "verifying checksums after remount"
( cd "${MNT}" && sha256sum -c "${CHECKSUMS}" )

log "unmounting ${MNT}"
umount "${MNT}"
stop_device

log "mount e2e PASSED ✓"
