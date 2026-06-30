#!/usr/bin/env bash
# Quick-start demo entrypoint: turn an Azure Page Blob (served by Azurite in this
# example) into a local block device and mount it as a filesystem.
#
# It auto-detects the transport, **preferring ublk** (`/dev/ublkbN`) and falling
# back to NBD (`/dev/nbdX`) when the host kernel has no `ublk_drv`. Both paths
# need the matching kernel module loaded on the *host* and the container running
# `--privileged` with `/dev` shared (see docker-compose.yml).
set -euo pipefail

# ── Configuration (all overridable via the environment / compose) ─────────────
BLOB_URL="${UBLK_BLOB_URL:?UBLK_BLOB_URL must be set (full Azure blob URL)}"
DISK_SIZE="${DISK_SIZE:-268435456}"            # 256 MiB, multiple of 512
MOUNT_DIR="${MOUNT_DIR:-/mnt/azblob}"
DEV_ID="${UBLK_DEV_ID:-7}"                      # ublk device id -> /dev/ublkb7
NBD_DEV="${NBD_DEV:-/dev/nbd0}"
NBD_ADDR="${NBD_ADDR:-127.0.0.1:10809}"
BIN="${UBLK_AZBLOB_BIN:-/usr/local/bin/ublk-azblob}"

log() { printf '\033[1;36m[demo]\033[0m %s\n' "$*"; }

DEV_PID=""
TRANSPORT=""

cleanup() {
  log "tearing down ..."
  mountpoint -q "$MOUNT_DIR" && umount "$MOUNT_DIR" || true
  if [ "$TRANSPORT" = "nbd" ]; then
    nbd-client -d "$NBD_DEV" 2>/dev/null || true
  fi
  # SIGINT lets the device flush its write-back buffer and detach cleanly.
  [ -n "$DEV_PID" ] && kill -INT "$DEV_PID" 2>/dev/null || true
  [ -n "$DEV_PID" ] && wait "$DEV_PID" 2>/dev/null || true
}
trap cleanup INT TERM EXIT

# ── 1. Wait for the blob endpoint (Azurite) to accept connections ─────────────
endpoint_host="$(printf '%s' "$BLOB_URL" | sed -E 's#^[a-z]+://([^/:]+).*#\1#')"
endpoint_port="$(printf '%s' "$BLOB_URL" | sed -nE 's#^[a-z]+://[^/:]+:([0-9]+).*#\1#p')"
endpoint_port="${endpoint_port:-10000}"
log "waiting for blob endpoint ${endpoint_host}:${endpoint_port} ..."
for _ in $(seq 1 60); do
  if (exec 3<>"/dev/tcp/${endpoint_host}/${endpoint_port}") 2>/dev/null; then
    exec 3>&- 3<&- || true
    break
  fi
  sleep 1
done

# ── 2. Auto-detect the transport (prefer ublk, fall back to NBD) ──────────────
modprobe ublk_drv 2>/dev/null || true
modprobe nbd 2>/dev/null || true
if [ -e /dev/ublk-control ]; then
  TRANSPORT="ublk"
elif [ -e "$NBD_DEV" ]; then
  TRANSPORT="nbd"
else
  log "ERROR: host kernel exposes neither ublk_drv (/dev/ublk-control) nor NBD (${NBD_DEV})."
  log "Load one on the HOST, e.g.:  sudo modprobe ublk_drv   (preferred)   or   sudo modprobe nbd"
  exit 1
fi
log "transport: ${TRANSPORT} (ublk preferred)"

# ── 3. Provision the page blob and bring the block device up ──────────────────
# `--create` is idempotent: it never zeroes an existing blob of the same size,
# so the demo is repeatable and your data persists across runs.
if [ "$TRANSPORT" = "ublk" ]; then
  DEV="/dev/ublkb${DEV_ID}"
  log "starting ublk device ${DEV} backed by ${BLOB_URL}"
  "$BIN" run --create --size "$DISK_SIZE" --id "$DEV_ID" &
  DEV_PID=$!

  # The ublk device node only appears once the device is up.
  log "waiting for ${DEV} ..."
  for _ in $(seq 1 60); do
    [ -b "$DEV" ] && break
    kill -0 "$DEV_PID" 2>/dev/null || { log "ERROR: device process exited (see logs above)"; exit 1; }
    sleep 1
  done
  [ -b "$DEV" ] || { log "ERROR: timed out waiting for ${DEV}"; exit 1; }
else
  DEV="$NBD_DEV"
  NBD_HOST="${NBD_ADDR%:*}"
  NBD_PORT="${NBD_ADDR##*:}"
  log "starting NBD server on ${NBD_ADDR} backed by ${BLOB_URL}"
  "$BIN" run --create --size "$DISK_SIZE" --nbd "$NBD_ADDR" &
  DEV_PID=$!

  # /dev/nbdX nodes pre-exist (created by the nbd module), so we can't just look
  # for the node — wait for the server to listen, attach the client, then wait
  # until the device is actually CONNECTED (reports a non-zero size).
  log "waiting for the NBD server to listen on ${NBD_ADDR} ..."
  for _ in $(seq 1 60); do
    if (exec 3<>"/dev/tcp/${NBD_HOST}/${NBD_PORT}") 2>/dev/null; then exec 3>&- 3<&- || true; break; fi
    kill -0 "$DEV_PID" 2>/dev/null || { log "ERROR: NBD server exited (see logs above)"; exit 1; }
    sleep 1
  done
  log "attaching ${DEV} via nbd-client"
  nbd-client -d "$DEV" 2>/dev/null || true   # clear any stale connection first
  nbd-client "$NBD_HOST" "$NBD_PORT" "$DEV"
  for _ in $(seq 1 30); do
    [ "$(blockdev --getsize64 "$DEV" 2>/dev/null || echo 0)" -gt 0 ] && break
    sleep 1
  done
  [ "$(blockdev --getsize64 "$DEV" 2>/dev/null || echo 0)" -gt 0 ] || \
    { log "ERROR: ${DEV} did not connect to the NBD server"; exit 1; }
fi

# ── 4. Make a filesystem (fresh blob) and mount it ────────────────────────────
if ! blkid "$DEV" >/dev/null 2>&1; then
  log "formatting ${DEV} as ext4"
  mkfs.ext4 -q -F -L azblob "$DEV"
fi
mkdir -p "$MOUNT_DIR"
log "mounting ${DEV} at ${MOUNT_DIR}"
mount "$DEV" "$MOUNT_DIR"

# ── 5. Write a first-run marker so `ls` shows content (yours persists too) ────
if [ ! -e "$MOUNT_DIR/HELLO.txt" ]; then
  printf 'hello from an Azure Page Blob, mounted via ublk-azblob (%s)\n' "$TRANSPORT" \
    > "$MOUNT_DIR/HELLO.txt"
fi
sync

log "✓ Azure Page Blob is mounted at ${MOUNT_DIR}. Contents:"
ls -la "$MOUNT_DIR"
log "Browse it with:  docker compose exec ublk-azblob ls -la ${MOUNT_DIR}"
log "The same bytes live in the page blob ${BLOB_URL} — unmount/remount and they persist."
log "Press Ctrl-C (or 'docker compose down') to flush and detach."

# Keep the device process in the foreground so the mount stays live.
wait "$DEV_PID"
