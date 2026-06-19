#!/usr/bin/env bash
#
# import-folder.sh — load a local folder into an Azure Page Blob through the
# ublk-azblob block device, then print the resulting blob URL.
#
# Unlike the `ublk-azblob-import` tool (which packs a folder into a single tar
# image), this script drives the *full block-device* path end to end:
#
#   1. start the ublk device over the page blob   (ublk-azblob run --create)
#   2. mkfs   — create a filesystem on /dev/ublkbN (default ext4)
#   3. mount  — mount it on a temporary mount point
#   4. copy   — copy the contents of the source folder into the mount
#   5. flush  — sync + SIGUSR1 to drain pending writes to the page blob
#   6. unmount + stop the device cleanly
#   7. output — print the blob URL on stdout
#
# The data lands in the blob as a real filesystem image, so it can later be
# served read/write with `ublk-azblob run` (or NBD) and mounted directly.
#
# Requirements: Linux >= 6.0 with `ublk_drv` loaded (`sudo modprobe ublk_drv`),
# root / CAP_SYS_ADMIN, and `mkfs.<fs>` (e.g. e2fsprogs for ext4).
#
# Example (Azurite / local dev):
#
#   sudo AZURE_STORAGE_KEY="<dev-key>" scripts/import-folder.sh \
#     --endpoint http://127.0.0.1:10000/devstoreaccount1 \
#     --account devstoreaccount1 \
#     --container mycontainer \
#     --blob folder.img \
#     --src ./mydir \
#     --size 268435456
#
set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
ACCOUNT="${AZURE_STORAGE_ACCOUNT:-}"
CONTAINER="${AZURE_STORAGE_CONTAINER:-}"
BLOB="${AZURE_STORAGE_BLOB:-}"
ENDPOINT="${AZURE_STORAGE_ENDPOINT:-}"
ACCOUNT_KEY="${AZURE_STORAGE_KEY:-}"
SRC=""
SIZE=""
FS="ext4"
MOUNT_POINT=""
BIN="${UBLK_AZBLOB_BIN:-}"
EXTRA_ARGS=()

usage() {
    cat <<'EOF'
import-folder.sh — load a local folder into an Azure Page Blob through the
ublk-azblob block device, then print the resulting blob URL.

It drives the full block-device path: start the ublk device, mkfs, mount, copy
the folder in, sync + SIGUSR1 flush, unmount, stop, then print the blob URL.
(Unlike `ublk-azblob-import`, which packs a folder into a single tar image.)

Requirements: Linux >= 6.0 with `ublk_drv` loaded (`sudo modprobe ublk_drv`),
root / CAP_SYS_ADMIN, and `mkfs.<fs>` (e.g. e2fsprogs for ext4).

Usage:
  sudo scripts/import-folder.sh --src DIR --size BYTES [options]

Options:
  --account NAME        Azure Storage account name      (env AZURE_STORAGE_ACCOUNT)
  --container NAME      Blob container name             (env AZURE_STORAGE_CONTAINER)
  --blob NAME          Page blob name (the image)      (env AZURE_STORAGE_BLOB)
  --endpoint URL       Service endpoint                (env AZURE_STORAGE_ENDPOINT)
  --account-key KEY    Shared-key auth (Azurite/dev)   (env AZURE_STORAGE_KEY)
  --src DIR            Local folder to copy in (required)
  --size BYTES         Blob/device size in bytes (multiple of 512, required)
  --fs TYPE            Filesystem to create (default: ext4)
  --mount-point DIR    Mount point (default: a fresh mktemp -d)
  --bin PATH           ublk-azblob binary (env UBLK_AZBLOB_BIN; else autodetect)
  --                   Everything after is passed through to `ublk-azblob run`
  -h, --help           Show this help

Example (Azurite / local dev):
  sudo AZURE_STORAGE_KEY="<dev-key>" scripts/import-folder.sh \
    --endpoint http://127.0.0.1:10000/devstoreaccount1 \
    --account devstoreaccount1 --container mycontainer --blob folder.img \
    --src ./mydir --size 268435456

Managed-identity auth (production) is selected by passing the matching flags
after `--`, e.g. `-- --msi`. Without `--account-key`/`--msi*` the device will
refuse to start.
EOF
}

# ── Arg parsing ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --account)      ACCOUNT="$2"; shift 2 ;;
        --container)    CONTAINER="$2"; shift 2 ;;
        --blob)         BLOB="$2"; shift 2 ;;
        --endpoint)     ENDPOINT="$2"; shift 2 ;;
        --account-key)  ACCOUNT_KEY="$2"; shift 2 ;;
        --src)          SRC="$2"; shift 2 ;;
        --size)         SIZE="$2"; shift 2 ;;
        --fs)           FS="$2"; shift 2 ;;
        --mount-point)  MOUNT_POINT="$2"; shift 2 ;;
        --bin)          BIN="$2"; shift 2 ;;
        --)             shift; EXTRA_ARGS+=("$@"); break ;;
        -h|--help)      usage; exit 0 ;;
        *) echo "error: unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
done

# ── Validation ────────────────────────────────────────────────────────────────
die() { echo "error: $*" >&2; exit 1; }

[[ -n "$ACCOUNT"   ]] || die "missing --account (or AZURE_STORAGE_ACCOUNT)"
[[ -n "$CONTAINER" ]] || die "missing --container (or AZURE_STORAGE_CONTAINER)"
[[ -n "$BLOB"      ]] || die "missing --blob (or AZURE_STORAGE_BLOB)"
[[ -n "$SRC"       ]] || die "missing --src (folder to copy in)"
[[ -d "$SRC"       ]] || die "--src is not a directory: $SRC"
[[ -n "$SIZE"      ]] || die "missing --size (bytes, a multiple of 512)"
[[ "$SIZE" =~ ^[0-9]+$ ]] || die "--size must be an integer number of bytes: $SIZE"
(( SIZE % 512 == 0 )) || die "--size must be a multiple of 512: $SIZE"

if [[ "$(id -u)" -ne 0 ]]; then
    die "must run as root (creating a ublk device needs CAP_SYS_ADMIN)"
fi
if [[ ! -e /dev/ublk-control ]]; then
    die "/dev/ublk-control missing — load the kernel module: sudo modprobe ublk_drv"
fi
command -v "mkfs.${FS}" >/dev/null 2>&1 || die "mkfs.${FS} not found in PATH"

# Locate the ublk-azblob binary if not given explicitly.
if [[ -z "$BIN" ]]; then
    repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
    for cand in \
        "$repo_root/target/release/ublk-azblob" \
        "$repo_root/target/debug/ublk-azblob"; do
        if [[ -x "$cand" ]]; then BIN="$cand"; break; fi
    done
fi
[[ -n "$BIN" ]] || BIN="$(command -v ublk-azblob || true)"
[[ -n "$BIN" && -x "$BIN" ]] || die "ublk-azblob binary not found; build it (cargo build --release --features ublk) or pass --bin"

export AZURE_STORAGE_ACCOUNT="$ACCOUNT"
export AZURE_STORAGE_CONTAINER="$CONTAINER"
export AZURE_STORAGE_BLOB="$BLOB"
[[ -n "$ENDPOINT"    ]] && export AZURE_STORAGE_ENDPOINT="$ENDPOINT"
[[ -n "$ACCOUNT_KEY" ]] && export AZURE_STORAGE_KEY="$ACCOUNT_KEY"

# ── Cleanup / teardown ────────────────────────────────────────────────────────
CHILD_PID=""
MOUNTED=""
OWN_MOUNT_POINT=""

log() { echo "=== $* ===" >&2; }

cleanup() {
    set +e
    if [[ -n "$MOUNTED" ]]; then
        sync
        umount "$MOUNT_POINT" 2>/dev/null && MOUNTED=""
    fi
    if [[ -n "$CHILD_PID" ]] && kill -0 "$CHILD_PID" 2>/dev/null; then
        kill -INT "$CHILD_PID" 2>/dev/null
        wait "$CHILD_PID" 2>/dev/null
        CHILD_PID=""
    fi
    [[ -n "$OWN_MOUNT_POINT" && -d "$MOUNT_POINT" ]] && rmdir "$MOUNT_POINT" 2>/dev/null
}
trap cleanup EXIT INT TERM

# ── 1. Start the device ───────────────────────────────────────────────────────
# Snapshot existing devices so we can identify the one we create (the kernel
# auto-allocates the next free /dev/ublkbN with --id -1).
existing=" "
for d in /dev/ublkb*; do [[ -e "$d" ]] && existing+="$d "; done

log "starting ublk device over ${CONTAINER}/${BLOB} (size ${SIZE})"
"$BIN" run --create --id -1 --size "$SIZE" "${EXTRA_ARGS[@]}" &
CHILD_PID=$!

# Wait for a new /dev/ublkbN to appear (or the child to die).
DEV=""
deadline=$(( $(date +%s) + 60 ))
while [[ -z "$DEV" ]]; do
    if ! kill -0 "$CHILD_PID" 2>/dev/null; then
        wait "$CHILD_PID"; die "ublk-azblob exited before the device appeared"
    fi
    for d in /dev/ublkb*; do
        [[ -e "$d" ]] || continue
        if [[ "$existing" != *" $d "* ]]; then DEV="$d"; break; fi
    done
    [[ -n "$DEV" ]] && break
    (( $(date +%s) < deadline )) || die "timed out waiting for a new /dev/ublkbN"
    sleep 1
done
log "device $DEV is up (pid $CHILD_PID)"

# ── 2. mkfs ───────────────────────────────────────────────────────────────────
log "mkfs.${FS} on $DEV"
if [[ "$FS" == ext* ]]; then
    "mkfs.${FS}" -q -F -E nodiscard "$DEV"
else
    "mkfs.${FS}" "$DEV"
fi

# ── 3. mount ──────────────────────────────────────────────────────────────────
if [[ -z "$MOUNT_POINT" ]]; then
    MOUNT_POINT="$(mktemp -d)"
    OWN_MOUNT_POINT=1
else
    mkdir -p "$MOUNT_POINT"
fi
log "mounting $DEV at $MOUNT_POINT"
mount "$DEV" "$MOUNT_POINT"
MOUNTED=1

# ── 4. copy ───────────────────────────────────────────────────────────────────
log "copying contents of $SRC into $MOUNT_POINT"
cp -a "$SRC"/. "$MOUNT_POINT"/

# ── 5. flush ──────────────────────────────────────────────────────────────────
log "sync + SIGUSR1 to flush pending writes to the page blob"
sync
kill -USR1 "$CHILD_PID"
sleep 2

# ── 6. unmount + stop ─────────────────────────────────────────────────────────
log "unmounting $MOUNT_POINT"
umount "$MOUNT_POINT"; MOUNTED=""
log "stopping ublk device $DEV"
kill -INT "$CHILD_PID"
wait "$CHILD_PID"; CHILD_PID=""

# ── 7. output URL ─────────────────────────────────────────────────────────────
if [[ -n "$ENDPOINT" ]]; then
    URL="${ENDPOINT%/}/${CONTAINER}/${BLOB}"
else
    URL="https://${ACCOUNT}.blob.core.windows.net/${CONTAINER}/${BLOB}"
fi
log "done — blob URL:"
echo "$URL"
