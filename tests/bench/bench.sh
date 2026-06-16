#!/usr/bin/env bash
#
# I/O benchmark: ublk-azblob block device vs. a raw local disk.
#
# This drives the *full* stack the same way the mount e2e does — a real
# `/dev/ublkbN` block device backed by an Azure Page Blob (Azurite in CI) — and
# benchmarks it with `fio`.  For a fair, apples-to-apples baseline it also
# benchmarks a raw local-disk block device (a loopback device backed by a file
# on the container's local filesystem) with the *identical* fio jobs, then
# prints a side-by-side comparison.
#
# Both targets are benchmarked as *raw block devices* (no filesystem), so the
# numbers reflect the block layer + backend, not ext4.
#
# Requirements (the docker-compose `runner` service provides these):
#   * Linux >=6.0 with `ublk_drv` loaded on the host, root / CAP_SYS_ADMIN
#   * `fio`, `jq`, `losetup` (util-linux)
#   * a running Azurite reachable at $AZURE_STORAGE_ENDPOINT
#
# Usage (after `sudo modprobe ublk_drv` on the host):
#   docker compose -f tests/bench/docker-compose.yml up --build \
#     --abort-on-container-exit --exit-code-from runner
#
# Tunables (all optional, shown with defaults):
#   DEV_SIZE_MIB=512   # size of the ublk blob and the local backing file
#   FIO_SIZE=256M      # how much of each device fio touches
#   FIO_BS=4k          # block size per I/O
#   FIO_IODEPTH=16     # queue depth
#   FIO_RUNTIME=15     # seconds per workload (time-based)
#   FIO_DIRECT=1       # O_DIRECT (bypass page cache for a fair comparison)
#   RESULT_FILE=...    # where to write the markdown result table
set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
DEV_ID="${DEV_ID:-0}"
DEV="/dev/ublkb${DEV_ID}"
DEV_SIZE_MIB="${DEV_SIZE_MIB:-512}"
DEV_SIZE_BYTES=$((DEV_SIZE_MIB * 1024 * 1024))

FIO_SIZE="${FIO_SIZE:-256M}"
FIO_BS="${FIO_BS:-4k}"
FIO_IODEPTH="${FIO_IODEPTH:-16}"
FIO_RUNTIME="${FIO_RUNTIME:-15}"
FIO_DIRECT="${FIO_DIRECT:-1}"

# Azure / Azurite connection (mirrors tests/mount_e2e.rs defaults).
export AZURE_STORAGE_ACCOUNT="${AZURE_STORAGE_ACCOUNT:-devstoreaccount1}"
export AZURE_STORAGE_KEY="${AZURE_STORAGE_KEY:-Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==}"
export AZURE_STORAGE_ENDPOINT="${AZURE_STORAGE_ENDPOINT:-http://127.0.0.1:10000/devstoreaccount1}"
export AZURE_STORAGE_CONTAINER="${AZURE_STORAGE_CONTAINER:-benchtest}"
export AZURE_STORAGE_BLOB="${AZURE_STORAGE_BLOB:-benchblob}"

WORKDIR="${WORKDIR:-/workspace}"
BIN="${BIN:-$WORKDIR/target/release/ublk-azblob}"
RESULT_FILE="${RESULT_FILE:-$WORKDIR/bench-results.md}"
TMPDIR_BENCH="$(mktemp -d /tmp/ublk-bench.XXXXXX)"
LOCAL_IMG="$TMPDIR_BENCH/local-disk.img"

UBLK_PID=""
LOOP_DEV=""

log() { echo "=== $* ===" >&2; }

# ── Teardown ──────────────────────────────────────────────────────────────────
cleanup() {
    set +e
    log "cleanup"
    if [[ -n "$LOOP_DEV" ]]; then
        losetup -d "$LOOP_DEV" 2>/dev/null
    fi
    if [[ -n "$UBLK_PID" ]] && kill -0 "$UBLK_PID" 2>/dev/null; then
        log "stopping ublk device (pid $UBLK_PID)"
        kill -INT "$UBLK_PID" 2>/dev/null
        # Wait up to 30s for a clean shutdown / device-node removal.
        for _ in $(seq 1 30); do
            kill -0 "$UBLK_PID" 2>/dev/null || break
            sleep 1
        done
        kill -9 "$UBLK_PID" 2>/dev/null
    fi
    rm -rf "$TMPDIR_BENCH"
}
trap cleanup EXIT

# ── Bring up the ublk-azblob device ───────────────────────────────────────────
start_ublk() {
    log "starting ublk-azblob device $DEV (size ${DEV_SIZE_MIB} MiB)"
    "$BIN" run --id "$DEV_ID" --size "$DEV_SIZE_BYTES" --create &
    UBLK_PID=$!

    for _ in $(seq 1 60); do
        if [[ -b "$DEV" ]]; then
            log "device $DEV is up (pid $UBLK_PID)"
            return 0
        fi
        if ! kill -0 "$UBLK_PID" 2>/dev/null; then
            echo "ublk-azblob exited before $DEV appeared" >&2
            exit 1
        fi
        sleep 1
    done
    echo "timed out waiting for $DEV" >&2
    exit 1
}

# ── Bring up the raw local-disk reference (loopback device) ───────────────────
start_local_disk() {
    log "creating local-disk reference image ($LOCAL_IMG, ${DEV_SIZE_MIB} MiB)"
    # Sparse file so we don't actually write 512 MiB up front.
    truncate -s "$DEV_SIZE_BYTES" "$LOCAL_IMG"
    LOOP_DEV="$(losetup --find --show "$LOCAL_IMG")"
    log "local disk is $LOOP_DEV"
}

# ── Run one fio workload against one device, emit a TSV table row ──────────────
# Args: <target-label> <device> <fio-rw> <human-workload-name>
run_fio() {
    local label="$1" dev="$2" rw="$3" name="$4"
    local json
    json="$TMPDIR_BENCH/fio-${label}-${rw}.json"

    log "fio: $label / $name ($rw, bs=$FIO_BS, iodepth=$FIO_IODEPTH, ${FIO_RUNTIME}s)"
    fio --name="${label}-${rw}" \
        --filename="$dev" \
        --rw="$rw" \
        --bs="$FIO_BS" \
        --size="$FIO_SIZE" \
        --direct="$FIO_DIRECT" \
        --ioengine=libaio \
        --iodepth="$FIO_IODEPTH" \
        --numjobs=1 \
        --runtime="$FIO_RUNTIME" \
        --time_based \
        --group_reporting \
        --output-format=json \
        --output="$json" >/dev/null

    # fio reports read stats for read workloads and write stats for write
    # workloads; pick whichever side did the I/O.
    local side
    case "$rw" in
        *read*) side="read" ;;
        *) side="write" ;;
    esac

    # bw is KiB/s -> MiB/s; lat_ns.mean is nanoseconds -> microseconds.
    jq -r --arg label "$label" --arg name "$name" --arg side "$side" '
        .jobs[0][$side] as $s
        | [$name, $label,
           ($s.bw / 1024          | . * 100 | round / 100),
           ($s.iops               | round),
           ($s.lat_ns.mean / 1000 | . * 100 | round / 100)]
        | @tsv' "$json"
}

# ── Main ──────────────────────────────────────────────────────────────────────
log "ublk-azblob I/O benchmark (device vs. raw local disk)"
[[ -x "$BIN" ]] || { echo "binary not found/executable: $BIN" >&2; exit 1; }

start_ublk
start_local_disk

# Workloads: (fio rw mode, human label).  Sequential then random, write then read.
WORKLOADS=(
    "write:sequential write"
    "read:sequential read"
    "randwrite:random write"
    "randread:random read"
)

declare -a ROWS
for entry in "${WORKLOADS[@]}"; do
    rw="${entry%%:*}"
    name="${entry#*:}"
    ROWS+=("$(run_fio ublk-azblob "$DEV" "$rw" "$name")")
    ROWS+=("$(run_fio local-disk "$LOOP_DEV" "$rw" "$name")")
done

# ── Emit a comparison table (stdout + markdown file) ──────────────────────────
{
    echo "# ublk-azblob I/O benchmark"
    echo
    echo "Raw block-device benchmark (no filesystem) with \`fio\`."
    echo
    echo "| Setting | Value |"
    echo "|---------|-------|"
    echo "| Block size | $FIO_BS |"
    echo "| Queue depth | $FIO_IODEPTH |"
    echo "| Direct I/O | $FIO_DIRECT |"
    echo "| Runtime/workload | ${FIO_RUNTIME}s |"
    echo "| fio size | $FIO_SIZE |"
    echo "| Device size | ${DEV_SIZE_MIB} MiB |"
    echo
    echo "| Workload | Target | Throughput (MiB/s) | IOPS | Mean latency (us) |"
    echo "|----------|--------|-------------------:|-----:|------------------:|"
    for row in "${ROWS[@]}"; do
        IFS=$'\t' read -r name label bw iops lat <<<"$row"
        printf '| %s | %s | %s | %s | %s |\n' "$name" "$label" "$bw" "$iops" "$lat"
    done
} | tee "$RESULT_FILE"

log "benchmark complete — results written to $RESULT_FILE"
