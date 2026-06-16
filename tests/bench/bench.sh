#!/usr/bin/env bash
#
# I/O benchmark: ublk-azblob block device vs. a raw local disk.
#
# This drives the *full* stack the same way the mount e2e does — a real
# `/dev/ublkbN` block device backed by an Azure Page Blob (Azurite in CI) — and
# benchmarks it with `fio`.  For a fair, apples-to-apples baseline it also
# benchmarks a raw local-disk block device (a loopback device backed by a file
# on the container's local filesystem) with the *identical* fio jobs, then
# prints a side-by-side comparison.  Each ublk-azblob result is also reported as
# a percentage of the raw-local-disk baseline (higher % is closer to local disk).
#
# Both targets are benchmarked as *raw block devices* (no filesystem), so the
# numbers reflect the block layer + backend, not ext4.
#
# The benchmark is organised into phases (see the fio matrix below):
#   Phase 1 — Raw block performance: the four base patterns (seq/rand read/write)
#             plus sweeps over block size, queue depth, and read/write mix.
#   Phase 2 — Cache behaviour: cold-cache vs. warm-cache read (buffered I/O) so
#             the backend/page-cache re-read speed-up is visible.
#   Phase 4 — Scalability: the same random-read workload at increasing thread
#             (fio numjobs) counts.
# (Phase 3 — backend Azure Blob latency/throughput — is covered separately by the
#  `ublk-azblob bench` subcommand, which reports backend GET/PUT/flush latency
#  percentiles directly over the BlobBackend trait.)
#
# Requirements (the docker-compose `runner` service provides these):
#   * Linux >=6.0 with `ublk_drv` loaded on the host, root / CAP_SYS_ADMIN
#   * `fio`, `jq`, `awk`, `losetup` (util-linux)
#   * a running Azurite reachable at $AZURE_STORAGE_ENDPOINT
#
# Usage (after `sudo modprobe ublk_drv` on the host):
#   docker compose -f tests/bench/docker-compose.yml up --build \
#     --abort-on-container-exit --exit-code-from runner
#
# Tunables (all optional, shown with defaults):
#   DEV_SIZE_MIB=512          # size of the ublk blob and the local backing file
#   FIO_SIZE=256M             # how much of each device fio touches
#   FIO_BS=4k                 # base block size per I/O
#   FIO_IODEPTH=16            # base queue depth
#   FIO_NUMJOBS=1             # base thread (job) count
#   FIO_RUNTIME=10            # seconds per workload (time-based)
#   FIO_DIRECT=1             # O_DIRECT (bypass page cache for a fair comparison)
#   FIO_BS_LIST="4k 16k 64k 256k 1M"   # block-size sweep (Phase 1)
#   FIO_IODEPTH_LIST="1 4 16 64 128"   # queue-depth sweep (Phase 1)
#   FIO_RWMIX_LIST="100 70 50"         # read-percentage sweep (Phase 1, randrw)
#   FIO_NUMJOBS_LIST="1 4 16 64"       # thread sweep (Phase 4)
#   RESULT_FILE=...           # where to write the markdown result table
set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
DEV_ID="${DEV_ID:-0}"
DEV="/dev/ublkb${DEV_ID}"
DEV_SIZE_MIB="${DEV_SIZE_MIB:-512}"
DEV_SIZE_BYTES=$((DEV_SIZE_MIB * 1024 * 1024))

FIO_SIZE="${FIO_SIZE:-256M}"
FIO_BS="${FIO_BS:-4k}"
FIO_IODEPTH="${FIO_IODEPTH:-16}"
FIO_NUMJOBS="${FIO_NUMJOBS:-1}"
FIO_RUNTIME="${FIO_RUNTIME:-10}"
FIO_DIRECT="${FIO_DIRECT:-1}"

# Sweep lists (Phase 1 / Phase 4).  Each sweep varies a single dimension around
# the base values above to keep the matrix bounded.
FIO_BS_LIST="${FIO_BS_LIST:-4k 16k 64k 256k 1M}"
FIO_IODEPTH_LIST="${FIO_IODEPTH_LIST:-1 4 16 64 128}"
FIO_RWMIX_LIST="${FIO_RWMIX_LIST:-100 70 50}"
FIO_NUMJOBS_LIST="${FIO_NUMJOBS_LIST:-1 4 16 64}"

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

# ── Run one fio workload against one device, emit "bw_mibs<TAB>iops<TAB>lat_us" ─
# Args: <target-label> <device> <fio-rw> <bs> <iodepth> <numjobs> <rwmixread|""> <direct>
run_fio() {
    local label="$1" dev="$2" rw="$3" bs="$4" iodepth="$5" numjobs="$6" rwmixread="${7:-}" direct="${8:-$FIO_DIRECT}"
    local json
    json="$TMPDIR_BENCH/fio-${label}-${rw}-${bs}-qd${iodepth}-j${numjobs}-mix${rwmixread:-na}-d${direct}.json"

    local -a mixargs=()
    if [[ -n "$rwmixread" ]]; then
        mixargs+=(--rwmixread="$rwmixread")
    fi

    log "fio: $label / $rw (bs=$bs, qd=$iodepth, jobs=$numjobs, mix=${rwmixread:-pure}, direct=$direct, ${FIO_RUNTIME}s)"
    fio --name="${label}-${rw}" \
        --filename="$dev" \
        --rw="$rw" \
        --bs="$bs" \
        --size="$FIO_SIZE" \
        --direct="$direct" \
        --ioengine=libaio \
        --iodepth="$iodepth" \
        --numjobs="$numjobs" \
        --runtime="$FIO_RUNTIME" \
        --time_based \
        --group_reporting \
        --output-format=json \
        --output="$json" \
        "${mixargs[@]}" >/dev/null

    # Sum read + write so mixed (randrw) workloads count both directions; for a
    # pure workload one side is zero.  bw is KiB/s -> MiB/s; lat_ns.mean is
    # nanoseconds -> microseconds (iops-weighted across read/write).
    jq -r '
        .jobs[0] as $j
        | ($j.read.bw + $j.write.bw)       as $bw
        | ($j.read.iops + $j.write.iops)   as $iops
        | (($j.read.iops * $j.read.lat_ns.mean) + ($j.write.iops * $j.write.lat_ns.mean)) as $latsum
        | (if $iops > 0 then $latsum / $iops else 0 end) as $lat
        | [ ($bw / 1024 | . * 100 | round / 100),
            ($iops | round),
            ($lat / 1000 | . * 100 | round / 100) ]
        | @tsv' "$json"
}

# Percentage of the local-disk baseline: 100 * ublk / local (one decimal).
pct_of_baseline() {
    awk -v a="$1" -v b="$2" 'BEGIN { if (b > 0) printf "%.1f", a / b * 100; else printf "n/a" }'
}

# ── Run one comparison case against both targets, append two result rows ───────
# Args: <phase> <workload-name> <fio-rw> <bs> <iodepth> <numjobs> <rwmixread|""> <direct>
run_case() {
    local phase="$1" name="$2" rw="$3" bs="$4" iodepth="$5" numjobs="$6" rwmixread="${7:-}" direct="${8:-$FIO_DIRECT}"
    local u l
    u="$(run_fio ublk-azblob "$DEV" "$rw" "$bs" "$iodepth" "$numjobs" "$rwmixread" "$direct")"
    l="$(run_fio local-disk "$LOOP_DEV" "$rw" "$bs" "$iodepth" "$numjobs" "$rwmixread" "$direct")"

    local ubw uiops ulat lbw liops llat
    IFS=$'\t' read -r ubw uiops ulat <<<"$u"
    IFS=$'\t' read -r lbw liops llat <<<"$l"

    local upct
    upct="$(pct_of_baseline "$ubw" "$lbw")"

    # ublk-azblob first (with % vs. local), then the local-disk baseline row.
    ROWS+=("$(printf '%s\t%s\t%s\t%s\t%s\tublk-azblob\t%s\t%s\t%s\t%s%%' \
        "$phase" "$name" "$bs" "$iodepth" "$numjobs" "$ubw" "$uiops" "$ulat" "$upct")")
    ROWS+=("$(printf '%s\t%s\t%s\t%s\t%s\tlocal-disk\t%s\t%s\t%s\tbaseline' \
        "$phase" "$name" "$bs" "$iodepth" "$numjobs" "$lbw" "$liops" "$llat")")
}

# ── Main ──────────────────────────────────────────────────────────────────────
log "ublk-azblob I/O benchmark (device vs. raw local disk)"
[[ -x "$BIN" ]] || { echo "binary not found/executable: $BIN" >&2; exit 1; }

start_ublk
start_local_disk

declare -a ROWS

# ── Phase 1: Raw block performance ────────────────────────────────────────────
# Base patterns (write first so the blob has data for the reads that follow).
run_case "1 raw" "sequential write" write     "$FIO_BS" "$FIO_IODEPTH" "$FIO_NUMJOBS"
run_case "1 raw" "sequential read"  read      "$FIO_BS" "$FIO_IODEPTH" "$FIO_NUMJOBS"
run_case "1 raw" "random write"     randwrite "$FIO_BS" "$FIO_IODEPTH" "$FIO_NUMJOBS"
run_case "1 raw" "random read"      randread  "$FIO_BS" "$FIO_IODEPTH" "$FIO_NUMJOBS"

# Block-size sweep (random read, base queue depth).
for bs in $FIO_BS_LIST; do
    run_case "1 bs" "randread bs=$bs" randread "$bs" "$FIO_IODEPTH" "$FIO_NUMJOBS"
done

# Queue-depth sweep (random read, base block size).
for qd in $FIO_IODEPTH_LIST; do
    run_case "1 qd" "randread qd=$qd" randread "$FIO_BS" "$qd" "$FIO_NUMJOBS"
done

# Read/write-mix sweep (random rw, base block size / queue depth).
for mix in $FIO_RWMIX_LIST; do
    run_case "1 mix" "randrw ${mix}/$((100 - mix))" randrw "$FIO_BS" "$FIO_IODEPTH" "$FIO_NUMJOBS" "$mix"
done

# ── Phase 2: Cache behaviour (buffered I/O so the cache is exercised) ──────────
# Cold cache = first read after writes; warm cache = immediate re-read.  Both use
# buffered I/O (direct=0); a warm/cold ratio > 1 shows the read cache working.
{
    cold="$(run_fio ublk-azblob "$DEV" read "$FIO_BS" "$FIO_IODEPTH" "$FIO_NUMJOBS" "" 0)"
    warm="$(run_fio ublk-azblob "$DEV" read "$FIO_BS" "$FIO_IODEPTH" "$FIO_NUMJOBS" "" 0)"
    IFS=$'\t' read -r cbw ciops clat <<<"$cold"
    IFS=$'\t' read -r wbw wiops wlat <<<"$warm"
    wpct="$(pct_of_baseline "$wbw" "$cbw")"
    ROWS+=("$(printf '%s\t%s\t%s\t%s\t%s\tublk-azblob\t%s\t%s\t%s\tbaseline' \
        "2 cache" "cold-cache read" "$FIO_BS" "$FIO_IODEPTH" "$FIO_NUMJOBS" "$cbw" "$ciops" "$clat")")
    ROWS+=("$(printf '%s\t%s\t%s\t%s\t%s\tublk-azblob\t%s\t%s\t%s\t%s%%' \
        "2 cache" "warm-cache read" "$FIO_BS" "$FIO_IODEPTH" "$FIO_NUMJOBS" "$wbw" "$wiops" "$wlat" "$wpct")")
}

# ── Phase 4: Scalability (random read at increasing thread counts) ────────────
for jobs in $FIO_NUMJOBS_LIST; do
    run_case "4 scale" "randread jobs=$jobs" randread "$FIO_BS" "$FIO_IODEPTH" "$jobs"
done

# ── Emit a comparison table (stdout + markdown file) ──────────────────────────
{
    echo "# ublk-azblob I/O benchmark"
    echo
    echo "Raw block-device benchmark (no filesystem) with \`fio\`, comparing the"
    echo "ublk-azblob device against a raw local disk.  The \`vs local\` column shows"
    echo "ublk-azblob throughput as a percentage of the raw-local-disk baseline."
    echo
    echo "| Setting | Value |"
    echo "|---------|-------|"
    echo "| Base block size | $FIO_BS |"
    echo "| Base queue depth | $FIO_IODEPTH |"
    echo "| Base threads (numjobs) | $FIO_NUMJOBS |"
    echo "| Direct I/O | $FIO_DIRECT |"
    echo "| Runtime/workload | ${FIO_RUNTIME}s |"
    echo "| fio size | $FIO_SIZE |"
    echo "| Device size | ${DEV_SIZE_MIB} MiB |"
    echo "| Block-size sweep | $FIO_BS_LIST |"
    echo "| Queue-depth sweep | $FIO_IODEPTH_LIST |"
    echo "| Read/write-mix sweep | $FIO_RWMIX_LIST |"
    echo "| Thread sweep | $FIO_NUMJOBS_LIST |"
    echo
    echo "| Phase | Workload | BS | QD | Jobs | Target | Throughput (MiB/s) | IOPS | Mean latency (us) | vs local |"
    echo "|-------|----------|----|----|------|--------|-------------------:|-----:|------------------:|---------:|"
    for row in "${ROWS[@]}"; do
        IFS=$'\t' read -r phase name bs qd jobs label bw iops lat pct <<<"$row"
        printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' \
            "$phase" "$name" "$bs" "$qd" "$jobs" "$label" "$bw" "$iops" "$lat" "$pct"
    done
} | tee "$RESULT_FILE"

log "benchmark complete — results written to $RESULT_FILE"
