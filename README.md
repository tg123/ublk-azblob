# ublk-azblob

A Linux **ublk** (userspace block device) target that exposes an **Azure Page Blob**
as a local block device (`/dev/ublkbN`), written in Rust.

> **Status:** Initial draft / scaffold. See [DESIGN.md](DESIGN.md) for the full
> architecture, phased plan, and known limitations.

---

## Requirements

| Requirement | Notes |
|-------------|-------|
| Linux kernel ≥ 6.0 | `ublk_drv` module must be loaded (`modprobe ublk_drv`) |
| `CAP_SYS_ADMIN` / root | Required to create ublk devices |
| Rust stable toolchain | Install via [rustup](https://rustup.rs) |
| Cargo | Comes with Rust |

> **Note:** The `BlobBackend`-level smoke test (no kernel, no `/dev/ublkbN`) runs
> without the kernel driver. Only the full block-device path needs root + ublk_drv.

---

## Build

```bash
# Clone
git clone https://github.com/tg123/ublk-azblob.git
cd ublk-azblob

# Build the full binary (ublk + CSI on by default).
# Needs `protoc` plus the protobuf well-known types and libclang for the ublk
# bindgen, e.g. apt `protobuf-compiler libprotobuf-dev libclang-dev`.
cargo build --release -p ublk-azblob

# Core-only build for any kernel / macOS (no ublk, no CSI, no protoc needed)
cargo build --release -p ublk-azblob --no-default-features
```

---

## Usage

### Run a smoke test against Azurite (local dev)

```bash
# Start Azurite
docker run -d -p 10000:10000 mcr.microsoft.com/azure-storage/azurite \
  azurite-blob --blobHost 0.0.0.0

# Run the built-in smoke test (create → write → read-back → clear → zero-verify)
cargo run -p ublk-azblob -- \
  --blob-url http://127.0.0.1:10000/devstoreaccount1/mycontainer/myblob \
  --account-key "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==" \
  test --size 4096
```

### Benchmark the backend (throughput / IOPS / latency)

```bash
# Provision a 64 MiB blob and run all four phases (seq write/read, rand write/read)
cargo run --release --features bench -p ublk-azblob -- \
  --blob-url http://127.0.0.1:10000/devstoreaccount1/mycontainer/mybench \
  --account-key "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==" \
  bench --create
```

The `bench` subcommand (gated behind the off-by-default `bench` feature so it is
not shipped in the release binary) issues a fixed number of fixed-size
operations against the `BlobBackend` using a configurable number of concurrent
workers (mirroring a ublk queue depth) and reports throughput (MiB/s), IOPS, and
latency percentiles (min / avg / p50 / p95 / p99 / max) per phase. It runs
against Azurite, real Azure, or the in-memory backend used in tests.

| Flag | Default | Notes |
|------|---------|-------|
| `--size` | `67108864` (64 MiB) | Benchmark blob size (multiple of 512) |
| `--block-size` | `4096` | I/O size per operation (multiple of 512) |
| `--count` | `1024` | Operations issued per phase |
| `--concurrency` | `16` | Concurrent in-flight operations (queue depth) |
| `--workload` | `all` | `seq`, `rand`, or `all` |
| `--create` | _off_ | Provision/overwrite the blob before benchmarking |

### Run as a block device (requires root + ublk_drv; ublk is built in by default)

```bash
# System-assigned Managed Identity (recommended on Azure VMs / AKS)
sudo ./target/release/ublk-azblob \
  --blob-url https://mystorageaccount.blob.core.windows.net/mydisks/myvm.vhd \
  --msi \
  run --size 10737418240

# User-assigned Managed Identity by client ID
sudo ./target/release/ublk-azblob \
  --blob-url https://mystorageaccount.blob.core.windows.net/mydisks/myvm.vhd \
  --msi-client-id 00000000-0000-0000-0000-000000000000 \
  run --size 10737418240

# Account key (local dev / Azurite)
sudo ./target/release/ublk-azblob \
  --blob-url http://127.0.0.1:10000/devstoreaccount1/mycontainer/myblob.vhd \
  --account-key "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==" \
  run --size 4194304
```

After launch, a `/dev/ublkbN` device appears and can be used like any block device:

```bash
sudo mkfs.ext4 /dev/ublkb0
sudo mount /dev/ublkb0 /mnt/azblob
```

---

## Read-only mode and blob snapshots

A device is exposed **read-only** by mounting an immutable **point-in-time
snapshot** of the blob: append `?snapshot=<SNAPSHOT>` (the `x-ms-snapshot`
timestamp returned when the snapshot was created) to `--blob-url`. There is no
separate read-only flag — a snapshot is immutable, so the ublk device (or NBD
export) is advertised read-only and every write, discard, and write-zeroes
request is rejected, and (because the content can never change) the local cache
is safe to reuse.

```bash
# Mount a specific blob snapshot (read-only is implied)
sudo ./target/release/ublk-azblob \
  --blob-url "https://mystorageaccount.blob.core.windows.net/mydisks/myvm.vhd?snapshot=2024-01-31T12:00:00.0000000Z" \
  --msi \
  run --size 10737418240
```

`--create` cannot be combined with a snapshot URL (a snapshot is immutable). A
snapshot mount skips the write-back buffer entirely (there are no writes to
batch); read caching via `--cache-dir` still works.

---

## Blob lock (single-writer safety)

To prevent two processes from writing to the same page blob at once (which would
corrupt it), the `run` subcommand acquires an **Azure blob lease** ("blob lock")
before mounting. This is **on by default**: if another process already holds the
lease, `run` refuses to mount. The lease is finite and renewed automatically
while the device is up, and released on clean shutdown (a crashed holder's lease
lapses within ≤60s).

```bash
# Default: the blob lock is acquired automatically — no extra flag needed.
sudo ./target/release/ublk-azblob \
  --blob-url https://mystorageaccount.blob.core.windows.net/mydisks/myvm.vhd \
  --msi \
  run --size 10737418240
```

Pass `--disable-blob-lock` to skip it — only when you are certain no other
process is using the blob:

```bash
sudo ./target/release/ublk-azblob \
  --blob-url https://mystorageaccount.blob.core.windows.net/mydisks/myvm.vhd --msi \
  run --size 10737418240 --disable-blob-lock
```

Read-only snapshot mounts (a `?snapshot=` blob URL) never take the lock, since they
never write. In Kubernetes, the CSI driver layers a cluster-wide lease on top of
this blob lock via `--coordination` so a dead node's volume can be safely taken
over; see [`docs/cluster-testing.md`](docs/cluster-testing.md). `--coordination`
relies on the blob lock and therefore cannot be combined with
`--disable-blob-lock`.

---

## NBD compatibility mode (no `ublk_drv` required)

For kernels or platforms **without** `ublk_drv` (older kernels, containers that
can't load the module, etc.), `ublk-azblob` can instead expose the blob over the
**NBD** (Network Block Device) protocol with the `--nbd <host:port>` option.
This mode needs no special kernel module — only a TCP socket and the standard
NBD client — and does **not** require the `ublk` Cargo feature.

```bash
# Start the NBD server (works on any kernel; no ublk device needed)
./target/release/ublk-azblob \
  --blob-url http://127.0.0.1:10000/devstoreaccount1/mycontainer/myblob.vhd \
  --account-key "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==" \
  run --size 4194304 --nbd 0.0.0.0:10809

# In another shell, attach it as /dev/nbd0
sudo modprobe nbd
sudo nbd-client 127.0.0.1 10809 /dev/nbd0
sudo mkfs.ext4 /dev/nbd0
sudo mount /dev/nbd0 /mnt/azblob

# Detach when done
sudo nbd-client -d /dev/nbd0
```

The ublk-specific options (`--nr-queues`, `--queue-depth`, `--id`) are ignored
in NBD mode. The local-disk / write-back cache options behave identically.

---

## Building an image from a folder or Docker image

To **pre-seed** a blob with data (instead of writing to it live through the
device), build an `ext4` filesystem image locally and upload it to the page
blob. The image then mounts read/write through `run` / NBD like any other blob.

Build the image with [`virt-make-fs`](https://libguestfs.org/virt-make-fs.1.html)
(from `libguestfs-tools`). It takes a directory **or a tar stream**, sizes the
filesystem automatically (`--size=+N` adds headroom), and needs **no root and no
loop mount**:

```bash
# From an existing folder
virt-make-fs --type=ext4 --format=raw --size=+1G ./mydir image.raw

# From a Docker image's root filesystem (stream its export straight in)
cid=$(docker create registry.example.com/org/myimage:tag)
docker export "$cid" | virt-make-fs --type=ext4 --format=raw --size=+2G - image.raw
docker rm "$cid"
```

Then upload `image.raw` as a **page blob**. Because each page write is a separate
PutPage round-trip, a parallel uploader such as
[`azcopy`](https://learn.microsoft.com/azure/storage/common/storage-use-azcopy-v10)
is dramatically faster than streaming through the device — it also skips the
all-zero ranges of a sparse image:

```bash
azcopy copy image.raw \
  "https://<account>.blob.core.windows.net/<container>/<blob>" \
  --blob-type PageBlob --overwrite=true
```

Serve it afterwards with a normal `run` (or NBD) against the same `<blob>`.

> **Tips**
> - The page blob size must be a multiple of 512 bytes; `virt-make-fs` raw
>   images already satisfy this.
> - Run `virt-make-fs` as **root** (or via `sudo`) if you need the source
>   files' original ownership/permissions preserved in the image (e.g. a
>   container root filesystem).
> - Lighter alternative without libguestfs: `mkfs.ext4 -d <dir> image.raw
>   <size>` (from `e2fsprogs`) populates an ext4 image from a directory at
>   creation time, but you must specify the size yourself.

---

## Environment variables

Common CLI flags have environment-variable equivalents:

| Flag | Env var |
|------|---------|
| `--blob-url` | `UBLK_BLOB_URL` |
| `--account-key` | `AZURE_STORAGE_KEY` |
| `--max-dirty-pages` | `UBLK_MAX_DIRTY_PAGES` |
| `--max-cached-pages` | `UBLK_MAX_CACHED_PAGES` |
| `--cache-dir` | `UBLK_CACHE_DIR` |
| `--cache-page-size` | `UBLK_CACHE_PAGE_SIZE` |
| `--cache-max-bytes` | `UBLK_CACHE_MAX_BYTES` |
| `--cache-share-pages` | `UBLK_CACHE_SHARE_PAGES` |
| `--cache-warmup` | `UBLK_CACHE_WARMUP` |
| `--cache-warmup-bytes` | `UBLK_CACHE_WARMUP_BYTES` |
| `--io-concurrency` | `UBLK_IO_CONCURRENCY` |
| `--download-concurrency` | `UBLK_DOWNLOAD_CONCURRENCY` |
| `--upload-concurrency` | `UBLK_UPLOAD_CONCURRENCY` |
| `--download-bandwidth` | `UBLK_DOWNLOAD_BANDWIDTH` |
| `--upload-bandwidth` | `UBLK_UPLOAD_BANDWIDTH` |
| `--nbd` | `NBD_LISTEN` |

---

## Centralized Azure I/O limits (bandwidth & threads)

Every Azure download (read) and upload (write / clear / server-side copy) is
funnelled through a single, process-wide **I/O gateway**. It is the one place
that bounds:

- **Bandwidth** — `--download-bandwidth` / `--upload-bandwidth` (bytes/sec,
  `UBLK_DOWNLOAD_BANDWIDTH` / `UBLK_UPLOAD_BANDWIDTH`), one limiter *per
  direction*. `0` (default) = unlimited.
- **Threads / concurrency** — a single shared budget `--io-concurrency`
  (`UBLK_IO_CONCURRENCY`) across both directions. `0` (default) auto-sizes it to
  the logical CPU count. Downloads and uploads draw from this budget
  *dynamically*, so a busy direction can use the whole budget while the other is
  idle (e.g. reads alone can reach the full CPU count when nothing is being
  written). `--download-concurrency` / `--upload-concurrency`
  (`UBLK_DOWNLOAD_CONCURRENCY` / `UBLK_UPLOAD_CONCURRENCY`) optionally cap how
  much of the shared budget each direction may use; `0` (default) = the full
  budget.

The gateway uses a **provider/consumer** model: producers (on-demand reads,
write-back flush, server-side copy and cache warm-up) enqueue work onto a
priority queue that the shared worker pool drains highest-priority-first. This
stops background work from starving foreground I/O. The priority order is:

**foreground read > flush > copy > warm-up**

Downloads and uploads keep separate priority queues and bandwidth limiters (the
order above is enforced *within* each direction), but share one concurrency
budget so neither direction's threads sit idle while the other has work.

> The per-subsystem knobs `UBLK_FLUSH_CONCURRENCY`, `UBLK_CACHE_WARMUP_CONCURRENCY`
> and `UBLK_COPY_CONCURRENCY` now only bound how many operations each producer
> keeps *in flight* (i.e. memory / pipeline depth); each such operation submits
> through the gateway, which enforces the authoritative Azure thread and
> bandwidth ceilings.

---

## Multi-level cache (memory → local disk → blob)

`ublk-azblob` can stack a persistent **local-disk cache** between the in-memory
write-back buffer and Azure, giving a three-level cache:

```
BufferedBackend (memory) ──► FileCacheBackend (local disk) ──► AzurePageBlobBackend (blob)
```

The **in-memory write-back buffer** (`BufferedBackend`) is itself a cache, not
just a write batcher: pages fetched to satisfy reads (and pages left behind
after a flush) stay resident so later accesses are served straight from memory.
Its resident set is bounded by a least-recently-used budget — `--max-cached-pages`
(`UBLK_MAX_CACHED_PAGES`, default `256`) — which evicts the least-recently-used
**clean** pages once the count is exceeded. Dirty (unflushed) pages are pinned
and never evicted; they are bounded separately by `--max-dirty-pages`, which
flushes them. Set `--max-cached-pages 0` for an unbounded (grow-only) memory
cache; any non-zero value must be `>= --max-dirty-pages`.

Enable the local-disk level by pointing `--cache-dir` at a directory on a local disk:

```bash
sudo ./target/release/ublk-azblob \
  --blob-url https://mystorageaccount.blob.core.windows.net/mydisks/myvm.vhd \
  --msi \
  run --size 10737418240 \
  --cache-dir /var/cache/ublk-azblob \
  --cache-page-size 1048576
```

The local-disk cache stores pages in a sparse `<container>-<blob>.dat` file with a
companion `.meta` file holding `present`/`dirty` bitmaps. Page data is `fsync`ed
before a page is marked dirty, and the dirty bitmap is `fsync`ed on every change,
so **unflushed dirty data survives a crash or restart**. On startup the cache is
recovered from disk and any recovered dirty pages are flushed to the blob, so
in-flight writes are never silently lost.

#### Reusing the cache across restarts (ETag validation)

The cache is reused across restarts in **read-write** mode as well as for
immutable snapshots. After each flush the backing blob's current **ETag** (its
validity token) is recorded next to the cache in a `<container>-<blob>.etag`
file. On the next start the live ETag is fetched and compared:

- **unchanged** — nothing modified the blob since this cache last wrote it, so
  the locally cached *clean* pages are still valid and are served from local
  disk (no re-download). This is what makes the read-write cache safe to reuse:
  write locally, flush to the blob, and a restart picks the local cache back up.
- **changed** — the blob was modified externally, so the stale *clean* pages are
  discarded and transparently re-fetched on demand.

Either way, *dirty* (unflushed) pages are this process's own pending writes: they
are always recovered and flushed, never dropped. When the backend cannot report
an ETag, validation is skipped and the cache is trusted as before.

### Bounding the cache size (shared LRU byte budget)

By default the local-disk cache grows without bound. Set `--cache-max-bytes`
(or `UBLK_CACHE_MAX_BYTES`) to cap how much disk the cache may consume:

```bash
  --cache-dir /var/cache/ublk-azblob \
  --cache-max-bytes 10737418240   # 10 GiB
```

When the cache exceeds the limit, the least-recently-used **clean** pages are
evicted by punching holes in the sparse `.dat` file (reclaiming the disk
blocks); the data is transparently re-fetched from the blob on the next access.
Dirty (unflushed) pages are never evicted, so no write is ever lost — if every
resident page is dirty the cache may temporarily exceed the limit until a flush
makes pages clean again. `0` (the default) means unlimited.

The budget is **shared across every `ublk-azblob` process that points at the
same `--cache-dir`**, coordinated through a small `.cache-budget` file in that
directory (locked with `flock`). This makes the cap meaningful in CSI / multi-volume
scenarios where many per-volume processes share one node's cache disk: a single
noisy volume cannot fill the disk at the expense of its neighbours. The shared
total is crash-safe — entries for processes that died are pruned automatically.

> **Eviction scope:** each process only ever evicts *its own* clean pages, so it
> never touches a peer's cache files. The budget therefore bounds the aggregate
> resident set of the *active* processes; a fully idle peer keeps its pages until
> it next does I/O or exits.

### Cross-process page sharing (`--cache-share-pages`)

> **Currently disabled.** Cross-process page sharing is implemented (the
> `.cache-index` machinery below) but **forced off in the shipped binary**:
> `--cache-share-pages` / `UBLK_CACHE_SHARE_PAGES` are accepted but ignored (a
> warning is logged), so every cache is single-process. Read caching for an
> immutable snapshot and the pre-upload write cache are both single-owner and do
> not need sharing. The description below documents the (gated-off) design; it is
> expected to be re-enabled in a later iteration.

With `--cache-share-pages` (or `UBLK_CACHE_SHARE_PAGES=1`), processes that cache
the **same blob** in the same `--cache-dir` serve each other's clean pages off
local disk instead of re-fetching from Azure. A shared `.cache-index` file
(locked with `flock`, alongside `.cache-budget`) maps each cached `(blob, page)`
to the owning process's `.dat` file and offset. On a read miss, a process first
consults the index: if a live peer holds the page clean, it copies the bytes from
the peer's file (read-only) and only falls back to the blob if the page is
absent, stale, or the peer has died. Shared reads are served directly and are
**not** double-counted against the budget — every resident page still has exactly
one on-disk owner.

Writes use **copy-on-write** to preserve the single-writer-per-file invariant:
before mutating a page that a peer owns, the writer withdraws it from the index
and writes into its *own* `.dat`, marking it dirty locally. Dirty pages are never
evicted and never served cross-process; once flushed and clean again the new
owner re-publishes the page so subsequent peer reads resolve to it. As with the
budget, a crashed peer's index entries are pruned automatically (`kill(pid, 0)`),
and losing the index only forgoes sharing — correctness falls back to the blob.

In CSI deployments enable this through the Helm chart's `node.cache.sharePages`;
the node plugin assigns each volume a unique cache instance so concurrent mounts
of the same blob transparently share clean pages.

### Cache warm-up (`--cache-warmup`)

By default there is no warm-up: the local cache is populated **on demand by
writes** (copy-on-write). A pure read miss is served straight from a live peer
or the blob and is **not** stored in the local `.dat`, so a read-only blob that
is never warmed keeps reading through to its source. With `--cache-warmup` (or
`UBLK_CACHE_WARMUP=1`) the process instead **prefetches the blob into the cache
on start**, sequentially, in the **background** (the device comes online
immediately). Each prefetched page is stored locally as a clean, resident page.
(While sharing is disabled, warm-up populates only this process's own cache.)

Prefetch stops after `--cache-warmup-bytes` (or `UBLK_CACHE_WARMUP_BYTES`); `0`
(the default) means auto — the cache byte budget (`--cache-max-bytes`) when set,
otherwise the whole device. Warm-up is best for **read-only / read-mostly
datasets that fit the cache budget** (e.g. an immutable snapshot golden image).
For large, write-heavy, or sparsely-accessed blobs, leave it off — the cache
only fetches what's actually used. In CSI deployments toggle it via the Helm
chart's `node.cache.warmup`.

**Sparse warm-up.** Before warming, the driver asks Azure for the blob's data
ranges (`Get Page Ranges`, `?comp=pagelist`). On a sparse page blob — e.g. an
ext4 image whose free space was never written — pages that fall entirely in a
zero gap are **skipped**: they are never downloaded and read back as zeros on
demand. Only the regions that actually hold data are transferred, so warming a
mostly-empty filesystem image costs a fraction of its nominal size. Backends or
blobs that cannot report ranges fall back to a full sequential sweep. The
all-zero pages that are loaded are also left as **holes** in the local `.dat`
file, so they consume no cache disk either.

---

## Auth modes

| Mode | Flag | Notes |
|------|------|-------|
| Managed Identity (system) | `--msi` | Recommended on Azure VMs; no secrets on disk |
| Managed Identity (user) | `--msi-client-id <id>` | Multiple identities per host |
| Shared Key | `--account-key <key>` | Local dev, CI, Azurite |

> **Note:** The Azure Rust SDK (`azure_identity`, `azure_storage_blob`) is
> **0.x / preview** — API changes between minor releases are expected. Exact
> dependency versions are pinned in `Cargo.toml` for reproducibility. See
> [DESIGN.md](DESIGN.md#the-thin-sdk-trait-boundary) for the isolation strategy.

---

## Running the e2e test locally

The e2e test exercises the **full stack**: a real `/dev/ublkbN` block device
backed by an Azure Page Blob (Azurite), with an ext4 filesystem mounted on top.
It writes random files, forces a flush (`SIGUSR1`), unmounts, tears the device
down, remounts over the same blob, and verifies every file's SHA-256 checksum.

It requires a Linux ≥6.0 host with `ublk_drv` loaded and Docker.  Everything
else — the Rust build, `mkfs.ext4`, and Azurite — runs inside docker compose:

```bash
# 1. Load the kernel module on the host (a container can't do this for you)
sudo modprobe ublk_drv

# 2. Build + run the mount → write → flush → unmount → remount → verify cycle.
#    The `runner` service builds the default (ublk + CSI) binary and runs the
#    Rust test; Azurite is started automatically as its dependency.
docker compose -f tests/e2e/docker-compose.yml up \
  --build --abort-on-container-exit --exit-code-from runner

# 3. Tear everything down when done
docker compose -f tests/e2e/docker-compose.yml down -v
```

The e2e test lives in [ublk-azblob/tests/mount_e2e.rs](ublk-azblob/tests/mount_e2e.rs);
it is gated behind the `ublk` feature and skips itself when not run as root with
`ublk_drv` loaded.

---

## Benchmarking I/O speed (ublk-azblob vs. raw local disk)

The benchmark **pipeline** measures the block-device I/O speed of `ublk-azblob`
against a **raw local disk** baseline using [`fio`](https://fio.readthedocs.io/).
Both targets are benchmarked as raw block devices (no filesystem):

* **ublk-azblob** — a real `/dev/ublkbN` device backed by an Azure Page Blob
  (Azurite in CI).
* **local disk** — a loopback (`losetup`) device backed by a file on the
  container's local filesystem, used as the reference baseline.

The same fio jobs run against each target and the script prints a side-by-side
comparison of throughput (MiB/s), IOPS, and mean latency, with each ublk-azblob
result also expressed as a **percentage of the raw-local-disk baseline** (the
`vs local` column). The jobs are grouped into phases:

* **Phase 1 — Raw block performance:** the four base patterns (sequential and
  random read/write) plus sweeps over block size (`4k…1M`), queue depth
  (`1…128`), and read/write mix (`100/0`, `70/30`, `50/50`).
* **Phase 2 — Cache behaviour:** cold-cache vs. warm-cache buffered reads, each
  compared against the raw-local-disk baseline, plus the warm/cold speed-up.
  The device runs without `--cache-dir` (and `BufferedBackend` does not cache
  clean reads), so this warm/cold speed-up reflects the kernel's block-device
  page cache, not a ublk-azblob read cache.
* **Phase 3 — backend latency:** GET/PUT/flush throughput, IOPS and latency
  measured directly against the Azure Page Blob backend via the `bench`
  subcommand (bypassing the kernel device). The pipeline builds the binary with
  the `bench` feature, runs it on a separate blob, and appends its results table
  to the same summary (set `BENCH_BACKEND=0` to skip).
* **Phase 4 — Scalability:** the random-read workload at increasing thread
  (`numjobs`) counts.

Like the e2e test, the Rust build, `fio`, and Azurite all run inside docker
compose:

```bash
# 1. Load the kernel module on the host (a container can't do this for you)
sudo modprobe ublk_drv

# 2. Build + run the fio benchmark against both targets and print the comparison.
docker compose -f tests/bench/docker-compose.yml up \
  --build --abort-on-container-exit --exit-code-from runner

# 3. Tear everything down when done
docker compose -f tests/bench/docker-compose.yml down -v
```

The comparison table is printed to stdout and written to `bench-results.md`.
The benchmark is tunable via environment variables — base block size, queue
depth, threads, runtime, direct vs. buffered I/O, and the per-phase sweep lists
(`FIO_BS_LIST`, `FIO_IODEPTH_LIST`, `FIO_RWMIX_LIST`, `FIO_NUMJOBS_LIST`) — see
the header of [tests/bench/bench.sh](tests/bench/bench.sh) for the full list,
e.g.:

```bash
# Trim the sweeps for a quick run.
FIO_BS_LIST="4k 64k" FIO_IODEPTH_LIST="1 16" FIO_NUMJOBS_LIST="1 4" \
  docker compose -f tests/bench/docker-compose.yml up \
    --build --abort-on-container-exit --exit-code-from runner
```

In CI the benchmark runs on pushes to `main`, on every pull request, and on
demand via the **`bench`** workflow (`workflow_dispatch` in the Actions tab, with
tunable inputs).  Results are attached as a `bench-results` artifact and rendered
into the run's job summary.

---

## Kubernetes (CSI driver)

`ublk-azblob` ships an in-tree **Container Storage Interface (CSI)** driver so
each `PersistentVolumeClaim` is provisioned as one Azure Page Blob and exposed
to pods as an ext4 filesystem on a ublk block device. The driver is the same
binary (ublk + CSI are on by default) run via the `csi` subcommand:

```bash
# controller (provisions/deletes page blobs) — runs in a Deployment
ublk-azblob csi --role controller --csi-endpoint unix:///csi/csi.sock

# node (attaches the ublk device + mounts the filesystem) — runs in a DaemonSet
ublk-azblob csi --role node --csi-endpoint unix:///csi/csi.sock
```

Driver name: `azblob.ublk.csi.tg123.github.io`. Volume IDs encode the blob
location as `<account>/<container>/<blob>`; the endpoint comes from the driver's
environment (`AZURE_STORAGE_*`), while the storage account and container can be
overridden per `StorageClass` via the `storageAccount` and `container`
parameters (the account is encoded in the volume ID so `DeleteVolume` can recover
a per-volume account).

### Deploy

The `ublk_drv` kernel module must be loaded on every node
(`sudo modprobe ublk_drv`); a container cannot load it.

```bash
# 1. Build + publish the driver image (or load it into your cluster).
#    CI publishes ghcr.io/tg123/ublk-azblob (and Docker Hub) on push to
#    main and on version tags via .github/workflows/docker.yml; to build
#    locally instead:
docker build -f deploy/Dockerfile -t ghcr.io/tg123/ublk-azblob:latest .

# 2. Install the Helm chart (CSIDriver, RBAC, controller, node, StorageClass).
#    See deploy/chart/README.md for all values (auth, NBD mode, secrets, ...).
helm install csi-ublk-azblob deploy/chart \
  --namespace kube-system \
  --set image.repository=ghcr.io/tg123/ublk-azblob --set image.tag=latest

# 3. Provide storage credentials to the namespace that will use the driver
#    (per-namespace mode, the default). For SharedKey auth:
kubectl -n <your-namespace> create secret generic azblob-csi-secret \
  --from-literal=AZURE_STORAGE_ACCOUNT=<storage-account> \
  --from-literal=accountKey=<storage-key>   # omit when using Managed Identity

# 4. Create a PVC + pod
kubectl apply -f deploy/example/pvc.yaml
kubectl apply -f deploy/example/pod.yaml
```

On an Azure VM/AKS node with Managed Identity, drop `accountKey` from the secret
and add `AZURE_USE_MSI=true` (optionally `AZURE_MSI_CLIENT_ID`) to the driver
containers instead.

### Kubernetes e2e

The **PVC lifecycle** e2e ([tests/k8s_pvc_e2e.rs](ublk-azblob/tests/k8s_pvc_e2e.rs))
runs against a multi-node `k3s` cluster started by
[tests/e2e/docker-compose.yml](tests/e2e/docker-compose.yml): it deploys the
driver via Helm, provisions a PVC, writes random data, tears the pod down, and
remounts the same PVC (including across nodes) to verify the data survived the
round-trip through the page blob. The whole suite — mount, NBD and PVC — shares
one compile of the shipped image and runs together:

```bash
sudo modprobe ublk_drv
sudo mkdir -p /var/lib/kubelet && sudo mount -t tmpfs tmpfs /var/lib/kubelet \
  && sudo mount --make-shared /var/lib/kubelet
docker compose -f tests/e2e/docker-compose.yml up \
  --build --abort-on-container-exit --exit-code-from runner
docker compose -f tests/e2e/docker-compose.yml down -v
```
---

## Running unit tests

```bash
cargo test -p ublk-azblob
```

Unit tests run against `MemBackend` — no network, no kernel required.

---

## CI

GitHub Actions runs on every push to `main` and every pull request:
- `cargo fmt --check`
- `cargo clippy -- -D warnings`
- `cargo test` (unit tests, `MemBackend`)
- the `e2e` workflow: one job that builds the shipped image once and runs the
  mount (`/dev/ublkbN` + ext4 ↔ Azurite), NBD, and Kubernetes PVC e2e against a
  k3s cluster from `tests/e2e/docker-compose.yml`.

The e2e job runs on `ubuntu-22.04`, loads `ublk_drv` from
`linux-modules-extra`, and runs the mount/remount/checksum cycle as root.
(`ubuntu-24.04` is avoided because its azure kernel currently Oopses in
`ublk_drv` — see [actions/runner-images#14175](https://github.com/actions/runner-images/issues/14175).)

A separate **`bench`** workflow (on pushes to `main`, pull requests, and manual `workflow_dispatch`)
runs the fio benchmark comparing the ublk-azblob device against a raw local disk,
on the same `ubuntu-22.04` runner.

---

## Project structure

```
ublk-azblob/
├── Cargo.toml                  # workspace root
├── DESIGN.md                   # architecture & phased plan
├── README.md                   # this file
├── ublk-azblob/
│   ├── Cargo.toml              # pinned dependencies
│   ├── src/
│   │   ├── main.rs             # CLI entry point (clap)
│   │   ├── auth.rs             # MSI + SharedKey credential factory
│   │   ├── bench.rs            # backend throughput / IOPS / latency benchmark
│   │   ├── ublk_target.rs      # ublk device I/O loop (default feature `ublk`)
│   │   ├── csi/                # Kubernetes CSI driver (default feature `csi`)
│   │   │   ├── mod.rs          # gRPC server, role/config, volume-id encoding
│   │   │   ├── identity.rs     # CSI Identity service
│   │   │   ├── controller.rs   # CSI Controller service (Create/DeleteVolume)
│   │   │   ├── node.rs         # CSI Node service (attach ublk device + mount)
│   │   │   └── mount.rs        # mkfs/mount/umount + ublk device discovery
│   │   └── backend/
│   │       ├── mod.rs          # BlobBackend trait (SDK isolation boundary)
│   │       ├── azure.rs        # AzurePageBlobBackend (real SDK impl)
│   │       ├── buffered.rs     # BufferedBackend (in-memory write-back cache)
│   │       ├── file.rs         # FileCacheBackend (persistent local-disk cache)
│   │       └── mem.rs          # MemBackend (in-memory, for unit tests)
│   ├── proto/csi/csi.proto     # vendored CSI spec (codegen via build.rs)
│   └── tests/
│       └── mount_e2e.rs        # full mount → write → flush → remount → verify
├── deploy/
│   ├── Dockerfile              # CSI driver image (default ublk + csi build)
│   ├── chart/                  # Helm chart (CSIDriver, RBAC, controller, node, StorageClass)
│   └── example/                # sample PVC + pod
├── tests/
│   ├── e2e/
│   │   ├── docker-compose.yml  # Azurite + k3s + runner for the whole e2e suite
│   │   ├── Dockerfile          # e2e runner image (rust + docker/kubectl/helm)
│   │   └── k8s/                # k8s manifests for the PVC e2e (helm values, writer/reader jobs)
│   └── bench/
│       ├── bench.sh            # fio benchmark: ublk-azblob vs. raw local disk
│       └── docker-compose.yml  # benchmark override reusing tests/e2e/docker-compose.yml
└── LICENSE.md                  # MIT license
```
