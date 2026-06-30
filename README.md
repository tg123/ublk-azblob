# ublk-azblob

**Mount an Azure Page Blob as a local Linux disk.** `ublk-azblob` exposes any
Azure Page Blob as a block device — via **[ublk](https://docs.kernel.org/block/ublk.html)**
(`/dev/ublkbN`, preferred) or **NBD** (`/dev/nbdX`) — so you can put a real
filesystem (ext4 / xfs / btrfs) on cheap, durable, network-backed storage. It
also ships a Kubernetes **CSI driver** so each `PersistentVolumeClaim` is one
page blob. Written in Rust on a thin, version-pinned Azure SDK boundary.

- **Cheap & elastic** — page blobs are sparse: pay only for written pages, up to 8 TiB.
- **Read-only snapshots** — fan one golden image out to many pods, each with its own ephemeral overlay.
- **Local-disk cache** — hot pages served from local SSD, with optional warm-up.
- **Single-writer safety** — Azure blob lease, plus an optional Kubernetes lease for take-over.

See [Architecture and design](#architecture-and-design) below for the full design.

---

## Quick start (Docker Compose)

Mount a page blob as a local disk in one command — **no Azure account needed**
(a local [Azurite](https://github.com/Azure/Azurite) emulator stands in for Azure):

```bash
cd examples/quickstart
sudo modprobe ublk_drv          # preferred — or: sudo modprobe nbd
docker compose up --build -d
docker compose logs -f ublk-azblob   # watch until "✓ … mounted", then Ctrl-C
```

This provisions a page blob, exposes it as a block device (**ublk preferred,
NBD fallback — auto-detected**), formats and mounts it at `/mnt/azblob` inside
the container.

Once it's up, browse and write to the remote-backed disk:

```bash
# List what's on the page-blob-backed disk
docker compose exec ublk-azblob ls -la /mnt/azblob

# Write a file — the bytes land in the Azure Page Blob, not the container
docker compose exec ublk-azblob sh -c 'echo "hello from a page blob" > /mnt/azblob/demo.txt'
docker compose exec ublk-azblob cat /mnt/azblob/demo.txt

# Prove it persists: the data is in the blob, so it survives a restart
docker compose down && docker compose up -d
docker compose exec ublk-azblob ls -la /mnt/azblob   # demo.txt is still there
```

To target **real Azure**, set `UBLK_BLOB_URL` plus a SAS or account key. Full
walkthrough: **[examples/quickstart/](examples/quickstart/)**.

> Requires a Linux host with `ublk_drv` (preferred) or `nbd` loaded, and root /
> `CAP_SYS_ADMIN` to create the device. The container image is built from
> `deploy/Dockerfile` on first run.

---

## Kubernetes (CSI driver) — install & example

Each PVC is provisioned as one Azure Page Blob and mounted into pods as a
filesystem on a ublk device. The `ublk_drv` module must be loaded on **every
node** (`sudo modprobe ublk_drv`); a container cannot load it.

```bash
# 1. Install the driver (CSIDriver, RBAC, controller Deployment, node DaemonSet,
#    StorageClass) via Helm. See deploy/chart/README.md for all values.
helm install csi-ublk-azblob deploy/chart \
  --namespace kube-system \
  --set image.repository=ghcr.io/tg123/ublk-azblob --set image.tag=latest

# 2. Give the consuming namespace its storage credentials (SharedKey shown;
#    drop accountKey and use AZURE_USE_MSI=true on Azure VMs / AKS instead).
kubectl -n <your-namespace> create secret generic azblob-csi-secret \
  --from-literal=AZURE_STORAGE_ACCOUNT=<storage-account> \
  --from-literal=accountKey=<storage-key>

# 3. Create a PVC + pod (one page blob, mounted into the pod).
kubectl apply -f deploy/example/pvc.yaml
kubectl apply -f deploy/example/pod.yaml
```

More detail (driver internals, snapshots, overlay, coordination) is in the
[Kubernetes CSI Driver design](#kubernetes-csi-driver) below.

---

## Typical scenarios

**a. Golden image fan-out for lots of small files (snapshots).**
Seed one page blob with an expensive-to-build tree — a cloned git repo, a
language package cache (`node_modules`, `~/.cargo`, pip wheels), CI build
dependencies, or model weights — then take a **read-only snapshot**. Many worker
pods mount that immutable snapshot simultaneously; each gets a private,
ephemeral **overlay** for its writes, so the shared base is never mutated.
Combined with `--cache-warmup` and the local-disk cache, the thousands of small
files are served from node-local SSD instead of being re-cloned per pod — far
cheaper and faster than baking them into a container image or re-pulling them.

**b. Cheap, durable "infinite" persistent disk.**
Back a workload with a page blob as its primary disk. Page blobs are **sparse**
(you pay only for pages actually written, up to 8 TiB) and live independently of
any node, so they're a low-cost alternative to premium managed disks for
cold/warm data. The disk survives node loss and can be re-mounted elsewhere; the
**blob lease** guarantees only one writer at a time, and the local-disk cache
keeps hot pages fast.

**c. More ideas worth exploring.**
- **Portable / migratable volumes** — the disk lives in blob storage, so a
  workload can detach on one node and re-attach on another (even another region)
  with no data copy; the optional Kubernetes lease enables safe take-over after a
  node dies.
- **Read-mostly datasets across many nodes** — large reference datasets or ML
  model weights mounted read-only from a single snapshot, with each node's cache
  warming the hot regions.
- **Per-branch / per-PR ephemeral environments** — clone a golden database or
  fixture volume per branch via server-side template copy + overlay, then throw
  it away.
- **Tiered cold storage** — an "always-available" block device whose cold blocks
  live in cheap blob storage while a bounded local cache absorbs the working set.

Have a scenario in mind? The building blocks — sparse page blobs, read-only
snapshots, ephemeral overlays, a persistent cache with warm-up, and single-writer
leasing — combine in a lot of ways.

---

## Build & test

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

# Fully-static musl binary (reproduces the published release/image artifact).
# Needs the musl target, `zig`, and `cargo-zigbuild` (`cargo install cargo-zigbuild`).
rustup target add x86_64-unknown-linux-musl
cargo zigbuild --release --target x86_64-unknown-linux-musl -p ublk-azblob

# Unit tests (run against an in-memory backend — no network, no kernel)
cargo test -p ublk-azblob
```

### Run the e2e tests locally

The end-to-end suite exercises the **full stack** on a real `/dev/ublkbN` device
backed by an Azure Page Blob (Azurite): it mounts an ext4 filesystem, writes
random files, flushes, unmounts, tears the device down, remounts over the same
blob, and verifies every checksum — and likewise drives the Kubernetes PVC
lifecycle against a `k3s` cluster. The Rust build, `mkfs`, Azurite and k3s all
run inside docker compose; you only need Docker and `ublk_drv` on the host.

```bash
# 1. Load the kernel module on the host (a container can't do this for you).
sudo modprobe ublk_drv

# 2. A shared mount is needed for the CSI mount-propagation leg.
sudo mkdir -p /var/lib/kubelet && sudo mount -t tmpfs tmpfs /var/lib/kubelet \
  && sudo mount --make-shared /var/lib/kubelet

# 3. Build the shipped image once and run the whole suite (mount + NBD + PVC).
docker compose -f tests/e2e/docker-compose.yml up \
  --build --abort-on-container-exit --exit-code-from runner

# 4. Tear everything down.
docker compose -f tests/e2e/docker-compose.yml down -v
```

There's also an fio benchmark pipeline (ublk-azblob vs. a raw local disk) — see
[tests/bench/bench.sh](tests/bench/bench.sh) and `tests/bench/docker-compose.yml`.

---
## Configuration (environment variables & flags)

Every flag has an environment-variable equivalent, so the same binary works from
a shell, a `docker run`, or a Kubernetes manifest. Flags take precedence over the
environment. The essentials:

| What | Flag | Env var |
|------|------|---------|
| Blob to expose (full URL; may carry `?snapshot=` / SAS) | `--blob-url` | `UBLK_BLOB_URL` |
| Shared Key auth | `--account-key` | `AZURE_STORAGE_KEY` |
| SAS auth | `--sas-token` | `AZURE_STORAGE_SAS` |
| Managed Identity | `--msi` / `--msi-client-id` | `AZURE_USE_MSI` / `AZURE_MSI_CLIENT_ID` |
| Device size (bytes, `run --create`) | `--size` | `UBLK_DEV_SIZE` |
| Expose over NBD instead of ublk | `--nbd <host:port>` | `NBD_LISTEN` |
| Persistent local-disk cache dir | `--cache-dir` | `UBLK_CACHE_DIR` |
| Cache disk budget (bytes) | `--cache-max-bytes` | `UBLK_CACHE_MAX_BYTES` |
| Prefetch the blob into cache on start | `--cache-warmup` | `UBLK_CACHE_WARMUP` |
| Azure read/write bandwidth ceilings | `--download-bandwidth` / `--upload-bandwidth` | `UBLK_DOWNLOAD_BANDWIDTH` / `UBLK_UPLOAD_BANDWIDTH` |
| Azure concurrency budget | `--io-concurrency` | `UBLK_IO_CONCURRENCY` |

`ublk-azblob --help` (and `run --help`) lists every flag. The design rationale
behind snapshots, the multi-level cache, the centralized I/O gateway, and
cluster coordination is detailed in [Architecture and design](#architecture-and-design) below.

---
## Direct CLI usage & features

Outside Docker/Kubernetes, run the binary on a host with `ublk_drv` loaded (root):

```bash
# Account key (local dev / Azurite); use --msi on Azure (see Authentication below)
sudo ublk-azblob \
  --blob-url http://127.0.0.1:10000/devstoreaccount1/mycontainer/disk.img \
  --account-key "<key>" \
  run --create --size 4194304

# A /dev/ublkbN device appears — use it like any disk:
sudo mkfs.ext4 /dev/ublkb0 && sudo mount /dev/ublkb0 /mnt/azblob
```

- **Read-only snapshots.** Append `?snapshot=<timestamp>` to `--blob-url` to mount
  an immutable point-in-time snapshot read-only (writes are rejected and the cache
  is always safe to reuse). Not combinable with `--create`.
- **NBD instead of ublk.** On kernels without `ublk_drv`, add `run --nbd <host:port>`
  and attach with `nbd-client <host> <port> /dev/nbd0`. No `ublk` feature needed.
- **Single-writer safety.** `run` takes an Azure **blob lease** by default and
  refuses to mount if another process holds it (a crashed holder's lease lapses in
  ≤60s). Pass `--disable-blob-lock` only when you're sure nothing else uses the
  blob; read-only snapshot mounts never lock.
- **Persistent cache + warm-up.** `--cache-dir` adds a crash-safe local-disk cache
  between memory and the blob; `--cache-max-bytes` bounds it (shared across
  processes) and `--cache-warmup` prefetches the blob (sparse-aware) on start.

### Authentication

Credentials are selected in priority order (first match wins):

| Mode | Flag / env | When |
|------|-----------|------|
| SAS | `--sas-token` / `AZURE_STORAGE_SAS` | Pre-signed access, no stored secret |
| Shared Key | `--account-key` / `AZURE_STORAGE_KEY` | Azurite, local dev, CI |
| Workload Identity | `--workload-identity` / `AZURE_USE_WORKLOAD_IDENTITY` | AKS federated token |
| Managed Identity | `--msi` / `--msi-client-id` | Azure VM / AKS, no secrets |
| Service Principal | `AZURE_CLIENT_ID` + `AZURE_TENANT_ID` + `AZURE_CLIENT_SECRET` | Entra app registration |

---
## Architecture and design

### Problem Statement

No existing open-source project exposes an **Azure Blob** as a Linux block device.  Comparison:

| Project | Backend | Block device | Azure? | Notes |
|---------|---------|-------------|--------|-------|
| nbdkit-s3 / s3backer | S3 | NBD / FUSE | ❌ | Great architecture reference |
| BlobFuse2 | Azure Blob | FUSE **filesystem** | ✅ | File semantics, not a block device |
| SPDK ublk / ublksrv | pluggable | ublk | ❌ | Framework we build on |
| **ublk-azblob** | Azure Page Blob | **ublk** | ✅ | This project |

`ublk-azblob` fills the gap: a Linux userspace block device that maps directly to
an **Azure Page Blob**, giving you a `/dev/ublkbN` that you can partition, format,
and mount like any other disk — without a FUSE filesystem layer.

---

### Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  Kernel space                                                    │
│  ┌──────────────┐    io_uring     ┌────────────────────────┐     │
│  │  /dev/ublkbN │◄───────────────►│  ublk_drv (kernel mod) │     │
│  └──────────────┘                 └───────────┬────────────┘     │
└──────────────────────────────────────────────────────────────────┘
                                                │ ublk cmd queue
┌───────────────────── Userspace ───────────────▼──────────────────┐
│                                                                  │
│   main.rs (CLI)                                                  │
│      │                                                           │
│      ▼                                                           │
│   ublk_target.rs  ──── libublk (Rust) ──── io_uring              │
│      │                                                           │
│      │  READ  → BlobBackend::read(offset, len)                   │
│      │  WRITE → BlobBackend::write(offset, data)                 │
│      │  DISCARD→BlobBackend::clear(offset, len)                  │
│      │  FLUSH → BlobBackend::flush()                             │
│      ▼                                                           │
│   BlobBackend trait  ◄────────────── isolation boundary          │
│      │                                                           │
│      ▼                                                           │
│   AzurePageBlobBackend                                           │
│      │                                                           │
│      ├─ read  → BlobClient::download(range)                      │
│      ├─ write → PageBlobClient::upload_pages(range, data)        │
│      ├─ clear → PageBlobClient::clear_pages(range)               │
│      └─ size  → BlobClient::get_properties() → content-length    │
│                                                                  │
│   Auth module  (first match wins)                                │
│      ├─ SAS              → ?sig= token on blob URL               │
│      ├─ SharedKey        → StorageSharedKeyPolicy (pipeline)     │
│      ├─ WorkloadIdentity → federated K8s SA token                │
│      ├─ ManagedIdentity  → system or user-assigned MSI           │
│      └─ ServicePrincipal → ClientSecretCredential                │
│                                                                  │
│   azure_storage_blob 1.0.0 SDK ◄── pinned, isolated              │
└──────────────────────────────────────────────────────────────────┘
                          │
                          ▼ HTTPS / HTTP
              Azure Blob Storage / Azurite
```

#### Why Azure Page Blob?

Page blobs are the only Azure Blob type with **512-byte-aligned random read/write**
semantics (`Put Page`, `Get Page Ranges`, `Clear Pages`).  This is the same
primitive used for Azure VM VHDs, making it a natural fit for a block device
abstraction.  Block blobs require chunked read-modify-write (the s3backer approach)
and are better suited for a Phase 3 "block-blob backend" option.

---

### Key Design Decisions

#### 1. `BlobBackend` trait boundary

The Azure SDK (`azure_storage_blob`) is **0.x / preview** and has a history of
breaking API changes between minor releases.  All SDK types are isolated behind
the `BlobBackend` trait:

```
BlobBackend::read/write/clear/flush/size  ←  only interface the I/O loop sees
AzurePageBlobBackend                      ←  all SDK types live here
BufferedBackend                           ←  in-memory write-back + read cache (wraps any backend)
FileCacheBackend                          ←  persistent local-disk cache (wraps any backend)
MemBackend                                ←  in-memory, no network, for unit tests
```

Because every layer implements `BlobBackend`, they compose into a *multi-level*
cache — for example `BufferedBackend` (memory) → `FileCacheBackend` (local disk)
→ `AzurePageBlobBackend` (blob).  The local-disk cache persists its `present` /
`dirty` page bitmaps so that **dirty pages survive a restart**: on startup the
cache is recovered from disk and any recovered dirty pages are flushed to the
blob.  Clean pages survive a restart too — including in read-write mode — gated
by the backing blob's **ETag**: `FileCacheBackend` records the blob ETag (via the
`BlobBackend::etag` accessor) after each flush and, on reopen, reuses the cached
clean pages only when the live ETag still matches (proving no external change);
on a mismatch the stale clean pages are dropped while dirty pages are kept.

A future SDK upgrade only requires modifying `src/backend/azure.rs`.

#### 1b. Pluggable front-ends: ublk and NBD

The same `BlobBackend` boundary lets the device be driven by more than one
kernel/userspace front-end.  Two targets are provided:

```
ublk_target.rs  ←  Linux ublk (io_uring) → /dev/ublkbN   (needs ublk_drv, root, kernel ≥6.0)
nbd_target.rs   ←  NBD server (TCP)       → /dev/nbdX     (compatibility; any kernel with nbd client)
```

`nbd_target.rs` implements the server side of the NBD *fixed newstyle*
handshake and transmission phase in pure `tokio` (no extra dependencies, no
Cargo feature flag) and maps each NBD command to the same trait the ublk loop
uses (`READ`/`WRITE`/`FLUSH`/`TRIM`/`WRITE_ZEROES` → `read`/`write`/`flush`/
`clear`).  It is selected with `run --nbd <host:port>` and exists so the blob
can be exposed on kernels/platforms where `ublk_drv` is unavailable.  It
advertises a 512-byte minimum / 4 KiB preferred block size so clients align I/O
to the page-blob granularity.

#### 2. 512-byte alignment

All offsets and lengths are validated to be multiples of 512 bytes.  Azure Page
Blob requires this; the block layer enforces it for us at the ublk driver level.
Misaligned requests return an immediate error instead of silently corrupting data.

#### 3. Write-back buffering (with write-through fallback)

The page blob is **write-through** at the storage layer: every `upload_pages`
call is durable from Azure's perspective once the HTTP 201 is received, and the
bare `AzurePageBlobBackend::flush()` is a no-op.

For performance the I/O stack normally runs a **write-back buffer**
(`BufferedBackend`) in front of the blob (and the local-disk cache): writes land
in memory, are coalesced per page, and are flushed to the inner backend
asynchronously — on an idle timer, when the dirty set exceeds a bound, or on an
explicit `flush()` (driven by the device's FLUSH/FUA path and by teardown). The
buffer doubles as a read cache with LRU eviction of clean pages. Set
`--page-size 0` (env `UBLK_PAGE_SIZE=0`) to disable buffering and fall back to
pure write-through at runtime.

Durability across a crash is preserved by the persistent local-disk cache, not
the in-memory buffer: dirty pages are recorded in the on-disk `dirty` bitmap
before being advertised, so a restart re-flushes them to the blob (see §1).

#### 4. Authentication

`build_auth` selects a credential in priority order; all are isolated behind the
`AuthConfig` enum and wired into the SDK pipeline in `src/auth.rs`:

| Mode | Credential | When to use |
|------|-----------|-------------|
| SAS | `?sig=…` query token on the blob URL | Pre-signed access, no stored secret |
| Shared Key | `StorageSharedKeyPolicy` (custom pipeline policy) | Azurite, local dev, CI |
| Workload Identity | `WorkloadIdentityCredential` (federated K8s SA token) | AKS workload identity, no secret |
| Managed Identity | `ManagedIdentityCredential` (system or user-assigned) | Azure VM / AKS, no secrets |
| Service Principal | `ClientSecretCredential` (tenant/client/secret) | Entra app registration |

Selection precedence (first match wins): **SAS → Shared Key → Workload Identity →
Managed Identity → Service Principal**.

**Azurite does not support Entra ID / MSI.**  The e2e tests therefore use the
SharedKey path with Azurite's well-known development account key.  The SharedKey
HMAC-SHA256 signing is injected as a pipeline `per_try` policy — this lets us sign
requests without going through the `TokenCredential` interface (which only covers
Bearer-token / Entra credentials, not SharedKey HMAC signing).

#### 5. Concurrency

The ublk queue handler drives async SDK calls on a multi-threaded Tokio runtime.
Rather than each subsystem sizing its own Azure parallelism, all Azure traffic is
funnelled through a single process-wide gateway that owns the authoritative
bandwidth and concurrency limits:

#### Centralized I/O gateway (`src/backend/io_gateway.rs`)

Every Azure download (read) and upload (write / clear / server-side copy) is
issued from exactly one place — `AzurePageBlobBackend` — so routing its
primitives through a single, process-wide `AzureIoGateway` makes it the one
chokepoint that enforces, *per direction independently*:

1. **Bandwidth** — a byte-rate ceiling backed by a leaky bucket
   (`leaky-bucket`), one limiter per direction; `0` = unlimited.
2. **Threads / concurrency** — a single shared pool of consumer worker tasks
   drawn from by both directions; at most that many Azure requests are in flight
   at once *combined*. The budget auto-sizes to the logical CPU count, and
   either direction can use all of it when the other is idle (a dynamic split,
   not a fixed half each). Optional per-direction ceilings cap how much of the
   budget each may use.
3. **Fairness** — a **provider/consumer** model. Producers (on-demand reads,
   write-back flush, server-side copy, cache warm-up) enqueue work onto a
   priority queue (`async-priority-channel`); the workers drain it
   highest-priority-first, so background work cannot starve foreground I/O.

The priority order is **foreground read > flush > copy > warm-up**. Producers
label their traffic with a task-local `IoClass` (`with_class`); on-demand reads
default to `ForegroundRead` and writes to `Flush`. Downloads and uploads keep
separate priority queues and bandwidth limiters (the order is enforced within
each direction), but share one concurrency budget.

The per-subsystem knobs (`UBLK_FLUSH_CONCURRENCY`,
`UBLK_CACHE_WARMUP_CONCURRENCY`, `UBLK_COPY_CONCURRENCY`) now only bound how much
work each producer keeps *enqueued* (pipeline depth / memory); the authoritative
Azure thread and bandwidth limits live in the gateway (`UBLK_IO_CONCURRENCY` /
`UBLK_DOWNLOAD_CONCURRENCY` / `UBLK_UPLOAD_CONCURRENCY` /
`UBLK_DOWNLOAD_BANDWIDTH` / `UBLK_UPLOAD_BANDWIDTH`).

#### 6. Retry / back-off

The Azure SDK's built-in retry policy handles transient 429 (throttled) and 5xx
errors.  Explicit handling of 412 (ETag mismatch for optimistic concurrency) and
observable metrics remain on the hardening roadmap.

#### 7. Failure semantics

On unrecoverable errors (persistent 403, malformed response), the I/O loop
returns `EIO` to the kernel.  The kernel will surface this as an I/O error to
the filesystem or application.  The device does **not** silently eat errors.

#### 8. Read-only snapshots

A blob URL carrying `?snapshot=<timestamp>` is mounted read-only:
`AzurePageBlobBackend::with_snapshot` scopes **every** operation — including
`get_properties` / `etag` — to that immutable snapshot, and `create` / `write` /
`clear` are rejected (they `bail`). Because the snapshot's ETag is stable, the
local-disk cache's clean pages stay valid indefinitely and are reused across
restarts without revalidation churn (see §1). Snapshot mounts never take the
write lock (§10), so many readers can share one snapshot concurrently.

#### 9. Template / golden-image provisioning

A volume can be provisioned as a clone of a `templateBlobUrl` (e.g. a golden
image). `copy_template` picks the cheapest path:

* **Server-side copy** — when the source is reachable with a SAS or a mintable
  Entra token, `copy_pages_from_url` issues concurrent `Put Page From URL`
  requests so the storage service copies range-by-range directly from the
  source: **no bytes flow through this process**.
* **Streamed copy** — the SharedKey / no-SAS fallback streams
  download → upload through `source`.

Both consult the source's sparseness map (`Get Page Ranges`) and, for any chunk
lying entirely in a source **zero gap**, issue `Clear Pages` / `clear` on the
destination instead of copying. This skips the source round-trip for unwritten
free space yet still guarantees the destination reads back as zero there — safe
even on a retry against an existing same-size blob (idempotent `create` does not
re-zero an existing target).

#### 10. Cluster coordination (single-writer lock)

To stop two nodes from mounting the same page blob read-write at once,
`coordination` combines:

* an **Azure blob lease** — the authoritative, storage-level lock. While held,
  Azure rejects any `Put Page` / `Clear Pages` lacking the matching lease id
  (HTTP 412), so even a network partition cannot corrupt the blob. It is finite
  (Azure caps an explicit lease at 60s) and kept alive by a renewal loop.
* an optional **Kubernetes `coordination.k8s.io` Lease** — a *liveness* signal
  (enabled with `--coordination`). If a holder dies hard, its blob lease may
  still appear held until it expires; a peer that observes the cluster lease
  going stale (older than the recovery timeout) can **break** the dead holder's
  blob lease and take over.

Without `--coordination` only the blob lease is used (no liveness arbiter, so a
held lease is never broken). Read-only snapshot mounts (§8) never take the lock.

---

### Kubernetes CSI Driver

The same binary doubles as a Kubernetes **Container Storage Interface (CSI)**
driver (ublk + CSI are on by default, run via the `csi` subcommand). It
reuses the ublk + Page Blob stack unchanged: each PVC maps to one page blob,
attached as a ublk device and mounted as ext4.

```
   kube-apiserver
        │  PVC
        ▼
   external-provisioner ──unix──► CSI Controller (`csi --role controller`)
                                     └─ BlobBackend::create / delete  → page blob
   ─────────────────────────────────────────────────────────────────────────────
   kubelet ──unix──► CSI Node (`csi --role node`)   (DaemonSet, privileged)
                        ├─ NodePublishVolume → spawn `ublk-azblob run` → /dev/ublkbN
                        │                      → mkfs.ext4 (first use) → mount(target)
                        └─ NodeUnpublishVolume → umount → SIGINT child (flush + teardown)
```

Key decisions:

1. **One binary, two roles.** Controller and node are split by `--role` so they
   can run as a Deployment and a DaemonSet respectively, sharing all backend and
   auth code.
2. **No attach stage.** `attachRequired: false`; the node plugin attaches the
   ublk device directly in `NodePublishVolume`, so there is no
   ControllerPublish/VolumeAttachment round-trip.
3. **Volume identity.** A volume ID is `<account>/<container>/<blob>` (account
   and container names cannot contain `/`). The blob name comes from the
   configured blob-path template (default `ublk-azblob-disk/${pv.name}`), not
   necessarily the raw CSI volume name. The endpoint is driver-level config
   (env); the account and container are `StorageClass` parameters, and the
   account is encoded in the ID so `DeleteVolume` — which only gets the volume
   ID and secrets — can recover a per-volume account.
4. **Node spawns the existing `run` path.** Rather than re-implementing the
   device loop, the node plugin spawns `ublk-azblob run` as a child per volume,
   discovers the new `/dev/ublkbN` under a publish lock, and tracks the child so
   `NodeUnpublishVolume` can signal it for a clean flush + teardown. The device
   sizes itself from the existing blob, so a remount reuses the persisted data.
5. **Filesystem profiles.** `StorageClass` parameters control on-device
   formatting: `newBlobFsType` is the `mkfs` type for a freshly created blob,
   `templateBlobFsType` records the filesystem already inside a `templateBlobUrl`
   clone (so the node skips `mkfs` and mounts it directly), and
   `templateBlobMountArgsOverwrite` supplies mount options a cloned filesystem
   needs (e.g. `nouuid` / `norecovery` for a duplicated XFS UUID).
6. **Persistent host-path cache + warm-up.** The node mounts a host directory
   (e.g. `/var/lib/ublk-azblob/cache`) into the plugin pod so the local-disk
   cache (§1) survives pod restarts on that node. On mount the node can warm the
   cache in parallel so first reads hit local disk instead of Azure. Whether a
   clean cache actually survives depends on the host path being a real
   node-persistent mount — an ephemeral `DirectoryOrCreate` is lost to pod churn
   (this is exactly the e2e-only caveat that made the cache-reload test
   environment-sensitive).

The CSI protobuf is vendored at `ublk-azblob/proto/csi/csi.proto` and compiled
by `build.rs` **only** when the `csi` feature is enabled, so the default build
needs no `protoc`.

---

### The Thin SDK Trait Boundary

The Azure Rust SDK is preview (`0.x`); its API has changed in every minor
release.  The `BlobBackend` trait is the **only interface** the rest of the
codebase uses.  Rationale:

1. **Upgrade isolation** — SDK upgrades require changes only in `src/backend/azure.rs`.
2. **Testability** — `MemBackend` provides full unit-test coverage of the I/O
   path without a network or a kernel.
3. **Portability** — a future block-blob or S3 backend can be swapped in by
   implementing the same trait.

---

## More

- **[deploy/chart/README.md](deploy/chart/README.md)** — Helm chart values.
- **[examples/quickstart/](examples/quickstart/)** — the Docker Compose demo.

## License

MIT — see [LICENSE.md](LICENSE.md).
