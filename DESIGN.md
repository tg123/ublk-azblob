# ublk-azblob Design Document

## Problem Statement

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

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Kernel space                                                   │
│  ┌──────────────┐    io_uring     ┌────────────────────────┐   │
│  │  /dev/ublkbN │◄───────────────►│  ublk_drv (kernel mod) │   │
│  └──────────────┘                 └───────────┬────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                                                │ ublk cmd queue
┌───────────────────────────── Userspace ────────▼────────────────┐
│                                                                  │
│   main.rs (CLI)                                                  │
│      │                                                           │
│      ▼                                                           │
│   ublk_target.rs  ──── libublk (Rust) ──── io_uring             │
│      │                                                           │
│      │  READ  → BlobBackend::read(offset, len)                  │
│      │  WRITE → BlobBackend::write(offset, data)                │
│      │  DISCARD→BlobBackend::clear(offset, len)                 │
│      │  FLUSH → BlobBackend::flush()                            │
│      ▼                                                           │
│   BlobBackend trait  ◄────────────── isolation boundary         │
│      │                                                           │
│      ▼                                                           │
│   AzurePageBlobBackend                                           │
│      │                                                           │
│      ├─ read  → BlobClient::download(range)                     │
│      ├─ write → PageBlobClient::upload_pages(range, data)       │
│      ├─ clear → PageBlobClient::clear_pages(range)              │
│      └─ size  → BlobClient::get_properties() → content-length   │
│                                                                  │
│   Auth module                                                    │
│      ├─ MSI  → azure_identity::ManagedIdentityCredential        │
│      └─ SharedKey → StorageSharedKeyPolicy (pipeline policy)    │
│                                                                  │
│   azure_storage_blob 1.0.0 SDK ◄── pinned, isolated             │
└──────────────────────────────────────────────────────────────────┘
                          │
                          ▼ HTTPS / HTTP
              Azure Blob Storage / Azurite
```

### Why Azure Page Blob?

Page blobs are the only Azure Blob type with **512-byte-aligned random read/write**
semantics (`Put Page`, `Get Page Ranges`, `Clear Pages`).  This is the same
primitive used for Azure VM VHDs, making it a natural fit for a block device
abstraction.  Block blobs require chunked read-modify-write (the s3backer approach)
and are better suited for a Phase 3 "block-blob backend" option.

---

## Key Design Decisions

### 1. `BlobBackend` trait boundary

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

### 1b. Pluggable front-ends: ublk and NBD

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

### 2. 512-byte alignment

All offsets and lengths are validated to be multiples of 512 bytes.  Azure Page
Blob requires this; the block layer enforces it for us at the ublk driver level.
Misaligned requests return an immediate error instead of silently corrupting data.

### 3. Write-through (Phase 1)

`flush()` is a no-op.  Every `upload_pages` call is immediately durable from
Azure's perspective once the HTTP 201 response is received.  Write-back caching
with explicit flush on `REQ_FUA` / `REQ_FLUSH` is a Phase 2 optimization.

### 4. Authentication

| Mode | Credential | When to use |
|------|-----------|-------------|
| Managed Identity (system) | `ManagedIdentityCredential::new(None)` | Azure VM / AKS, no secrets |
| Managed Identity (user-assigned) | `ManagedIdentityCredentialOptions { user_assigned_id: Some(...) }` | Multiple identities on same host |
| Shared Key | `StorageSharedKeyPolicy` (custom pipeline policy) | Azurite, local dev, CI |

**Azurite does not support Entra ID / MSI.**  The e2e tests therefore use the
SharedKey path with Azurite's well-known development account key.  The `SharedKeyPolicy`
is injected into the SDK's `ClientOptions::per_try_policies` — this lets us sign
requests with HMAC-SHA256 without going through the `TokenCredential` interface
(which only covers Bearer-token / Entra ID credentials, not SharedKey HMAC signing).

### 5. Concurrency

Phase 1: single queue, single thread.  The libublk queue handler calls
`tokio::runtime::Handle::block_on()` to drive async SDK calls.

Phase 2: spawn one Tokio task per ublk queue, use `tokio::spawn` + channel for
back-pressure.  Map io_uring depth → parallel REST calls.

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

### 6. Retry / back-off

Phase 1: the Azure SDK's built-in retry policy handles transient 429 (throttled)
and 5xx errors.  Phase 3 will add explicit handling of 412 (ETag mismatch for
optimistic concurrency) and observable metrics.

### 7. Failure semantics

On unrecoverable errors (persistent 403, malformed response), the I/O loop
returns `EIO` to the kernel.  The kernel will surface this as an I/O error to
the filesystem or application.  The device does **not** silently eat errors.

---

## Kubernetes CSI Driver

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

The CSI protobuf is vendored at `ublk-azblob/proto/csi/csi.proto` and compiled
by `build.rs` **only** when the `csi` feature is enabled, so the default build
needs no `protoc`.

---

## The Thin SDK Trait Boundary

The Azure Rust SDK is preview (`0.x`); its API has changed in every minor
release.  The `BlobBackend` trait is the **only interface** the rest of the
codebase uses.  Rationale:

1. **Upgrade isolation** — SDK upgrades require changes only in `src/backend/azure.rs`.
2. **Testability** — `MemBackend` provides full unit-test coverage of the I/O
   path without a network or a kernel.
3. **Portability** — a future block-blob or S3 backend can be swapped in by
   implementing the same trait.

---

## Phased Plan

### Phase 0 — Read-only spike *(done conceptually)*
Prove range reads work: `nbdkit curl` plugin + SAS URL → confirmed end-to-end.

### Phase 1 — MVP *(this PR)*
- ✅ `BlobBackend` trait + `AzurePageBlobBackend` + `MemBackend`
- ✅ SharedKey auth (Azurite) + MSI auth wiring
- ✅ ublk target (real impl is the default `ublk` feature; `--no-default-features` stub otherwise)
- ✅ Full mount-based e2e test against Azurite (ext4 on `/dev/ublkbN`)
- ✅ CI: fmt + clippy + unit tests + mount e2e pipeline

### Phase 2 — Performance
- ✅ Read cache (LRU): the local-disk cache supports a configurable byte budget
  (`--cache-max-bytes`) and evicts least-recently-used **clean** pages (by
  hole-punching the sparse `.dat` file) once the budget is exceeded; dirty pages
  are never evicted. The budget is shared across every process using the same
  `--cache-dir` via a `flock`-coordinated, crash-safe `.cache-budget` file, so a
  single noisy volume cannot fill a shared CSI node's cache disk. Each process
  evicts only its own clean pages and never touches a peer's cache files. The
  same LRU bookkeeping (`backend::cache_lru::Lru`) bounds the **in-memory**
  write-back buffer too: it doubles as a read cache and evicts least-recently-
  used clean pages once the resident set exceeds `--max-cached-pages` (dirty
  pages stay pinned).
- ⚠️ Cross-process clean-page sharing (`--cache-share-pages`) — **implemented but
  currently disabled in the shipped binary** (the flag is accepted but ignored;
  every cache is single-process). Design, retained for a later iteration:
  processes caching
  the same blob in one `--cache-dir` serve each other's clean pages off local
  disk via a `flock`-coordinated `.cache-index` (sibling to `.cache-budget`)
  mapping `(blob, page) → owner .dat + offset`. A read miss consults the index
  and copies a live peer's clean page read-only instead of fetching the blob,
  falling back to the blob if the page is absent/stale/peer-dead. Shared reads
  are not double-counted (one on-disk owner per resident page), and dead-PID
  entries are pruned exactly as in the budget file.
- ✅ Copy-on-write writes: before mutating a page a peer owns, the writer
  withdraws it from the index and writes into its **own** `.dat`, marking it
  dirty locally — preserving the single-writer-per-file invariant. Dirty pages
  are never evicted and never served cross-process; once flushed clean the new
  owner re-publishes the page so later peer reads resolve to it.
- ✅ Persistent local-disk cache (`FileCacheBackend`), composable into a
  multi-level cache (memory → local disk → blob) with crash-recoverable dirty
  pages that are flushed to the blob on restart
- Write coalescing (merge adjacent pages before `upload_pages`)
- Multiple queues / true async (one Tokio task per queue)
- FLUSH / FUA handling (drain write buffer before responding)
- ✅ `list_page_ranges` sparse map to skip zero reads — warm-up queries
  `Get Page Ranges` (`?comp=pagelist`) and skips downloading pages that fall in
  a zero gap; all-zero pages are also left as holes in the local `.dat`

### Phase 3 — Hardening
- MSI live testing on Azure VM / AKS
- Retry/back-off with exponential jitter; 412 / 429 handling
- Prometheus metrics (IOPS, latency, error rate)
- Crash-consistency test suite (power-failure simulation)
- ✅ Node-local restart safety for the CSI node plugin: ublk user-recovery
  (`UBLK_F_USER_RECOVERY`) keeps `/dev/ublkbN` alive while the plugin restarts
  (crash / OOM / DaemonSet upgrade); a per-volume state file under
  `CSI_STATE_DIR` lets the new plugin re-attach (`run --recover`) and resume I/O
  without disturbing existing mounts (NBD reconnects `nbd-client` instead)
- Optional block-blob backend (chunked, cheaper storage, slower random write)
- Packaging: container image, systemd unit, cloud-init example

---

## CI: ublk Kernel Path

GitHub-hosted runners do **not** load `ublk_drv` by default, but the module
ships in `linux-modules-extra-$(uname -r)` and can be loaded with `modprobe`.
The CI workflow therefore:

1. **Always runs** `cargo fmt --check`, `cargo clippy`, and `cargo test` (unit
   tests against `MemBackend`).
2. **Runs the full mount e2e** on `ubuntu-22.04`: it loads `ublk_drv`, starts
   Azurite, builds the default (ublk) binary, then mounts an ext4 filesystem on
   `/dev/ublkbN`, writes random files, forces a flush via `SIGUSR1`, unmounts,
   tears the device down, remounts over the same page blob, and verifies every
   file's SHA-256 checksum.

`ubuntu-24.04` is intentionally avoided: its azure kernel currently Oopses in
`ublk_drv` ([actions/runner-images#14175](https://github.com/actions/runner-images/issues/14175)).

---

## Risks

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Azure SDK 0.x breaking change | High | Thin `BlobBackend` trait; pin exact versions |
| ublk kernel requirement (≥6.0) | Medium | Clear docs; CI loads `ublk_drv` and runs the mount e2e |
| Page blob cost / latency vs block blob | Medium | Phase 3: optional block-blob backend |
| Azurite Page Blob parity gaps | Low | CI catches regressions; use real Azure for perf tests |
| SharedKey auth complexity | Low | Implemented and tested in e2e; MSI for production |
