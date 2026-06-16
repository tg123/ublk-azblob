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
MemBackend                                ←  in-memory, no network, for unit tests
```

A future SDK upgrade only requires modifying `src/backend/azure.rs`.

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

### 6. Retry / back-off

Phase 1: the Azure SDK's built-in retry policy handles transient 429 (throttled)
and 5xx errors.  Phase 3 will add explicit handling of 412 (ETag mismatch for
optimistic concurrency) and observable metrics.

### 7. Failure semantics

On unrecoverable errors (persistent 403, malformed response), the I/O loop
returns `EIO` to the kernel.  The kernel will surface this as an I/O error to
the filesystem or application.  The device does **not** silently eat errors.

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
- ✅ ublk target (real impl gated behind `--features ublk`; stub otherwise)
- ✅ Full mount-based e2e test against Azurite (ext4 on `/dev/ublkbN`)
- ✅ CI: fmt + clippy + unit tests + mount e2e pipeline

### Phase 2 — Performance
- Read cache (LRU, configurable size)
- Write coalescing (merge adjacent pages before `upload_pages`)
- Multiple queues / true async (one Tokio task per queue)
- FLUSH / FUA handling (drain write buffer before responding)
- `list_page_ranges` sparse map to skip zero reads

### Phase 3 — Hardening
- MSI live testing on Azure VM / AKS
- Retry/back-off with exponential jitter; 412 / 429 handling
- Prometheus metrics (IOPS, latency, error rate)
- Crash-consistency test suite (power-failure simulation)
- Optional block-blob backend (chunked, cheaper storage, slower random write)
- Packaging: container image, systemd unit, cloud-init example

---

## CI: ublk Kernel Path

GitHub-hosted runners do **not** load `ublk_drv` by default, but the module
ships in `linux-modules-extra-$(uname -r)` and can be loaded with `modprobe`.
The CI workflow therefore:

1. **Always runs** `cargo fmt --check`, `cargo clippy` (with and without
   `--features ublk`), and `cargo test` (unit tests against `MemBackend`).
2. **Runs the full mount e2e** on `ubuntu-22.04`: it loads `ublk_drv`, starts
   Azurite, builds with `--features ublk`, then mounts an ext4 filesystem on
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
