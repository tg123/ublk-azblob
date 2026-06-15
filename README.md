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

# Build (no ublk feature — works on any kernel)
cargo build --release -p ublk-azblob

# Build with real ublk device support (requires Linux ≥6.0 + ublk_drv)
cargo build --release -p ublk-azblob --features ublk
```

---

## Usage

### Run a smoke test against Azurite (local dev)

```bash
# Start Azurite
docker run -d -p 10000:10000 mcr.microsoft.com/azure-storage/azurite \
  azurite-blob --blobHost 0.0.0.0

# Run the built-in smoke test (create → write → read-back → clear → zero-verify)
cargo run -p ublk-azblob -- test \
  --storage-endpoint http://127.0.0.1:10000/devstoreaccount1 \
  --storage-account devstoreaccount1 \
  --storage-key "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==" \
  --container mycontainer \
  --blob myblob \
  --size 4096
```

### Run as a block device (requires root + ublk_drv + `--features ublk`)

```bash
# System-assigned Managed Identity (recommended on Azure VMs / AKS)
sudo ./target/release/ublk-azblob run \
  --storage-account mystorageaccount \
  --container mydisks \
  --blob myvm.vhd \
  --size 10737418240 \
  --msi

# User-assigned Managed Identity by client ID
sudo ./target/release/ublk-azblob run \
  --storage-account mystorageaccount \
  --container mydisks \
  --blob myvm.vhd \
  --size 10737418240 \
  --msi \
  --msi-client-id 00000000-0000-0000-0000-000000000000

# Account key (local dev / Azurite)
sudo ./target/release/ublk-azblob run \
  --storage-endpoint http://127.0.0.1:10000/devstoreaccount1 \
  --storage-account devstoreaccount1 \
  --storage-key "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==" \
  --container mycontainer \
  --blob myblob.vhd \
  --size 4194304
```

After launch, a `/dev/ublkbN` device appears and can be used like any block device:

```bash
sudo mkfs.ext4 /dev/ublkb0
sudo mount /dev/ublkb0 /mnt/azblob
```

---

## Environment variables

All CLI flags have environment-variable equivalents:

| Flag | Env var |
|------|---------|
| `--storage-account` | `AZURE_STORAGE_ACCOUNT` |
| `--storage-endpoint` | `AZURE_STORAGE_ENDPOINT` |
| `--storage-key` | `AZURE_STORAGE_KEY` |
| `--container` | `AZURE_STORAGE_CONTAINER` |
| `--blob` | `AZURE_STORAGE_BLOB` |

---

## Auth modes

| Mode | Flag | Notes |
|------|------|-------|
| Managed Identity (system) | `--msi` | Recommended on Azure VMs; no secrets on disk |
| Managed Identity (user) | `--msi --msi-client-id <id>` | Multiple identities per host |
| Shared Key | `--storage-key <key>` | Local dev, CI, Azurite |

> **Note:** The Azure Rust SDK (`azure_identity`, `azure_storage_blob`) is
> **0.x / preview** — API changes between minor releases are expected. Exact
> dependency versions are pinned in `Cargo.toml` for reproducibility. See
> [DESIGN.md](DESIGN.md#the-thin-sdk-trait-boundary) for the isolation strategy.

---

## Running the e2e test locally

The docker-compose pipeline spins up Azurite and a test-runner that builds and
exercises the binary end-to-end (no live Azure account needed):

```bash
docker compose -f tests/e2e/docker-compose.yml \
  up --build --abort-on-container-exit --exit-code-from test-runner
```

See [tests/e2e/](tests/e2e/) for the full setup.

---

## Running unit tests

```bash
cargo test -p ublk-azblob
```

Unit tests run against `MemBackend` — no network, no kernel required.

---

## CI

GitHub Actions runs on every push and pull request:
- `cargo fmt --check`
- `cargo clippy -- -D warnings`
- `cargo test` (unit tests, `MemBackend`)
- docker-compose e2e pipeline (Rust binary ↔ Azurite)

The full `/dev/ublkbN` mount path is **skipped on GitHub-hosted runners**
(no `ublk_drv`, insufficient privileges) and is documented as opt-in for
self-hosted runners with a ≥6.0 kernel.

---

## Project structure

```
ublk-azblob/
├── Cargo.toml                  # workspace root
├── DESIGN.md                   # architecture & phased plan
├── README.md                   # this file
├── ublk-azblob/
│   ├── Cargo.toml              # pinned dependencies
│   └── src/
│       ├── main.rs             # CLI entry point (clap)
│       ├── auth.rs             # MSI + SharedKey credential factory
│       ├── ublk_target.rs      # ublk device I/O loop (gated on --features ublk)
│       └── backend/
│           ├── mod.rs          # BlobBackend trait (SDK isolation boundary)
│           ├── azure.rs        # AzurePageBlobBackend (real SDK impl)
│           └── mem.rs          # MemBackend (in-memory, for unit tests)
└── tests/
    └── e2e/
        ├── docker-compose.yml  # Azurite + test-runner
        ├── Dockerfile          # builds & runs the Rust binary
        └── run_e2e.sh          # convenience wrapper
```
