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
cargo run -p ublk-azblob -- \
  --endpoint http://127.0.0.1:10000/devstoreaccount1 \
  --account devstoreaccount1 \
  --account-key "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==" \
  --container mycontainer \
  --blob myblob \
  test --size 4096
```

### Benchmark the backend (throughput / IOPS / latency)

```bash
# Provision a 64 MiB blob and run all four phases (seq write/read, rand write/read)
cargo run --release -p ublk-azblob -- \
  --endpoint http://127.0.0.1:10000/devstoreaccount1 \
  --account devstoreaccount1 \
  --account-key "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==" \
  --container mycontainer \
  --blob mybench \
  bench --create
```

The `bench` subcommand issues a fixed number of fixed-size operations against
the `BlobBackend` using a configurable number of concurrent workers (mirroring a
ublk queue depth) and reports throughput (MiB/s), IOPS, and latency percentiles
(min / avg / p50 / p95 / p99 / max) per phase. It runs against Azurite, real
Azure, or the in-memory backend used in tests.

| Flag | Default | Notes |
|------|---------|-------|
| `--size` | `67108864` (64 MiB) | Benchmark blob size (multiple of 512) |
| `--block-size` | `4096` | I/O size per operation (multiple of 512) |
| `--count` | `1024` | Operations issued per phase |
| `--concurrency` | `16` | Concurrent in-flight operations (queue depth) |
| `--workload` | `all` | `seq`, `rand`, or `all` |
| `--create` | _off_ | Provision/overwrite the blob before benchmarking |

### Run as a block device (requires root + ublk_drv + `--features ublk`)

```bash
# System-assigned Managed Identity (recommended on Azure VMs / AKS)
sudo ./target/release/ublk-azblob \
  --account mystorageaccount \
  --container mydisks \
  --blob myvm.vhd \
  --msi \
  run --size 10737418240

# User-assigned Managed Identity by client ID
sudo ./target/release/ublk-azblob \
  --account mystorageaccount \
  --container mydisks \
  --blob myvm.vhd \
  --msi-client-id 00000000-0000-0000-0000-000000000000 \
  run --size 10737418240

# Account key (local dev / Azurite)
sudo ./target/release/ublk-azblob \
  --endpoint http://127.0.0.1:10000/devstoreaccount1 \
  --account devstoreaccount1 \
  --account-key "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==" \
  --container mycontainer \
  --blob myblob.vhd \
  run --size 4194304
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
| `--account` | `AZURE_STORAGE_ACCOUNT` |
| `--endpoint` | `AZURE_STORAGE_ENDPOINT` |
| `--account-key` | `AZURE_STORAGE_KEY` |
| `--container` | `AZURE_STORAGE_CONTAINER` |
| `--blob` | `AZURE_STORAGE_BLOB` |

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
#    The `runner` service builds with --features ublk and runs the Rust test;
#    Azurite is started automatically as its dependency.
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

The same fio jobs (sequential/random read/write) run against each target and the
script prints a side-by-side comparison of throughput (MiB/s), IOPS, and mean
latency.  Like the e2e test, the Rust build, `fio`, and Azurite all run inside
docker compose:

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
The benchmark is tunable via environment variables (block size, queue depth,
runtime, direct vs. buffered I/O, etc.) — see the header of
[tests/bench/bench.sh](tests/bench/bench.sh) for the full list, e.g.:

```bash
FIO_BS=64k FIO_IODEPTH=32 FIO_RUNTIME=30 \
  docker compose -f tests/bench/docker-compose.yml up \
    --build --abort-on-container-exit --exit-code-from runner
```

In CI the benchmark runs on demand via the **`bench`** workflow
(`workflow_dispatch` in the Actions tab); it is not run on every push/PR because
fio benchmarks take minutes.  Results are attached as a `bench-results` artifact
and rendered into the run's job summary.

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
- `cargo clippy -- -D warnings` (with and without `--features ublk`)
- `cargo test` (unit tests, `MemBackend`)
- the full mount-based e2e (`/dev/ublkbN` + ext4 ↔ Azurite)

The e2e job runs on `ubuntu-22.04`, loads `ublk_drv` from
`linux-modules-extra`, and runs the mount/remount/checksum cycle as root.
(`ubuntu-24.04` is avoided because its azure kernel currently Oopses in
`ublk_drv` — see [actions/runner-images#14175](https://github.com/actions/runner-images/issues/14175).)

A separate **`bench`** workflow (manual `workflow_dispatch`) runs the fio
benchmark comparing the ublk-azblob device against a raw local disk, on the same
`ubuntu-22.04` runner.

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
│   │   ├── ublk_target.rs      # ublk device I/O loop (gated on --features ublk)
│   │   └── backend/
│   │       ├── mod.rs          # BlobBackend trait (SDK isolation boundary)
│   │       ├── azure.rs        # AzurePageBlobBackend (real SDK impl)
│   │       └── mem.rs          # MemBackend (in-memory, for unit tests)
│   └── tests/
│       └── mount_e2e.rs        # full mount → write → flush → remount → verify
├── tests/
│   ├── e2e/
│   │   ├── docker-compose.yml  # Azurite + test runner for the e2e test
│   │   └── Dockerfile          # build + test runner image (rust + e2fsprogs)
│   └── bench/
│       ├── bench.sh            # fio benchmark: ublk-azblob vs. raw local disk
│       ├── docker-compose.yml  # Azurite + benchmark runner
│       └── Dockerfile          # build + benchmark runner image (rust + fio + jq)
└── LICENSE.md                  # MIT license
```
