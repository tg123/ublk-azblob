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

# Build with the Kubernetes CSI driver
# (needs `protoc` plus the protobuf well-known types, e.g. apt
#  `protobuf-compiler libprotobuf-dev`; combine with `ublk`)
cargo build --release -p ublk-azblob --features "ublk csi"
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
cargo run --release --features bench -p ublk-azblob -- \
  --endpoint http://127.0.0.1:10000/devstoreaccount1 \
  --account devstoreaccount1 \
  --account-key "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==" \
  --container mycontainer \
  --blob mybench \
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

## NBD compatibility mode (no `ublk_drv` required)

For kernels or platforms **without** `ublk_drv` (older kernels, containers that
can't load the module, etc.), `ublk-azblob` can instead expose the blob over the
**NBD** (Network Block Device) protocol with the `--nbd <host:port>` option.
This mode needs no special kernel module — only a TCP socket and the standard
NBD client — and does **not** require the `ublk` Cargo feature.

```bash
# Start the NBD server (works on any kernel; no --features ublk needed)
./target/release/ublk-azblob \
  --endpoint http://127.0.0.1:10000/devstoreaccount1 \
  --account devstoreaccount1 \
  --account-key "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==" \
  --container mycontainer \
  --blob myblob.vhd \
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

## Environment variables

All CLI flags have environment-variable equivalents:

| Flag | Env var |
|------|---------|
| `--account` | `AZURE_STORAGE_ACCOUNT` |
| `--endpoint` | `AZURE_STORAGE_ENDPOINT` |
| `--account-key` | `AZURE_STORAGE_KEY` |
| `--container` | `AZURE_STORAGE_CONTAINER` |
| `--blob` | `AZURE_STORAGE_BLOB` |
| `--cache-dir` | `UBLK_CACHE_DIR` |
| `--cache-page-size` | `UBLK_CACHE_PAGE_SIZE` |
| `--nbd` | `NBD_LISTEN` |

---

## Multi-level cache (memory → local disk → blob)

`ublk-azblob` can stack a persistent **local-disk cache** between the in-memory
write-back buffer and Azure, giving a three-level cache:

```
BufferedBackend (memory) ──► FileCacheBackend (local disk) ──► AzurePageBlobBackend (blob)
```

Enable it by pointing `--cache-dir` at a directory on a local disk:

```bash
sudo ./target/release/ublk-azblob \
  --account mystorageaccount \
  --container mydisks \
  --blob myvm.vhd \
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

The same fio jobs run against each target and the script prints a side-by-side
comparison of throughput (MiB/s), IOPS, and mean latency, with each ublk-azblob
result also expressed as a **percentage of the raw-local-disk baseline** (the
`vs local` column). The jobs are grouped into phases:

* **Phase 1 — Raw block performance:** the four base patterns (sequential and
  random read/write) plus sweeps over block size (`4k…1M`), queue depth
  (`1…128`), and read/write mix (`100/0`, `70/30`, `50/50`).
* **Phase 2 — Cache behaviour:** a cold-cache vs. warm-cache buffered read so the
  read-cache speed-up is visible.
* **Phase 4 — Scalability:** the random-read workload at increasing thread
  (`numjobs`) counts.

(Phase 3 — backend Azure Blob GET/PUT/flush latency — is covered by the
`bench` subcommand above, which measures the `BlobBackend` directly.)

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

In CI the benchmark runs on every pull request and on demand via the **`bench`**
workflow (`workflow_dispatch` in the Actions tab, with tunable inputs).  Results
are attached as a `bench-results` artifact and rendered into the run's job
summary.

---

## Kubernetes (CSI driver)

`ublk-azblob` ships an in-tree **Container Storage Interface (CSI)** driver so
each `PersistentVolumeClaim` is provisioned as one Azure Page Blob and exposed
to pods as an ext4 filesystem on a ublk block device. The driver is the same
binary, built with `--features "ublk csi"` and run via the `csi` subcommand:

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
- `cargo clippy --all-features -- -D warnings`
- `cargo test --all-features` (unit tests, `MemBackend`)
- the `e2e` workflow: one job that builds the shipped image once and runs the
  mount (`/dev/ublkbN` + ext4 ↔ Azurite), NBD, and Kubernetes PVC e2e against a
  k3s cluster from `tests/e2e/docker-compose.yml`.

The e2e job runs on `ubuntu-22.04`, loads `ublk_drv` from
`linux-modules-extra`, and runs the mount/remount/checksum cycle as root.
(`ubuntu-24.04` is avoided because its azure kernel currently Oopses in
`ublk_drv` — see [actions/runner-images#14175](https://github.com/actions/runner-images/issues/14175).)

A separate **`bench`** workflow (on pull requests and manual `workflow_dispatch`)
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
│   │   ├── ublk_target.rs      # ublk device I/O loop (gated on --features ublk)
│   │   ├── csi/                # Kubernetes CSI driver (gated on --features csi)
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
│   ├── Dockerfile              # CSI driver image (--features "ublk csi")
│   ├── chart/                  # Helm chart (CSIDriver, RBAC, controller, node, StorageClass)
│   └── example/                # sample PVC + pod
├── tests/
│   ├── e2e/
│   │   ├── docker-compose.yml  # Azurite + k3s + runner for the whole e2e suite
│   │   ├── Dockerfile          # e2e runner image (rust + docker/kubectl/helm)
│   │   └── k8s/                # k8s manifests for the PVC e2e (helm values, writer/reader jobs)
│   └── bench/
│       ├── bench.sh            # fio benchmark: ublk-azblob vs. raw local disk
│       ├── docker-compose.yml  # Azurite + benchmark runner
│       └── Dockerfile          # build + benchmark runner image (rust + fio + jq)
└── LICENSE.md                  # MIT license
```
