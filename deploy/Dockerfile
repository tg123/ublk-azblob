# Container image for the ublk-azblob CSI driver.
#
# Builds the binary with both the `ublk` (real block device) and `csi`
# (Kubernetes Container Storage Interface) features, and ships it on a slim
# runtime that carries the userspace tools the node plugin shells out to
# (`mkfs.ext4`, `mount`/`umount`, `blkid`).
#
# Build from the repository root:
#   docker build -f deploy/Dockerfile -t ghcr.io/tg123/ublk-azblob:latest .

# ── Builder ───────────────────────────────────────────────────────────────────
# Pinned to bookworm so the produced binary links against the same glibc as the
# debian:bookworm-slim runtime below (plain rust:1.96-slim tracks a newer Debian
# and yields `GLIBC_2.x not found` at runtime).
FROM rust:1.96-slim-bookworm AS builder

# - protobuf-compiler : `protoc`, required by the CSI build script (build.rs)
# - libprotobuf-dev   : well-known types (google/protobuf/*.proto) imported by csi.proto
# - libclang-dev      : bindgen, used by the libublk-rs-sys build script
# - pkg-config        : transitive build dependency probing
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        protobuf-compiler \
        libprotobuf-dev \
        libclang-dev \
        pkg-config \
        ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN cargo build --release --features "ublk csi" -p ublk-azblob

# ── Runtime ───────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

# - e2fsprogs : mkfs.ext4 + blkid
# - util-linux: mount / umount
# (the ublk_drv module is loaded on the host, so no kmod/modprobe is needed here)
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        e2fsprogs \
        util-linux \
        ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/ublk-azblob /usr/local/bin/ublk-azblob

ENTRYPOINT ["/usr/local/bin/ublk-azblob"]
