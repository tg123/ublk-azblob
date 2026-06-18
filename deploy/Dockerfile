# Container image for the ublk-azblob CSI driver.
#
# Builds the binary with both the `ublk` (real block device) and `csi`
# (Kubernetes Container Storage Interface) features, and ships it on a slim
# Debian runtime that carries the userspace tools the node plugin shells out to
# (`mkfs.ext4`, `mount`/`umount`, `blkid`).
#
# Build from the repository root:
#   docker build -f deploy/Dockerfile -t ghcr.io/tg123/ublk-azblob:latest .

# ── Builder ───────────────────────────────────────────────────────────────────
# Use Debian-based Rust image for better bindgen compatibility and faster builds with cache
FROM rust:1.96-bookworm AS builder

# Install build dependencies
# - protobuf-compiler, libprotobuf-dev: protoc and well-known types for CSI gRPC
# - libclang-dev: libclang for bindgen (used by libublk-rs-sys)
# - pkg-config: build dependency probing
RUN apt-get update && apt-get install -y \
        protobuf-compiler \
        libprotobuf-dev \
        libclang-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Use BuildKit cache mounts for faster builds:
# - Cache cargo registry (crates.io index and downloads)
# - Cache cargo build artifacts in /build/target
# - Mount source code instead of copying (avoids layer invalidation)
RUN --mount=type=bind,source=.,target=/src,readonly \
    --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cd /src && \
    CARGO_TARGET_DIR=/build/target cargo build --release --features "ublk csi" -p ublk-azblob && \
    cp /build/target/release/ublk-azblob /ublk-azblob

# ── Runtime ───────────────────────────────────────────────────────────────────
# Use Debian slim runtime to match builder's glibc
FROM debian:bookworm-slim

# - e2fsprogs       : mkfs.ext4 + blkid
# - util-linux      : mount / umount / blockdev
# - ca-certificates : TLS roots for talking to Azure Storage / the Kubernetes API
# - nbd-client      : NBD client for NBD mode (compatibility with kernels without ublk_drv)
# (the ublk_drv module is loaded on the host, so no kmod/modprobe is needed here)
RUN apt-get update && apt-get install -y \
        e2fsprogs \
        util-linux \
        ca-certificates \
        nbd-client \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /ublk-azblob /usr/local/bin/ublk-azblob

ENTRYPOINT ["/usr/local/bin/ublk-azblob"]
