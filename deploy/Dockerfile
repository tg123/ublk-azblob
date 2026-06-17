# Container image for the ublk-azblob CSI driver.
#
# Builds the binary with both the `ublk` (real block device) and `csi`
# (Kubernetes Container Storage Interface) features, and ships it on a tiny
# Alpine runtime that carries the userspace tools the node plugin shells out to
# (`mkfs.ext4`, `mount`/`umount`, `blkid`).
#
# Build from the repository root:
#   docker build -f deploy/Dockerfile -t ghcr.io/tg123/ublk-azblob:latest .

# ── Builder ───────────────────────────────────────────────────────────────────
# Alpine's Rust toolchain targets x86_64-unknown-linux-musl (and aarch64 on
# arm64) by default, so a plain `cargo build` already produces a fully static,
# self-contained binary — no glibc, so it runs on busybox/Alpine/scratch with no
# `GLIBC_2.x not found` surprises.
FROM rust:1.96-alpine AS builder

# - musl-dev          : C runtime/headers for the `ring` (crypto) C build
# - protoc, protobuf-dev : `protoc` and the well-known types
#                       (google/protobuf/*.proto) imported by csi.proto (CSI build.rs)
# - clang, clang-dev, llvm-libunwind : libclang for bindgen build script
# - linux-headers     : asm/ioctl.h, linux/blkzoned.h etc. that bindgen parses
# - pkgconfig         : transitive build dependency probing
RUN apk add --no-cache \
        musl-dev \
        protoc \
        protobuf-dev \
        clang \
        clang-dev \
        llvm-libunwind \
        linux-headers \
        pkgconfig

# Set libclang path for bindgen
ENV LIBCLANG_PATH=/usr/lib

WORKDIR /src
COPY . .

RUN cargo build --release --features "ublk csi" -p ublk-azblob

# ── Runtime ───────────────────────────────────────────────────────────────────
FROM alpine:3.21

# - e2fsprogs       : mkfs.ext4 + blkid
# - util-linux      : mount / umount
# - ca-certificates : TLS roots for talking to Azure Storage / the Kubernetes API
# (the ublk_drv module is loaded on the host, so no kmod/modprobe is needed here)
RUN apk add --no-cache \
        e2fsprogs \
        util-linux \
        ca-certificates

COPY --from=builder /src/target/release/ublk-azblob /usr/local/bin/ublk-azblob

ENTRYPOINT ["/usr/local/bin/ublk-azblob"]
