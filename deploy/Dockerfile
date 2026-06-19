# Container image for the ublk-azblob CSI driver.
#
# Builds the binary with both the `ublk` (real block device) and `csi`
# (Kubernetes Container Storage Interface) features.  These are on by default,
# so a plain `cargo build --release` produces the full node plugin.  It ships on
# a slim Debian runtime that carries the userspace tools the node plugin shells
# out to (`mkfs.ext4`, `mount`/`umount`, `blkid`).
#
# Multi-arch (linux/amd64 + linux/arm64) is built by *cross-compiling* on the
# native build platform rather than emulating the target under QEMU, which is
# dramatically faster (native Rust compile vs. emulated). The builder stage is
# pinned to `$BUILDPLATFORM`; only the small runtime stage is pulled for the
# target platform.
#
# Build from the repository root:
#   docker build -f deploy/Dockerfile -t ghcr.io/tg123/ublk-azblob:latest .

# ── Builder ───────────────────────────────────────────────────────────────────
# Pin the builder to the native build platform so the Rust toolchain runs on
# real hardware; the target architecture is reached via cross-compilation.
FROM --platform=$BUILDPLATFORM rust:1.96-bookworm AS builder

# Provided automatically by BuildKit.
ARG TARGETARCH
ARG TARGETPLATFORM
ARG BUILDPLATFORM

# Install architecture-independent build dependencies:
# - protobuf-compiler, libprotobuf-dev: protoc and well-known types for CSI gRPC
# - libclang-dev, clang: libclang for bindgen (used by libublk-rs-sys)
# - cmake: native build of the aws-lc-sys / ring crypto crates
# - pkg-config: build dependency probing
RUN apt-get update && apt-get install -y \
        protobuf-compiler \
        libprotobuf-dev \
        libclang-dev \
        clang \
        cmake \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Resolve the Rust target triple + cross toolchain for $TARGETARCH and, when the
# target differs from the build host, install the matching GCC cross compiler.
# The resulting environment (target triple, linker, CC/CXX/AR, and the bindgen
# clang sysroot args) is written to /cross-env.sh for the build step to source.
RUN <<'EOF'
set -eux
case "$TARGETARCH" in
    amd64) triple=x86_64-unknown-linux-gnu;  prefix=x86_64-linux-gnu ;;
    arm64) triple=aarch64-unknown-linux-gnu; prefix=aarch64-linux-gnu ;;
    *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;;
esac

rustup target add "$triple"

# Upper-case underscore form used by cargo/cc/bindgen env var names.
triple_us=$(echo "$triple" | tr '-' '_')
triple_uc=$(echo "$triple_us" | tr '[:lower:]' '[:upper:]')

{
    echo "export CARGO_BUILD_TARGET=$triple"
    echo "export TARGET_TRIPLE=$triple"
} > /cross-env.sh

buildarch=$(dpkg --print-architecture)
if [ "$TARGETARCH" != "$buildarch" ]; then
    # Cross build: pull the GCC cross toolchain (also drags in the target
    # libc/linux UAPI headers under /usr/$prefix) and point cargo, the `cc`
    # crate, and bindgen at it.
    apt-get update
    apt-get install -y "gcc-$prefix" "g++-$prefix"
    rm -rf /var/lib/apt/lists/*

    {
        echo "export CARGO_TARGET_${triple_uc}_LINKER=${prefix}-gcc"
        echo "export CC_${triple_us}=${prefix}-gcc"
        echo "export CXX_${triple_us}=${prefix}-g++"
        echo "export AR_${triple_us}=${prefix}-ar"
        echo "export BINDGEN_EXTRA_CLANG_ARGS_${triple_us}=\"--sysroot=/usr/${prefix} --target=${triple} -I/usr/${prefix}/include\""
    } >> /cross-env.sh
fi

cat /cross-env.sh
EOF

# Use BuildKit cache mounts for faster builds:
# - Cache cargo registry (crates.io index and downloads)
# - Cache cargo build artifacts in /build/target
# - Mount source code instead of copying (avoids layer invalidation)
RUN --mount=type=bind,source=.,target=/src,readonly \
    --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    . /cross-env.sh && \
    cd /src && \
    CARGO_TARGET_DIR=/build/target cargo build --release -p ublk-azblob && \
    cp "/build/target/${TARGET_TRIPLE}/release/ublk-azblob" /ublk-azblob

# ── Runtime ───────────────────────────────────────────────────────────────────
# Pulled for the *target* platform; carries the cross-built target-arch binary.
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
