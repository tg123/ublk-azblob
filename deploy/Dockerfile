# Container image for the ublk-azblob CSI driver.
#
# Builds the binary with both the `ublk` (real block device) and `csi`
# (Kubernetes Container Storage Interface) features.  These are on by default,
# so a plain `cargo build --release` produces the full node plugin.
#
# The binary is statically linked against **musl libc** (no glibc, no shared
# objects) — the same fully-static artifact published on GitHub Releases — so it
# carries no dynamic-linker dependency on the runtime image.  It still ships on a
# slim Debian runtime because the node plugin shells out to userspace tools
# (`mkfs.ext4`, `mount`/`umount`, `blkid`, `nbd-client`, …).
#
# The builder runs on the *target* platform (emulated under QEMU for a foreign
# arch by `docker buildx`).  This keeps the toolchain simple: the target-arch
# `musl-gcc` and a native `clang`/`protoc` are all that bindgen, the `ring`
# crypto C code, and the CSI proto compiler need — no cross sysroot juggling.
#
# Build from the repository root:
#   docker build -f deploy/Dockerfile -t ghcr.io/tg123/ublk-azblob:latest .

# ── Builder ───────────────────────────────────────────────────────────────────
FROM rust:1.96-bookworm AS builder

# Provided automatically by BuildKit.
ARG TARGETARCH

# Install build dependencies (all native to the builder's own architecture):
# - protobuf-compiler, libprotobuf-dev: protoc and well-known types for CSI gRPC
# - libclang-dev, clang: libclang for bindgen (used by libublk-rs-sys)
# - musl-tools: musl-gcc, the C compiler the static musl binary links against
#   (compiles the ring crypto C code and the libublk C shim against musl libc)
# - cmake: native build of the aws-lc-sys / ring crypto crates
# - pkg-config: build dependency probing
RUN apt-get update && apt-get install -y \
        protobuf-compiler \
        libprotobuf-dev \
        libclang-dev \
        clang \
        musl-tools \
        cmake \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Resolve the musl Rust target triple for $TARGETARCH and install its std.
RUN <<'EOF'
set -eux
case "$TARGETARCH" in
    amd64) triple=x86_64-unknown-linux-musl ;;
    arm64) triple=aarch64-unknown-linux-musl ;;
    *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;;
esac
rustup target add "$triple"
echo "export TARGET_TRIPLE=$triple" > /target-env.sh
# Upper/lower-case underscore forms used by cargo/cc env var names.
triple_us=$(echo "$triple" | tr '-' '_')
{
    echo "export CARGO_BUILD_TARGET=$triple"
    # Point the musl target's C compiler at musl-gcc so the bundled C code
    # (ring, libublk shim) is compiled against musl libc.  Rust's musl target
    # already links the final binary fully static (crt-static + self-contained
    # startup objects); do NOT override the linker, which would disable that and
    # produce a dynamically-linked musl executable.
    echo "export CC_${triple_us}=musl-gcc"
} >> /target-env.sh
cat /target-env.sh
EOF

# Use BuildKit cache mounts for faster builds:
# - Cache cargo registry (crates.io index and downloads), keyed per target
#   architecture to avoid concurrent unpack races during multi-platform builds
# - Cache cargo build artifacts in /build/target, keyed per target architecture
# - Mount source code instead of copying (avoids layer invalidation)
RUN --mount=type=bind,source=.,target=/src,readonly \
    --mount=type=cache,id=cargo-registry-${TARGETARCH},target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-target-${TARGETARCH},target=/build/target \
    . /target-env.sh && \
    cd /src && \
    CARGO_TARGET_DIR=/build/target cargo build --release -p ublk-azblob && \
    cp "/build/target/${TARGET_TRIPLE}/release/ublk-azblob" /ublk-azblob && \
    # Fail the build unless the binary is fully static (no ELF interpreter).
    file /ublk-azblob | grep -qiE "static-pie|statically linked" && \
    ! file /ublk-azblob | grep -qi "interpreter" && \
    # Strip here, once, so the runtime image and the release tarball ship the
    # byte-for-byte identical artifact (both consume the `binary` stage below,
    # and the release workflow no longer re-strips).
    strip /ublk-azblob

# ── Static binary export ────────────────────────────────────────────────────
# Minimal stage that contains only the fully-static, stripped binary, so the
# release workflow can reuse this exact build instead of re-implementing the
# musl static build:
#   docker buildx build --target binary --output type=local,dest=out .
# The runtime stage below copies the *same* stage, so the published image binary
# and the release tarball binary are identical.
FROM scratch AS binary
COPY --from=builder /ublk-azblob /ublk-azblob

# ── Runtime ───────────────────────────────────────────────────────────────────
# Pulled for the *target* platform; carries the static target-arch binary.
FROM debian:bookworm-slim

# - e2fsprogs       : mkfs.ext4 + blkid + fsck.ext4 (e2fsck)
# - xfsprogs        : mkfs.xfs (newBlobFsType: xfs)
# - btrfs-progs     : mkfs.btrfs (newBlobFsType: btrfs)
# - ntfs-3g         : mount.ntfs (templateBlobFsType: ntfs, mount-only via FUSE)
# - util-linux      : mount / umount / blockdev / fsck
# - ca-certificates : TLS roots for talking to Azure Storage / the Kubernetes API
# - nbd-client      : NBD client for NBD mode (compatibility with kernels without ublk_drv)
# squashfs mounts via the in-kernel driver (no userspace tooling). `zfs` is NOT
# shipped: mounting it needs the ZFS kernel module (absent from a stock kernel)
# plus zfsutils-linux, so `templateBlobFsType: zfs` requires a custom runtime image.
# (the ublk_drv module is loaded on the host, so no kmod/modprobe is needed here)
RUN apt-get update && apt-get install -y \
        e2fsprogs \
        xfsprogs \
        btrfs-progs \
        ntfs-3g \
        util-linux \
        ca-certificates \
        nbd-client \
    && rm -rf /var/lib/apt/lists/*

COPY --from=binary /ublk-azblob /usr/local/bin/ublk-azblob

ENTRYPOINT ["/usr/local/bin/ublk-azblob"]
