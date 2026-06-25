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
# Multi-arch (linux/amd64 + linux/arm64) is built by *cross-compiling* on the
# native build platform with `cargo-zigbuild` — never under QEMU emulation. Zig
# ships its own cross C compiler, linker and musl sysroot, so a single amd64
# builder produces the static arm64 binary at native speed with no per-arch
# toolchain juggling (no cross GCC, no sysroot headers). The builder stage is
# pinned to `$BUILDPLATFORM`; only the small runtime stage is pulled for the
# target platform.
#
# Build from the repository root:
#   docker build -f deploy/Dockerfile -t ghcr.io/tg123/ublk-azblob:latest .

# ── Builder ───────────────────────────────────────────────────────────────────
# Pinned to the native build platform so the Rust/zig toolchain runs on real
# hardware; the target architecture is reached via cross-compilation, not QEMU.
FROM --platform=$BUILDPLATFORM rust:1.96-bookworm AS builder

# Provided automatically by BuildKit.
ARG TARGETARCH

# Pinned tool versions for reproducible builds.
ARG ZIG_VERSION=0.13.0
ARG CARGO_ZIGBUILD_VERSION=0.19.8

# Install build dependencies (all native to the builder's own architecture —
# nothing here is target-arch specific, because zig provides the cross compiler):
# - protobuf-compiler, libprotobuf-dev: protoc and well-known types for CSI gRPC
# - libclang-dev, clang: libclang for bindgen (used by libublk-rs-sys)
# - cmake, pkg-config: native build deps for some -sys crates
# - curl, xz-utils: fetch + unpack the zig toolchain
RUN apt-get update && apt-get install -y \
        protobuf-compiler \
        libprotobuf-dev \
        libclang-dev \
        clang \
        cmake \
        pkg-config \
        curl \
        xz-utils \
    && rm -rf /var/lib/apt/lists/*

# Install zig (the cross C compiler + linker + bundled musl sysroot) for the
# builder's own architecture, plus cargo-zigbuild which drives cargo through it.
RUN <<'EOF'
set -eux
case "$(dpkg --print-architecture)" in
    amd64) zig_arch=x86_64 ;;
    arm64) zig_arch=aarch64 ;;
    *) echo "unsupported builder arch: $(dpkg --print-architecture)" >&2; exit 1 ;;
esac
curl -fsSL "https://ziglang.org/download/${ZIG_VERSION}/zig-linux-${zig_arch}-${ZIG_VERSION}.tar.xz" -o /tmp/zig.tar.xz
mkdir -p /opt/zig
tar -C /opt/zig --strip-components=1 -xf /tmp/zig.tar.xz
rm /tmp/zig.tar.xz
/opt/zig/zig version
EOF
ENV PATH="/opt/zig:${PATH}"
RUN cargo install cargo-zigbuild --version "${CARGO_ZIGBUILD_VERSION}" --locked

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
    # cargo-zigbuild cross-compiles to the musl target via zig (no QEMU). The
    # release profile sets `strip = true`, so the artifact is already stripped —
    # no separate `strip` step (which would fail on a cross-arch binary anyway).
    CARGO_TARGET_DIR=/build/target cargo zigbuild --release \
        --target "${TARGET_TRIPLE}" -p ublk-azblob && \
    cp "/build/target/${TARGET_TRIPLE}/release/ublk-azblob" /ublk-azblob && \
    # Fail the build unless the binary is fully static (no ELF interpreter).
    file /ublk-azblob | grep -qiE "static-pie|statically linked" && \
    ! file /ublk-azblob | grep -qi "interpreter"

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
