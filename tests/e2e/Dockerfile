# Unified test-runner image for the ublk-azblob e2e suite.
#
# One runner drives every e2e from a single compile (default ublk + csi build):
#   * mount_e2e / nbd_e2e — run the freshly-built binary against Azurite.
#   * k8s_pvc_e2e         — package that same binary into a thin image and run
#                           the CSI driver on the in-compose k3s cluster.
#
# It therefore carries both the host-side tools the mount tests shell out to
# (`mkfs.ext4`, `mount`/`umount`) and the Kubernetes tooling (docker CLI,
# kubectl, helm).  The crate is built at run time from the bind-mounted source
# tree; this image only provides the environment.
#
FROM rust:1.96-slim

ARG DOCKER_VERSION=27.5.1
ARG KUBECTL_VERSION=v1.32.3
ARG HELM_VERSION=v3.17.3

# - e2fsprogs / util-linux : mkfs.ext4, mount/umount, losetup (run by mount_e2e
#                            and the mount-module unit tests directly)
# - xfsprogs / btrfs-progs  : mkfs.xfs / mkfs.btrfs, so the profile-driven
#                            format+mount unit test and the `mount_e2e`
#                            `mount_formattable_fs_profiles` test cover xfs/btrfs
# - libclang-dev           : bindgen for libublk-rs-sys (ublk feature)
# - protobuf-compiler + libprotobuf-dev : protoc + well-known types (csi feature)
# (nbd_e2e uses an in-process TCP client and the node plugin's mkfs/mount/
#  nbd-client live in the driver image, so neither is needed here.)
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        e2fsprogs \
        xfsprogs \
        btrfs-progs \
        util-linux \
        ca-certificates \
        curl \
        libclang-dev \
        protobuf-compiler \
        libprotobuf-dev \
 && rm -rf /var/lib/apt/lists/*

# docker CLI (client only) + buildx plugin — talks to the host daemon via the
# mounted socket. buildx is required for `docker build --target` to only build
# the targeted stage (the legacy builder compiles every stage up to the target).
RUN ARCH=$(dpkg --print-architecture) \
 && case "$ARCH" in \
        amd64) DOCKER_ARCH=x86_64 ;; \
        arm64) DOCKER_ARCH=aarch64 ;; \
        *) echo "unsupported arch: $ARCH" >&2; exit 1 ;; \
    esac \
 && curl -fsSL \
      "https://download.docker.com/linux/static/stable/${DOCKER_ARCH}/docker-${DOCKER_VERSION}.tgz" \
    | tar -xzC /usr/local/bin --strip-components=1 docker/docker \
 && mkdir -p /usr/local/lib/docker/cli-plugins \
 && curl -fsSL -o /usr/local/lib/docker/cli-plugins/docker-buildx \
      "https://github.com/docker/buildx/releases/download/v0.20.1/buildx-v0.20.1.linux-${ARCH}" \
 && chmod +x /usr/local/lib/docker/cli-plugins/docker-buildx

# kubectl — arch-aware
RUN ARCH=$(dpkg --print-architecture) \
 && curl -fsSL -o /usr/local/bin/kubectl \
      "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${ARCH}/kubectl" \
 && chmod +x /usr/local/bin/kubectl

# helm — arch-aware tarball
RUN ARCH=$(dpkg --print-architecture) \
 && curl -fsSL \
      "https://get.helm.sh/helm-${HELM_VERSION}-linux-${ARCH}.tar.gz" \
    | tar -xzC /usr/local/bin --strip-components=1 "linux-${ARCH}/helm"

# Azure CLI — used by the mount/NBD read-only e2e tests to snapshot the blob
# (`az storage blob snapshot`) before reopening it read-only. The official
# Debian install script sets up the Microsoft apt repo for the build arch
# (amd64/arm64) and installs `azure-cli`.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        apt-transport-https \
        lsb-release \
        gnupg \
 && curl -sL https://aka.ms/InstallAzureCLIDeb | bash \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
