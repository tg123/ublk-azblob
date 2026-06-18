# Test runner image for the ublk-azblob Kubernetes PVC e2e.
#
# Provides: Rust toolchain, protoc/libprotobuf-dev, the docker CLI, kubectl,
# and helm.  The source tree is bind-mounted at /workspace at runtime;
# nothing is compiled here — only the environment is prepared.
#
FROM rust:1.96-slim

ARG DOCKER_VERSION=27.5.1
ARG KUBECTL_VERSION=v1.32.3
ARG HELM_VERSION=v3.17.3

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        protobuf-compiler \
        libprotobuf-dev \
        curl \
        ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# docker CLI (client only) — talks to the host daemon via the mounted socket.
# The static archive uses x86_64/aarch64 naming; map from dpkg arch.
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

WORKDIR /workspace
