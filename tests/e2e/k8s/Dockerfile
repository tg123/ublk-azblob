# Test runner image for the ublk-azblob Kubernetes PVC e2e.
#
# Provides: Rust toolchain, protoc/libprotobuf-dev, kind, kubectl, and helm.
# The source tree is bind-mounted at /workspace at runtime; nothing is compiled
# here — only the environment is prepared.
#
FROM rust:1.96-slim

ARG KIND_VERSION=v0.26.0
ARG KUBECTL_VERSION=v1.32.3
ARG HELM_VERSION=v3.17.3

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        protobuf-compiler \
        libprotobuf-dev \
        curl \
        ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# kind — single static binary, arch-aware
RUN ARCH=$(dpkg --print-architecture) \
 && curl -fsSL -o /usr/local/bin/kind \
      "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-${ARCH}" \
 && chmod +x /usr/local/bin/kind

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
