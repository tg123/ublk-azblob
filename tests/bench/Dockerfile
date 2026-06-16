# Benchmark runner image for ublk-azblob (device vs. raw local disk).
#
# Same base as the mount e2e runner, plus the benchmark tooling:
#   - fio         : the I/O benchmark itself
#   - jq          : parse fio's JSON output into the comparison table
#   - util-linux  : losetup (loopback raw-disk reference) + mount tooling
#   - libaio-dev  : fio's libaio ioengine (direct I/O)
#
# The crate is built at run time from the bind-mounted source tree (see the
# `runner` service in docker-compose.yml), so this image only provides the
# environment, not the compiled binary.
#
FROM rust:1.96-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        fio \
        jq \
        util-linux \
        libaio-dev \
        kmod \
        ca-certificates \
        libclang-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
