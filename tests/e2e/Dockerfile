# Test runner image for the ublk-azblob mount e2e.
#
# It carries the Rust toolchain plus the host-side tools the test shells out to
# (`mkfs.ext4`, `mount`/`umount`).  The crate itself is built at run time from
# the bind-mounted source tree (see the `runner` service in docker-compose.yml),
# so this image only provides the environment, not the compiled binary.
#
FROM rust:1.96-slim

# - e2fsprogs : mkfs.ext4
# - util-linux: mount / umount
# - kmod      : modprobe / lsmod (the module is loaded on the host)
# - libclang-dev: required by bindgen (used by libublk-rs-sys build script)
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        e2fsprogs \
        util-linux \
        kmod \
        ca-certificates \
        libclang-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
