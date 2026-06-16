# Test runner image for the ublk-azblob mount e2e.
#
# It carries the Rust toolchain plus the host-side tools the test shells out to
# (`mkfs.ext4`, `mount`/`umount`).  The crate itself is built at run time from
# the bind-mounted source tree (see the `runner` service in docker-compose.yml),
# so this image only provides the environment, not the compiled binary.
#
# rust:1.88 is required: the pinned Azure SDK crates (azure_core 1.0.0,
# azure_storage_blob 1.0.0, typespec 1.0.0, time 0.3.49) need rustc ≥ 1.88.
FROM rust:1.88-slim

# - e2fsprogs : mkfs.ext4
# - util-linux: mount / umount
# - kmod      : modprobe / lsmod (the module is loaded on the host)
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        e2fsprogs \
        util-linux \
        kmod \
        ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
