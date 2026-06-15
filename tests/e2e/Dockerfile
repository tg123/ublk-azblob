# Build stage: compile the Rust binary
FROM rust:1.88-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy workspace files
COPY Cargo.toml ./
COPY ublk-azblob/Cargo.toml ./ublk-azblob/Cargo.toml
COPY ublk-azblob/src ./ublk-azblob/src

# Build release binary (without ublk feature — kernel driver not available in CI)
RUN cargo build --release -p ublk-azblob

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/ublk-azblob /usr/local/bin/ublk-azblob

COPY tests/e2e/run_e2e.sh /usr/local/bin/run_e2e.sh
RUN chmod +x /usr/local/bin/run_e2e.sh

ENTRYPOINT ["/usr/local/bin/run_e2e.sh"]
