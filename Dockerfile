FROM lukemathwalker/cargo-chef:latest-rust-1-bullseye AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder 
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .
RUN cargo build --release --bin brahmand \
    && cargo build --release --bin brahmand-client

# We do not need the Rust toolchain to run the binary!
FROM debian:bullseye-slim AS runtime
RUN apt-get update && \
    apt-get install -y libssl1.1 ca-certificates && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /app
# Copy binaries
COPY --from=builder /app/target/release/brahmand /usr/local/bin/
COPY --from=builder /app/target/release/brahmand-client /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/brahmand"]