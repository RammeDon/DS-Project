FROM rust:1.84 AS chef

# Stop if a command fails
RUN set -eux

# Only fetch crates.io index for used crates
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

# Install PostgreSQL development libraries
RUN apt-get update && apt-get install -y libpq-dev

# cargo-chef will be cached from the second build onwards
RUN cargo install cargo-chef
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
RUN cargo build --release --bin server

FROM debian:bookworm-slim AS runtime
WORKDIR /app

# Install only the PostgreSQL client runtime libraries
RUN apt-get update && apt-get install -y libpq5 ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/server /usr/local/bin
EXPOSE 8000
ENTRYPOINT ["/usr/local/bin/server"]