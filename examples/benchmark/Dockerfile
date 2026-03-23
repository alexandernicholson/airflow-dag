FROM rust:1.93-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/
COPY examples/benchmark/Cargo.toml ./examples/benchmark/
COPY examples/benchmark/src/ ./examples/benchmark/src/

WORKDIR /app/examples/benchmark
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/examples/benchmark/target/release/ironpipe-benchmark /usr/local/bin/ironpipe-benchmark

ENTRYPOINT ["ironpipe-benchmark"]
