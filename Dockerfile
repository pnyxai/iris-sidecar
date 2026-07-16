# syntax=docker/dockerfile:1
FROM rust:1-slim AS builder

WORKDIR /app
COPY Cargo.toml ./
COPY src ./src

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
RUN cargo build --release

FROM debian:bookworm-slim AS runtime

WORKDIR /app
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/iris-sidecar /usr/local/bin/iris-sidecar

EXPOSE 8080

ENTRYPOINT ["iris-sidecar"]
