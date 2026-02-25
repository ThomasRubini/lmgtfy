FROM rust:1.88-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libssl-dev \
    pkg-config \
    libclang-dev \
    cmake \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml ./
COPY Cargo.lock ./
COPY common ./common
COPY email-sim ./email-sim
COPY whatsapp-sim ./whatsapp-sim
COPY email-trt ./email-trt
COPY whatsapp-trt ./whatsapp-trt
COPY labelize-ticket-trt ./labelize-ticket-trt
COPY alerting-dlq ./alerting-dlq

RUN cargo build --release --package email-sim --package whatsapp-sim --package email-trt --package whatsapp-trt --package labelize-ticket-trt --package alerting-dlq

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/email-sim /app/email-sim
COPY --from=builder /app/target/release/whatsapp-sim /app/whatsapp-sim
COPY --from=builder /app/target/release/email-trt /app/email-trt
COPY --from=builder /app/target/release/whatsapp-trt /app/whatsapp-trt
COPY --from=builder /app/target/release/labelize-ticket-trt /app/labelize-ticket-trt
COPY --from=builder /app/target/release/alerting-dlq /app/alerting-dlq

ENV RUST_LOG=info
