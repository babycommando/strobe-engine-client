# syntax=docker/dockerfile:1

############################
# Builder
############################
FROM rust:1.81-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config build-essential ca-certificates git && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
# Copy full source
COPY . .

# Embed a build stamp
ARG GIT_SHA
ARG BUILD_TIME
ENV BUILD_GIT_SHA=${GIT_SHA} \
    BUILD_TIME=${BUILD_TIME}

# Tuned build
ENV RUSTFLAGS="-C target-cpu=native -C opt-level=3 -C codegen-units=1 -C strip=symbols"
# Clean to avoid stale target cache in builder layer
RUN cargo clean && cargo build --release

############################
# Runtime
############################
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl openssl tini procps && \
    rm -rf /var/lib/apt/lists/*

# Create runtime user and dirs
RUN useradd -m -u 10001 strobe && \
    mkdir -p /data /certs && \
    chown -R strobe:strobe /data /certs

WORKDIR /app
COPY --from=builder /app/target/release/strobe /app/strobe

# Entrypoint script
COPY <<'SH' /app/entrypoint.sh
#!/bin/sh
set -e

: "${MODE:=h2}"
: "${BIND:=0.0.0.0:7700}"
: "${DATA_DIR:=/data}"
: "${SHARDS:=1}"
: "${SHARD_ID:=0}"
: "${WAL_SYNC:=coalesce:1048576}"
: "${FLUSH_DOCS:=4096}"
: "${FLUSH_MS:=5}"
: "${CERT:=/certs/cert.pem}"
: "${KEY:=/certs/key.pem}"

mkdir -p "$DATA_DIR" /certs
chown -R strobe:strobe "$DATA_DIR" /certs || true

# Raise open file limit if possible (Docker --ulimit sets hard/soft; this bumps soft)
ulimit -n 1048576 || true

# Generate self-signed cert if missing (dev)
if [ "$MODE" = "h2" ]; then
  if [ ! -s "$CERT" ] || [ ! -s "$KEY" ]; then
    echo "[entrypoint] generating self-signed cert at ${CERT} / ${KEY}"
    openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 3650 \
      -keyout "$KEY" -out "$CERT" -subj "/CN=localhost" >/dev/null 2>&1
    chown strobe:strobe "$CERT" "$KEY"
  fi
fi

# Run as strobe directly (no su -> preserves RLIMIT_NOFILE)
exec /app/strobe --cert "$CERT" --key "$KEY"
SH

RUN chmod +x /app/entrypoint.sh

# Expose TCP port
EXPOSE 7700

# Environment defaults (HTTP/2 over TLS)
ENV MODE=h2 \
    BIND=0.0.0.0:7700 \
    DATA_DIR=/data \
    SHARDS=1 \
    SHARD_ID=0 \
    WAL_SYNC=coalesce:1048576 \
    FLUSH_DOCS=4096 \
    FLUSH_MS=5

# Healthcheck: hit /stats over h2 (skip verify for self-signed)
HEALTHCHECK --interval=10s --timeout=3s --retries=3 CMD \
  sh -c 'code=$(curl -sk --http2 -o /dev/null -w "%{http_code}" https://127.0.0.1:7700/stats) && [ "$code" = "200" ]'

# Tini as PID1 for signal handling
ENTRYPOINT ["/usr/bin/tini", "-g", "--"]
CMD ["/app/entrypoint.sh"]

# Run as non-root
USER strobe
