# ---- GUI Builder ----
FROM node:22-bookworm-slim AS gui-builder

WORKDIR /build

# Build @inputlayer/api-client first (GUI depends on it at compile time)
COPY packages/api-client/package.json packages/api-client/package-lock.json ./packages/api-client/
RUN cd packages/api-client && npm ci --silent
COPY packages/api-client/src/ ./packages/api-client/src/
COPY packages/api-client/tsconfig.json ./packages/api-client/
RUN cd packages/api-client && npm run build

# Build GUI (Next.js static export)
COPY gui/package.json ./gui/
RUN cd gui && npm install --no-audit --no-fund --silent
COPY gui/ ./gui/
COPY docs/content/ ./docs/content/
RUN cd gui && npm run build

# ---- Rust Builder ----
FROM rust:1.88-bookworm AS builder

WORKDIR /build

# Cache dependencies: copy manifest first, build a dummy lib to cache deps
COPY Cargo.toml ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    mkdir -p src/bin && echo "fn main() {}" > src/bin/server.rs && \
    echo "" > src/lib.rs && \
    cargo generate-lockfile && \
    cargo build --release --bin inputlayer-server 2>/dev/null || true && \
    rm -rf src

# Build the real binary
COPY src/ src/
COPY docs/ docs/
RUN cargo build --all-features --release --bin inputlayer-server && \
    strip target/release/inputlayer-server

# ---- Runtime ----
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

RUN useradd -r -s /bin/false -m -d /var/lib/inputlayer inputlayer

COPY --from=builder /build/target/release/inputlayer-server /usr/local/bin/
COPY --from=gui-builder /build/gui/dist/ /var/lib/inputlayer/gui/dist/

ENV INPUTLAYER_HTTP__HOST=0.0.0.0
ENV INPUTLAYER_HTTP__PORT=8080
ENV INPUTLAYER_STORAGE__DATA_DIR=/var/lib/inputlayer/data
ENV INPUTLAYER_STORAGE__AUTO_CREATE_KNOWLEDGE_GRAPHS=true
ENV INPUTLAYER_LOGGING__LEVEL=info

VOLUME /var/lib/inputlayer/data
EXPOSE 8080
USER inputlayer
WORKDIR /var/lib/inputlayer

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -sf http://localhost:8080/health || exit 1

ENTRYPOINT ["inputlayer-server"]
