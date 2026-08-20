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

# ---- Rust Builder (shared by both images) ----
FROM rust:1.88-bookworm AS builder

WORKDIR /build

# Cache dependencies: copy manifests first, build dummy targets to cache deps
COPY Cargo.toml ./
COPY gateway/Cargo.toml ./gateway/
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    mkdir -p src/bin && echo "fn main() {}" > src/bin/server.rs && \
    echo "" > src/lib.rs && \
    mkdir -p gateway/src && echo "fn main() {}" > gateway/src/main.rs && \
    echo "" > gateway/src/lib.rs && \
    cargo generate-lockfile && \
    (cargo build --release -p inputlayer --bin inputlayer-server && \
     cargo build --release -p inputlayer-gateway --bin inputlayer-gateway) 2>/dev/null || true && \
    rm -rf src gateway/src

# Build the real binaries. The touch is load-bearing: COPY preserves context
# mtimes, which in CI predate the dummy dep-cache build above - without it
# cargo considers the crates fresh and ships the dummy `fn main() {}`
# binaries (the engine only escaped via the --all-features fingerprint
# change; the featureless gateway crate has no such protection).
COPY src/ src/
COPY gateway/ gateway/
COPY docs/ docs/
RUN find src gateway/src -type f -exec touch {} + && \
    cargo build --all-features --release -p inputlayer --bin inputlayer-server && \
    cargo build --release -p inputlayer-gateway --bin inputlayer-gateway && \
    strip target/release/inputlayer-server target/release/inputlayer-gateway

# ---- Gateway Runtime (build with: docker build --target gateway) ----
FROM debian:bookworm-slim AS gateway

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

RUN useradd -r -s /bin/false -m -d /var/lib/inputlayer gateway

COPY --from=builder /build/target/release/inputlayer-gateway /usr/local/bin/

# Gateway configuration is its own surface, separate from the engine:
#   GATEWAY_HOST / GATEWAY_PORT   bind address
#   INPUTLAYER_URL                engine base URL
#   INPUTLAYER_API_KEY            engine API key (WS access, from #83 on)
#   ANTHROPIC_API_KEY             model provider key (never seen by the engine)
ENV GATEWAY_HOST=0.0.0.0
ENV GATEWAY_PORT=8081
ENV INPUTLAYER_URL=http://inputlayer:8080

EXPOSE 8081
USER gateway

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -sf "http://localhost:${GATEWAY_PORT:-8081}/health" || exit 1

ENTRYPOINT ["inputlayer-gateway"]

# ---- Engine Runtime (default target, keep last) ----
FROM debian:bookworm-slim AS engine

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

RUN useradd -r -s /bin/false -m -d /var/lib/inputlayer inputlayer

COPY --from=builder /build/target/release/inputlayer-server /usr/local/bin/
COPY --chown=inputlayer:inputlayer --from=gui-builder /build/gui/dist/ /var/lib/inputlayer/gui/dist/

RUN mkdir -p /var/lib/inputlayer/data && \
    chown -R inputlayer:inputlayer /var/lib/inputlayer

ENV INPUTLAYER_HTTP__HOST=0.0.0.0
ENV INPUTLAYER_HTTP__PORT=8080
ENV INPUTLAYER_STORAGE__DATA_DIR=/var/lib/inputlayer/data
ENV INPUTLAYER_STORAGE__AUTO_CREATE_KNOWLEDGE_GRAPHS=true
ENV INPUTLAYER_LOGGING__LEVEL=info

EXPOSE 8080
USER inputlayer
WORKDIR /var/lib/inputlayer

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -sf http://localhost:8080/health || exit 1

ENTRYPOINT ["inputlayer-server"]
