# Multi-stage build for the `teo` harness + the tursodb sync-server CLI.
# Ollama is NOT in here — it runs on the host (GPU/Metal); containers reach it via
# host.docker.internal:11434.

FROM rust:1.96-bookworm AS build
WORKDIR /app
# system deps for the duckdb bundled build (C/C++ toolchain) are already in rust:bookworm
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --bin teo

# Fetch the matching tursodb CLI (Linux) for the sync server.
FROM debian:bookworm-slim AS cli
ARG TURSO_VERSION=v0.6.1
RUN apt-get update && apt-get install -y --no-install-recommends curl xz-utils ca-certificates && rm -rf /var/lib/apt/lists/*
RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    case "$arch" in amd64) cpu=x86_64;; arm64) cpu=aarch64;; *) echo "unsupported $arch"; exit 1;; esac; \
    asset="turso_cli-${cpu}-unknown-linux-gnu.tar.xz"; \
    curl -sSL "https://github.com/tursodatabase/turso/releases/download/${TURSO_VERSION}/${asset}" -o /tmp/cli.tar.xz; \
    mkdir -p /opt/turso && tar xf /tmp/cli.tar.xz -C /opt/turso --strip-components=1; \
    find /opt/turso -name tursodb -type f -exec cp {} /usr/local/bin/tursodb \; ; \
    chmod +x /usr/local/bin/tursodb; tursodb --version

FROM debian:bookworm-slim
# libssl3: turso/iceberg/reqwest link OpenSSL (native-tls); ca-certificates: Wikimedia HTTPS stream.
RUN for i in 1 2 3; do apt-get update && apt-get install -y --no-install-recommends libssl3 ca-certificates && break || sleep 5; done && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/teo /usr/local/bin/teo
COPY --from=cli /usr/local/bin/tursodb /usr/local/bin/tursodb
WORKDIR /work
ENV TURSO_EXP_HOME=/work/.work
ENV TEO_REPO_ROOT=/work
ENV TURSODB=/usr/local/bin/tursodb
# Ollama on the host:
ENV OLLAMA_URL=http://host.docker.internal:11434
ENV EMBED_MODEL=all-minilm
ENV EMBED_DIM=384
ENTRYPOINT ["teo"]
CMD ["--help"]
