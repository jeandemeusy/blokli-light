FROM lukemathwalker/cargo-chef:0.1.78-rust-1.99.1-bookworm@sha256:b75e76d787d5e11dabafa95eecc9fa1b50c38d5e03a38efb99cacb4e02125a76 AS chef
WORKDIR /src

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY bloklid ./bloklid
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS dependencies
ARG TARGETARCH
COPY --from=planner /src/recipe.json recipe.json
RUN --mount=type=cache,id=blokli-cargo-registry-${TARGETARCH},target=/usr/local/cargo/registry,sharing=locked \
    cargo chef cook --locked --release --recipe-path recipe.json

FROM dependencies AS production-builder
ARG TARGETARCH
COPY Cargo.toml Cargo.lock ./
COPY bloklid ./bloklid
RUN --mount=type=cache,id=blokli-cargo-registry-${TARGETARCH},target=/usr/local/cargo/registry,sharing=locked \
    cargo build --locked --release --bin blokli-light \
    && mkdir /out \
    && cp /src/target/release/blokli-light /out/blokli-light

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
USER 65532:65532

FROM runtime AS development
COPY --from=production-builder /out/blokli-light /usr/local/bin/blokli-light
ENTRYPOINT ["/usr/local/bin/blokli-light"]

FROM runtime AS production
COPY --from=production-builder /out/blokli-light /usr/local/bin/blokli-light
ENTRYPOINT ["/usr/local/bin/blokli-light"]
