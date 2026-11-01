FROM rust:1.99-bookworm AS source
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY bloklid ./bloklid

FROM source AS development-builder
ARG TARGETARCH
RUN --mount=type=cache,id=blokli-cargo-registry-${TARGETARCH},target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=blokli-target-${TARGETARCH},target=/src/target,sharing=locked \
    cargo build --locked --release --bin blokli-light --bin blokli-contract-deployer \
    && mkdir /out \
    && cp /src/target/release/blokli-light /out/blokli-light \
    && cp /src/target/release/blokli-contract-deployer /out/blokli-contract-deployer

FROM source AS production-builder
ARG TARGETARCH
RUN --mount=type=cache,id=blokli-cargo-registry-${TARGETARCH},target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=blokli-target-${TARGETARCH},target=/src/target,sharing=locked \
    cargo build --locked --release --bin blokli-light \
    && mkdir /out \
    && cp /src/target/release/blokli-light /out/blokli-light

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
USER 65532:65532

FROM runtime AS development
COPY --from=development-builder /out/blokli-light /usr/local/bin/blokli-light
COPY --from=development-builder /out/blokli-contract-deployer /usr/local/bin/blokli-contract-deployer
ENTRYPOINT ["/usr/local/bin/blokli-light"]

FROM runtime AS production
COPY --from=production-builder /out/blokli-light /usr/local/bin/blokli-light
ENTRYPOINT ["/usr/local/bin/blokli-light"]
