FROM rust:1.99-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY bloklid ./bloklid
ARG TARGETARCH
RUN --mount=type=cache,id=blokli-cargo-registry-${TARGETARCH},target=/usr/local/cargo/registry \
    --mount=type=cache,id=blokli-target-${TARGETARCH},target=/src/target \
    cargo build --locked --release --bin blokli-light --bin blokli-contract-deployer \
    && mkdir /out \
    && cp /src/target/release/blokli-light /out/blokli-light \
    && cp /src/target/release/blokli-contract-deployer /out/blokli-contract-deployer

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/blokli-light /usr/local/bin/blokli-light
COPY --from=builder /out/blokli-contract-deployer /usr/local/bin/blokli-contract-deployer
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/blokli-light"]
