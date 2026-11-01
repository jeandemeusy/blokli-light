# Run these recipes from `nix develop`, as described in AGENTS.md.

default:
    @just --list

# Required fast validation: format, lint every target, and compile the workspace.
quick: fmt clippy

# Full local development gate.
dev: quick test

fmt:
    treefmt

check:
    cargo check --workspace --all-targets

clippy:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

test:
    cargo test --workspace --no-fail-fast

build:
    cargo build --workspace

build-release:
    cargo build --workspace --release

run *args:
    cargo run --bin blokli-light -- {{ args }}

status *args:
    cargo run --bin blokli-light -- {{ args }} status

migrate *args:
    cargo run --bin blokli-light -- {{ args }} migrate

# Run PostgreSQL-backed storage and reader-permission tests in an isolated cluster.
test-postgres:
    #!/usr/bin/env bash
    set -euo pipefail
    pg_dir=$(mktemp -d "${TMPDIR:-/tmp}/blokli-light-pg.XXXXXX")
    trap 'pg_ctl -D "$pg_dir/data" -m immediate stop >/dev/null 2>&1 || true; rm -rf "$pg_dir"' EXIT
    initdb -D "$pg_dir/data" -U postgres --auth=trust --locale=C --encoding=UTF8 >/dev/null
    pg_ctl -D "$pg_dir/data" -l "$pg_dir/postgres.log" -o "-k $pg_dir -c listen_addresses=''" -w start >/dev/null
    export BLOKLI_LIGHT_TEST_DATABASE_URL="postgresql://postgres@localhost/postgres?host=$pg_dir"
    cargo test -p blokli-light --test postgres -- --ignored --test-threads=1

doc:
    cargo doc --workspace --no-deps --open

docker-up:
    docker compose -f docker/docker-compose.yml up --build

docker-down:
    docker compose -f docker/docker-compose.yml down

docker-build:
    nix build -L .#docker-bloklid-x86_64-linux
