# blokli-light Architecture

## Scope

blokli-light is a single PostgreSQL-backed daemon that follows selected contracts on one Ethereum-compatible chain and persists their raw
logs. It does not decode events into derived state, expose a GraphQL API, submit transactions, serve subscriptions, or operate an HTTP API.
Consumers query PostgreSQL directly, typically through Arrakis or their own SQL.

## System Overview

```text
Ethereum / Gnosis RPC
          |
          v
  blokli-light daemon
  - finality-aware follower
  - lower-priority backfill worker
  - source reconciler
          |
          v
      PostgreSQL
  - raw logs and watermarks
  - source catalog and status view
  - read-only consumer role
          |
          v
 Arrakis / SQL consumers
```

The daemon has one writer role. At startup it acquires a PostgreSQL advisory lock for the lifetime of its writer session, so a second
instance cannot advance the same database concurrently.

## Sources

The `source` table is the sole catalog of watched contracts. Built-in HOPR sources are reconciled from `hopr-bindings` and configured
contract overrides at startup. Operator-defined sources may be declared in configuration or added while the daemon is running. Every source
records its own `from_block` and `indexed_to` watermark, so long historical backfills do not make a source appear complete before it is
caught up.

Each source either has a list of event signature hashes or follows every event emitted by its address. Textual Solidity signatures are
normalised and Keccak-256 hashed before persistence. Sources are never deleted: disabling one stops future retrieval while preserving its
already stored log history.

## Data Model and Compatibility

`log`, `chain_info`, and `log_topic_info` retain Blokli's established column names and raw byte layout. In particular, `topics` is the
concatenation of complete 32-byte topics with topic0 first, and `data` is the untouched ABI payload. This lets the existing Arrakis bundle
query blokli-light without schema adaptation.

`chain_info.last_indexed_block` remains the watermark for the built-in HOPR set. `source.indexed_to` is the completion marker for every
individual source, including custom contracts. The `indexer_status` view exposes source completion and lag for direct SQL consumers.

The database stores no event-derived account, channel, Safe, balance, or service tables. Interpretation and materialisation are explicitly
downstream concerns.

## Indexing Flow

1. The daemon migrates PostgreSQL, acquires the writer lock, checks the stored chain identity, and reconciles source definitions.
2. The follower reads the RPC head, subtracts configured confirmations, and retrieves finalised logs for caught-up enabled sources.
3. A backfill worker independently fetches sources whose `indexed_to` has not reached the built-in watermark. Its lower priority avoids
   delaying head tracking.
4. Each completed source batch writes raw logs and advances that source in one PostgreSQL transaction. After every built-in source reaches
   the same finalised head, a separate compatibility-watermark transaction advances `chain_info` and emits `blokli_light_head` through
   `pg_notify`. A crash between those commits only causes an idempotent refetch; it cannot skip logs.
5. A restart reconciliation window re-fetches recent finalised blocks. Replaced block hashes overwrite conflicting positions; logs no longer
   present in a replaced block are marked `removed`, never hard-deleted.

RPC log queries learn and retain a smaller block range after provider limit errors. Sources are queried independently, which preserves each
source's event filter and naturally avoids provider address-list limits.

## Security and Operations

The daemon writes with its configured database role. An optional reader role is limited to `SELECT` on the indexer's public tables and uses
read-only transactions, a bounded connection count, statement timeout, and idle-in-transaction timeout. No reader privilege is granted on
unrelated schemas.

Status is available through the `indexer_status` SQL view and the daemon CLI. `chain_info` records network identity, RPC head, and the
built-in watermark, allowing consumers to calculate lag without an API. Structured text or JSON logs use the standard `RUST_LOG` filter.
Optional OTLP export sends metrics, traces, and logs to a configured collector; the two daemon gauges expose the observed RPC head and
committed finalised watermark. No inbound HTTP endpoint is required.

The published production container contains only the daemon. Local integration environments use a separate development container target that
also carries the contract deployment helper needed to initialise Anvil; that helper is not part of the VPS deployment artifact.
