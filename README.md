# blokli-light

`blokli-light` follows HOPR and operator-defined contracts on one EVM chain and stores their logs, unprocessed, in PostgreSQL. SQL tools
such as Arrakis read the database directly; there is no GraphQL server, transaction API, event handler layer, or derived state.

This GPL-3.0-only project is a fork of [`hoprnet/blokli`](https://github.com/hoprnet/blokli) at base commit
[`2c9ecb7`](https://github.com/hoprnet/blokli/commit/2c9ecb7). Git history is retained for attribution.

## Data contract

The `log`, `chain_info`, and `log_topic_info` tables retain Blokli's Arrakis-compatible column layout. `source` tracks built-in and custom
contracts independently, and `indexer_status` exposes per-source lag. Logs are byte-exact: topics are concatenated 32-byte words and event
data is stored without decoding.

Built-in addresses, deployment blocks, and event signatures come from `hopr-bindings`. The token is intentionally excluded unless an
operator registers it as a custom source.

## Development

All commands run inside the Nix shell:

```bash
nix develop
just quick          # format, Clippy, compile check
just test           # unit tests
just test-postgres  # storage, reorg/origin, and reader-permission contract
```

## Configuration and startup

Copy [`bloklid/example-config.toml`](bloklid/example-config.toml), set the writer database URL and RPC endpoint, then run:

```bash
blokli-light generate-config blokli-light.toml
blokli-light --config config.toml
```

The daemon applies SQLx migrations, checks the RPC chain ID against the configured HOPR network, reconciles sources, validates the origin
guard, acquires a PostgreSQL advisory lock, and resumes from persisted watermarks.

Important environment overrides are:

| Variable                                      | Purpose                                     |
| --------------------------------------------- | ------------------------------------------- |
| `BLOKLI_LIGHT_CONFIG_PATH`                    | TOML configuration path                     |
| `BLOKLI_LIGHT_DATABASE_URL`                   | PostgreSQL writer URL                       |
| `BLOKLI_LIGHT_DATABASE_MAX_CONNECTIONS`       | Writer pool size                            |
| `BLOKLI_LIGHT_RPC_URL`                        | EVM JSON-RPC endpoint                       |
| `BLOKLI_LIGHT_NETWORK`                        | `hopr-bindings` network name                |
| `BLOKLI_LIGHT_MAX_BLOCK_RANGE`                | Initial inclusive `eth_getLogs` range       |
| `BLOKLI_LIGHT_MAX_RPC_REQUESTS_PER_SEC`       | RPC rate limit; `0` is unlimited            |
| `BLOKLI_LIGHT_CONFIRMATIONS`                  | Finality depth                              |
| `BLOKLI_LIGHT_POLL_INTERVAL_SECONDS`          | Head polling interval                       |
| `BLOKLI_LIGHT_INDEXER_TOPICS`                 | `known` or `all` for built-ins              |
| `BLOKLI_LIGHT_READER_USER`                    | Direct-SQL reader role name                 |
| `BLOKLI_LIGHT_READER_PASSWORD`                | Reader password; absence skips provisioning |
| `BLOKLI_LIGHT_OTLP_ENDPOINT`                  | Optional OTLP collector base URL            |
| `BLOKLI_LIGHT_OTLP_SIGNALS`                   | `metrics`, `traces`, and/or `logs`          |
| `BLOKLI_LIGHT_METRIC_EXPORT_INTERVAL_SECONDS` | OTLP metric interval                        |
| `BLOKLI_LIGHT_LOG_FORMAT`                     | Set to `json` for structured logs           |
| `RUST_LOG`                                    | Standard tracing filter                     |

Custom sources are declared with `[[sources]]` or managed while the daemon runs:

```bash
blokli-light source add --name my-vault --address 0x... --from-block 38000000 \
  --events 'Deposit(address,uint256)'
blokli-light source widen --name my-vault --events 'Withdraw(address,uint256)'
blokli-light source disable --name my-vault
blokli-light source list
```

Addresses must contain contract code. Event text is normalized and Keccak-256 hashed; a 32-byte topic hash is also accepted. Removing a
config-managed source disables it without deleting logs.

## Direct SQL reader and status

Set `BLOKLI_LIGHT_READER_PASSWORD` to provision the configured reader on daemon startup, or run it explicitly. Use an admin URL only when
the writer lacks `CREATEROLE`:

```bash
blokli-light db reader --print-url
blokli-light db reader --admin-url postgresql://postgres:...@db/blokli_light --print-url
blokli-light status
```

`--print-url` emits the exact `DATA_SOURCES=postgresql://...` value for Arrakis. The reader receives `SELECT` only, a ten-connection limit,
read-only defaults, and query/idle timeouts.

## Operations

The indexer subtracts confirmations from the RPC head, adaptively shrinks rejected log ranges, retries transient RPC failures, reconciles
recent block hashes after restarts, and commits logs with source progress atomically. Built-in progress remains available at
`chain_info.last_indexed_block`; custom completeness is `source.indexed_to`.

Optional OTLP export supports gRPC and HTTP collectors without opening an inbound HTTP port. Metrics include `blokli_light_head_block` and
`blokli_light_indexed_block`; traces and structured logs may be selected alongside metrics.

### Docker image and VPS deployment

CI builds the image for `linux/amd64`, suitable for a standard x86-64 Linux VPS. Pull requests validate it without publishing; pushes to
`main` publish `latest`, `main`, and an immutable `sha-<commit>` tag to GitHub Container Registry. A Git tag such as `v0.1.0` also publishes
an image with that tag. The published production image contains only the daemon; the local Docker Compose development target additionally
contains the Anvil contract deployment helper.

```bash
docker pull ghcr.io/jeandemeusy/blokli-light:latest

docker run --detach \
  --name blokli-light \
  --restart unless-stopped \
  --env BLOKLI_LIGHT_DATABASE_URL='postgresql://blokli_light:password@postgres.example:5432/blokli_light' \
  --env BLOKLI_LIGHT_RPC_URL='https://your-gnosis-rpc.example' \
  --env BLOKLI_LIGHT_NETWORK='jura-prod' \
  --env BLOKLI_LIGHT_READER_PASSWORD='replace-with-a-long-random-password' \
  ghcr.io/jeandemeusy/blokli-light:latest
```

Use an immutable `sha-<commit>` or release tag instead of `latest` for reproducible production deployments. The PostgreSQL hostname in the
writer URL must be reachable from the container; `localhost` inside the container is the container itself. The daemon has no inbound HTTP
port to expose.

GHCR packages are private by default. Either make the package public under the repository's **Packages** settings or authenticate the VPS
before pulling:

```bash
echo "$GHCR_TOKEN" | docker login ghcr.io --username YOUR_GITHUB_USER --password-stdin
```

For a private image, `GHCR_TOKEN` needs `read:packages`. CI publishes with the repository's built-in `GITHUB_TOKEN`; no additional CI secret
is required.

For a complete local stack, run `docker compose -f docker/docker-compose.yml up --build --wait`. It starts Anvil and PostgreSQL, deploys the
HOPR contracts, and waits for blokli-light to begin following the finalised head.

See [`design/architecture.md`](design/architecture.md) for data flow and
[`bloklid/migrations/0001_initial.sql`](bloklid/migrations/0001_initial.sql) for the authoritative schema.
