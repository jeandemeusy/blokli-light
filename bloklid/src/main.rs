use std::{process::ExitCode, time::Duration};

use blokli_light::{
    builtin::{BuiltinTopics, sources as builtin_sources},
    database::{Database, NewSource, SourceKind},
    indexer::{IndexerConfig, run},
    rpc::RpcClient,
    source::{format_address, normalize_events, parse_address},
};
use clap::{Parser, Subcommand, ValueEnum};

/// Raw-log indexer for HOPR and operator-defined Ethereum contracts.
#[derive(Debug, Parser)]
#[command(name = "blokli-light", version, about)]
struct Args {
    /// PostgreSQL connection string used for migrations and storage.
    #[arg(long, env = "BLOKLI_LIGHT_DATABASE_URL")]
    database_url: Option<String>,

    /// Ethereum JSON-RPC URL, required by `source add` to validate contract code.
    #[arg(long, env = "BLOKLI_LIGHT_RPC_URL")]
    rpc_url: Option<String>,

    /// Number of confirmations excluded from the durable watermark.
    #[arg(long, env = "BLOKLI_LIGHT_CONFIRMATIONS", default_value_t = 3)]
    confirmations: u64,

    /// Initial maximum number of blocks fetched by an eth_getLogs request.
    #[arg(long, env = "BLOKLI_LIGHT_MAX_BLOCK_RANGE", default_value_t = 10_000)]
    max_block_range: u64,

    /// Seconds between finality-head polls.
    #[arg(long, env = "BLOKLI_LIGHT_POLL_INTERVAL_SECONDS", default_value_t = 5)]
    poll_interval_seconds: u64,

    #[arg(long, env = "BLOKLI_LIGHT_NETWORK", default_value = "jura-dev")]
    network: String,

    #[arg(long, env = "BLOKLI_LIGHT_INDEXER_TOPICS", value_enum, default_value_t = TopicMode::Known)]
    topics: TopicMode,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Apply PostgreSQL migrations and exit.
    Migrate,
    /// Manage contracts whose raw logs are indexed.
    Source {
        #[command(subcommand)]
        command: SourceCommand,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum TopicMode { Known, All }

#[derive(Debug, Subcommand)]
enum SourceCommand {
    /// Add a custom contract source after validating that code exists at its address.
    Add {
        /// Stable name used by status and management commands.
        #[arg(long)]
        name: String,
        /// Contract address.
        #[arg(long)]
        address: String,
        /// First block to fetch. This value is required because no safe default exists.
        #[arg(long)]
        from_block: u64,
        /// Text signatures or 0x-prefixed topic0 hashes. Omit to index every event.
        #[arg(long, value_delimiter = ',')]
        events: Vec<String>,
    },
    /// Add event signatures to a source and re-backfill it idempotently.
    Widen {
        /// Existing custom source name.
        #[arg(long)]
        name: String,
        /// Text signatures or 0x-prefixed topic0 hashes to add.
        #[arg(long, required = true, value_delimiter = ',')]
        events: Vec<String>,
    },
    /// Stop following a source without deleting its stored logs.
    Disable {
        /// Existing custom source name.
        #[arg(long)]
        name: String,
    },
    /// List built-in and custom source state.
    List,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();

    let Some(database_url) = args.database_url else {
        tracing::error!("BLOKLI_LIGHT_DATABASE_URL or --database-url is required");
        return ExitCode::FAILURE;
    };

    let database = match Database::connect(&database_url).await {
        Ok(database) => database,
        Err(error) => {
            tracing::error!(%error, "failed to connect to PostgreSQL");
            return ExitCode::FAILURE;
        }
    };

    if let Err(error) = database.migrate().await {
        tracing::error!(%error, "failed to apply database migrations");
        return ExitCode::FAILURE;
    }

    match args.command {
        Some(Command::Migrate) => ExitCode::SUCCESS,
        Some(Command::Source { command }) => run_source_command(&database, args.rpc_url.as_deref(), command).await,
        None => {
            run_indexer(
                &database,
                args.rpc_url.as_deref(),
                args.confirmations,
                args.max_block_range,
                args.poll_interval_seconds,
            &args.network,
            args.topics,
            )
            .await
        }
    }
}

async fn run_indexer(
    database: &Database,
    rpc_url: Option<&str>,
    confirmations: u64,
    max_block_range: u64,
    poll_interval_seconds: u64,
    network: &str,
    topics: TopicMode,
) -> ExitCode {
    let Some(rpc_url) = rpc_url else {
        tracing::error!("BLOKLI_LIGHT_RPC_URL or --rpc-url is required to run the indexer");
        return ExitCode::FAILURE;
    };
    let rpc = match RpcClient::new(rpc_url) {
        Ok(rpc) => rpc,
        Err(error) => {
            tracing::error!(%error, "invalid RPC client configuration");
            return ExitCode::FAILURE;
        }
    };
    let Some(_lock) = (match database.try_advisory_lock().await {
        Ok(lock) => lock,
        Err(error) => {
            tracing::error!(%error, "failed to acquire indexer advisory lock");
            return ExitCode::FAILURE;
        }
    }) else {
        tracing::error!("another blokli-light writer already holds the advisory lock");
        return ExitCode::FAILURE;
    };
    let topics = match topics { TopicMode::Known => BuiltinTopics::Known, TopicMode::All => BuiltinTopics::All };
    let builtins = match builtin_sources(network, topics) {
        Ok(sources) => sources,
        Err(error) => { tracing::error!(%error, "failed to resolve built-in sources"); return ExitCode::FAILURE; }
    };
    for source in builtins {
        if let Err(error) = database.upsert_source(&source).await {
            tracing::error!(%error, source = %source.name, "failed to reconcile built-in source");
            return ExitCode::FAILURE;
        }
    }

    match run(
        database,
        &rpc,
        IndexerConfig {
            confirmations,
            max_block_range,
            poll_interval: Duration::from_secs(poll_interval_seconds),
        },
    )
    .await
    {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "indexer stopped");
            ExitCode::FAILURE
        }
    }
}

async fn run_source_command(database: &Database, rpc_url: Option<&str>, command: SourceCommand) -> ExitCode {
    let result: anyhow::Result<()> = match command {
        SourceCommand::Add {
            name,
            address,
            from_block,
            events,
        } => {
            let rpc_url = match rpc_url {
                Some(rpc_url) => rpc_url,
                None => {
                    tracing::error!("BLOKLI_LIGHT_RPC_URL or --rpc-url is required for source add");
                    return ExitCode::FAILURE;
                }
            };
            let address = match parse_address(&address) {
                Ok(address) => address,
                Err(error) => {
                    tracing::error!(%error, "invalid source address");
                    return ExitCode::FAILURE;
                }
            };
            let rpc = match RpcClient::new(rpc_url) {
                Ok(rpc) => rpc,
                Err(error) => {
                    tracing::error!(%error, "invalid RPC client configuration");
                    return ExitCode::FAILURE;
                }
            };
            match rpc.has_code(&address).await {
                Ok(true) => {}
                Ok(false) => {
                    tracing::error!(address = %format_address(&address), "source address has no contract code at the current head");
                    return ExitCode::FAILURE;
                }
                Err(error) => {
                    tracing::error!(%error, "failed to validate source contract code");
                    return ExitCode::FAILURE;
                }
            }

            let events = if events.is_empty() {
                None
            } else {
                match normalize_events(&events) {
                    Ok(events) => Some(events),
                    Err(error) => {
                        tracing::error!(%error, "invalid source events");
                        return ExitCode::FAILURE;
                    }
                }
            };
            database
                .upsert_source(&NewSource {
                    name,
                    address,
                    kind: SourceKind::Custom,
                    from_block,
                    events,
                })
                .await
                .map(|source| println!("added source {} ({})", source.name, format_address(&source.address)))
                .map_err(anyhow::Error::from)
        }
        SourceCommand::Widen { name, events } => match normalize_events(&events) {
            Ok(events) => database
                .widen_source(&name, &events)
                .await
                .map(|source| println!("widened source {}", source.name))
                .map_err(anyhow::Error::from),
            Err(error) => Err(error.into()),
        },
        SourceCommand::Disable { name } => database
            .disable_source(&name)
            .await
            .map(|()| println!("disabled source {name}"))
            .map_err(anyhow::Error::from),
        SourceCommand::List => database
            .list_sources()
            .await
            .map(|sources| {
                for source in sources {
                    println!(
                        "{}\t{}\t{}\t{}\t{}\t{}",
                        source.name,
                        format_address(&source.address),
                        match source.kind {
                            SourceKind::Builtin => "builtin",
                            SourceKind::Custom => "custom",
                        },
                        source.from_block,
                        source.indexed_to,
                        source.enabled
                    );
                }
            })
            .map_err(anyhow::Error::from),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "source command failed");
            ExitCode::FAILURE
        }
    }
}
