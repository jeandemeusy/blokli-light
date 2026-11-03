use std::{collections::BTreeMap, fs::OpenOptions, io::Write, path::PathBuf, process::ExitCode, time::Duration};

use anyhow::{Error as AnyhowError, Result, anyhow, bail};
use blokli_light::{
    config::{Config, SourceConfig},
    database::{Database, NewSource, SourceKind},
    indexer::{IndexerConfig, run},
    rpc::RpcClient,
    source::{format_address, normalize_events, parse_address},
    telemetry,
};
use clap::{Parser, Subcommand};
use url::Url;

/// Raw-log indexer for deployment-defined Ethereum contracts.
#[derive(Debug, Parser)]
#[command(name = "blokli-light", version, about)]
struct Args {
    /// TOML configuration file.
    #[arg(short = 'c', long, env = "BLOKLI_LIGHT_CONFIG_PATH")]
    config: Option<PathBuf>,

    /// PostgreSQL connection string used for migrations and storage.
    #[arg(long, env = "BLOKLI_LIGHT_DATABASE_URL")]
    database_url: Option<String>,

    /// Maximum PostgreSQL writer-pool connections.
    #[arg(long, env = "BLOKLI_LIGHT_DATABASE_MAX_CONNECTIONS")]
    database_max_connections: Option<u32>,

    /// Ethereum JSON-RPC URL, required by `source add` to validate contract code.
    #[arg(long, env = "BLOKLI_LIGHT_RPC_URL")]
    rpc_url: Option<String>,

    /// Maximum JSON-RPC requests per second. Zero disables throttling.
    #[arg(long, env = "BLOKLI_LIGHT_MAX_RPC_REQUESTS_PER_SEC")]
    max_rpc_requests_per_sec: Option<u32>,

    /// Number of confirmations excluded from the durable watermark.
    #[arg(long, env = "BLOKLI_LIGHT_CONFIRMATIONS")]
    confirmations: Option<u64>,

    /// Initial maximum number of blocks fetched by an eth_getLogs request.
    #[arg(long, env = "BLOKLI_LIGHT_MAX_BLOCK_RANGE")]
    max_block_range: Option<u64>,

    /// Seconds between finality-head polls.
    #[arg(long, env = "BLOKLI_LIGHT_POLL_INTERVAL_SECONDS")]
    poll_interval_seconds: Option<u64>,

    /// Maximum seconds before changed source rows are observed.
    #[arg(long, env = "BLOKLI_LIGHT_SOURCE_POLL_INTERVAL_SECONDS")]
    source_poll_interval_seconds: Option<u64>,

    /// EIP-155 chain ID expected from the RPC endpoint.
    #[arg(long, env = "BLOKLI_LIGHT_CHAIN_ID")]
    chain_id: Option<u64>,

    /// Deployment label persisted in chain_info.network for downstream consumers.
    #[arg(long, env = "BLOKLI_LIGHT_NETWORK")]
    network: Option<String>,

    /// Maximum concurrent lower-priority custom backfills.
    #[arg(long, env = "BLOKLI_LIGHT_BACKFILL_CONCURRENCY")]
    backfill_concurrency: Option<usize>,

    /// Read-only PostgreSQL role name.
    #[arg(long, env = "BLOKLI_LIGHT_READER_USER")]
    reader_user: Option<String>,

    /// Read-only PostgreSQL role password.
    #[arg(long, env = "BLOKLI_LIGHT_READER_PASSWORD", hide_env_values = true)]
    reader_password: Option<String>,

    /// OTLP collector endpoint. Supports grpc://, http://, and https://.
    #[arg(long, env = "BLOKLI_LIGHT_OTLP_ENDPOINT")]
    otlp_endpoint: Option<String>,

    /// Comma-separated OTLP signals: metrics,traces,logs.
    #[arg(long, env = "BLOKLI_LIGHT_OTLP_SIGNALS")]
    otlp_signals: Option<String>,

    /// Interval in seconds between OTLP metric exports.
    #[arg(long, env = "BLOKLI_LIGHT_METRIC_EXPORT_INTERVAL_SECONDS")]
    metric_export_interval_seconds: Option<u64>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Write a documented example configuration without overwriting an existing file.
    GenerateConfig {
        #[arg(default_value = "blokli-light.toml")]
        output: PathBuf,
    },
    /// Apply PostgreSQL migrations and exit.
    Migrate,
    /// Manage contracts whose raw logs are indexed.
    Source {
        #[command(subcommand)]
        command: SourceCommand,
    },
    /// Show chain lag, raw row count, and per-source completeness.
    Status,
    /// Manage database access.
    Db {
        #[command(subcommand)]
        command: DbCommand,
    },
}

#[derive(Debug, Subcommand)]
enum DbCommand {
    /// Create or update the restricted direct-SQL reader role.
    Reader {
        /// One-off privileged URL when the writer cannot create roles.
        #[arg(long, env = "BLOKLI_LIGHT_DATABASE_ADMIN_URL")]
        admin_url: Option<String>,
        /// Print the Arrakis DATA_SOURCES setting after provisioning.
        #[arg(long)]
        print_url: bool,
    },
}

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
        #[arg(long)]
        events: Vec<String>,
    },
    /// Add event signatures to a source and re-backfill it idempotently.
    Widen {
        /// Existing custom source name.
        #[arg(long)]
        name: String,
        /// Text signatures or 0x-prefixed topic0 hashes to add.
        #[arg(long, required = true)]
        events: Vec<String>,
    },
    /// Stop following a source without deleting its stored logs.
    Disable {
        /// Existing custom source name.
        #[arg(long)]
        name: String,
    },
    /// List deployment-defined and dynamically added source state.
    List,
}

#[tokio::main]
async fn main() -> ExitCode {
    if let Err(error) = telemetry::install_base_subscriber() {
        eprintln!("failed to initialise tracing: {error}");
        return ExitCode::FAILURE;
    }
    let args = Args::parse();
    if let Some(Command::GenerateConfig { output }) = &args.command {
        return match write_example_config(output) {
            Ok(()) => {
                println!("wrote {}", output.display());
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("failed to write configuration: {error}");
                ExitCode::FAILURE
            }
        };
    }
    let file_config = match args.config.as_deref().map(Config::from_file).transpose() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("failed to load configuration: {error}");
            return ExitCode::FAILURE;
        }
    };
    let mut telemetry_config = file_config
        .as_ref()
        .map(|config| config.telemetry.clone())
        .unwrap_or_default();
    telemetry_config.otlp_endpoint = args.otlp_endpoint.clone().or(telemetry_config.otlp_endpoint);
    telemetry_config.otlp_signals = args.otlp_signals.clone().unwrap_or(telemetry_config.otlp_signals);
    if let Some(seconds) = args.metric_export_interval_seconds {
        telemetry_config.metric_export_interval = Duration::from_secs(seconds);
    }
    let _telemetry_handles = match telemetry::configure(&telemetry_config) {
        Ok(handles) => handles,
        Err(error) => {
            tracing::error!(%error, "failed to configure telemetry");
            return ExitCode::FAILURE;
        }
    };

    let database_url = args
        .database_url
        .as_deref()
        .or_else(|| file_config.as_ref().map(|config| config.database.url.as_str()));
    let Some(database_url) = database_url else {
        tracing::error!("BLOKLI_LIGHT_DATABASE_URL or --database-url is required");
        return ExitCode::FAILURE;
    };
    let max_connections = args
        .database_max_connections
        .or_else(|| file_config.as_ref().map(|config| config.database.max_connections));
    let database = match Database::connect_with_max_connections(database_url, max_connections.unwrap_or(5)).await {
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
        Some(Command::GenerateConfig { .. }) => ExitCode::SUCCESS,
        Some(Command::Migrate) => ExitCode::SUCCESS,
        Some(Command::Source { command }) => {
            let rpc_url = args
                .rpc_url
                .as_deref()
                .or_else(|| file_config.as_ref().map(|config| config.rpc_url.as_str()));
            run_source_command(&database, rpc_url, command).await
        }
        Some(Command::Status) => run_status_command(&database).await,
        Some(Command::Db { command }) => {
            let reader_user = args
                .reader_user
                .as_deref()
                .or_else(|| file_config.as_ref().map(|config| config.database.reader.user.as_str()))
                .unwrap_or("blokli_reader");
            run_db_command(
                &database,
                database_url,
                reader_user,
                args.reader_password.as_deref(),
                command,
            )
            .await
        }
        None => {
            let rpc_url = args
                .rpc_url
                .as_deref()
                .or_else(|| file_config.as_ref().map(|config| config.rpc_url.as_str()));
            let chain_id = args
                .chain_id
                .or_else(|| file_config.as_ref().map(|config| config.chain.id));
            let Some(chain_id) = chain_id else {
                tracing::error!("BLOKLI_LIGHT_CHAIN_ID, --chain-id, or chain.id is required");
                return ExitCode::FAILURE;
            };
            let network = args
                .network
                .as_deref()
                .or_else(|| file_config.as_ref().map(|config| config.chain.network.as_str()));
            let Some(network) = network else {
                tracing::error!("BLOKLI_LIGHT_NETWORK, --network, or chain.network is required");
                return ExitCode::FAILURE;
            };
            let max_block_range = args
                .max_block_range
                .or_else(|| file_config.as_ref().map(|config| config.max_block_range))
                .unwrap_or(10_000);
            let empty_sources = BTreeMap::new();
            let declarative_sources = file_config
                .as_ref()
                .map(|config| &config.source)
                .unwrap_or(&empty_sources);
            let reader_user = args
                .reader_user
                .as_deref()
                .or_else(|| file_config.as_ref().map(|config| config.database.reader.user.as_str()))
                .unwrap_or("blokli_reader");
            if let Some(password) = args.reader_password.as_deref()
                && let Err(error) = database.provision_reader(reader_user, password).await
            {
                tracing::error!(%error, "failed to provision database reader role");
                return ExitCode::FAILURE;
            }
            run_indexer(
                &database,
                RuntimeConfig {
                    rpc_url,
                    confirmations: args
                        .confirmations
                        .or_else(|| file_config.as_ref().map(|config| config.indexer.confirmations))
                        .unwrap_or(3),
                    max_block_range,
                    poll_interval: args
                        .poll_interval_seconds
                        .map(Duration::from_secs)
                        .or_else(|| file_config.as_ref().map(|config| config.indexer.poll_interval))
                        .unwrap_or(Duration::from_secs(5)),
                    source_poll_interval: args
                        .source_poll_interval_seconds
                        .map(Duration::from_secs)
                        .or_else(|| file_config.as_ref().map(|config| config.indexer.source_poll_interval))
                        .unwrap_or(Duration::from_secs(30)),
                    max_rpc_requests_per_sec: args
                        .max_rpc_requests_per_sec
                        .or_else(|| file_config.as_ref().map(|config| config.max_rpc_requests_per_sec))
                        .unwrap_or_default(),
                    backfill_concurrency: args
                        .backfill_concurrency
                        .or_else(|| file_config.as_ref().map(|config| config.indexer.backfill_concurrency))
                        .unwrap_or(2),
                    chain_id,
                    network,
                    declarative_sources,
                    reconcile_config_sources: file_config.is_some(),
                },
            )
            .await
        }
    }
}

fn write_example_config(path: &PathBuf) -> Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(include_bytes!("../example-config.toml"))?;
    Ok(())
}

struct RuntimeConfig<'a> {
    rpc_url: Option<&'a str>,
    confirmations: u64,
    max_block_range: u64,
    poll_interval: Duration,
    source_poll_interval: Duration,
    max_rpc_requests_per_sec: u32,
    backfill_concurrency: usize,
    chain_id: u64,
    network: &'a str,
    declarative_sources: &'a BTreeMap<String, SourceConfig>,
    reconcile_config_sources: bool,
}

async fn run_indexer(database: &Database, config: RuntimeConfig<'_>) -> ExitCode {
    let Some(rpc_url) = config.rpc_url else {
        tracing::error!("BLOKLI_LIGHT_RPC_URL or --rpc-url is required to run the indexer");
        return ExitCode::FAILURE;
    };
    let rpc = match RpcClient::new_with_rate_limit(rpc_url, config.max_rpc_requests_per_sec) {
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
    let rpc_chain_id = match rpc.chain_id().await {
        Ok(chain_id) => chain_id,
        Err(error) => {
            tracing::error!(%error, "failed to read RPC chain ID");
            return ExitCode::FAILURE;
        }
    };
    if rpc_chain_id != config.chain_id {
        tracing::error!(
            rpc_chain_id,
            configured_chain_id = config.chain_id,
            "RPC chain does not match the configured chain"
        );
        return ExitCode::FAILURE;
    }
    if let Err(error) = database.ensure_chain_identity(config.network, rpc_chain_id).await {
        tracing::error!(%error, "database chain identity check failed");
        return ExitCode::FAILURE;
    }
    for (name, source) in config.declarative_sources {
        if let Err(error) = reconcile_config_source(database, &rpc, name, source).await {
            tracing::error!(%error, source = %name, "failed to reconcile configured source");
            return ExitCode::FAILURE;
        }
    }
    if config.reconcile_config_sources {
        let configured_names = config.declarative_sources.keys().cloned().collect::<Vec<_>>();
        if let Err(error) = database.disable_removed_config_sources(&configured_names).await {
            tracing::error!(%error, "failed to disable sources removed from configuration");
            return ExitCode::FAILURE;
        }
    }
    match run(
        database,
        &rpc,
        IndexerConfig {
            confirmations: config.confirmations,
            max_block_range: config.max_block_range,
            poll_interval: config.poll_interval,
            source_poll_interval: config.source_poll_interval,
            backfill_concurrency: config.backfill_concurrency,
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

async fn reconcile_config_source(
    database: &Database,
    rpc: &RpcClient,
    name: &str,
    source: &SourceConfig,
) -> Result<()> {
    let address = parse_address(&source.address)?;
    if !rpc.has_code(&address).await? {
        bail!("source address {} has no contract code", source.address);
    }
    let events = source.events.as_deref().map(normalize_events).transpose()?;
    database
        .upsert_config_source(&NewSource {
            name: name.to_owned(),
            address,
            kind: SourceKind::Builtin,
            from_block: source.from_block,
            events,
        })
        .await?;
    Ok(())
}

async fn run_status_command(database: &Database) -> ExitCode {
    let result: Result<()> = async {
        let chain = database.chain_info().await?;
        let rows = database.log_count().await?;
        let lag = chain
            .head_block
            .map(|head| head.saturating_sub(chain.last_indexed_block));
        println!("indexed_block\t{}", chain.last_indexed_block);
        println!(
            "head_block\t{}",
            chain
                .head_block
                .map_or_else(|| "unknown".to_owned(), |head| head.to_string())
        );
        println!(
            "lag\t{}",
            lag.map_or_else(|| "unknown".to_owned(), |lag| lag.to_string())
        );
        println!("log_rows\t{rows}");
        println!("source\tkind\tfrom_block\tindexed_to\tlag\tenabled\taddress");
        for source in database.list_sources().await? {
            let source_lag = chain.head_block.map_or_else(
                || "unknown".to_owned(),
                |head| {
                    let indexed_to = u64::try_from(source.indexed_to).unwrap_or_default();
                    head.saturating_sub(indexed_to).to_string()
                },
            );
            println!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}",
                source.name,
                match source.kind {
                    SourceKind::Builtin => "configured",
                    SourceKind::Custom => "custom",
                },
                source.from_block,
                source.indexed_to,
                source_lag,
                source.enabled,
                format_address(&source.address)
            );
        }
        Ok(())
    }
    .await;

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "status command failed");
            ExitCode::FAILURE
        }
    }
}

async fn run_db_command(
    database: &Database,
    database_url: &str,
    reader_user: &str,
    reader_password: Option<&str>,
    command: DbCommand,
) -> ExitCode {
    let result: Result<()> = async {
        match command {
            DbCommand::Reader { admin_url, print_url } => {
                let password = reader_password
                    .ok_or_else(|| anyhow!("BLOKLI_LIGHT_READER_PASSWORD or --reader-password is required"))?;
                if let Some(admin_url) = admin_url {
                    Database::connect(&admin_url)
                        .await?
                        .provision_reader(reader_user, password)
                        .await?;
                } else {
                    database.provision_reader(reader_user, password).await?;
                }
                println!("provisioned read-only role {reader_user}");
                if print_url {
                    println!(
                        "DATA_SOURCES={}",
                        reader_connection_url(database_url, reader_user, password)?
                    );
                }
            }
        }
        Ok(())
    }
    .await;

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "database command failed");
            ExitCode::FAILURE
        }
    }
}

fn reader_connection_url(database_url: &str, user: &str, password: &str) -> Result<String> {
    let mut url = Url::parse(database_url)?;
    url.set_username(user)
        .map_err(|()| anyhow!("database URL cannot contain a username"))?;
    url.set_password(Some(password))
        .map_err(|()| anyhow!("database URL cannot contain a password"))?;
    Ok(url.into())
}

async fn run_source_command(database: &Database, rpc_url: Option<&str>, command: SourceCommand) -> ExitCode {
    let result: Result<()> = match command {
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
                .map_err(AnyhowError::from)
        }
        SourceCommand::Widen { name, events } => match normalize_events(&events) {
            Ok(events) => database
                .widen_source(&name, &events)
                .await
                .map(|source| println!("widened source {}", source.name))
                .map_err(AnyhowError::from),
            Err(error) => Err(error.into()),
        },
        SourceCommand::Disable { name } => database
            .disable_source(&name)
            .await
            .map(|()| println!("disabled source {name}"))
            .map_err(AnyhowError::from),
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
                            SourceKind::Builtin => "configured",
                            SourceKind::Custom => "custom",
                        },
                        source.from_block,
                        source.indexed_to,
                        source.enabled
                    );
                }
            })
            .map_err(AnyhowError::from),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "source command failed");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Args, Command, SourceCommand};

    #[test]
    fn event_signatures_with_commas_remain_one_cli_value() {
        let args = Args::try_parse_from([
            "blokli-light",
            "source",
            "add",
            "--name",
            "token",
            "--address",
            "0x1234567890123456789012345678901234567890",
            "--from-block",
            "1",
            "--events",
            "Transfer(address,address,uint256)",
        ])
        .expect("valid source arguments");

        let Some(Command::Source {
            command: SourceCommand::Add { events, .. },
        }) = args.command
        else {
            panic!("source add command expected");
        };
        assert_eq!(events, ["Transfer(address,address,uint256)"]);
    }
}
