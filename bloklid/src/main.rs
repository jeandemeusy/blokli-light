use std::{fs::OpenOptions, io::Write, path::PathBuf, process::ExitCode, time::Duration};

use anyhow::{Error as AnyhowError, Result, anyhow, bail};
use blokli_light::{
    builtin::{BuiltinTopics, chain_id as binding_chain_id, service_registry_source, sources as builtin_sources},
    config::{Config, SourceConfig, TopicsMode},
    database::{Database, NewSource, SourceKind},
    indexer::{IndexerConfig, run},
    rpc::RpcClient,
    source::{format_address, normalize_events, parse_address},
    telemetry,
};
use clap::{Parser, Subcommand, ValueEnum};
use url::Url;

/// Raw-log indexer for HOPR and operator-defined Ethereum contracts.
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

    #[arg(long, env = "BLOKLI_LIGHT_NETWORK")]
    network: Option<String>,

    #[arg(long, env = "BLOKLI_LIGHT_INDEXER_TOPICS", value_enum)]
    topics: Option<TopicMode>,

    /// Override the bindings start block when non-zero.
    #[arg(long, env = "BLOKLI_LIGHT_INDEXER_START_BLOCK")]
    indexer_start_block: Option<u64>,

    /// Maximum concurrent lower-priority custom backfills.
    #[arg(long, env = "BLOKLI_LIGHT_BACKFILL_CONCURRENCY")]
    backfill_concurrency: Option<usize>,

    #[arg(long, env = "BLOKLI_LIGHT_CONTRACT_CHANNELS")]
    contract_channels: Option<String>,
    #[arg(long, env = "BLOKLI_LIGHT_CONTRACT_ANNOUNCEMENTS")]
    contract_announcements: Option<String>,
    #[arg(long, env = "BLOKLI_LIGHT_CONTRACT_NODE_SAFE_REGISTRY")]
    contract_node_safe_registry: Option<String>,
    #[arg(long, env = "BLOKLI_LIGHT_CONTRACT_NODE_STAKE_FACTORY")]
    contract_node_stake_factory: Option<String>,
    #[arg(long, env = "BLOKLI_LIGHT_CONTRACT_TICKET_PRICE_ORACLE")]
    contract_ticket_price_oracle: Option<String>,
    #[arg(long, env = "BLOKLI_LIGHT_CONTRACT_WINNING_PROBABILITY_ORACLE")]
    contract_winning_probability_oracle: Option<String>,
    #[arg(long, env = "BLOKLI_LIGHT_CONTRACT_SERVICE_REGISTRY")]
    contract_service_registry: Option<String>,

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

#[derive(Clone, Copy, Debug, ValueEnum)]
enum TopicMode {
    Known,
    All,
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
    /// List built-in and custom source state.
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
    let mut contract_overrides = file_config
        .as_ref()
        .map(|config| config.contracts.clone())
        .unwrap_or_default();
    contract_overrides.channels = args.contract_channels.clone().or(contract_overrides.channels);
    contract_overrides.announcements = args.contract_announcements.clone().or(contract_overrides.announcements);
    contract_overrides.node_safe_registry = args
        .contract_node_safe_registry
        .clone()
        .or(contract_overrides.node_safe_registry);
    contract_overrides.node_stake_factory = args
        .contract_node_stake_factory
        .clone()
        .or(contract_overrides.node_stake_factory);
    contract_overrides.ticket_price_oracle = args
        .contract_ticket_price_oracle
        .clone()
        .or(contract_overrides.ticket_price_oracle);
    contract_overrides.winning_probability_oracle = args
        .contract_winning_probability_oracle
        .clone()
        .or(contract_overrides.winning_probability_oracle);
    contract_overrides.service_registry = args
        .contract_service_registry
        .clone()
        .or(contract_overrides.service_registry);

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
            let network = args
                .network
                .as_deref()
                .or_else(|| file_config.as_ref().map(|config| config.network.as_str()))
                .unwrap_or("jura-dev");
            let max_block_range = args
                .max_block_range
                .or_else(|| file_config.as_ref().map(|config| config.max_block_range))
                .unwrap_or(10_000);
            let topics =
                args.topics
                    .unwrap_or_else(|| match file_config.as_ref().map(|config| config.indexer.topics) {
                        Some(TopicsMode::All) => TopicMode::All,
                        _ => TopicMode::Known,
                    });
            let declarative_sources = file_config
                .as_ref()
                .map(|config| config.sources.as_slice())
                .unwrap_or_default();
            let start_block = args
                .indexer_start_block
                .or_else(|| file_config.as_ref().map(|config| config.indexer.start_block))
                .unwrap_or_default();
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
                    network,
                    topics,
                    start_block,
                    declarative_sources,
                    reconcile_config_sources: file_config.is_some(),
                    contracts: &contract_overrides,
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
    network: &'a str,
    topics: TopicMode,
    start_block: u64,
    declarative_sources: &'a [SourceConfig],
    reconcile_config_sources: bool,
    contracts: &'a blokli_light::config::ContractsConfig,
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
    let expected_chain_id = match binding_chain_id(config.network) {
        Ok(chain_id) => chain_id,
        Err(error) => {
            tracing::error!(%error, "failed to resolve binding chain ID");
            return ExitCode::FAILURE;
        }
    };
    let rpc_chain_id = match rpc.chain_id().await {
        Ok(chain_id) => chain_id,
        Err(error) => {
            tracing::error!(%error, "failed to read RPC chain ID");
            return ExitCode::FAILURE;
        }
    };
    if rpc_chain_id != expected_chain_id {
        tracing::error!(
            rpc_chain_id,
            expected_chain_id,
            network = config.network,
            "RPC chain does not match configured network"
        );
        return ExitCode::FAILURE;
    }
    if let Err(error) = database.ensure_chain_identity(config.network, rpc_chain_id).await {
        tracing::error!(%error, "database chain identity check failed");
        return ExitCode::FAILURE;
    }
    let topics = match config.topics {
        TopicMode::Known => BuiltinTopics::Known,
        TopicMode::All => BuiltinTopics::All,
    };
    let mut builtins = match builtin_sources(config.network, topics) {
        Ok(sources) => sources,
        Err(error) => {
            tracing::error!(%error, "failed to resolve built-in sources");
            return ExitCode::FAILURE;
        }
    };
    if !builtins.iter().any(|source| source.name == "service-registry")
        && let Some(address) = config.contracts.service_registry.as_deref()
    {
        let address = match parse_address(address) {
            Ok(address) => address,
            Err(error) => {
                tracing::error!(%error, "invalid service-registry contract override");
                return ExitCode::FAILURE;
            }
        };
        let from_block = builtins
            .first()
            .map(|source| source.from_block)
            .unwrap_or(config.start_block);
        builtins.push(service_registry_source(address, from_block, topics));
    }
    for mut source in builtins {
        if let Some(address) = config.contracts.address_for(&source.name) {
            source.address = match parse_address(address) {
                Ok(address) => address,
                Err(error) => {
                    tracing::error!(%error, source = %source.name, "invalid built-in contract override");
                    return ExitCode::FAILURE;
                }
            };
        }
        if config.start_block != 0 {
            source.from_block = config.start_block;
        }
        if let Err(error) = database.upsert_source(&source).await {
            tracing::error!(%error, source = %source.name, "failed to reconcile built-in source");
            return ExitCode::FAILURE;
        }
    }
    for source in config.declarative_sources {
        if let Err(error) = reconcile_custom_source(database, &rpc, source).await {
            tracing::error!(%error, source = %source.name, "failed to reconcile declarative source");
            return ExitCode::FAILURE;
        }
    }
    if config.reconcile_config_sources {
        let configured_names = config
            .declarative_sources
            .iter()
            .map(|source| source.name.clone())
            .collect::<Vec<_>>();
        if let Err(error) = database.disable_removed_config_sources(&configured_names).await {
            tracing::error!(%error, "failed to disable sources removed from configuration");
            return ExitCode::FAILURE;
        }
    }
    if let Err(error) = database.ensure_logs_origin().await {
        tracing::error!(%error, "log origin validation failed");
        return ExitCode::FAILURE;
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

async fn reconcile_custom_source(database: &Database, rpc: &RpcClient, source: &SourceConfig) -> Result<()> {
    let address = parse_address(&source.address)?;
    if !rpc.has_code(&address).await? {
        bail!("source address {} has no contract code", source.address);
    }
    let events = source.events.as_deref().map(normalize_events).transpose()?;
    database
        .upsert_config_source(&NewSource {
            name: source.name.clone(),
            address,
            kind: SourceKind::Custom,
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
                    SourceKind::Builtin => "builtin",
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
                            SourceKind::Builtin => "builtin",
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
