//! File configuration for the raw-log daemon.

use std::{fs, path::Path, time::Duration};

use serde::Deserialize;
use thiserror::Error;

/// Daemon configuration loaded from TOML.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// HOPR binding network name.
    #[serde(default = "default_network")]
    pub network: String,
    /// Ethereum HTTP RPC endpoint.
    #[serde(default = "default_rpc_url")]
    pub rpc_url: String,
    /// Initial log-request range limit.
    #[serde(default = "default_max_block_range")]
    pub max_block_range: u64,
    /// PostgreSQL writer connection settings.
    pub database: DatabaseConfig,
    /// Indexer progress and source polling settings.
    #[serde(default)]
    pub indexer: IndexerConfig,
    /// Declarative custom sources reconciled at startup.
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
}

/// PostgreSQL writer connection settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseConfig {
    /// PostgreSQL URL used by the daemon writer.
    pub url: String,
    /// Maximum writer-pool connections.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// Optional reader role settings.
    #[serde(default)]
    pub reader: ReaderConfig,
}

/// Optional read-only role settings.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReaderConfig {
    /// Reader login name.
    #[serde(default = "default_reader_user")]
    pub user: String,
}

/// Indexing policy settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerConfig {
    /// Overrides the bindings start block when non-zero.
    #[serde(default)]
    pub start_block: u64,
    /// Built-in source event selection.
    #[serde(default)]
    pub topics: TopicsMode,
    /// Interval for observing custom-source changes.
    #[serde(default = "default_source_poll_interval", with = "humantime_serde")]
    pub source_poll_interval: Duration,
    /// Maximum concurrent custom-source backfills.
    #[serde(default = "default_backfill_concurrency")]
    pub backfill_concurrency: usize,
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self { start_block: 0, topics: TopicsMode::Known, source_poll_interval: default_source_poll_interval(), backfill_concurrency: default_backfill_concurrency() }
    }
}

/// Built-in event selection policy.
#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TopicsMode { #[default] Known, All }

/// Declarative custom source.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    /// Stable source name.
    pub name: String,
    /// Contract address.
    pub address: String,
    /// First block to index.
    pub from_block: u64,
    /// Omitted means every event for this address.
    pub events: Option<Vec<String>>,
}

impl Config {
    /// Loads and validates one TOML configuration file.
    pub fn from_file(path: &Path) -> Result<Self, ConfigError> {
        let contents = fs::read_to_string(path).map_err(ConfigError::Read)?;
        let config = toml::from_str(&contents).map_err(ConfigError::Parse)?;
        Ok(config)
    }
}

fn default_network() -> String { "jura-dev".to_owned() }
fn default_rpc_url() -> String { "http://localhost:8545".to_owned() }
const fn default_max_block_range() -> u64 { 10_000 }
const fn default_max_connections() -> u32 { 5 }
fn default_reader_user() -> String { "blokli_reader".to_owned() }
fn default_source_poll_interval() -> Duration { Duration::from_secs(30) }
const fn default_backfill_concurrency() -> usize { 2 }

/// Configuration loading failure.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// File access failed.
    #[error("failed to read configuration")]
    Read(#[source] std::io::Error),
    /// TOML structure was invalid.
    #[error("invalid configuration")]
    Parse(#[source] toml::de::Error),
}
