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
    /// Maximum JSON-RPC request rate. Zero disables throttling.
    #[serde(default)]
    pub max_rpc_requests_per_sec: u32,
    /// Optional built-in address overrides.
    #[serde(default)]
    pub contracts: ContractsConfig,
    /// PostgreSQL writer connection settings.
    pub database: DatabaseConfig,
    /// Indexer progress and source polling settings.
    #[serde(default)]
    pub indexer: IndexerConfig,
    /// Declarative custom sources reconciled at startup.
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
    /// Optional OpenTelemetry export settings.
    #[serde(default)]
    pub telemetry: TelemetryConfig,
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
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReaderConfig {
    /// Reader login name.
    #[serde(default = "default_reader_user")]
    pub user: String,
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            user: default_reader_user(),
        }
    }
}

/// Optional overrides for addresses supplied by `hopr-bindings`.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContractsConfig {
    pub channels: Option<String>,
    pub announcements: Option<String>,
    pub node_safe_registry: Option<String>,
    pub node_stake_factory: Option<String>,
    pub ticket_price_oracle: Option<String>,
    pub winning_probability_oracle: Option<String>,
    pub service_registry: Option<String>,
}

impl ContractsConfig {
    /// Returns the override corresponding to a stable built-in source name.
    pub fn address_for(&self, source_name: &str) -> Option<&str> {
        match source_name {
            "channels" => self.channels.as_deref(),
            "announcements" => self.announcements.as_deref(),
            "node-safe-registry" => self.node_safe_registry.as_deref(),
            "node-stake-factory" => self.node_stake_factory.as_deref(),
            "ticket-price-oracle" => self.ticket_price_oracle.as_deref(),
            "winning-probability-oracle" => self.winning_probability_oracle.as_deref(),
            "service-registry" => self.service_registry.as_deref(),
            _ => None,
        }
    }
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
    /// Finality depth subtracted from the RPC head.
    #[serde(default = "default_confirmations")]
    pub confirmations: u64,
    /// Delay between head polling iterations.
    #[serde(default = "default_poll_interval", with = "humantime_serde")]
    pub poll_interval: Duration,
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            start_block: 0,
            topics: TopicsMode::Known,
            source_poll_interval: default_source_poll_interval(),
            backfill_concurrency: default_backfill_concurrency(),
            confirmations: default_confirmations(),
            poll_interval: default_poll_interval(),
        }
    }
}

/// Built-in event selection policy.
#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TopicsMode {
    #[default]
    Known,
    All,
}

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

/// OpenTelemetry Protocol export settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    /// Collector base URL. Omitted disables OTLP export.
    #[serde(default)]
    pub otlp_endpoint: Option<String>,
    /// Comma-separated subset of `metrics,traces,logs`.
    #[serde(default = "default_otlp_signals")]
    pub otlp_signals: String,
    /// Interval between metric exports.
    #[serde(default = "default_metric_export_interval", with = "humantime_serde")]
    pub metric_export_interval: Duration,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            otlp_endpoint: None,
            otlp_signals: default_otlp_signals(),
            metric_export_interval: default_metric_export_interval(),
        }
    }
}

impl Config {
    /// Loads and validates one TOML configuration file.
    pub fn from_file(path: &Path) -> Result<Self, ConfigError> {
        let contents = fs::read_to_string(path).map_err(ConfigError::Read)?;
        let config = toml::from_str(&contents).map_err(ConfigError::Parse)?;
        Ok(config)
    }
}

fn default_network() -> String {
    "jura-dev".to_owned()
}
fn default_rpc_url() -> String {
    "http://localhost:8545".to_owned()
}
const fn default_max_block_range() -> u64 {
    10_000
}
const fn default_max_connections() -> u32 {
    5
}
fn default_reader_user() -> String {
    "blokli_reader".to_owned()
}
fn default_source_poll_interval() -> Duration {
    Duration::from_secs(30)
}
const fn default_backfill_concurrency() -> usize {
    2
}
const fn default_confirmations() -> u64 {
    3
}
fn default_poll_interval() -> Duration {
    Duration::from_secs(5)
}
fn default_otlp_signals() -> String {
    "metrics".to_owned()
}
fn default_metric_export_interval() -> Duration {
    Duration::from_secs(15)
}

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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Config, TopicsMode};

    #[test]
    fn applies_safe_defaults_to_minimal_configuration() {
        let config: Config = toml::from_str(
            r#"
            [database]
            url = "postgresql://localhost/blokli_light"
            "#,
        )
        .expect("minimal configuration should parse");

        assert_eq!(config.database.max_connections, 5);
        assert_eq!(config.database.reader.user, "blokli_reader");
        assert!(matches!(config.indexer.topics, TopicsMode::Known));
        assert_eq!(config.indexer.source_poll_interval, Duration::from_secs(30));
        assert_eq!(config.indexer.backfill_concurrency, 2);
    }

    #[test]
    fn rejects_unknown_keys() {
        let result = toml::from_str::<Config>(
            r#"
            unsupported = true
            [database]
            url = "postgresql://localhost/blokli_light"
            "#,
        );
        assert!(result.is_err());
    }
}
