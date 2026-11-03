//! File configuration for the raw-log daemon.

use std::{collections::BTreeMap, fs, path::Path, time::Duration};

use serde::Deserialize;
use thiserror::Error;

/// Daemon configuration loaded from TOML.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Identity of the single EVM chain indexed by this deployment.
    pub chain: ChainConfig,
    /// Ethereum HTTP RPC endpoint.
    #[serde(default = "default_rpc_url")]
    pub rpc_url: String,
    /// Initial log-request range limit.
    #[serde(default = "default_max_block_range")]
    pub max_block_range: u64,
    /// Maximum JSON-RPC request rate. Zero disables throttling.
    #[serde(default)]
    pub max_rpc_requests_per_sec: u32,
    /// PostgreSQL writer connection settings.
    pub database: DatabaseConfig,
    /// Indexer progress and source polling settings.
    #[serde(default)]
    pub indexer: IndexerConfig,
    /// Deployment-defined sources reconciled at startup.
    #[serde(default)]
    pub source: BTreeMap<String, SourceConfig>,
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

/// Identity of the chain served by one daemon deployment.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainConfig {
    /// EIP-155 chain ID expected from the configured RPC endpoint.
    pub id: u64,
    /// Operator-defined deployment label persisted for downstream consumers.
    pub network: String,
}

/// Indexing policy settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerConfig {
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
            source_poll_interval: default_source_poll_interval(),
            backfill_concurrency: default_backfill_concurrency(),
            confirmations: default_confirmations(),
            poll_interval: default_poll_interval(),
        }
    }
}

/// Deployment-defined contract source.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
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

    use super::Config;

    #[test]
    fn applies_safe_defaults_to_minimal_configuration() {
        let config: Config = toml::from_str(
            r#"
            [chain]
            id = 100
            network = "jura-prod"
            [database]
            url = "postgresql://localhost/blokli_light"
            "#,
        )
        .expect("minimal configuration should parse");

        assert_eq!(config.database.max_connections, 5);
        assert_eq!(config.database.reader.user, "blokli_reader");
        assert_eq!(config.chain.id, 100);
        assert_eq!(config.chain.network, "jura-prod");
        assert!(config.source.is_empty());
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

    #[test]
    fn parses_named_source_tables() {
        let config: Config = toml::from_str(
            r#"
            [chain]
            id = 1
            network = "mainnet-contracts"

            [database]
            url = "postgresql://localhost/blokli_light"

            [source.mainnet-contract]
            address = "0xB413a589ec21Cc1FEc27d1175105a47628676552"
            from_block = 123
            "#,
        )
        .expect("named source configuration should parse");

        assert_eq!(config.source["mainnet-contract"].from_block, 123);
    }
}
