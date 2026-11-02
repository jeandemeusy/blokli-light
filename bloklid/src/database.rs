use std::{collections::BTreeSet, fmt};

use sqlx::{AssertSqlSafe, PgPool, Postgres, pool::PoolConnection, postgres::PgPoolOptions};
use thiserror::Error;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// A raw Ethereum log in the byte-exact layout persisted by blokli-light.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawLog {
    /// Position of the transaction in its block.
    pub tx_index: u64,
    /// Position of the log in its block.
    pub log_index: u64,
    /// Number of the block containing this log.
    pub block_number: u64,
    /// Hash of the containing block.
    pub block_hash: [u8; 32],
    /// Hash of the containing transaction.
    pub transaction_hash: [u8; 32],
    /// Emitting contract address.
    pub address: [u8; 20],
    /// Concatenated 32-byte topics, with topic0 first.
    pub topics: Vec<u8>,
    /// ABI-encoded event payload.
    pub data: Vec<u8>,
    /// Whether the RPC has marked the log as removed by a chain reorganisation.
    pub removed: bool,
}

/// Lifecycle class of a watched contract address.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceKind {
    /// A source declared in the deployment configuration.
    Builtin,
    /// A contract address registered by an operator.
    Custom,
}

impl SourceKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Builtin => "builtin",
            Self::Custom => "custom",
        }
    }
}

/// A source definition ready to be persisted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewSource {
    /// Stable operator-facing source name.
    pub name: String,
    /// Contract emitting the logs.
    pub address: [u8; 20],
    /// Whether the source is declared in deployment configuration or added at runtime.
    pub kind: SourceKind,
    /// First block that must be fetched for this source.
    pub from_block: u64,
    /// `None` follows every event; otherwise these are normalised `0x` topic0 hashes.
    pub events: Option<Vec<String>>,
}

impl NewSource {
    /// Validates source fields which do not require RPC access.
    pub fn validate(&self) -> Result<(), DatabaseError> {
        if self.name.trim().is_empty() {
            return Err(DatabaseError::EmptySourceName);
        }

        if self.from_block > i64::MAX as u64 {
            return Err(DatabaseError::BigIntOverflow {
                name: "from_block",
                value: self.from_block,
            });
        }

        Ok(())
    }
}

/// Persisted source state used by indexing and status commands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Source {
    /// Stable source identifier.
    pub id: i64,
    /// Stable operator-facing source name.
    pub name: String,
    /// Contract emitting the logs.
    pub address: [u8; 20],
    /// Whether the source is declared in deployment configuration or added at runtime.
    pub kind: SourceKind,
    /// First block that must be fetched for this source.
    pub from_block: u64,
    /// Highest block completely indexed for this source.
    pub indexed_to: i64,
    /// `None` follows every event; otherwise these are normalised `0x` topic0 hashes.
    pub events: Option<Vec<String>>,
    /// Whether the follower should include this source.
    pub enabled: bool,
}

/// The global raw-log indexing watermark exposed for Arrakis compatibility.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChainInfo {
    /// Last fully indexed built-in block.
    pub last_indexed_block: u64,
    /// Position within `last_indexed_block`, when a log was stored there.
    pub last_indexed_tx_index: Option<u64>,
    /// Position within `last_indexed_block`, when a log was stored there.
    pub last_indexed_log_index: Option<u64>,
    /// Last RPC head observed before finality adjustment.
    pub head_block: Option<u64>,
}

impl RawLog {
    /// Validates values which PostgreSQL cannot represent in its signed bigint columns.
    pub fn validate(&self) -> Result<(), DatabaseError> {
        if !self.topics.len().is_multiple_of(32) {
            return Err(DatabaseError::InvalidTopicsLength(self.topics.len()));
        }

        for (name, value) in [
            ("tx_index", self.tx_index),
            ("log_index", self.log_index),
            ("block_number", self.block_number),
        ] {
            if value > i64::MAX as u64 {
                return Err(DatabaseError::BigIntOverflow { name, value });
            }
        }

        Ok(())
    }
}

/// PostgreSQL access used by the indexer and administrative commands.
#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

impl fmt::Debug for Database {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Database").finish_non_exhaustive()
    }
}

impl Database {
    /// Opens a PostgreSQL pool without applying migrations.
    pub async fn connect(database_url: &str) -> Result<Self, DatabaseError> {
        Self::connect_with_max_connections(database_url, 5).await
    }

    /// Opens a PostgreSQL pool with an explicit connection limit.
    pub async fn connect_with_max_connections(database_url: &str, max_connections: u32) -> Result<Self, DatabaseError> {
        if max_connections == 0 {
            return Err(DatabaseError::ZeroMaxConnections);
        }
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    /// Applies the one-way schema migrations and creates the chain-info singleton.
    pub async fn migrate(&self) -> Result<(), DatabaseError> {
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    /// Obtains the session-scoped lock used to guarantee one writer per database.
    ///
    /// The returned guard must remain alive for the lifetime of the writer. PostgreSQL
    /// associates advisory locks with one physical connection, so returning only a boolean
    /// would allow the connection to return to the pool and silently release the guarantee.
    pub async fn try_advisory_lock(&self) -> Result<Option<AdvisoryLock>, DatabaseError> {
        const LOCK_KEY: i64 = 0x626c_6f6b_6c69_6c67;
        let mut connection = self.pool.acquire().await?;
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(LOCK_KEY)
            .fetch_one(&mut *connection)
            .await?;

        if acquired {
            Ok(Some(AdvisoryLock {
                connection,
                key: LOCK_KEY,
            }))
        } else {
            Ok(None)
        }
    }

    /// Stores raw logs idempotently, replacing a position if a reorganisation changed its block hash.
    ///
    /// Callers must run this in the same transaction as their watermark update.
    pub async fn store_logs(&self, logs: &[RawLog]) -> Result<(), DatabaseError> {
        let mut transaction = self.pool.begin().await?;
        self.store_logs_in_transaction(&mut transaction, logs).await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Atomically stores a completed source batch and advances its per-source watermark.
    pub async fn store_source_batch(
        &self,
        source_name: &str,
        completed_through: u64,
        logs: &[RawLog],
    ) -> Result<(), DatabaseError> {
        let completed_through = to_bigint("completed_through", completed_through)?;
        let mut transaction = self.pool.begin().await?;
        self.store_logs_in_transaction(&mut transaction, logs).await?;
        let result =
            sqlx::query("UPDATE source SET indexed_to = GREATEST(indexed_to, $2) WHERE name = $1 AND enabled = true")
                .bind(source_name)
                .bind(completed_through)
                .execute(&mut *transaction)
                .await?;
        if result.rows_affected() == 0 {
            return Err(DatabaseError::EnabledSourceNotFound(source_name.to_owned()));
        }
        transaction.commit().await?;
        Ok(())
    }

    /// Atomically stores a follower batch, advances every followed source, and updates the
    /// Arrakis-compatible built-in watermark.
    pub async fn store_follower_batch(
        &self,
        completed_through: u64,
        head_block: u64,
        source_names: &[String],
        logs: &[RawLog],
    ) -> Result<(), DatabaseError> {
        let completed_block = completed_through;
        let completed_through = to_bigint("completed_through", completed_through)?;
        let head_block = to_bigint("head_block", head_block)?;
        let position = logs
            .iter()
            .filter(|log| log.block_number == completed_block)
            .max_by_key(|log| (log.tx_index, log.log_index));
        let last_tx_index = position.map(|log| to_bigint("tx_index", log.tx_index)).transpose()?;
        let last_log_index = position.map(|log| to_bigint("log_index", log.log_index)).transpose()?;

        let mut transaction = self.pool.begin().await?;
        self.store_logs_in_transaction(&mut transaction, logs).await?;
        sqlx::query("UPDATE source SET indexed_to = GREATEST(indexed_to, $1) WHERE name = ANY($2) AND enabled = true")
            .bind(completed_through)
            .bind(source_names)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "UPDATE chain_info SET last_indexed_block = GREATEST(last_indexed_block, $1), last_indexed_tx_index = $2, \
             last_indexed_log_index = $3, head_block = $4, updated_at = now() WHERE id = 1",
        )
        .bind(completed_through)
        .bind(last_tx_index)
        .bind(last_log_index)
        .bind(head_block)
        .execute(&mut *transaction)
        .await?;
        sqlx::query("SELECT pg_notify('blokli_light_head', $1)")
            .bind(completed_through.to_string())
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Returns the singleton chain watermark, which always exists after migration.
    pub async fn chain_info(&self) -> Result<ChainInfo, DatabaseError> {
        let row = sqlx::query_as::<_, ChainInfoRow>(
            "SELECT last_indexed_block, last_indexed_tx_index, last_indexed_log_index, head_block FROM chain_info \
             WHERE id = 1",
        )
        .fetch_one(&self.pool)
        .await?;
        row.try_into()
    }

    /// Returns the number of raw log rows currently retained.
    pub async fn log_count(&self) -> Result<u64, DatabaseError> {
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM log")
            .fetch_one(&self.pool)
            .await?;
        from_bigint("log count", count)
    }

    /// Records the database chain identity on first start and refuses later mismatches.
    pub async fn ensure_chain_identity(&self, network: &str, chain_id: u64) -> Result<(), DatabaseError> {
        let chain_id = to_bigint("chain_id", chain_id)?;
        let existing = sqlx::query_as::<_, ChainIdentityRow>("SELECT network, chain_id FROM chain_info WHERE id = 1")
            .fetch_one(&self.pool)
            .await?;
        match (existing.network, existing.chain_id) {
            (Some(stored_network), Some(stored_chain_id))
                if stored_network != network || stored_chain_id != chain_id =>
            {
                Err(DatabaseError::ChainIdentityMismatch {
                    stored_network,
                    stored_chain_id,
                    configured_network: network.to_owned(),
                    configured_chain_id: chain_id,
                })
            }
            (Some(_), Some(_)) => Ok(()),
            _ => {
                sqlx::query("UPDATE chain_info SET network = $1, chain_id = $2, updated_at = now() WHERE id = 1")
                    .bind(network)
                    .bind(chain_id)
                    .execute(&self.pool)
                    .await?;
                Ok(())
            }
        }
    }

    /// Inserts or reconciles a source from deployment configuration or runtime management.
    ///
    /// Reconciliation never discards indexed data. Moving `from_block` earlier resets
    /// the source watermark so the missing range is backfilled; moving it later is ignored.
    pub async fn upsert_source(&self, source: &NewSource) -> Result<Source, DatabaseError> {
        source.validate()?;

        let existing = sqlx::query_scalar::<_, Vec<u8>>("SELECT address FROM source WHERE name = $1")
            .bind(&source.name)
            .fetch_optional(&self.pool)
            .await?;

        if let Some(address) = existing
            && address.as_slice() != source.address
        {
            return Err(DatabaseError::SourceAddressImmutable(source.name.clone()));
        }

        let source = sqlx::query_as::<_, SourceRow>(
            "INSERT INTO source (name, address, kind, from_block, indexed_to, events) VALUES ($1, $2, $3, $4, $4 - 1, \
             $5) ON CONFLICT (name) DO UPDATE SET kind = EXCLUDED.kind, from_block = LEAST(source.from_block, \
             EXCLUDED.from_block), indexed_to = CASE WHEN EXCLUDED.from_block < source.from_block OR source.events IS \
             DISTINCT FROM EXCLUDED.events THEN LEAST(source.indexed_to, LEAST(source.from_block, \
             EXCLUDED.from_block) - 1) ELSE source.indexed_to END, events = EXCLUDED.events, enabled = true RETURNING \
             id, name, address, kind, from_block, indexed_to, events, enabled",
        )
        .bind(&source.name)
        .bind(source.address.as_slice())
        .bind(source.kind.as_str())
        .bind(
            i64::try_from(source.from_block).map_err(|_| DatabaseError::BigIntOverflow {
                name: "from_block",
                value: source.from_block,
            })?,
        )
        .bind(&source.events)
        .fetch_one(&self.pool)
        .await?;

        sqlx::query("SELECT pg_notify('blokli_light_sources', $1)")
            .bind(&source.name)
            .execute(&self.pool)
            .await?;

        source.try_into()
    }

    /// Reconciles a declarative source and records its ownership by the configuration file.
    pub async fn upsert_config_source(&self, source: &NewSource) -> Result<Source, DatabaseError> {
        let source = self.upsert_source(source).await?;
        sqlx::query("UPDATE source SET managed_by_config = true WHERE id = $1")
            .bind(source.id)
            .execute(&self.pool)
            .await?;
        Ok(source)
    }

    /// Disables declarative sources removed from the current configuration without deleting logs.
    pub async fn disable_removed_config_sources(&self, configured_names: &[String]) -> Result<u64, DatabaseError> {
        let result = sqlx::query(
            "UPDATE source SET enabled = false WHERE managed_by_config = true AND NOT (name = ANY($1)) AND enabled = \
             true",
        )
        .bind(configured_names)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() > 0 {
            sqlx::query("SELECT pg_notify('blokli_light_sources', 'config-reconciled')")
                .execute(&self.pool)
                .await?;
        }
        Ok(result.rows_affected())
    }

    /// Adds events to a custom source and resets its watermark for an idempotent re-backfill.
    pub async fn widen_source(&self, name: &str, events: &[String]) -> Result<Source, DatabaseError> {
        if events.is_empty() {
            return Err(DatabaseError::EmptySourceEventSet);
        }

        let source = sqlx::query_as::<_, SourceRow>(
            "UPDATE source SET events = ARRAY(SELECT DISTINCT event FROM unnest(COALESCE(events, ARRAY[]::text[]) || \
             $2::text[]) AS event), indexed_to = from_block - 1, enabled = true WHERE name = $1 AND kind = 'custom' \
             AND events IS NOT NULL RETURNING id, name, address, kind, from_block, indexed_to, events, enabled",
        )
        .bind(name)
        .bind(events)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| DatabaseError::CustomSourceNotFound(name.to_owned()))?;

        sqlx::query("SELECT pg_notify('blokli_light_sources', $1)")
            .bind(name)
            .execute(&self.pool)
            .await?;

        source.try_into()
    }

    /// Stops following a custom source without modifying its historic logs.
    pub async fn disable_source(&self, name: &str) -> Result<(), DatabaseError> {
        let result = sqlx::query("UPDATE source SET enabled = false WHERE name = $1 AND kind = 'custom'")
            .bind(name)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
            return Err(DatabaseError::CustomSourceNotFound(name.to_owned()));
        }

        sqlx::query("SELECT pg_notify('blokli_light_sources', $1)")
            .bind(name)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Lists sources, including disabled sources whose historic logs remain queryable.
    pub async fn list_sources(&self) -> Result<Vec<Source>, DatabaseError> {
        sqlx::query_as::<_, SourceRow>(
            "SELECT id, name, address, kind, from_block, indexed_to, events, enabled FROM source ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
    }

    /// Primes and verifies the raw-log origin guard.
    ///
    /// Deployment-defined rows must match exactly once the guard exists. Runtime-added rows are
    /// additive and do not invalidate a database when operators register new sources.
    pub async fn ensure_logs_origin(&self) -> Result<(), DatabaseError> {
        let sources = self.list_sources().await?;
        let mut expected_all = BTreeSet::new();
        let mut expected_builtin = BTreeSet::new();
        let builtin_addresses = sources
            .iter()
            .filter(|source| source.kind == SourceKind::Builtin)
            .map(|source| source.address.to_vec())
            .collect::<BTreeSet<_>>();

        for source in sources.iter().filter(|source| source.enabled) {
            let topics = match &source.events {
                Some(events) => events
                    .iter()
                    .map(|event| decode_topic(event))
                    .collect::<Result<Vec<_>, _>>()?,
                None => vec![vec![0; 32]],
            };
            for topic in topics {
                let pair = (source.address.to_vec(), topic);
                if source.kind == SourceKind::Builtin {
                    expected_builtin.insert(pair.clone());
                }
                expected_all.insert(pair);
            }
        }

        let existing = sqlx::query_as::<_, LogTopicRow>("SELECT address, topic FROM log_topic_info")
            .fetch_all(&self.pool)
            .await?;
        let existing_builtin = existing
            .iter()
            .filter(|row| builtin_addresses.contains(&row.address))
            .map(|row| (row.address.clone(), row.topic.clone()))
            .collect::<BTreeSet<_>>();
        if !existing.is_empty() && existing_builtin != expected_builtin {
            return Err(DatabaseError::LogsOriginMismatch);
        }

        let mut transaction = self.pool.begin().await?;
        for (address, topic) in expected_all {
            sqlx::query(
                "INSERT INTO log_topic_info (address, topic) VALUES ($1, $2) ON CONFLICT (address, topic) DO NOTHING",
            )
            .bind(address)
            .bind(topic)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    /// Marks logs from replaced versions of one block as removed without deleting history.
    pub async fn mark_noncanonical_logs_removed(
        &self,
        block_number: u64,
        canonical_hash: &[u8; 32],
    ) -> Result<u64, DatabaseError> {
        let result = sqlx::query(
            "UPDATE log SET removed = true WHERE block_number = $1 AND block_hash <> $2 AND removed = false",
        )
        .bind(to_bigint("block_number", block_number)?)
        .bind(canonical_hash.as_slice())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// Creates or updates the restricted PostgreSQL login used by direct SQL consumers.
    pub async fn provision_reader(&self, user: &str, password: &str) -> Result<(), DatabaseError> {
        if !is_valid_role_name(user) {
            return Err(DatabaseError::InvalidReaderRole(user.to_owned()));
        }
        let role = quote_identifier(user);
        let password = quote_literal(password);
        let database: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&self.pool)
            .await?;
        let database = quote_identifier(&database);
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = $1)")
            .bind(user)
            .fetch_one(&self.pool)
            .await?;
        let role_statement = if exists {
            format!("ALTER ROLE {role} LOGIN PASSWORD {password} CONNECTION LIMIT 10")
        } else {
            format!("CREATE ROLE {role} LOGIN PASSWORD {password} CONNECTION LIMIT 10")
        };
        sqlx::query(AssertSqlSafe(role_statement)).execute(&self.pool).await?;
        for statement in [
            format!("GRANT CONNECT ON DATABASE {database} TO {role}"),
            format!("GRANT USAGE ON SCHEMA public TO {role}"),
            format!("REVOKE CREATE ON SCHEMA public FROM {role}"),
            format!("GRANT SELECT ON ALL TABLES IN SCHEMA public TO {role}"),
            format!("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {role}"),
            format!("ALTER ROLE {role} SET default_transaction_read_only = on"),
            format!("ALTER ROLE {role} SET statement_timeout = '60s'"),
            format!("ALTER ROLE {role} SET idle_in_transaction_session_timeout = '30s'"),
            format!("ALTER ROLE {role} SET search_path = public"),
        ] {
            sqlx::query(AssertSqlSafe(statement)).execute(&self.pool).await?;
        }
        Ok(())
    }

    async fn store_logs_in_transaction(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        logs: &[RawLog],
    ) -> Result<(), DatabaseError> {
        for log in logs {
            log.validate()?;

            sqlx::query(
                "INSERT INTO log (tx_index, log_index, block_number, block_hash, transaction_hash, address, topics, \
                 data, removed) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (block_number, tx_index, \
                 log_index) DO UPDATE SET block_hash = EXCLUDED.block_hash, transaction_hash = \
                 EXCLUDED.transaction_hash, address = EXCLUDED.address, topics = EXCLUDED.topics, data = \
                 EXCLUDED.data, removed = EXCLUDED.removed WHERE log.block_hash <> EXCLUDED.block_hash OR log.removed \
                 <> EXCLUDED.removed",
            )
            .bind(i64::try_from(log.tx_index).map_err(|_| DatabaseError::BigIntOverflow {
                name: "tx_index",
                value: log.tx_index,
            })?)
            .bind(i64::try_from(log.log_index).map_err(|_| DatabaseError::BigIntOverflow {
                name: "log_index",
                value: log.log_index,
            })?)
            .bind(
                i64::try_from(log.block_number).map_err(|_| DatabaseError::BigIntOverflow {
                    name: "block_number",
                    value: log.block_number,
                })?,
            )
            .bind(log.block_hash.as_slice())
            .bind(log.transaction_hash.as_slice())
            .bind(log.address.as_slice())
            .bind(&log.topics)
            .bind(&log.data)
            .bind(log.removed)
            .execute(&mut **transaction)
            .await?;
        }

        Ok(())
    }
}

#[derive(sqlx::FromRow)]
struct SourceRow {
    id: i64,
    name: String,
    address: Vec<u8>,
    kind: String,
    from_block: i64,
    indexed_to: i64,
    events: Option<Vec<String>>,
    enabled: bool,
}

#[derive(sqlx::FromRow)]
struct ChainInfoRow {
    last_indexed_block: i64,
    last_indexed_tx_index: Option<i64>,
    last_indexed_log_index: Option<i64>,
    head_block: Option<i64>,
}

#[derive(sqlx::FromRow)]
struct ChainIdentityRow {
    network: Option<String>,
    chain_id: Option<i64>,
}

#[derive(sqlx::FromRow)]
struct LogTopicRow {
    address: Vec<u8>,
    topic: Vec<u8>,
}

impl TryFrom<ChainInfoRow> for ChainInfo {
    type Error = DatabaseError;

    fn try_from(value: ChainInfoRow) -> Result<Self, Self::Error> {
        Ok(Self {
            last_indexed_block: from_bigint("last_indexed_block", value.last_indexed_block)?,
            last_indexed_tx_index: value
                .last_indexed_tx_index
                .map(|value| from_bigint("last_indexed_tx_index", value))
                .transpose()?,
            last_indexed_log_index: value
                .last_indexed_log_index
                .map(|value| from_bigint("last_indexed_log_index", value))
                .transpose()?,
            head_block: value
                .head_block
                .map(|value| from_bigint("head_block", value))
                .transpose()?,
        })
    }
}

impl TryFrom<SourceRow> for Source {
    type Error = DatabaseError;

    fn try_from(value: SourceRow) -> Result<Self, Self::Error> {
        let address = value
            .address
            .try_into()
            .map_err(|address: Vec<u8>| DatabaseError::InvalidSourceAddressLength(address.len()))?;
        let kind = match value.kind.as_str() {
            "builtin" => SourceKind::Builtin,
            "custom" => SourceKind::Custom,
            kind => return Err(DatabaseError::InvalidSourceKind(kind.to_owned())),
        };
        let from_block = from_bigint("from_block", value.from_block)?;

        Ok(Self {
            id: value.id,
            name: value.name,
            address,
            kind,
            from_block,
            indexed_to: value.indexed_to,
            events: value.events,
            enabled: value.enabled,
        })
    }
}

fn to_bigint(name: &'static str, value: u64) -> Result<i64, DatabaseError> {
    i64::try_from(value).map_err(|_| DatabaseError::BigIntOverflow { name, value })
}

fn from_bigint(name: &'static str, value: i64) -> Result<u64, DatabaseError> {
    u64::try_from(value).map_err(|_| DatabaseError::InvalidStoredBlockNumber(name, value))
}

fn decode_topic(topic: &str) -> Result<Vec<u8>, DatabaseError> {
    let topic = topic
        .strip_prefix("0x")
        .ok_or_else(|| DatabaseError::InvalidStoredTopic(topic.to_owned()))?;
    let bytes = hex::decode(topic).map_err(|_| DatabaseError::InvalidStoredTopic(topic.to_owned()))?;
    if bytes.len() != 32 {
        return Err(DatabaseError::InvalidStoredTopic(topic.to_owned()));
    }
    Ok(bytes)
}

fn is_valid_role_name(role: &str) -> bool {
    let mut characters = role.chars();
    matches!(characters.next(), Some(character) if character.is_ascii_alphabetic() || character == '_')
        && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Guard retaining the PostgreSQL session that owns the daemon writer lock.
pub struct AdvisoryLock {
    connection: PoolConnection<Postgres>,
    key: i64,
}

impl fmt::Debug for AdvisoryLock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("AdvisoryLock").finish_non_exhaustive()
    }
}

impl AdvisoryLock {
    /// Explicitly releases the writer lock before returning its connection to the pool.
    pub async fn release(mut self) -> Result<(), DatabaseError> {
        let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
            .bind(self.key)
            .fetch_one(&mut *self.connection)
            .await?;

        if released {
            Ok(())
        } else {
            Err(DatabaseError::AdvisoryLockNotHeld)
        }
    }
}

/// Errors produced by the PostgreSQL storage layer.
#[derive(Debug, Error)]
pub enum DatabaseError {
    /// A PostgreSQL pool must retain at least one connection.
    #[error("database max_connections must be greater than zero")]
    ZeroMaxConnections,
    /// A source name must contain non-whitespace characters.
    #[error("source name cannot be empty")]
    EmptySourceName,
    /// A `widen` operation must introduce at least one event.
    #[error("source event set cannot be empty")]
    EmptySourceEventSet,
    /// A stored source name already belongs to a different contract address.
    #[error("source '{0}' cannot change address; add a new source instead")]
    SourceAddressImmutable(String),
    /// A custom source could not be found or is a built-in source.
    #[error("custom source '{0}' was not found")]
    CustomSourceNotFound(String),
    /// A source batch was committed for a source that is disabled or absent.
    #[error("enabled source '{0}' was not found")]
    EnabledSourceNotFound(String),
    /// Stored origin rows disagree with the configured built-in contract/topic set.
    #[error("configured built-in log origin does not match this database")]
    LogsOriginMismatch,
    /// A persisted source topic is not a complete 32-byte hash.
    #[error("stored source topic is invalid: {0}")]
    InvalidStoredTopic(String),
    /// The configured chain differs from the identity already stored in the database.
    #[error(
        "database belongs to {stored_network} chain {stored_chain_id}, not {configured_network} chain \
         {configured_chain_id}"
    )]
    ChainIdentityMismatch {
        /// Previously stored network.
        stored_network: String,
        /// Previously stored chain ID.
        stored_chain_id: i64,
        /// Requested deployment label.
        configured_network: String,
        /// Requested chain ID.
        configured_chain_id: i64,
    },
    /// Reader role names are restricted to ordinary PostgreSQL identifiers.
    #[error("invalid reader role name: {0}")]
    InvalidReaderRole(String),
    /// A source row has an invalid persisted address width.
    #[error("stored source address must be 20 bytes, got {0}")]
    InvalidSourceAddressLength(usize),
    /// A source row contains a kind outside the migration's allowed values.
    #[error("stored source kind is invalid: {0}")]
    InvalidSourceKind(String),
    /// A stored signed block field cannot be represented by the public unsigned model.
    #[error("stored {0} is negative: {1}")]
    InvalidStoredBlockNumber(&'static str, i64),
    /// The PostgreSQL session no longer held the expected advisory lock.
    #[error("PostgreSQL advisory lock was not held by this session")]
    AdvisoryLockNotHeld,
    /// A log's concatenated topic bytes do not consist of complete 32-byte words.
    #[error("topics must be a concatenation of 32-byte values, got {0} bytes")]
    InvalidTopicsLength(usize),
    /// A Rust unsigned value cannot be stored in PostgreSQL's signed bigint type.
    #[error("{name} value {value} exceeds PostgreSQL bigint")]
    BigIntOverflow {
        /// Field that overflowed.
        name: &'static str,
        /// Original unsigned value.
        value: u64,
    },
    /// An error reported by PostgreSQL or SQLx.
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    /// A migration error reported by SQLx.
    #[error(transparent)]
    Migration(#[from] sqlx::migrate::MigrateError),
}

#[cfg(test)]
mod tests {
    use super::{DatabaseError, RawLog};

    fn log_with_topics(topics: Vec<u8>) -> RawLog {
        RawLog {
            tx_index: 0,
            log_index: 0,
            block_number: 0,
            block_hash: [0; 32],
            transaction_hash: [0; 32],
            address: [0; 20],
            topics,
            data: Vec::new(),
            removed: false,
        }
    }

    #[test]
    fn validates_complete_topic_words() {
        assert!(log_with_topics(vec![0; 64]).validate().is_ok());
    }

    #[test]
    fn rejects_partial_topic_words() {
        assert!(matches!(
            log_with_topics(vec![0; 31]).validate(),
            Err(DatabaseError::InvalidTopicsLength(31))
        ));
    }

    #[test]
    fn rejects_unsigned_values_outside_postgres_bigint() {
        let mut log = log_with_topics(Vec::new());
        log.block_number = i64::MAX as u64 + 1;

        assert!(matches!(
            log.validate(),
            Err(DatabaseError::BigIntOverflow {
                name: "block_number",
                ..
            })
        ));
    }
}
