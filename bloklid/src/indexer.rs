//! Finality-aware raw-log follower and resumable per-source backfill worker.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use thiserror::Error;
use tokio::{
    sync::Semaphore,
    task::{JoinError, JoinSet},
    time::sleep,
};

use crate::{
    database::{Database, DatabaseError, Source, SourceKind},
    rpc::{LogFilter, RpcClient, RpcError},
    telemetry,
};

/// Runtime policy for finality and adaptive-sized source batches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexerConfig {
    /// Number of unfinalised blocks excluded from the durable watermark.
    pub confirmations: u64,
    /// Initial maximum inclusive block span for an RPC log request.
    pub max_block_range: u64,
    /// Delay between finality-head polling iterations.
    pub poll_interval: Duration,
    /// Maximum delay before observing source catalog changes.
    pub source_poll_interval: Duration,
    /// Maximum number of lagging custom-source batches fetched concurrently.
    pub backfill_concurrency: usize,
}

/// Runs the follower indefinitely until the process is stopped.
pub async fn run(database: &Database, rpc: &RpcClient, config: IndexerConfig) -> Result<(), IndexerError> {
    if config.poll_interval.is_zero() || config.source_poll_interval.is_zero() {
        return Err(IndexerError::ZeroPollInterval);
    }
    let mut rpc_failures = 0_u32;
    loop {
        match run_once(database, rpc, config).await {
            Ok(()) => {
                rpc_failures = 0;
                sleep(config.poll_interval.min(config.source_poll_interval)).await;
            }
            Err(IndexerError::Rpc(error)) if rpc_failures < 5 => {
                rpc_failures += 1;
                let delay = Duration::from_secs(2_u64.saturating_pow(rpc_failures));
                tracing::warn!(%error, rpc_failures, ?delay, "RPC indexing iteration failed; retrying");
                sleep(delay).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Advances every enabled source to the current finalised head once.
pub async fn run_once(database: &Database, rpc: &RpcClient, config: IndexerConfig) -> Result<(), IndexerError> {
    if config.max_block_range == 0 {
        return Err(IndexerError::ZeroBlockRange);
    }
    if config.backfill_concurrency == 0 {
        return Err(IndexerError::ZeroBackfillConcurrency);
    }

    let head = rpc.head_block().await?;
    let finalised_head = head.saturating_sub(config.confirmations);
    let finalised_head_i64 =
        i64::try_from(finalised_head).map_err(|_| IndexerError::InvalidFinalisedHead(finalised_head))?;
    let chain_info = database.chain_info().await?;
    let sources = database.list_sources().await?;
    database.ensure_logs_origin().await?;

    let reconciliation_start = finalised_head.saturating_sub(config.confirmations.saturating_mul(2));
    for block_number in reconciliation_start..=finalised_head {
        let canonical_hash = rpc.block_hash(block_number).await?;
        let removed = database
            .mark_noncanonical_logs_removed(block_number, &canonical_hash)
            .await?;
        if removed > 0 {
            tracing::warn!(block_number, removed, "marked logs from replaced block hash as removed");
        }
    }
    let builtin_names = sources
        .iter()
        .filter(|source| source.enabled && source.kind == SourceKind::Builtin)
        .map(|source| source.name.clone())
        .collect::<Vec<_>>();

    let (followers, lagging): (Vec<_>, Vec<_>) = sources
        .into_iter()
        .filter(|source| source.enabled)
        .partition(|source| source.indexed_to >= i64::try_from(chain_info.last_indexed_block).unwrap_or(i64::MAX));

    // Sources already at the compatibility watermark remain on the head path. Any newly added
    // source backfills at lower priority, so it cannot delay contracts already following the head.
    for source in followers {
        backfill_source(database, rpc, config.max_block_range, finalised_head, &source, None).await?;
    }

    // `chain_info` retains the historical meaning used by Arrakis: completion of the built-in set.
    // Source progress is already committed separately, so this transaction merely advances the
    // compatibility watermark after every built-in source has reached the same finalised head.
    if !builtin_names.is_empty() {
        let builtins_complete = database
            .list_sources()
            .await?
            .iter()
            .filter(|source| source.enabled && source.kind == SourceKind::Builtin)
            .all(|source| source.indexed_to >= finalised_head_i64);
        if builtins_complete {
            database
                .store_follower_batch(finalised_head, head, &builtin_names, &[])
                .await?;
        }
    }

    backfill_lagging_sources(
        database,
        rpc,
        config.max_block_range,
        finalised_head,
        config.backfill_concurrency,
        lagging,
    )
    .await?;

    let sources = database.list_sources().await?;
    let chain_info = database.chain_info().await?;
    telemetry::record_indexer_state(head, chain_info.last_indexed_block);
    telemetry::record_source_states(finalised_head, &sources);

    Ok(())
}

async fn backfill_lagging_sources(
    database: &Database,
    rpc: &RpcClient,
    max_block_range: u64,
    finalised_head: u64,
    concurrency: usize,
    sources: Vec<Source>,
) -> Result<(), IndexerError> {
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut tasks = JoinSet::new();
    for source in sources {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| IndexerError::BackfillSemaphoreClosed)?;
        let database = database.clone();
        let rpc = rpc.clone();
        tasks.spawn(async move {
            let _permit = permit;
            backfill_source(&database, &rpc, max_block_range, finalised_head, &source, Some(1)).await
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.map_err(IndexerError::BackfillTask)??;
    }
    Ok(())
}

async fn backfill_source(
    database: &Database,
    rpc: &RpcClient,
    max_block_range: u64,
    finalised_head: u64,
    source: &Source,
    max_batches: Option<usize>,
) -> Result<(), IndexerError> {
    let mut block_range = rpc.log_block_range(max_block_range);
    let mut completed_batches = 0;
    let mut next_block = if source.indexed_to < 0 {
        source.from_block
    } else {
        u64::try_from(source.indexed_to)
            .map_err(|_| IndexerError::InvalidSourceWatermark(source.name.clone(), source.indexed_to))?
            .saturating_add(1)
            .max(source.from_block)
    };

    while next_block <= finalised_head {
        let end_block = finalised_head.min(next_block.saturating_add(block_range - 1));
        let filter = LogFilter {
            addresses: vec![source.address],
            events: source.events.clone(),
            from_block: next_block,
            to_block: end_block,
        };
        let batch_started = Instant::now();
        let logs = match rpc.get_logs(&filter).await {
            Ok(logs) => logs,
            Err(error) if error.is_log_range_limit() && block_range > 1 => {
                block_range = rpc.note_log_range_limit(block_range);
                telemetry::record_log_range_limit(&source.name);
                tracing::warn!(source = %source.name, block_range, "provider limited eth_getLogs range; shrinking");
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        let removed_logs = logs.iter().filter(|log| log.removed).count();
        if removed_logs > 0 {
            tracing::warn!(source = %source.name, removed_logs, "RPC returned removed logs");
        }
        database.store_source_batch(&source.name, end_block, &logs).await?;
        telemetry::record_source_batch(
            &source.name,
            end_block.saturating_sub(next_block).saturating_add(1),
            logs.len(),
            removed_logs,
            batch_started.elapsed(),
        );
        next_block = end_block.saturating_add(1);
        completed_batches += 1;
        if max_batches.is_some_and(|maximum| completed_batches >= maximum) {
            break;
        }
    }

    Ok(())
}

/// Indexing failures which should terminate the process so its supervisor can restart it.
#[derive(Debug, Error)]
pub enum IndexerError {
    /// RPC queries failed.
    #[error(transparent)]
    Rpc(#[from] RpcError),
    /// PostgreSQL storage failed.
    #[error(transparent)]
    Database(#[from] DatabaseError),
    /// A zero-size range cannot make progress.
    #[error("max_block_range must be greater than zero")]
    ZeroBlockRange,
    /// A zero custom-backfill concurrency cannot schedule work.
    #[error("backfill_concurrency must be greater than zero")]
    ZeroBackfillConcurrency,
    /// Poll intervals must be positive to avoid a busy loop.
    #[error("poll intervals must be greater than zero")]
    ZeroPollInterval,
    /// The custom-backfill concurrency limiter was unexpectedly closed.
    #[error("custom-source backfill scheduler closed")]
    BackfillSemaphoreClosed,
    /// A custom-source backfill task failed to join.
    #[error("custom-source backfill task failed")]
    BackfillTask(#[source] JoinError),
    /// A source row contained an invalid progress watermark.
    #[error("source '{0}' has an invalid indexed_to watermark: {1}")]
    InvalidSourceWatermark(String, i64),
    /// The finalised chain height cannot be represented by PostgreSQL bigint.
    #[error("finalised head exceeds PostgreSQL bigint: {0}")]
    InvalidFinalisedHead(u64),
}
