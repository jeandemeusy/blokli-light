//! Finality-aware raw-log follower and resumable per-source backfill worker.

use std::time::Duration;

use thiserror::Error;
use tokio::time::sleep;

use crate::{
    database::{Database, DatabaseError, Source, SourceKind},
    rpc::{LogFilter, RpcClient, RpcError},
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
}

/// Runs the follower indefinitely until the process is stopped.
pub async fn run(database: &Database, rpc: &RpcClient, config: IndexerConfig) -> Result<(), IndexerError> {
    loop {
        run_once(database, rpc, config).await?;
        sleep(config.poll_interval).await;
    }
}

/// Advances every enabled source to the current finalised head once.
pub async fn run_once(database: &Database, rpc: &RpcClient, config: IndexerConfig) -> Result<(), IndexerError> {
    if config.max_block_range == 0 {
        return Err(IndexerError::ZeroBlockRange);
    }

    let head = rpc.head_block().await?;
    let finalised_head = head.saturating_sub(config.confirmations);
    let finalised_head_i64 =
        i64::try_from(finalised_head).map_err(|_| IndexerError::InvalidFinalisedHead(finalised_head))?;
    let sources = database.list_sources().await?;
    let builtin_names = sources
        .iter()
        .filter(|source| source.enabled && source.kind == SourceKind::Builtin)
        .map(|source| source.name.clone())
        .collect::<Vec<_>>();

    for source in sources.into_iter().filter(|source| source.enabled) {
        backfill_source(database, rpc, config.max_block_range, finalised_head, &source).await?;
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

    Ok(())
}

async fn backfill_source(
    database: &Database,
    rpc: &RpcClient,
    max_block_range: u64,
    finalised_head: u64,
    source: &Source,
) -> Result<(), IndexerError> {
    let mut next_block = if source.indexed_to < 0 {
        source.from_block
    } else {
        u64::try_from(source.indexed_to)
            .map_err(|_| IndexerError::InvalidSourceWatermark(source.name.clone(), source.indexed_to))?
            .saturating_add(1)
            .max(source.from_block)
    };

    while next_block <= finalised_head {
        let end_block = finalised_head.min(next_block.saturating_add(max_block_range - 1));
        let logs = rpc
            .get_logs(&LogFilter {
                addresses: vec![source.address],
                events: source.events.clone(),
                from_block: next_block,
                to_block: end_block,
            })
            .await?;
        database.store_source_batch(&source.name, end_block, &logs).await?;
        next_block = end_block.saturating_add(1);
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
    /// A source row contained an invalid progress watermark.
    #[error("source '{0}' has an invalid indexed_to watermark: {1}")]
    InvalidSourceWatermark(String, i64),
    /// The finalised chain height cannot be represented by PostgreSQL bigint.
    #[error("finalised head exceeds PostgreSQL bigint: {0}")]
    InvalidFinalisedHead(u64),
}
