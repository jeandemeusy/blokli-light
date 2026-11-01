use std::time::Duration;

use blokli_light::{
    database::{Database, DatabaseError, NewSource, RawLog, SourceKind},
    indexer::{IndexerConfig, run_once},
    rpc::RpcClient,
};
use mockito::Matcher;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use url::Url;

fn test_database_url() -> Option<String> {
    std::env::var("BLOKLI_LIGHT_TEST_DATABASE_URL").ok()
}

fn sample_log(block_hash: [u8; 32]) -> RawLog {
    RawLog {
        tx_index: 2,
        log_index: 3,
        block_number: 100,
        block_hash,
        transaction_hash: [4; 32],
        address: [5; 20],
        topics: vec![6; 32],
        data: vec![7, 8, 9],
        removed: false,
    }
}

#[tokio::test]
#[ignore = "requires BLOKLI_LIGHT_TEST_DATABASE_URL; run `just test-postgres`"]
async fn postgres_storage_origin_and_reader_contract() -> anyhow::Result<()> {
    let database_url = test_database_url().ok_or_else(|| anyhow::anyhow!("test database URL is missing"))?;
    let database = Database::connect(&database_url).await?;
    database.migrate().await?;
    let admin = PgPoolOptions::new().connect(&database_url).await?;
    for table in ["log_topic_info", "log", "source"] {
        let statement = format!("TRUNCATE TABLE {table} RESTART IDENTITY CASCADE");
        sqlx::query(sqlx::AssertSqlSafe(statement)).execute(&admin).await?;
    }

    let log = sample_log([1; 32]);
    database.store_logs(&[log.clone(), log]).await?;
    assert_eq!(database.log_count().await?, 1);

    database.store_logs(&[sample_log([2; 32])]).await?;
    assert_eq!(database.log_count().await?, 1);
    let stored_hash: Vec<u8> =
        sqlx::query_scalar("SELECT block_hash FROM log WHERE block_number = 100 AND tx_index = 2 AND log_index = 3")
            .fetch_one(&admin)
            .await?;
    assert_eq!(stored_hash, vec![2; 32]);

    let mut builtin = NewSource {
        name: "test-builtin".to_owned(),
        address: [9; 20],
        kind: SourceKind::Builtin,
        from_block: 10,
        events: Some(vec![format!("0x{}", hex::encode([10; 32]))]),
    };
    database.upsert_source(&builtin).await?;
    database.ensure_logs_origin().await?;
    builtin.events = Some(vec![format!("0x{}", hex::encode([11; 32]))]);
    database.upsert_source(&builtin).await?;
    assert!(matches!(
        database.ensure_logs_origin().await,
        Err(DatabaseError::LogsOriginMismatch)
    ));

    let custom = NewSource {
        name: "custom-vault".to_owned(),
        address: [12; 20],
        kind: SourceKind::Custom,
        from_block: 20,
        events: Some(vec![format!("0x{}", hex::encode([13; 32]))]),
    };
    database.upsert_source(&custom).await?;
    database.store_source_batch("custom-vault", 25, &[]).await?;
    let widened = database
        .widen_source("custom-vault", &[format!("0x{}", hex::encode([14; 32]))])
        .await?;
    assert_eq!(widened.indexed_to, 19);
    assert_eq!(widened.events.as_ref().map(Vec::len), Some(2));
    database.disable_source("custom-vault").await?;
    let custom = database
        .list_sources()
        .await?
        .into_iter()
        .find(|source| source.name == "custom-vault")
        .ok_or_else(|| anyhow::anyhow!("custom source disappeared"))?;
    assert!(!custom.enabled);
    assert_eq!(database.log_count().await?, 1);

    let configured = NewSource {
        name: "configured-vault".to_owned(),
        address: [15; 20],
        kind: SourceKind::Custom,
        from_block: 30,
        events: None,
    };
    database.upsert_config_source(&configured).await?;
    assert_eq!(database.disable_removed_config_sources(&[]).await?, 1);
    let configured = database
        .list_sources()
        .await?
        .into_iter()
        .find(|source| source.name == "configured-vault")
        .ok_or_else(|| anyhow::anyhow!("configured source disappeared"))?;
    assert!(!configured.enabled);

    let reader_user = "blokli_reader_test";
    let reader_password = "reader-test-password";
    database.provision_reader(reader_user, reader_password).await?;
    let mut reader_url = Url::parse(&database_url)?;
    reader_url
        .set_username(reader_user)
        .map_err(|()| anyhow::anyhow!("invalid reader username"))?;
    reader_url
        .set_password(Some(reader_password))
        .map_err(|()| anyhow::anyhow!("invalid reader password"))?;
    let reader = PgPoolOptions::new().connect(reader_url.as_str()).await?;

    for table in ["log", "source", "chain_info"] {
        let query = format!("SELECT count(*) FROM {table}");
        sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(query))
            .fetch_one(&reader)
            .await?;
    }
    assert!(
        sqlx::query("INSERT INTO chain_info (id) VALUES (2)")
            .execute(&reader)
            .await
            .is_err()
    );
    assert!(sqlx::query("DELETE FROM log").execute(&reader).await.is_err());
    assert!(
        sqlx::query("CREATE TABLE reader_must_not_create (id bigint)")
            .execute(&reader)
            .await
            .is_err()
    );
    sqlx::query("SET default_transaction_read_only = off")
        .execute(&reader)
        .await?;
    assert!(
        sqlx::query("INSERT INTO chain_info (id) VALUES (2)")
            .execute(&reader)
            .await
            .is_err()
    );

    Ok(())
}

#[tokio::test]
#[ignore = "requires BLOKLI_LIGHT_TEST_DATABASE_URL; run `just test-postgres`"]
async fn indexer_resumes_with_finality_and_reconciles_reorgs() -> anyhow::Result<()> {
    let database_url = test_database_url().ok_or_else(|| anyhow::anyhow!("test database URL is missing"))?;
    let database = Database::connect(&database_url).await?;
    database.migrate().await?;
    let admin = PgPoolOptions::new().connect(&database_url).await?;
    for table in ["log_topic_info", "log", "source"] {
        let statement = format!("TRUNCATE TABLE {table} RESTART IDENTITY CASCADE");
        sqlx::query(sqlx::AssertSqlSafe(statement)).execute(&admin).await?;
    }
    sqlx::query(
        "UPDATE chain_info SET last_indexed_block = 0, last_indexed_tx_index = NULL, last_indexed_log_index = NULL, \
         head_block = NULL",
    )
    .execute(&admin)
    .await?;
    let source = NewSource {
        name: "indexer-test".to_owned(),
        address: [5; 20],
        kind: SourceKind::Builtin,
        from_block: 0,
        events: None,
    };
    database.upsert_source(&source).await?;
    database.store_source_batch("indexer-test", 2, &[]).await?;
    database
        .upsert_source(&NewSource {
            name: "custom-backfill".to_owned(),
            address: [7; 20],
            kind: SourceKind::Custom,
            from_block: 0,
            events: None,
        })
        .await?;
    let disappeared = RawLog {
        tx_index: 1,
        log_index: 1,
        block_number: 4,
        block_hash: [1; 32],
        transaction_hash: [2; 32],
        address: [5; 20],
        topics: vec![3; 32],
        data: vec![],
        removed: false,
    };
    database.store_logs(&[disappeared]).await?;

    let mut server = mockito::Server::new_async().await;
    let head = server
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(json!({"method": "eth_blockNumber"})))
        .with_status(200)
        .with_body(r#"{"jsonrpc":"2.0","id":1,"result":"0x6"}"#)
        .create_async()
        .await;
    let block_hash = format!("0x{}", hex::encode([8; 32]));
    let blocks = server
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(json!({"method": "eth_getBlockByNumber"})))
        .with_status(200)
        .with_body(format!(
            r#"{{"jsonrpc":"2.0","id":1,"result":{{"hash":"{block_hash}"}}}}"#
        ))
        .expect(5)
        .create_async()
        .await;
    let logs = server
        .mock("POST", "/")
        .match_body(Matcher::Regex(
            r#"(?s)"method":"eth_getLogs".*"fromBlock":"0x3".*"toBlock":"0x4""#.to_owned(),
        ))
        .with_status(200)
        .with_body(format!(
            r#"{{"jsonrpc":"2.0","id":1,"result":[{{"address":"0x{}","topics":["0x{}"],"data":"0x","transactionIndex":"0x2","blockNumber":"0x4","blockHash":"{}","transactionHash":"0x{}","logIndex":"0x2","removed":true}}]}}"#,
            hex::encode([5; 20]),
            hex::encode([4; 32]),
            block_hash,
            hex::encode([6; 32]),
        ))
        .create_async()
        .await;
    let custom_logs = server
        .mock("POST", "/")
        .match_body(Matcher::Regex(
            r#"(?s)"method":"eth_getLogs".*"fromBlock":"0x0".*"toBlock":"0x4""#.to_owned(),
        ))
        .with_status(200)
        .with_body(r#"{"jsonrpc":"2.0","id":1,"result":[]}"#)
        .create_async()
        .await;
    let no_code = server
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(json!({"method": "eth_getCode"})))
        .with_status(200)
        .with_body(r#"{"jsonrpc":"2.0","id":1,"result":"0x"}"#)
        .create_async()
        .await;

    let rpc = RpcClient::new(&server.url())?;
    assert!(!rpc.has_code(&[7; 20]).await?);
    run_once(
        &database,
        &rpc,
        IndexerConfig {
            confirmations: 2,
            max_block_range: 100,
            poll_interval: Duration::from_millis(1),
            source_poll_interval: Duration::from_millis(1),
            backfill_concurrency: 1,
        },
    )
    .await?;

    head.assert_async().await;
    blocks.assert_async().await;
    logs.assert_async().await;
    custom_logs.assert_async().await;
    no_code.assert_async().await;
    let chain = database.chain_info().await?;
    assert_eq!(chain.last_indexed_block, 4);
    assert_eq!(chain.head_block, Some(6));
    let source = database
        .list_sources()
        .await?
        .into_iter()
        .find(|source| source.name == "indexer-test")
        .ok_or_else(|| anyhow::anyhow!("indexer source disappeared"))?;
    assert_eq!(source.indexed_to, 4);
    let custom = database
        .list_sources()
        .await?
        .into_iter()
        .find(|source| source.name == "custom-backfill")
        .ok_or_else(|| anyhow::anyhow!("custom backfill source disappeared"))?;
    assert_eq!(custom.indexed_to, 4);
    let removed: i64 = sqlx::query_scalar("SELECT count(*) FROM log WHERE removed")
        .fetch_one(&admin)
        .await?;
    assert_eq!(removed, 2);

    Ok(())
}
