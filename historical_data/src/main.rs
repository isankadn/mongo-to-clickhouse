use anyhow::{anyhow, Context, Result};
use clickhouse_rs::{Client as ClickhouseClient, Pool as ClickhousePool};
use futures::stream::{self, StreamExt};
use log::{error, info, warn};
use mongodb::{
    bson::{self, doc, Bson, Document},
    options::FindOptions,
    Client as MongoClient,
};
use rayon::prelude::*;
use regex::Regex;
use serde::Deserialize;
use serde_json::to_string;
use sha2::{Digest, Sha256};

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Timelike, Utc};
use std::{
    env,
    error::Error,
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{signal, sync::mpsc};
use tokio::{
    sync::{broadcast, oneshot},
    task::JoinHandle,
};
type PgPool = Pool<PostgresConnectionManager<tokio_postgres::NoTls>>;

lazy_static::lazy_static! {
    static ref BACKSLASH_REGEX_1: Regex = Regex::new(r"\\{2}").expect("Failed to compile BACKSLASH_REGEX_1");
    static ref BACKSLASH_REGEX_2: Regex = Regex::new(r"\\(?:\\\\)*").expect("Failed to compile BACKSLASH_REGEX_2");
    static ref BACKSLASH_REGEX_3: Regex = Regex::new(r"\\{4,}").expect("Failed to compile BACKSLASH_REGEX_3");
}

const MAX_BATCH_SIZE: usize = 10000;
const MAX_RETRIES: u32 = 5;
const INITIAL_RETRY_DELAY: u64 = 1000;

#[derive(Deserialize, Clone)]
struct TenantConfig {
    name: String,
    mongo_uri: String,
    mongo_db: String,
    mongo_collection: String,
    clickhouse_uri: String,
    clickhouse_db: String,
    clickhouse_table: String,
}

#[derive(Deserialize, Clone)]
struct AppConfig {
    tenants: Vec<TenantConfig>,
    encryption_salt: String,
    batch_size: u64,
    number_of_workers: usize,
    pg_database_url: String,
}

type ClickhousePoolType = ClickhousePool;

struct AppState {
    config: AppConfig,
    clickhouse_pools: Vec<ClickhousePoolType>,
    pg_pool: PgPool,
}

async fn run(
    app_state: Arc<AppState>,
    tenant_name: String,
    start_date: chrono::NaiveDateTime,
    end_date: chrono::NaiveDateTime,
) -> Result<bool> {
    let tenant = app_state
        .config
        .tenants
        .iter()
        .find(|t| t.name == tenant_name)
        .ok_or_else(|| anyhow!("Tenant not found in the configuration"))?;

    process_tenant_historical_data(
        Arc::new(tenant.clone()),
        Arc::clone(&app_state),
        0,
        start_date,
        end_date,
    )
    .await
    .map_err(|e| {
        error!("Error processing tenant {}: {}", tenant.name, e);
        e
    })?;

    info!("Successfully processed data for tenant {}", tenant.name);
    Ok(true)
}

async fn connect_to_mongo(mongo_uri: &str) -> Result<MongoClient> {
    let mut retry_delay = Duration::from_secs(1);
    let mut attempts = 0;
    loop {
        match MongoClient::with_uri_str(mongo_uri).await {
            Ok(client) => return Ok(client),
            Err(e) => {
                attempts += 1;
                if attempts > MAX_RETRIES {
                    return Err(e.into());
                }
                error!("Failed to connect to MongoDB: {}. Retrying in {:?}", e, retry_delay);
                tokio::time::sleep(retry_delay).await;
                retry_delay *= 2;
            }
        }
    }
}

async fn process_tenant_historical_data(
    tenant_config: Arc<TenantConfig>,
    app_state: Arc<AppState>,
    pool_index: usize,
    start_date: NaiveDateTime,
    end_date: NaiveDateTime,
) -> Result<()> {
    let mongo_client = connect_to_mongo(&tenant_config.mongo_uri)
        .await
        .context("Failed to connect to MongoDB")?;
    let mongo_db = mongo_client.database(&tenant_config.mongo_db);
    let mongo_collection = mongo_db.collection::<Document>(&tenant_config.mongo_collection);

    info!("Processing data from {} to {}", start_date, end_date);

    let ch_pool = Arc::new(app_state.clickhouse_pools[pool_index].clone());
    let start_datetime = DateTime::<Utc>::from_utc(start_date, Utc);
    let end_datetime = DateTime::<Utc>::from_utc(end_date, Utc);

    let filter = doc! {
        "timestamp": {
            "$gte": bson::DateTime::from_millis(start_datetime.timestamp_millis()),
            "$lte": bson::DateTime::from_millis(end_datetime.timestamp_millis()),
        }
    };

    let total_docs = mongo_collection
    .count_documents(filter.clone(), None)
    .await
    .context("Failed to count documents in MongoDB")? as usize;  // Cast to usize
info!("Total documents to process: {}", total_docs);

    let mut cursor = mongo_collection
        .find(filter, None)
        .await
        .context("Failed to create MongoDB cursor")?;
    let mut batch = Vec::with_capacity(MAX_BATCH_SIZE);
    let mut processed_docs = 0;

    while let Some(result) = cursor.next().await {
        let doc = result.context("Failed to get next document from MongoDB cursor")?;
        let record_id = doc
            .get("_id")
            .and_then(|id| id.as_object_id())
            .ok_or_else(|| anyhow!("Document is missing _id field"))?;
        let statement = doc
            .get("statement")
            .and_then(|s| s.as_document())
            .ok_or_else(|| anyhow!("Document is missing statement field"))?;

        let record_id_str = record_id.to_hex();
        let mut statement = statement.to_owned();

        anonymize_statement(
            &mut statement,
            &app_state.config.encryption_salt,
            &tenant_config.name,
        )?;

        let statement_str = to_string(&statement).context("Failed to serialize statement to JSON")?;
        batch.push((record_id_str, statement_str));

        if batch.len() >= MAX_BATCH_SIZE {
            insert_into_clickhouse(
                &ch_pool,
                &batch,
                &tenant_config.clickhouse_db,
                &tenant_config.clickhouse_table,
                &app_state.pg_pool,
                &tenant_config.name,
            )
            .await
            .context("Failed to insert batch into ClickHouse")?;
            processed_docs += batch.len();
            info!(
                "Processed {} out of {} documents",
                processed_docs, total_docs
            );
            batch.clear();
        }
    }

    // Insert any remaining documents
    if !batch.is_empty() {
        insert_into_clickhouse(
            &ch_pool,
            &batch,
            &tenant_config.clickhouse_db,
            &tenant_config.clickhouse_table,
            &app_state.pg_pool,
            &tenant_config.name,
        )
        .await
        .context("Failed to insert final batch into ClickHouse")?;
        processed_docs += batch.len();
    }

    info!(
        "Completed processing {} out of {} documents",
        processed_docs, total_docs
    );

    if processed_docs < total_docs {
        warn!("Some documents were skipped during processing");
    }

    Ok(())
}

fn anonymize_statement(statement: &mut Document, encryption_salt: &str, tenant_name: &str) -> Result<()> {
    // Implement your anonymization logic here
    // This is a placeholder implementation
    let mut hasher = Sha256::new();
    hasher.update(format!("{}{}", encryption_salt, tenant_name));
    hasher.update(statement.to_string().as_bytes());
    let result = hasher.finalize();
    statement.insert("anonymized_hash", hex::encode(result));
    Ok(())
}

async fn process_statement(statement: &str) -> Result<String> {
    let output1 = BACKSLASH_REGEX_1
        .replace_all(statement, "\\\\\\\\")
        .to_string();

    let output2 = BACKSLASH_REGEX_2.replace_all(&output1, |caps: &regex::Captures| {
        if caps[0].len() % 2 == 1 {
            "\\\\".to_string()
        } else {
            caps[0].to_string()
        }
    });

    let output3 = BACKSLASH_REGEX_3
        .replace_all(&output2, "\\\\\\\\")
        .to_string();

    let trimmed_statement = output3
        .trim_start_matches('"')
        .trim_end_matches('"')
        .replace("\\'", "\\\\'")
        .replace("'", "\\'")
        .to_string();

    Ok(trimmed_statement)
}

async fn insert_into_clickhouse(
    ch_pool: &ClickhousePool,
    bulk_insert_values: &[(String, String)],
    clickhouse_db: &str,
    clickhouse_table: &str,
    pg_pool: &PgPool,
    tenant_name: &str,
) -> Result<()> {
    let full_table_name = format!("{}.{}", clickhouse_db, clickhouse_table);

    for (chunk_index, chunk) in bulk_insert_values.chunks(MAX_BATCH_SIZE).enumerate() {
        let mut retry_count = 0;
        let mut delay = INITIAL_RETRY_DELAY;

        loop {
            match insert_batch(ch_pool, chunk, &full_table_name).await {
                Ok(_) => {
                    info!(
                        "Successfully inserted batch {} of {} records",
                        chunk_index + 1,
                        chunk.len()
                    );
                    break;
                }
                Err(e) => {
                    error!("Failed to insert batch {}: {}", chunk_index + 1, e);
                    retry_count += 1;
                    if retry_count >= MAX_RETRIES {
                        error!(
                            "Max retries reached for batch {}. Logging failed batch.",
                            chunk_index + 1
                        );
                        log_failed_batch(
                            pg_pool,
                            tenant_name,
                            clickhouse_db,
                            clickhouse_table,
                            chunk,
                        )
                        .await
                        .context("Failed to log failed batch")?;
                        return Err(anyhow!(
                            "Max retries exceeded for batch {}",
                            chunk_index + 1
                        ));
                    } else {
                        warn!("Retrying batch {} in {} ms...", chunk_index + 1, delay);
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                        delay = delay.saturating_mul(2); // Exponential backoff with overflow protection
                    }
                }
            }
        }
    }

    Ok(())
}

async fn insert_batch(
    ch_pool: &ClickhousePool,
    batch: &[(String, String)],
    full_table_name: &str,
) -> Result<()> {
    let mut client = ch_pool
        .get_handle()
        .await
        .context("Failed to get client from ClickHouse pool")?;

    let insert_data: Result<Vec<String>> = futures::future::try_join_all(
        batch.iter().map(|(record_id, statement)| async move {
            let processed_statement = process_statement(statement).await?;
            Ok(format!(
                "('{}', '{}', now())",
                record_id, processed_statement
            ))
        }),
    )
    .await;

    let insert_data = insert_data.context("Failed to process statements")?;

    let insert_query = format!(
        "INSERT INTO {} (id, statement, created_at) VALUES {}",
        full_table_name,
        insert_data.join(" , ")
    );

    client
        .execute(insert_query.as_str())
        .await
        .context("Failed to execute insert query")?;

    Ok(())
}

async fn deduplicate_clickhouse_data(
    ch_pool: &ClickhousePool,
    clickhouse_db: &str,
    clickhouse_table: &str,
) -> Result<()> {
    let full_table_name = format!("{}.{}", clickhouse_db, clickhouse_table);
    let mut client = ch_pool
        .get_handle()
        .await
        .context("Failed to get client from ClickHouse pool")?;

    info!("Processing duplicate data...");

    let create_dedup_table_query = format!(
        "CREATE TABLE {table}_dedup ENGINE = MergeTree() PARTITION BY toYYYYMM(created_at) PRIMARY KEY id ORDER BY (id, created_at) SETTINGS index_granularity = 8192 AS SELECT id, any(statement) AS statement, any(created_at) AS created_at FROM {table} GROUP BY id",
        table = full_table_name
    );

    let drop_table_query = format!("DROP TABLE {}", full_table_name);
    let rename_table_query = format!(
        "RENAME TABLE {}_dedup TO {}",
        full_table_name, full_table_name
    );

    client
        .execute(create_dedup_table_query.as_str())
        .await
        .context("Failed to create dedup table in ClickHouse")?;

    client
        .execute(drop_table_query.as_str())
        .await
        .context("Failed to drop original table in ClickHouse")?;

    client
        .execute(rename_table_query.as_str())
        .await
        .context("Failed to rename dedup table in ClickHouse")?;

    Ok(())
}

async fn log_failed_batch(
    pg_pool: &PgPool,
    tenant_name: &str,
    clickhouse_db: &str,
    clickhouse_table: &str,
    failed_batch: &[(String, String)],
) -> Result<()> {
    let failed_batch_json =
        serde_json::to_string(failed_batch).context("Failed to serialize failed batch to JSON")?;

    let mut client = pg_pool
        .get()
        .await
        .context("Failed to get client from PostgreSQL pool")?;

        let statement = client
            .prepare(
                "INSERT INTO failed_batches (tenant_name, clickhouse_db, clickhouse_table, failed_batch)
                 VALUES ($1, $2, $3, $4)",
            )
            .await
            .context("Failed to prepare PostgreSQL statement")?;

        client
            .execute(
                &statement,
                &[
                    &tenant_name,
                    &clickhouse_db,
                    &clickhouse_table,
                    &failed_batch_json,
                ],
            )
            .await
            .context("Failed to execute PostgreSQL statement")?;

        Ok(())
    }

    async fn retry_failed_batches(app_state: Arc<AppState>) -> Result<()> {
        let pg_pool = &app_state.pg_pool;

        loop {
            let mut client = pg_pool.get().await.context("Failed to get PostgreSQL client")?;
            let statement = client
                .prepare(
                    "SELECT id, tenant_name, clickhouse_db, clickhouse_table, failed_batch
                     FROM failed_batches
                     ORDER BY created_at
                     LIMIT 100",
                )
                .await
                .context("Failed to prepare PostgreSQL statement")?;

            let rows = client.query(&statement, &[]).await.context("Failed to execute PostgreSQL query")?;

            for row in rows {
                let failed_batch_id: i32 = row.get(0);
                let tenant_name: String = row.get(1);
                let clickhouse_db: String = row.get(2);
                let clickhouse_table: String = row.get(3);
                let failed_batch: String = row.get(4);

                let tenant_config = app_state
                    .config
                    .tenants
                    .iter()
                    .find(|t| t.name == tenant_name)
                    .cloned();

                if let Some(tenant_config) = tenant_config {
                    let ch_pool = ClickhousePool::new(tenant_config.clickhouse_uri);
                    let bulk_insert_values: Vec<(String, String)> =
                        serde_json::from_str(&failed_batch).context("Failed to deserialize failed batch")?;

                    if let Err(e) = insert_into_clickhouse(
                        &ch_pool,
                        &bulk_insert_values,
                        &clickhouse_db,
                        &clickhouse_table,
                        pg_pool,
                        &tenant_name,
                    )
                    .await
                    {
                        error!("Error retrying failed batch: {}", e);
                    } else {
                        let delete_statement = client
                            .prepare("DELETE FROM failed_batches WHERE id = $1")
                            .await
                            .context("Failed to prepare delete statement")?;
                        client
                            .execute(&delete_statement, &[&failed_batch_id])
                            .await
                            .context("Failed to delete processed failed batch")?;
                    }
                }
            }

            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
    }

    fn validate_date_time(date_time_str: &str) -> Result<NaiveDateTime> {
        NaiveDateTime::parse_from_str(date_time_str, "%Y-%m-%dT%H:%M")
            .map_err(|e| anyhow!("Invalid date and time format: {}", e))
    }

    #[tokio::main]
    async fn main() -> Result<()> {
        env_logger::init();
        info!(target: "main", "Starting up");

        let env = env::var("ENV").unwrap_or_else(|_| "dev".into());
        let config_path = match env.as_str() {
            "dev" => "config-dev.yml",
            "prod" => "config-prod.yml",
            _ => {
                error!("Unsupported environment: {}", env);
                return Err(anyhow!("Unsupported environment"));
            }
        };
        let config: AppConfig = serde_yaml::from_reader(std::fs::File::open(config_path)?)?;

        let tenant_name = env::args()
            .nth(1)
            .ok_or_else(|| anyhow!("Missing tenant name argument"))?;

        let start_date = env::args()
            .nth(2)
            .ok_or_else(|| anyhow!("Missing start date argument"))?;

        let end_date = env::args()
            .nth(3)
            .ok_or_else(|| anyhow!("Missing end date argument"))?;

        let start_date = validate_date_time(&start_date)?;
        let end_date = validate_date_time(&end_date)?;

        if end_date < start_date {
            return Err(anyhow!("End date must be greater than or equal to start date"));
        }

        let tenant = config
            .tenants
            .iter()
            .find(|t| t.name == tenant_name)
            .ok_or_else(|| anyhow!("Tenant not found in the configuration"))?
            .clone();

        let clickhouse_pool = ClickhousePool::new(tenant.clickhouse_uri);
        let pg_manager = PostgresConnectionManager::new_from_stringlike(
            &config.pg_database_url,
            tokio_postgres::NoTls,
        )?;
        let pg_pool = Pool::builder().build(pg_manager).await?;

        let app_state = Arc::new(AppState {
            config: config.clone(),
            clickhouse_pools: vec![clickhouse_pool],
            pg_pool,
        });

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let shutdown_tx_opt = Arc::new(Mutex::new(Some(shutdown_tx)));
        let shutdown_tx_opt_clone = Arc::clone(&shutdown_tx_opt);

        let app_state_clone = app_state.clone();
        let run_handle = tokio::spawn(async move {
            match run(app_state_clone, tenant_name, start_date, end_date).await {
                Ok(success) => {
                    if success {
                        info!("Program ran successfully");
                    } else {
                        error!("Program encountered an error");
                    }
                    if let Some(shutdown_tx) = shutdown_tx_opt.lock().unwrap().take() {
                        if let Err(_) = shutdown_tx.send(()) {
                            error!("Failed to send shutdown signal");
                        }
                    }
                }
                Err(e) => {
                    error!("Error running the program: {}", e);
                    if let Some(shutdown_tx) = shutdown_tx_opt.lock().unwrap().take() {
                        if let Err(_) = shutdown_tx.send(()) {
                            error!("Failed to send shutdown signal");
                        }
                    }
                }
            }
        });

        let retry_handle = tokio::spawn(retry_failed_batches(app_state));

        tokio::select! {
            _ = signal::ctrl_c() => {
                info!("Received shutdown signal, shutting down gracefully...");
                if let Some(shutdown_tx) = shutdown_tx_opt_clone.lock().unwrap().take() {
                    if let Err(_) = shutdown_tx.send(()) {
                        error!("Failed to send shutdown signal");
                    }
                }
            }
            _ = shutdown_rx => {
                info!("Program finished, shutting down gracefully...");
            }
            run_result = run_handle => {
                match run_result {
                    Ok(_) => info!("Run task completed"),
                    Err(e) => error!("Run task failed: {}", e),
                }
            }
            retry_result = retry_handle => {
                match retry_result {
                    Ok(inner_result) => {
                        match inner_result {
                            Ok(_) => info!("Retry task completed successfully"),
                            Err(e) => error!("Retry task failed: {}", e),
                        }
                    }
                    Err(e) => error!("Retry task panicked: {}", e),
                }
            }
        }

        Ok(())
    }
