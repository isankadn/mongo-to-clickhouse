// historical_data/src/main.rs
use anyhow::{anyhow, Context, Result};
use clickhouse_rs::Pool as ClickhousePool;
use futures::stream::StreamExt;
use log::{error, info, warn};
use mongodb::{
    bson::{self, doc, Document},
    Client as MongoClient,
};

use regex::Regex;
use serde::Deserialize;
use serde_json::to_string;
use sha2::{Digest, Sha256};

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use chrono::{DateTime, NaiveDateTime, Utc};

use std::{
    env,
    sync::{Arc, Mutex},
    time::Duration,
    time::Instant,
};
use tokio::signal;
use tokio::sync::oneshot;
type PgPool = Pool<PostgresConnectionManager<tokio_postgres::NoTls>>;

lazy_static::lazy_static! {
    static ref BACKSLASH_REGEX_1: Regex = Regex::new(r"\\{2}").expect("Failed to compile BACKSLASH_REGEX_1");
    static ref BACKSLASH_REGEX_2: Regex = Regex::new(r"\\(?:\\\\)*").expect("Failed to compile BACKSLASH_REGEX_2");
    static ref BACKSLASH_REGEX_3: Regex = Regex::new(r"\\{4,}").expect("Failed to compile BACKSLASH_REGEX_3");
}

struct BatchSizeManager {
    current_size: usize,
    min_size: usize,
    max_size: usize,
    performance_threshold: f64, // in documents per second
}

impl BatchSizeManager {
    fn new(
        initial_size: usize,
        min_size: usize,
        max_size: usize,
        performance_threshold: f64,
    ) -> Self {
        BatchSizeManager {
            current_size: initial_size,
            min_size,
            max_size,
            performance_threshold,
        }
    }

    fn adjust_batch_size(&mut self, docs_processed: usize, time_taken: std::time::Duration) {
        let performance = docs_processed as f64 / time_taken.as_secs_f64();

        if performance > self.performance_threshold {
            self.current_size = (self.current_size * 2).min(self.max_size);
        } else {
            self.current_size = (self.current_size / 2).max(self.min_size);
        }
    }

    fn get_current_size(&self) -> usize {
        self.current_size
    }
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
                error!(
                    "Failed to connect to MongoDB: {}. Retrying in {:?}",
                    e, retry_delay
                );
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
        .context("Failed to count documents in MongoDB")? as usize;
    info!("Total documents to process: {}", total_docs);

    let mut cursor = mongo_collection
        .find(filter, None)
        .await
        .context("Failed to create MongoDB cursor")?;

    let mut batch_manager = BatchSizeManager::new(10000, 1000, 100000, 5000.0);
    let mut batch = Vec::with_capacity(batch_manager.get_current_size());
    let mut processed_docs = 0;
    let mut failed_docs = 0;
    let start_time = Instant::now();

    while let Some(result) = cursor.next().await {
        match result {
            Ok(doc) => {
                let record_id = match doc.get("_id").and_then(|id| id.as_object_id()) {
                    Some(id) => id.to_hex(),
                    None => {
                        warn!("Document is missing _id field, skipping");
                        failed_docs += 1;
                        continue;
                    }
                };

                let statement = match doc.get("statement").and_then(|s| s.as_document()) {
                    Some(s) => s.to_owned(),
                    None => {
                        warn!(
                            "Document {} is missing statement field, skipping",
                            record_id
                        );
                        failed_docs += 1;
                        continue;
                    }
                };

                let mut statement = statement.to_owned();

                if let Err(e) = anonymize_statement(
                    &mut statement,
                    &app_state.config.encryption_salt,
                    &tenant_config.name,
                ) {
                    warn!(
                        "Failed to anonymize statement for document {}: {}",
                        record_id, e
                    );
                    failed_docs += 1;
                    continue;
                }

                let statement_str = match to_string(&statement) {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(
                            "Failed to serialize statement to JSON for document {}: {}",
                            record_id, e
                        );
                        failed_docs += 1;
                        continue;
                    }
                };

                batch.push((record_id, statement_str));

                if batch.len() >= batch_manager.get_current_size() {
                    let batch_start_time = Instant::now();

                    if let Err(e) = insert_into_clickhouse(
                        &ch_pool,
                        &batch,
                        &tenant_config.clickhouse_db,
                        &tenant_config.clickhouse_table,
                        &app_state.pg_pool,
                        &tenant_config.name,
                        &mut batch_manager,
                    )
                    .await
                    {
                        error!("Failed to insert batch into ClickHouse: {}", e);
                        failed_docs += batch.len();
                    } else {
                        let batch_duration = batch_start_time.elapsed();
                        batch_manager.adjust_batch_size(batch.len(), batch_duration);

                        processed_docs += batch.len();
                        info!(
                            "Processed {} out of {} documents. Current batch size: {}",
                            processed_docs,
                            total_docs,
                            batch_manager.get_current_size()
                        );
                    }
                    batch.clear();
                }
            }
            Err(e) => {
                warn!("Error fetching document from cursor: {}", e);
                failed_docs += 1;
            }
        }
    }

    // Insert any remaining documents
    if !batch.is_empty() {
        if let Err(e) = insert_into_clickhouse(
            &ch_pool,
            &batch,
            &tenant_config.clickhouse_db,
            &tenant_config.clickhouse_table,
            &app_state.pg_pool,
            &tenant_config.name,
            &mut batch_manager,
        )
        .await
        {
            error!("Failed to insert final batch into ClickHouse: {}", e);
            failed_docs += batch.len();
        } else {
            processed_docs += batch.len();
        }
    }

    let total_duration = start_time.elapsed();
    info!(
        "Completed processing. Total processed: {}, Total failed: {}, Duration: {:?}, Final batch size: {}",
        processed_docs,
        failed_docs,
        total_duration,
        batch_manager.get_current_size()
    );

    if processed_docs + failed_docs < total_docs {
        warn!("Some documents were skipped during processing");
    }

    Ok(())
}

fn anonymize_statement(
    statement: &mut Document,
    encryption_salt: &str,
    tenant_name: &str,
) -> Result<()> {
    // Create a deep copy of the statement
    let mut statement_copy = statement.clone();

    // Check if all required fields exist
    if !statement_copy.contains_key("actor")
        || !statement_copy
            .get_document("actor")?
            .contains_key("account")
        || !statement_copy
            .get_document("actor")?
            .get_document("account")?
            .contains_key("name")
    {
        return Err(anyhow!("Statement is missing required fields"));
    }

    let name = statement_copy
        .get_document("actor")?
        .get_document("account")?
        .get_str("name")?;

    let value_to_hash = if name.contains('@') {
        name.split('@').next().unwrap_or("")
    } else if name.contains(':') {
        name.split(':').last().unwrap_or("")
    } else {
        name
    };

    if value_to_hash.is_empty() {
        return Err(anyhow!("Empty value to hash for name: {}", name));
    }

    let mut hasher = Sha256::new();
    hasher.update(encryption_salt.as_bytes());
    hasher.update(tenant_name.as_bytes());
    hasher.update(value_to_hash.as_bytes());
    let result = hasher.finalize();
    let hashed_value = hex::encode(result);

    // Update the copy
    statement_copy
        .get_document_mut("actor")?
        .get_document_mut("account")?
        .insert("name", hashed_value);

    // If we've made it this far without errors, update the original statement
    *statement = statement_copy;
    // println!("Anonymized statement: {:?}", statement);
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
    batch_manager: &mut BatchSizeManager,
) -> Result<()> {
    let full_table_name = format!("{}.{}", clickhouse_db, clickhouse_table);

    for (chunk_index, chunk) in bulk_insert_values
        .chunks(batch_manager.get_current_size())
        .enumerate()
    {
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

        // Add logging for batch size changes here
        let old_size = batch_manager.get_current_size();
        batch_manager.adjust_batch_size(chunk.len(), Duration::from_secs(1)); // Assume 1 second per batch, adjust as needed
        let new_size = batch_manager.get_current_size();
        if old_size != new_size {
            info!("Batch size adjusted from {} to {}", old_size, new_size);
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

    let insert_data: Result<Vec<String>> =
        futures::future::try_join_all(batch.iter().map(|(record_id, statement)| async move {
            let processed_statement = process_statement(statement).await?;
            Ok(format!(
                "('{}', '{}', now())",
                record_id, processed_statement
            ))
        }))
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
        let mut client = pg_pool
            .get()
            .await
            .context("Failed to get PostgreSQL client")?;
        let statement = client
            .prepare(
                "SELECT id, tenant_name, clickhouse_db, clickhouse_table, failed_batch
                     FROM failed_batches
                     ORDER BY created_at
                     LIMIT 100",
            )
            .await
            .context("Failed to prepare PostgreSQL statement")?;

        let rows = client
            .query(&statement, &[])
            .await
            .context("Failed to execute PostgreSQL query")?;

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
                let bulk_insert_values: Vec<(String, String)> = serde_json::from_str(&failed_batch)
                    .context("Failed to deserialize failed batch")?;
                let mut batch_manager = BatchSizeManager::new(10000, 1000, 100000, 5000.0);
                if let Err(e) = insert_into_clickhouse(
                    &ch_pool,
                    &bulk_insert_values,
                    &clickhouse_db,
                    &clickhouse_table,
                    pg_pool,
                    &tenant_name,
                    &mut batch_manager,
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
        return Err(anyhow!(
            "End date must be greater than or equal to start date"
        ));
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
