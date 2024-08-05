// src/main.rs - Live data processing
use anyhow::{anyhow, Context, Result};
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use clickhouse_rs::Pool as ClickhousePool;
use config::{Config, File};
use futures::{future::join_all, stream::StreamExt};
use log::{error, info, warn};
use mongodb::{
    bson::{self, doc, Document},
    change_stream::event::{ChangeStreamEvent, ResumeToken},
    options::ChangeStreamOptions,
    Client as MongoClient,
};
use regex::Regex;
use serde::Deserialize;
use serde_json::to_string;
use sha2::{Digest, Sha256};
use std::{env, sync::Arc, time::Duration};
use tokio::{signal, sync::oneshot, task};

lazy_static::lazy_static! {
    static ref BACKSLASH_REGEX_1: Regex = Regex::new(r"\\{2}").expect("Failed to compile BACKSLASH_REGEX_1");
    static ref BACKSLASH_REGEX_2: Regex = Regex::new(r"\\(?:\\\\)*").expect("Failed to compile BACKSLASH_REGEX_2");
    static ref BACKSLASH_REGEX_3: Regex = Regex::new(r"\\{4,}").expect("Failed to compile BACKSLASH_REGEX_3");
}

const MAX_BATCH_SIZE: usize = 10000;
const MAX_RETRIES: u32 = 5;
const INITIAL_RETRY_DELAY: u64 = 1000;

#[derive(Deserialize, Clone, Debug)]
struct TenantConfig {
    name: String,
    mongo_uri: String,
    mongo_db: String,
    mongo_collection: String,
    clickhouse_uri: String,
    clickhouse_db: String,
    clickhouse_table: String,
}

#[derive(Deserialize, Debug)]
struct AppConfig {
    tenants: Vec<TenantConfig>,
    pg_database_url: String,
    encryption_salt: String,
    batch_size: usize,
    number_of_workers: usize,
}

type PostgresPool = Pool<PostgresConnectionManager<tokio_postgres::NoTls>>;
type ClickhousePoolType = ClickhousePool;

struct AppState {
    config: AppConfig,
    postgres_pool: PostgresPool,
    clickhouse_pools: Vec<ClickhousePoolType>,
}

struct BatchSizeManager {
    current_size: usize,
    min_size: usize,
    max_size: usize,
    performance_threshold: f64,
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

    fn adjust_batch_size(&mut self, docs_processed: usize, time_taken: Duration) {
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

async fn run(app_state: Arc<AppState>) -> Result<()> {
    let tenants = app_state.config.tenants.clone();

    let mut tasks = Vec::new();
    for (index, tenant) in tenants.iter().enumerate() {
        let tenant_config = tenant.clone();
        let app_state = app_state.clone();
        let task = task::spawn(async move {
            if let Err(e) = process_tenant_records(tenant_config.clone(), app_state, index).await {
                error!("Error processing tenant {}: {}", tenant_config.name, e);
            }
        });
        tasks.push(task);
    }

    join_all(tasks).await;
    Ok(())
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

async fn process_tenant_records(
    tenant_config: TenantConfig,
    app_state: Arc<AppState>,
    pool_index: usize,
) -> Result<()> {
    let mongo_client = connect_to_mongo(&tenant_config.mongo_uri).await?;
    let mongo_db = mongo_client.database(&tenant_config.mongo_db);
    let mongo_collection: mongodb::Collection<Document> =
        mongo_db.collection(&tenant_config.mongo_collection);

    let pg_pool = &app_state.postgres_pool;
    let ch_pool = &app_state.clickhouse_pools[pool_index];

    let pg_conn = pg_pool.get().await?;
    let row = pg_conn
        .query_one(
            "SELECT token FROM resume_token WHERE tenant_name = $1 ORDER BY id DESC LIMIT 1",
            &[&tenant_config.name],
        )
        .await
        .ok();

    let mut options = ChangeStreamOptions::default();
    if let Some(row) = row {
        let token_bytes: Vec<u8> = row.get("token");
        if let Ok(resume_token) = bson::from_slice::<ResumeToken>(&token_bytes) {
            options.resume_after = Some(resume_token);
        }
    }

    let mut change_stream = mongo_collection.watch(None, options).await?;
    let mut batch = Vec::with_capacity(app_state.config.batch_size);
    let mut batch_manager =
        BatchSizeManager::new(app_state.config.batch_size, 1000, MAX_BATCH_SIZE, 5000.0);

    while let Some(result) = change_stream.next().await {
        match result {
            Ok(change_event) => {
                if let ChangeStreamEvent {
                    full_document: Some(doc),
                    ..
                } = change_event
                {
                    let record_id = doc.get("_id").and_then(|id| id.as_object_id());
                    let statement = doc.get("statement").and_then(|s| s.as_document());

                    match (record_id, statement) {
                        (Some(record_id), Some(statement)) => {
                            let record_id_str = record_id.to_hex();
                            let mut statement = statement.to_owned();

                            if let Err(e) = anonymize_statement(
                                &mut statement,
                                &app_state.config.encryption_salt,
                                &tenant_config.name,
                            ) {
                                warn!("Failed to anonymize statement: {}", e);
                                continue;
                            }

                            let statement_str = match to_string(&statement) {
                                Ok(s) => s,
                                Err(e) => {
                                    error!("Failed to convert statement to string: {}", e);
                                    continue;
                                }
                            };

                            batch.push((record_id_str, statement_str));

                            if batch.len() >= batch_manager.get_current_size() {
                                let batch_start_time = std::time::Instant::now();

                                if let Err(e) = insert_into_clickhouse(
                                    &ch_pool,
                                    &batch,
                                    &tenant_config.clickhouse_db,
                                    &tenant_config.clickhouse_table,
                                    pg_pool,
                                    &tenant_config.name,
                                    &mut batch_manager,
                                )
                                .await
                                {
                                    error!("Failed to insert batch into ClickHouse: {}", e);
                                } else {
                                    let batch_duration = batch_start_time.elapsed();
                                    batch_manager.adjust_batch_size(batch.len(), batch_duration);

                                    info!(
                                        "Processed {} documents. Current batch size: {}",
                                        batch.len(),
                                        batch_manager.get_current_size()
                                    );
                                }
                                batch.clear();
                            }
                        }
                        (None, Some(_)) => {
                            warn!("Missing '_id' field in the document");
                        }
                        (Some(_), None) => {
                            warn!("Missing 'statement' field in the document");
                        }
                        (None, None) => {
                            warn!("Missing both '_id' and 'statement' fields in the document");
                        }
                    }

                    if let Some(resume_token) = change_stream.resume_token() {
                        let token_bytes = bson::to_vec(&resume_token)?;
                        let tenant_name = tenant_config.name.clone();

                        let pg_pool = app_state.postgres_pool.clone();
                        if let Err(e) =
                            update_resume_token(&pg_pool, &token_bytes, &tenant_name).await
                        {
                            error!("Failed to update resume token in PostgreSQL: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                error!("Change stream error: {}", e);
            }
        }
    }

    if !batch.is_empty() {
        if let Err(e) = insert_into_clickhouse(
            &ch_pool,
            &batch,
            &tenant_config.clickhouse_db,
            &tenant_config.clickhouse_table,
            pg_pool,
            &tenant_config.name,
            &mut batch_manager,
        )
        .await
        {
            error!("Failed to insert final batch into ClickHouse: {}", e);
        }
    }
    Ok(())
}

async fn update_resume_token(
    pg_pool: &PostgresPool,
    token_bytes: &[u8],
    tenant_name: &str,
) -> Result<()> {
    let pg_conn = pg_pool.get().await?;
    pg_conn
        .execute(
            "INSERT INTO resume_token (token, tenant_name) VALUES ($1, $2) ON CONFLICT (tenant_name) DO UPDATE SET token = EXCLUDED.token",
            &[&token_bytes, &tenant_name],
        )
        .await?;
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
    pg_pool: &PostgresPool,
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
    pg_pool: &PostgresPool,
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
    let pg_pool = &app_state.postgres_pool;

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
        if rows.is_empty() {
            tokio::time::sleep(Duration::from_secs(60)).await;
            continue;
        }
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

        tokio::time::sleep(Duration::from_secs(60)).await;
    }
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

    let config: AppConfig = Config::builder()
        .add_source(File::with_name(config_path))
        .build()?
        .try_deserialize()
        .context("Failed to deserialize config")?;

    let postgres_manager = PostgresConnectionManager::new(
        config
            .pg_database_url
            .parse()
            .context("Invalid PostgreSQL URL")?,
        tokio_postgres::NoTls,
    );
    let postgres_pool = Pool::builder().build(postgres_manager).await?;

    let mut clickhouse_pools = Vec::new();
    for tenant in &config.tenants {
        let clickhouse_pool = ClickhousePool::new(tenant.clickhouse_uri.as_str());
        clickhouse_pools.push(clickhouse_pool);
    }

    let app_state = Arc::new(AppState {
        config,
        postgres_pool,
        clickhouse_pools,
    });

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let shutdown_tx = Arc::new(tokio::sync::Mutex::new(Some(shutdown_tx)));

    let app_state_clone = app_state.clone();
    let run_handle = tokio::spawn(async move {
        if let Err(e) = run(app_state_clone).await {
            error!("Error in main run loop: {}", e);
        }
    });

    let app_state_clone = app_state.clone();
    let retry_handle = tokio::spawn(async move {
        if let Err(e) = retry_failed_batches(app_state_clone).await {
            error!("Error in retry failed batches loop: {}", e);
        }
    });

    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("Received shutdown signal, shutting down gracefully...");
            if let Some(tx) = shutdown_tx.lock().await.take() {
                let _ = tx.send(());
            }
        }
        _ = shutdown_rx => {
            info!("Shutdown signal received from within the application...");
        }
        _ = run_handle => {
            info!("Main run loop has completed.");
        }
        _ = retry_handle => {
            info!("Retry failed batches loop has completed.");
        }
    }

    info!("Shutting down...");
    info!("Application shutdown complete.");
    Ok(())
}
