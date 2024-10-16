// src/main.rs - Live data processing
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use clickhouse_rs::Pool as ClickhousePool;
use config::{Config, File};
use futures::stream::StreamExt;
// use log::{error, info, warn};
use mongodb::{
    bson::{self, doc, DateTime as BsonDateTime, Document},
    change_stream::event::{ChangeStreamEvent, ResumeToken},
    options::ChangeStreamOptions,
    Client as MongoClient,
};
use regex::Regex;
use rocksdb::{Error as RocksError, Options, DB};
use serde::Deserialize;
use serde_json::to_string;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::path::Path;

use std::{env, sync::Arc, time::Duration, time::Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tracing::{debug, error, info, instrument, warn};

use futures::future::join_all;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::{signal, sync::oneshot, task};

lazy_static::lazy_static! {
    static ref BACKSLASH_REGEX_1: Regex = Regex::new(r"\\{2}").expect("Failed to compile BACKSLASH_REGEX_1");
    static ref BACKSLASH_REGEX_2: Regex = Regex::new(r"\\(?:\\\\)*").expect("Failed to compile BACKSLASH_REGEX_2");
    static ref BACKSLASH_REGEX_3: Regex = Regex::new(r"\\{4,}").expect("Failed to compile BACKSLASH_REGEX_3");
}

const MAX_BATCH_SIZE: usize = 10000;
const MAX_RETRIES: u32 = 5;
const INITIAL_RETRY_DELAY: u64 = 1000;
const MAX_RETRY_COUNT: usize = 5;

#[derive(Debug)]
pub enum StoreError {
    RocksDB(RocksError),
    OpenFailed,
}
struct RocksDBResumeTokenStore {
    db: Arc<DB>,
}
impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::RocksDB(e) => write!(f, "RocksDB error: {}", e),
            StoreError::OpenFailed => write!(f, "Failed to open database after multiple attempts"),
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StoreError::RocksDB(e) => Some(e),
            StoreError::OpenFailed => None,
        }
    }
}

impl From<RocksError> for StoreError {
    fn from(error: RocksError) -> Self {
        StoreError::RocksDB(error)
    }
}

impl RocksDBResumeTokenStore {
    #[instrument(skip(path), err)]
    pub fn new(path: &str) -> Result<Self, StoreError> {
        info!("Creating new RocksDBResumeTokenStore");
        let db = Self::open_db_aggressive(path)?;
        Ok(Self { db: Arc::new(db) })
    }

    #[instrument(skip(path), err)]
    fn open_db_aggressive(path: &str) -> Result<DB, StoreError> {
        let db_path = Path::new(path);
        let lock_file_path = db_path.join("LOCK");

        // Step 1: Try to open normally
        match Self::try_open_db(path) {
            Ok(db) => return Ok(db),
            Err(e) => warn!("Failed to open RocksDB normally: {}", e),
        }

        // Step 2: Remove LOCK file if it exists
        if lock_file_path.exists() {
            info!("Removing existing LOCK file");
            if let Err(e) = fs::remove_file(&lock_file_path) {
                warn!("Failed to remove LOCK file: {}", e);
            }
        }

        // Step 3: Try to open again after removing LOCK file
        match Self::try_open_db(path) {
            Ok(db) => return Ok(db),
            Err(e) => warn!("Failed to open RocksDB after removing LOCK file: {}", e),
        }

        // Step 4: If all else fails, delete the entire database and create a new one
        warn!("Recreating the entire database");
        if db_path.exists() {
            if let Err(e) = fs::remove_dir_all(db_path) {
                error!("Failed to remove existing database directory: {}", e);
                return Err(StoreError::OpenFailed);
            }
        }

        Self::try_open_db(path)
    }

    fn try_open_db(path: &str) -> Result<DB, StoreError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_max_open_files(10_000);
        opts.set_keep_log_file_num(10);
        opts.set_max_total_wal_size(64 * 1024 * 1024); // 64MB
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB
        opts.set_max_write_buffer_number(3);
        opts.set_target_file_size_base(64 * 1024 * 1024); // 64MB
        opts.set_level_zero_file_num_compaction_trigger(8);
        opts.set_level_zero_slowdown_writes_trigger(17);
        opts.set_level_zero_stop_writes_trigger(24);
        opts.set_num_levels(4);
        opts.set_max_bytes_for_level_base(512 * 1024 * 1024); // 512MB
        opts.set_max_bytes_for_level_multiplier(8.0);

        DB::open(&opts, path).map_err(|e| {
            error!("Failed to open RocksDB: {}", e);
            StoreError::RocksDB(e)
        })
    }

    #[instrument(skip(self, key), err)]
    pub fn get_resume_token(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        debug!("Getting resume token");
        self.db.get(key).map_err(|e| {
            error!("Failed to get resume token: {}", e);
            StoreError::from(e)
        })
    }

    #[instrument(skip(self, key, value), err)]
    pub fn set_resume_token(&self, key: &str, value: &[u8]) -> Result<(), StoreError> {
        debug!("Setting resume token for key: {}", key);
        self.db.put(key.as_bytes(), value).map_err(|e| {
            error!("Failed to set resume token: {}", e);
            StoreError::from(e)
        })
    }
    // Add other methods as needed
}

#[derive(Deserialize, Clone, Debug)]
struct TenantConfig {
    name: String,
    mongo_uri: String,
    mongo_db: String,
    mongo_collection: String,
    clickhouse_uri: String,
    clickhouse_db: String,
    clickhouse_table: String,
    clickhouse_table_opt_out: String,
}

#[derive(Deserialize, Debug)]
struct AppConfig {
    tenants: Vec<TenantConfig>,
    encryption_salt: String,
    batch_size: usize,
    clickhouse_uri: String,
}

type ClickhousePoolType = ClickhousePool;

struct AppState {
    config: AppConfig,
    clickhouse_pools: Vec<ClickhousePoolType>,
    resume_token_store: Arc<RocksDBResumeTokenStore>,
    cached_hashes: Arc<RwLock<CachedHashSet>>,
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
        let old_size = self.current_size;

        if performance > self.performance_threshold {
            self.current_size = (self.current_size * 2).min(self.max_size);
        } else {
            self.current_size = (self.current_size / 2).max(self.min_size);
        }

        info!(
            "Batch size adjusted: {} -> {}. Performance: {:.2}, Threshold: {:.2}",
            old_size, self.current_size, performance, self.performance_threshold
        );
    }

    fn get_current_size(&self) -> usize {
        self.current_size // if enable this applicaitn will automatically adjust the batch size based on the system performance.
    }
}

async fn run(app_state: Arc<AppState>) -> Result<()> {
    info!("Starting main run loop");
    let tenants = app_state.config.tenants.clone();

    let mut tasks = Vec::new();
    for (index, tenant) in tenants.iter().enumerate() {
        let tenant_config = tenant.clone();
        let app_state = app_state.clone();
        let task = task::spawn(async move {
            loop {
                match process_tenant_records(tenant_config.clone(), app_state.clone(), index).await
                {
                    Ok(_) => {
                        info!("Tenant {} processing completed", tenant_config.name);
                        break;
                    }
                    Err(e) => {
                        error!(
                            "Error processing tenant {}: {}. Retrying in 60 seconds...",
                            tenant_config.name, e
                        );
                        tokio::time::sleep(Duration::from_secs(60)).await;
                    }
                }
            }
        });
        tasks.push(task);
    }

    // Wait for all tasks to complete, but don't stop if one fails
    for task in tasks {
        if let Err(e) = task.await {
            error!("A tenant processing task panicked: {:?}", e);
        }
    }

    Ok(())
}

async fn connect_to_mongo(mongo_uri: &str) -> Result<MongoClient> {
    let client = MongoClient::with_uri_str(mongo_uri).await?;

    // Perform a simple operation to check if the connection is actually working
    client.list_database_names(None, None).await?;
    info!("Connected to MongoDB at {}", mongo_uri);
    Ok(client)
}

async fn process_tenant_records(
    tenant_config: TenantConfig,
    app_state: Arc<AppState>,
    pool_index: usize,
) -> Result<()> {
    println!("mongo_uri: {}", tenant_config.mongo_uri);
    let ch_pool = &app_state.clickhouse_pools[pool_index];
    let resume_token_store = &app_state.resume_token_store;

    loop {
        let mongo_client = match connect_to_mongo(&tenant_config.mongo_uri).await {
            Ok(client) => client,
            Err(e) => {
                error!(
                    "Failed to connect to MongoDB for tenant {}: {}",
                    tenant_config.name, e
                );
                tokio::time::sleep(Duration::from_secs(60)).await;
                continue;
            }
        };

        let mongo_db = mongo_client.database(&tenant_config.mongo_db);
        let mongo_collection: mongodb::Collection<Document> =
            mongo_db.collection(&tenant_config.mongo_collection);

        let resume_token = resume_token_store.get_resume_token(&tenant_config.name)?;
        info!("Resuming change stream for tenant: {}", tenant_config.name);
        info!("Resume token: {:?}", resume_token);

        let mut options = ChangeStreamOptions::default();
        if let Some(token_bytes) = resume_token {
            if let Ok(resume_token) = bson::from_slice::<ResumeToken>(&token_bytes) {
                options.resume_after = Some(resume_token);
            }
        }

        let mut change_stream = match mongo_collection.watch(None, options.clone()).await {
            Ok(stream) => stream,
            Err(e) => {
                if let mongodb::error::ErrorKind::Command(ref cmd_err) = *e.kind {
                    if cmd_err.code == 280 {
                        warn!(
                            "Resume token not found for tenant {}. Clearing resume token and retrying.",
                            tenant_config.name
                        );
                        resume_token_store.set_resume_token(&tenant_config.name, &[])?;
                        options.resume_after = None;
                        continue;
                    }
                }
                error!(
                    "Failed to create change stream for tenant {}: {}",
                    tenant_config.name, e
                );
                tokio::time::sleep(Duration::from_secs(60)).await;
                continue;
            }
        };

        let mut batch: Vec<(String, String, BsonDateTime, String)> =
            Vec::with_capacity(app_state.config.batch_size);
        let mut batch_start_time = Instant::now();
        info!(
            "Starting change stream processing batch size: {}",
            batch.capacity()
        );
        let mut batch_manager =
            BatchSizeManager::new(app_state.config.batch_size, 1, MAX_BATCH_SIZE, 5000.0);

        while let Some(result) = change_stream.next().await {
            info!("Processing change event");
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

                                let hashed_value = match anonymize_statement(
                                    &mut statement,
                                    &app_state.config.encryption_salt,
                                    &tenant_config.name,
                                ) {
                                    Ok(hashed) => hashed,
                                    Err(e) => {
                                        warn!("Failed to anonymize statement: {}", e);
                                        continue;
                                    }
                                };

                                let statement_str = match to_string(&statement) {
                                    Ok(s) => s,
                                    Err(e) => {
                                        error!("Failed to convert statement to string: {}", e);
                                        continue;
                                    }
                                };

                                let timestamp =
                                    match doc.get("timestamp").and_then(|ts| ts.as_datetime()) {
                                        Some(ts) => ts,
                                        None => {
                                            warn!("Document is missing timestamp field, skipping");
                                            continue;
                                        }
                                    };

                                batch.push((
                                    record_id_str,
                                    statement_str,
                                    *timestamp,
                                    hashed_value,
                                ));
                                let should_process = batch.len()
                                    >= batch_manager.get_current_size()
                                    || batch_start_time.elapsed() >= Duration::from_secs(5);

                                if should_process {
                                    let batch_duration = batch_start_time.elapsed();
                                    if let Err(e) = process_batch(
                                        ch_pool,
                                        &batch,
                                        &tenant_config,
                                        resume_token_store,
                                        &mut batch_manager,
                                        batch_duration,
                                        &app_state,
                                    )
                                    .await
                                    {
                                        error!("Failed to process batch: {}", e);
                                        continue;
                                    }

                                    // Update resume token after successful batch processing
                                    if let Some(resume_token) = change_stream.resume_token() {
                                        let token_bytes = bson::to_vec(&resume_token)?;
                                        let tenant_name = tenant_config.name.clone();

                                        if let Err(e) = resume_token_store
                                            .set_resume_token(&tenant_name, &token_bytes)
                                        {
                                            error!(
                                                "Failed to update resume token in RocksDB: {}",
                                                e
                                            );
                                        }
                                    }

                                    batch.clear();
                                    batch_start_time = Instant::now();
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
                    }
                }
                Err(e) => {
                    if let mongodb::error::ErrorKind::Command(ref cmd_err) = *e.kind {
                        if cmd_err.code == 280 {
                            warn!(
                                "Change stream error for tenant {}: Resume token not found. Clearing resume token and restarting stream.",
                                tenant_config.name
                            );
                            resume_token_store.set_resume_token(&tenant_config.name, &[])?;

                            // Process any remaining batch before restarting
                            if !batch.is_empty() {
                                let batch_duration = batch_start_time.elapsed();
                                if let Err(e) = process_batch(
                                    ch_pool,
                                    &batch,
                                    &tenant_config,
                                    resume_token_store,
                                    &mut batch_manager,
                                    batch_duration,
                                    &app_state,
                                )
                                .await
                                {
                                    error!("Failed to process remaining batch: {}", e);
                                } else {
                                    // Update resume token after successful batch processing
                                    if let Some(resume_token) = change_stream.resume_token() {
                                        let token_bytes = bson::to_vec(&resume_token)?;
                                        let tenant_name = tenant_config.name.clone();

                                        if let Err(e) = resume_token_store
                                            .set_resume_token(&tenant_name, &token_bytes)
                                        {
                                            error!(
                                                "Failed to update resume token in RocksDB: {}",
                                                e
                                            );
                                        }
                                    }
                                }
                                batch.clear();
                            }

                            break; // Break to restart the change stream
                        }
                    }
                    error!(
                        "Change stream error for tenant {}: {}. Restarting stream...",
                        tenant_config.name, e
                    );

                    // Process any remaining batch before restarting
                    if !batch.is_empty() {
                        let batch_duration = batch_start_time.elapsed();
                        if let Err(e) = process_batch(
                            ch_pool,
                            &batch,
                            &tenant_config,
                            resume_token_store,
                            &mut batch_manager,
                            batch_duration,
                            &app_state,
                        )
                        .await
                        {
                            error!("Failed to process remaining batch: {}", e);
                        } else {
                            // Update resume token after successful batch processing
                            if let Some(resume_token) = change_stream.resume_token() {
                                let token_bytes = bson::to_vec(&resume_token)?;
                                let tenant_name = tenant_config.name.clone();

                                if let Err(e) =
                                    resume_token_store.set_resume_token(&tenant_name, &token_bytes)
                                {
                                    error!("Failed to update resume token in RocksDB: {}", e);
                                }
                            }
                        }
                        batch.clear();
                    }

                    tokio::time::sleep(Duration::from_secs(60)).await;
                    break; // Break to restart the change stream
                }
            }
        }

        // After the while loop ends, process any remaining batch
        if !batch.is_empty() {
            let batch_duration = batch_start_time.elapsed();
            if let Err(e) = process_batch(
                ch_pool,
                &batch,
                &tenant_config,
                resume_token_store,
                &mut batch_manager,
                batch_duration,
                &app_state,
            )
            .await
            {
                error!("Failed to process final batch: {}", e);
            } else {
                // Update resume token after successful batch processing
                if let Some(resume_token) = change_stream.resume_token() {
                    let token_bytes = bson::to_vec(&resume_token)?;
                    let tenant_name = tenant_config.name.clone();

                    if let Err(e) = resume_token_store.set_resume_token(&tenant_name, &token_bytes)
                    {
                        error!("Failed to update resume token in RocksDB: {}", e);
                    }
                }
            }
            batch.clear();
        }
    }
}

async fn process_batch(
    ch_pool: &ClickhousePoolType,
    batch: &[(String, String, BsonDateTime, String)],
    tenant_config: &TenantConfig,
    resume_token_store: &Arc<RocksDBResumeTokenStore>,
    batch_manager: &mut BatchSizeManager,
    batch_duration: Duration,
    app_state: &Arc<AppState>,
) -> Result<()> {
    if let Err(e) = insert_into_clickhouse(
        ch_pool,
        batch,
        &tenant_config.clickhouse_db,
        &tenant_config.clickhouse_table,
        &tenant_config.clickhouse_table_opt_out,
        resume_token_store,
        &tenant_config.name,
        batch_manager,
        &app_state,
    )
    .await
    {
        error!("Failed to insert batch into ClickHouse: {}", e);
    } else {
        batch_manager.adjust_batch_size(batch.len(), batch_duration);
        info!(
            "Processed {} documents in {:?}. Current batch size: {}",
            batch.len(),
            batch_duration,
            batch_manager.get_current_size()
        );
    }
    Ok(())
}

fn anonymize_statement(
    statement: &mut Document,
    encryption_salt: &str,
    tenant_name: &str,
) -> Result<String> {
    let actor = statement
        .get_document_mut("actor")
        .context("Missing 'actor' field")?;
    let account = actor
        .get_document_mut("account")
        .context("Missing 'account' field in actor")?;
    let name = account
        .get_str("name")
        .context("Missing 'name' field in account")?;

    let value_to_hash = name
        .split('@')
        .next()
        .or_else(|| name.split(':').last())
        .unwrap_or(name);

    if value_to_hash.is_empty() {
        return Err(anyhow!("Empty value to hash for name: {}", name));
    }

    let mut hasher = Sha256::new();
    hasher.update(encryption_salt.as_bytes());
    hasher.update(tenant_name.as_bytes());
    hasher.update(value_to_hash.as_bytes());
    let hashed_value = hex::encode(hasher.finalize());

    account.insert("name", hashed_value.clone());
    Ok(hashed_value)
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
    bulk_insert_values: &[(String, String, BsonDateTime, String)],
    clickhouse_db: &str,
    clickhouse_table: &str,
    clickhouse_table_opt_out: &str,
    resume_token_store: &RocksDBResumeTokenStore,
    tenant_name: &str,
    batch_manager: &mut BatchSizeManager,
    app_state: &Arc<AppState>,
) -> Result<()> {
    let full_table_name = format!("{}.{}", clickhouse_db, clickhouse_table);
    let full_table_name_opt_out = format!("{}.{}", clickhouse_db, clickhouse_table_opt_out);

    for (chunk_index, chunk) in bulk_insert_values
        .chunks(batch_manager.get_current_size())
        .enumerate()
    {
        let mut retry_count = 0;
        let mut delay = INITIAL_RETRY_DELAY;

        loop {
            match insert_batch(
                ch_pool,
                chunk,
                &full_table_name,
                &full_table_name_opt_out,
                &app_state.cached_hashes,
            )
            .await
            {
                Ok(_) => {
                    // println!("chunk---------{:?}", chunk);
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
                            resume_token_store,
                            tenant_name,
                            clickhouse_db,
                            clickhouse_table,
                            clickhouse_table_opt_out,
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

struct CachedHashSet {
    data: Arc<RwLock<HashSet<String>>>,
    last_updated: Arc<RwLock<DateTime<Utc>>>,
}

impl CachedHashSet {
    async fn new(ch_pool: &ClickhousePool) -> Result<Self> {
        let data = Arc::new(RwLock::new(HashSet::new()));
        let last_updated = Arc::new(RwLock::new(Utc::now()));
        let cache = Self { data, last_updated };
        cache.refresh(ch_pool).await?;
        Ok(cache)
    }

    #[instrument(skip(self, ch_pool))]
    async fn refresh(&self, ch_pool: &ClickhousePool) -> Result<()> {
        info!("Refreshing cached HashSet");
        let mut client = ch_pool.get_handle().await?;
        let query = "SELECT email, hashed_moodle_id FROM default.moodle_ids WHERE (email, version) IN ( SELECT email, MAX(version) AS max_version FROM default.moodle_ids GROUP BY email )";
        let mut cursor = client.query(query).stream();
        let mut new_data = HashSet::new();

        while let Some(row) = cursor.next().await {
            let row = row.context("Failed to fetch row")?;
            let hash: String = row.get("hashed_moodle_id")?;
            debug!("Inserting hashed value: {}", hash); // Added debug log
            new_data.insert(hash);
        }

        let mut data = self.data.write().await;
        *data = new_data;
        let mut last_updated = self.last_updated.write().await;
        *last_updated = Utc::now();

        info!("Cached HashSet refreshed successfully");
        debug!("Total number of hashed values: {}", data.len()); // Added debug log
        Ok(())
    }
    async fn contains(&self, value: &str) -> bool {
        let data = self.data.read().await;
        data.contains(value)
    }
}

#[instrument(skip(socket, cached_hashes, ch_pool))]
async fn handle_client(
    mut socket: TcpStream,
    cached_hashes: Arc<RwLock<CachedHashSet>>,
    ch_pool: Arc<ClickhousePool>,
) -> Result<()> {
    let mut buffer = [0; 1024];
    let n = socket.read(&mut buffer).await?;
    let command = String::from_utf8_lossy(&buffer[..n]).to_string();

    match command.trim() {
        "invalidate" => {
            info!("Received invalidation command");
            cached_hashes.write().await.refresh(&ch_pool).await?;
            socket.write_all(b"Cache invalidated successfully").await?;
        }
        _ => {
            socket.write_all(b"Unknown command").await?;
        }
    }
    socket.write_all(b"OK\n").await?;
    Ok(())
}

async fn insert_batch(
    ch_pool: &ClickhousePool,
    batch: &[(String, String, BsonDateTime, String)],
    full_table_name: &str,
    full_table_name_opt_out: &str,
    cached_hashes: &Arc<RwLock<CachedHashSet>>,
) -> Result<()> {
    let mut client = match ch_pool.get_handle().await {
        Ok(client) => client,
        Err(e) => {
            error!("Failed to get ClickHouse client: {:?}", e);
            return Err(anyhow!("ClickHouse connection error: {:?}", e));
        }
    };
    let mut insert_data = Vec::new();
    let mut insert_data_opt_out = Vec::new();

    let cached_hashes_guard = cached_hashes.read().await;
    debug!(
        "Cached HashSet last updated: {:?}",
        *cached_hashes_guard.data.read().await
    );
    let processing_futures = batch
        .iter()
        .map(|(record_id, statement, timestamp, hashed_value)| {
            let cached_hashes_guard = &cached_hashes_guard;
            async move {
                let processed_statement = process_statement(statement).await?;
                let is_opt_out = cached_hashes_guard.contains(hashed_value).await;
                debug!(
                    "Processed statement: {}, is_opt_out: {}",
                    processed_statement, is_opt_out
                );
                let millis = timestamp.timestamp_millis();
                let chrono_timestamp: DateTime<Utc> =
                    DateTime::from_timestamp_millis(millis).unwrap();

                // Format the timestamp for ClickHouse
                let formatted_timestamp = chrono_timestamp.format("%Y-%m-%d %H:%M:%S%.3f");

                let formatted = format!(
                    "('{}', '{}', now(), '{}')",
                    record_id, processed_statement, formatted_timestamp
                );
                Ok::<_, anyhow::Error>((formatted, is_opt_out))
            }
        });

    let results = join_all(processing_futures).await;

    for result in results {
        match result {
            Ok((formatted, is_opt_out)) => {
                if is_opt_out {
                    insert_data_opt_out.push(formatted);
                } else {
                    insert_data.push(formatted);
                }
            }
            Err(e) => return Err(e),
        }
    }

    drop(cached_hashes_guard); // Release the read lock

    // Insert into full_table_name
    if !insert_data.is_empty() {
        let insert_query = format!(
            "INSERT INTO {} (id, statement, created_at, timestamp) VALUES {}",
            full_table_name,
            insert_data.join(", ")
        );
        client
            .execute(insert_query.as_str())
            .await
            .context("Failed to execute insert query for full_table_name")?;
    }

    // Insert into full_table_name_opt_out
    if !insert_data_opt_out.is_empty() {
        let insert_query_opt_out = format!(
            "INSERT INTO {} (id, statement, created_at, timestamp) VALUES {}",
            full_table_name_opt_out,
            insert_data_opt_out.join(", ")
        );
        client
            .execute(insert_query_opt_out.as_str())
            .await
            .context("Failed to execute insert query for full_table_name_opt_out")?;
    }

    Ok(())
}

async fn log_failed_batch(
    resume_token_store: &RocksDBResumeTokenStore,
    tenant_name: &str,
    clickhouse_db: &str,
    clickhouse_table: &str,
    clickhouse_table_opt_out: &str,
    failed_batch: &[(String, String, BsonDateTime, String)],
) -> Result<()> {
    let failed_batch_json =
        serde_json::to_string(failed_batch).context("Failed to serialize failed batch to JSON")?;

    resume_token_store.db.put(
        format!(
            "failed_batch:{}:{}:{}:{}",
            tenant_name, clickhouse_db, clickhouse_table, clickhouse_table_opt_out
        )
        .as_bytes(),
        failed_batch_json.as_bytes(),
    )?;

    Ok(())
}

async fn retry_failed_batches(app_state: Arc<AppState>) -> Result<()> {
    let resume_token_store = &app_state.resume_token_store;
    loop {
        let iter = resume_token_store.db.iterator(rocksdb::IteratorMode::Start);
        let failed_batch_prefix = b"failed_batch:";

        for item in iter {
            let (key, value) = item?;
            if key.starts_with(failed_batch_prefix) {
                let key_str = String::from_utf8_lossy(&key);
                // println!("key_str: {}", key_str);
                let parts: Vec<&str> = key_str.splitn(4, ':').collect();
                if parts.len() != 4 {
                    error!("Invalid failed batch key format: {}", key_str);
                    continue;
                }

                let tenant_name = parts[1];
                let clickhouse_db = parts[2];
                let clickhouse_table = parts[3];
                let clickhouse_table_opt_out = parts[4];

                let failed_batch: Vec<(String, String, BsonDateTime, String)> =
                    serde_json::from_slice(&value).context("Failed to deserialize failed batch")?;

                let tenant_config = app_state
                    .config
                    .tenants
                    .iter()
                    .find(|t| t.name == tenant_name)
                    .cloned();

                if let Some(tenant_config) = tenant_config {
                    let ch_pool = ClickhousePool::new(tenant_config.clickhouse_uri.as_str());
                    let mut batch_manager = BatchSizeManager::new(
                        app_state.config.batch_size,
                        1,
                        MAX_BATCH_SIZE,
                        1000.0,
                    );

                    match insert_into_clickhouse(
                        &ch_pool,
                        &failed_batch,
                        clickhouse_db,
                        clickhouse_table,
                        clickhouse_table_opt_out,
                        resume_token_store,
                        tenant_name,
                        &mut batch_manager,
                        &app_state,
                    )
                    .await
                    {
                        Ok(_) => {
                            info!(
                                "Successfully retried failed batch for tenant: {}",
                                tenant_name
                            );
                            resume_token_store.db.delete(key)?;
                        }
                        Err(e) => {
                            error!(
                                "Error retrying failed batch for tenant {}: {}",
                                tenant_name, e
                            );
                        }
                    }
                } else {
                    error!("Tenant config not found for tenant: {}", tenant_name);
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

async fn run_socket_server(
    cached_hashes: Arc<RwLock<CachedHashSet>>,
    ch_pool: Arc<ClickhousePool>,
    retry_count: Arc<AtomicUsize>,
) -> Result<()> {
    loop {
        match TcpListener::bind("0.0.0.0:8088").await {
            Ok(listener) => {
                info!("Socket server listening on 0.0.0.0:8088");
                retry_count.store(0, Ordering::SeqCst);

                while let Ok((socket, _)) = listener.accept().await {
                    let cached_hashes = Arc::clone(&cached_hashes);
                    let ch_pool = Arc::clone(&ch_pool);

                    tokio::spawn(async move {
                        if let Err(e) = handle_client(socket, cached_hashes, ch_pool).await {
                            error!("Error handling client: {:?}", e);
                        }
                    });
                }
            }
            Err(e) => {
                error!("Failed to bind socket server: {:?}", e);
                let current_retry = retry_count.fetch_add(1, Ordering::SeqCst);
                if current_retry >= MAX_RETRY_COUNT {
                    error!("Socket server failed to start after {} attempts. Shutting down socket server.", MAX_RETRY_COUNT);
                    return Err(anyhow!("Socket server permanently down"));
                }
                let backoff_duration = Duration::from_secs(2u64.pow(current_retry as u32));
                error!("Retrying in {:?}...", backoff_duration);
                tokio::time::sleep(backoff_duration).await;
            }
        }

        error!("Socket server stopped unexpectedly. Restarting...");
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

    let mut clickhouse_pools = Vec::new();
    for tenant in &config.tenants {
        let clickhouse_pool = ClickhousePool::new(tenant.clickhouse_uri.as_str());
        clickhouse_pools.push(clickhouse_pool);
    }
    let ch_pool = Arc::new(ClickhousePool::new(config.clickhouse_uri.as_str()));
    let cached_hashes = Arc::new(RwLock::new(CachedHashSet::new(&ch_pool).await?));
    let resume_token_store = Arc::new(
        RocksDBResumeTokenStore::new("/app/data/rocksdb")
            .expect("Failed to create resume token store"),
    );

    let app_state = Arc::new(AppState {
        config,
        clickhouse_pools,
        resume_token_store,
        cached_hashes: Arc::clone(&cached_hashes),
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

    let retry_count = Arc::new(AtomicUsize::new(0));
    let socket_server_handle = tokio::spawn({
        let cached_hashes = Arc::clone(&cached_hashes);
        let ch_pool = Arc::clone(&ch_pool);
        let retry_count = Arc::clone(&retry_count);
        async move {
            if let Err(e) = run_socket_server(cached_hashes, ch_pool, retry_count).await {
                error!("Socket server encountered a permanent error: {:?}", e);
                error!("Socket server will not be restarted.");
            }
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
        _ = socket_server_handle => {
            if retry_count.load(Ordering::SeqCst) >= MAX_RETRY_COUNT {
                warn!("Socket server has permanently shut down after maximum retry attempts.");
            } else {
                info!("Socket server has unexpectedly shut down.");
            }
        }
    }

    info!("Shutting down...");
    info!("Application shutdown complete.");
    Ok(())
}
