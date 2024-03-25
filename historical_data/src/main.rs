use anyhow::{anyhow, Result};
use clickhouse_rs::{Client as ClickhouseClient, Pool as ClickhousePool};
use futures::stream::{self, StreamExt};
use log::{error, info};
use mongodb::{
    bson::{self, doc, Bson, Document},
    options::FindOptions,
    Client as MongoClient,
};
use rayon::prelude::*;
use serde::Deserialize;
use serde_json::to_string;
use sha2::{Digest, Sha256};

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use chrono::{DateTime, NaiveDate, Timelike, Utc};
use rayon::prelude::*;
use std::{
    env,
    error::Error,
    sync::{Arc, Mutex},
};
use tokio::{signal, sync::mpsc};
use tokio::{
    sync::{broadcast, oneshot},
    task::JoinHandle,
};
type PgPool = Pool<PostgresConnectionManager<tokio_postgres::NoTls>>;

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
    start_date: chrono::NaiveDate,
    end_date: chrono::NaiveDate,
) -> Result<bool, Box<dyn Error>> {
    let tenant = app_state
        .config
        .tenants
        .iter()
        .find(|t| t.name == tenant_name)
        .ok_or_else(|| anyhow!("Tenant not found in the configuration"))?;
    // println!("tenant name {:?}", tenant.name);
    if let Err(e) = process_tenant_historical_data(
        Arc::new(tenant.clone()),
        Arc::clone(&app_state),
        0,
        start_date,
        end_date,
    )
    .await
    {
        error!("Error processing tenant {}: {}", tenant.name, e);
    }

    let ch_pool = &app_state.clickhouse_pools[0];
    match deduplicate_clickhouse_data(ch_pool, &tenant.clickhouse_db, &tenant.clickhouse_table)
        .await
    {
        Ok(_) => {
            info!("Successfully deduplicated data for tenant {}", tenant.name);
            Ok(true)
        }
        Err(e) => {
            error!("Error deduplicating data for tenant {}: {}", tenant.name, e);
            Ok(false)
        }
    }
}

async fn process_tenant_historical_data(
    tenant_config: Arc<TenantConfig>,
    app_state: Arc<AppState>,
    pool_index: usize,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<()> {
    let mongo_client = MongoClient::with_uri_str(&tenant_config.mongo_uri)
        .await
        .map_err(|e| anyhow!("Failed to connect to MongoDB: {}", e))?;
    let mongo_db = mongo_client.database(&tenant_config.mongo_db);
    let mongo_collection = mongo_db.collection::<Document>(&tenant_config.mongo_collection);

    let ch_pool = Arc::new(app_state.clickhouse_pools[pool_index].clone());
    let start_datetime = DateTime::<Utc>::from_utc(start_date.and_hms(0, 0, 0), Utc);
    let end_datetime = DateTime::<Utc>::from_utc(end_date.and_hms(23, 59, 59), Utc);

    let filter = doc! {
        "timestamp": {
            "$gte": bson::DateTime::from_millis(start_datetime.timestamp_millis()),
            "$lte": bson::DateTime::from_millis(end_datetime.timestamp_millis()),
        }
    };
    let filter_clone = filter.clone();
    let total_docs = mongo_collection.count_documents(filter_clone, None).await?;

    info!(
        "Total documents in {}: {}",
        tenant_config.mongo_collection, total_docs
    );

    let batch_size = app_state.config.batch_size;
    let num_batches = (total_docs as f64 / batch_size as f64).ceil() as u64;

    let batches: Vec<_> = (0..num_batches)
        .into_par_iter()
        .map(|batch_index| {
            let tenant_config = Arc::clone(&tenant_config);
            let ch_pool = Arc::clone(&ch_pool);
            let pg_pool = app_state.pg_pool.clone();
            let mongo_collection = mongo_collection.clone();
            let filter = filter.clone();
            let app_state = Arc::clone(&app_state);

            async move {
                let skip = batch_index * batch_size;
                let options = FindOptions::builder()
                    .skip(skip)
                    .limit(batch_size as i64)
                    .projection(doc! { "_id": 1, "statement": 1 })
                    .build();

                let mut cursor = match mongo_collection.find(filter, options).await {
                    Ok(cursor) => cursor,
                    Err(e) => {
                        error!("Error retrieving documents from MongoDB: {}", e);
                        return Vec::new();
                    }
                };

                let mut batch = Vec::with_capacity(batch_size as usize);
                while let Some(result) = cursor.next().await {
                    if let Ok(doc) = result {
                        let record_id = doc.get("_id").and_then(|id| id.as_object_id());
                        let statement = doc.get("statement").and_then(|s| s.as_document());

                        if let (Some(record_id), Some(statement)) = (record_id, statement) {
                            let record_id_str = record_id.to_hex();

                            let mut statement = statement.to_owned();
                            if let Some(actor) =
                                statement.get_mut("actor").and_then(|a| a.as_document_mut())
                            {
                                if let Some(account) = actor
                                    .get_mut("account")
                                    .and_then(|acc| acc.as_document_mut())
                                {
                                    if let Some(name) = account.get_mut("name") {
                                        let anonymized_name = anonymize_data(
                                            name,
                                            &app_state.config.encryption_salt,
                                            &tenant_config.name,
                                        );
                                        *name = bson::Bson::String(anonymized_name);
                                    }
                                }
                            }

                            let statement_str = to_string(&statement).unwrap_or_default();
                            batch.push((record_id_str, statement_str));
                        }
                    }
                }

                if let Err(e) = insert_into_clickhouse(
                    &ch_pool,
                    &batch,
                    &tenant_config.clickhouse_db,
                    &tenant_config.clickhouse_table,
                    &pg_pool,
                    &tenant_config.name,
                )
                .await
                {
                    error!("Error inserting into ClickHouse: {}", e);
                }

                batch
            }
        })
        .collect();

    let handles: Vec<JoinHandle<Vec<_>>> = batches.into_iter().map(tokio::spawn).collect();

    for handle in handles {
        handle.await.unwrap();
    }

    Ok(())
}

fn anonymize_data(data: &bson::Bson, encryption_salt: &str, tenant_name: &str) -> String {
    let mut hasher = Sha256::new();
    let salt = format!("{}{}", encryption_salt, tenant_name);
    hasher.update(salt.as_bytes());

    let data_str = match data {
        bson::Bson::String(s) => s.as_str(),
        bson::Bson::Int32(i) => return i.to_string(),
        bson::Bson::Int64(i) => return i.to_string(),
        _ => return String::new(),
    };

    hasher.update(data_str.as_bytes());
    let result = hasher.finalize();
    hex::encode(result)
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
    let mut client = match ch_pool.get_handle().await {
        Ok(client) => client,
        Err(e) => {
            let err_msg = format!("Failed to get client from ClickHouse pool: {}", e);
            error!("{}", err_msg);
            return Err(anyhow!(err_msg));
        }
    };

    let max_retries = 5;
    let mut retry_count = 0;
    // info!("bulk_insert_values: {:?}", bulk_insert_values);
    while retry_count < max_retries {
        let insert_data: Vec<String> = bulk_insert_values
            .iter()
            .map(|(record_id, statement)| {
                let escaped_statement = statement.replace("'", "\\'");
                format!("('{}', '{}')", record_id, escaped_statement)
            })
            .collect();

        let insert_query = format!(
            "INSERT INTO {} (id, statement) VALUES {}",
            full_table_name,
            insert_data.join(",")
        );
        // info!("insert_query: {:?}", insert_query);
        match client.execute(insert_query.as_str()).await {
            Ok(_) => {
                info!("Successfully inserted statements into ClickHouse");
                return Ok(());
            }
            Err(e) => {
                let err_msg = format!("Failed to insert statements into ClickHouse: {}", e);
                error!("{}", err_msg);
                retry_count += 1;
                if retry_count == max_retries {
                    let err_msg = "Max retries reached for insertion. Logging failed batch.";
                    error!("{}", err_msg);
                    log_failed_batch(
                        pg_pool,
                        tenant_name,
                        clickhouse_db,
                        clickhouse_table,
                        &bulk_insert_values,
                    )
                    .await?;
                    return Err(e.into());
                } else {
                    let delay_ms = 1000 * retry_count;
                    info!("Retrying in {} ms...", delay_ms);
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }
            }
        }
    }

    Err(anyhow!("Max retries exceeded"))
}

async fn deduplicate_clickhouse_data(
    ch_pool: &ClickhousePool,
    clickhouse_db: &str,
    clickhouse_table: &str,
) -> Result<()> {
    let full_table_name = format!("{}.{}", clickhouse_db, clickhouse_table);
    let mut client = match ch_pool.get_handle().await {
        Ok(client) => client,
        Err(e) => {
            error!("Failed to get client from ClickHouse pool: {}", e);
            return Err(e.into());
        }
    };

    info!("processing duplicate data...");

    let create_dedup_table_query = format!(
        "CREATE TABLE {table}_dedup ENGINE = MergeTree() PARTITION BY toYYYYMM(created_at) PRIMARY KEY id ORDER BY (id, created_at) SETTINGS index_granularity = 8192 AS SELECT id, any(statement) AS statement, any(created_at) AS created_at FROM {table} GROUP BY id",
        table = full_table_name
    );

    let drop_table_query = format!("DROP TABLE {}", full_table_name);
    let rename_table_query = format!(
        "RENAME TABLE {}_dedup TO {}",
        full_table_name, full_table_name
    );

    match client.execute(create_dedup_table_query.as_str()).await {
        Ok(_) => {
            info!("Successfully created dedup table in ClickHouse");
        }
        Err(e) => {
            error!("Failed to create dedup table in ClickHouse: {}", e);
            return Err(e.into());
        }
    }

    match client.execute(drop_table_query.as_str()).await {
        Ok(_) => {
            info!("Successfully dropped original table in ClickHouse");
        }
        Err(e) => {
            error!("Failed to drop original table in ClickHouse: {}", e);
            return Err(e.into());
        }
    }

    match client.execute(rename_table_query.as_str()).await {
        Ok(_) => {
            info!("Successfully renamed dedup table in ClickHouse");
        }
        Err(e) => {
            error!("Failed to rename dedup table in ClickHouse: {}", e);
            return Err(e.into());
        }
    }

    Ok(())
}

async fn log_failed_batch(
    pg_pool: &PgPool,
    tenant_name: &str,
    clickhouse_db: &str,
    clickhouse_table: &str,
    failed_batch: &[(String, String)],
) -> Result<()> {
    let failed_batch_json = serde_json::to_string(failed_batch)?;

    let mut client = pg_pool.get().await?;
    let statement = client
        .prepare(
            "INSERT INTO failed_batches (tenant_name, clickhouse_db, clickhouse_table, failed_batch)
             VALUES ($1, $2, $3, $4)",
        )
        .await?;

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
        .await?;

    Ok(())
}

async fn retry_failed_batches(app_state: Arc<AppState>) -> Result<()> {
    let pg_pool = &app_state.pg_pool;

    loop {
        let mut client = pg_pool.get().await?;
        let statement = client
            .prepare(
                "SELECT id, tenant_name, clickhouse_db, clickhouse_table, failed_batch
                 FROM failed_batches
                 ORDER BY created_at
                 LIMIT 100",
            )
            .await?;

        let rows = client.query(&statement, &[]).await?;

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
                    serde_json::from_str(&failed_batch)?;

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
                        .await?;
                    client
                        .execute(&delete_statement, &[&failed_batch_id])
                        .await?;
                }
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}

fn validate_date(date_str: &str) -> Result<chrono::NaiveDate> {
    chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .map_err(|e| anyhow!("Invalid date format: {}", e))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    info!(target: "main", "Starting up");

    let env = env::var("ENV").unwrap_or_else(|_| "dev".into());
    let config_path = match env.as_str() {
        "dev" => "config-dev.yml",
        "prod" => "config-prod.yml",
        _ => {
            error!("Unsupported environment: {}", env);
            return Err("Unsupported environment".into());
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

    let start_date = validate_date(&start_date)?;
    let end_date = validate_date(&end_date)?;

    if end_date < start_date {
        return Err(anyhow!("End date must be greater than or equal to start date").into());
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
