use anyhow::{anyhow, Result};
use clickhouse_rs::{Client as ClickhouseClient, Pool as ClickhousePool};
use futures::stream::{self, StreamExt};
use log::{error, info};
use mongodb::{
    bson::{self, Bson, Document},
    options::FindOptions,
    Client as MongoClient,
};
use rayon::prelude::*;
use rayon::prelude::*;
use serde::Deserialize;
use serde_json::to_string;
use sha2::{Digest, Sha256};
use std::{env, error::Error, sync::Arc};
use tokio::sync::Semaphore;

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

#[derive(Deserialize)]
struct AppConfig {
    tenants: Vec<TenantConfig>,
    encryption_salt: String,
    batch_size: u64,
    max_concurrency: usize,
}

type ClickhousePoolType = ClickhousePool;

struct AppState {
    config: AppConfig,
    clickhouse_pools: Vec<ClickhousePoolType>,
}

async fn run(app_state: Arc<AppState>) -> Result<(), Box<dyn Error>> {
    let tenants = app_state.config.tenants.clone();

    let futures = tenants.into_iter().enumerate().map(|(index, tenant)| {
        let app_state = app_state.clone();
        let tenant_cloned = tenant.clone();
        tokio::spawn(async move {
            if let Err(e) = process_tenant_historical_data(tenant, app_state, index).await {
                error!("Error processing tenant {}: {}", tenant_cloned.name, e);
            }
        })
    });

    futures::future::join_all(futures).await;

    Ok(())
}

async fn process_tenant_historical_data(
    tenant_config: TenantConfig,
    app_state: Arc<AppState>,
    pool_index: usize,
) -> Result<()> {
    let mongo_client = MongoClient::with_uri_str(&tenant_config.mongo_uri).await?;
    let mongo_db = mongo_client.database(&tenant_config.mongo_db);
    let mongo_collection = mongo_db.collection::<Document>(&tenant_config.mongo_collection);

    let ch_pool = &app_state.clickhouse_pools[pool_index];

    let total_docs = mongo_collection.estimated_document_count(None).await?;
    info!(
        "Total documents in {}: {}",
        tenant_config.mongo_collection, total_docs
    );

    let batch_size = app_state.config.batch_size;
    let num_batches = (total_docs as f64 / batch_size as f64).ceil() as u64;

    for batch_index in 0..num_batches {
        let skip = batch_index * batch_size;
        let options = FindOptions::builder()
            .skip(skip)
            .limit(batch_size as i64)
            .build();

        match mongo_collection.find(None, options).await {
            Ok(mut cursor) => {
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

                            let ch_pool = ch_pool.clone();
                            let tenant_config = tenant_config.clone();
                            tokio::spawn(async move {
                                if let Err(e) = insert_into_clickhouse(
                                    &ch_pool,
                                    &[(record_id_str, statement_str)],
                                    &tenant_config.clickhouse_db,
                                    &tenant_config.clickhouse_table,
                                )
                                .await
                                {
                                    error!("Error inserting into ClickHouse: {}", e);
                                }
                            });
                        }
                    }
                }
            }
            Err(e) => {
                error!("Error retrieving documents from MongoDB: {}", e);
            }
        }
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
) -> Result<()> {
    let full_table_name = format!("{}.{}", clickhouse_db, clickhouse_table);
    let mut client = match ch_pool.get_handle().await {
        Ok(client) => client,
        Err(e) => {
            error!("Failed to get client from ClickHouse pool: {}", e);
            return Err(e.into());
        }
    };

    let max_retries = 3;
    let mut retry_count = 0;

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

        match client.execute(insert_query.as_str()).await {
            Ok(_) => {
                info!("Successfully inserted statements into ClickHouse");
                return Ok(());
            }
            Err(e) => {
                error!("Failed to insert statements into ClickHouse: {}", e);
                retry_count += 1;
                if retry_count == max_retries {
                    error!("Max retries reached. Giving up.");
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

    let mut clickhouse_pools = Vec::new();
    for tenant in &config.tenants {
        let clickhouse_pool = ClickhousePool::new(&*tenant.clickhouse_uri);
        clickhouse_pools.push(clickhouse_pool);
    }

    let app_state = Arc::new(AppState {
        config,
        clickhouse_pools,
    });

    run(app_state).await?;

    Ok(())
}
