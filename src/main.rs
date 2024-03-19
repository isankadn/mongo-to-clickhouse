use anyhow::Result;
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use clickhouse_rs::Pool as ClickhousePool;
use config::{Config, File};
use futures::{future::join_all, stream::StreamExt};
use log::{error, info, warn};
use mongodb::{
    bson::{self, Bson, Document},
    change_stream::event::{ChangeStreamEvent, ResumeToken},
    options::ChangeStreamOptions,
    Client as MongoClient,
};
use serde::Deserialize;
use serde_json::{to_string, Value};
use sha2::{Digest, Sha256};
use std::{env, error::Error, sync::Arc};
use tokio::task;
use tokio_postgres::{Client as PostgresClient, NoTls};

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
    postgres_db: String,
    pg_database_url: String,
    encryption_salt: String,
}

type PostgresPool = Pool<PostgresConnectionManager<NoTls>>;
type ClickhousePoolType = ClickhousePool;

struct AppState {
    config: AppConfig,
    postgres_pool: PostgresPool,
    clickhouse_pools: Vec<ClickhousePoolType>,
}

async fn run(app_state: Arc<AppState>) -> Result<(), Box<dyn Error>> {
    let tenants = app_state.config.tenants.clone();

    let mut tasks = Vec::new();
    for (index, tenant) in tenants.iter().enumerate() {
        let tenant_config = tenant.clone();
        let tenant_config_cloned = tenant_config.clone();
        let app_state = app_state.clone();
        let task = task::spawn(async move {
            if let Err(e) = process_tenant_records(tenant_config, app_state, index).await {
                log::error!(
                    "Error processing tenant {}: {}",
                    tenant_config_cloned.name,
                    e
                );
            }
        });
        tasks.push(task);
    }

    join_all(tasks).await;
    Ok(())
}

async fn process_tenant_records(
    tenant_config: TenantConfig,
    app_state: Arc<AppState>,
    pool_index: usize,
) -> Result<()> {
    let mongo_client = MongoClient::with_uri_str(&tenant_config.mongo_uri).await?;
    println!("<<-- mongo_uri {:?}", &tenant_config.mongo_uri);
    // println!("<<-- mongo_client {:?}", mongo_client);
    let mongo_db = mongo_client.database(&tenant_config.mongo_db);
    println!("<<-- mongo_db: {:?}", mongo_db);
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
    let mut options = mongodb::options::ChangeStreamOptions::default();
    if let Some(row) = row {
        let token_bytes: Vec<u8> = row.get("token");
        if let Ok(resume_token) = bson::from_slice::<ResumeToken>(&token_bytes) {
            options.resume_after = Some(resume_token);
        }
    }

    let change_stream_options = ChangeStreamOptions::default();
    let mut change_stream = mongo_collection.watch(None, change_stream_options).await?;

    while let Some(result) = change_stream.next().await {
        println!(">>--- Change event: {:?}", result);
        match result {
            Ok(change_event) => {
                if let ChangeStreamEvent {
                    full_document: Some(doc),
                    ..
                } = change_event
                {
                    let record_id = doc.get("_id").and_then(|id| id.as_object_id());
                    let statement = doc.get("statement").and_then(|s| s.as_document());

                    // Extract additional fields as needed
                    // let additional_field1 = doc.get("additional_field1").and_then(|f| f.as_str());
                    // let additional_field2 = doc.get("additional_field2").and_then(|f| f.as_i32());

                    match (record_id, statement) {
                        (Some(record_id), Some(statement)) => {
                            let record_id_str = record_id.to_hex();
                            info!("Record ID: {}", record_id_str);

                            let mut statement = statement.to_owned();
                            if let Some(actor) =
                                statement.get_mut("actor").and_then(|a| a.as_document_mut())
                            {
                                if let Some(account) = actor
                                    .get_mut("account")
                                    .and_then(|acc| acc.as_document_mut())
                                {
                                    if let Some(name) = account.get_mut("name") {
                                        let anonymized_name =
                                            anonymize_data(name, &app_state.config.encryption_salt, &tenant_config.name);
                                        *name = Bson::String(anonymized_name);
                                        info!("<<-- Modified statement: {:?}", statement);
                                    } else {
                                        warn!("Missing 'name' field in 'actor.account'");
                                    }
                                } else {
                                    warn!("Missing 'account' field in 'actor'");
                                }
                            } else {
                                warn!("Missing 'actor' field in 'statement'");
                            }

                            let statement_str = match to_string(&statement) {
                                Ok(s) => s,
                                Err(e) => {
                                    error!("Failed to convert statement to string: {}", e);
                                    continue;
                                }
                            };
                            info!("Inserting statement into ClickHouse: {}", statement_str);

                            insert_into_clickhouse(
                                &ch_pool,
                                &statement_str,
                                &record_id_str,
                                &tenant_config.clickhouse_db,
                                &tenant_config.clickhouse_table,
                            )
                            .await;

                            // println!(">>-- Statement: {}", statement_str);
                        }
                        (None, Some(_)) => {
                            warn!("Missing '_id' field in the document");
                            // Handle the missing '_id' field, e.g., generate a unique identifier or skip the document
                        }
                        (Some(_), None) => {
                            warn!("Missing 'statement' field in the document");
                            // Handle the missing 'statement' field, e.g., skip the document or use default values
                        }
                        (None, None) => {
                            warn!("Missing both '_id' and 'statement' fields in the document");
                            // Handle the case when both fields are missing
                        }
                    }

                    if let Some(resume_token) = change_stream.resume_token() {
                        let token_bytes = match bson::to_vec(&resume_token) {
                            Ok(bytes) => bytes,
                            Err(e) => {
                                error!("Failed to serialize resume token: {}", e);
                                continue;
                            }
                        };
                        let tenant_name = tenant_config.name.clone();

                        let pg_pool = app_state.postgres_pool.clone();
                        match pg_pool.get().await {
                            Ok(pg_conn) => {
                                if let Err(e) = pg_conn
                                    .execute(
                                        "INSERT INTO resume_token (token, tenant_name) VALUES ($1, $2) ON CONFLICT (token) DO UPDATE SET token = EXCLUDED.token",
                                        &[&token_bytes, &tenant_name],
                                    )
                                    .await
                                {
                                    error!("Failed to update resume token in PostgreSQL: {}", e);
                                    // Handle the error: retry or log the failure
                                }
                            }
                            Err(e) => {
                                error!("Failed to get PostgreSQL connection: {}", e);
                                // Handle the error: retry or log the failure
                            }
                        };
                    }
                } else {
                    warn!("Missing 'full_document' field in the change stream event");
                    // Handle the missing 'full_document' field: skip the event or use default values
                }
            }
            Err(e) => {
                error!("Change stream error: {}", e);
                // Handle change stream errors: implement retry logic or log the error
            }
        }
    }

    Ok(())
}

fn anonymize_data(data: &Bson, encryption_salt: &str, tenant_name: &str) -> String {
    let mut hasher = Sha256::new();
    // let salt = &settings.encryption_salt;
    let salt = format!("{}{}", encryption_salt, tenant_name);
    hasher.update(salt.as_bytes());

    let data_str = match data {
        Bson::String(s) => s.as_str(),
        Bson::Int32(i) => return i.to_string(),
        Bson::Int64(i) => return i.to_string(),
        _ => return String::new(),
    };

    hasher.update(data_str.as_bytes());
    let result = hasher.finalize();
    hex::encode(result)
}

async fn insert_into_clickhouse(
    ch_pool: &ClickhousePool,
    statement_str: &str,
    record_id_str: &str,
    clickhouse_db: &str,
    clickhouse_table: &str,
) {
    let full_table_name = format!("{}.{}", clickhouse_db, clickhouse_table);

    let escaped_statement_str = statement_str.replace("'", "\\\'");

    let insert_query = format!(
        "INSERT INTO {} (id, statement) VALUES ('{}', '{}')",
        full_table_name, record_id_str, escaped_statement_str
    );

    let mut client = match ch_pool.get_handle().await {
        Ok(client) => client,
        Err(e) => {
            error!("Failed to get client from ClickHouse pool: {}", e);
            return;
        }
    };

    let max_retries = 3;
    let mut retry_count = 0;

    while retry_count < max_retries {
        match client.execute(insert_query.as_str()).await {
            Ok(_) => {
                info!("Successfully inserted statement into ClickHouse");
                return;
            }
            Err(e) => {
                error!("Failed to insert statement into ClickHouse: {}", e);
                retry_count += 1;

                if retry_count == max_retries {
                    error!("Max retries reached. Giving up.");
                    return;
                } else {
                    let delay_ms = 1000 * retry_count; 
                    info!("Retrying in {} ms...", delay_ms);
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }
            }
        }
    }
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
            log::error!("Unsupported environment: {}", env);
            return Err("Unsupported environment".into());
        }
    };

    let config_file = File::with_name(config_path);
    let config: AppConfig = match Config::builder().add_source(config_file).build() {
        Ok(config_builder) => match config_builder.try_deserialize() {
            Ok(config) => config,
            Err(err) => {
                log::error!("Failed to deserialize config: {}", err);
                return Err(err.into());
            }
        },
        Err(err) => {
            log::error!("Failed to build config: {}", err);
            return Err(err.into());
        }
    };

    let postgres_manager = PostgresConnectionManager::new(config.pg_database_url.parse()?, NoTls);
    let postgres_pool = Pool::builder().build(postgres_manager).await?;

    let mut clickhouse_pools = Vec::new();
    for tenant in &config.tenants {
        let clickhouse_pool = ClickhousePool::new(&*tenant.clickhouse_uri);
        clickhouse_pools.push(clickhouse_pool);
    }

    let app_state = Arc::new(AppState {
        config,
        postgres_pool,
        clickhouse_pools,
    });

    let _ = run(app_state).await;
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl-c signal");

    Ok(())
}
