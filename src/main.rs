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
use regex::Regex;
use serde::Deserialize;
use serde_json::{to_string, Value};
use sha2::{Digest, Sha256};
use std::{env, error::Error, sync::Arc, time::Duration};
use tokio::task;
use tokio_postgres::{Client as PostgresClient, NoTls};

lazy_static::lazy_static! {
    static ref BACKSLASH_REGEX_1: Regex = match Regex::new(r"\\{2}") {
        Ok(regex) => regex,
        Err(e) => {
            error!("Failed to compile BACKSLASH_REGEX_1: {}", e);
            panic!("Invalid regular expression pattern");
        }
    };
    static ref BACKSLASH_REGEX_2: Regex = match Regex::new(r"\\(?:\\\\)*") {
        Ok(regex) => regex,
        Err(e) => {
            error!("Failed to compile BACKSLASH_REGEX_2: {}", e);
            panic!("Invalid regular expression pattern");
        }
    };
    static ref BACKSLASH_REGEX_3: Regex = match Regex::new(r"\\{4,}") {
        Ok(regex) => regex,
        Err(e) => {
            error!("Failed to compile BACKSLASH_REGEX_3: {}", e);
            panic!("Invalid regular expression pattern");
        }
    };
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
}

#[derive(Deserialize, Debug)]
struct AppConfig {
    tenants: Vec<TenantConfig>,
    postgres_db: String,
    pg_database_url: String,
    encryption_salt: String,
}

type PostgresPool = Pool<PostgresConnectionManager<NoTls>>;
type ClickhousePoolType = ClickhousePool;

#[derive(Debug)]
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

async fn connect_to_mongo(mongo_uri: &str) -> Result<MongoClient, mongodb::error::Error> {
    let mut retry_delay = Duration::from_secs(1);
    loop {
        match MongoClient::with_uri_str(mongo_uri).await {
            Ok(client) => return Ok(client),
            Err(e) => {
                error!("Failed to connect to MongoDB: {}", e);
                tokio::time::sleep(retry_delay).await;
                retry_delay *= 2;
                if retry_delay > Duration::from_secs(60) {
                    return Err(e);
                }
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
    //let mongo_client = MongoClient::with_uri_str(&tenant_config.mongo_uri).await?;
    // println!("<<-- mongo_uri {:?}", &tenant_config.mongo_uri);
    // println!("<<-- mongo_client {:?}", mongo_client);
    let mongo_db = mongo_client.database(&tenant_config.mongo_db);
    // println!("<<-- mongo_db: {:?}", mongo_db);
    let mongo_collection: mongodb::Collection<Document> =
        mongo_db.collection(&tenant_config.mongo_collection);

    let pg_pool = &app_state.postgres_pool;
    let ch_pool = &app_state.clickhouse_pools[pool_index];
    // println!("pg_pool {:?}", pg_pool);
    let pg_conn = pg_pool.get().await?;
    // println!("pg_conn {:?}", pg_conn);
    let row = pg_conn
        .query_one(
            "SELECT token FROM resume_token WHERE tenant_name = $1 ORDER BY id DESC LIMIT 1",
            &[&tenant_config.name],
        )
        .await
        .ok();
    // println!("row {:?}", row);
    let mut options = mongodb::options::ChangeStreamOptions::default();
    if let Some(row) = row {
        let token_bytes: Vec<u8> = row.get("token");
        if let Ok(resume_token) = bson::from_slice::<ResumeToken>(&token_bytes) {
            options.resume_after = Some(resume_token);
        }
    }
    // println!("app_state {:?}", &app_state.config);
    let change_stream_options = ChangeStreamOptions::default();
    let mut change_stream = mongo_collection.watch(None, change_stream_options).await?;

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
                                        if let bson::Bson::String(name_str) = name {
                                            let anonymized_name = if name_str.contains(':') {
                                                let parts: Vec<&str> =
                                                    name_str.split(':').collect();
                                                info!(
                                                    "{}",
                                                    &bson::Bson::String(parts[1].to_string())
                                                );
                                                if parts.len() == 2 {
                                                    anonymize_data(
                                                        &bson::Bson::String(parts[1].to_string()),
                                                        &app_state.config.encryption_salt,
                                                        &tenant_config.name,
                                                    )
                                                } else {
                                                    anonymize_data(
                                                        &bson::Bson::String(name_str.to_string()),
                                                        &app_state.config.encryption_salt,
                                                        &tenant_config.name,
                                                    )
                                                }
                                            } else if name_str.contains('@') {
                                                let parts: Vec<&str> =
                                                    name_str.split('@').collect();
                                                info!(
                                                    "{}",
                                                    &bson::Bson::String(parts[0].to_string())
                                                );
                                                if parts.len() == 2 {
                                                    anonymize_data(
                                                        &bson::Bson::String(parts[0].to_string()),
                                                        &app_state.config.encryption_salt,
                                                        &tenant_config.name,
                                                    )
                                                } else {
                                                    anonymize_data(
                                                        &bson::Bson::String(name_str.to_string()),
                                                        &app_state.config.encryption_salt,
                                                        &tenant_config.name,
                                                    )
                                                }
                                            } else {
                                                info!(
                                                    "{}",
                                                    &bson::Bson::String(name_str.to_string())
                                                );
                                                anonymize_data(
                                                    &bson::Bson::String(name_str.to_string()),
                                                    &app_state.config.encryption_salt,
                                                    &tenant_config.name,
                                                )
                                            };
                                            *name = bson::Bson::String(anonymized_name);
                                        } else {
                                            warn!("Missing 'name' field in 'actor.account'");
                                        }
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

                            insert_into_clickhouse(
                                &ch_pool,
                                &statement_str,
                                &record_id_str,
                                &tenant_config.clickhouse_db,
                                &tenant_config.clickhouse_table,
                            )
                            .await;
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
                                }
                            }
                            Err(e) => {
                                error!("Failed to get PostgreSQL connection: {}", e);
                            }
                        };
                    }
                } else {
                    warn!("Missing 'full_document' field in the change stream event");
                }
            }
            Err(e) => {
                error!("Change stream error: {}", e);
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

async fn process_statement(statement: &str) -> Result<String, Box<dyn Error + Send + Sync>> {
    // Replace all double consecutive backslashes with four backslashes
    let output1 = BACKSLASH_REGEX_1
        .replace_all(statement, "\\\\\\\\")
        .to_string();

    // Replace all single backslashes with two backslashes
    let output2 = BACKSLASH_REGEX_2.replace_all(&output1, |caps: &regex::Captures| {
        if caps[0].len() % 2 == 1 {
            "\\\\".to_string()
        } else {
            caps[0].to_string()
        }
    });

    // Replace all more than four backslashes with four backslashes
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
    statement_str: &str,
    record_id_str: &str,
    clickhouse_db: &str,
    clickhouse_table: &str,
) {
    let full_table_name = format!("{}.{}", clickhouse_db, clickhouse_table);

    // let escaped_statement_str = statement_str.replace("'", "\\\'");
    let escaped_statement_str = match process_statement(statement_str).await {
        Ok(escaped_str) => escaped_str,
        Err(e) => {
            error!("Failed to process statement: {}", e);
            return;
        }
    };

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
    // println!("app_state_main {:?}", app_state);
    let _ = run(app_state).await;
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl-c signal");

    Ok(())
}
