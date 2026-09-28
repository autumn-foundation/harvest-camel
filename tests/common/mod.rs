//! Shared Postgres and camel helpers for the e2e tests.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use autumn_harvest::worker::DbPool;
use camel_api::BoxProcessor;
use camel_component_api::{NoOpComponentContext, RuntimeObservability};
use camel_core::CamelContext;
use diesel::sql_types::{Bool, Text};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};

pub const DB_ENV: &str = "HARVEST_CAMEL_TEST_DATABASE_URL";
const CONNECTOR_DLQ_SQL: &str = include_str!("../sql/harvest_connector_dead_letters.sql");

pub fn database_url() -> Option<String> {
    let url = std::env::var(DB_ENV).ok();
    if url.is_none() {
        eprintln!("skipping: set {DB_ENV} to a Postgres URL to run the e2e tests");
    }
    url
}

#[derive(diesel::QueryableByName)]
struct Exists {
    #[diesel(sql_type = Bool)]
    present: bool,
}

async fn table_exists(conn: &mut AsyncPgConnection, table: &str) -> bool {
    diesel::sql_query("SELECT to_regclass($1) IS NOT NULL AS present")
        .bind::<Text, _>(table)
        .load::<Exists>(conn)
        .await
        .expect("to_regclass")[0]
        .present
}

/// Apply the harvest schema and this crate's claim table once per database,
/// serialized across concurrently running tests by an advisory lock.
pub async fn migrate(url: &str) -> AsyncPgConnection {
    let mut conn = AsyncPgConnection::establish(url)
        .await
        .expect("connect to test database");
    conn.batch_execute("SELECT pg_advisory_lock(7_319_001)")
        .await
        .expect("advisory lock");
    if !table_exists(&mut conn, "harvest_workflow_executions").await {
        conn.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("apply harvest migrations");
    }
    if !table_exists(&mut conn, "harvest_connector_dead_letters").await {
        conn.batch_execute(CONNECTOR_DLQ_SQL)
            .await
            .expect("apply connector dead-letter migration");
    }
    harvest_camel::external::migrate(&mut conn)
        .await
        .expect("apply harvest-camel claim table");
    conn.batch_execute("SELECT pg_advisory_unlock(7_319_001)")
        .await
        .expect("advisory unlock");
    conn
}

pub fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool")
}

/// Dedupe claims and workflow ids outlive a test run in a shared database,
/// so every run uses fresh ids and topics.
pub fn nonce() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{nanos:x}")
}

/// A producer for `uri` on a started `camel` context.
pub fn producer(camel: &CamelContext, uri: &str) -> BoxProcessor {
    let scheme = uri.split_once(':').expect("scheme").0;
    let component = camel.registry().get_or_err(scheme).expect("component");
    let endpoint = component.create_endpoint(uri, camel).expect("endpoint");
    let rt: Arc<dyn RuntimeObservability> = Arc::new(NoOpComponentContext);
    endpoint
        .create_producer(rt, &camel.producer_context())
        .expect("producer")
}
