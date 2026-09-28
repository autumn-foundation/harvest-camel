//! End-to-end: camel `direct:` route → `harvest:` → harvest connector runtime
//! → a workflow execution row in Postgres.
//!
//! Needs a Postgres database in `HARVEST_CAMEL_TEST_DATABASE_URL` (CI provides
//! one as a service container). The harvest schema is applied on first use.
//! Without the variable every test here is skipped with a notice.
//!
//! No worker runs: a started workflow is a `harvest_workflow_executions` row,
//! which is all the dedupe guarantee is about.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use autumn_harvest::WorkflowContext;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime};
use autumn_harvest_plugin::connector::{
    ConnectorRuntime, EventSource, MappedMessage, PostgresDeadLetterSink, SourceBinding,
    resolve_idempotency_mode,
};
use camel_api::{BoxProcessor, CamelError, Exchange, Message, Value};
use camel_builder::{RouteBuilder, StepAccumulator};
use camel_component_api::{NoOpComponentContext, RuntimeObservability};
use camel_component_direct::DirectComponent;
use camel_core::CamelContext;
use diesel::sql_types::{BigInt, Bool, Text};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use harvest_camel::{CamelSource, HarvestBridge, check_route, is_unsettled};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

const DB_ENV: &str = "HARVEST_CAMEL_TEST_DATABASE_URL";
const CONNECTOR_DLQ_SQL: &str = include_str!("sql/harvest_connector_dead_letters.sql");

#[autumn_harvest::prelude::workflow]
async fn fulfil_order(_ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    Ok(input)
}

// ───────────────────────────── database ─────────────────────────────

fn database_url() -> Option<String> {
    let url = std::env::var(DB_ENV).ok();
    if url.is_none() {
        eprintln!("skipping: set {DB_ENV} to a Postgres URL to run the e2e tests");
    }
    url
}

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    n: i64,
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

/// Apply the harvest schema once per database, serialized across the
/// concurrently running tests by an advisory lock.
async fn migrate(url: &str) -> AsyncPgConnection {
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
    conn.batch_execute("SELECT pg_advisory_unlock(7_319_001)")
        .await
        .expect("advisory unlock");
    conn
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool")
}

async fn executions_with_prefix(conn: &mut AsyncPgConnection, prefix: &str) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_workflow_executions \
         WHERE workflow_name = 'fulfil_order' AND workflow_id LIKE $1",
    )
    .bind::<Text, _>(format!("{prefix}%"))
    .load::<Count>(conn)
    .await
    .expect("count")[0]
        .n
}

/// Dedupe claims outlive a test run in a shared database, so every run uses
/// fresh ids and topics.
fn nonce() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{nanos:x}")
}

// ───────────────────────────── harness ─────────────────────────────

fn started_api_state(pool: &DbPool) -> HarvestApiState {
    let state = HarvestApiState::new();
    state.set_admin_auth_boundary(true);
    state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![fulfil_order_info()], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("harvest-camel-e2e".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    state
}

/// `{"order_id": ..}` → workflow id `<prefix><order_id>-<n>`, where `n` is
/// fresh on every mapping call.
///
/// The drifting suffix matters: harvest also refuses a second *active* run of
/// one `workflow_id`, which would mask a broken coordinate dedupe. With a
/// fresh id per call, only the connector's coordinate-derived idempotency key
/// can collapse a redelivery into one execution.
fn binding(name: &'static str, stream: &str, prefix: String) -> SourceBinding {
    let seq = std::sync::atomic::AtomicU64::new(0);
    SourceBinding::starts(name, stream.to_string(), "fulfil_order").map_json(
        move |_ctx, body: Value| {
            let order_id = body["order_id"].as_str().ok_or("missing order_id")?;
            let n = seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok::<_, &str>(MappedMessage::new(format!("{prefix}{order_id}-{n}"), body))
        },
    )
}

/// Run the connector runtime for `source` in the background until dropped.
struct Harvest {
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl Harvest {
    fn start(
        binding: SourceBinding,
        source: Arc<CamelSource>,
        state: HarvestApiState,
        pool: &DbPool,
    ) -> Self {
        assert_eq!(
            binding.stream,
            source.stream(),
            "binding and bridge streams must match"
        );
        let mode = resolve_idempotency_mode(binding.target, binding.idempotency_mode, None);
        let runtime = ConnectorRuntime::new(
            Arc::new(binding),
            source as Arc<dyn EventSource>,
            state,
            Arc::new(NoOpMetrics),
            mode,
        )
        .with_dead_letter_sink(Arc::new(PostgresDeadLetterSink::new(pool.clone())));
        let cancel = CancellationToken::new();
        let task = tokio::spawn({
            let cancel = cancel.clone();
            async move { runtime.run(cancel).await }
        });
        Self { cancel, task }
    }

    async fn stop(self) {
        self.cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(10), self.task).await;
    }
}

/// A started camel context with `direct:in → harvest:<stream>?<options>`.
async fn camel_with_route(
    component: harvest_camel::HarvestComponent,
    harvest_uri: &str,
) -> (CamelContext, BoxProcessor) {
    let mut camel = CamelContext::builder()
        .build()
        .await
        .expect("camel context");
    camel.register_component(DirectComponent::new());
    camel.register_component(component);
    let route = RouteBuilder::from("direct:in")
        .route_id("to-harvest")
        .to(harvest_uri)
        .build()
        .expect("route");
    check_route(&route).expect("route error handler propagates harvest failures");
    camel.add_route_definition(route).await.expect("add route");
    camel.start().await.expect("start camel");

    let direct = camel.registry().get_or_err("direct").expect("direct");
    let endpoint = direct
        .create_endpoint("direct:in", &camel)
        .expect("endpoint");
    let rt: Arc<dyn RuntimeObservability> = Arc::new(NoOpComponentContext);
    let producer = endpoint
        .create_producer(rt, &camel.producer_context())
        .expect("producer");
    (camel, producer)
}

async fn send(producer: &BoxProcessor, message: Message) -> Result<Exchange, CamelError> {
    producer.clone().oneshot(Exchange::new(message)).await
}

// ───────────────────────────── tests ─────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redelivered_id_header_message_starts_exactly_one_workflow() {
    let Some(url) = database_url() else { return };
    let mut conn = migrate(&url).await;
    let pool = build_pool(&url);
    let run = nonce();
    let prefix = format!("e2e-id-{run}-");

    let (source, component) = HarvestBridge::new("orders");
    let harvest = Harvest::start(
        binding("camel-orders-id", "orders", prefix.clone()),
        Arc::clone(&source),
        started_api_state(&pool),
        &pool,
    );
    let (mut camel, producer) =
        camel_with_route(component, "harvest:orders?idHeader=OrderMessageId").await;

    // The same logical message delivered three times, as an un-committed
    // consumer crash would redeliver it.
    for _ in 0..3 {
        let mut msg = Message::new(json!({"order_id": "o-1"}).to_string());
        msg.set_header("OrderMessageId", format!("msg-{run}-1"));
        send(&producer, msg)
            .await
            .expect("every delivery is acked once harvest holds it");
    }
    // A different message id is a different message.
    let mut msg = Message::new(json!({"order_id": "o-2"}).to_string());
    msg.set_header("OrderMessageId", format!("msg-{run}-2"));
    send(&producer, msg).await.expect("second message acked");

    assert_eq!(
        executions_with_prefix(&mut conn, &format!("{prefix}o-1-")).await,
        1
    );
    assert_eq!(
        executions_with_prefix(&mut conn, &format!("{prefix}o-2-")).await,
        1
    );
    assert_eq!(source.in_flight(), 0);

    camel.stop().await.expect("stop camel");
    harvest.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_coordinates_dedupe_redelivery_of_the_same_offset() {
    let Some(url) = database_url() else { return };
    let mut conn = migrate(&url).await;
    let pool = build_pool(&url);
    let run = nonce();
    let prefix = format!("e2e-kafka-{run}-");
    let topic = format!("orders.raw.{run}");

    let (source, component) = HarvestBridge::new("orders");
    let harvest = Harvest::start(
        binding("camel-orders-kafka", "orders", prefix.clone()),
        Arc::clone(&source),
        started_api_state(&pool),
        &pool,
    );
    let (mut camel, producer) = camel_with_route(component, "harvest:orders").await;

    // Offsets 10, 10 (redelivery), 11 — as camel's Kafka consumer would set
    // the headers. Each offset's order id differs only to count them apart.
    for (offset, order) in [(10, "a"), (10, "a"), (11, "b")] {
        let mut msg = Message::new(json!({"order_id": order}).to_string());
        msg.set_header(harvest_camel::KAFKA_TOPIC, topic.clone());
        msg.set_header(harvest_camel::KAFKA_PARTITION, 0);
        msg.set_header(harvest_camel::KAFKA_OFFSET, offset);
        send(&producer, msg).await.expect("acked");
    }

    assert_eq!(
        executions_with_prefix(&mut conn, &format!("{prefix}a-")).await,
        1
    );
    assert_eq!(
        executions_with_prefix(&mut conn, &format!("{prefix}b-")).await,
        1
    );

    camel.stop().await.expect("stop camel");
    harvest.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn harvest_not_ready_abandons_and_camel_sees_err() {
    let Some(url) = database_url() else { return };
    let mut conn = migrate(&url).await;
    let pool = build_pool(&url);
    let run = nonce();
    let prefix = format!("e2e-abandon-{run}-");

    // An API state with no runtime installed answers "harvest runtime is not
    // started": a transient failure, which the connector abandons.
    let not_ready = HarvestApiState::new();
    not_ready.install_storage_pool(HarvestDbPool::from(pool.clone()));
    let (source, component) = HarvestBridge::new("orders");
    let harvest = Harvest::start(
        binding("camel-orders-abandon", "orders", prefix.clone()),
        Arc::clone(&source),
        not_ready,
        &pool,
    );
    let (mut camel, producer) = camel_with_route(component, "harvest:orders?idHeader=Id").await;

    let mut msg = Message::new(json!({"order_id": "o-1"}).to_string());
    msg.set_header("Id", format!("msg-{run}"));
    let err = send(&producer, msg)
        .await
        .expect_err("an abandoned message must fail the exchange so it is not committed");
    assert!(is_unsettled(&err), "{err}");
    assert_eq!(executions_with_prefix(&mut conn, &prefix).await, 0);

    camel.stop().await.expect("stop camel");
    harvest.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_id_never_reaches_harvest() {
    let Some(url) = database_url() else { return };
    let mut conn = migrate(&url).await;
    let pool = build_pool(&url);
    let prefix = format!("e2e-noid-{}-", nonce());

    let (source, component) = HarvestBridge::new("orders");
    let harvest = Harvest::start(
        binding("camel-orders-noid", "orders", prefix.clone()),
        Arc::clone(&source),
        started_api_state(&pool),
        &pool,
    );
    let (mut camel, producer) = camel_with_route(component, "harvest:orders").await;

    let err = send(
        &producer,
        Message::new(json!({"order_id": "o-1"}).to_string()),
    )
    .await
    .expect_err("no Kafka headers and no idHeader");
    assert!(matches!(err, CamelError::ValidationError(_)), "{err}");
    assert_eq!(executions_with_prefix(&mut conn, &prefix).await, 0);

    camel.stop().await.expect("stop camel");
    harvest.stop().await;
}
