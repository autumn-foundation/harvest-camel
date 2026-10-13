//! An order flow: camel connects to the external systems, and harvest runs
//! the workflow.
//!
//! ```text
//! direct:orders ─► harvest:orders ─► workflow process_order
//!   1. activity reserve_stock ─► direct:inventory              (outbound)
//!   2. external charge_card   ─► harvest-external:charge_card  (request/reply)
//!        ─► direct:payment-gateway ─► harvest-complete:charge_card
//! ```
//!
//! The `direct:` routes replace real connectors (Kafka, HTTP, JMS).
//! The example sends the same order two times, the same as a broker after a
//! crash. Harvest starts one workflow, so the inventory route runs one time.
//! The example checks its result and exits with an error if it is wrong.
//!
//! ```sh
//! DATABASE_URL=postgres://postgres:postgres@localhost:5432/harvest_camel_test \
//!   cargo run --example order_flow
//! ```

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use autumn_harvest::execution::resolve_execution_id_by_workflow_id;
use autumn_harvest::failure::ActivityFailure;
use autumn_harvest::handle::WorkflowHandleClient;
use autumn_harvest::prelude::{ActivityContext, WorkflowContext, activity, workflow};
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::worker::DbPool;
use autumn_harvest_plugin::api::HarvestApiState;
use autumn_harvest_plugin::connector::{ConnectorRuntime, MappedMessage, SourceBinding};
use autumn_harvest_plugin::{
    HarvestDatabaseConfig, HarvestMode, HarvestRunner, HarvestRunnerResources, HarvestRuntimeConfig,
};
use camel_api::{Body, Exchange};
use camel_builder::{RouteBuilder, StepAccumulator};
use camel_component_direct::DirectComponent;
use camel_core::CamelContext;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, SimpleAsyncConnection};
use harvest_camel::{
    CamelHandle, ExternalTasks, HarvestBridge, call_route, call_route_from_activity, check_route,
};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const ORDERS: &str = "direct:orders";
const INVENTORY: &str = "direct:inventory";
/// The header with the message id. The id does not change when the broker
/// sends the message again. Harvest uses the id to find duplicates.
const MESSAGE_ID: &str = "MessageId";
/// The maximum time for one run. CI must not hang.
const RUN_TIMEOUT: Duration = Duration::from_secs(120);

// ─────────────────────────── harvest: workflow and activity ───────────────────────────

#[workflow]
async fn process_order(ctx: &WorkflowContext, order: Value) -> Result<Value, String> {
    let reservation: Value = ctx
        .execute_activity(&reserve_stock_info(), order.clone())
        .await
        .map_err(|e| e.to_string())?;
    // The workflow waits until camel completes the token. A crash does not
    // stop the wait.
    let payment = ctx
        .execute_activity_external("charge_card", order.clone(), "default", 300)
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!({
        "order_id": order["order_id"],
        "reservation": reservation,
        "payment": payment,
    }))
}

/// Harvest replays workflow code. Thus, only activities call camel.
#[activity(start_to_close = "30s")]
async fn reserve_stock(ctx: &ActivityContext, order: Value) -> Result<Value, ActivityFailure> {
    call_route_from_activity(ctx, INVENTORY, order).await
}

// ─────────────────────────────────── camel: routes ───────────────────────────────────

/// Makes the routes. `reserved` gets the order id of each inventory call.
fn routes(reserved: Arc<Mutex<Vec<Value>>>) -> Result<Vec<camel_core::RouteDefinition>, BoxError> {
    // The inbound route. In production, use `from("kafka:…")` and set
    // `CamelContext::builder().supervision(..)`.
    let inbound = RouteBuilder::from(ORDERS)
        .route_id("orders-in")
        .to(format!("harvest:orders?idHeader={MESSAGE_ID}"))
        .build()?;

    // The outbound route. In production, use an `http:` producer. Do not
    // add `.retry(n)`: harvest retries the activity. The service can use the
    // `HarvestIdempotencyKey` header to find duplicates.
    let inventory = RouteBuilder::from(INVENTORY)
        .route_id("inventory")
        .process(move |mut ex: Exchange| {
            let order = json_body(&ex);
            // Only for the check at the end of `run`.
            reserved
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(order["order_id"].clone());
            ex.input.body = Body::Json(json!({
                "sku": order["sku"],
                "reserved": order["quantity"],
            }));
            async move { Ok(ex) }
        })
        .build()?;

    // The request route. In production, use `.to("jms:queue:charge-requests")`.
    // `pollInterval=100` makes the example fast. The default is 1000 ms.
    let charge_requests = RouteBuilder::from("harvest-external:charge_card?pollInterval=100")
        .route_id("charge-requests")
        .to("direct:payment-gateway")
        .build()?;
    // The reply route. In production, use `from("jms:queue:charge-replies")`.
    // The reply keeps the `HarvestExternalToken` header of the request.
    let charge_replies = RouteBuilder::from("direct:payment-gateway")
        .route_id("charge-replies")
        .process(|mut ex: Exchange| {
            let order = json_body(&ex);
            ex.input.body = Body::Json(json!({
                "approved": true,
                "amount": order["amount"],
            }));
            async move { Ok(ex) }
        })
        .to("harvest-complete:charge_card")
        .build()?;

    Ok(vec![inbound, inventory, charge_requests, charge_replies])
}

/// The JSON body. `direct:` keeps JSON. A real connector can send text or
/// bytes, which this function changes to `null`.
fn json_body(ex: &Exchange) -> Value {
    match &ex.input.body {
        Body::Json(v) => v.clone(),
        _ => Value::Null,
    }
}

// ─────────────────────────────────── wiring ───────────────────────────────────

/// Runs the flow against the Postgres database at `url` and checks the
/// result.
async fn run(url: &str) -> Result<Value, BoxError> {
    tokio::time::timeout(RUN_TIMEOUT, run_unbounded(url))
        .await
        .map_err(|_| format!("the order flow did not stop in {RUN_TIMEOUT:?}"))?
}

async fn run_unbounded(url: &str) -> Result<Value, BoxError> {
    migrate(url).await?;
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    let pool: DbPool = deadpool::managed::Pool::builder(manager).build()?;

    // camel: register the components, then add and start the routes.
    let (source, harvest_component) = HarvestBridge::new("orders");
    let reserved = Arc::new(Mutex::new(Vec::new()));
    let mut camel = CamelContext::builder().build().await?;
    camel.register_component(DirectComponent::new());
    camel.register_component(harvest_component);
    ExternalTasks::new(pool.clone()).register(&mut camel);
    for route in routes(Arc::clone(&reserved))? {
        // Refuse error handlers that change an abandoned message into a
        // committed message.
        check_route(&route)?;
        camel.add_route_definition(route).await?;
    }
    camel.start().await?;

    // Activities get this handle. Resolve their endpoints before they run.
    let handle = CamelHandle::new();
    handle.resolve(&camel, INVENTORY)?;

    // harvest: a worker runs the workflow and the activity. In production,
    // `HarvestPlugin` starts the worker and the connector.
    let runner = HarvestRunner::start(
        autumn_harvest::HarvestBuilder::new()
            .workflows(vec![process_order_info()])
            .activities(vec![reserve_stock_info()])
            .state(handle)
            .build(),
        &HarvestRuntimeConfig {
            mode: HarvestMode::External,
            database: HarvestDatabaseConfig {
                url: Some(url.to_string()),
            },
            ..HarvestRuntimeConfig::default()
        },
        HarvestRunnerResources::new(pool.clone()),
    )
    .await
    .map_err(|e| e.to_string())?;

    // harvest: the connector reads stream "orders" and starts one workflow
    // for each new message. `HarvestPlugin` also adds a dead-letter sink.
    // Without it, a bad message is abandoned and the broker sends it again.
    let api = HarvestApiState::new();
    api.install_storage_pool(runner.storage_pool());
    api.install(runner.api_runtime());
    let binding = SourceBinding::starts("camel-orders", "orders", "process_order").map_json(
        |_ctx, order: Value| {
            let id = order["order_id"].as_str().ok_or("missing order_id")?;
            Ok::<_, &str>(MappedMessage::new(id.to_string(), order))
        },
    );
    let connector = ConnectorRuntime::for_binding(
        Arc::new(binding),
        source,
        api,
        Arc::new(NoOpMetrics),
        Some(&process_order_info()),
    );
    let cancel = CancellationToken::new();
    let connector = tokio::spawn({
        let cancel = cancel.clone();
        async move { connector.run(cancel).await }
    });

    let result = send_order_and_wait(&camel, &pool, url).await;

    // Stop in all cases: the connector, then the worker, then camel.
    cancel.cancel();
    connector.await?;
    runner.stop().await;
    camel.stop().await?;

    let (order_id, result) = result?;
    let calls = reserved
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter(|id| **id == order_id)
        .count();
    check(&order_id, &result, calls)?;
    Ok(result)
}

/// Sends one order two times, as a broker does. Returns the order id and the
/// workflow result.
async fn send_order_and_wait(
    camel: &CamelContext,
    pool: &DbPool,
    url: &str,
) -> Result<(String, Value), BoxError> {
    // This example is also the broker. Activities do not get this handle.
    let broker = CamelHandle::new();
    broker.resolve(camel, ORDERS)?;

    // Use a new order id for each run. If not, harvest finds the workflow of
    // an earlier run.
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let order_id = format!("order-{nanos:x}");
    let order = json!({"order_id": order_id, "sku": "sku-1", "quantity": 2, "amount": 42});
    // `call_route` returns after harvest accepts the message.
    for _ in 0..2 {
        let headers = [(MESSAGE_ID.to_string(), json!(format!("msg-{order_id}")))];
        call_route(&broker, ORDERS, order.clone(), headers).await?;
    }

    let mut conn = pool.get().await?;
    let execution = resolve_execution_id_by_workflow_id(&mut conn, "process_order", &order_id)
        .await?
        .ok_or("the workflow did not start")?;
    drop(conn);
    let result = WorkflowHandleClient::single(pool.clone(), url)
        .handle(execution.exec_id)
        .result_raw_with_timeout(Duration::from_secs(60))
        .await
        .map_err(|e| format!("workflow {order_id}: {e}"))?;
    Ok((order_id, result))
}

/// Makes sure that the result is correct and that the inventory route ran
/// one time.
fn check(order_id: &str, result: &Value, inventory_calls: usize) -> Result<(), String> {
    let expected = json!({
        "order_id": order_id,
        "reservation": {"sku": "sku-1", "reserved": 2},
        "payment": {"approved": true, "amount": 42},
    });
    if *result != expected {
        return Err(format!("result {result} is not {expected}"));
    }
    if inventory_calls != 1 {
        return Err(format!(
            "the example sent the order two times, but the inventory route ran {inventory_calls} times"
        ));
    }
    Ok(())
}

#[derive(diesel::QueryableByName)]
struct Present {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    present: bool,
}

/// Applies the harvest schema if it is missing. Then it creates the claim
/// table. In production, apply the harvest migrations before you start.
async fn migrate(url: &str) -> Result<(), BoxError> {
    use diesel_async::RunQueryDsl;

    let mut conn = AsyncPgConnection::establish(url).await?;
    // Only one run at a time can apply migrations.
    conn.batch_execute("SELECT pg_advisory_lock(7319001)")
        .await?;
    let schema = diesel::sql_query(
        "SELECT to_regclass('harvest_workflow_executions') IS NOT NULL AS present",
    )
    .get_result::<Present>(&mut conn)
    .await?;
    if !schema.present {
        conn.batch_execute(autumn_harvest::full_migrations_sql())
            .await?;
    }
    harvest_camel::external::migrate(&mut conn).await?;
    conn.batch_execute("SELECT pg_advisory_unlock(7319001)")
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let url = std::env::var("DATABASE_URL")
        .ok()
        .filter(|url| !url.is_empty())
        .ok_or("set DATABASE_URL to a Postgres URL")?;
    let result = run(&url).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const DB_ENV: &str = "HARVEST_CAMEL_TEST_DATABASE_URL";

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn order_is_reserved_one_time_and_charged() {
        let Ok(url) = std::env::var(DB_ENV) else {
            eprintln!("skipping: set {DB_ENV} to a Postgres URL to run the example");
            return;
        };
        run(&url)
            .await
            .expect("the order flow completes and checks");
    }
}
