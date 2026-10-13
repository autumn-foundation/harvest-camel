//! Runnable order flow: camel handles the edges, harvest handles the middle.
//!
//! ```text
//! direct:orders ─► harvest:orders ─► workflow process_order
//!   1. activity reserve_stock ─► direct:inventory              (outbound)
//!   2. external charge_card   ─► harvest-external:charge_card  (request/reply)
//!        ─► direct:payment-gateway ─► harvest-complete:charge_card
//! ```
//!
//! The `direct:` routes replace real connectors (Kafka, HTTP, JMS).
//! The example sends the same order two times, as a broker does after a
//! crash. Harvest starts one workflow, so stock is reserved one time.
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
use autumn_harvest_plugin::connector::{
    ConnectorRuntime, EventSource, MappedMessage, SourceBinding, resolve_idempotency_mode,
};
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
/// Header with a message id that does not change on redelivery.
/// Harvest uses it to find duplicates.
const MESSAGE_ID: &str = "MessageId";

// ─────────────────────────── harvest: the durable middle ───────────────────────────

#[workflow]
async fn process_order(ctx: &WorkflowContext, order: Value) -> Result<Value, String> {
    let reservation: Value = ctx
        .execute_activity(&reserve_stock_info(), order.clone())
        .await
        .map_err(|e| e.to_string())?;
    // Waits until camel completes the token. The wait survives a crash.
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

/// Only activities call camel. Workflow code is replayed.
#[activity(start_to_close = "30s")]
async fn reserve_stock(ctx: &ActivityContext, order: Value) -> Result<Value, ActivityFailure> {
    call_route_from_activity(ctx, INVENTORY, order).await
}

// ─────────────────────────── camel: the stateless edges ───────────────────────────

/// Builds the routes. `reserved` records the order id of each inventory call.
fn routes(reserved: Arc<Mutex<Vec<Value>>>) -> Result<Vec<camel_core::RouteDefinition>, BoxError> {
    // Inbound. In production: `from("kafka:orders?…")`.
    let inbound = RouteBuilder::from(ORDERS)
        .route_id("orders-in")
        .to(format!("harvest:orders?idHeader={MESSAGE_ID}"))
        .build()?;
    // Refuse error handlers that would turn an abandon into a commit.
    check_route(&inbound)?;

    // Outbound. In production: an `http:` producer.
    let inventory = RouteBuilder::from(INVENTORY)
        .route_id("inventory")
        .process(move |mut ex: Exchange| {
            let order = json_body(&ex);
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

    // Request out. In production: `.to("jms:queue:charge-requests")`.
    let charge_requests = RouteBuilder::from("harvest-external:charge_card?pollInterval=100")
        .route_id("charge-requests")
        .to("direct:payment-gateway")
        .build()?;
    // Reply back. In production: `from("jms:queue:charge-replies")`.
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

fn json_body(ex: &Exchange) -> Value {
    match &ex.input.body {
        Body::Json(v) => v.clone(),
        _ => Value::Null,
    }
}

// ─────────────────────────────────── wiring ───────────────────────────────────

/// What one run of the flow did.
#[derive(Debug)]
pub struct Outcome {
    /// The order id of this run.
    pub order_id: String,
    /// The workflow result.
    pub result: Value,
    /// The number of inventory calls for this order.
    pub inventory_calls: usize,
}

impl Outcome {
    /// Makes sure that the order is reserved one time and charged.
    pub fn check(&self) -> Result<(), String> {
        let expected = json!({
            "order_id": self.order_id,
            "reservation": {"sku": "sku-1", "reserved": 2},
            "payment": {"approved": true, "amount": 42},
        });
        if self.result != expected {
            return Err(format!("result {} is not {expected}", self.result));
        }
        if self.inventory_calls != 1 {
            return Err(format!(
                "the order was sent two times, but stock was reserved {} times",
                self.inventory_calls
            ));
        }
        Ok(())
    }
}

/// Runs the flow against the Postgres database at `url`.
pub async fn run(url: &str) -> Result<Outcome, BoxError> {
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
        camel.add_route_definition(route).await?;
    }
    camel.start().await?;

    // Resolve the endpoints before an activity can call them.
    let handle = CamelHandle::new();
    handle.resolve(&camel, INVENTORY)?;
    handle.resolve(&camel, ORDERS)?;

    // harvest: a worker runs the workflow and the activity. In production,
    // `HarvestPlugin` starts the worker, applies the migrations and adds a
    // dead-letter sink.
    let runner = HarvestRunner::start(
        autumn_harvest::HarvestBuilder::new()
            .workflows(vec![process_order_info()])
            .activities(vec![reserve_stock_info()])
            .state(handle.clone())
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

    // harvest: the connector starts one workflow per message on stream "orders".
    let api = HarvestApiState::new();
    api.install_storage_pool(runner.storage_pool());
    api.install(runner.api_runtime());
    let binding = SourceBinding::starts("camel-orders", "orders", "process_order").map_json(
        |_ctx, order: Value| {
            let id = order["order_id"].as_str().ok_or("missing order_id")?;
            Ok::<_, &str>(MappedMessage::new(id.to_string(), order))
        },
    );
    let mode = resolve_idempotency_mode(binding.target, binding.idempotency_mode, None);
    let connector = ConnectorRuntime::new(
        Arc::new(binding),
        source as Arc<dyn EventSource>,
        api,
        Arc::new(NoOpMetrics),
        mode,
    );
    let cancel = CancellationToken::new();
    tokio::spawn({
        let cancel = cancel.clone();
        async move { connector.run(cancel).await }
    });

    // Use a new id for each run. With a fixed id, harvest gives the result of the first run.
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let order_id = format!("order-{nanos:x}");
    let order = json!({"order_id": order_id, "sku": "sku-1", "quantity": 2, "amount": 42});
    // Send the same message two times. Each send returns when harvest holds the message.
    for _ in 0..2 {
        let headers = [(MESSAGE_ID.to_string(), json!(format!("msg-{order_id}")))];
        call_route(&handle, ORDERS, order.clone(), headers).await?;
    }

    let mut conn = pool.get().await?;
    let execution = resolve_execution_id_by_workflow_id(&mut conn, "process_order", &order_id)
        .await?
        .ok_or("the workflow did not start")?;
    drop(conn);
    let result = WorkflowHandleClient::single(pool.clone(), url)
        .handle(execution.exec_id)
        .result_raw_with_timeout(Duration::from_secs(60))
        .await?;

    camel.stop().await?;
    cancel.cancel();
    runner.stop().await;
    let inventory_calls = reserved
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter(|id| **id == order_id)
        .count();
    Ok(Outcome {
        order_id,
        result,
        inventory_calls,
    })
}

#[derive(diesel::QueryableByName)]
struct Present {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    present: bool,
}

/// Applies the harvest schema if it is missing. Then applies the claim table.
async fn migrate(url: &str) -> Result<(), BoxError> {
    use diesel_async::RunQueryDsl;

    let mut conn = AsyncPgConnection::establish(url).await?;
    // Lets one run at a time migrate. The lock stops when the connection closes.
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
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let url = std::env::var("DATABASE_URL").map_err(|_| "set DATABASE_URL to a Postgres URL")?;
    let outcome = run(&url).await?;
    println!("{}", serde_json::to_string_pretty(&outcome.result)?);
    Ok(outcome.check()?)
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
        let outcome = run(&url).await.expect("the order flow completes");
        outcome.check().unwrap();
    }
}
