//! End-to-end on SQLite: camel `direct:` route → `harvest:` → [`SqliteInbound`]
//! → a workflow execution in a caller-owned `SqliteRuntime` (issue #9).
//!
//! No database server is needed. Each test opens an in-memory runtime, so
//! every test always runs.
//!
//! One task owns the runtime, as a single-writer daemon does. It receives a
//! batch, applies it and settles it. The camel producer blocks until its
//! message is settled, so the sends run beside that task.

use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::WorkflowContext;
use autumn_harvest_plugin::connector::{
    EventSource, IdempotencyMode, MappedMessage, PassSummary, SourceBinding,
};
use autumn_harvest_sqlite::{ExecutionFilter, SqliteAuthStore, SqliteRuntime};
use camel_api::{BoxProcessor, CamelError, Exchange, Message, Value};
use camel_builder::{RouteBuilder, StepAccumulator};
use camel_component_api::{NoOpComponentContext, RuntimeObservability};
use camel_component_direct::DirectComponent;
use camel_core::CamelContext;
use harvest_camel::sqlite::{SqliteDeadLetters, SqliteInbound};
use harvest_camel::{CamelSource, HarvestBridge, check_route, is_unsettled};
use serde_json::json;
use tokio::sync::oneshot;
use tower::ServiceExt;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[autumn_harvest::prelude::workflow]
async fn fulfil_order(_ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    Ok(input)
}

/// Collects every `msg` signal until it has seen `input.expect` of them.
#[autumn_harvest::prelude::workflow]
async fn order_inbox(ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    let expect = input["expect"].as_u64().unwrap_or(u64::MAX);
    let mut seen = Vec::new();
    while (seen.len() as u64) < expect {
        seen.push(
            ctx.wait_for_signal("msg")
                .await
                .map_err(|e| e.to_string())?,
        );
    }
    Ok(Value::Array(seen))
}

fn runtime() -> TestResult<SqliteRuntime> {
    let mut rt = SqliteRuntime::open_in_memory()?;
    rt.register_workflow(&fulfil_order_info());
    rt.register_workflow(&order_inbox_info());
    Ok(rt)
}

/// `{"order_id": ..}` → workflow id `order-<order_id>`.
fn starts_binding() -> SourceBinding {
    SourceBinding::starts("camel-orders", "orders", "fulfil_order")
        .idempotency_mode(IdempotencyMode::WorkflowId)
        .map_json(|_ctx, body: Value| {
            let order_id = body["order_id"].as_str().ok_or("missing order_id")?;
            Ok::<_, &str>(MappedMessage::new(format!("order-{order_id}"), body))
        })
}

/// The task that owns the runtime. Stopping it hands the runtime back.
struct Owner {
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<(SqliteRuntime, PassSummary)>,
}

impl Owner {
    fn start(inbound: SqliteInbound, mut rt: SqliteRuntime) -> Self {
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut total = PassSummary::default();
            loop {
                if stopped.try_recv().is_ok() {
                    return (rt, total);
                }
                let Ok(pass) = inbound.pump(&mut rt, Duration::from_millis(20)).await else {
                    return (rt, total);
                };
                total.received += pass.received;
                total.acked += pass.acked;
                total.retried += pass.retried;
                total.dead_lettered += pass.dead_lettered;
            }
        });
        Self { stop, task }
    }

    async fn stop(self) -> TestResult<(SqliteRuntime, PassSummary)> {
        let _ = self.stop.send(());
        Ok(tokio::time::timeout(Duration::from_secs(10), self.task).await??)
    }
}

/// A started camel context with `direct:in → <harvest_uri>`.
async fn camel_with_route(
    component: harvest_camel::HarvestComponent,
    harvest_uri: &str,
) -> TestResult<(CamelContext, BoxProcessor)> {
    let mut camel = CamelContext::builder().build().await?;
    camel.register_component(DirectComponent::new());
    camel.register_component(component);
    let route = RouteBuilder::from("direct:in")
        .route_id("to-harvest")
        .to(harvest_uri)
        .build()?;
    check_route(&route)?;
    camel.add_route_definition(route).await?;
    camel.start().await?;

    let direct = camel.registry().get_or_err("direct")?;
    let endpoint = direct.create_endpoint("direct:in", &camel)?;
    let rt: Arc<dyn RuntimeObservability> = Arc::new(NoOpComponentContext);
    let producer = endpoint.create_producer(rt, &camel.producer_context())?;
    Ok((camel, producer))
}

/// Boxed, because `CamelError` is 168 bytes (clippy `result_large_err`).
async fn send(producer: &BoxProcessor, message: Message) -> Result<Exchange, Box<CamelError>> {
    producer
        .clone()
        .oneshot(Exchange::new(message))
        .await
        .map_err(Box::new)
}

fn order(order_id: &str, message_id: &str) -> Message {
    let mut msg = Message::new(json!({ "order_id": order_id }).to_string());
    msg.set_header("Id", message_id.to_string());
    msg
}

fn executions(rt: &SqliteRuntime, workflow: &str, id: &str) -> TestResult<usize> {
    let filter = ExecutionFilter {
        workflow_name: Some(workflow.to_string()),
        workflow_id: Some(id.to_string()),
        ..ExecutionFilter::default()
    };
    Ok(rt.list_executions(&filter)?.executions.len())
}

fn bridge(
    binding: SourceBinding,
) -> TestResult<(
    Arc<CamelSource>,
    harvest_camel::HarvestComponent,
    SqliteInbound,
)> {
    let (source, component) = HarvestBridge::new(binding.stream.clone());
    let inbound = SqliteInbound::new(binding, Arc::clone(&source) as Arc<dyn EventSource>)?;
    Ok((source, component, inbound))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_redelivered_message_starts_exactly_one_workflow() -> TestResult {
    let (source, component, inbound) = bridge(starts_binding())?;
    let owner = Owner::start(inbound, runtime()?);
    let (mut camel, producer) = camel_with_route(component, "harvest:orders?idHeader=Id").await?;

    // An uncommitted consumer crash delivers the same message three times.
    for _ in 0..3 {
        send(&producer, order("o-1", "msg-1")).await?;
    }
    send(&producer, order("o-2", "msg-2")).await?;
    camel.stop().await?;
    let (rt, total) = owner.stop().await?;

    assert_eq!(executions(&rt, "fulfil_order", "order-o-1")?, 1);
    assert_eq!(executions(&rt, "fulfil_order", "order-o-2")?, 1);
    assert_eq!((total.received, total.acked), (4, 4), "{total:?}");
    assert_eq!(source.in_flight(), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signals_with_start_fan_in_to_one_run_and_dedupe_redelivery() -> TestResult {
    let binding = SourceBinding::signals_with_start("camel-inbox", "inbox", "order_inbox", "msg")
        .map_json(|_ctx, body: Value| {
            let customer = body["customer"].as_str().ok_or("missing customer")?;
            Ok::<_, &str>(MappedMessage::new(
                format!("inbox-{customer}"),
                json!({ "expect": 2, "text": body["text"] }),
            ))
        });
    let (_source, component, inbound) = bridge(binding)?;
    let owner = Owner::start(inbound, runtime()?);
    let (mut camel, producer) = camel_with_route(component, "harvest:inbox?idHeader=Id").await?;

    for (id, text) in [("m-1", "hi"), ("m-1", "hi"), ("m-2", "there")] {
        let mut msg = Message::new(json!({ "customer": "c-1", "text": text }).to_string());
        msg.set_header("Id", id.to_string());
        send(&producer, msg).await?;
    }
    camel.stop().await?;
    let (mut rt, _) = owner.stop().await?;

    assert_eq!(executions(&rt, "order_inbox", "inbox-c-1")?, 1);
    let exec = rt
        .newest_run_of_key("order_inbox", "inbox-c-1")?
        .ok_or("no run")?;
    rt.run_until_blocked(exec).await?;
    let signals = rt
        .load_history(exec)?
        .iter()
        .filter(|e| e.type_name() == "SignalReceived")
        .count();
    assert_eq!(signals, 2, "the redelivery of m-1 adds no signal");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_malformed_body_is_dead_lettered_and_acked() -> TestResult {
    let store = Arc::new(SqliteAuthStore::open_in_memory()?);
    let (_source, component, inbound) = bridge(starts_binding())?;
    let inbound =
        inbound.with_dead_letter_sink(Arc::new(SqliteDeadLetters::new(Arc::clone(&store))));
    let owner = Owner::start(inbound, runtime()?);
    let (mut camel, producer) = camel_with_route(component, "harvest:orders?idHeader=Id").await?;

    let mut bad = Message::new("not json".to_string());
    bad.set_header("Id", "bad-1".to_string());
    // A dead letter is durable, so the camel consumer may commit it.
    send(&producer, bad.clone()).await?;
    send(&producer, bad).await?;
    camel.stop().await?;
    let (_rt, total) = owner.stop().await?;

    assert_eq!(total.dead_lettered, 2, "{total:?}");
    assert_eq!(store.dead_letter_keys()?.len(), 1, "one row per message");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn with_no_sink_a_poison_message_fails_the_exchange() -> TestResult {
    let (_source, component, inbound) = bridge(starts_binding())?;
    let owner = Owner::start(inbound, runtime()?);
    let (mut camel, producer) = camel_with_route(component, "harvest:orders?idHeader=Id").await?;

    let mut bad = Message::new("not json".to_string());
    bad.set_header("Id", "bad-1".to_string());
    let err = send(&producer, bad)
        .await
        .expect_err("an unrecorded poison message must not be committed");
    assert!(is_unsettled(&err), "{err}");
    camel.stop().await?;
    let (_rt, total) = owner.stop().await?;
    assert_eq!(total.retried, 1, "{total:?}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unknown_workflow_is_poison_not_a_retry_loop() -> TestResult {
    let store = Arc::new(SqliteAuthStore::open_in_memory()?);
    let binding = SourceBinding::starts("camel-ghost", "orders", "ghost")
        .idempotency_mode(IdempotencyMode::WorkflowId)
        .map_json(|_ctx, body: Value| Ok::<_, &str>(MappedMessage::new("g-1", body)));
    let (_source, component, inbound) = bridge(binding)?;
    let inbound =
        inbound.with_dead_letter_sink(Arc::new(SqliteDeadLetters::new(Arc::clone(&store))));
    let owner = Owner::start(inbound, runtime()?);
    let (mut camel, producer) = camel_with_route(component, "harvest:orders?idHeader=Id").await?;

    send(&producer, order("o-1", "ghost-1")).await?;
    camel.stop().await?;
    let (_rt, total) = owner.stop().await?;
    assert_eq!(total.dead_lettered, 1, "{total:?}");
    assert_eq!(store.dead_letter_keys()?.len(), 1);
    Ok(())
}

/// The daemon shape: receive in one task, apply in the runtime task, settle
/// back in the first. The batch crosses a channel both ways.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_three_phases_run_in_different_tasks() -> TestResult {
    let (source, component, inbound) = bridge(starts_binding())?;
    let inbound = Arc::new(inbound);
    let (jobs, mut job_rx) = tokio::sync::mpsc::channel::<(
        harvest_camel::sqlite::InboundBatch,
        oneshot::Sender<harvest_camel::sqlite::AppliedBatch>,
    )>(4);
    let runtime_task = tokio::spawn({
        let mut rt = runtime()?;
        async move {
            while let Some((batch, reply)) = job_rx.recv().await {
                let _ = reply.send(batch.apply(&mut rt));
            }
            rt
        }
    });
    let receiver = tokio::spawn({
        let inbound = Arc::clone(&inbound);
        async move {
            let mut acked = 0;
            while acked < 2 {
                let Ok(batch) = inbound.receive(Duration::from_millis(20)).await else {
                    break;
                };
                if batch.is_empty() {
                    continue;
                }
                let (reply, applied) = oneshot::channel();
                if jobs.send((batch, reply)).await.is_err() {
                    break;
                }
                let Ok(applied) = applied.await else { break };
                acked += inbound.settle(applied).await.acked;
            }
            acked
        }
    });
    let (mut camel, producer) = camel_with_route(component, "harvest:orders?idHeader=Id").await?;
    send(&producer, order("o-1", "msg-1")).await?;
    send(&producer, order("o-1", "msg-1")).await?;
    camel.stop().await?;

    assert_eq!(receiver.await?, 2);
    let rt = runtime_task.await?;
    assert_eq!(executions(&rt, "fulfil_order", "order-o-1")?, 1);
    assert_eq!(source.in_flight(), 0);
    Ok(())
}
