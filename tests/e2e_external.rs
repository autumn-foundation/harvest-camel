//! End-to-end durable request/reply against a real harvest worker:
//!
//! ```text
//! direct:start ─► harvest:orders ─► workflow charge_order
//!     ctx.execute_activity_external("<activity>", input) ── parks, token issued
//! harvest-external:<activity> ─► route (the "remote system") ─► harvest-complete:<activity>
//!     workflow resumes with the reply and completes
//! ```
//!
//! Needs `HARVEST_CAMEL_TEST_DATABASE_URL`; skipped with a notice otherwise.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::WorkflowContext;
use autumn_harvest::handle::WorkflowHandleClient;
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::worker::DbPool;
use autumn_harvest_plugin::api::HarvestApiState;
use autumn_harvest_plugin::connector::{
    ConnectorRuntime, EventSource, MappedMessage, PostgresDeadLetterSink, SourceBinding,
    resolve_idempotency_mode,
};
use autumn_harvest_plugin::{
    HarvestBatchConfig, HarvestDatabaseConfig, HarvestMode, HarvestOutboxConfig,
    HarvestReadinessConfig, HarvestRunner, HarvestRunnerResources, HarvestRuntimeConfig,
    HarvestStartupConfig,
};
use camel_api::{Body, CamelError, Exchange, Message, Value};
use camel_builder::{RouteBuilder, StepAccumulator};
use camel_component_direct::DirectComponent;
use camel_core::CamelContext;
use common::{build_pool, database_url, migrate, nonce, producer};
use harvest_camel::external::{
    HEADER_DISPATCH_ATTEMPT, HEADER_EXTERNAL_ERROR, HEADER_EXTERNAL_SETTLED,
};
use harvest_camel::{ExternalTasks, HEADER_EXTERNAL_TOKEN, HarvestBridge};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

/// `{"activity": name, ..}` → awaits external activity `name` with the whole
/// input, and returns `{"reply": <completion output>}`.
#[autumn_harvest::prelude::workflow]
async fn charge_order(ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    let activity = input["activity"]
        .as_str()
        .ok_or("missing activity")?
        .to_string();
    let reply = ctx
        .execute_activity_external(&activity, input, "default", 120)
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!({ "reply": reply }))
}

/// One harvest runner at a time: each test starts its own worker.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Stack {
    url: String,
    pool: DbPool,
    runner: Option<HarvestRunner>,
    connector: CancellationToken,
    camel: CamelContext,
}

impl Stack {
    /// A running harvest worker, a camel context with `direct:start →
    /// harvest:orders` feeding `charge_order`, and the external-task
    /// components registered. `routes` adds the test's own routes.
    async fn start(url: String, routes: Vec<camel_core::RouteDefinition>) -> Self {
        migrate(&url).await;
        let pool = build_pool(&url);
        let runner = HarvestRunner::start(
            autumn_harvest::HarvestBuilder::new()
                .workflows(vec![charge_order_info()])
                .build(),
            &HarvestRuntimeConfig {
                mode: HarvestMode::External,
                worker_enabled: true,
                scheduler_enabled: true,
                database: HarvestDatabaseConfig {
                    url: Some(url.clone()),
                },
                outbox: HarvestOutboxConfig::default(),
                batch: HarvestBatchConfig::default(),
                readiness: HarvestReadinessConfig::default(),
                startup: HarvestStartupConfig::default(),
            },
            HarvestRunnerResources::new(pool.clone()),
        )
        .await
        .expect("harvest runner");

        // Inbound: harvest:orders starts charge_order with workflow id = Id header.
        let api_state = HarvestApiState::new();
        api_state.set_admin_auth_boundary(true);
        api_state.install_storage_pool(runner.storage_pool());
        api_state.install(runner.api_runtime());
        let (source, component) = HarvestBridge::new("orders");
        let binding = SourceBinding::starts("camel-orders-external", "orders", "charge_order")
            .map_json(|ctx, body: Value| {
                let id = ctx.header("Id").ok_or("missing Id")?.to_string();
                Ok::<_, &str>(MappedMessage::new(id, body))
            });
        let mode = resolve_idempotency_mode(binding.target, binding.idempotency_mode, None);
        let connector = ConnectorRuntime::new(
            Arc::new(binding),
            source as Arc<dyn EventSource>,
            api_state,
            Arc::new(NoOpMetrics),
            mode,
        )
        .with_dead_letter_sink(Arc::new(PostgresDeadLetterSink::new(pool.clone())));
        let cancel = CancellationToken::new();
        tokio::spawn({
            let cancel = cancel.clone();
            async move { connector.run(cancel).await }
        });

        let mut camel = CamelContext::builder().build().await.expect("camel");
        camel.register_component(DirectComponent::new());
        camel.register_component(component);
        ExternalTasks::new(pool.clone()).register(&mut camel);
        let start = RouteBuilder::from("direct:start")
            .route_id("start")
            .to("harvest:orders?idHeader=Id")
            .build()
            .unwrap();
        camel.add_route_definition(start).await.unwrap();
        for route in routes {
            camel.add_route_definition(route).await.unwrap();
        }
        camel.start().await.expect("start camel");

        Self {
            url,
            pool,
            runner: Some(runner),
            connector: cancel,
            camel,
        }
    }

    async fn start_workflow(&self, workflow_id: &str, input: Value) {
        let mut msg = Message::new(Body::Json(input));
        msg.set_header("Id", workflow_id);
        producer(&self.camel, "direct:start")
            .oneshot(Exchange::new(msg))
            .await
            .expect("workflow start acked by harvest");
    }

    async fn result(&self, workflow_id: &str) -> Result<Value, String> {
        let mut conn = self.pool.get().await.unwrap();
        let run = autumn_harvest::execution::resolve_execution_id_by_workflow_id(
            &mut conn,
            "charge_order",
            workflow_id,
        )
        .await
        .unwrap()
        .expect("execution exists");
        drop(conn);
        WorkflowHandleClient::single(self.pool.clone(), self.url.clone())
            .handle(run.exec_id)
            .result_raw_with_timeout(Duration::from_secs(60))
            .await
            .map_err(|e| e.to_string())
    }

    async fn stop(mut self) {
        self.camel.stop().await.expect("stop camel");
        self.connector.cancel();
        if let Some(runner) = self.runner.take() {
            runner.stop().await;
        }
    }
}

/// Records every exchange a route sees, keyed by token.
#[derive(Clone, Default)]
struct Seen(Arc<Mutex<Vec<(String, Value, i64)>>>);

impl Seen {
    fn record(&self, ex: &Exchange) {
        let token = ex
            .input
            .header(HEADER_EXTERNAL_TOKEN)
            .and_then(Value::as_str);
        let attempt = ex
            .input
            .header(HEADER_DISPATCH_ATTEMPT)
            .and_then(Value::as_i64);
        let body = match &ex.input.body {
            Body::Json(v) => v.clone(),
            _ => Value::Null,
        };
        self.0.lock().unwrap().push((
            token.unwrap_or_default().to_string(),
            body,
            attempt.unwrap_or_default(),
        ));
    }

    fn all(&self) -> Vec<(String, Value, i64)> {
        self.0.lock().unwrap().clone()
    }
}

// ───────────────────────────── tests ─────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_activity_round_trips_through_camel() {
    let Some(url) = database_url() else { return };
    let _serial = SERIAL.lock().await;
    let run = nonce();
    let activity = format!("charge_{run}");

    // The "remote system": echoes an approval carrying the request's amount,
    // replying through harvest-complete: with the token header intact.
    let seen = Seen::default();
    let recorder = seen.clone();
    let remote = RouteBuilder::from(&format!("harvest-external:{activity}?pollInterval=50"))
        .route_id("remote")
        .process(move |mut ex: Exchange| {
            recorder.record(&ex);
            let amount = match &ex.input.body {
                Body::Json(v) => v["amount"].clone(),
                _ => Value::Null,
            };
            ex.input.body = Body::Json(json!({"approved": true, "amount": amount}));
            async move { Ok(ex) }
        })
        .to(format!("harvest-complete:{activity}"))
        .build()
        .unwrap();
    let stack = Stack::start(url, vec![remote]).await;

    let wf = format!("order-{run}");
    stack
        .start_workflow(&wf, json!({"activity": activity, "amount": 42}))
        .await;
    let result = stack.result(&wf).await.expect("workflow completes");
    assert_eq!(
        result,
        json!({"reply": {"approved": true, "amount": 42}}),
        "the workflow resumes with the reply camel completed it with"
    );

    let seen = seen.all();
    assert_eq!(seen.len(), 1, "dispatched exactly once: {seen:?}");
    assert_eq!(
        seen[0].1["amount"], 42,
        "the route received the activity input"
    );
    assert_eq!(seen[0].2, 1, "first dispatch attempt");
    stack.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn harvest_fail_fails_the_workflow_with_the_message() {
    let Some(url) = database_url() else { return };
    let _serial = SERIAL.lock().await;
    let run = nonce();
    let activity = format!("decline_{run}");

    let remote = RouteBuilder::from(&format!("harvest-external:{activity}?pollInterval=50"))
        .route_id("remote")
        .process(|mut ex: Exchange| {
            ex.input.set_header(HEADER_EXTERNAL_ERROR, "card declined");
            async move { Ok(ex) }
        })
        .to(format!("harvest-fail:{activity}"))
        .build()
        .unwrap();
    let stack = Stack::start(url, vec![remote]).await;

    let wf = format!("order-{run}");
    stack
        .start_workflow(&wf, json!({"activity": activity}))
        .await;
    let err = stack
        .result(&wf)
        .await
        .expect_err("a failed external activity fails the workflow");
    assert!(err.contains("card declined"), "{err}");
    stack.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_route_is_redispatched_and_then_completes() {
    let Some(url) = database_url() else { return };
    let _serial = SERIAL.lock().await;
    let run = nonce();
    let activity = format!("flaky_{run}");

    // First delivery: the downstream send fails. The claim is released and
    // the same token comes back after retryDelay.
    let seen = Seen::default();
    let recorder = seen.clone();
    let remote = RouteBuilder::from(&format!(
        "harvest-external:{activity}?pollInterval=50&retryDelay=100"
    ))
    .route_id("remote")
    .process(move |mut ex: Exchange| {
        recorder.record(&ex);
        let first = ex
            .input
            .header(HEADER_DISPATCH_ATTEMPT)
            .and_then(Value::as_i64)
            == Some(1);
        ex.input.body = Body::Json(json!("ok"));
        async move {
            if first {
                Err(CamelError::Io("broker unavailable".into()))
            } else {
                Ok(ex)
            }
        }
    })
    .to(format!("harvest-complete:{activity}"))
    .build()
    .unwrap();
    let stack = Stack::start(url, vec![remote]).await;

    let wf = format!("order-{run}");
    stack
        .start_workflow(&wf, json!({"activity": activity}))
        .await;
    assert_eq!(stack.result(&wf).await.unwrap(), json!({"reply": "ok"}));

    let seen = seen.all();
    assert_eq!(
        seen.len(),
        2,
        "one failed and one successful dispatch: {seen:?}"
    );
    assert_eq!(seen[0].0, seen[1].0, "the same token both times");
    assert_eq!((seen[0].2, seen[1].2), (1, 2), "dispatch attempts 1 then 2");
    stack.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn competing_consumers_dispatch_each_token_once() {
    let Some(url) = database_url() else { return };
    let _serial = SERIAL.lock().await;
    let run = nonce();
    let activity = format!("fanout_{run}");

    // Two consumers on the same activity, as two app instances would run.
    let seen = Seen::default();
    let route = |id: &str, seen: Seen| {
        RouteBuilder::from(&format!(
            "harvest-external:{activity}?pollInterval=20&batchSize=4"
        ))
        .route_id(id)
        .process(move |ex: Exchange| {
            seen.record(&ex);
            async move { Ok(ex) }
        })
        .to(format!("harvest-complete:{activity}"))
        .build()
        .unwrap()
    };
    let stack = Stack::start(
        url,
        vec![route("a", seen.clone()), route("b", seen.clone())],
    )
    .await;

    let ids: Vec<String> = (0..6).map(|i| format!("order-{run}-{i}")).collect();
    for id in &ids {
        stack
            .start_workflow(id, json!({"activity": activity, "n": id}))
            .await;
    }
    for id in &ids {
        let result = stack.result(id).await.expect("completes");
        assert_eq!(
            result["reply"]["n"],
            json!(id),
            "each workflow gets its own reply"
        );
    }

    let mut per_token: HashMap<String, usize> = HashMap::new();
    for (token, _, _) in seen.all() {
        *per_token.entry(token).or_default() += 1;
    }
    assert_eq!(per_token.len(), ids.len());
    assert!(
        per_token.values().all(|n| *n == 1),
        "every token dispatched exactly once across both consumers: {per_token:?}"
    );
    stack.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settling_unknown_or_settled_tokens() {
    let Some(url) = database_url() else { return };
    let _serial = SERIAL.lock().await;
    let run = nonce();
    let activity = format!("replay_{run}");

    // Complete once, then replay the identical reply: harmless, Settled=false.
    let replies = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&replies);
    let remote = RouteBuilder::from(&format!("harvest-external:{activity}?pollInterval=50"))
        .route_id("remote")
        .process(move |ex: Exchange| {
            let token = ex.input.header(HEADER_EXTERNAL_TOKEN).cloned();
            captured.lock().unwrap().push(token);
            async move { Ok(ex) }
        })
        .to(format!("harvest-complete:{activity}"))
        .build()
        .unwrap();
    let stack = Stack::start(url, vec![remote]).await;
    let wf = format!("order-{run}");
    stack
        .start_workflow(&wf, json!({"activity": activity}))
        .await;
    stack.result(&wf).await.expect("completes");

    let token = replies.lock().unwrap()[0].clone().expect("token header");
    let complete = producer(&stack.camel, &format!("harvest-complete:{activity}"));
    let mut msg = Message::new(Body::Json(json!("late duplicate")));
    msg.set_header(HEADER_EXTERNAL_TOKEN, token);
    let ex = complete.clone().oneshot(Exchange::new(msg)).await.unwrap();
    assert_eq!(
        ex.input.header(HEADER_EXTERNAL_SETTLED),
        Some(&Value::Bool(false)),
        "an already-completed token is a no-op"
    );

    // Unknown token: fails by default, is dropped with onUnknownToken=ignore.
    let unknown = uuid::Uuid::new_v4().to_string();
    let mut msg = Message::new("x");
    msg.set_header(HEADER_EXTERNAL_TOKEN, unknown.clone());
    let err = complete.oneshot(Exchange::new(msg)).await.unwrap_err();
    assert!(matches!(err, CamelError::ValidationError(_)), "{err}");

    let mut msg = Message::new("x");
    msg.set_header(HEADER_EXTERNAL_TOKEN, unknown);
    let ex = producer(&stack.camel, "harvest-fail:x?onUnknownToken=ignore")
        .oneshot(Exchange::new(msg))
        .await
        .unwrap();
    assert_eq!(
        ex.input.header(HEADER_EXTERNAL_SETTLED),
        Some(&Value::Bool(false))
    );

    // A missing or malformed token header is a clear validation error.
    let err = producer(&stack.camel, "harvest-complete:x")
        .oneshot(Exchange::new(Message::new("x")))
        .await
        .unwrap_err();
    assert!(err.to_string().contains(HEADER_EXTERNAL_TOKEN), "{err}");
    let mut msg = Message::new("x");
    msg.set_header(HEADER_EXTERNAL_TOKEN, "not-a-uuid");
    let err = producer(&stack.camel, "harvest-complete:x")
        .oneshot(Exchange::new(msg))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not-a-uuid"), "{err}");
    stack.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outstanding_requests_do_not_starve_new_ones() {
    let Some(url) = database_url() else { return };
    let _serial = SERIAL.lock().await;
    let run = nonce();
    let activity = format!("backlog_{run}");

    // batchSize=1 and a request that is sent but never answered: it stays
    // PENDING in harvest (awaiting its reply) for the whole test, and must not
    // occupy the only batch slot forever.
    let seen = Seen::default();
    let recorder = seen.clone();
    let remote = RouteBuilder::from(&format!(
        "harvest-external:{activity}?pollInterval=20&batchSize=1"
    ))
    .route_id("remote")
    .process(move |mut ex: Exchange| {
        recorder.record(&ex);
        let answer = matches!(&ex.input.body, Body::Json(v) if v["answer"] == true);
        if !answer {
            // Request sent; the reply will come "later" (never, here).
            ex.set_property(camel_api::CAMEL_STOP, true);
        }
        async move { Ok(ex) }
    })
    .to(format!("harvest-complete:{activity}"))
    .build()
    .unwrap();
    let stack = Stack::start(url, vec![remote]).await;

    let silent = format!("order-{run}-silent");
    stack
        .start_workflow(&silent, json!({"activity": activity, "answer": false}))
        .await;
    // Wait until the silent request is dispatched, so it is the older one.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while seen.all().is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "silent request never dispatched"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let answered = format!("order-{run}-answered");
    stack
        .start_workflow(&answered, json!({"activity": activity, "answer": true}))
        .await;
    let result = stack
        .result(&answered)
        .await
        .expect("the newer request is still dispatched");
    assert_eq!(result["reply"]["answer"], true);
    assert_eq!(seen.all().len(), 2, "each request dispatched once");
    stack.stop().await;
}
