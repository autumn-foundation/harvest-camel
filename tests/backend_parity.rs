//! Cross-backend parity of the inbound bridge (issue #9).
//!
//! One scenario table runs twice: through the plugin's Postgres
//! `ConnectorRuntime`, and through [`SqliteInbound`] on an in-memory
//! `SqliteRuntime`. Each scenario feeds the same deliveries to a
//! `MockSource` in the same passes. The test asserts that both backends give
//! the same pass summaries, execution counts, signal counts and dead letters.
//!
//! The Postgres half needs `HARVEST_CAMEL_TEST_DATABASE_URL`, as the other
//! e2e suites do. Without it the test checks each scenario's expectation on
//! SQLite only, and says so.

use std::sync::Arc;

use autumn_harvest::WorkflowContext;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime};
use autumn_harvest_plugin::connector::{
    ConnectorRuntime, EventSource, IdempotencyMode, MappedMessage, MockSource, PassSummary,
    PostgresDeadLetterSink, SourceBinding, resolve_idempotency_mode,
};
use autumn_harvest_sqlite::{ExecutionFilter, SqliteAuthStore, SqliteRuntime};
use diesel::sql_types::{BigInt, Text};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use harvest_camel::sqlite::{SqliteDeadLetters, SqliteInbound};
use serde_json::{Value, json};

mod common;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[autumn_harvest::prelude::workflow]
async fn fulfil_order(_ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    Ok(input)
}

/// Waits for `msg` signals forever, so its history keeps each one.
#[autumn_harvest::prelude::workflow]
async fn order_inbox(ctx: &WorkflowContext, _input: Value) -> Result<Value, String> {
    loop {
        ctx.wait_for_signal("msg")
            .await
            .map_err(|e| e.to_string())?;
    }
}

// ───────────────────────────── scenarios ─────────────────────────────

/// One scenario: a binding, the deliveries of each pass, and what to count.
struct Scenario {
    name: &'static str,
    binding: fn(&str) -> SourceBinding,
    /// `(message id, body)` per pass. A message left for redelivery comes
    /// back on the next pass by itself.
    passes: Vec<Vec<(&'static str, Value)>>,
    /// The workflow and the ids, without the run prefix, whose runs to count.
    workflow: &'static str,
    ids: Vec<&'static str>,
    /// Count the signals of the first id.
    signals: bool,
}

/// What a backend did with a scenario. Both backends must agree on it.
#[derive(Debug, PartialEq, Eq)]
struct Observed {
    passes: Vec<(usize, usize, usize, usize)>,
    executions: Vec<usize>,
    signals: Option<usize>,
    dead_letters: usize,
}

const fn summary(s: &PassSummary) -> (usize, usize, usize, usize) {
    (s.received, s.acked, s.retried, s.dead_lettered)
}

fn order_starts(prefix: &str) -> SourceBinding {
    let prefix = prefix.to_string();
    SourceBinding::starts("parity-orders", "orders", "fulfil_order")
        .idempotency_mode(IdempotencyMode::WorkflowId)
        .poison_threshold(2)
        .map_json(move |_ctx, body: Value| {
            if body.get("reject").is_some() {
                return Err("rejected by the mapping".to_string());
            }
            let order_id = body["order_id"].as_str().ok_or("missing order_id")?;
            Ok(MappedMessage::new(format!("{prefix}{order_id}"), body))
        })
}

fn ghost_starts(prefix: &str) -> SourceBinding {
    let prefix = prefix.to_string();
    SourceBinding::starts("parity-ghost", "orders", "ghost_workflow")
        .idempotency_mode(IdempotencyMode::WorkflowId)
        .map_json(move |_ctx, body: Value| {
            Ok::<_, &str>(MappedMessage::new(format!("{prefix}g-1"), body))
        })
}

fn inbox_signals(prefix: &str) -> SourceBinding {
    let prefix = prefix.to_string();
    SourceBinding::signals_with_start("parity-inbox", "orders", "order_inbox", "msg").map_json(
        move |_ctx, body: Value| {
            let customer = body["customer"].as_str().ok_or("missing customer")?;
            Ok::<_, &str>(MappedMessage::new(format!("{prefix}{customer}"), body))
        },
    )
}

fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "a redelivered start attaches",
            binding: order_starts,
            passes: vec![
                vec![
                    ("a", json!({"order_id": "o-1"})),
                    ("b", json!({"order_id": "o-2"})),
                ],
                vec![("a", json!({"order_id": "o-1"}))],
            ],
            workflow: "fulfil_order",
            ids: vec!["o-1", "o-2"],
            signals: false,
        },
        Scenario {
            name: "signals fan in and a redelivered signal is dropped",
            binding: inbox_signals,
            passes: vec![
                vec![("m-1", json!({"customer": "c-1"}))],
                vec![
                    ("m-1", json!({"customer": "c-1"})),
                    ("m-2", json!({"customer": "c-1"})),
                ],
            ],
            workflow: "order_inbox",
            ids: vec!["c-1"],
            signals: true,
        },
        Scenario {
            name: "a malformed body is dead-lettered once",
            binding: order_starts,
            passes: vec![
                vec![("bad", json!("not an object"))],
                vec![("bad", json!("not an object"))],
            ],
            workflow: "fulfil_order",
            ids: vec![],
            signals: false,
        },
        Scenario {
            name: "a rejection is retried until the threshold",
            binding: order_starts,
            passes: vec![vec![("r", json!({"reject": true}))], vec![]],
            workflow: "fulfil_order",
            ids: vec![],
            signals: false,
        },
        Scenario {
            name: "an unknown workflow is poison",
            binding: ghost_starts,
            passes: vec![vec![("g", json!({}))]],
            workflow: "ghost_workflow",
            ids: vec!["g-1"],
            signals: false,
        },
    ]
}

/// Queue one pass of deliveries. The message id carries the run prefix, so
/// a shared Postgres database never sees an id twice across runs.
fn push(source: &MockSource, run: &str, deliveries: &[(&'static str, Value)]) {
    for (id, body) in deliveries {
        let payload = match body {
            Value::String(raw) => raw.clone().into_bytes(),
            other => other.to_string().into_bytes(),
        };
        source.push_opaque(format!("{run}{id}"), payload);
    }
}

// ───────────────────────────── SQLite ─────────────────────────────

async fn run_sqlite(scenario: &Scenario, run: &str) -> TestResult<Observed> {
    let mut rt = SqliteRuntime::open_in_memory()?;
    rt.register_workflow(&fulfil_order_info());
    rt.register_workflow(&order_inbox_info());
    let store = Arc::new(SqliteAuthStore::open_in_memory()?);
    let source = MockSource::new("orders");
    let inbound = SqliteInbound::new(
        (scenario.binding)(run),
        Arc::new(source.clone()) as Arc<dyn EventSource>,
    )?
    .with_dead_letter_sink(Arc::new(SqliteDeadLetters::new(Arc::clone(&store))));

    let mut passes = Vec::new();
    for deliveries in &scenario.passes {
        push(&source, run, deliveries);
        let pass = inbound
            .pump(&mut rt, std::time::Duration::from_millis(10))
            .await?;
        passes.push(summary(&pass));
    }

    let mut executions = Vec::new();
    for id in &scenario.ids {
        let filter = ExecutionFilter {
            workflow_name: Some(scenario.workflow.to_string()),
            workflow_id: Some(format!("{run}{id}")),
            ..ExecutionFilter::default()
        };
        executions.push(rt.list_executions(&filter)?.executions.len());
    }
    let signals = if scenario.signals {
        let id = format!("{run}{}", scenario.ids[0]);
        let exec = rt
            .newest_run_of_key(scenario.workflow, &id)?
            .ok_or("no run")?;
        // SQLite stages a signal until the run's next cycle ingests it.
        rt.run_until_blocked(exec).await?;
        let history = rt.load_history(exec)?;
        Some(
            history
                .iter()
                .filter(|e| e.type_name() == "SignalReceived")
                .count(),
        )
    } else {
        None
    };
    Ok(Observed {
        passes,
        executions,
        signals,
        dead_letters: store.dead_letter_keys()?.len(),
    })
}

// ───────────────────────────── Postgres ─────────────────────────────

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

async fn count(conn: &mut AsyncPgConnection, sql: &str, bind: String) -> TestResult<usize> {
    let rows = diesel::sql_query(sql)
        .bind::<Text, _>(bind)
        .load::<Count>(conn)
        .await?;
    Ok(usize::try_from(<[Count]>::first(&rows).map_or(0, |r| r.n))?)
}

fn started_api_state(pool: &DbPool) -> HarvestApiState {
    let state = HarvestApiState::new();
    state.set_admin_auth_boundary(true);
    state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(
            vec![fulfil_order_info(), order_inbox_info()],
            vec![],
        )),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("harvest-camel-parity".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    state
}

async fn run_postgres(
    scenario: &Scenario,
    run: &str,
    pool: &DbPool,
    conn: &mut AsyncPgConnection,
) -> TestResult<Observed> {
    let source = MockSource::new("orders");
    let binding = (scenario.binding)(run);
    let mode = resolve_idempotency_mode(binding.target, binding.idempotency_mode, None);
    let runtime = ConnectorRuntime::new(
        Arc::new(binding),
        Arc::new(source.clone()) as Arc<dyn EventSource>,
        started_api_state(pool),
        Arc::new(NoOpMetrics),
        mode,
    )
    .with_dead_letter_sink(Arc::new(PostgresDeadLetterSink::new(pool.clone())));

    let mut passes = Vec::new();
    for deliveries in &scenario.passes {
        push(&source, run, deliveries);
        passes.push(summary(&runtime.run_once().await?));
    }

    let mut executions = Vec::new();
    for id in &scenario.ids {
        executions.push(
            count(
                conn,
                "SELECT COUNT(*) AS n FROM harvest_workflow_executions WHERE workflow_id = $1",
                format!("{run}{id}"),
            )
            .await?,
        );
    }
    let signals = if scenario.signals {
        Some(
            count(
                conn,
                "SELECT COUNT(*) AS n FROM harvest_signals s \
                 JOIN harvest_workflow_executions e ON e.id = s.workflow_exec_id \
                 WHERE e.workflow_id = $1",
                format!("{run}{}", scenario.ids[0]),
            )
            .await?,
        )
    } else {
        None
    };
    let dead_letters = count(
        conn,
        "SELECT COUNT(*) AS n FROM harvest_connector_dead_letters WHERE coordinates LIKE $1",
        format!("%{run}%"),
    )
    .await?;
    Ok(Observed {
        passes,
        executions,
        signals,
        dead_letters,
    })
}

// ───────────────────────────── the test ─────────────────────────────

/// What each scenario must do, on every backend.
fn expected(name: &str) -> Observed {
    match name {
        "a redelivered start attaches" => Observed {
            passes: vec![(2, 2, 0, 0), (1, 1, 0, 0)],
            executions: vec![1, 1],
            signals: None,
            dead_letters: 0,
        },
        "signals fan in and a redelivered signal is dropped" => Observed {
            passes: vec![(1, 1, 0, 0), (2, 2, 0, 0)],
            executions: vec![1],
            signals: Some(2),
            dead_letters: 0,
        },
        "a malformed body is dead-lettered once" => Observed {
            passes: vec![(1, 0, 0, 1), (1, 0, 0, 1)],
            executions: vec![],
            signals: None,
            dead_letters: 1,
        },
        "a rejection is retried until the threshold" => Observed {
            passes: vec![(1, 0, 1, 0), (1, 0, 0, 1)],
            executions: vec![],
            signals: None,
            dead_letters: 1,
        },
        "an unknown workflow is poison" => Observed {
            passes: vec![(1, 0, 0, 1)],
            executions: vec![0],
            signals: None,
            dead_letters: 1,
        },
        other => panic!("no expectation for scenario {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn both_backends_settle_every_scenario_the_same_way() -> TestResult {
    let postgres = match common::database_url() {
        Some(url) => {
            let conn = common::migrate(&url).await;
            Some((common::build_pool(&url), conn))
        }
        None => None,
    };
    let mut postgres = postgres;
    for (n, scenario) in scenarios().iter().enumerate() {
        let run = format!("parity-{}-{n}-", common::nonce());
        let want = expected(scenario.name);
        let sqlite = run_sqlite(scenario, &run).await?;
        assert_eq!(sqlite, want, "SQLite: {}", scenario.name);
        if let Some((pool, conn)) = postgres.as_mut() {
            let pg = run_postgres(scenario, &run, pool, conn).await?;
            assert_eq!(pg, sqlite, "Postgres and SQLite differ: {}", scenario.name);
        }
    }
    Ok(())
}
