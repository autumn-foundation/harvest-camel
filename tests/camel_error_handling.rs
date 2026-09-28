//! What a camel route error handler does to an abandoned `harvest:` message,
//! measured against real camel routes (no Postgres needed).
//!
//! camel's at-least-once consumers commit when `send_and_wait` returns `Ok`.
//! These tests pin down which handler shapes let the bridge's `Err` through
//! (safe) and which turn it into `Ok` (the consumer would commit a message
//! harvest abandoned), and that `check_route` flags exactly the latter.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use autumn_harvest_plugin::connector::EventSource;
use camel_api::{BoxProcessor, CamelError, ErrorHandlerConfig, Exchange, Message};
use camel_builder::{RouteBuilder, StepAccumulator};
use camel_component_api::{NoOpComponentContext, RuntimeObservability};
use camel_component_direct::DirectComponent;
use camel_core::{CamelContext, RouteDefinition};
use harvest_camel::{CamelSource, HarvestBridge, check_route, is_unsettled};
use tower::ServiceExt;

/// Abandon every message the source receives, as harvest does on a
/// transient failure.
fn abandon_everything(source: Arc<CamelSource>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match source.receive(8, Duration::from_millis(50)).await {
                Ok(batch) => {
                    for m in batch {
                        source.abandon(&m.handle).await.unwrap();
                    }
                }
                Err(_) => return,
            }
        }
    })
}

struct Outcome {
    result: Result<Exchange, CamelError>,
    dead_lettered: usize,
    check: Result<(), harvest_camel::RouteCheckError>,
}

/// Run one exchange through `direct:in → harvest:orders` configured by
/// `configure`, with harvest abandoning everything.
async fn run(configure: impl FnOnce(RouteBuilder) -> RouteBuilder) -> Outcome {
    let (source, component) = HarvestBridge::new("orders");
    let abandoner = abandon_everything(Arc::clone(&source));

    let mut camel = CamelContext::builder().build().await.unwrap();
    camel.register_component(DirectComponent::new());
    camel.register_component(component);

    let dead_lettered = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&dead_lettered);
    let dlq = RouteBuilder::from("direct:dlq")
        .route_id("dlq")
        .process(move |ex| {
            counter.fetch_add(1, Ordering::SeqCst);
            async move { Ok(ex) }
        })
        .build()
        .unwrap();
    camel.add_route_definition(dlq).await.unwrap();

    let route: RouteDefinition = configure(RouteBuilder::from("direct:in").route_id("to-harvest"))
        .to("harvest:orders?idHeader=Id")
        .build()
        .unwrap();
    let check = check_route(&route);
    camel.add_route_definition(route).await.unwrap();
    camel.start().await.unwrap();

    let direct = camel.registry().get_or_err("direct").unwrap();
    let endpoint = direct.create_endpoint("direct:in", &camel).unwrap();
    let rt: Arc<dyn RuntimeObservability> = Arc::new(NoOpComponentContext);
    let producer: BoxProcessor = endpoint
        .create_producer(rt, &camel.producer_context())
        .unwrap();
    let mut msg = Message::new("{}");
    msg.set_header("Id", "m-1");
    let result = producer.oneshot(Exchange::new(msg)).await;

    camel.stop().await.unwrap();
    source.close().await;
    abandoner.abort();
    Outcome {
        result,
        dead_lettered: dead_lettered.load(Ordering::SeqCst),
        check,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_error_handler_propagates_the_abandon() {
    let out = run(|r| r).await;
    let err = out
        .result
        .expect_err("abandon must reach the consumer as Err");
    assert!(is_unsettled(&err), "{err}");
    assert!(out.check.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn log_only_propagates_the_abandon() {
    let out = run(|r| r.error_handler(ErrorHandlerConfig::log_only())).await;
    assert!(is_unsettled(&out.result.unwrap_err()));
    assert!(out.check.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn camel_retry_policy_still_propagates_after_exhaustion() {
    let cfg = ErrorHandlerConfig::log_only()
        .on_exception(|_| true)
        .retry(1)
        .with_backoff(Duration::from_millis(1), 1.0, Duration::from_millis(1))
        .build();
    let out = run(|r| r.error_handler(cfg)).await;
    assert!(is_unsettled(&out.result.unwrap_err()));
    assert!(out.check.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catch_all_dead_letter_channel_swallows_the_abandon_and_is_rejected() {
    let out = run(|r| r.error_handler(ErrorHandlerConfig::dead_letter_channel("direct:dlq"))).await;
    // The hazard: the route reports success, so a Kafka consumer would commit.
    assert!(
        out.result.is_ok(),
        "camel absorbs the error into the DLC and returns Ok"
    );
    assert_eq!(out.dead_lettered, 1);
    assert!(
        matches!(
            out.check,
            Err(harvest_camel::RouteCheckError::CatchAllDeadLetter { .. })
        ),
        "check_route must flag it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handled_policy_swallows_the_abandon_and_is_rejected() {
    let cfg = ErrorHandlerConfig::log_only()
        .on_exception(|_| true)
        .handled(true)
        .build();
    let out = run(|r| r.error_handler(cfg)).await;
    assert!(out.result.is_ok(), "handled(true) returns Ok");
    assert!(matches!(
        out.check,
        Err(harvest_camel::RouteCheckError::AbsorbingPolicy { .. })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dead_letter_channel_excluding_unsettled_errors_is_safe() {
    let cfg = ErrorHandlerConfig::dead_letter_channel("direct:dlq")
        .on_exception(|e| !is_unsettled(e))
        .handled(true)
        .build();
    let out = run(|r| r.error_handler(cfg)).await;
    let err = out
        .result
        .expect_err("unsettled errors bypass the absorbing policy");
    assert!(is_unsettled(&err));
    assert!(out.check.is_ok());
}
