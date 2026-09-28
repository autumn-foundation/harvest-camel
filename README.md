# harvest-camel

Bridge [rust-camel](https://github.com/kennycallado/rust-camel) and
[autumn-harvest](https://github.com/autumn-foundation/autumn-harvest).
**Camel handles the edges, harvest handles the middle.**

- **rust-camel** is an Apache Camel–style integration framework. It has about 30
  connectors (Kafka, JMS, SQL, HTTP, file, …) and tower-based Enterprise
  Integration Patterns. It persists no message state.
- **autumn-harvest** is a Postgres-backed, event-sourced durable workflow
  engine. It provides workflows, activities, signals, timers and idempotent
  start.

The glue lives here, not in either upstream. That keeps each project's
dependencies, MSRV and release cadence its own. This crate tracks the
camel ↔ harvest compatibility matrix.

| harvest-camel | autumn-harvest | rust-camel | MSRV |
|---------------|----------------|------------|------|
| 0.1           | 0.6            | 0.55       | 1.89 |

## Layering

```text
             ┌────────────────────── camel (stateless, in-memory) ──────────────────────┐
  broker ──► │ kafka/jms/sql consumer ─► route (filter, transform, …) ─► to("harvest:s") │
             └──────────────────────────────────────────────────────────┬───────────────┘
                   ▲ commit only if the producer returned Ok             │ InboundMessage + oneshot
                   │                                                     ▼
             ┌─────┴──────────────── harvest (durable, Postgres) ──────────────────────────┐
             │ CamelSource: EventSource ─► ConnectorRuntime ─► start / signal-with-start   │
             │   ack ⇒ producer Ok        abandon ⇒ producer Err    (idempotent by coords)  │
             │                                                                             │
             │ workflow ─► activity ─► call_route("direct:x") ─────────────┐               │
             └─────────────────────────────────────────────────────────────┼───────────────┘
                                                                           ▼
                                          camel route ─► http / jms / kafka producer ─► world
```

The durability boundaries are the `harvest:` producer, the activity call, and
the external-activity handoff (`harvest-external:` out, `harvest-complete:` /
`harvest-fail:` back). Nothing between them is durable. A camel `Exchange` cannot be
serialized, and an in-flight pipeline dies with the process. So don't wrap a
camel pipeline in a tower `Layer` to try to make it durable.

## Inbound: camel → harvest

```rust,ignore
use std::sync::Arc;
use autumn_harvest_plugin::connector::{MappedMessage, SourceBinding};
use camel_builder::{RouteBuilder, StepAccumulator};
use harvest_camel::{HarvestBridge, check_route};

let (source, component) = HarvestBridge::new("orders");

// harvest side: the binding's stream must equal the bridge's stream.
let plugin = HarvestPlugin::new()
    .workflows(workflows![fulfil_order])
    .connector(
        SourceBinding::starts("orders", "orders", "fulfil_order")
            .map_json(|_ctx, order: Order| Ok::<_, String>(MappedMessage::new(order.id.clone(), json!(order)))),
        source,
    );

// camel side
let mut camel = CamelContext::builder().supervision(supervision).build().await?;
camel.register_component(component);
let route = RouteBuilder::from("kafka:orders.raw?brokers=…&groupId=orders")
    .route_id("orders-in")
    .to("harvest:orders")
    .build()?;
check_route(&route)?;              // refuse error handlers that would swallow an abandon
camel.add_route_definition(route).await?;
camel.start().await?;
```

The `harvest:` producer hands the message to harvest's connector runtime.
Then it **waits**:

| harvest outcome                                           | producer returns | camel consumer             |
|-----------------------------------------------------------|------------------|----------------------------|
| started / signalled / idempotent replay / throttle-parked | `Ok`             | commits                    |
| dead-lettered to `harvest_connector_dead_letters`         | `Ok`             | commits (harvest holds it) |
| transient failure → `abandon`                             | `Err`            | no commit → redelivery     |
| no settlement within `ackTimeout`                         | `Err`            | no commit → redelivery     |
| `CamelSource` dropped or closed                           | `Err`            | no commit → redelivery     |

### Endpoint options

`harvest:<stream>?idHeader=<name>&ackTimeout=<ms>&maxBodySize=<bytes>`

| Option        | Default         | Meaning |
|---------------|-----------------|---------|
| `idHeader`    | none            | Header holding a redelivery-stable message id. |
| `ackTimeout`  | `30000`         | Milliseconds to wait for harvest to settle the message. |
| `maxBodySize` | `2097152` (2 MiB, harvest's input cap) | Bodies larger than this fail the exchange. |

The path must equal the bridge's stream. Create one `HarvestBridge` per
stream. The channel between the producers and the source is bounded
(`BridgeConfig::channel_capacity`, default 64). When harvest falls behind,
`harvest:` producers wait, and that wait is what pushes back on the camel
consumer.

### Dedupe: coordinates must be stable across redelivery

harvest derives each message's idempotency key from its coordinates. The
whole exactly-once-start guarantee rests on those coordinates being the same
every time the broker redelivers.

1. If the endpoint has `idHeader=Name`, the coordinates are
   `Opaque { stream, id: header(Name) }`.
2. Otherwise, if camel's Kafka consumer headers `CamelKafkaTopic`,
   `CamelKafkaPartition` and `CamelKafkaOffset` are present, the
   coordinates are `KafkaOffset { topic, partition, offset }`.
3. Otherwise the exchange **fails** with a `ValidationError`.

The bridge never falls back to `exchange.correlation_id()`, because that id is
new on every redelivery and would silently defeat dedupe.

The payload is `Body::into_bytes(maxBodySize)`. The body is put back on the
exchange as bytes, so later route steps can still read it. Scalar headers
(string, number and bool) are copied as strings into
`InboundMessage.headers`. `CamelKafkaKey` becomes `InboundMessage.key`.

Each message gets an opaque handle with no partition, so harvest calls
`ack`/`abandon` once per message and never `commit_position`. Commit ordering
stays with camel's consumer.

### Required route configuration

camel's `send_and_wait` returns whatever the route pipeline returns. Its
Kafka, SQL and JMS consumers commit on `Ok`. A route error handler that
**absorbs** the `harvest:` producer's `Err` turns an abandoned message into a
commit, and the message is lost. `tests/camel_error_handling.rs` measures
this against real camel routes:

| Route error handler                                         | abandon reaches the consumer as | Safe |
|-------------------------------------------------------------|---------------------------------|------|
| none                                                        | `Err`                           | ✅ |
| `ErrorHandlerConfig::log_only()`                            | `Err`                           | ✅ |
| policy with `.retry(n)` (propagates after exhaustion)       | `Err`                           | ✅ |
| DLC **with** explicit policies, error unmatched             | `Err` (after the DLC)           | ✅ |
| `dead_letter_channel(uri)` with **no** policies             | `Ok` (implicit catch-all `Handled`) | ❌ |
| policy matching the error with `.handled(true)` / `.continued(true)` | `Ok`                   | ❌ |

- Call **`check_route(&route)`** before `add_route_definition`. It rejects the
  unsafe shapes.
- A context-wide handler set with `CamelContext::set_error_handler` cannot be
  inspected from a route. Check it with `check_error_handler`, or give
  harvest-bound routes their own handler.
- To keep a catch-all DLC for other errors, exclude the bridge's errors from
  it:

  ```rust,ignore
  ErrorHandlerConfig::dead_letter_channel("direct:dlq")
      .on_exception(|e| !harvest_camel::is_unsettled(e))
      .handled(true)
      .build()
  ```

Also:

- **Configure camel supervision**
  (`CamelContext::builder().supervision(..)`). On `Err`, camel's Kafka
  consumer stops. Without supervision it is not restarted, and nothing is
  redelivered.
- **Don't put an aggregator before `harvest:`** in a Kafka route. A pending
  aggregate returns `Ok` to the consumer right away, so the offset is committed
  before harvest holds anything.
- **Don't run harvest's built-in Kafka connector and camel's Kafka consumer on
  the same topic.**

## Outbound: activity → camel

```rust,ignore
use autumn_harvest::failure::ActivityFailure;
use harvest_camel::{CamelHandle, call_route_from_activity};

// startup: resolve every endpoint activities may call, then share the handle
let handle = CamelHandle::new();
handle.resolve(&camel, "direct:charge-card")?;
let plugin = HarvestPlugin::new().state(handle.clone()) /* … */;

#[activity(start_to_close = "30s", retry = RetryPolicy::exponential(5, Duration::from_secs(1)))]
async fn charge_card(ctx: &ActivityContext, input: Value) -> Result<Value, ActivityFailure> {
    call_route_from_activity(ctx, "direct:charge-card", input).await
}
```

- `call_route(&handle, uri, input, headers)` is the lower-level form. It sends
  `input` as a JSON body and returns the reply body as JSON. A JSON body is
  returned as-is, text or bytes are parsed as JSON (falling back to a string),
  and an empty body becomes `null`.
- `call_route_from_activity` adds the following headers:
  - `HarvestIdempotencyKey`: `ctx.idempotency_key()`, stable across retries,
    so the route or the system behind it can dedupe;
  - `HarvestWorkflowId`;
  - `HarvestActivityType`;
  - `HarvestActivityAttempt`.
- Declare the activity as returning `Result<T, ActivityFailure>`. harvest's
  macro honours the retryable flag only for that exact return type.

### Retry rules

- **Only activities call camel. Workflow code never does.** Workflow code is
  replayed. An activity's result is recorded once.
- **Disable camel redelivery on routes that activities call** (no `.retry(n)`).
  harvest already retries the activity under its `RetryPolicy`. Camel
  redelivery inside each attempt multiplies the attempts, and it can outlast
  `start_to_close`.
- `CamelError` is mapped to `ActivityFailure` (`error_type = "camel.<Variant>"`)
  as follows:

| Retryable (transient) | Non-retryable (the same input will fail again) |
|---|---|
| `Io`, `ChannelClosed`, `ConsumerStopping`, `CircuitOpen`, `AuthProviderUnavailable`, `RouteError`, `DeadLetterChannelFailed`, `TemplateReload`, `ProcessorError*`, any future variant | `ValidationError`, `TypeConversionFailed`, `InvalidUri`, `EndpointUri`, `Config`, `ConfigValidation`, `ComponentNotFound`, `EndpointCreationFailed*`, `Unauthenticated`, `Unauthorized`, `UnsupportedMediaType`, `NotAcceptable`, `StreamLimitExceeded`, `AlreadyConsumed` |
| `HttpOperationFailed` 5xx, 408, 425, 429 | `HttpOperationFailed` other 4xx (status and body in `details`) |

`ProcessorError` is camel's catch-all, so it is treated as retryable. A
deterministic failure therefore fails only after harvest's retry policy is
exhausted.

## Request/reply: external activities

Some replies arrive asynchronously, over JMS, Kafka or a webhook, minutes or
days after the request was sent. Make the workflow wait on harvest's **external
activity** and let camel carry the request out and the reply back:

```rust,ignore
#[workflow]
async fn charge_order(ctx: &WorkflowContext, order: Value) -> Result<Value, String> {
    // Parks durably; no worker slot is held while waiting.
    let receipt = ctx
        .execute_activity_external("charge_card", order, "default", 24 * 3600)
        .await
        .map_err(|e| e.to_string())?;
    Ok(receipt)
}
```

```rust,ignore
let mut conn = pool.get().await?;
harvest_camel::external::migrate(&mut conn).await?;   // claim table, idempotent
harvest_camel::ExternalTasks::new(pool.clone()).register(&mut camel);

// out: each pending charge_card handoff becomes one exchange
RouteBuilder::from("harvest-external:charge_card")
    .to("jms:queue:charge-requests")      // the request carries HarvestExternalToken
    .build()?;
// back: the payment system echoes the token on its reply
RouteBuilder::from("jms:queue:charge-replies")
    .to("harvest-complete:charge_card")   // or harvest-fail:
    .build()?;
```

Harvest never pushes an external activity's token anywhere. It records a
PENDING handoff and waits. So `harvest-external:<activity>` **polls** for
PENDING handoffs of that activity and handles each one as follows:

1. It **claims** the token in `harvest_camel_external_claims`, a table this
   crate owns, with a lease. Only one consumer can hold a token, across any
   number of app instances.
2. It sends an exchange into the route. The body is the activity input, and
   these headers are set:

   | Header | Value |
   |---|---|
   | `HarvestExternalToken` | the token |
   | `HarvestWorkflowId`, `HarvestWorkflowName`, `HarvestActivityName` | the waiting workflow and activity |
   | `HarvestExternalDeadline` | RFC 3339 schedule-to-close deadline |
   | `HarvestDispatchAttempt` | 1-based dispatch count |

3. If the route returns `Ok`, the claim is marked **dispatched** and the token
   is never sent again. If the route returns `Err`, the claim is released and
   the token is dispatched again after `retryDelay`.

Dispatch is **at least once**. If a process dies after the route returned but
before the claim was marked dispatched, the token is dispatched again once the
lease lapses. The same happens if the route takes longer than `lease`. The
token is stable, so downstream systems should dedupe on it.

A request that is lost after dispatch is never retried by this crate.
Harvest's own `schedule_to_close` deadline fails the activity instead.

| `harvest-external:` option | Default | Meaning |
|---|---|---|
| `pollInterval` | `1000` | ms between polls when idle |
| `batchSize` | `16` | handoffs claimed per poll |
| `lease` | `60000` | ms a claim is held while the route runs; must exceed the route's worst-case latency |
| `retryDelay` | `5000` | ms before a token whose route failed is dispatched again |
| `claimRetention` | 7 days | dispatched claims older than this are pruned |

On the reply side, `harvest-complete:<label>` reads the `HarvestExternalToken`
header and completes the activity. The body becomes the activity's JSON output:
a JSON body as-is, text or bytes parsed as JSON (falling back to a string), and
an empty body as `null`. `harvest-fail:<label>` fails the activity instead. The
failure message is taken from the `HarvestExternalError` header if present,
otherwise from the body.

| Producer option | Default | Meaning |
|---|---|---|
| `tokenHeader` | `HarvestExternalToken` | header holding the token |
| `onUnknownToken` | `fail` | `fail` the exchange, or `ignore` it (Ok, `HarvestExternalSettled=false`) |
| `maxBodySize` | 2 MiB | `harvest-complete:` only: largest output accepted |
| `retryable` | `false` | `harvest-fail:` only: recorded on the failure; the `HarvestExternalRetryable` header overrides it |

- A token that has already been completed or failed is a no-op, reported as
  `HarvestExternalSettled=false`. Redelivered replies are therefore harmless.
- A reply route consuming from Kafka should still follow the
  [required route configuration](#required-route-configuration). A database
  error while settling returns `Err`, so the reply is redelivered.

Scope and caveats:

- **Single shard.** `ExternalTasks` takes one pool: the database harvest runs on.
- **Payload codecs.** If harvest encrypts or compresses payloads, pass the same
  codecs with `ExternalTasks::with_codecs`.
- **No claim-check inflation.** Inputs that were offloaded to a `PayloadStore`
  are not inflated.
- **Couples to harvest internals.** The consumer reads `harvest_external_tasks`
  and `harvest_workflow_executions`, the same columns as harvest's public
  `list_external_handoffs`. It also reads history through harvest's
  `#[doc(hidden)]` `store::load_history_with_codecs`. Both are pinned by the
  compatibility table above and covered by the e2e tests.

## Payload limits

By default harvest caps activity input and results at 2 MiB, workflow input
at 2 MiB, and signals at 256 KiB. `maxBodySize` defaults to 2 MiB. For
anything larger, use harvest's `PayloadStore` claim check rather than raising
the caps.

## Development

```sh
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test                              # unit + camel-only integration tests
HARVEST_CAMEL_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/harvest_camel_test \
  cargo test --test e2e_postgres --test e2e_external   # applies the harvest schema on first use
```

`e2e_external` runs a real harvest worker: a workflow parks on an external
activity, `harvest-external:` dispatches it to a camel route, and the reply
through `harvest-complete:` resumes the workflow.

Without `HARVEST_CAMEL_TEST_DATABASE_URL`, the Postgres e2e tests print a
notice and pass without running. CI runs them against a `postgres:16` service
container.

Kafka is not a dependency: `autumn-harvest-plugin`'s `connectors` feature pulls
in no broker client. A `kafka-order-flow` example with a crash test is planned.

## License

MIT OR Apache-2.0
