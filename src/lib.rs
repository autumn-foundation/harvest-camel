//! Bridge [rust-camel](https://crates.io/crates/camel-core) and
//! [autumn-harvest](https://crates.io/crates/autumn-harvest):
//! **camel handles the edges, harvest the middle.**
//!
//! camel supplies connector breadth (Kafka, JMS, SQL, HTTP, file, …) and
//! stateless routing; harvest owns everything that must survive a crash. The
//! durability boundaries are exactly two:
//!
//! * **Inbound** — the `harvest:` camel producer ([`HarvestComponent`]) feeds
//!   a harvest [`EventSource`](autumn_harvest_plugin::connector::EventSource)
//!   ([`CamelSource`]). The producer returns `Ok` only after harvest has made
//!   the dispatch durable, so camel's at-least-once consumers commit only
//!   what harvest holds, and harvest dedupes redeliveries by the message's
//!   coordinates.
//! * **Outbound** — activities call camel routes with [`call_route`] /
//!   [`call_route_from_activity`]; harvest owns the retries.
//!
//! Nothing between those two points is durable: a camel `Exchange` is not
//! serializable and an in-flight pipeline dies with the process.
//!
//! See the README for the wiring, the dedupe rules and the required route
//! configuration ([`check_route`]).

mod check;
mod route;
mod source;

pub use check::{RouteCheckError, check_error_handler, check_route};
pub use route::{
    CamelHandle, HEADER_ACTIVITY_TYPE, HEADER_ATTEMPT, HEADER_IDEMPOTENCY_KEY, HEADER_WORKFLOW_ID,
    call_route, call_route_from_activity, is_retryable, to_activity_failure,
};
pub use source::{
    BridgeConfig, CamelSource, DEFAULT_ACK_TIMEOUT, DEFAULT_CHANNEL_CAPACITY,
    DEFAULT_MAX_BODY_BYTES, HarvestBridge, HarvestComponent, KAFKA_KEY, KAFKA_OFFSET,
    KAFKA_PARTITION, KAFKA_TOPIC, SCHEME, UNSETTLED_MARKER, is_unsettled,
};
