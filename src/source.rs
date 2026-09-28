//! Inbound bridge: the `harvest:` camel component and [`CamelSource`].
//!
//! ```text
//! camel consumer ─► route ─► to("harvest:<stream>")
//!                               │ push (InboundMessage, oneshot::Sender) into a bounded mpsc
//!                               │ then await the oneshot
//!                               ▼
//!                    CamelSource: EventSource  ◄── harvest ConnectorRuntime
//!                      receive() drains the channel
//!                      ack()     → Sender(Acked)     → producer Ok  → camel commits
//!                      abandon() → Sender(Abandoned) → producer Err → no commit → redelivery
//! ```
//!
//! The producer returns `Ok` **only** once harvest has made the dispatch
//! outcome durable. Every other path (abandon, ack timeout, source dropped or
//! closed, missing dedupe id) returns `Err`, so an at-least-once camel consumer
//! never commits a message harvest does not hold.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use autumn_harvest_plugin::connector::{
    ConnectorError, EventSource, InboundMessage, MessageCoordinates, MessageHandle,
};
use camel_api::{Body, BoxProcessor, BoxProcessorExt, CamelError, Exchange, Value};
use camel_component_api::{
    Component, ComponentContext, Consumer, Endpoint, ProducerContext, RuntimeObservability,
    parse_uri,
};
use tokio::sync::{mpsc, oneshot};

/// The camel URI scheme this crate registers.
pub const SCHEME: &str = "harvest";

/// Kafka consumer headers the producer reads coordinates from.
pub const KAFKA_TOPIC: &str = "CamelKafkaTopic";
/// See [`KAFKA_TOPIC`].
pub const KAFKA_PARTITION: &str = "CamelKafkaPartition";
/// See [`KAFKA_TOPIC`].
pub const KAFKA_OFFSET: &str = "CamelKafkaOffset";
/// Copied into [`InboundMessage::key`] when present.
pub const KAFKA_KEY: &str = "CamelKafkaKey";

/// harvest's default activity-input / workflow-input cap (2 MiB).
pub const DEFAULT_MAX_BODY_BYTES: usize = 2 * 1024 * 1024;
/// How long a `harvest:` producer waits for harvest to settle a message.
pub const DEFAULT_ACK_TIMEOUT: Duration = Duration::from_secs(30);
/// Bounded channel capacity between the camel producers and the source.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 64;

/// Prefix of every error message the `harvest:` producer emits for a message
/// harvest did **not** durably accept. See [`is_unsettled`].
pub const UNSETTLED_MARKER: &str = "[harvest-camel:unsettled]";

/// Returns `true` for the errors a `harvest:` producer returns when harvest
/// did not durably accept the message (abandoned, ack timeout, source gone).
///
/// These errors **must reach the camel consumer** as `Err` so it does not
/// commit. A route error handler that marks them handled or continued
/// silently drops the message. Exclude them in your own exception predicates:
///
/// ```
/// # use camel_api::{CamelError, ErrorHandlerConfig};
/// let cfg = ErrorHandlerConfig::dead_letter_channel("log:dlc")
///     .on_exception(|e: &CamelError| !harvest_camel::is_unsettled(e))
///     .handled(true)
///     .build();
/// # let _ = cfg;
/// ```
#[must_use]
pub fn is_unsettled(err: &CamelError) -> bool {
    matches!(err, CamelError::ProcessorError(msg) if msg.starts_with(UNSETTLED_MARKER))
}

fn unsettled(reason: impl fmt::Display) -> CamelError {
    CamelError::ProcessorError(format!("{UNSETTLED_MARKER} {reason}"))
}

/// A sample unsettled error, used to probe route error-handler predicates.
pub(crate) fn sample_unsettled() -> CamelError {
    unsettled("probe")
}

/// Tuning for one [`HarvestBridge`].
#[derive(Debug, Clone, Copy)]
pub struct BridgeConfig {
    /// Capacity of the bounded channel between producers and the source.
    /// When it is full, `harvest:` producers wait: that is the backpressure.
    pub channel_capacity: usize,
    /// Default for the `ackTimeout` endpoint option.
    pub ack_timeout: Duration,
    /// Default for the `maxBodySize` endpoint option.
    pub max_body_bytes: usize,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self {
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            ack_timeout: DEFAULT_ACK_TIMEOUT,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
        }
    }
}

/// Constructs the two halves of one inbound bridge.
///
/// ```no_run
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// use harvest_camel::HarvestBridge;
///
/// let (source, component) = HarvestBridge::new("orders");
/// // harvest side:
/// //   HarvestPlugin::new().connector(SourceBinding::starts("orders", "orders", "fulfil"), source)
/// // camel side:
/// let mut camel = camel_core::CamelContext::builder().build().await?;
/// camel.register_component(component);
/// // route: from("kafka:orders.raw?...").to("harvest:orders")
/// # let _ = source;
/// # Ok(()) }
/// ```
pub struct HarvestBridge;

impl HarvestBridge {
    /// A bridge for `stream` with [`BridgeConfig::default`].
    ///
    /// `stream` must equal the harvest `SourceBinding`'s stream (the plugin
    /// panics at build time otherwise) and the path of every `harvest:` URI
    /// routed to it.
    #[must_use]
    #[allow(clippy::new_ret_no_self)]
    pub fn new(stream: impl Into<String>) -> (Arc<CamelSource>, HarvestComponent) {
        Self::with_config(stream, BridgeConfig::default())
    }

    /// A bridge for `stream` with explicit tuning.
    ///
    /// # Panics
    ///
    /// Panics if `config.channel_capacity` is zero.
    #[must_use]
    pub fn with_config(
        stream: impl Into<String>,
        config: BridgeConfig,
    ) -> (Arc<CamelSource>, HarvestComponent) {
        assert!(config.channel_capacity > 0, "channel_capacity must be > 0");
        let stream = stream.into();
        let (tx, rx) = mpsc::channel(config.channel_capacity);
        let source = Arc::new(CamelSource {
            stream: stream.clone(),
            rx: tokio::sync::Mutex::new(rx),
            pending: Mutex::new(HashMap::new()),
            next_token: AtomicU64::new(0),
        });
        let component = HarvestComponent { stream, tx, config };
        (source, component)
    }
}

/// How harvest settled one message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Settlement {
    Acked,
    Abandoned,
}

struct Delivery {
    message: InboundMessage,
    reply: oneshot::Sender<Settlement>,
}

/// The harvest [`EventSource`] fed by `harvest:` producers.
///
/// Handles are [`MessageHandle::opaque`] (no partition), so the connector
/// runtime acknowledges each message individually through [`Self::ack`] /
/// [`Self::abandon`]; ordering of commits is left to the camel consumer.
///
/// Dropping the source (or calling [`Self::close`]) fails every waiting
/// producer with an [unsettled](is_unsettled) error.
pub struct CamelSource {
    stream: String,
    rx: tokio::sync::Mutex<mpsc::Receiver<Delivery>>,
    pending: Mutex<HashMap<String, oneshot::Sender<Settlement>>>,
    next_token: AtomicU64,
}

impl fmt::Debug for CamelSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CamelSource")
            .field("stream", &self.stream)
            .field("in_flight", &self.in_flight())
            .finish_non_exhaustive()
    }
}

impl CamelSource {
    fn pending(&self) -> std::sync::MutexGuard<'_, HashMap<String, oneshot::Sender<Settlement>>> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Messages handed to harvest and not yet acked or abandoned.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.pending().len()
    }

    /// Stop accepting messages and fail every waiting producer.
    ///
    /// Queued and in-flight messages are answered with an unsettled error, so
    /// their camel consumers do not commit. Further `receive` calls return
    /// [`ConnectorError::Closed`] once the queue is drained.
    pub async fn close(&self) {
        let mut rx = self.rx.lock().await;
        rx.close();
        // Dropping the queued deliveries drops their reply senders.
        while rx.try_recv().is_ok() {}
        drop(rx);
        self.pending().clear();
    }

    fn settle(&self, handle: &MessageHandle, outcome: Settlement) {
        let sender = self.pending().remove(&handle.token);
        match sender {
            Some(tx) => {
                if tx.send(outcome).is_err() {
                    // The producer already gave up (ack timeout) and returned
                    // Err; the camel consumer will redeliver and harvest will
                    // dedupe it against this settlement.
                    tracing::debug!(token = %handle.token, ?outcome, "producer no longer waiting");
                }
            }
            None => tracing::debug!(token = %handle.token, ?outcome, "settle for unknown token"),
        }
    }

    fn track(&self, delivery: Delivery) -> Option<InboundMessage> {
        if delivery.reply.is_closed() {
            // The producer timed out while the message sat in the queue; it
            // has already failed the exchange, so harvest need not see it.
            return None;
        }
        let n = self.next_token.fetch_add(1, Ordering::Relaxed);
        let token = format!("{}#{n}", delivery.message.coordinates.render());
        let mut message = delivery.message;
        message.handle = MessageHandle::opaque(token.clone());
        self.pending().insert(token, delivery.reply);
        Some(message)
    }
}

#[async_trait]
impl EventSource for CamelSource {
    fn stream(&self) -> &str {
        &self.stream
    }

    async fn receive(
        &self,
        max: usize,
        timeout: Duration,
    ) -> Result<Vec<InboundMessage>, ConnectorError> {
        let mut rx = self.rx.lock().await;
        let mut batch = Vec::new();
        if max == 0 {
            return Ok(batch);
        }
        // Wait (bounded) for the first live delivery, then take whatever else
        // is already queued without waiting again.
        let deadline = tokio::time::Instant::now() + timeout;
        while batch.is_empty() {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Err(_elapsed) => return Ok(batch),
                Ok(None) => return Err(ConnectorError::Closed),
                Ok(Some(delivery)) => batch.extend(self.track(delivery)),
            }
        }
        while batch.len() < max {
            match rx.try_recv() {
                Ok(delivery) => batch.extend(self.track(delivery)),
                Err(_) => break,
            }
        }
        drop(rx);
        // Forget producers that gave up on already-received messages.
        self.pending().retain(|_, tx| !tx.is_closed());
        Ok(batch)
    }

    async fn ack(&self, handle: &MessageHandle) -> Result<(), ConnectorError> {
        self.settle(handle, Settlement::Acked);
        Ok(())
    }

    async fn abandon(&self, handle: &MessageHandle) -> Result<(), ConnectorError> {
        self.settle(handle, Settlement::Abandoned);
        Ok(())
    }
}

/// The `harvest:` camel component. Producer-only.
///
/// URI: `harvest:<stream>[?idHeader=<name>&ackTimeout=<ms>&maxBodySize=<bytes>]`
///
/// | Option        | Default    | Meaning |
/// |---------------|------------|---------|
/// | `idHeader`    | (none)     | Header holding a redelivery-stable message id → `Opaque{stream, id}` coordinates. Takes precedence over the Kafka headers. |
/// | `ackTimeout`  | 30000      | Milliseconds to wait for harvest to settle the message before failing the exchange. |
/// | `maxBodySize` | 2097152    | Largest body (bytes) accepted; larger bodies fail the exchange. |
///
/// With no `idHeader`, the `CamelKafkaTopic` / `CamelKafkaPartition` /
/// `CamelKafkaOffset` headers set by camel's Kafka consumer give
/// `KafkaOffset` coordinates. With neither, the exchange fails: the
/// coordinates are harvest's dedupe key and must be stable across redelivery.
#[derive(Clone)]
pub struct HarvestComponent {
    stream: String,
    tx: mpsc::Sender<Delivery>,
    config: BridgeConfig,
}

impl fmt::Debug for HarvestComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HarvestComponent")
            .field("stream", &self.stream)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl HarvestComponent {
    /// The stream this component's endpoints feed.
    #[must_use]
    pub fn stream(&self) -> &str {
        &self.stream
    }
}

#[async_trait]
impl Component for HarvestComponent {
    fn scheme(&self) -> &str {
        SCHEME
    }

    fn create_endpoint(
        &self,
        uri: &str,
        _ctx: &dyn ComponentContext,
    ) -> Result<Box<dyn Endpoint>, CamelError> {
        let options = EndpointOptions::parse(uri, &self.stream, &self.config)?;
        Ok(Box::new(HarvestEndpoint {
            uri: uri.to_string(),
            producer: Producer {
                stream: self.stream.clone(),
                tx: self.tx.clone(),
                options: Arc::new(options),
            },
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EndpointOptions {
    id_header: Option<String>,
    ack_timeout: Duration,
    max_body_bytes: usize,
}

impl EndpointOptions {
    fn parse(uri: &str, stream: &str, defaults: &BridgeConfig) -> Result<Self, CamelError> {
        let parts = parse_uri(uri)?;
        if parts.scheme != SCHEME {
            return Err(CamelError::InvalidUri(format!(
                "expected scheme '{SCHEME}', got '{}'",
                parts.scheme
            )));
        }
        let path = parts.path.trim_start_matches('/');
        if path != stream {
            return Err(CamelError::EndpointCreationFailed(format!(
                "'{SCHEME}:{path}' does not match this bridge's stream '{stream}'; \
                 register one HarvestBridge per stream"
            )));
        }
        let mut options = Self {
            id_header: None,
            ack_timeout: defaults.ack_timeout,
            max_body_bytes: defaults.max_body_bytes,
        };
        for (key, value) in &parts.params {
            match key.as_str() {
                "idHeader" if !value.is_empty() => options.id_header = Some(value.clone()),
                "ackTimeout" => {
                    let ms: u64 = value.parse().map_err(|_| {
                        CamelError::InvalidUri(format!(
                            "ackTimeout must be milliseconds: '{value}'"
                        ))
                    })?;
                    options.ack_timeout = Duration::from_millis(ms);
                }
                "maxBodySize" => {
                    options.max_body_bytes = value.parse().map_err(|_| {
                        CamelError::InvalidUri(format!("maxBodySize must be bytes: '{value}'"))
                    })?;
                }
                other => {
                    return Err(CamelError::InvalidUri(format!(
                        "unknown {SCHEME}: option '{other}' (expected idHeader, ackTimeout, maxBodySize)"
                    )));
                }
            }
        }
        Ok(options)
    }
}

struct HarvestEndpoint {
    uri: String,
    producer: Producer,
}

impl Endpoint for HarvestEndpoint {
    fn uri(&self) -> &str {
        &self.uri
    }

    fn create_consumer(
        &self,
        _rt: Arc<dyn RuntimeObservability>,
    ) -> Result<Box<dyn Consumer>, CamelError> {
        Err(CamelError::EndpointCreationFailed(format!(
            "{SCHEME}: is producer-only; harvest consumes it through CamelSource"
        )))
    }

    fn create_producer(
        &self,
        _rt: Arc<dyn RuntimeObservability>,
        _ctx: &ProducerContext,
    ) -> Result<BoxProcessor, CamelError> {
        let producer = self.producer.clone();
        Ok(BoxProcessor::from_fn(move |exchange| {
            let producer = producer.clone();
            async move { producer.send(exchange).await }
        }))
    }
}

#[derive(Clone)]
struct Producer {
    stream: String,
    tx: mpsc::Sender<Delivery>,
    options: Arc<EndpointOptions>,
}

impl Producer {
    async fn send(&self, mut exchange: Exchange) -> Result<Exchange, CamelError> {
        let coordinates = coordinates(&exchange, &self.stream, self.options.id_header.as_deref())?;
        let key = match exchange.input.header(KAFKA_KEY) {
            Some(Value::String(s)) => Some(s.as_bytes().to_vec()),
            _ => None,
        };
        let headers = string_headers(&exchange.input.headers);

        // Materialize the body once and put the bytes back, so later route
        // steps still see it (a stream body can only be read once).
        let body = std::mem::replace(&mut exchange.input.body, Body::Empty);
        let body = match body {
            Body::Stream(_) | Body::Bytes(_) => {
                let bytes = body.into_bytes(self.options.max_body_bytes).await?;
                exchange.input.body = Body::Bytes(bytes.clone());
                bytes.to_vec()
            }
            other => {
                let bytes = other.clone().into_bytes(self.options.max_body_bytes).await;
                exchange.input.body = other;
                bytes?.to_vec()
            }
        };

        let rendered = coordinates.render();
        let (reply, settled) = oneshot::channel();
        let delivery = Delivery {
            message: InboundMessage {
                coordinates,
                payload: body,
                key,
                headers,
                // Replaced by CamelSource with a unique token on receive.
                handle: MessageHandle::opaque(String::new()),
            },
            reply,
        };

        let wait = async {
            self.tx.send(delivery).await.map_err(|_| {
                unsettled(format!("harvest source for '{}' is closed", self.stream))
            })?;
            settled.await.map_err(|_| {
                unsettled(format!(
                    "harvest source for '{}' dropped message {rendered} before settling it",
                    self.stream
                ))
            })
        };
        let outcome = match tokio::time::timeout(self.options.ack_timeout, wait).await {
            Ok(outcome) => outcome?,
            Err(_) => {
                return Err(unsettled(format!(
                    "harvest did not settle message {rendered} within {:?}",
                    self.options.ack_timeout
                )));
            }
        };
        match outcome {
            Settlement::Acked => Ok(exchange),
            Settlement::Abandoned => Err(unsettled(format!(
                "harvest abandoned message {rendered}; not acknowledging so the source redelivers"
            ))),
        }
    }
}

/// Derive stable coordinates for the exchange, or fail it.
fn coordinates(
    exchange: &Exchange,
    stream: &str,
    id_header: Option<&str>,
) -> Result<MessageCoordinates, CamelError> {
    let msg = &exchange.input;
    if let Some(name) = id_header {
        let id = msg.header(name).and_then(scalar_string).ok_or_else(|| {
            CamelError::ValidationError(format!(
                "{SCHEME}:{stream} requires header '{name}' (idHeader) as a stable message id"
            ))
        })?;
        if id.is_empty() {
            return Err(CamelError::ValidationError(format!(
                "{SCHEME}:{stream} header '{name}' (idHeader) is empty"
            )));
        }
        return Ok(MessageCoordinates::Opaque {
            stream: stream.to_string(),
            id,
        });
    }

    let topic = msg.header(KAFKA_TOPIC).and_then(Value::as_str);
    let partition = msg.header(KAFKA_PARTITION).and_then(as_i64);
    let offset = msg.header(KAFKA_OFFSET).and_then(as_i64);
    match (topic, partition, offset) {
        (Some(topic), Some(partition), Some(offset)) => {
            let partition = i32::try_from(partition).map_err(|_| {
                CamelError::ValidationError(format!("{KAFKA_PARTITION} out of range: {partition}"))
            })?;
            Ok(MessageCoordinates::KafkaOffset {
                topic: topic.to_string(),
                partition,
                offset,
            })
        }
        _ => Err(CamelError::ValidationError(format!(
            "{SCHEME}:{stream} cannot derive a redelivery-stable message id: the exchange has no \
             {KAFKA_TOPIC}/{KAFKA_PARTITION}/{KAFKA_OFFSET} headers and the endpoint sets no \
             idHeader option (e.g. {SCHEME}:{stream}?idHeader=MessageId). Without one, harvest \
             could not dedupe a redelivery"
        ))),
    }
}

fn as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn scalar_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Scalar headers (string, number, bool) rendered as strings; objects, arrays
/// and nulls are skipped.
fn string_headers(headers: &HashMap<String, Value>) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(k, v)| scalar_string(v).map(|s| (k.clone(), s)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use camel_api::Message;
    use camel_component_api::NoOpComponentContext;
    use tower::ServiceExt;

    fn producer(component: &HarvestComponent, uri: &str) -> BoxProcessor {
        let endpoint = component
            .create_endpoint(uri, &NoOpComponentContext)
            .expect("endpoint");
        endpoint
            .create_producer(Arc::new(NoOpComponentContext), &ProducerContext::new())
            .expect("producer")
    }

    fn kafka_exchange(offset: i64, body: &str) -> Exchange {
        let mut msg = Message::new(body);
        msg.set_header(KAFKA_TOPIC, "orders.raw");
        msg.set_header(KAFKA_PARTITION, 3);
        msg.set_header(KAFKA_OFFSET, offset);
        msg.set_header(KAFKA_KEY, "tenant-7");
        msg.set_header("traceparent", "00-abc");
        msg.set_header("nested", serde_json::json!({"a": 1}));
        Exchange::new(msg)
    }

    fn id_exchange(id: &str) -> Exchange {
        let mut msg = Message::new(r#"{"order_id":"o-1"}"#);
        msg.set_header("MessageId", id);
        Exchange::new(msg)
    }

    async fn receive_one(source: &CamelSource) -> InboundMessage {
        let mut batch = source
            .receive(16, Duration::from_secs(5))
            .await
            .expect("receive");
        assert_eq!(batch.len(), 1, "expected exactly one message");
        batch.remove(0)
    }

    #[tokio::test]
    async fn ack_returns_ok_to_the_producer() {
        let (source, component) = HarvestBridge::new("orders");
        let p = producer(&component, "harvest:orders");
        let call = tokio::spawn(p.oneshot(kafka_exchange(42, r#"{"x":1}"#)));

        let msg = receive_one(&source).await;
        assert_eq!(msg.payload, br#"{"x":1}"#);
        assert_eq!(source.in_flight(), 1);
        source.ack(&msg.handle).await.unwrap();

        let exchange = call.await.unwrap().expect("acked exchange is Ok");
        assert_eq!(exchange.input.body.as_text(), Some(r#"{"x":1}"#));
        assert_eq!(source.in_flight(), 0);
    }

    #[tokio::test]
    async fn abandon_returns_unsettled_err() {
        let (source, component) = HarvestBridge::new("orders");
        let p = producer(&component, "harvest:orders");
        let call = tokio::spawn(p.oneshot(kafka_exchange(42, "{}")));

        let msg = receive_one(&source).await;
        source.abandon(&msg.handle).await.unwrap();

        let err = call
            .await
            .unwrap()
            .expect_err("abandon must fail the exchange");
        assert!(is_unsettled(&err), "{err}");
        assert!(err.to_string().contains("abandoned"), "{err}");
    }

    #[tokio::test]
    async fn dropped_source_fails_waiting_producers() {
        let (source, component) = HarvestBridge::new("orders");
        let p = producer(&component, "harvest:orders");
        let received = tokio::spawn(p.clone().oneshot(kafka_exchange(1, "{}")));
        let _ = receive_one(&source).await;
        let queued = tokio::spawn(p.clone().oneshot(kafka_exchange(2, "{}")));
        tokio::task::yield_now().await;

        drop(source);

        for call in [received, queued] {
            let err = call.await.unwrap().expect_err("dropped source must fail");
            assert!(is_unsettled(&err), "{err}");
        }
        // And a producer called after the source is gone fails at once.
        let err = p.oneshot(kafka_exchange(3, "{}")).await.unwrap_err();
        assert!(is_unsettled(&err), "{err}");
    }

    #[tokio::test]
    async fn close_fails_waiting_producers_and_ends_receive() {
        let (source, component) = HarvestBridge::new("orders");
        let p = producer(&component, "harvest:orders");
        let received = tokio::spawn(p.clone().oneshot(kafka_exchange(1, "{}")));
        let _ = receive_one(&source).await;

        source.close().await;

        let err = received.await.unwrap().unwrap_err();
        assert!(is_unsettled(&err), "{err}");
        assert!(matches!(
            source.receive(1, Duration::from_millis(10)).await,
            Err(ConnectorError::Closed)
        ));
    }

    #[tokio::test]
    async fn ack_timeout_returns_err_and_late_ack_is_harmless() {
        let (source, component) = HarvestBridge::new("orders");
        let p = producer(&component, "harvest:orders?ackTimeout=50");
        let call = tokio::spawn(p.oneshot(kafka_exchange(9, "{}")));

        let msg = receive_one(&source).await;
        let err = call
            .await
            .unwrap()
            .expect_err("timeout must fail the exchange");
        assert!(is_unsettled(&err), "{err}");
        assert!(err.to_string().contains("did not settle"), "{err}");

        // The runtime settles later; that must not panic or error.
        source.ack(&msg.handle).await.unwrap();
        assert_eq!(source.in_flight(), 0);
    }

    #[tokio::test]
    async fn timed_out_queued_message_is_not_handed_to_harvest() {
        let (source, component) = HarvestBridge::new("orders");
        let p = producer(&component, "harvest:orders?ackTimeout=20");
        let err = p.oneshot(kafka_exchange(9, "{}")).await.unwrap_err();
        assert!(is_unsettled(&err));

        let batch = source.receive(8, Duration::from_millis(50)).await.unwrap();
        assert!(
            batch.is_empty(),
            "a message its producer gave up on is skipped"
        );
    }

    #[tokio::test]
    async fn kafka_headers_give_kafka_offset_coordinates() {
        let (source, component) = HarvestBridge::new("orders");
        let p = producer(&component, "harvest:orders");
        let call = tokio::spawn(p.oneshot(kafka_exchange(42, "{}")));

        let msg = receive_one(&source).await;
        assert_eq!(
            msg.coordinates,
            MessageCoordinates::KafkaOffset {
                topic: "orders.raw".into(),
                partition: 3,
                offset: 42
            }
        );
        assert_eq!(msg.key.as_deref(), Some(&b"tenant-7"[..]));
        assert_eq!(
            msg.headers.get("traceparent").map(String::as_str),
            Some("00-abc")
        );
        assert_eq!(
            msg.headers.get(KAFKA_OFFSET).map(String::as_str),
            Some("42")
        );
        assert!(
            !msg.headers.contains_key("nested"),
            "non-scalar headers are skipped"
        );
        assert_eq!(
            msg.handle.partition, None,
            "handles are opaque: per-message ack"
        );
        source.ack(&msg.handle).await.unwrap();
        call.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn id_header_gives_opaque_coordinates_and_wins_over_kafka() {
        let (source, component) = HarvestBridge::new("orders");
        let p = producer(&component, "harvest:orders?idHeader=MessageId");
        let mut exchange = kafka_exchange(42, "{}");
        exchange.input.set_header("MessageId", "m-17");
        let call = tokio::spawn(p.oneshot(exchange));

        let msg = receive_one(&source).await;
        assert_eq!(
            msg.coordinates,
            MessageCoordinates::Opaque {
                stream: "orders".into(),
                id: "m-17".into()
            }
        );
        source.ack(&msg.handle).await.unwrap();
        call.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn redelivery_keeps_coordinates_but_gets_a_fresh_token() {
        let (source, component) = HarvestBridge::new("orders");
        let p = producer(&component, "harvest:orders?idHeader=MessageId");
        let a = tokio::spawn(p.clone().oneshot(id_exchange("m-1")));
        let b = tokio::spawn(p.oneshot(id_exchange("m-1")));

        let batch = source.receive(8, Duration::from_secs(5)).await.unwrap();
        let batch = if batch.len() == 2 {
            batch
        } else {
            let mut all = batch;
            all.extend(source.receive(8, Duration::from_secs(5)).await.unwrap());
            all
        };
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].coordinates, batch[1].coordinates);
        assert_ne!(batch[0].handle.token, batch[1].handle.token);
        for m in &batch {
            source.ack(&m.handle).await.unwrap();
        }
        a.await.unwrap().unwrap();
        b.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn missing_id_is_a_clear_error_and_nothing_is_queued() {
        let (source, component) = HarvestBridge::new("orders");

        let err = producer(&component, "harvest:orders")
            .oneshot(Exchange::new(Message::new("{}")))
            .await
            .unwrap_err();
        assert!(matches!(err, CamelError::ValidationError(_)), "{err}");
        assert!(err.to_string().contains("idHeader"), "{err}");
        assert!(!is_unsettled(&err));

        let err = producer(&component, "harvest:orders?idHeader=MessageId")
            .oneshot(Exchange::new(Message::new("{}")))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("'MessageId'"), "{err}");

        // Never uses the per-attempt correlation id as a fallback.
        let batch = source.receive(8, Duration::from_millis(20)).await.unwrap();
        assert!(batch.is_empty());
    }

    #[tokio::test]
    async fn oversized_body_fails_before_reaching_harvest() {
        let (source, component) = HarvestBridge::new("orders");
        let err = producer(
            &component,
            "harvest:orders?idHeader=MessageId&maxBodySize=4",
        )
        .oneshot(id_exchange("m-1"))
        .await
        .unwrap_err();
        assert!(matches!(err, CamelError::StreamLimitExceeded(4)), "{err}");
        assert!(
            source
                .receive(8, Duration::from_millis(20))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn json_body_is_serialized_and_kept_on_the_exchange() {
        let (source, component) = HarvestBridge::new("orders");
        let mut msg = Message::new(serde_json::json!({"order_id": "o-9"}));
        msg.set_header("MessageId", 5);
        let call = tokio::spawn(
            producer(&component, "harvest:orders?idHeader=MessageId").oneshot(Exchange::new(msg)),
        );
        let m = receive_one(&source).await;
        assert_eq!(
            serde_json::from_slice::<Value>(&m.payload).unwrap(),
            serde_json::json!({"order_id": "o-9"})
        );
        assert_eq!(
            m.coordinates,
            MessageCoordinates::Opaque {
                stream: "orders".into(),
                id: "5".into()
            }
        );
        source.ack(&m.handle).await.unwrap();
        let ex = call.await.unwrap().unwrap();
        assert!(matches!(ex.input.body, Body::Json(_)));
    }

    #[tokio::test]
    async fn backpressure_blocks_producers_when_the_channel_is_full() {
        let config = BridgeConfig {
            channel_capacity: 1,
            ..BridgeConfig::default()
        };
        let (source, component) = HarvestBridge::with_config("orders", config);
        let p = producer(&component, "harvest:orders?idHeader=MessageId");

        // First fills the single slot; second must wait for room.
        let first = tokio::spawn(p.clone().oneshot(id_exchange("m-1")));
        while component.tx.capacity() > 0 {
            tokio::task::yield_now().await;
        }
        let mut second = tokio::spawn(p.oneshot(id_exchange("m-2")));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut second)
                .await
                .is_err(),
            "second producer must block while the channel is full"
        );
        assert_eq!(component.tx.capacity(), 0, "the channel is full");

        // Draining the channel admits the second message.
        let mut seen = Vec::new();
        while seen.len() < 2 {
            let batch = source.receive(8, Duration::from_secs(5)).await.unwrap();
            for m in batch {
                source.ack(&m.handle).await.unwrap();
                seen.push(m.coordinates.render());
            }
        }
        assert_eq!(seen, ["orders:m-1", "orders:m-2"]);
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn receive_respects_max_and_timeout() {
        let (source, component) = HarvestBridge::new("orders");
        let p = producer(&component, "harvest:orders?idHeader=MessageId");
        let started = tokio::time::Instant::now();
        assert!(
            source
                .receive(4, Duration::from_millis(30))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(started.elapsed() >= Duration::from_millis(30));

        let calls: Vec<_> = (0..3)
            .map(|i| tokio::spawn(p.clone().oneshot(id_exchange(&format!("m-{i}")))))
            .collect();
        while source.rx.lock().await.len() < 3 {
            tokio::task::yield_now().await;
        }
        let first = source.receive(2, Duration::from_secs(1)).await.unwrap();
        assert_eq!(first.len(), 2);
        let rest = source.receive(2, Duration::from_secs(1)).await.unwrap();
        assert_eq!(rest.len(), 1);
        for m in first.iter().chain(&rest) {
            source.ack(&m.handle).await.unwrap();
        }
        for c in calls {
            c.await.unwrap().unwrap();
        }
    }

    #[test]
    fn endpoint_options_are_validated() {
        let (_source, component) = HarvestBridge::new("orders");
        let ok = |uri: &str| {
            component
                .create_endpoint(uri, &NoOpComponentContext)
                .is_ok()
        };
        assert!(ok("harvest:orders"));
        assert!(ok(
            "harvest:orders?idHeader=Id&ackTimeout=10&maxBodySize=100"
        ));
        assert!(!ok("harvest:other"), "stream mismatch");
        assert!(!ok("harvest:orders?bogus=1"), "unknown option");
        assert!(!ok("harvest:orders?ackTimeout=soon"), "bad timeout");

        let opts =
            EndpointOptions::parse("harvest:orders?ackTimeout=10", "orders", &component.config)
                .unwrap();
        assert_eq!(opts.ack_timeout, Duration::from_millis(10));
        assert_eq!(opts.max_body_bytes, DEFAULT_MAX_BODY_BYTES);
        assert_eq!(opts.id_header, None);
    }

    #[test]
    fn harvest_endpoint_has_no_consumer() {
        let (_source, component) = HarvestBridge::new("orders");
        let ep = component
            .create_endpoint("harvest:orders", &NoOpComponentContext)
            .unwrap();
        assert!(ep.create_consumer(Arc::new(NoOpComponentContext)).is_err());
    }
}
