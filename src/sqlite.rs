//! Inbound bridge onto a caller-owned SQLite runtime (issue #9).
//!
//! ```text
//! camel route ─► to("harvest:<stream>") ─► CamelSource (EventSource)
//!                                              │
//!   SqliteInbound::receive(timeout)  ◄─────────┘  maps each message, no runtime borrow
//!        │ InboundBatch (Send + 'static)
//!        ▼
//!   InboundBatch::apply(&mut SqliteRuntime)        sync: start or signal-with-start
//!        │ AppliedBatch (Send + 'static)
//!        ▼
//!   SqliteInbound::settle(applied)                 ack, abandon or dead-letter
//! ```
//!
//! The SQLite runtime has one writer. An embedder such as a daemon keeps it
//! in one task, so this bridge splits a pass into three phases. Only
//! [`InboundBatch::apply`] touches the runtime, and it never awaits. The
//! other two phases run in any task. [`SqliteInbound::pump`] runs all three
//! in a row for a caller that owns the runtime directly.
//!
//! The rules match the Postgres
//! [`ConnectorRuntime`](autumn_harvest_plugin::connector::ConnectorRuntime)
//! and the plugin's own SQLite path, because this module calls the same
//! public pieces:
//!
//! - The dedupe key is [`message_idempotency_key_in`].
//! - The poison rules are [`decide_disposition`] and [`PoisonTracker`].
//! - A dead letter goes through a [`DeadLetterSink`]. [`SqliteDeadLetters`]
//!   writes `harvest_connector_dead_letters` in a SQLite file.
//! - A `Starts` target dedupes on the mapped workflow id, under
//!   `AllowDuplicate`. A redelivery attaches to the running execution.
//! - A `SignalsWithStart` target dedupes the signal on the derived key.
//!
//! A message is acknowledged only after [`InboundBatch::apply`] returns, so
//! its write is durable before the camel consumer commits. A dead letter is
//! acknowledged only after the sink write succeeds.
//!
//! Request/reply (`harvest-external:`) is not available here. The SQLite
//! engine refuses external activities (autumn-harvest issue #2607).

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use autumn_harvest::WorkflowIdReusePolicy;
use autumn_harvest_plugin::connector::idempotency::message_idempotency_key_in;
use autumn_harvest_plugin::connector::{
    ConnectorDeadLetter, ConnectorError, ConnectorTarget, DeadLetterOutcome, DeadLetterSink,
    DispatchOutcome, EventSource, IdempotencyMode, InboundMessage, MappedMessage, MappingError,
    MessageCtx, MessageDisposition, PassSummary, PoisonTracker, SourceBinding,
    UnconfiguredDeadLetterSink, broker_native_dead_letter_is_supported, decide_disposition,
    resolve_idempotency_mode,
};
use autumn_harvest_sqlite::{
    NewDeadLetter, SignalWithStart, SqliteAuthStore, SqliteError, SqliteRuntime,
};
use chrono::Utc;

/// Default bound on the messages one [`SqliteInbound::receive`] takes.
pub const DEFAULT_BATCH_SIZE: usize = 64;

/// The longest refusal detail kept in a dead-letter record.
const MAX_DETAIL_BYTES: usize = 1000;

/// Why a binding cannot run on the SQLite runtime.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SqliteInboundError {
    /// The binding and the source name different streams.
    #[error(
        "binding '{binding}' consumes stream '{expected}', but its source is stream '{actual}'"
    )]
    StreamMismatch {
        /// The binding name.
        binding: &'static str,
        /// The binding stream.
        expected: String,
        /// The source stream.
        actual: String,
    },
    /// The binding sets a task queue. One SQLite writer drains every queue.
    #[error(
        "binding '{binding}' sets task queue '{queue}': one SQLite writer drains every queue, \
         so drop SourceBinding::queue"
    )]
    QueueOverride {
        /// The binding name.
        binding: &'static str,
        /// The queue the binding sets.
        queue: &'static str,
    },
    /// A `Starts` binding resolves to broker-coordinate dedupe.
    #[error(
        "binding '{binding}' is a Starts binding in BrokerCoordinates mode: SQLite has no start \
         idempotency table. Map a stable workflow id and set IdempotencyMode::WorkflowId"
    )]
    CoordinateStart {
        /// The binding name.
        binding: &'static str,
    },
    /// Broker-native dead letters on a source that has no dead-letter queue.
    #[error(
        "binding '{binding}' asks for broker-native dead letters, but the source of stream \
         '{stream}' has no dead-letter destination"
    )]
    NoBrokerDeadLetter {
        /// The binding name.
        binding: &'static str,
        /// The stream.
        stream: String,
    },
}

/// Feeds one [`SourceBinding`] from one [`EventSource`] into a
/// [`SqliteRuntime`] that the caller owns.
///
/// Share it with an [`Arc`] between the task that receives and the task
/// that settles. Only [`InboundBatch::apply`] needs the runtime.
pub struct SqliteInbound {
    binding: Arc<SourceBinding>,
    source: Arc<dyn EventSource>,
    sink: Arc<dyn DeadLetterSink>,
    poison: Mutex<PoisonTracker>,
    batch_size: usize,
}

impl fmt::Debug for SqliteInbound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteInbound")
            .field("binding", &self.binding.name)
            .field("stream", &self.binding.stream)
            .field("batch_size", &self.batch_size)
            .finish_non_exhaustive()
    }
}

impl SqliteInbound {
    /// A bridge for `binding` over `source`.
    ///
    /// The dead-letter sink defaults to one that fails every write, so a
    /// poison message stays on the broker until you set a sink with
    /// [`Self::with_dead_letter_sink`].
    ///
    /// # Errors
    ///
    /// Returns a [`SqliteInboundError`] for a binding that the SQLite
    /// runtime cannot honour. The plugin refuses the same bindings.
    pub fn new(
        binding: SourceBinding,
        source: Arc<dyn EventSource>,
    ) -> Result<Self, SqliteInboundError> {
        validate(&binding, source.as_ref())?;
        Ok(Self {
            binding: Arc::new(binding),
            source,
            sink: Arc::new(UnconfiguredDeadLetterSink),
            poison: Mutex::new(PoisonTracker::new()),
            batch_size: DEFAULT_BATCH_SIZE,
        })
    }

    /// Write poison messages to `sink`.
    #[must_use]
    pub fn with_dead_letter_sink(mut self, sink: Arc<dyn DeadLetterSink>) -> Self {
        self.sink = sink;
        self
    }

    /// Take at most `size` messages per [`Self::receive`]. Zero means one.
    #[must_use]
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size.max(1);
        self
    }

    /// The binding this bridge feeds.
    #[must_use]
    pub fn binding(&self) -> &SourceBinding {
        &self.binding
    }

    /// Wait up to `timeout` for messages, then map each one.
    ///
    /// This phase does not touch the runtime. The batch is `Send + 'static`,
    /// so it can cross a channel to the task that owns the runtime.
    ///
    /// # Errors
    ///
    /// Returns the source error, for example [`ConnectorError::Closed`].
    pub async fn receive(&self, timeout: Duration) -> Result<InboundBatch, ConnectorError> {
        let messages = self.source.receive(self.batch_size, timeout).await?;
        let items = messages
            .into_iter()
            .map(|message| Pending::map(&self.binding, message))
            .collect();
        Ok(InboundBatch {
            binding: Arc::clone(&self.binding),
            items,
        })
    }

    /// Acknowledge, abandon or dead-letter each message of `applied`.
    ///
    /// Call it after [`InboundBatch::apply`] returns. It awaits the source
    /// and the dead-letter sink, never the runtime.
    pub async fn settle(&self, applied: AppliedBatch) -> PassSummary {
        let mut summary = PassSummary {
            received: applied.items.len(),
            ..PassSummary::default()
        };
        for item in applied.items {
            match self.settle_one(item).await {
                MessageDisposition::Ack => summary.acked += 1,
                MessageDisposition::Retry => summary.retried += 1,
                MessageDisposition::DeadLetter(_)
                | MessageDisposition::AbandonToBrokerDeadLetter(_) => summary.dead_lettered += 1,
            }
        }
        summary
    }

    /// Receive, apply and settle one batch on a runtime that the caller holds.
    ///
    /// The runtime borrow lasts the whole call, including the wait for the
    /// first message. A caller that shares the runtime calls the three
    /// phases itself.
    ///
    /// # Errors
    ///
    /// Returns the source error of [`Self::receive`].
    pub async fn pump(
        &self,
        rt: &mut SqliteRuntime,
        timeout: Duration,
    ) -> Result<PassSummary, ConnectorError> {
        let batch = self.receive(timeout).await?;
        let applied = batch.apply(rt);
        Ok(self.settle(applied).await)
    }

    fn poison(&self) -> std::sync::MutexGuard<'_, PoisonTracker> {
        self.poison.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The strike count of `key` after this delivery.
    ///
    /// A threshold of zero turns the counter off, so nothing is recorded.
    fn strike(&self, key: &str, outcome: &DispatchOutcome) -> u32 {
        if self.binding.poison_threshold > 0
            && matches!(outcome, DispatchOutcome::MappingRejected(_))
        {
            self.poison().strike(key, std::time::Instant::now())
        } else {
            0
        }
    }

    async fn settle_one(&self, item: Applied) -> MessageDisposition {
        let Applied {
            message,
            key,
            coordinates,
            outcome,
        } = item;
        let strikes = self.strike(&key, &outcome);
        let decided = decide_disposition(
            &outcome,
            strikes,
            self.binding.poison_threshold,
            self.binding.dead_letter_mode,
        );
        let effective = match decided {
            MessageDisposition::Ack => {
                self.ack(&message, &coordinates).await;
                MessageDisposition::Ack
            }
            MessageDisposition::Retry => {
                tracing::warn!(
                    source = self.binding.name,
                    coordinates,
                    outcome = ?outcome,
                    "connector dispatch not durable; leaving message for redelivery"
                );
                self.abandon(&message, &coordinates).await;
                MessageDisposition::Retry
            }
            MessageDisposition::DeadLetter(reason) => {
                let entry = ConnectorDeadLetter {
                    binding: self.binding.name.to_string(),
                    stream: self.binding.stream.clone(),
                    coordinates: coordinates.clone(),
                    idempotency_key: key.clone(),
                    workflow_name: self.binding.target.workflow().to_string(),
                    reason,
                    detail: detail_of(&outcome),
                    // A deterministic poison is quarantined on its first delivery.
                    attempts: i32::try_from(strikes.max(1)).unwrap_or(i32::MAX),
                    payload: message.payload.clone(),
                    failed_at: Utc::now(),
                };
                match self.sink.write(&entry).await {
                    Ok(_) => {
                        tracing::warn!(
                            source = self.binding.name,
                            coordinates,
                            reason = reason.as_str(),
                            "connector message dead-lettered"
                        );
                        // The record is durable, so the ack cannot lose the message.
                        self.ack(&message, &coordinates).await;
                        MessageDisposition::DeadLetter(reason)
                    }
                    Err(err) => {
                        tracing::error!(
                            source = self.binding.name,
                            coordinates,
                            error = %err,
                            "dead-letter write failed; leaving message for redelivery"
                        );
                        self.abandon(&message, &coordinates).await;
                        MessageDisposition::Retry
                    }
                }
            }
            MessageDisposition::AbandonToBrokerDeadLetter(reason) => {
                if let Err(err) = self.source.nack_for_dead_letter(&message.handle).await {
                    tracing::warn!(source = self.binding.name, coordinates, %err, "dead-letter nack failed");
                }
                MessageDisposition::AbandonToBrokerDeadLetter(reason)
            }
        };
        self.forget_strikes(&key, &outcome, &effective);
        effective
    }

    /// Keep the strikes only while they still count toward the threshold.
    ///
    /// A rejection below the threshold keeps them, because they are the
    /// counter. A failed dead-letter write keeps them, so the next delivery
    /// quarantines at once. A transient failure clears them, because the
    /// mapping succeeded and the streak broke. Each terminal disposition
    /// clears them too, as the Postgres runtime does.
    fn forget_strikes(&self, key: &str, outcome: &DispatchOutcome, effective: &MessageDisposition) {
        let keep = matches!(effective, MessageDisposition::Retry)
            && !matches!(outcome, DispatchOutcome::Transient(_));
        let now = std::time::Instant::now();
        match effective {
            MessageDisposition::AbandonToBrokerDeadLetter(_) => {
                self.poison().mark_terminal_as_of(key, now);
            }
            MessageDisposition::DeadLetter(_) => {
                self.poison().mark_sink_terminal_as_of(key, now);
            }
            _ if keep => {}
            _ => self.poison().clear(key),
        }
    }

    async fn ack(&self, message: &InboundMessage, coordinates: &str) {
        if let Err(err) = self.source.ack(&message.handle).await {
            // Best effort, as on Postgres: a redelivery dedupes against the
            // write that is already durable.
            tracing::warn!(source = self.binding.name, coordinates, %err, "ack failed");
        }
    }

    async fn abandon(&self, message: &InboundMessage, coordinates: &str) {
        if let Err(err) = self.source.abandon(&message.handle).await {
            tracing::warn!(source = self.binding.name, coordinates, %err, "abandon failed");
        }
    }
}

fn validate(binding: &SourceBinding, source: &dyn EventSource) -> Result<(), SqliteInboundError> {
    if binding.stream != source.stream() {
        return Err(SqliteInboundError::StreamMismatch {
            binding: binding.name,
            expected: binding.stream.clone(),
            actual: source.stream().to_string(),
        });
    }
    if let Some(queue) = binding.queue {
        return Err(SqliteInboundError::QueueOverride {
            binding: binding.name,
            queue,
        });
    }
    if matches!(binding.target, ConnectorTarget::Starts { .. })
        && resolve_idempotency_mode(binding.target, binding.idempotency_mode, None)
            == IdempotencyMode::BrokerCoordinates
    {
        return Err(SqliteInboundError::CoordinateStart {
            binding: binding.name,
        });
    }
    if !broker_native_dead_letter_is_supported(
        binding.dead_letter_mode,
        source.has_native_dead_letter(),
    ) {
        return Err(SqliteInboundError::NoBrokerDeadLetter {
            binding: binding.name,
            stream: binding.stream.clone(),
        });
    }
    Ok(())
}

/// Received messages, mapped and not yet written. Apply it with
/// [`InboundBatch::apply`].
#[derive(Debug)]
pub struct InboundBatch {
    binding: Arc<SourceBinding>,
    items: Vec<Pending>,
}

impl InboundBatch {
    /// The number of messages in the batch.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether the batch holds no message. A receive that times out gives one.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Write each mapped message into `rt`: a start or a signal-with-start.
    ///
    /// It never awaits, so the task that owns the runtime holds it only for
    /// the writes. Settle the result with [`SqliteInbound::settle`].
    #[must_use]
    pub fn apply(self, rt: &mut SqliteRuntime) -> AppliedBatch {
        let target = self.binding.target;
        let items = self
            .items
            .into_iter()
            .map(|pending| {
                let outcome = match pending.mapped {
                    Ok(mapped) => dispatch(rt, target, &pending.key, mapped),
                    Err(outcome) => outcome,
                };
                Applied {
                    message: pending.message,
                    key: pending.key,
                    coordinates: pending.coordinates,
                    outcome,
                }
            })
            .collect();
        AppliedBatch { items }
    }
}

/// The outcome of each write of a batch. Settle it with
/// [`SqliteInbound::settle`].
#[derive(Debug)]
pub struct AppliedBatch {
    items: Vec<Applied>,
}

impl AppliedBatch {
    /// The dispatch outcome of each message, in receive order.
    pub fn outcomes(&self) -> impl Iterator<Item = &DispatchOutcome> {
        self.items.iter().map(|item| &item.outcome)
    }
}

#[derive(Debug)]
struct Pending {
    message: InboundMessage,
    key: String,
    coordinates: String,
    /// The mapped message, or the outcome that a failed mapping decides.
    mapped: Result<MappedMessage, DispatchOutcome>,
}

impl Pending {
    fn map(binding: &SourceBinding, message: InboundMessage) -> Self {
        let coordinates = message.coordinates.render();
        let key = message_idempotency_key_in(
            binding.name,
            binding.target.signal_name(),
            &coordinates,
            binding.key_incarnation,
        );
        let mapped = map_message(binding, &message, &coordinates);
        Self {
            message,
            key,
            coordinates,
            mapped,
        }
    }
}

/// Run the mapping function, as the Postgres runtime does.
///
/// A panic is deterministic in the bytes, so it is `Malformed`. Then one
/// bad message is quarantined, and the stream does not stall.
fn map_message(
    binding: &SourceBinding,
    message: &InboundMessage,
    coordinates: &str,
) -> Result<MappedMessage, DispatchOutcome> {
    let ctx = MessageCtx::new(binding.name, message);
    // The mapper output is the only state that crosses the boundary.
    let mapping = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (binding.mapper)(&ctx)));
    match mapping {
        Ok(Ok(mapped)) => Ok(mapped),
        Ok(Err(MappingError::Deserialize(m))) => Err(DispatchOutcome::Malformed(m)),
        Ok(Err(MappingError::Rejected(m))) => Err(DispatchOutcome::MappingRejected(m)),
        Err(payload) => {
            let detail = autumn_harvest::error::panic_message(payload);
            tracing::error!(
                source = binding.name,
                coordinates,
                detail = %detail,
                "connector mapping function panicked; quarantining the message"
            );
            Err(DispatchOutcome::Malformed(format!(
                "mapping function panicked: {detail}"
            )))
        }
    }
}

#[derive(Debug)]
struct Applied {
    message: InboundMessage,
    key: String,
    coordinates: String,
    outcome: DispatchOutcome,
}

/// Start or signal one mapped message, under the plugin's SQLite rules.
fn dispatch(
    rt: &mut SqliteRuntime,
    target: ConnectorTarget,
    key: &str,
    mapped: MappedMessage,
) -> DispatchOutcome {
    let workflow_id = mapped.workflow_id.as_str().to_string();
    match target {
        ConnectorTarget::Starts { workflow } => match rt.start_workflow_with_reuse_policy(
            workflow,
            &workflow_id,
            mapped.payload,
            WorkflowIdReusePolicy::AllowDuplicate,
        ) {
            Ok(outcome) if outcome.created => DispatchOutcome::Dispatched {
                execution_id: Some(outcome.exec_id.to_string()),
                workflow_id,
            },
            Ok(outcome) => DispatchOutcome::IdempotentReplay {
                execution_id: Some(outcome.exec_id.to_string()),
                workflow_id,
            },
            Err(err) => classify_error(&err, workflow_id),
        },
        ConnectorTarget::SignalsWithStart {
            workflow,
            signal_name,
        } => match rt.signal_with_start(SignalWithStart {
            workflow_name: workflow,
            workflow_id: &workflow_id,
            input: mapped.payload.clone(),
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            signal_name,
            signal_payload: mapped.payload,
            idempotency_key: Some(key),
        }) {
            Ok(outcome) if outcome.started_fresh || outcome.signal_delivered => {
                DispatchOutcome::Dispatched {
                    execution_id: Some(outcome.exec_id.to_string()),
                    workflow_id,
                }
            }
            Ok(outcome) => DispatchOutcome::IdempotentReplay {
                execution_id: Some(outcome.exec_id.to_string()),
                workflow_id,
            },
            Err(err) => classify_error(&err, workflow_id),
        },
    }
}

/// Classify a runtime refusal as the Postgres connector classifies its HTTP
/// status.
///
/// The status is the one the plugin's SQLite routes answer for the same
/// error. A refusal of this message is `TargetRejected`, so it is poison. A
/// storage error is `Transient`, so the broker redelivers the message.
fn classify_error(err: &SqliteError, workflow_id: String) -> DispatchOutcome {
    let status = match err {
        // The run that this message wants already exists.
        SqliteError::AlreadyExists { .. } => {
            return DispatchOutcome::IdempotentReplay {
                execution_id: None,
                workflow_id,
            };
        }
        SqliteError::UnknownWorkflow(_) | SqliteError::WorkflowIdNotFound { .. } => 404,
        SqliteError::BlankWorkflowId
        | SqliteError::InvalidSignal(_)
        | SqliteError::SignalPayloadInvalid { .. }
        | SqliteError::WorkflowInputInvalid { .. }
        | SqliteError::PayloadTooDeep { .. }
        | SqliteError::InvalidSearchAttribute(_) => 400,
        SqliteError::PayloadTooLarge { .. } => 413,
        _ => return DispatchOutcome::Transient(err.to_string()),
    };
    DispatchOutcome::TargetRejected {
        status,
        detail: truncate(&err.to_string(), MAX_DETAIL_BYTES),
    }
}

fn detail_of(outcome: &DispatchOutcome) -> String {
    match outcome {
        DispatchOutcome::Malformed(m) | DispatchOutcome::MappingRejected(m) => m.clone(),
        DispatchOutcome::TargetRejected { status, detail } => format!("{status}: {detail}"),
        other => format!("{other:?}"),
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &s[..end])
}

/// A [`DeadLetterSink`] that writes `harvest_connector_dead_letters` in a
/// SQLite file.
///
/// The table has the Postgres columns and the same unique idempotency key,
/// so a redelivered poison message is recorded once. The write runs on the
/// blocking pool.
pub struct SqliteDeadLetters {
    store: Arc<SqliteAuthStore>,
}

impl fmt::Debug for SqliteDeadLetters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteDeadLetters").finish_non_exhaustive()
    }
}

impl SqliteDeadLetters {
    /// Write dead letters to the file of `store`.
    ///
    /// Open the store on the Harvest file, as the plugin does, or on a file
    /// of its own.
    #[must_use]
    pub const fn new(store: Arc<SqliteAuthStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl DeadLetterSink for SqliteDeadLetters {
    async fn write(
        &self,
        entry: &ConnectorDeadLetter,
    ) -> Result<DeadLetterOutcome, ConnectorError> {
        let store = Arc::clone(&self.store);
        let entry = entry.clone();
        let inserted = tokio::task::spawn_blocking(move || {
            store.insert_dead_letter(&NewDeadLetter {
                binding: &entry.binding,
                stream: &entry.stream,
                coordinates: &entry.coordinates,
                idempotency_key: &entry.idempotency_key,
                workflow_name: &entry.workflow_name,
                reason: entry.reason.as_str(),
                detail: &entry.detail,
                attempts: entry.attempts,
                payload: &entry.payload,
                failed_at: entry.failed_at,
            })
        })
        .await
        .map_err(|e| ConnectorError::Broker(format!("dead-letter task: {e}")))?
        .map_err(|e| ConnectorError::Broker(format!("dead-letter insert: {e}")))?;
        Ok(if inserted {
            DeadLetterOutcome::Recorded
        } else {
            DeadLetterOutcome::AlreadyRecorded
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_harvest_plugin::connector::MockSource;
    use serde_json::Value;

    fn starts(name: &'static str) -> SourceBinding {
        SourceBinding::starts(name, "s", "w")
            .idempotency_mode(IdempotencyMode::WorkflowId)
            .map_json(|_ctx, body: Value| Ok::<_, &str>(MappedMessage::new("id", body)))
    }

    fn source() -> Arc<dyn EventSource> {
        Arc::new(MockSource::new("s"))
    }

    #[test]
    fn a_coordinate_start_is_refused() {
        let binding = SourceBinding::starts("b", "s", "w")
            .map_json(|_ctx, body: Value| Ok::<_, &str>(MappedMessage::new("id", body)));
        let err = SqliteInbound::new(binding, source()).unwrap_err();
        assert_eq!(err, SqliteInboundError::CoordinateStart { binding: "b" });
        assert!(err.to_string().contains("IdempotencyMode::WorkflowId"));
    }

    #[test]
    fn a_queue_override_is_refused() {
        let err = SqliteInbound::new(starts("b").queue("fast"), source()).unwrap_err();
        assert_eq!(
            err,
            SqliteInboundError::QueueOverride {
                binding: "b",
                queue: "fast"
            }
        );
    }

    #[test]
    fn a_stream_mismatch_is_refused() {
        let other: Arc<dyn EventSource> = Arc::new(MockSource::new("other"));
        let err = SqliteInbound::new(starts("b"), other).unwrap_err();
        assert!(
            matches!(err, SqliteInboundError::StreamMismatch { .. }),
            "{err}"
        );
    }

    #[test]
    fn broker_native_dead_letters_need_a_broker_queue() {
        // A camel bridge has no broker queue to abandon a message into.
        let (camel, _component) = crate::HarvestBridge::new("s");
        let err = SqliteInbound::new(starts("b").broker_native_dead_letter(), camel).unwrap_err();
        assert!(
            matches!(err, SqliteInboundError::NoBrokerDeadLetter { .. }),
            "{err}"
        );
        // A source with one is accepted.
        assert!(SqliteInbound::new(starts("b").broker_native_dead_letter(), source()).is_ok());
    }

    #[test]
    fn a_signal_binding_is_accepted_without_a_mode() {
        let binding = SourceBinding::signals_with_start("b", "s", "w", "msg")
            .map_json(|_ctx, body: Value| Ok::<_, &str>(MappedMessage::new("id", body)));
        let inbound = SqliteInbound::new(binding, source())
            .unwrap()
            .with_batch_size(0);
        assert_eq!(inbound.batch_size, 1, "zero means one");
        assert_eq!(inbound.binding().name, "b");
    }

    #[test]
    fn storage_errors_are_transient_and_refusals_are_poison() {
        let transient = classify_error(&SqliteError::Corrupt("x".to_string()), "id".to_string());
        assert!(
            matches!(transient, DispatchOutcome::Transient(_)),
            "{transient:?}"
        );
        let unknown = classify_error(
            &SqliteError::UnknownWorkflow("w".to_string()),
            "id".to_string(),
        );
        assert!(
            matches!(unknown, DispatchOutcome::TargetRejected { status: 404, .. }),
            "{unknown:?}"
        );
        let blank = classify_error(&SqliteError::BlankWorkflowId, "id".to_string());
        assert!(matches!(
            blank,
            DispatchOutcome::TargetRejected { status: 400, .. }
        ));
    }

    #[test]
    fn truncate_keeps_char_boundaries() {
        assert_eq!(truncate("héllo", 2), "h...");
        assert_eq!(truncate("abc", 5), "abc");
    }

    #[test]
    fn a_panicking_mapper_is_malformed() {
        let binding = SourceBinding::starts("b", "s", "w")
            .idempotency_mode(IdempotencyMode::WorkflowId)
            .map_raw(|_ctx| panic!("boom"));
        let message = InboundMessage {
            coordinates: autumn_harvest_plugin::connector::MessageCoordinates::Opaque {
                stream: "s".to_string(),
                id: "m-1".to_string(),
            },
            payload: b"{}".to_vec(),
            key: None,
            headers: std::collections::BTreeMap::new(),
            handle: autumn_harvest_plugin::connector::MessageHandle::opaque("t"),
        };
        let pending = Pending::map(&binding, message);
        assert!(
            matches!(&pending.mapped, Err(DispatchOutcome::Malformed(m)) if m.contains("boom")),
            "{:?}",
            pending.mapped
        );
        assert!(pending.key.contains('b'));
    }

    #[test]
    fn batches_cross_threads() {
        fn assert_send_static<T: Send + 'static>() {}
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_static::<InboundBatch>();
        assert_send_static::<AppliedBatch>();
        assert_send_sync::<SqliteInbound>();
    }
}
