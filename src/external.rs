//! Durable asynchronous request/reply over harvest's external activities.
//!
//! ```text
//! workflow ── ctx.execute_activity_external("charge_card", input, queue, secs) ──┐ (parks, token issued)
//!                                                                                │
//!   harvest-external:charge_card  (camel consumer)  ◄── polls PENDING handoffs ──┘
//!     claim token in harvest_camel_external_claims (lease)
//!     exchange(body = input, HarvestExternalToken = token) ─► route ─► jms/kafka/http request
//!     route Ok  → claim marked dispatched          route Err → lease released, retried later
//!
//!   reply route: from(jms:replies) ─► to("harvest-complete:charge_card")   (or harvest-fail:)
//!     complete_externally(token, body) → workflow resumes with the reply
//! ```
//!
//! Dispatch is **at least once**: a crash between the route returning and the
//! claim being marked dispatched redispatches the token after the lease
//! expires. The token is stable, so downstream systems dedupe on it, and a
//! second completion of one token is a harmless no-op in harvest.
//!
//! If the request is lost after dispatch, harvest's own `schedule_to_close`
//! deadline fails the activity; the claim is never retried after a successful
//! dispatch.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use autumn_harvest::error::HarvestError;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::external_task;
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::worker::DbPool;
use autumn_harvest::{ExecutionId, ExternalActivityToken};
use camel_api::{Body, BoxProcessor, BoxProcessorExt, CamelError, Exchange, Message, Value};
use camel_component_api::{
    Component, ComponentContext, Consumer, ConsumerContext, Endpoint, ProducerContext,
    RuntimeObservability, parse_uri,
};
use diesel::sql_types::{BigInt, Integer, Text, Timestamptz, Uuid as SqlUuid};
use diesel_async::{AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use tokio_util::sync::CancellationToken;

use crate::route::body_to_json;
use crate::source::DEFAULT_MAX_BODY_BYTES;

/// Scheme of the dispatching consumer.
pub const EXTERNAL_SCHEME: &str = "harvest-external";
/// Scheme of the producer that completes an external activity.
pub const COMPLETE_SCHEME: &str = "harvest-complete";
/// Scheme of the producer that fails an external activity.
pub const FAIL_SCHEME: &str = "harvest-fail";

/// Header carrying the external activity token (UUID string).
pub const HEADER_EXTERNAL_TOKEN: &str = "HarvestExternalToken";
/// Header carrying the owning workflow's name on a dispatched exchange.
pub const HEADER_WORKFLOW_NAME: &str = "HarvestWorkflowName";
/// Header carrying the external activity's name on a dispatched exchange.
pub const HEADER_ACTIVITY_NAME: &str = "HarvestActivityName";
/// Header carrying the activity's schedule-to-close deadline (RFC 3339).
pub const HEADER_EXTERNAL_DEADLINE: &str = "HarvestExternalDeadline";
/// Header carrying how many times this token has been dispatched (1-based).
pub const HEADER_DISPATCH_ATTEMPT: &str = "HarvestDispatchAttempt";
/// Set by `harvest-complete:` / `harvest-fail:`: `true` if this call settled
/// the activity, `false` if it was already terminal (or unknown and ignored).
pub const HEADER_EXTERNAL_SETTLED: &str = "HarvestExternalSettled";
/// Read by `harvest-fail:` as the failure message (falls back to the body).
pub const HEADER_EXTERNAL_ERROR: &str = "HarvestExternalError";
/// Read by `harvest-fail:` to override its `retryable` option.
pub const HEADER_EXTERNAL_RETRYABLE: &str = "HarvestExternalRetryable";

/// The claim table this crate owns. Apply it with [`migrate`].
pub const CLAIMS_TABLE: &str = "harvest_camel_external_claims";

/// Idempotent DDL for [`CLAIMS_TABLE`].
pub const MIGRATION_SQL: &str = "\
CREATE TABLE IF NOT EXISTS harvest_camel_external_claims (
    token UUID PRIMARY KEY,
    activity_name TEXT NOT NULL,
    owner TEXT NOT NULL,
    lease_until TIMESTAMPTZ NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 1,
    dispatched_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_harvest_camel_external_claims_dispatched
    ON harvest_camel_external_claims (dispatched_at)
    WHERE dispatched_at IS NOT NULL;
";

/// Create the claim table if it does not exist. Safe to call on every start.
///
/// # Errors
///
/// The database error, if the DDL fails.
pub async fn migrate(conn: &mut AsyncPgConnection) -> Result<(), diesel::result::Error> {
    conn.batch_execute(MIGRATION_SQL).await
}

/// Delete claims whose token was dispatched more than `older_than` ago.
/// Returns the number of rows removed.
///
/// The `harvest-external:` consumer calls this itself once per
/// `claimRetention`; it is public for operators who run it elsewhere.
///
/// # Errors
///
/// The database error, if the delete fails.
pub async fn prune_claims(
    conn: &mut AsyncPgConnection,
    older_than: Duration,
) -> Result<usize, diesel::result::Error> {
    diesel::sql_query(
        "DELETE FROM harvest_camel_external_claims \
         WHERE dispatched_at IS NOT NULL \
           AND dispatched_at < NOW() - ($1 * INTERVAL '1 millisecond')",
    )
    .bind::<BigInt, _>(millis(older_than))
    .execute(conn)
    .await
}

fn millis(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

/// The three external-activity components, sharing one database pool.
///
/// ```no_run
/// # async fn demo(pool: autumn_harvest::worker::DbPool) -> Result<(), Box<dyn std::error::Error>> {
/// let mut camel = camel_core::CamelContext::builder().build().await?;
/// let mut conn = pool.get().await?;
/// harvest_camel::external::migrate(&mut conn).await?;
/// harvest_camel::ExternalTasks::new(pool).register(&mut camel);
/// // from("harvest-external:charge_card").to("jms:queue:charge-requests")
/// // from("jms:queue:charge-replies").to("harvest-complete:charge_card")
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct ExternalTasks {
    pool: DbPool,
    codecs: PayloadCodecs,
}

impl fmt::Debug for ExternalTasks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExternalTasks").finish_non_exhaustive()
    }
}

impl ExternalTasks {
    /// Components backed by `pool`: the same database harvest uses (single
    /// shard). Payloads are decoded with [`PayloadCodecs::default`].
    #[must_use]
    pub fn new(pool: DbPool) -> Self {
        Self {
            pool,
            codecs: PayloadCodecs::default(),
        }
    }

    /// Decode dispatched inputs with these codecs; pass the same ones harvest
    /// is configured with if payloads are encrypted or compressed.
    #[must_use]
    pub fn with_codecs(mut self, codecs: PayloadCodecs) -> Self {
        self.codecs = codecs;
        self
    }

    /// The `harvest-external:` consumer component.
    #[must_use]
    pub fn consumer_component(&self) -> HarvestExternalComponent {
        HarvestExternalComponent {
            pool: self.pool.clone(),
            codecs: self.codecs.clone(),
        }
    }

    /// The `harvest-complete:` producer component.
    #[must_use]
    pub fn complete_component(&self) -> HarvestSettleComponent {
        HarvestSettleComponent {
            pool: self.pool.clone(),
            kind: SettleKind::Complete,
        }
    }

    /// The `harvest-fail:` producer component.
    #[must_use]
    pub fn fail_component(&self) -> HarvestSettleComponent {
        HarvestSettleComponent {
            pool: self.pool.clone(),
            kind: SettleKind::Fail,
        }
    }

    /// Register all three components on `camel`.
    pub fn register(&self, camel: &mut camel_core::CamelContext) {
        camel.register_component(self.consumer_component());
        camel.register_component(self.complete_component());
        camel.register_component(self.fail_component());
    }
}

fn db_error(context: &str, err: impl fmt::Display) -> CamelError {
    // Io: transient, so a camel consumer upstream does not commit and an
    // activity calling this through call_route is retried.
    CamelError::Io(format!("{context}: {err}"))
}

fn ensure_scheme(
    uri: &str,
    scheme: &str,
) -> Result<camel_component_api::UriComponents, CamelError> {
    let parts = parse_uri(uri)?;
    if parts.scheme != scheme {
        return Err(CamelError::InvalidUri(format!(
            "expected scheme '{scheme}', got '{}'",
            parts.scheme
        )));
    }
    Ok(parts)
}

fn parse_millis(key: &str, value: &str) -> Result<Duration, CamelError> {
    value
        .parse::<u64>()
        .map(Duration::from_millis)
        .map_err(|_| CamelError::InvalidUri(format!("{key} must be milliseconds: '{value}'")))
}

fn unknown_option(scheme: &str, key: &str, expected: &str) -> CamelError {
    CamelError::InvalidUri(format!(
        "unknown {scheme}: option '{key}' (expected {expected})"
    ))
}

// ───────────────────────────── consumer ─────────────────────────────

/// `harvest-external:<activityName>` — dispatches pending external activities
/// named `activityName` into the route.
///
/// | Option           | Default   | Meaning |
/// |------------------|-----------|---------|
/// | `pollInterval`   | `1000`    | Milliseconds between polls when idle. |
/// | `batchSize`      | `16`      | Pending handoffs fetched per poll. |
/// | `lease`          | `60000`   | Milliseconds a claim is held while dispatching; another instance may take the token after it lapses. Must exceed the route's worst-case latency. |
/// | `retryDelay`     | `5000`    | Milliseconds before a token whose route failed is dispatched again. |
/// | `claimRetention` | `604800000` (7 days) | How long dispatched claims are kept before pruning. |
#[derive(Clone)]
pub struct HarvestExternalComponent {
    pool: DbPool,
    codecs: PayloadCodecs,
}

impl fmt::Debug for HarvestExternalComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HarvestExternalComponent")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DispatchOptions {
    activity_name: String,
    poll_interval: Duration,
    batch_size: i64,
    lease: Duration,
    retry_delay: Duration,
    claim_retention: Duration,
}

impl DispatchOptions {
    fn parse(uri: &str) -> Result<Self, CamelError> {
        let parts = ensure_scheme(uri, EXTERNAL_SCHEME)?;
        let activity_name = parts.path.trim_start_matches('/').to_string();
        if activity_name.is_empty() {
            return Err(CamelError::InvalidUri(format!(
                "{EXTERNAL_SCHEME}: needs the external activity name, e.g. {EXTERNAL_SCHEME}:charge_card"
            )));
        }
        let mut options = Self {
            activity_name,
            poll_interval: Duration::from_secs(1),
            batch_size: 16,
            lease: Duration::from_secs(60),
            retry_delay: Duration::from_secs(5),
            claim_retention: Duration::from_secs(7 * 24 * 60 * 60),
        };
        for (key, value) in &parts.params {
            match key.as_str() {
                "pollInterval" => options.poll_interval = parse_millis(key, value)?,
                "lease" => options.lease = parse_millis(key, value)?,
                "retryDelay" => options.retry_delay = parse_millis(key, value)?,
                "claimRetention" => options.claim_retention = parse_millis(key, value)?,
                "batchSize" => {
                    options.batch_size =
                        value
                            .parse::<i64>()
                            .ok()
                            .filter(|n| *n > 0)
                            .ok_or_else(|| {
                                CamelError::InvalidUri(format!(
                                    "batchSize must be a positive integer: '{value}'"
                                ))
                            })?;
                }
                other => {
                    return Err(unknown_option(
                        EXTERNAL_SCHEME,
                        other,
                        "pollInterval, batchSize, lease, retryDelay, claimRetention",
                    ));
                }
            }
        }
        if options.lease.is_zero() {
            return Err(CamelError::InvalidUri("lease must be > 0".to_string()));
        }
        Ok(options)
    }
}

#[async_trait]
impl Component for HarvestExternalComponent {
    fn scheme(&self) -> &str {
        EXTERNAL_SCHEME
    }

    fn create_endpoint(
        &self,
        uri: &str,
        _ctx: &dyn ComponentContext,
    ) -> Result<Box<dyn Endpoint>, CamelError> {
        Ok(Box::new(ExternalEndpoint {
            uri: uri.to_string(),
            dispatcher: Dispatcher {
                pool: self.pool.clone(),
                codecs: self.codecs.clone(),
                options: Arc::new(DispatchOptions::parse(uri)?),
                owner: format!("harvest-camel-{}", uuid::Uuid::new_v4()),
            },
        }))
    }
}

struct ExternalEndpoint {
    uri: String,
    dispatcher: Dispatcher,
}

impl Endpoint for ExternalEndpoint {
    fn uri(&self) -> &str {
        &self.uri
    }

    fn create_consumer(
        &self,
        _rt: Arc<dyn RuntimeObservability>,
    ) -> Result<Box<dyn Consumer>, CamelError> {
        Ok(Box::new(ExternalConsumer {
            dispatcher: self.dispatcher.clone(),
            stop: CancellationToken::new(),
        }))
    }

    fn create_producer(
        &self,
        _rt: Arc<dyn RuntimeObservability>,
        _ctx: &ProducerContext,
    ) -> Result<BoxProcessor, CamelError> {
        Err(CamelError::EndpointCreationFailed(format!(
            "{EXTERNAL_SCHEME}: is consumer-only; reply with {COMPLETE_SCHEME}: or {FAIL_SCHEME}:"
        )))
    }
}

struct ExternalConsumer {
    dispatcher: Dispatcher,
    stop: CancellationToken,
}

#[async_trait]
impl Consumer for ExternalConsumer {
    async fn start(&mut self, context: ConsumerContext) -> Result<(), CamelError> {
        let dispatcher = self.dispatcher.clone();
        let stop = self.stop.clone();
        let mut last_prune: Option<tokio::time::Instant> = None;
        loop {
            if context.is_cancelled() || stop.is_cancelled() {
                return Ok(());
            }
            if last_prune.is_none_or(|t| t.elapsed() >= dispatcher.options.claim_retention) {
                dispatcher.prune().await;
                last_prune = Some(tokio::time::Instant::now());
            }
            let dispatched = match dispatcher.poll_once(&context).await {
                Ok(n) => n,
                Err(err) => {
                    tracing::warn!(activity = %dispatcher.options.activity_name, %err, "external dispatch poll failed");
                    0
                }
            };
            // A full batch suggests more is waiting: poll again at once.
            if usize::try_from(dispatcher.options.batch_size).is_ok_and(|b| dispatched >= b) {
                continue;
            }
            tokio::select! {
                () = context.cancelled() => return Ok(()),
                () = stop.cancelled() => return Ok(()),
                () = tokio::time::sleep(dispatcher.options.poll_interval) => {}
            }
        }
    }

    async fn stop(&mut self) -> Result<(), CamelError> {
        self.stop.cancel();
        Ok(())
    }
}

#[derive(Clone)]
struct Dispatcher {
    pool: DbPool,
    codecs: PayloadCodecs,
    options: Arc<DispatchOptions>,
    owner: String,
}

/// A pending external activity nobody has dispatched or currently leases.
#[derive(diesel::QueryableByName)]
struct Candidate {
    #[diesel(sql_type = SqlUuid)]
    token: uuid::Uuid,
    #[diesel(sql_type = SqlUuid)]
    workflow_exec_id: uuid::Uuid,
    #[diesel(sql_type = Text)]
    workflow_id: String,
    #[diesel(sql_type = Text)]
    workflow_name: String,
    #[diesel(sql_type = Text)]
    activity_name: String,
    #[diesel(sql_type = Timestamptz)]
    deadline_at: chrono::DateTime<chrono::Utc>,
}

impl Candidate {
    fn token(&self) -> ExternalActivityToken {
        ExternalActivityToken::from_uuid(self.token)
    }
}

#[derive(diesel::QueryableByName)]
struct ClaimRow {
    #[diesel(sql_type = Integer)]
    attempts: i32,
}

impl Dispatcher {
    async fn conn(
        &self,
    ) -> Result<
        deadpool::managed::Object<
            diesel_async::pooled_connection::AsyncDieselConnectionManager<AsyncPgConnection>,
        >,
        CamelError,
    > {
        self.pool
            .get()
            .await
            .map_err(|e| db_error("harvest database pool", e))
    }

    async fn prune(&self) {
        let result = async {
            let mut conn = self.conn().await?;
            prune_claims(&mut conn, self.options.claim_retention)
                .await
                .map_err(|e| db_error("prune external claims", e))
        }
        .await;
        match result {
            Ok(0) => {}
            Ok(n) => tracing::debug!(pruned = n, "pruned dispatched external claims"),
            Err(err) => tracing::warn!(%err, "could not prune external claims"),
        }
    }

    /// One poll: fetch pending handoffs, claim and dispatch each. Returns how
    /// many handoffs the poll saw (claimed or not).
    async fn poll_once(&self, context: &ConsumerContext) -> Result<usize, CamelError> {
        let candidates = {
            let mut conn = self.conn().await?;
            // Read-only over harvest's own tables (the same columns its public
            // `list_external_handoffs` reads), joined with our claims so that
            // tokens already dispatched, or leased by another instance, never
            // fill the batch. Harvest's listing has no such filter and no
            // cursor: with `batchSize` requests awaiting replies it would
            // return only those, and new handoffs would starve.
            diesel::sql_query(
                "SELECT t.token, t.workflow_exec_id, e.workflow_id, e.workflow_name, \
                        t.name AS activity_name, t.schedule_to_close_at AS deadline_at \
                 FROM harvest_external_tasks t \
                 JOIN harvest_workflow_executions e ON e.id = t.workflow_exec_id \
                 LEFT JOIN harvest_camel_external_claims c ON c.token = t.token \
                 WHERE t.state = 'PENDING' AND t.name = $1 \
                   AND (c.token IS NULL \
                        OR (c.dispatched_at IS NULL AND c.lease_until < NOW())) \
                 ORDER BY t.schedule_to_close_at ASC, t.token ASC \
                 LIMIT $2",
            )
            .bind::<Text, _>(&self.options.activity_name)
            .bind::<BigInt, _>(self.options.batch_size)
            .load::<Candidate>(&mut *conn)
            .await
            .map_err(|e| db_error("select pending external activities", e))?
        };
        let seen = candidates.len();
        for candidate in candidates {
            if context.is_cancelled() {
                break;
            }
            let token = candidate.token();
            if let Err(err) = self.dispatch_one(context, &candidate).await {
                tracing::warn!(%token, %err, "external dispatch failed");
            }
        }
        Ok(seen)
    }

    async fn dispatch_one(
        &self,
        context: &ConsumerContext,
        handoff: &Candidate,
    ) -> Result<(), CamelError> {
        let token = handoff.token();
        let Some(attempt) = self.claim(token).await? else {
            return Ok(()); // held by another instance, or already dispatched
        };
        let exchange = match self.build_exchange(handoff, attempt).await {
            Ok(exchange) => exchange,
            Err(err) => {
                self.release(token).await;
                return Err(err);
            }
        };
        match context.send_and_wait(exchange).await {
            Ok(_) => self.mark_dispatched(token).await,
            Err(err) => {
                tracing::warn!(%token, %err, "route failed for external activity; will redispatch");
                self.release(token).await;
                Ok(())
            }
        }
    }

    /// Take (or renew a lapsed) lease on `token`. `Some(attempt)` if we hold it.
    async fn claim(&self, token: ExternalActivityToken) -> Result<Option<i32>, CamelError> {
        let mut conn = self.conn().await?;
        let rows = diesel::sql_query(
            "INSERT INTO harvest_camel_external_claims \
                 (token, activity_name, owner, lease_until, attempts) \
             VALUES ($1, $2, $3, NOW() + ($4 * INTERVAL '1 millisecond'), 1) \
             ON CONFLICT (token) DO UPDATE SET \
                 owner = EXCLUDED.owner, \
                 lease_until = EXCLUDED.lease_until, \
                 attempts = harvest_camel_external_claims.attempts + 1 \
             WHERE harvest_camel_external_claims.dispatched_at IS NULL \
               AND harvest_camel_external_claims.lease_until < NOW() \
             RETURNING attempts",
        )
        .bind::<SqlUuid, _>(token.as_uuid())
        .bind::<Text, _>(&self.options.activity_name)
        .bind::<Text, _>(&self.owner)
        .bind::<BigInt, _>(millis(self.options.lease))
        .load::<ClaimRow>(&mut *conn)
        .await
        .map_err(|e| db_error("claim external token", e))?;
        Ok(rows.into_iter().next().map(|r| r.attempts))
    }

    async fn mark_dispatched(&self, token: ExternalActivityToken) -> Result<(), CamelError> {
        let mut conn = self.conn().await?;
        let updated = diesel::sql_query(
            "UPDATE harvest_camel_external_claims SET dispatched_at = NOW() \
             WHERE token = $1 AND owner = $2 AND dispatched_at IS NULL",
        )
        .bind::<SqlUuid, _>(token.as_uuid())
        .bind::<Text, _>(&self.owner)
        .execute(&mut *conn)
        .await
        .map_err(|e| db_error("mark external token dispatched", e))?;
        if updated == 0 {
            // Our lease lapsed mid-dispatch and another instance took the
            // token: it will be dispatched again. Correct (at least once), but
            // a sign `lease` is shorter than the route's latency.
            tracing::warn!(%token, lease = ?self.options.lease, "lease lapsed during dispatch; token may be dispatched twice");
        }
        Ok(())
    }

    /// Let the token be claimed again after `retryDelay`. Best effort: if this
    /// fails, the lease simply lapses on its own.
    async fn release(&self, token: ExternalActivityToken) {
        let result = async {
            let mut conn = self.conn().await?;
            diesel::sql_query(
                "UPDATE harvest_camel_external_claims \
                 SET lease_until = NOW() + ($3 * INTERVAL '1 millisecond') \
                 WHERE token = $1 AND owner = $2 AND dispatched_at IS NULL",
            )
            .bind::<SqlUuid, _>(token.as_uuid())
            .bind::<Text, _>(&self.owner)
            .bind::<BigInt, _>(millis(self.options.retry_delay))
            .execute(&mut *conn)
            .await
            .map_err(|e| db_error("release external token", e))
        }
        .await;
        if let Err(err) = result {
            tracing::warn!(%token, %err, "could not release external claim; it will lapse");
        }
    }

    async fn build_exchange(
        &self,
        handoff: &Candidate,
        attempt: i32,
    ) -> Result<Exchange, CamelError> {
        let token = handoff.token();
        let input = self
            .load_input(ExecutionId::from_uuid(handoff.workflow_exec_id), token)
            .await?;
        let mut msg = Message::new(Body::Json(input));
        msg.set_header(HEADER_EXTERNAL_TOKEN, token.to_string());
        msg.set_header(
            crate::route::HEADER_WORKFLOW_ID,
            handoff.workflow_id.clone(),
        );
        msg.set_header(HEADER_WORKFLOW_NAME, handoff.workflow_name.clone());
        msg.set_header(HEADER_ACTIVITY_NAME, handoff.activity_name.clone());
        msg.set_header(HEADER_EXTERNAL_DEADLINE, handoff.deadline_at.to_rfc3339());
        msg.set_header(HEADER_DISPATCH_ATTEMPT, attempt);
        Ok(Exchange::new(msg))
    }

    /// The activity input, from the `ActivityAwaitingExternal` event that
    /// issued `token`.
    async fn load_input(
        &self,
        exec_id: ExecutionId,
        token: ExternalActivityToken,
    ) -> Result<Value, CamelError> {
        let mut conn = self.conn().await?;
        let history =
            autumn_harvest::store::load_history_with_codecs(&mut conn, exec_id, &self.codecs)
                .await
                .map_err(|e| db_error("load workflow history", e))?;
        history
            .events
            .into_iter()
            .find_map(|event| match event {
                WorkflowEvent::ActivityAwaitingExternal {
                    token: t, input, ..
                } if t == token => Some(input),
                _ => None,
            })
            .ok_or_else(|| {
                CamelError::ProcessorError(format!(
                    "no ActivityAwaitingExternal event for token {token} in execution {exec_id}"
                ))
            })
    }
}

// ───────────────────────────── producers ─────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SettleKind {
    Complete,
    Fail,
}

impl SettleKind {
    const fn scheme(self) -> &'static str {
        match self {
            Self::Complete => COMPLETE_SCHEME,
            Self::Fail => FAIL_SCHEME,
        }
    }
}

/// `harvest-complete:<label>` and `harvest-fail:<label>` — settle the external
/// activity whose token is in the `HarvestExternalToken` header. The path is
/// a free-form label.
///
/// `harvest-complete:` completes it with the body as JSON output (JSON bodies
/// as-is; text or bytes parsed as JSON, else a JSON string; empty is `null`).
///
/// `harvest-fail:` fails it with the `HarvestExternalError` header, else the
/// body text, as the message.
///
/// | Option           | Default                 | Meaning |
/// |------------------|-------------------------|---------|
/// | `tokenHeader`    | `HarvestExternalToken`  | Header holding the token. |
/// | `onUnknownToken` | `fail`                  | `fail` the exchange, or `ignore` (exchange Ok, `HarvestExternalSettled=false`). |
/// | `maxBodySize`    | `2097152`               | `harvest-complete:` only: largest output accepted. |
/// | `retryable`      | `false`                 | `harvest-fail:` only: recorded on the failure; the `HarvestExternalRetryable` header overrides it. |
///
/// Settling an already-terminal token succeeds with
/// `HarvestExternalSettled=false`, so a redelivered reply is harmless.
#[derive(Clone)]
pub struct HarvestSettleComponent {
    pool: DbPool,
    kind: SettleKind,
}

impl fmt::Debug for HarvestSettleComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HarvestSettleComponent")
            .field("scheme", &self.kind.scheme())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SettleOptions {
    token_header: String,
    ignore_unknown: bool,
    max_body_bytes: usize,
    retryable: bool,
}

impl SettleOptions {
    fn parse(uri: &str, kind: SettleKind) -> Result<Self, CamelError> {
        let scheme = kind.scheme();
        let parts = ensure_scheme(uri, scheme)?;
        let mut options = Self {
            token_header: HEADER_EXTERNAL_TOKEN.to_string(),
            ignore_unknown: false,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            retryable: false,
        };
        let expected = match kind {
            SettleKind::Complete => "tokenHeader, onUnknownToken, maxBodySize",
            SettleKind::Fail => "tokenHeader, onUnknownToken, retryable",
        };
        for (key, value) in &parts.params {
            match (key.as_str(), kind) {
                ("tokenHeader", _) if !value.is_empty() => options.token_header = value.clone(),
                ("onUnknownToken", _) => {
                    options.ignore_unknown = match value.as_str() {
                        "fail" => false,
                        "ignore" => true,
                        _ => {
                            return Err(CamelError::InvalidUri(format!(
                                "onUnknownToken must be 'fail' or 'ignore': '{value}'"
                            )));
                        }
                    };
                }
                ("maxBodySize", SettleKind::Complete) => {
                    options.max_body_bytes = value.parse().map_err(|_| {
                        CamelError::InvalidUri(format!("maxBodySize must be bytes: '{value}'"))
                    })?;
                }
                ("retryable", SettleKind::Fail) => {
                    options.retryable = camel_component_api::uri::parse_bool_param(value)
                        .map_err(CamelError::InvalidUri)?;
                }
                (other, _) => return Err(unknown_option(scheme, other, expected)),
            }
        }
        Ok(options)
    }
}

#[async_trait]
impl Component for HarvestSettleComponent {
    fn scheme(&self) -> &str {
        self.kind.scheme()
    }

    fn create_endpoint(
        &self,
        uri: &str,
        _ctx: &dyn ComponentContext,
    ) -> Result<Box<dyn Endpoint>, CamelError> {
        Ok(Box::new(SettleEndpoint {
            uri: uri.to_string(),
            settler: Settler {
                pool: self.pool.clone(),
                kind: self.kind,
                options: Arc::new(SettleOptions::parse(uri, self.kind)?),
            },
        }))
    }
}

struct SettleEndpoint {
    uri: String,
    settler: Settler,
}

impl Endpoint for SettleEndpoint {
    fn uri(&self) -> &str {
        &self.uri
    }

    fn create_consumer(
        &self,
        _rt: Arc<dyn RuntimeObservability>,
    ) -> Result<Box<dyn Consumer>, CamelError> {
        Err(CamelError::EndpointCreationFailed(format!(
            "{}: is producer-only",
            self.settler.kind.scheme()
        )))
    }

    fn create_producer(
        &self,
        _rt: Arc<dyn RuntimeObservability>,
        _ctx: &ProducerContext,
    ) -> Result<BoxProcessor, CamelError> {
        let settler = self.settler.clone();
        Ok(BoxProcessor::from_fn(move |exchange| {
            let settler = settler.clone();
            async move { settler.settle(exchange).await }
        }))
    }
}

#[derive(Clone)]
struct Settler {
    pool: DbPool,
    kind: SettleKind,
    options: Arc<SettleOptions>,
}

impl Settler {
    async fn settle(&self, mut exchange: Exchange) -> Result<Exchange, CamelError> {
        let token = self.token(&exchange)?;
        let outcome = match self.kind {
            SettleKind::Complete => {
                let body = std::mem::take(&mut exchange.input.body);
                let output = body_to_json(body, self.options.max_body_bytes).await?;
                let size = serde_json::to_vec(&output).map_or(0, |v| v.len());
                if size > self.options.max_body_bytes {
                    return Err(CamelError::StreamLimitExceeded(self.options.max_body_bytes));
                }
                exchange.input.body = Body::Json(output.clone());
                let mut conn = self.conn().await?;
                external_task::complete_externally(&mut conn, token, output).await
            }
            SettleKind::Fail => {
                let error = self.error_message(&mut exchange).await?;
                let retryable = match exchange.input.header(HEADER_EXTERNAL_RETRYABLE) {
                    Some(Value::Bool(b)) => *b,
                    Some(Value::String(s)) => camel_component_api::uri::parse_bool_param(s)
                        .map_err(CamelError::ValidationError)?,
                    _ => self.options.retryable,
                };
                let mut conn = self.conn().await?;
                external_task::fail_externally(&mut conn, token, error, retryable).await
            }
        };
        let settled = match outcome {
            Ok(settled) => settled,
            Err(HarvestError::NotFound(_)) if self.options.ignore_unknown => {
                tracing::warn!(%token, scheme = self.kind.scheme(), "ignoring unknown external token");
                false
            }
            Err(HarvestError::NotFound(msg)) => {
                return Err(CamelError::ValidationError(format!(
                    "{}: unknown {msg} (set onUnknownToken=ignore to drop such replies)",
                    self.kind.scheme()
                )));
            }
            Err(err) => return Err(db_error(self.kind.scheme(), err)),
        };
        exchange.input.set_header(HEADER_EXTERNAL_SETTLED, settled);
        Ok(exchange)
    }

    fn token(&self, exchange: &Exchange) -> Result<ExternalActivityToken, CamelError> {
        let name = &self.options.token_header;
        let raw = exchange
            .input
            .header(name)
            .and_then(Value::as_str)
            .ok_or_else(|| {
                CamelError::ValidationError(format!(
                    "{}: missing '{name}' header with the external activity token",
                    self.kind.scheme()
                ))
            })?;
        raw.trim().parse().map_err(|_| {
            CamelError::ValidationError(format!(
                "{}: '{name}' header is not an external activity token: '{raw}'",
                self.kind.scheme()
            ))
        })
    }

    async fn error_message(&self, exchange: &mut Exchange) -> Result<String, CamelError> {
        if let Some(Value::String(s)) = exchange.input.header(HEADER_EXTERNAL_ERROR) {
            return Ok(s.clone());
        }
        let body = std::mem::take(&mut exchange.input.body);
        let text = match body_to_json(body, DEFAULT_MAX_BODY_BYTES).await? {
            Value::Null => "external activity failed".to_string(),
            Value::String(s) => s,
            other => other.to_string(),
        };
        exchange.input.body = Body::Text(text.clone());
        Ok(text)
    }

    async fn conn(
        &self,
    ) -> Result<
        deadpool::managed::Object<
            diesel_async::pooled_connection::AsyncDieselConnectionManager<AsyncPgConnection>,
        >,
        CamelError,
    > {
        self.pool
            .get()
            .await
            .map_err(|e| db_error("harvest database pool", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_options() {
        let o = DispatchOptions::parse("harvest-external:charge_card").unwrap();
        assert_eq!(o.activity_name, "charge_card");
        assert_eq!(o.batch_size, 16);
        assert_eq!(o.lease, Duration::from_secs(60));

        let o = DispatchOptions::parse(
            "harvest-external:x?pollInterval=10&batchSize=2&lease=500&retryDelay=20&claimRetention=1000",
        )
        .unwrap();
        assert_eq!(o.poll_interval, Duration::from_millis(10));
        assert_eq!(o.batch_size, 2);
        assert_eq!(o.lease, Duration::from_millis(500));
        assert_eq!(o.retry_delay, Duration::from_millis(20));
        assert_eq!(o.claim_retention, Duration::from_secs(1));

        for bad in [
            "harvest-external:",
            "harvest-external:x?batchSize=0",
            "harvest-external:x?lease=0",
            "harvest-external:x?lease=soon",
            "harvest-external:x?bogus=1",
            "harvest-complete:x",
        ] {
            assert!(
                DispatchOptions::parse(bad).is_err(),
                "{bad} should be rejected"
            );
        }
    }

    #[test]
    fn settle_options() {
        let o = SettleOptions::parse("harvest-complete:reply", SettleKind::Complete).unwrap();
        assert_eq!(o.token_header, HEADER_EXTERNAL_TOKEN);
        assert!(!o.ignore_unknown);

        let o = SettleOptions::parse(
            "harvest-complete:r?tokenHeader=Corr&onUnknownToken=ignore&maxBodySize=10",
            SettleKind::Complete,
        )
        .unwrap();
        assert_eq!(o.token_header, "Corr");
        assert!(o.ignore_unknown);
        assert_eq!(o.max_body_bytes, 10);

        let o = SettleOptions::parse("harvest-fail:r?retryable=true", SettleKind::Fail).unwrap();
        assert!(o.retryable);

        for (bad, kind) in [
            ("harvest-complete:r?retryable=true", SettleKind::Complete),
            ("harvest-fail:r?maxBodySize=1", SettleKind::Fail),
            ("harvest-fail:r?onUnknownToken=maybe", SettleKind::Fail),
            ("harvest-fail:r?retryable=perhaps", SettleKind::Fail),
            ("harvest-complete:r", SettleKind::Fail),
        ] {
            assert!(
                SettleOptions::parse(bad, kind).is_err(),
                "{bad} should be rejected"
            );
        }
    }
}
