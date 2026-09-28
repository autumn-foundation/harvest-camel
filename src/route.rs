//! Outbound bridge: harvest activities calling camel routes.
//!
//! Only **activities** call camel, never workflow code: an activity is the
//! durability boundary (its result is recorded, its retries are harvest's),
//! while workflow code is replayed and must stay deterministic.
//!
//! Routes called from activities should **disable camel redelivery** (no
//! `retry(..)` in their error handler). harvest already retries the activity
//! under its `RetryPolicy`; camel redelivery inside each attempt multiplies
//! the attempts and can outlive the activity's `start_to_close` timeout.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, PoisonError, RwLock};

use autumn_harvest::ActivityContext;
use autumn_harvest::failure::ActivityFailure;
use camel_api::{Body, BoxProcessor, CamelError, Exchange, Message, Value};
use camel_component_api::{NoOpComponentContext, RuntimeObservability};
use camel_core::CamelContext;
use tower::ServiceExt;

/// Header carrying [`ActivityContext::idempotency_key`], stable across the
/// activity's retries, so the route (or the system behind it) can dedupe.
pub const HEADER_IDEMPOTENCY_KEY: &str = "HarvestIdempotencyKey";
/// Header carrying the calling workflow's id.
pub const HEADER_WORKFLOW_ID: &str = "HarvestWorkflowId";
/// Header carrying the calling activity's type name.
pub const HEADER_ACTIVITY_TYPE: &str = "HarvestActivityType";
/// Header carrying the 1-based activity attempt number.
pub const HEADER_ATTEMPT: &str = "HarvestActivityAttempt";

/// Cached producers for the camel endpoints activities may call.
///
/// Resolve every URI once at startup with [`Self::resolve`] (it needs the
/// `CamelContext`, which activities cannot hold since `start`/`stop` take
/// `&mut`), then register the handle as harvest state:
///
/// ```no_run
/// # async fn demo(camel: camel_core::CamelContext) -> Result<(), camel_api::CamelError> {
/// let handle = harvest_camel::CamelHandle::new();
/// handle.resolve(&camel, "direct:charge-card")?;
/// // HarvestPlugin::new().state(handle.clone()) ...
/// // inside an activity: harvest_camel::call_route_from_activity(ctx, "direct:charge-card", input)
/// # Ok(()) }
/// ```
///
/// Cloning is cheap and clones share the cache.
#[derive(Clone, Default)]
pub struct CamelHandle {
    producers: Arc<RwLock<HashMap<String, BoxProcessor>>>,
}

impl fmt::Debug for CamelHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let producers = self
            .producers
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        f.debug_struct("CamelHandle")
            .field("uris", &producers.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl CamelHandle {
    /// An empty handle.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolve `uri` against `camel` and cache its producer. Idempotent.
    ///
    /// # Errors
    ///
    /// Fails if the URI's component is not registered or the endpoint
    /// cannot build a producer.
    pub fn resolve(&self, camel: &CamelContext, uri: &str) -> Result<(), CamelError> {
        if self.contains(uri) {
            return Ok(());
        }
        let scheme = uri
            .split_once(':')
            .map(|(scheme, _)| scheme)
            .ok_or_else(|| CamelError::InvalidUri(format!("missing scheme in '{uri}'")))?;
        let component = camel.registry().get_or_err(scheme)?;
        let endpoint = component.create_endpoint(uri, camel)?;
        let rt: Arc<dyn RuntimeObservability> = Arc::new(NoOpComponentContext);
        let producer = endpoint.create_producer(rt, &camel.producer_context())?;
        self.insert(uri, producer);
        Ok(())
    }

    /// Cache an already-built producer under `uri` (tests, custom wiring).
    pub fn insert(&self, uri: impl Into<String>, producer: BoxProcessor) {
        self.producers
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(uri.into(), producer);
    }

    /// Whether `uri` has a cached producer.
    #[must_use]
    pub fn contains(&self, uri: &str) -> bool {
        self.producers
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(uri)
    }

    fn producer(&self, uri: &str) -> Result<BoxProcessor, CamelError> {
        self.producers
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(uri)
            .cloned()
            .ok_or_else(|| {
                CamelError::Config(format!(
                    "camel endpoint '{uri}' was not resolved on this CamelHandle; \
                     call CamelHandle::resolve at startup"
                ))
            })
    }
}

/// Send `input` as a JSON body to the camel endpoint `uri` and return the
/// reply body as JSON.
///
/// The reply is the returned exchange's `output` message if set, else its
/// `input` message. `Json` bodies are returned as-is; text and byte bodies
/// are parsed as JSON, falling back to a JSON string for non-JSON text; an
/// empty body is `null`. An exchange that comes back `Ok` but still carries
/// an error is reported as that error.
///
/// # Errors
///
/// The route's `CamelError`, or [`CamelError::Config`] if `uri` was never
/// [resolved](CamelHandle::resolve).
pub async fn call_route(
    camel: &CamelHandle,
    uri: &str,
    input: Value,
    headers: impl IntoIterator<Item = (String, Value)>,
) -> Result<Value, CamelError> {
    let producer = camel.producer(uri)?;
    let mut message = Message::new(Body::Json(input));
    message.headers.extend(headers);
    let mut exchange = producer.oneshot(Exchange::new(message)).await?;
    if let Some(err) = exchange.error.take() {
        return Err(err);
    }
    let reply = exchange.output.unwrap_or(exchange.input);
    body_to_json(reply.body, MAX_REPLY_BYTES).await
}

/// Largest reply body [`call_route`] reads: harvest's 2 MiB result cap.
const MAX_REPLY_BYTES: usize = 2 * 1024 * 1024;

pub(crate) async fn body_to_json(body: Body, max_bytes: usize) -> Result<Value, CamelError> {
    match body {
        Body::Empty => Ok(Value::Null),
        Body::Json(v) => Ok(v),
        Body::Text(s) | Body::Xml(s) => Ok(serde_json::from_str(&s).unwrap_or(Value::String(s))),
        other => {
            let bytes = other.into_bytes(max_bytes).await?;
            if bytes.is_empty() {
                return Ok(Value::Null);
            }
            if let Ok(v) = serde_json::from_slice(&bytes) {
                return Ok(v);
            }
            String::from_utf8(bytes.to_vec())
                .map(Value::String)
                .map_err(|_| {
                    CamelError::TypeConversionFailed(
                        "route reply is neither JSON nor UTF-8 text".to_string(),
                    )
                })
        }
    }
}

/// [`call_route`] from inside a harvest activity.
///
/// Reads the [`CamelHandle`] from harvest state (`HarvestPlugin::state`),
/// adds the [`HEADER_IDEMPOTENCY_KEY`], [`HEADER_WORKFLOW_ID`],
/// [`HEADER_ACTIVITY_TYPE`] and [`HEADER_ATTEMPT`] headers, and maps a camel
/// error with [`to_activity_failure`]. Declare the activity as returning
/// `Result<T, ActivityFailure>` so harvest honours the retryable flag:
///
/// ```ignore
/// #[activity(start_to_close = "30s", retry = RetryPolicy::exponential(5, Duration::from_secs(1)))]
/// async fn charge_card(ctx: &ActivityContext, input: Value) -> Result<Value, ActivityFailure> {
///     harvest_camel::call_route_from_activity(ctx, "direct:charge-card", input).await
/// }
/// ```
///
/// # Errors
///
/// A non-retryable [`ActivityFailure`] if no `CamelHandle` is registered;
/// otherwise the mapped route error.
pub async fn call_route_from_activity(
    ctx: &ActivityContext,
    uri: &str,
    input: Value,
) -> Result<Value, ActivityFailure> {
    let camel = ctx.state::<CamelHandle>().ok_or_else(|| {
        ActivityFailure::non_retryable(
            "camel.NoHandle",
            "no CamelHandle registered; add it with HarvestPlugin::state(handle)",
        )
    })?;
    let mut headers = vec![
        (
            HEADER_WORKFLOW_ID.to_string(),
            Value::String(ctx.workflow_id().to_string()),
        ),
        (
            HEADER_ACTIVITY_TYPE.to_string(),
            Value::String(ctx.activity_type().to_string()),
        ),
        (HEADER_ATTEMPT.to_string(), Value::from(ctx.attempt())),
    ];
    if let Ok(key) = ctx.idempotency_key() {
        headers.push((
            HEADER_IDEMPOTENCY_KEY.to_string(),
            Value::String(key.as_str().to_string()),
        ));
    }
    call_route(camel, uri, input, headers)
        .await
        .map_err(|e| to_activity_failure(&e))
}

/// Whether harvest should retry an activity that failed with `err`.
///
/// | Retryable (transient) | Non-retryable (the same input will fail again) |
/// |---|---|
/// | `Io`, `ChannelClosed`, `ConsumerStopping`, `CircuitOpen`, `AuthProviderUnavailable`, `RouteError`, `DeadLetterChannelFailed`, `TemplateReload`, `ProcessorError*` | `ValidationError`, `TypeConversionFailed`, `InvalidUri`, `EndpointUri`, `Config`, `ConfigValidation`, `ComponentNotFound`, `EndpointCreationFailed*`, `Unauthenticated`, `Unauthorized`, `UnsupportedMediaType`, `NotAcceptable`, `StreamLimitExceeded`, `AlreadyConsumed` |
/// | `HttpOperationFailed` 5xx, 408, 425, 429 | `HttpOperationFailed` other 4xx |
///
/// `ProcessorError` is camel's catch-all, so it is treated as transient: a
/// deterministic failure then fails the activity once harvest's retry policy
/// is exhausted rather than on the first attempt. Variants added to camel
/// later default to retryable for the same reason.
#[must_use]
pub fn is_retryable(err: &CamelError) -> bool {
    match err {
        CamelError::ValidationError(_)
        | CamelError::TypeConversionFailed(_)
        | CamelError::InvalidUri(_)
        | CamelError::EndpointUri(_)
        | CamelError::Config(_)
        | CamelError::ConfigValidation(_)
        | CamelError::ComponentNotFound(_)
        | CamelError::EndpointCreationFailed(_)
        | CamelError::EndpointCreationFailedWithSource(..)
        | CamelError::Unauthenticated(_)
        | CamelError::Unauthorized(_)
        | CamelError::UnsupportedMediaType { .. }
        | CamelError::NotAcceptable { .. }
        | CamelError::StreamLimitExceeded(_)
        | CamelError::AlreadyConsumed => false,
        CamelError::HttpOperationFailed { status_code, .. } => {
            !(400..500).contains(status_code) || matches!(status_code, 408 | 425 | 429)
        }
        _ => true,
    }
}

/// Map a camel error to a harvest [`ActivityFailure`] per [`is_retryable`].
///
/// The failure's `error_type` is `camel.<Variant>` (e.g. `camel.Io`).
#[must_use]
pub fn to_activity_failure(err: &CamelError) -> ActivityFailure {
    let error_type = format!("camel.{}", err.variant_name());
    let message = err.to_string();
    let failure = if is_retryable(err) {
        ActivityFailure::retryable(error_type, message)
    } else {
        ActivityFailure::non_retryable(error_type, message)
    };
    match err {
        CamelError::HttpOperationFailed {
            status_code,
            response_body,
            ..
        } => failure.with_details(serde_json::json!({
            "status_code": status_code,
            "response_body": response_body,
        })),
        _ => failure,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use camel_api::BoxProcessorExt;

    fn http(status_code: u16) -> CamelError {
        CamelError::HttpOperationFailed {
            method: "POST".into(),
            url: "http://x".into(),
            status_code,
            status_text: String::new(),
            response_body: Some("nope".into()),
        }
    }

    #[test]
    fn error_mapping() {
        for e in [
            CamelError::Io("reset".into()),
            CamelError::ChannelClosed,
            CamelError::CircuitOpen("x".into()),
            CamelError::ProcessorError("x".into()),
            http(500),
            http(503),
            http(408),
            http(429),
        ] {
            assert!(is_retryable(&e), "{e} should be retryable");
            assert!(!to_activity_failure(&e).non_retryable);
        }
        for e in [
            CamelError::ValidationError("bad".into()),
            CamelError::TypeConversionFailed("x".into()),
            CamelError::Unauthorized("x".into()),
            CamelError::Config("x".into()),
            http(400),
            http(404),
            http(422),
        ] {
            assert!(!is_retryable(&e), "{e} should not be retryable");
            assert!(to_activity_failure(&e).non_retryable);
        }
        let f = to_activity_failure(&http(422));
        assert_eq!(f.error_type, "camel.HttpOperationFailed");
        assert_eq!(f.details.unwrap()["status_code"], 422);
        assert_eq!(
            to_activity_failure(&CamelError::Io("x".into())).error_type,
            "camel.Io"
        );
    }

    #[tokio::test]
    async fn call_route_round_trips_json_and_headers() {
        let handle = CamelHandle::new();
        handle.insert(
            "direct:echo",
            BoxProcessor::from_fn(|mut ex: Exchange| async move {
                let key = ex.input.header(HEADER_IDEMPOTENCY_KEY).cloned();
                let Body::Json(v) = std::mem::take(&mut ex.input.body) else {
                    return Err(CamelError::ValidationError("expected json".into()));
                };
                ex.input.body = Body::Json(serde_json::json!({"echo": v, "key": key}));
                Ok(ex)
            }),
        );
        let out = call_route(
            &handle,
            "direct:echo",
            serde_json::json!({"a": 1}),
            [(HEADER_IDEMPOTENCY_KEY.to_string(), Value::from("k-1"))],
        )
        .await
        .unwrap();
        assert_eq!(out, serde_json::json!({"echo": {"a": 1}, "key": "k-1"}));
    }

    #[tokio::test]
    async fn call_route_reply_bodies() {
        let handle = CamelHandle::new();
        let reply = |body: Body| {
            let body = Arc::new(body);
            BoxProcessor::from_fn(move |mut ex: Exchange| {
                let body = (*body).clone();
                async move {
                    ex.input.body = body;
                    Ok(ex)
                }
            })
        };
        handle.insert("direct:text", reply(Body::Text("hello".into())));
        handle.insert("direct:jsontext", reply(Body::Text("[1,2]".into())));
        handle.insert("direct:empty", reply(Body::Empty));
        handle.insert(
            "direct:bytes",
            reply(Body::Bytes(b"{\"b\":true}".to_vec().into())),
        );
        let call = |uri: &'static str| call_route(&handle, uri, Value::Null, []);
        assert_eq!(call("direct:text").await.unwrap(), Value::from("hello"));
        assert_eq!(
            call("direct:jsontext").await.unwrap(),
            serde_json::json!([1, 2])
        );
        assert_eq!(call("direct:empty").await.unwrap(), Value::Null);
        assert_eq!(
            call("direct:bytes").await.unwrap(),
            serde_json::json!({"b": true})
        );
    }

    #[tokio::test]
    async fn call_route_from_activity_without_a_handle_is_non_retryable() {
        let ctx = ActivityContext::new_test();
        let failure = call_route_from_activity(&ctx, "direct:x", Value::Null)
            .await
            .unwrap_err();
        assert!(failure.non_retryable);
        assert_eq!(failure.error_type, "camel.NoHandle");
    }

    #[tokio::test]
    async fn call_route_surfaces_errors() {
        let handle = CamelHandle::new();
        handle.insert(
            "direct:fail",
            BoxProcessor::from_fn(|_ex: Exchange| async move {
                Err(CamelError::ValidationError("no".into()))
            }),
        );
        handle.insert(
            "direct:error-on-ok",
            BoxProcessor::from_fn(|mut ex: Exchange| async move {
                ex.set_error(CamelError::Io("late".into()));
                Ok(ex)
            }),
        );
        let err = call_route(&handle, "direct:fail", Value::Null, [])
            .await
            .unwrap_err();
        assert!(matches!(err, CamelError::ValidationError(_)));
        let err = call_route(&handle, "direct:error-on-ok", Value::Null, [])
            .await
            .unwrap_err();
        assert!(matches!(err, CamelError::Io(_)));
        let err = call_route(&handle, "direct:unknown", Value::Null, [])
            .await
            .unwrap_err();
        assert!(matches!(err, CamelError::Config(_)));
        assert!(!is_retryable(&err));
    }
}
