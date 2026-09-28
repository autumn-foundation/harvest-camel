//! Startup guard for routes that feed a `harvest:` endpoint.
//!
//! camel's `ConsumerContext::send_and_wait` returns whatever the route
//! pipeline returns, and camel's Kafka / SQL / JMS consumers commit only on
//! `Ok`. A route error handler that **absorbs** the `harvest:` producer's
//! error turns an abandoned message into `Ok`, so the consumer commits a
//! message harvest never durably accepted. Absorbing handlers are:
//!
//! * `ErrorHandlerConfig::dead_letter_channel(uri)` with **no** explicit
//!   policies (camel adds an implicit catch-all `Handled` policy);
//! * any `on_exception(..)` policy with `.handled(true)` or
//!   `.continued(true)` whose predicate matches the bridge's error.
//!
//! No handler, `ErrorHandlerConfig::log_only()`, and policies that propagate
//! (the default) are safe. [`check_route`] rejects the unsafe shapes.
//!
//! It can only see a route's **own** handler. A context-wide handler set with
//! `CamelContext::set_error_handler` applies to routes that have none and
//! cannot be inspected; if you use one, check that config with
//! [`check_error_handler`] too, or give harvest-bound routes their own
//! handler.

use camel_api::{ErrorHandlerConfig, ExceptionDisposition};
use camel_core::RouteDefinition;

use crate::source::sample_unsettled;

/// Why a route's error handler would lose messages bound for harvest.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RouteCheckError {
    /// A dead-letter channel with no policies absorbs every error.
    #[error(
        "route '{route_id}': a dead letter channel with no exception policies marks every error \
         handled, so an abandoned harvest message would be committed by the consumer. Add a \
         policy excluding harvest_camel::is_unsettled"
    )]
    CatchAllDeadLetter {
        /// The offending route.
        route_id: String,
    },
    /// A policy matching the bridge's error does not propagate it.
    #[error(
        "route '{route_id}': exception policy #{index} matches harvest-camel's unsettled error \
         and is {disposition}, so an abandoned harvest message would be committed by the \
         consumer. Exclude harvest_camel::is_unsettled from its predicate"
    )]
    AbsorbingPolicy {
        /// The offending route.
        route_id: String,
        /// Index of the policy within the handler (first match wins).
        index: usize,
        /// `handled`, `continued`, or `not propagating` for an unknown kind.
        disposition: &'static str,
    },
}

/// Check that `route`'s own error handler propagates `harvest:` failures.
///
/// Call it on every route that sends to a `harvest:` endpoint, before
/// `add_route_definition`.
///
/// # Errors
///
/// A [`RouteCheckError`] describing the absorbing handler.
pub fn check_route(route: &RouteDefinition) -> Result<(), RouteCheckError> {
    match route.error_handler_config() {
        None => Ok(()),
        Some(cfg) => check_error_handler(route.route_id(), cfg),
    }
}

/// [`check_route`] for a bare [`ErrorHandlerConfig`], e.g. the one passed to
/// `CamelContext::set_error_handler`. `route_id` is only used in the error.
///
/// # Errors
///
/// A [`RouteCheckError`] describing the absorbing handler.
pub fn check_error_handler(
    route_id: &str,
    cfg: &ErrorHandlerConfig,
) -> Result<(), RouteCheckError> {
    if cfg.dlc_uri.is_some() && cfg.policies.is_empty() {
        return Err(RouteCheckError::CatchAllDeadLetter {
            route_id: route_id.to_string(),
        });
    }
    let probe = sample_unsettled();
    // First matching policy wins; anything after it is unreachable for us.
    let Some((index, policy)) = cfg
        .policies
        .iter()
        .enumerate()
        .find(|(_, p)| (p.matches)(&probe))
    else {
        return Ok(());
    };
    let disposition = match policy.disposition {
        ExceptionDisposition::Propagate => return Ok(()),
        ExceptionDisposition::Handled => "handled",
        ExceptionDisposition::Continued => "continued",
        // A disposition added upstream later: not known to propagate.
        _ => "not propagating",
    };
    Err(RouteCheckError::AbsorbingPolicy {
        route_id: route_id.to_string(),
        index,
        disposition,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::is_unsettled;
    use camel_api::CamelError;

    fn check(cfg: &ErrorHandlerConfig) -> Result<(), RouteCheckError> {
        check_error_handler("r", cfg)
    }

    #[test]
    fn propagating_handlers_pass() {
        assert!(check(&ErrorHandlerConfig::log_only()).is_ok());
        assert!(
            check(
                &ErrorHandlerConfig::log_only()
                    .on_exception(|_| true)
                    .retry(2)
                    .build()
            )
            .is_ok()
        );
        // DLC with an explicit policy: unmatched errors go to the DLC and
        // then propagate.
        assert!(
            check(
                &ErrorHandlerConfig::dead_letter_channel("log:dlc")
                    .on_exception(|e| matches!(e, CamelError::Io(_)))
                    .handled(true)
                    .build()
            )
            .is_ok()
        );
        // An absorbing policy that excludes the bridge's error.
        assert!(
            check(
                &ErrorHandlerConfig::dead_letter_channel("log:dlc")
                    .on_exception(|e| !is_unsettled(e))
                    .handled(true)
                    .build()
            )
            .is_ok()
        );
    }

    #[test]
    fn catch_all_dlc_is_rejected() {
        assert!(matches!(
            check(&ErrorHandlerConfig::dead_letter_channel("log:dlc")),
            Err(RouteCheckError::CatchAllDeadLetter { .. })
        ));
    }

    #[test]
    fn absorbing_policies_are_rejected() {
        let handled = ErrorHandlerConfig::log_only()
            .on_exception(|_| true)
            .handled(true)
            .build();
        assert_eq!(
            check(&handled),
            Err(RouteCheckError::AbsorbingPolicy {
                route_id: "r".into(),
                index: 0,
                disposition: "handled"
            })
        );
        let continued = ErrorHandlerConfig::log_only()
            .on_exception(|e| matches!(e, CamelError::Io(_)))
            .build()
            .on_exception(|e| matches!(e, CamelError::ProcessorError(_)))
            .continued(true)
            .build();
        assert!(matches!(
            check(&continued),
            Err(RouteCheckError::AbsorbingPolicy {
                index: 1,
                disposition: "continued",
                ..
            })
        ));
    }

    #[test]
    fn first_match_wins() {
        let cfg = ErrorHandlerConfig::log_only()
            .on_exception(|e| is_unsettled(e))
            .build()
            .on_exception(|_| true)
            .handled(true)
            .build();
        assert!(
            check(&cfg).is_ok(),
            "an earlier propagating match shadows the catch-all"
        );
    }
}
