//! The shared DAV status-to-`AccountError` ladder and error constructors.
//!
//! The two protocol crates carried this ladder twice. The copies differed in
//! exactly three tokens - the `ResourceKind` a 404 and a 403 name, the
//! `Protocol` stamp, and the `field` label on a local argument error - so those
//! are the parameter, and everything else is one implementation.

use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, AttemptCause, Cause,
    DiagnosticText, Protocol, ProtocolErrorKind, RecoveryClass, RequestCause, RequestErrorKind,
    ResourceKind, ServerCause, ServerErrorKind, StateCause, TransmissionState, TransportCause,
    TransportErrorKind, TransportKind, WireCause,
};
use reqwest::StatusCode;

/// Which DAV dialect an error is being minted for.
///
/// Carries the three values that distinguish the two otherwise identical error
/// surfaces, so a caller cannot mix a CalDAV `Protocol` stamp with a CardDAV
/// `ResourceKind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DavProtocol {
    CalDav,
    CardDav,
}

impl DavProtocol {
    #[must_use]
    pub fn protocol(self) -> Protocol {
        match self {
            Self::CalDav => Protocol::CalDav,
            Self::CardDav => Protocol::CardDav,
        }
    }

    /// The resource a bare status refers to when the server names none.
    #[must_use]
    pub fn resource(self) -> ResourceKind {
        match self {
            Self::CalDav => ResourceKind::Calendar,
            Self::CardDav => ResourceKind::Contact,
        }
    }

    /// The protocol's name as it appears in support-only diagnostics.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::CalDav => "CalDAV",
            Self::CardDav => "CardDAV",
        }
    }

    /// The `field` label on a locally-refused request.
    #[must_use]
    pub fn field(self) -> &'static str {
        match self {
            Self::CalDav => "caldav",
            Self::CardDav => "carddav",
        }
    }

    /// The RFC 6764 well-known service name discovery probes first
    /// (`/.well-known/caldav`, `/.well-known/carddav`).
    #[must_use]
    pub fn well_known_service(self) -> &'static str {
        match self {
            Self::CalDav => "caldav",
            Self::CardDav => "carddav",
        }
    }
}

#[must_use]
pub fn unsupported_error(operation: AccountOperation, protocol: DavProtocol) -> AccountError {
    AccountError::unsupported(operation, protocol.protocol())
}

/// The server answered, and what it answered says the resource named by `id`
/// is not there - a GET that returned a calendar resource holding no VEVENT is
/// the one caller.
///
/// Carries an ACKNOWLEDGED attempt, as [`status_error`] does: the verdict is
/// read out of a server answer, so nothing about it is a local refusal. It
/// carried none for a while, which read as `Unsent` in telemetry and support
/// exports, while CardDAV's `not_found_error` - built on [`status_error`] -
/// stamped `Acknowledged` for the same kind of answer. Not for a local
/// refusal: that is [`local_error`].
#[must_use]
pub fn not_found_error(
    operation: AccountOperation,
    id: impl Into<String>,
    protocol: DavProtocol,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::NotFound(protocol.resource()),
        Cause::Request(RequestCause::NotFound {
            what: protocol.resource(),
            id: Some(id.into()),
        }),
    )
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::Acknowledged,
    )))
    .protocol(protocol.protocol())
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

/// A server answer that decoded but lacks a property the request exists to
/// read - a principal lookup naming no `current-user-principal`, a principal
/// naming no calendar or addressbook home.
///
/// `Protocol(MissingField)` rather than [`parse_error`]'s `ParseFailed`: the
/// document parsed, it just did not say what it had to. Both derive
/// `ProviderContractViolation`; the kind is what tells an operator which of the
/// two happened. Carries an ACKNOWLEDGED attempt for the same reason
/// [`parse_error`] does.
///
/// Deliberately NOT an arm of [`should_fallback_discovery`]. A well-known probe
/// naming no principal already falls back, as a successful lookup returning
/// nothing rather than as an error; the only producers of this kind are the
/// configured-base leg and the steps after the principal, whose failures must
/// propagate.
#[must_use]
pub fn missing_field_error(
    operation: AccountOperation,
    message: impl Into<String>,
    protocol: DavProtocol,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Protocol(ProtocolErrorKind::MissingField),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: protocol.protocol(),
            detail: Some(DiagnosticText::support_only(message)),
        }),
    )
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::Acknowledged,
    )))
    .protocol(protocol.protocol())
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

#[must_use]
pub fn local_error(
    operation: AccountOperation,
    message: impl Into<String>,
    protocol: DavProtocol,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::InvalidArgument {
            field: Some(protocol.field()),
            message: Some(DiagnosticText::support_only(message)),
        }),
    )
    .protocol(protocol.protocol())
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

/// A server response whose body will not decode into what the request asked
/// for.
///
/// Carries an ACKNOWLEDGED attempt, as [`status_error`] does: there is a body
/// to fail to parse only because the server answered. Every caller hands this
/// a body the server sent - a Multi-Status, a discovery property, an iCalendar
/// or vCard resource - and none uses it for local input, which is what
/// [`local_error`] is for.
#[must_use]
pub fn parse_error(
    operation: AccountOperation,
    message: impl Into<String>,
    protocol: DavProtocol,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: protocol.protocol(),
            detail: Some(DiagnosticText::support_only(message)),
        }),
    )
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::Acknowledged,
    )))
    .protocol(protocol.protocol())
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

/// A transport failure raised BEFORE any byte of the request left this process.
///
/// The attempt is `Unsent`, not `InFlight`. The sole production caller is
/// `auth_headers`, which reaches here when the token source cannot mint a
/// credential - there is no request on the wire, and nothing at the target to
/// reconcile against. Claiming `InFlight` made a non-idempotent operation derive
/// `Reconcile(CheckTarget)` and sent the consumer probing for a resource that was
/// never written; `Unsent` derives `Retry(SameRequest)`, which is what a failed
/// token read deserves. Wire-level transmission evidence comes from
/// `bifrost_net::into_account_error`, which carries the state the transport
/// actually observed.
#[must_use]
pub fn transport_error(
    operation: AccountOperation,
    message: impl Into<String>,
    protocol: DavProtocol,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Transport(TransportErrorKind::Network),
        Cause::Transport(TransportCause::new(
            TransportKind::Network,
            Some(DiagnosticText::support_only(message)),
        )),
    )
    .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
    .protocol(protocol.protocol())
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

/// A response body that exceeded the buffered ceiling.
///
/// Classified `Protocol(PartialResponse)` with an ACKNOWLEDGED attempt: the
/// request reached the server and may have taken effect, so a non-idempotent
/// mutation must reconcile rather than replay blindly.
#[must_use]
pub fn response_read_error(
    operation: AccountOperation,
    message: String,
    protocol: DavProtocol,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: protocol.protocol(),
            detail: Some(DiagnosticText::support_only(message)),
        }),
    )
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::Acknowledged,
    )))
    .protocol(protocol.protocol())
    .operation(operation)
    .try_build()
    .expect("valid acknowledged response-overflow classification")
}

/// Classify a non-2xx status the server answered.
///
/// DAV deliberately overrides generic HTTP conflicts: 409, conditional-write
/// 412, and lock refusal 423 require a state refresh. A 410 is cursor invalid
/// only on sync-collection; that caller interprets it before this generic
/// resource ladder. A 400/422 remains malformed and 408/502/503/504 unavailable,
/// matching the shared HTTP classification. Synthetic member statuses pass an
/// empty header map because a 207 member has no separate HTTP headers.
///
/// Every status here is one the server sent - on the response line, or as the
/// member status of a 207 whose every response failed - so the error carries an
/// ACKNOWLEDGED attempt, the transmission evidence `bifrost-net` stamps on the
/// status-bearing failures it classifies itself. It moves no recovery class
/// today (`derive` treats an acknowledged attempt like an unsent one on every
/// kind this ladder mints), but its absence read as `Unsent` in the telemetry
/// and support exports, which claimed the request never left the process.
#[must_use]
pub fn status_error(
    headers: &reqwest::header::HeaderMap,
    operation: AccountOperation,
    status: StatusCode,
    body: String,
    protocol: DavProtocol,
) -> AccountError {
    let retry_hint =
        bifrost_net::request::parse_retry_after_hint(headers.get(reqwest::header::RETRY_AFTER));
    let resource = protocol.resource();
    let kind = if status == StatusCode::UNAUTHORIZED {
        AccountErrorKind::Authentication(bifrost_types::AuthErrorKind::ReauthorizationRequired)
    } else if status == StatusCode::FORBIDDEN {
        AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PermissionDenied)
    } else if status == StatusCode::NOT_FOUND {
        AccountErrorKind::NotFound(resource)
    } else if status == StatusCode::CONFLICT
        || status == StatusCode::PRECONDITION_FAILED
        || status == StatusCode::LOCKED
    {
        AccountErrorKind::ConcurrencyConflict
    } else if status == StatusCode::TOO_MANY_REQUESTS {
        AccountErrorKind::Server(ServerErrorKind::RateLimited)
    } else if matches!(status.as_u16(), 408 | 502 | 503 | 504) {
        AccountErrorKind::Server(ServerErrorKind::Unavailable)
    } else if status == StatusCode::INSUFFICIENT_STORAGE {
        AccountErrorKind::Server(ServerErrorKind::QuotaExhausted)
    } else if matches!(status.as_u16(), 400 | 422) {
        AccountErrorKind::Request(RequestErrorKind::Malformed)
    } else {
        AccountErrorKind::Server(ServerErrorKind::Error {
            status: Some(status.as_u16()),
        })
    };
    let cause = if status == StatusCode::UNAUTHORIZED {
        Cause::Auth(bifrost_types::AuthCause::ReauthorizationRequired)
    } else if status == StatusCode::FORBIDDEN {
        Cause::Access(bifrost_types::AccessCause::PermissionDenied {
            resource: Some(resource),
        })
    } else if status == StatusCode::NOT_FOUND {
        Cause::Request(RequestCause::NotFound {
            what: resource,
            id: None,
        })
    } else if status == StatusCode::CONFLICT
        || status == StatusCode::PRECONDITION_FAILED
        || status == StatusCode::LOCKED
    {
        Cause::State(StateCause::ConcurrencyConflict)
    } else if status == StatusCode::TOO_MANY_REQUESTS {
        Cause::Server(ServerCause::RateLimited { retry_hint })
    } else if matches!(status.as_u16(), 408 | 502 | 503 | 504) {
        Cause::Server(ServerCause::Unavailable { retry_hint })
    } else if status == StatusCode::INSUFFICIENT_STORAGE {
        Cause::Server(ServerCause::QuotaExhausted { retry_hint })
    } else if matches!(status.as_u16(), 400 | 422) {
        Cause::Request(RequestCause::InvalidArgument {
            field: Some(protocol.field()),
            message: None,
        })
    } else {
        Cause::Server(ServerCause::Error {
            status: Some(status.as_u16()),
        })
    };

    let mut builder = AccountErrorBuilder::new(kind, cause)
        .push_cause(Cause::Attempt(AttemptCause::new(
            TransmissionState::Acknowledged,
        )))
        .protocol(protocol.protocol())
        .operation(operation)
        .status(Some(status.as_u16()));
    if matches!(status.as_u16(), 429 | 507) {
        builder = builder.throttle_scope(bifrost_types::ThrottleScope::Account);
    }
    let body = body.trim();
    if !body.is_empty() {
        builder = builder.text(DiagnosticText::support_only(body.to_string()));
    }
    builder
        .try_build()
        .expect("valid account error classification")
}

// This is a DAV aggregation policy, not a general ordering of engine actions.
// Its production inputs are multiget, filtered-query and directory-query legs:
// HTTP failures, malformed XML, and member statuses. Cursor invalidation is
// minted only by CalDAV sync_events, a single sync-collection REPORT whose
// error propagates directly to the account; it never enters this fold. Neither
// these legs nor their net context (which has no cursor scope) mint DisableScope.
// Moving this policy onto RecoveryClass would imply an ordering valid for
// unrelated engine directives without a reachable mixed-leg case to define it.
// If a cursor-bearing leg is added, preserve its scope/action explicitly rather
// than assuming this local severity table can order restart and termination.
/// Keep whichever of two errors a consumer most needs to act on.
///
/// A multi-leg REPORT can fail several ways at once; the surviving error is the
/// one whose recovery class demands the most, so a 401 in one leg is never
/// buried under a retryable 503 in another.
#[must_use]
pub fn worse_recovery(
    current: Option<AccountError>,
    candidate: AccountError,
) -> Option<AccountError> {
    match current {
        Some(existing)
            if recovery_rank(existing.recovery()) >= recovery_rank(candidate.recovery()) =>
        {
            Some(existing)
        }
        _ => Some(candidate),
    }
}

#[must_use]
pub fn recovery_rank(class: &RecoveryClass) -> u8 {
    match class {
        RecoveryClass::AuthLost => 4,
        RecoveryClass::NeedsAdminConsent { .. }
        | RecoveryClass::NeedsPolicyChange
        | RecoveryClass::NoPermission { .. } => 3,
        RecoveryClass::Retry(_) => 0,
        RecoveryClass::Reconcile(_) | RecoveryClass::Engine(_) => 1,
        RecoveryClass::Unsupported(_)
        | RecoveryClass::ClientBug
        | RecoveryClass::ProviderContractViolation
        | RecoveryClass::ProviderRefused
        | RecoveryClass::UnknownPermanent
        | RecoveryClass::InternalFailure => 2,
        // RecoveryClass is non-exhaustive. An unknown future class must win
        // rather than being silently ranked below a known terminal failure.
        _ => u8::MAX,
    }
}

/// The DAV precondition elements a server names when it refuses to RUN a
/// report's filter, as opposed to refusing the caller.
///
/// Matched case-insensitively against the response body, which is the only
/// place the precondition appears: RFC 4918 s16 carries it inside a
/// `DAV:error` document, and the status alone (403) cannot tell "I do not
/// implement that filter" apart from "you may not read this collection".
const FILTER_PRECONDITIONS: [&str; 4] = [
    "supported-filter",
    "supported-collation",
    "valid-filter",
    "supported-report",
];

/// Does this REPORT rejection mean "I will not run that filter" rather than
/// "no"?
///
/// The filtered listing lanes push their predicate to the server so a page can
/// be sliced before anything is hydrated. A server that will not run the filter
/// must not fail the call: the lane degrades to listing the collection and
/// matching locally, which is what both crates did unconditionally before.
/// Only three answers mean that, and each is a statement about the REPORT
/// rather than about the credential:
///
/// - `400`, the catch-all for a body the server would not process. Servers that
///   implement no query filter at all answer this, with no precondition
///   element to inspect.
/// - `501`, an explicit "not implemented".
/// - `405`, the server refusing the REPORT method itself. It is the same
///   answer one layer down - there is no query to run - and it reaches these
///   lanes from static-ish or proxy-fronted deployments. Degrading it matters
///   most for the cursor listing lane, whose alternative is failing a sync that
///   the unfiltered PROPFIND would have served.
/// - `403` naming one of [`FILTER_PRECONDITIONS`]. A bare 403 is deliberately
///   NOT degraded: it is far more often a permission refusal, and swallowing it
///   into a whole-collection walk would replace a classified `NoPermission`
///   with whatever the listing happens to answer.
///
/// `401` is never here. A stale credential must reach the consumer as a
/// reauthorize signal, not as a quietly narrower search.
#[must_use]
pub fn filter_unsupported(status: StatusCode, body: &str) -> bool {
    if status == StatusCode::BAD_REQUEST
        || status == StatusCode::NOT_IMPLEMENTED
        || status == StatusCode::METHOD_NOT_ALLOWED
    {
        return true;
    }
    if status != StatusCode::FORBIDDEN {
        return false;
    }
    let body = body.to_ascii_lowercase();
    FILTER_PRECONDITIONS
        .iter()
        .any(|precondition| body.contains(precondition))
}

/// Does this well-known PROBE failure mean "this is not a discovery endpoint"?
///
/// Applied to the `/.well-known/<dialect>` attempt ONLY, never to a request
/// against the configured base URL, so widening it cannot mask a real failure
/// of the account itself. Its one caller is the shared principal walk,
/// `DavDispatch::discover_current_user_principal`, which feeds it the raw `Err`
/// of a principal lookup whose XML decode is part of the lookup, so a body that
/// will not parse reaches it as an error like any other probe answer.
///
/// A 401 or 403 still fails the open: those are answers from a discovery
/// endpoint that exists and refused the credential, and quietly retrying the
/// base URL would turn a reauthorization signal into a confusing later failure.
///
/// Five answers mean the endpoint simply is not there:
/// - 404, the spec-correct one (`NotFound` naming the dialect's own resource).
/// - 405 Method Not Allowed, what a static site or a proxy in front of the DAV
///   path answers a PROPFIND on the origin root with. Accepting only 404 failed
///   the open on deployments whose configured base URL works perfectly.
/// - A locally-refused redirect (`Request(Malformed)` from the redirect walk).
///   RFC 6764's canonical shape is a well-known that redirects to another host,
///   and that host cannot be admitted to the credential-origin set before
///   discovery has authenticated anything - so the walk refuses it locally, and
///   that refusal is evidence about the probe, not about the account.
/// - A body that will not parse as DAV XML (`Protocol(ParseFailed)`). A front
///   end sitting on the origin root answers a PROPFIND with `200 text/html` and
///   its index page as often as it answers 404 or 405 - the same deployment
///   shape, one status apart - and an HTML document fails the XML parse rather
///   than decoding as an empty multistatus. Without this arm that deployment
///   fails the open while the identical one answering an empty 207 falls back
///   and works.
/// - A redirect the walk will not follow (`Protocol(ContractViolation)`): a
///   chain past the hop cap, or a `Location` that will not resolve or decode.
///   These classify as the transport's own redirect failures do, a provider
///   contract violation. That is honest about blame, and without this arm it
///   would silently narrow the predicate: as `Request(Malformed)` they used to
///   fall back through the refused-redirect arm above.
///   A well-known that loops is a probe answer nobody can use, the same shape as
///   one that will not parse, and the fallback's fresh lookup against the
///   configured base cannot inherit anything from it. On the probe path nothing
///   else mints this kind: redirects are disabled in the transport, so its own
///   redirect loop never runs, and the dispatcher maps an oversized body to
///   `Protocol(PartialResponse)` before the transport's mapping is reached.
///
/// Do NOT narrow the parse-failure arm. ANY `Protocol(ParseFailed)` from the
/// well-known principal lookup triggers the fallback, INCLUDING malformed or truncated DAV
/// XML from a genuine discovery endpoint. The predicate cannot distinguish that
/// from a non-DAV body: both arrive here as an XML parse failure with no headers
/// available, so the distinction is unobtainable at this seam rather than merely
/// unimplemented. It is safe because the fallback cannot accept bad data - it
/// performs a FRESH principal lookup against the configured base URL, requires
/// its result, and runs the subsequent discovery normally; nothing partially
/// parsed from the failed probe is reused, no origin from it is admitted, and
/// both the base leg and everything after the principal still propagate their
/// failures. The most a fallback can do is prefer the user-configured endpoint
/// over a probe that would not parse. The cost is a lost DIAGNOSTIC: an operator
/// does not learn that the well-known endpoint is serving truncated XML. That
/// cost is deliberately accepted here; it is not an open defect.
///
/// The arm stays safe precisely because it is probe-scoped: a garbage document
/// from the CONFIGURED base URL is a real contract violation and still fails,
/// because the base leg never consults this predicate.
#[must_use]
pub fn should_fallback_discovery(error: &AccountError, protocol: DavProtocol) -> bool {
    match error.kind() {
        AccountErrorKind::NotFound(resource) => *resource == protocol.resource(),
        AccountErrorKind::Request(RequestErrorKind::Malformed)
        | AccountErrorKind::Protocol(
            ProtocolErrorKind::ParseFailed | ProtocolErrorKind::ContractViolation,
        )
        | AccountErrorKind::Server(ServerErrorKind::Error { status: Some(405) }) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_headers_preserve_retry_timing_and_account_throttle_scope() {
        use bifrost_types::{RetryHint, ThrottleScope};
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::RETRY_AFTER,
            "300".parse().expect("valid header"),
        );
        for protocol in [DavProtocol::CalDav, DavProtocol::CardDav] {
            for status in [429, 503, 507, 408, 502, 504] {
                let error = status_error(
                    &headers,
                    AccountOperation::EventGet,
                    StatusCode::from_u16(status).expect("valid status"),
                    String::new(),
                    protocol,
                );
                let hint = Some(RetryHint::After(std::time::Duration::from_secs(300)));
                assert!(error.chain().iter().any(|cause| matches!(cause,
                    Cause::Server(ServerCause::RateLimited { retry_hint }
                        | ServerCause::Unavailable { retry_hint }
                        | ServerCause::QuotaExhausted { retry_hint }) if *retry_hint == hint)));
                let RecoveryClass::Retry(advice) = error.recovery() else {
                    panic!("temporary/quota status must retain retry advice: {error:?}");
                };
                assert_eq!(advice.retry_hint, hint);
                if matches!(status, 429 | 507) {
                    assert_eq!(advice.throttle_scope, Some(ThrottleScope::Account));
                }
            }
            for status in [400, 422] {
                assert_eq!(
                    status_error(
                        &headers,
                        AccountOperation::EventGet,
                        StatusCode::from_u16(status).expect("valid status"),
                        String::new(),
                        protocol
                    )
                    .recovery(),
                    &RecoveryClass::ClientBug
                );
            }
        }
    }

    #[test]
    fn status_headers_preserve_absolute_and_elapsed_retry_dates() {
        use bifrost_types::RetryHint;
        // Fixed dates keep this independent of the wall clock. Both must stay At,
        // even when the date has elapsed, rather than being dropped or relativized.
        for (value, seconds) in [
            ("Thu, 01 Jan 1970 00:00:00 GMT", 0),
            ("Fri, 01 Jan 2100 00:00:00 GMT", 4_102_444_800),
        ] {
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::RETRY_AFTER,
                value.parse().expect("valid header"),
            );
            let hint = Some(RetryHint::At(
                std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds),
            ));
            for protocol in [DavProtocol::CalDav, DavProtocol::CardDav] {
                for status in [429, 503, 507, 408, 502, 504] {
                    let error = status_error(
                        &headers,
                        AccountOperation::EventGet,
                        StatusCode::from_u16(status).expect("valid status"),
                        String::new(),
                        protocol,
                    );
                    assert!(error.chain().iter().any(|cause| matches!(cause,
                        Cause::Server(ServerCause::RateLimited { retry_hint }
                            | ServerCause::Unavailable { retry_hint }
                            | ServerCause::QuotaExhausted { retry_hint }) if *retry_hint == hint)));
                    let RecoveryClass::Retry(advice) = error.recovery() else {
                        panic!("temporary/quota status must retain retry advice: {error:?}");
                    };
                    assert_eq!(advice.retry_hint, hint);
                }
            }
        }
    }

    /// Migrated from both crates, which each pinned it for one dialect. The
    /// probe falls back on "not a discovery endpoint" answers and only those: a
    /// credential refusal must still fail the open.
    #[test]
    fn discovery_falls_back_on_not_found_405_and_a_refused_redirect() {
        for protocol in [DavProtocol::CalDav, DavProtocol::CardDav] {
            let status = |status| {
                status_error(
                    &reqwest::header::HeaderMap::new(),
                    AccountOperation::Discover,
                    status,
                    String::new(),
                    protocol,
                )
            };

            assert!(!should_fallback_discovery(
                &status(StatusCode::UNAUTHORIZED),
                protocol
            ));
            assert!(!should_fallback_discovery(
                &status(StatusCode::FORBIDDEN),
                protocol
            ));
            assert!(!should_fallback_discovery(
                &status(StatusCode::INTERNAL_SERVER_ERROR),
                protocol
            ));

            assert!(should_fallback_discovery(
                &status(StatusCode::NOT_FOUND),
                protocol
            ));
            // A static site or proxy in front of the DAV path answers a
            // PROPFIND on the origin root with 405.
            assert!(should_fallback_discovery(
                &status(StatusCode::METHOD_NOT_ALLOWED),
                protocol
            ));
            // RFC 6764's canonical redirect to another host, refused locally
            // by the credential-origin gate before any request went out.
            assert!(should_fallback_discovery(
                &local_error(
                    AccountOperation::Discover,
                    "redirect to an unadmitted origin",
                    protocol,
                ),
                protocol
            ));
            // The origin root answered 200 with a body that will not parse as
            // DAV XML. An index page is the readable case; a truncated DAV
            // document reaches the predicate identically, which is why it
            // admits both.
            assert!(should_fallback_discovery(
                &parse_error(AccountOperation::Discover, "XML parse error", protocol),
                protocol
            ));
            // A redirect the walk will not follow, classified as the transport
            // classifies its own. It fell back as `Request(Malformed)` before
            // it was reclassified, and must keep doing so.
            assert!(should_fallback_discovery(
                &into_redirect_loop(protocol),
                protocol
            ));
            // A missing discovery property is minted only by the base leg and
            // the steps after the principal, never by the probe (a probe naming
            // no principal falls back as an empty success), so it must not
            // widen the predicate.
            assert!(!should_fallback_discovery(
                &missing_field_error(
                    AccountOperation::Discover,
                    "missing calendar-home-set",
                    protocol,
                ),
                protocol
            ));
        }
    }

    fn into_redirect_loop(protocol: DavProtocol) -> AccountError {
        bifrost_net::into_account_error_with_throttle_scope(
            bifrost_net::Error::RedirectLoop { hops: 11 },
            bifrost_net::NetErrorContext {
                provider: None,
                protocol: protocol.protocol(),
                operation: AccountOperation::Discover,
                scope: None,
            },
            None,
        )
    }

    fn acknowledged(error: &AccountError) -> bool {
        error.chain().iter().any(|cause| {
            matches!(
                cause,
                Cause::Attempt(attempt)
                    if attempt.transmission_state == TransmissionState::Acknowledged
            )
        })
    }

    /// A status the server answered, and a body it sent that will not parse,
    /// both carry an ACKNOWLEDGED attempt; a local refusal and a pre-send
    /// transport failure do not.
    ///
    /// Against the code before the attempt was stamped, every `status_error` and
    /// `parse_error` assertion fails: neither pushed an attempt, so the chain
    /// read as `Unsent`. The recovery half pins that the evidence moves no
    /// class: a 503 on a NON-idempotent create still retries (an acknowledged
    /// answer is a refusal, not a drop), and a 404 is still a refusal.
    #[test]
    fn a_server_answer_carries_acknowledged_transmission_evidence() {
        for protocol in [DavProtocol::CalDav, DavProtocol::CardDav] {
            for status in [
                StatusCode::BAD_REQUEST,
                StatusCode::UNAUTHORIZED,
                StatusCode::FORBIDDEN,
                StatusCode::NOT_FOUND,
                StatusCode::CONFLICT,
                StatusCode::PRECONDITION_FAILED,
                StatusCode::TOO_MANY_REQUESTS,
                StatusCode::INTERNAL_SERVER_ERROR,
                StatusCode::SERVICE_UNAVAILABLE,
                StatusCode::INSUFFICIENT_STORAGE,
            ] {
                let error = status_error(
                    &reqwest::header::HeaderMap::new(),
                    AccountOperation::EventCreate,
                    status,
                    String::new(),
                    protocol,
                );
                assert!(
                    acknowledged(&error),
                    "{protocol:?} {status} is a server answer: {error:?}"
                );
            }
            assert!(acknowledged(&parse_error(
                AccountOperation::Discover,
                "XML parse error",
                protocol
            )));
            // A resource the server returned holding nothing of the dialect's
            // kind is read out of its answer too. Carried no attempt before.
            let missing_resource = not_found_error(AccountOperation::EventGet, "one.ics", protocol);
            assert!(acknowledged(&missing_resource), "{missing_resource:?}");
            assert_eq!(missing_resource.recovery(), &RecoveryClass::ProviderRefused);
            assert!(acknowledged(&missing_field_error(
                AccountOperation::Discover,
                "missing current-user-principal",
                protocol
            )));
            assert!(!acknowledged(&local_error(
                AccountOperation::EventCreate,
                "refused before the wire",
                protocol
            )));
            assert!(!acknowledged(&transport_error(
                AccountOperation::EventCreate,
                "token read failed",
                protocol
            )));

            let unavailable = status_error(
                &reqwest::header::HeaderMap::new(),
                AccountOperation::EventCreate,
                StatusCode::SERVICE_UNAVAILABLE,
                String::new(),
                protocol,
            );
            assert!(
                matches!(unavailable.recovery(), RecoveryClass::Retry(_)),
                "{protocol:?}: an answered 503 is retried, never reconciled: {:?}",
                unavailable.recovery()
            );
            let missing = status_error(
                &reqwest::header::HeaderMap::new(),
                AccountOperation::EventCreate,
                StatusCode::NOT_FOUND,
                String::new(),
                protocol,
            );
            assert_eq!(missing.recovery(), &RecoveryClass::ProviderRefused);
        }
    }

    /// A 404 names the dialect's own resource, so the predicate keys on it: a
    /// CardDAV-stamped not-found is not a CalDAV probe answer.
    #[test]
    fn discovery_fallback_reads_the_dialects_own_not_found() {
        let contact_missing = status_error(
            &reqwest::header::HeaderMap::new(),
            AccountOperation::Discover,
            StatusCode::NOT_FOUND,
            String::new(),
            DavProtocol::CardDav,
        );
        assert!(should_fallback_discovery(
            &contact_missing,
            DavProtocol::CardDav
        ));
        assert!(!should_fallback_discovery(
            &contact_missing,
            DavProtocol::CalDav
        ));
    }

    #[test]
    fn a_refused_filter_is_told_apart_from_a_refused_caller() {
        assert!(filter_unsupported(StatusCode::BAD_REQUEST, ""));
        assert!(filter_unsupported(StatusCode::NOT_IMPLEMENTED, ""));
        // A server that refuses REPORT outright will not run the filter either.
        assert!(filter_unsupported(StatusCode::METHOD_NOT_ALLOWED, ""));
        assert!(filter_unsupported(
            StatusCode::FORBIDDEN,
            "<D:error xmlns:D=\"DAV:\"><C:supported-filter/></D:error>"
        ));
        assert!(filter_unsupported(
            StatusCode::FORBIDDEN,
            "<D:error><C:SUPPORTED-COLLATION/></D:error>"
        ));
        // A bare permission refusal keeps its classification.
        assert!(!filter_unsupported(StatusCode::FORBIDDEN, "go away"));
        // A stale credential must stay a reauthorize signal.
        assert!(!filter_unsupported(StatusCode::UNAUTHORIZED, ""));
        assert!(!filter_unsupported(
            StatusCode::SERVICE_UNAVAILABLE,
            "supported-filter"
        ));
    }

    /// The two dialects differ in exactly the three values `DavProtocol`
    /// carries, and in nothing else.
    #[test]
    fn the_dialects_differ_only_in_resource_protocol_and_field() {
        assert_eq!(DavProtocol::CalDav.protocol(), Protocol::CalDav);
        assert_eq!(DavProtocol::CardDav.protocol(), Protocol::CardDav);
        assert_eq!(DavProtocol::CalDav.resource(), ResourceKind::Calendar);
        assert_eq!(DavProtocol::CardDav.resource(), ResourceKind::Contact);
        assert_eq!(DavProtocol::CalDav.field(), "caldav");
        assert_eq!(DavProtocol::CardDav.field(), "carddav");
        assert_eq!(DavProtocol::CalDav.well_known_service(), "caldav");
        assert_eq!(DavProtocol::CardDav.well_known_service(), "carddav");
    }

    /// Migrated from both crates, which each carried it for one dialect. The
    /// loop is the point: the ladder must answer identically for both.
    #[test]
    fn status_error_maps_write_conflicts() {
        for protocol in [DavProtocol::CalDav, DavProtocol::CardDav] {
            for status in [
                StatusCode::CONFLICT,
                StatusCode::PRECONDITION_FAILED,
                StatusCode::LOCKED,
            ] {
                let error = status_error(
                    &reqwest::header::HeaderMap::new(),
                    AccountOperation::EventUpdate,
                    status,
                    String::new(),
                    protocol,
                );
                assert!(
                    matches!(error.kind(), AccountErrorKind::ConcurrencyConflict),
                    "{protocol:?} {status} must be a write conflict"
                );
            }
        }
    }

    /// Migrated from `bifrost-caldav`, which was the only crate still pinning
    /// the transient and quota statuses after the merge.
    #[test]
    fn status_error_maps_transient_and_quota_statuses() {
        for protocol in [DavProtocol::CalDav, DavProtocol::CardDav] {
            let kind = |status| {
                status_error(
                    &reqwest::header::HeaderMap::new(),
                    AccountOperation::EventUpdate,
                    status,
                    String::new(),
                    protocol,
                )
            };
            assert!(matches!(
                kind(StatusCode::TOO_MANY_REQUESTS).kind(),
                AccountErrorKind::Server(ServerErrorKind::RateLimited)
            ));
            assert!(matches!(
                kind(StatusCode::SERVICE_UNAVAILABLE).kind(),
                AccountErrorKind::Server(ServerErrorKind::Unavailable)
            ));
            assert!(matches!(
                kind(StatusCode::INSUFFICIENT_STORAGE).kind(),
                AccountErrorKind::Server(ServerErrorKind::QuotaExhausted)
            ));
        }
    }

    #[test]
    fn status_error_names_the_dialects_own_resource() {
        let calendar = status_error(
            &reqwest::header::HeaderMap::new(),
            AccountOperation::EventGet,
            StatusCode::NOT_FOUND,
            String::new(),
            DavProtocol::CalDav,
        );
        assert!(matches!(
            calendar.kind(),
            AccountErrorKind::NotFound(ResourceKind::Calendar)
        ));
        let contact = status_error(
            &reqwest::header::HeaderMap::new(),
            AccountOperation::ContactGet,
            StatusCode::NOT_FOUND,
            String::new(),
            DavProtocol::CardDav,
        );
        assert!(matches!(
            contact.kind(),
            AccountErrorKind::NotFound(ResourceKind::Contact)
        ));
    }

    /// Migrated from `bifrost-caldav` with the ranking it pins. It was the
    /// stronger of the two copies - it enumerates every nameable variant rather
    /// than sampling three - so it is the one that survived the merge.
    ///
    /// The `_ =>` arm cannot be pinned hermetically: `RecoveryClass` is
    /// `#[non_exhaustive]` and lives in `bifrost-types`, so no test outside it
    /// can name a variant this `match` does not already list. What is pinnable
    /// is that every variant we CAN name ranks strictly below the sentinel,
    /// which is what makes an unknown one win by construction.
    #[test]
    fn recovery_ranks_order_from_retryable_up_to_auth_lost() {
        use bifrost_types::{EngineDirective, RetryAdvice, RetryDisposition, RetryReason};

        let retry = RecoveryClass::Retry(RetryAdvice::new(
            RetryDisposition::SameRequest,
            None,
            RetryReason::Transport,
            None,
        ));
        let engine = RecoveryClass::Engine(EngineDirective::RestartAccount);
        let terminal = [
            RecoveryClass::Unsupported(AccountOperation::EventSearch),
            RecoveryClass::ClientBug,
            RecoveryClass::ProviderContractViolation,
            RecoveryClass::ProviderRefused,
            RecoveryClass::UnknownPermanent,
            // Named, so it ranks with the permanent failures rather than
            // falling to the unknown-class sentinel that outranks AuthLost.
            RecoveryClass::InternalFailure,
        ];
        let consent = [
            RecoveryClass::NeedsAdminConsent { needed: "scope" },
            RecoveryClass::NeedsPolicyChange,
            RecoveryClass::NoPermission { resource: None },
        ];

        assert!(recovery_rank(&retry) < recovery_rank(&engine));
        for class in &terminal {
            assert!(
                recovery_rank(&engine) < recovery_rank(class),
                "{class:?} must outrank an engine directive"
            );
            for stronger in &consent {
                assert!(
                    recovery_rank(class) < recovery_rank(stronger),
                    "{stronger:?} must outrank {class:?}"
                );
            }
        }
        for class in &consent {
            assert!(
                recovery_rank(class) < recovery_rank(&RecoveryClass::AuthLost),
                "AuthLost must outrank {class:?}"
            );
            // Every named variant sits below the catch-all sentinel, so an
            // unknown future class escalates rather than being buried.
            assert!(recovery_rank(class) < u8::MAX);
        }
        assert!(recovery_rank(&RecoveryClass::AuthLost) < u8::MAX);
    }

    /// A retryable failure in one leg must never bury a reauthorization in
    /// another, whichever order the legs complete in. Migrated from the two
    /// crates' `the_worst_recovery_class_wins_whatever_the_chunk_order`, and
    /// now run against BOTH dialects rather than one.
    #[test]
    fn the_worst_recovery_class_wins_whatever_the_leg_order() {
        for protocol in [DavProtocol::CalDav, DavProtocol::CardDav] {
            let error = |status| {
                status_error(
                    &reqwest::header::HeaderMap::new(),
                    AccountOperation::EventSearch,
                    status,
                    "refused".to_string(),
                    protocol,
                )
            };
            let auth_first = worse_recovery(
                Some(error(StatusCode::UNAUTHORIZED)),
                error(StatusCode::SERVICE_UNAVAILABLE),
            );
            let transient_first = worse_recovery(
                Some(error(StatusCode::SERVICE_UNAVAILABLE)),
                error(StatusCode::UNAUTHORIZED),
            );

            for surviving in [auth_first, transient_first] {
                assert_eq!(
                    surviving.expect("kept").recovery(),
                    &RecoveryClass::AuthLost,
                    "{protocol:?} must keep the reauthorization signal"
                );
            }
        }
    }
}

/// Keep generic transport throttles on the same account budget as DAV statuses.
pub(crate) fn net_account_error(
    error: bifrost_net::Error,
    ctx: bifrost_net::NetErrorContext,
) -> AccountError {
    bifrost_net::into_account_error_with_throttle_scope(
        error,
        ctx,
        Some(bifrost_types::ThrottleScope::Account),
    )
}

#[cfg(test)]
mod net_policy_tests {
    use super::*;

    #[test]
    fn generic_transport_throttle_matches_dav_status_policy() {
        for protocol in [DavProtocol::CalDav, DavProtocol::CardDav] {
            let error = net_account_error(
                bifrost_net::Error::Status {
                    code: reqwest::StatusCode::TOO_MANY_REQUESTS,
                    headers: reqwest::header::HeaderMap::new(),
                    body: bytes::Bytes::new(),
                },
                bifrost_net::NetErrorContext {
                    provider: None,
                    protocol: protocol.protocol(),
                    operation: AccountOperation::Discover,
                    scope: None,
                },
            );
            assert_eq!(
                error.telemetry_fields().throttle_scope,
                Some(bifrost_types::ThrottleScope::Account)
            );
        }
    }
}
