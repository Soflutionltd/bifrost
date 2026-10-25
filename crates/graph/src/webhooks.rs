//! Microsoft Graph change notification subscription CRUD.

use crate::client::Resource;
use serde::{Deserialize, Serialize};

use crate::error::GraphError;

use super::client::GraphClient;

// The subscription resource documents 10,080 minutes as the maximum for
// Outlook message, event, and contact subscriptions without resource data.
// These requests create basic notifications, so use that full lifetime for
// more renewal margin. https://learn.microsoft.com/en-us/graph/api/resources/subscription?view=graph-rest-1.0
const DEFAULT_EXPIRATION_MINUTES: u32 = 10_080;
const MAX_EXPIRATION_MINUTES: u32 = 10_080;

fn requested_expiration_minutes(minutes: Option<u32>) -> u32 {
    minutes
        .unwrap_or(DEFAULT_EXPIRATION_MINUTES)
        .min(MAX_EXPIRATION_MINUTES)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphSubscription {
    change_type: String,
    notification_url: String,
    resource: String,
    expiration_date_time: String,
    client_state: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SubscriptionResponse {
    pub(crate) id: String,
    pub(crate) expiration_date_time: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RenewSubscriptionRequest {
    expiration_date_time: String,
}

pub(crate) async fn create_subscription(
    client: &GraphClient,
    resource: &str,
    notification_url: &str,
    client_state: &str,
    expiration_minutes: Option<u32>,
) -> Result<SubscriptionResponse, GraphError> {
    let minutes = requested_expiration_minutes(expiration_minutes);

    let body = GraphSubscription {
        change_type: "created,updated,deleted".to_string(),
        notification_url: notification_url.to_string(),
        resource: resource.to_string(),
        expiration_date_time: compute_expiry_iso8601(minutes),
        client_state: client_state.to_string(),
    };

    let response: SubscriptionResponse = client
        .post(
            Resource::Unmetered,
            "/subscriptions",
            &body,
            bifrost_net::ReplayPolicy::Never,
        )
        .await?;
    tracing::info!(
        "[Graph webhooks] Created subscription {} for resource '{}' (expires {})",
        response.id,
        resource,
        response.expiration_date_time
    );
    Ok(response)
}

pub(crate) async fn renew_subscription(
    client: &GraphClient,
    subscription_id: &str,
    expiration_minutes: Option<u32>,
) -> Result<String, GraphError> {
    let minutes = requested_expiration_minutes(expiration_minutes);
    let requested_expiry = compute_expiry_iso8601(minutes);
    let body = RenewSubscriptionRequest {
        expiration_date_time: requested_expiry,
    };

    let response: SubscriptionResponse = client
        .patch_json(
            Resource::Unmetered,
            &format!("/subscriptions/{subscription_id}"),
            &body,
            bifrost_net::ReplayPolicy::Idempotent,
        )
        .await?;
    tracing::info!(
        "[Graph webhooks] Renewed subscription {subscription_id} (new expiry: {})",
        response.expiration_date_time
    );
    Ok(response.expiration_date_time)
}

/// Did the server answer "this subscription no longer exists"?
///
/// Reads the status through `GraphError::response_status`, which sees both
/// shapes a Graph failure arrives in. Matching only `GraphError::Response`
/// here made this predicate permanently false on the live path - a REST
/// 404/410 is a `bifrost_net::Error::Status`, never a response - so
/// `delete_subscription` failed on an already-vanished row and the renewal
/// worker never took its recreate branch.
pub(crate) fn subscription_is_gone(error: &GraphError) -> bool {
    matches!(
        error.response_status(),
        Some(reqwest::StatusCode::NOT_FOUND | reqwest::StatusCode::GONE)
    )
}

pub(crate) async fn delete_subscription(
    client: &GraphClient,
    subscription_id: &str,
) -> Result<(), GraphError> {
    let server_result = client
        .delete(
            Resource::Unmetered,
            &format!("/subscriptions/{subscription_id}"),
            bifrost_net::ReplayPolicy::IdempotentUnlessConditional,
        )
        .await;
    if let Err(error) = server_result {
        if !subscription_is_gone(&error) {
            return Err(error);
        }
        tracing::info!(
            "[Graph webhooks] Subscription {subscription_id} already gone on server (404)"
        );
    }

    tracing::info!("[Graph webhooks] Deleted subscription {subscription_id}");
    Ok(())
}

fn compute_expiry_iso8601(minutes: u32) -> String {
    let now = jiff::Timestamp::now();
    let at = jiff::Span::new()
        .try_minutes(i64::from(minutes))
        .and_then(|span| now.checked_add(span))
        .unwrap_or(now);
    unix_to_iso8601(at.as_second())
}

fn now_unix() -> i64 {
    jiff::Timestamp::now().as_second()
}

/// Second-precision UTC, the form Graph accepts on `expirationDateTime`.
fn unix_to_iso8601(secs: i64) -> String {
    let at = jiff::Timestamp::from_second(secs).unwrap_or(jiff::Timestamp::UNIX_EPOCH);
    jiff::tz::Offset::UTC
        .to_datetime(at)
        .strftime("%Y-%m-%dT%H:%M:%SZ")
        .to_string()
}

/// `None` when the value is not a timestamp this client understands.
///
/// This drives a control loop (the 10-minute renewal tick), so it must have
/// an error channel: an infallible parser that coerces to the epoch reports
/// every unreadable value as long-expired and PATCHes it forever. `jiff`
/// accepts every RFC 3339 form Graph might emit, offsets included, so the
/// `None` lane means genuinely unparseable rather than merely unexpected.
fn parse_iso8601_to_unix(s: &str) -> Option<i64> {
    s.parse::<jiff::Timestamp>()
        .ok()
        .map(jiff::Timestamp::as_second)
}

/// Renewing is the safe direction for an unparseable expiry (the alternative
/// is a subscription that silently dies at its real expiry), so `None` from
/// the parser answers "expiring". The unparseable value itself is a defect
/// worth seeing; the renewal session logs it once per subscription via
/// `ExpiryCheck::Unparseable` rather than once per tick.
pub(crate) fn check_expiry(expiration_iso: &str, threshold_minutes: i64) -> ExpiryCheck {
    let Some(expiry) = parse_iso8601_to_unix(expiration_iso) else {
        return ExpiryCheck::Unparseable;
    };
    if expiry - now_unix() < threshold_minutes * 60 {
        ExpiryCheck::ExpiringSoon
    } else {
        ExpiryCheck::Live
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExpiryCheck {
    Live,
    ExpiringSoon,
    /// Treated as due for renewal; a successful renewal replaces the value
    /// with `compute_expiry_iso8601`'s own output.
    Unparseable,
}

#[cfg(test)]
pub(crate) fn is_expiring_soon(expiration_iso: &str, threshold_minutes: i64) -> bool {
    check_expiry(expiration_iso, threshold_minutes) != ExpiryCheck::Live
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{GraphClient, ScriptedRestResponse};

    #[test]
    fn subscription_request_serialization() {
        let sub = GraphSubscription {
            change_type: "created,updated,deleted".to_string(),
            notification_url: "https://example.com/notify".to_string(),
            resource: "/me/messages".to_string(),
            expiration_date_time: "2024-06-15T12:00:00Z".to_string(),
            client_state: "abc123".to_string(),
        };

        let json = serde_json::to_string(&sub).expect("should serialize");
        assert!(json.contains("\"changeType\":\"created,updated,deleted\""));
        assert!(json.contains("\"notificationUrl\":\"https://example.com/notify\""));
        assert!(json.contains("\"resource\":\"/me/messages\""));
        assert!(json.contains("\"clientState\":\"abc123\""));
    }

    #[test]
    fn subscription_response_deserialization() {
        let json = r#"{
            "id": "sub-123",
            "expirationDateTime": "2024-06-15T12:00:00Z",
            "clientState": "secret123"
        }"#;

        let resp: SubscriptionResponse = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(resp.id, "sub-123");
        assert_eq!(resp.expiration_date_time, "2024-06-15T12:00:00Z");
    }

    #[test]
    fn iso_roundtrip_parse() {
        let iso = unix_to_iso8601(1_718_450_400);
        assert_eq!(parse_iso8601_to_unix(&iso), Some(1_718_450_400));
    }

    #[test]
    fn renewal_threshold_compares_remaining_minutes() {
        // The worker wakes every 10 min and renews inside a 30 min
        // threshold; a subscription with 45 min left must not be renewed,
        // one with 15 min left must be.
        assert!(!is_expiring_soon(&compute_expiry_iso8601(45), 30));
        assert!(is_expiring_soon(&compute_expiry_iso8601(15), 30));
    }

    #[test]
    fn already_expired_subscription_is_expiring_soon() {
        assert!(is_expiring_soon("1970-01-01T00:00:01Z", 30));
    }

    #[test]
    fn fractional_seconds_expiry_parses_like_the_whole_second_form() {
        // Graph emits 7-digit fractional seconds on `expirationDateTime`.
        assert_eq!(
            parse_iso8601_to_unix("2099-01-01T00:00:00.0000000Z"),
            parse_iso8601_to_unix("2099-01-01T00:00:00Z")
        );
        assert!(!is_expiring_soon("2099-01-01T00:00:00.0000000Z", 30));
    }

    /// An unreadable expiry is reported as such rather than coerced to the
    /// epoch. `is_expiring_soon` still answers "renew" - that is the safe
    /// direction, and one successful renewal replaces the value with this
    /// module's own output - but the parse now has a lane to log from.
    #[test]
    fn unreadable_expiry_is_a_parse_failure_and_still_forces_renewal() {
        for bad in ["", "2099-01-01", "not-a-timestamp", "2099-01-01T00:00"] {
            assert_eq!(parse_iso8601_to_unix(bad), None, "{bad}");
            assert!(is_expiring_soon(bad, 30), "{bad}");
        }
    }

    /// The hand-rolled parser this replaced only accepted a `Z` suffix, so a
    /// Graph format change to the offset form would have read as epoch 0 and
    /// driven a PATCH per subscription per tick. Offsets now resolve to the
    /// instant they denote.
    #[test]
    fn numeric_utc_offsets_resolve_to_the_instant_they_denote() {
        let utc = parse_iso8601_to_unix("2026-01-01T10:00:00Z").expect("utc form");
        assert_eq!(
            parse_iso8601_to_unix("2026-01-01T10:00:00+00:00"),
            Some(utc)
        );
        assert_eq!(
            parse_iso8601_to_unix("2026-01-01T05:00:00-05:00"),
            Some(utc)
        );
        assert_eq!(
            parse_iso8601_to_unix("2026-01-01T15:00:00+05:00"),
            Some(utc)
        );
    }

    #[test]
    fn iso_round_trip_spans_leap_days_and_year_boundaries() {
        for iso in [
            "2024-02-29T23:59:59Z",
            "2000-02-29T00:00:00Z",
            "2026-12-31T23:59:59Z",
            "2027-01-01T00:00:00Z",
        ] {
            let secs = parse_iso8601_to_unix(iso).expect(iso);
            assert_eq!(unix_to_iso8601(secs), iso);
        }
    }

    #[tokio::test]
    async fn create_and_renew_request_the_seven_day_outlook_lifetime() {
        assert_eq!(requested_expiration_minutes(None), 10_080);
        assert_eq!(requested_expiration_minutes(Some(10_080)), 10_080);
        assert_eq!(requested_expiration_minutes(Some(20_160)), 10_080);
        assert_eq!(requested_expiration_minutes(Some(60)), 60);

        let client = GraphClient::new("token");
        client.script_rest([
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                serde_json::json!({
                    "id": "sub-created",
                    "expirationDateTime": "2099-01-01T00:00:00Z"
                }),
            ),
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                serde_json::json!({
                    "id": "sub-renewed",
                    "expirationDateTime": "2099-01-01T00:00:00Z"
                }),
            ),
        ]);

        create_subscription(
            &client,
            "/me/events",
            "https://example.com/notify",
            "client-state",
            None,
        )
        .await
        .expect("subscription create succeeds");
        renew_subscription(&client, "sub-created", Some(20_160))
            .await
            .expect("subscription renewal succeeds");

        let requests = client.take_rest_requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].method, "POST");
        assert_eq!(requests[1].method, "PATCH");
        for request in requests {
            let body = request.body.as_ref().expect("subscription request body");
            let expiry = body["expirationDateTime"]
                .as_str()
                .expect("expiration is a string");
            let remaining = parse_iso8601_to_unix(expiry).expect("own output parses") - now_unix();
            let want = 10_080 * 60;
            assert!(
                (remaining - want).abs() <= 2,
                "remaining {remaining} vs want {want}"
            );
        }
    }

    /// `subscription_is_gone` is the gate on the renewal session's
    /// recreate path: only a subscription Graph no longer has may be
    /// replaced by a fresh create. A throttle or an auth failure must
    /// stay a plain renewal failure - recreating on those would mint a
    /// duplicate subscription beside one the server still holds.
    #[test]
    fn only_a_vanished_subscription_routes_to_recreation() {
        let gone = |status| {
            GraphError::Response(crate::error::GraphResponseError::from_response(
                status,
                reqwest::header::HeaderMap::new(),
                bytes::Bytes::new(),
            ))
        };
        assert!(subscription_is_gone(&gone(reqwest::StatusCode::NOT_FOUND)));
        assert!(subscription_is_gone(&gone(reqwest::StatusCode::GONE)));
        for still_there in [
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            reqwest::StatusCode::UNAUTHORIZED,
            reqwest::StatusCode::FORBIDDEN,
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            assert!(!subscription_is_gone(&gone(still_there)), "{still_there}");
        }
        assert!(!subscription_is_gone(&GraphError::Net(
            bifrost_net::Error::Network {
                message: "connection reset".to_string(),
                transmission_state: bifrost_types::TransmissionState::Unsent,
                source: None,
            }
        )));

        // The shape a REST 404/410 actually arrives in: bifrost-net turns a
        // terminal 4xx into `Error::Status` before this crate sees it, so a
        // predicate that reads only `GraphError::Response` is dead on the
        // live path however well it is unit-pinned against that variant.
        let net_gone = |status| {
            GraphError::Net(bifrost_net::Error::Status {
                code: status,
                body: bytes::Bytes::new(),
                headers: reqwest::header::HeaderMap::new(),
            })
        };
        assert!(subscription_is_gone(&net_gone(
            reqwest::StatusCode::NOT_FOUND
        )));
        assert!(subscription_is_gone(&net_gone(reqwest::StatusCode::GONE)));
        assert!(!subscription_is_gone(&net_gone(
            reqwest::StatusCode::FORBIDDEN
        )));
    }

    #[test]
    fn subscription_deletion_accepts_every_vanished_status() {
        let gone = |status| {
            GraphError::Response(crate::error::GraphResponseError::from_response(
                status,
                reqwest::header::HeaderMap::new(),
                bytes::Bytes::new(),
            ))
        };

        assert!(subscription_is_gone(&gone(reqwest::StatusCode::NOT_FOUND)));
        assert!(subscription_is_gone(&gone(reqwest::StatusCode::GONE)));
    }

    /// `clientState` is the only thing an out-of-process receiver can
    /// authenticate a notification with, so it comes from the caller and is
    /// sent verbatim. Nothing in this module mints one: a locally generated
    /// secret is a secret nobody holds.
    #[test]
    fn the_callers_client_state_is_sent_verbatim() {
        let sub = GraphSubscription {
            change_type: "created".to_string(),
            notification_url: "https://example.com/notify".to_string(),
            resource: "/me/messages".to_string(),
            expiration_date_time: "2099-01-01T00:00:00Z".to_string(),
            client_state: "receiver-owned".to_string(),
        };
        let json = serde_json::to_string(&sub).expect("should serialize");
        assert!(
            json.contains("\"clientState\":\"receiver-owned\""),
            "{json}"
        );
    }
}
