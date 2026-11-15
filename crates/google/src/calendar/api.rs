use crate::Result;
use crate::client::{GmailClient, parse_json_response};
use crate::quota::ApiSurface;
use serde::{Serialize, de::DeserializeOwned};

/// A service-bound view: request attribution never depends on URL spelling.
pub(crate) struct CalendarApi<'a>(pub(crate) &'a GmailClient);

impl CalendarApi<'_> {
    pub(crate) async fn execute<B: Serialize>(
        &self,
        url: &str,
        method: reqwest::Method,
        body: Option<&B>,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<bifrost_net::Response> {
        self.execute_if_match(url, method, body, None, replay).await
    }

    pub(crate) async fn execute_if_match<B: Serialize>(
        &self,
        url: &str,
        method: reqwest::Method,
        body: Option<&B>,
        expected: Option<&str>,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<bifrost_net::Response> {
        self.0
            .execute_on(ApiSurface::Calendar, url, method, body, expected, replay)
            .await
    }

    pub(crate) async fn get<T: DeserializeOwned>(&self, url: &str) -> Result<T> {
        let response = self
            .execute(
                url,
                reqwest::Method::GET,
                None::<&()>,
                bifrost_net::ReplayPolicy::Idempotent,
            )
            .await?;
        parse_json_response(response, ApiSurface::Calendar).await
    }

    pub(crate) async fn post<T: DeserializeOwned, B: Serialize>(
        &self,
        url: &str,
        body: &B,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<T> {
        let response = self
            .execute(url, reqwest::Method::POST, Some(body), replay)
            .await?;
        parse_json_response(response, ApiSurface::Calendar).await
    }
}

pub(crate) fn event_url(base: &str, calendar_id: &str, event_id: &str) -> String {
    format!(
        "{base}/calendars/{}/events/{}",
        bifrost_net::url::encode_path_component(calendar_id),
        bifrost_net::url::encode_path_component(event_id)
    )
}

pub(crate) fn event_move_url(
    base: &str,
    calendar_id: &str,
    event_id: &str,
    target_calendar_id: &str,
) -> String {
    format!(
        "{}/move?destination={}",
        event_url(base, calendar_id, event_id),
        // Query encoder, not the path encoder: the two differ on the complete
        // `.` and `..` components, which the path encoder double-escapes
        // against WHATWG path navigation. A calendar id is a query value here.
        bifrost_net::url::encode_query_value(target_calendar_id)
    )
}

#[cfg(test)]
mod url_tests {
    use super::*;
    #[test]
    fn event_move_url_encodes_destination_calendar() {
        // The path and query encoders differ on exactly two inputs: the
        // complete `.` and `..` components, which the path encoder
        // double-escapes so the WHATWG parser cannot resolve them as
        // navigation. `destination` is a query value, where dots carry
        // no structural meaning, so both must survive verbatim.
        let base = "https://www.googleapis.com/calendar/v3";
        let url = event_move_url(base, "primary", "event/1", ".");
        assert!(url.contains("/calendars/primary/events/event%2F1/move"));
        assert!(url.ends_with("destination=."));

        let parent = event_move_url(base, "primary", "event/1", "..");
        assert!(parent.ends_with("destination=.."));

        // Everything else encodes identically under both, so a value
        // carrying query delimiters still pins that the destination is
        // escaped at all.
        let delimited = event_move_url(base, "primary", "event/1", "a&b=c+d");
        assert!(delimited.ends_with("destination=a%26b%3Dc%2Bd"));
    }
}
