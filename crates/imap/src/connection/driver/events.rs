use crate::types::response::{ResponseCode, TaggedResponse, UntaggedResponse};

use super::super::typed_event::TypedEvent;
use super::event_sink;

/// Emit [`TypedEvent::Alert`] or [`TypedEvent::NotificationOverflow`]
/// from an untagged response's response code, if present.
///
/// RFC 3501 Section7.1: `[ALERT]` response codes MUST be presented to the
/// user. RFC 5465 Section5.8: `[NOTIFICATIONOVERFLOW]` means the NOTIFY
/// registration was dropped. Both must reach the event queue regardless
/// of how the response is classified (solicited, unsolicited, or
/// impossible). The driver calls this before classification so the
/// event is emitted even when the response is routed to a consumer.
///
/// Returns `true` if a critical event was extracted (caller may skip
/// the redundant `From<UntaggedResponse>` conversion to avoid
/// double-emitting the same data).
pub(super) fn emit_untagged_response_code_events(
    u: &UntaggedResponse,
    event_sink: &mut event_sink::DriverEventSink,
) -> bool {
    match untagged_critical_event(u) {
        Some(event) => {
            let _ = event_sink.emit(event);
            true
        }
        None => false,
    }
}

/// The ONE definition of which response codes are critical, and of the
/// typed event each one becomes. The untagged and tagged emitters and
/// `has_critical_response_code` all derive from it, so a new critical code
/// added here reaches every one of them - they used to enumerate the codes
/// separately, with nothing linking the lists.
fn critical_code_event(code: Option<&ResponseCode>, text: &str) -> Option<TypedEvent> {
    match code? {
        ResponseCode::Alert => Some(TypedEvent::Alert(text.to_owned())),
        ResponseCode::NotificationOverflow(detail) => Some(TypedEvent::NotificationOverflow {
            code: detail.clone(),
            text: text.to_owned(),
        }),
        _ => None,
    }
}

fn untagged_critical_event(u: &UntaggedResponse) -> Option<TypedEvent> {
    match u {
        UntaggedResponse::Status { code, text, .. } => critical_code_event(code.as_ref(), text),
        _ => None,
    }
}

/// Emit [`TypedEvent::Alert`] or [`TypedEvent::NotificationOverflow`]
/// from a tagged response's response code, if present.
///
/// Tagged responses go to `consumer.finalize_erased()` and never reach
/// the `From<UntaggedResponse>` event conversion. Without this
/// explicit emission, `[ALERT]` and `[NOTIFICATIONOVERFLOW]` in tagged
/// responses would be captured by `apply_side_effects` (state mutation)
/// but never published as typed events (bug B7, I13).
pub(super) fn emit_tagged_response_code_events(
    t: &TaggedResponse,
    event_sink: &mut event_sink::DriverEventSink,
) {
    if let Some(event) = critical_code_event(t.code.as_ref(), &t.text) {
        let _ = event_sink.emit(event);
    }
}

/// Check whether an untagged response carries `[ALERT]` or
/// `[NOTIFICATIONOVERFLOW]`: response codes that were already emitted
/// as typed events in the pre-classification pass.
///
/// Used by the reclassified-as-events loop: if a consumer reclassifies
/// a response that was already emitted by `emit_untagged_response_code_events`,
/// the reclassified copy must be skipped to prevent double-delivery, except
/// for BYE, whose conversion publishes the separate session-end event.
pub(super) fn has_critical_response_code(u: &UntaggedResponse) -> bool {
    untagged_critical_event(u).is_some()
        // LOGOUT's consumer surrenders its expected BYE at finalization.
        // The prologue emitted its critical code, but conversion produces a
        // distinct Bye event, not another Alert/NotificationOverflow. Keep
        // that event even when its response code has already been published.
        && !matches!(
            u,
            UntaggedResponse::Status {
                status: crate::types::response::UntaggedStatus::Bye,
                ..
            }
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::response::UntaggedStatus;

    fn status(status: UntaggedStatus, code: Option<ResponseCode>) -> UntaggedResponse {
        UntaggedResponse::Status {
            status,
            code,
            text: "text".to_owned(),
        }
    }

    /// BYE retains its separate session-end event after code emission.
    #[test]
    fn the_predicate_agrees_with_the_emitter_except_on_bye() {
        let codes = [
            Some(ResponseCode::Alert),
            Some(ResponseCode::NotificationOverflow(None)),
            Some(ResponseCode::ReadOnly),
            None,
        ];
        for code in codes {
            for kind in [UntaggedStatus::Ok, UntaggedStatus::No, UntaggedStatus::Bad] {
                let response = status(kind, code.clone());
                assert_eq!(
                    has_critical_response_code(&response),
                    untagged_critical_event(&response).is_some(),
                    "{response:?}",
                );
            }
            let bye = status(UntaggedStatus::Bye, code);
            assert!(!has_critical_response_code(&bye), "{bye:?}");
        }
        assert!(
            untagged_critical_event(&status(UntaggedStatus::Bye, Some(ResponseCode::Alert)))
                .is_some()
        );
        assert!(
            untagged_critical_event(&status(UntaggedStatus::Ok, Some(ResponseCode::Alert)))
                .is_some()
        );
        assert!(
            untagged_critical_event(&status(UntaggedStatus::Ok, Some(ResponseCode::ReadOnly)))
                .is_none()
        );
    }

    #[tokio::test]
    async fn logout_bye_alert_emits_both_code_and_session_end_once() {
        use crate::connection::test_support::{
            driver_pair, preauth_greeting, read_line, respond, tag_of,
        };

        let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
        let script = async {
            let logout = read_line(&mut server).await;
            assert!(logout.contains("LOGOUT"), "{logout}");
            respond(
                &mut server,
                &format!(
                    "* BYE [ALERT] text\r\n{} OK logged out\r\n",
                    tag_of(&logout),
                ),
            )
            .await;
        };
        let (result, ()) = tokio::join!(conn.logout(), script);
        result.expect("BYE followed by tagged OK completes LOGOUT");
        let events = conn.drain_events().await;
        assert_eq!(events.len(), 2, "{events:?}");
        assert!(matches!(&events[0], TypedEvent::Alert(text) if text == "text"));
        assert!(matches!(&events[1], TypedEvent::Bye {
            code: Some(ResponseCode::Alert), text,
        } if text == "text"));
    }
}
