#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::types::response::{
    Capability, GreetingResponse, GreetingStatus, ResponseCode, StatusKind, TaggedResponse,
};

// ---------------------------------------------------------------------------
// apply_greeting  -  OK (no caps, no alert)
// ---------------------------------------------------------------------------

#[test]
fn apply_greeting_ok_no_caps_no_alert() {
    let mut state = ProtocolState::new();
    let greeting = GreetingResponse {
        status: GreetingStatus::Ok,
        code: None,
        text: "ready".into(),
    };
    let result = state.apply_greeting(&greeting).unwrap();
    assert!(result.is_none(), "no alert expected");
    assert_eq!(state.session_state(), SessionState::NotAuthenticated);
    assert!(state.capabilities().is_empty());
}

// ---------------------------------------------------------------------------
// apply_greeting  -  OK + [CAPABILITY]
// ---------------------------------------------------------------------------

#[test]
fn apply_greeting_ok_with_capabilities() {
    let mut state = ProtocolState::new();
    let caps = vec![Capability::Imap4Rev1, Capability::StartTls];
    let greeting = GreetingResponse {
        status: GreetingStatus::Ok,
        code: Some(ResponseCode::Capability(caps.clone())),
        text: "ready".into(),
    };
    let result = state.apply_greeting(&greeting).unwrap();
    assert!(result.is_none(), "no alert expected");
    assert_eq!(state.session_state(), SessionState::NotAuthenticated);
    assert_eq!(state.capabilities(), &caps);
}

// ---------------------------------------------------------------------------
// apply_greeting  -  OK + [ALERT]
// ---------------------------------------------------------------------------

#[test]
fn apply_greeting_ok_with_alert() {
    let mut state = ProtocolState::new();
    let greeting = GreetingResponse {
        status: GreetingStatus::Ok,
        code: Some(ResponseCode::Alert),
        text: "disk almost full".into(),
    };
    let result = state.apply_greeting(&greeting).unwrap();
    assert_eq!(result, Some("disk almost full".to_owned()));
    assert_eq!(state.session_state(), SessionState::NotAuthenticated);
}

// ---------------------------------------------------------------------------
// apply_greeting  -  PREAUTH
// ---------------------------------------------------------------------------

#[test]
fn apply_greeting_preauth() {
    let mut state = ProtocolState::new();
    let greeting = GreetingResponse {
        status: GreetingStatus::PreAuth,
        code: None,
        text: "logged in via TLS client cert".into(),
    };
    let result = state.apply_greeting(&greeting).unwrap();
    assert!(result.is_none());
    assert_eq!(state.session_state(), SessionState::Authenticated);
}

// ---------------------------------------------------------------------------
// apply_greeting  -  BYE (returns Err)
// ---------------------------------------------------------------------------

#[test]
fn apply_greeting_bye_returns_error() {
    let mut state = ProtocolState::new();
    let greeting = GreetingResponse {
        status: GreetingStatus::Bye,
        code: None,
        text: "server shutting down".into(),
    };
    let result = state.apply_greeting(&greeting);
    assert!(result.is_err(), "BYE greeting must return Err");
    assert_eq!(state.session_state(), SessionState::Logout);
}

// ---------------------------------------------------------------------------
// apply_infrastructure_failure  -  sets Logout
// ---------------------------------------------------------------------------

#[test]
fn apply_infrastructure_failure_sets_logout() {
    let mut state = ProtocolState::new();
    // Start in NotAuthenticated (default).
    assert_eq!(state.session_state(), SessionState::NotAuthenticated);
    state.apply_infrastructure_failure();
    assert_eq!(state.session_state(), SessionState::Logout);
}

// ---------------------------------------------------------------------------
// apply_capability_fetch  -  replaces and clears capabilities
// ---------------------------------------------------------------------------

#[test]
fn apply_capability_fetch_replaces_caps() {
    let mut state = ProtocolState::new();
    assert!(state.capabilities().is_empty());

    let caps = vec![Capability::Imap4Rev1, Capability::Idle];
    state.apply_capability_fetch(caps.clone());
    assert_eq!(state.capabilities(), &caps);

    // Replace with different caps.
    let new_caps = vec![Capability::StartTls];
    state.apply_capability_fetch(new_caps.clone());
    assert_eq!(state.capabilities(), &new_caps);

    // Clear caps.
    state.apply_capability_fetch(Vec::new());
    assert!(state.capabilities().is_empty());
}

// ---------------------------------------------------------------------------
// apply_side_effects  -  BYE is detected independently of its response code
// ---------------------------------------------------------------------------

#[test]
fn bye_carrying_a_capability_code_still_reports_shutdown() {
    let mut state = ProtocolState::new();

    let caps = vec![Capability::Imap4Rev1];
    let resp = Response::Untagged(Box::new(UntaggedResponse::Status {
        status: UntaggedStatus::Bye,
        code: Some(ResponseCode::Capability(caps.clone())),
        text: "shutting down".into(),
    }));
    let digest = state.apply_side_effects(&resp);

    assert!(
        digest.had_bye(),
        "a response code with its own side-effect arm must not shadow the BYE tag"
    );
    assert_eq!(state.session_state(), SessionState::Logout);
    assert_eq!(state.capabilities(), &caps);
}

// ---------------------------------------------------------------------------
// apply_side_effects  -  ENABLED appends to state
// ---------------------------------------------------------------------------

#[test]
fn enabled_appends_to_state() {
    let mut state = ProtocolState::new();
    assert!(state.enabled().is_empty());

    let resp = Response::Untagged(Box::new(UntaggedResponse::Enabled(vec![
        "CONDSTORE".into(),
        "QRESYNC".into(),
    ])));
    let digest = state.apply_side_effects(&resp);

    assert_eq!(state.enabled(), &["CONDSTORE", "QRESYNC"]);
    assert!(!digest.had_bye());
}

// ---------------------------------------------------------------------------
// apply_side_effects  -  ENABLED deduplicates case-insensitively
// ---------------------------------------------------------------------------

#[test]
fn enabled_dedup_case_insensitive() {
    let mut state = ProtocolState::new();

    let resp1 = Response::Untagged(Box::new(UntaggedResponse::Enabled(vec![
        "IMAP4rev2".into(),
    ])));
    let _ = state.apply_side_effects(&resp1);

    // Same extension, different case  -  should NOT duplicate.
    let resp2 = Response::Untagged(Box::new(UntaggedResponse::Enabled(vec![
        "imap4rev2".into(),
    ])));
    let _ = state.apply_side_effects(&resp2);

    assert_eq!(state.enabled().len(), 1);
    assert_eq!(state.enabled()[0], "IMAP4rev2");
}

// ---------------------------------------------------------------------------
// apply_side_effects  -  ENABLED accumulates across calls
// ---------------------------------------------------------------------------

#[test]
fn enabled_accumulates_across_calls() {
    let mut state = ProtocolState::new();

    let resp1 = Response::Untagged(Box::new(UntaggedResponse::Enabled(vec![
        "CONDSTORE".into(),
    ])));
    let _ = state.apply_side_effects(&resp1);

    let resp2 = Response::Untagged(Box::new(UntaggedResponse::Enabled(vec!["QRESYNC".into()])));
    let _ = state.apply_side_effects(&resp2);

    assert_eq!(state.enabled().len(), 2);
    assert!(state.enabled().contains(&"CONDSTORE".to_owned()));
    assert!(state.enabled().contains(&"QRESYNC".to_owned()));
}

// ---------------------------------------------------------------------------
// UNAUTHENTICATE  -  tagged OK transitions to NotAuthenticated (RFC 8437 Section2)
// ---------------------------------------------------------------------------

#[test]
fn unauthenticate_ok_transitions_to_not_authenticated() {
    let mut state = ProtocolState::new();

    // Simulate authenticated state with capabilities and ENABLED extensions.
    let greeting = GreetingResponse {
        status: GreetingStatus::PreAuth,
        code: Some(ResponseCode::Capability(vec![
            Capability::Imap4Rev1,
            Capability::Unauthenticate,
        ])),
        text: "ready".into(),
    };
    state.apply_greeting(&greeting).unwrap();
    assert_eq!(state.session_state(), SessionState::Authenticated);

    // Simulate ENABLED extensions.
    let enabled_resp = Response::Untagged(Box::new(UntaggedResponse::Enabled(vec![
        "CONDSTORE".into(),
    ])));
    let _ = state.apply_side_effects(&enabled_resp);
    assert_eq!(state.enabled().len(), 1);

    // Mark UNAUTHENTICATE in flight.
    state
        .pending
        .insert("A1".into(), PendingEffect::Unauthenticate);

    // Tagged OK -> NotAuthenticated, all per-user state cleared.
    let tagged_ok = Response::Tagged(TaggedResponse {
        tag: "A1".into(),
        status: StatusKind::Ok,
        code: None,
        text: "unauthenticated".into(),
    });
    let _ = state.apply_side_effects(&tagged_ok);

    assert_eq!(state.session_state(), SessionState::NotAuthenticated);
    assert!(state.enabled().is_empty());
    let nf = state.notify();
    assert!(
        !nf.list && !nf.status && !nf.metadata,
        "notify flags should be default"
    );
}

// ---------------------------------------------------------------------------
// UNAUTHENTICATE  -  tagged OK from Selected clears mailbox (RFC 8437 Section2)
// ---------------------------------------------------------------------------

#[test]
fn unauthenticate_ok_from_selected_clears_mailbox() {
    use crate::types::validated::MailboxName;

    let mut state = ProtocolState::new();

    // Simulate authenticated state.
    let greeting = GreetingResponse {
        status: GreetingStatus::PreAuth,
        code: None,
        text: "ready".into(),
    };
    state.apply_greeting(&greeting).unwrap();

    // Simulate SELECT -> Selected state.
    state.pending.insert(
        "A1".into(),
        PendingEffect::Select(MailboxName::new("INBOX").unwrap()),
    );
    let select_ok = Response::Tagged(TaggedResponse {
        tag: "A1".into(),
        status: StatusKind::Ok,
        code: None,
        text: "selected".into(),
    });
    let _ = state.apply_side_effects(&select_ok);
    assert_eq!(state.session_state(), SessionState::Selected);

    // Mark UNAUTHENTICATE in flight.
    state
        .pending
        .insert("A2".into(), PendingEffect::Unauthenticate);

    // Tagged OK -> NotAuthenticated, selected mailbox cleared.
    let tagged_ok = Response::Tagged(TaggedResponse {
        tag: "A2".into(),
        status: StatusKind::Ok,
        code: None,
        text: "unauthenticated".into(),
    });
    let _ = state.apply_side_effects(&tagged_ok);

    assert_eq!(state.session_state(), SessionState::NotAuthenticated);
}

// ---------------------------------------------------------------------------
// Selection history  -  ENABLE after SELECT (RFC 5161 Section 3.1)
// ---------------------------------------------------------------------------

fn tagged(tag: &str, status: StatusKind) -> Response {
    Response::Tagged(TaggedResponse {
        tag: tag.into(),
        status,
        code: None,
        text: "done".into(),
    })
}

/// A successful selection is session history: UNSELECT returns the session
/// to Authenticated, where `legal_states` alone admits ENABLE, but the driver
/// still refuses it. A refused SELECT selects nothing and leaves ENABLE
/// legal; UNAUTHENTICATE starts a new session and clears the history.
///
/// Removing the `refused_after_selection` check from `live_state_refusal`
/// fails the post-UNSELECT assertion; never setting `mailbox_selected` on a
/// successful SELECT fails it too; not clearing it on UNAUTHENTICATE fails the
/// last assertion.
#[test]
fn enable_is_refused_once_a_mailbox_has_been_selected_in_the_session() {
    use crate::connection::driver::live_state_refusal;
    use crate::types::CommandKind;
    use crate::types::validated::MailboxName;

    let mut state = ProtocolState::new();
    state
        .apply_greeting(&GreetingResponse {
            status: GreetingStatus::PreAuth,
            code: None,
            text: "ready".into(),
        })
        .unwrap();
    assert!(live_state_refusal(&state, CommandKind::Enable).is_none());

    state.pending.insert(
        "A1".into(),
        PendingEffect::Select(MailboxName::new("Missing").unwrap()),
    );
    let _ = state.apply_side_effects(&tagged("A1", StatusKind::No));
    assert!(
        !state.mailbox_selected(),
        "a refused SELECT selects nothing"
    );
    assert!(live_state_refusal(&state, CommandKind::Enable).is_none());

    state.pending.insert(
        "A2".into(),
        PendingEffect::Select(MailboxName::new("INBOX").unwrap()),
    );
    let _ = state.apply_side_effects(&tagged("A2", StatusKind::Ok));
    state.pending.insert("A3".into(), PendingEffect::Close);
    let _ = state.apply_side_effects(&tagged("A3", StatusKind::Ok));
    assert_eq!(state.session_state(), SessionState::Authenticated);
    assert!(
        matches!(
            live_state_refusal(&state, CommandKind::Enable),
            Some(crate::error::Error::StateChangedBeforeSend(_))
        ),
        "ENABLE after a mailbox was selected must be refused at the head of the queue"
    );
    assert!(
        live_state_refusal(&state, CommandKind::Noop).is_none(),
        "the history gates ENABLE only"
    );

    state
        .pending
        .insert("A4".into(), PendingEffect::Unauthenticate);
    let _ = state.apply_side_effects(&tagged("A4", StatusKind::Ok));
    assert!(
        !state.mailbox_selected(),
        "UNAUTHENTICATE starts a new session (RFC 8437 Section 2)"
    );
}

// ---------------------------------------------------------------------------
// UNAUTHENTICATE  -  tagged NO preserves state (RFC 8437 Section2)
// ---------------------------------------------------------------------------

#[test]
fn unauthenticate_no_preserves_state() {
    let mut state = ProtocolState::new();

    let greeting = GreetingResponse {
        status: GreetingStatus::PreAuth,
        code: None,
        text: "ready".into(),
    };
    state.apply_greeting(&greeting).unwrap();
    assert_eq!(state.session_state(), SessionState::Authenticated);

    state
        .pending
        .insert("A1".into(), PendingEffect::Unauthenticate);

    let tagged_no = Response::Tagged(TaggedResponse {
        tag: "A1".into(),
        status: StatusKind::No,
        code: None,
        text: "not allowed".into(),
    });
    let _ = state.apply_side_effects(&tagged_no);

    assert_eq!(state.session_state(), SessionState::Authenticated);
}

// ---------------------------------------------------------------------------
// UNAUTHENTICATE  -  tagged BAD preserves state (RFC 8437 Section2)
// ---------------------------------------------------------------------------

#[test]
fn unauthenticate_bad_preserves_state() {
    let mut state = ProtocolState::new();

    let greeting = GreetingResponse {
        status: GreetingStatus::PreAuth,
        code: None,
        text: "ready".into(),
    };
    state.apply_greeting(&greeting).unwrap();
    assert_eq!(state.session_state(), SessionState::Authenticated);

    state
        .pending
        .insert("A1".into(), PendingEffect::Unauthenticate);

    let tagged_bad = Response::Tagged(TaggedResponse {
        tag: "A1".into(),
        status: StatusKind::Bad,
        code: None,
        text: "unknown command".into(),
    });
    let _ = state.apply_side_effects(&tagged_bad);

    assert_eq!(state.session_state(), SessionState::Authenticated);
}

#[test]
fn refused_logout_does_not_complete_on_a_later_commands_ok() {
    for refusal in [StatusKind::No, StatusKind::Bad] {
        let mut state = ProtocolState::new();
        state
            .apply_greeting(&GreetingResponse {
                status: GreetingStatus::PreAuth,
                code: None,
                text: "ready".into(),
            })
            .unwrap();
        state.pending.insert("A1".into(), PendingEffect::Logout);
        let _ = state.apply_side_effects(&Response::Tagged(TaggedResponse {
            tag: "A1".into(),
            status: refusal,
            code: None,
            text: "refused".into(),
        }));
        let _ = state.apply_side_effects(&Response::Tagged(TaggedResponse {
            tag: "A2".into(),
            status: StatusKind::Ok,
            code: None,
            text: "NOOP completed".into(),
        }));
        assert_eq!(state.session_state(), SessionState::Authenticated);
        assert!(
            !state
                .pending
                .values()
                .any(|effect| matches!(effect, PendingEffect::Logout))
        );
    }
}

#[test]
fn advertisements_survive_a_failed_command() {
    for refusal in [StatusKind::No, StatusKind::Bad] {
        let mut state = ProtocolState::new();
        let _ = state.apply_side_effects(&Response::Untagged(Box::new(
            UntaggedResponse::Capability(vec![Capability::Imap4Rev1, Capability::Idle]),
        )));
        let _ = state.apply_side_effects(&Response::Untagged(Box::new(UntaggedResponse::Enabled(
            vec!["QRESYNC".into()],
        ))));
        // Adoption precedes completion, and a refusal does not undo it.
        assert_eq!(
            state.capabilities(),
            &[Capability::Imap4Rev1, Capability::Idle]
        );
        assert_eq!(state.enabled(), &["QRESYNC"]);
        let _ = state.apply_side_effects(&Response::Tagged(TaggedResponse {
            tag: "A1".into(),
            status: refusal,
            code: None,
            text: "refused".into(),
        }));
        assert_eq!(
            state.capabilities(),
            &[Capability::Imap4Rev1, Capability::Idle]
        );
        assert_eq!(state.enabled(), &["QRESYNC"]);
    }
}

/// Snapshots expose the driver's mailbox identity through every selection
/// outcome, rather than requiring consumers to infer it from command errors.
#[test]
fn snapshot_tracks_selected_mailbox_identity() {
    use crate::types::validated::MailboxName;

    let mut state = ProtocolState::new();
    assert_eq!(state.snapshot().selected, None);
    let inbox = MailboxName::new("INBOX").unwrap();
    let archive = MailboxName::new("Archive").unwrap();
    let reply = |status| {
        Response::Tagged(TaggedResponse {
            tag: "A1".into(),
            status,
            code: None,
            text: "done".into(),
        })
    };
    state
        .pending
        .insert("A1".into(), PendingEffect::Select(inbox.clone()));
    let _ = state.apply_side_effects(&reply(StatusKind::Ok));
    assert_eq!(state.snapshot().selected, Some(inbox.clone()));

    state
        .pending
        .insert("A1".into(), PendingEffect::Select(archive.clone()));
    let _ = state.apply_side_effects(&reply(StatusKind::Bad));
    assert_eq!(state.snapshot().selected, Some(inbox));

    state
        .pending
        .insert("A1".into(), PendingEffect::Select(archive.clone()));
    let _ = state.apply_side_effects(&reply(StatusKind::No));
    assert_eq!(state.snapshot().selected, None);

    state
        .pending
        .insert("A1".into(), PendingEffect::Select(archive.clone()));
    let _ = state.apply_side_effects(&reply(StatusKind::Ok));
    assert_eq!(state.snapshot().selected, Some(archive.clone()));
    state.pending.insert("A1".into(), PendingEffect::Close);
    let _ = state.apply_side_effects(&reply(StatusKind::Ok));
    assert_eq!(state.snapshot().selected, None);

    state
        .pending
        .insert("A1".into(), PendingEffect::Select(archive));
    let _ = state.apply_side_effects(&reply(StatusKind::Ok));
    state
        .pending
        .insert("A1".into(), PendingEffect::Unauthenticate);
    let _ = state.apply_side_effects(&reply(StatusKind::Ok));
    assert_eq!(state.snapshot().selected, None);
}

#[test]
fn foreign_tagged_responses_do_not_complete_or_disarm_logout() {
    for status in [StatusKind::Ok, StatusKind::No, StatusKind::Bad] {
        let mut state = ProtocolState::new();
        state
            .apply_greeting(&GreetingResponse {
                status: GreetingStatus::PreAuth,
                code: None,
                text: "ready".into(),
            })
            .unwrap();
        state.pending.insert("logout".into(), PendingEffect::Logout);
        let _ = state.apply_side_effects(&Response::Tagged(TaggedResponse {
            tag: "foreign".into(),
            status,
            code: None,
            text: "unrelated completion".into(),
        }));
        assert_eq!(state.session_state(), SessionState::Authenticated);
        assert!(matches!(
            state.pending.get("logout"),
            Some(PendingEffect::Logout)
        ));
        let _ = state.apply_side_effects(&Response::Tagged(TaggedResponse {
            tag: "logout".into(),
            status: StatusKind::Ok,
            code: None,
            text: "logout complete".into(),
        }));
        assert_eq!(state.session_state(), SessionState::Logout);
        assert!(
            !state
                .pending
                .values()
                .any(|effect| matches!(effect, PendingEffect::Logout))
        );
    }
}

#[test]
fn notify_pending_effects_belong_to_their_own_tags() {
    use crate::types::{Command, MailboxFilter, NotifyEvent, NotifyEventGroup, NotifySetParams};

    let command = Command::NotifySet(NotifySetParams::new(
        vec![NotifyEventGroup::new(
            MailboxFilter::Personal,
            vec![NotifyEvent::MailboxName],
        )],
        false,
    ));
    let mut state = ProtocolState::new();
    state.arm_effect(&command, "N1");
    state.arm_effect(&Command::NotifyNone, "N2");
    // A foreign refusal must not consume either pending registration.
    state.apply_tagged(&TaggedResponse {
        tag: "other".into(),
        status: StatusKind::No,
        code: None,
        text: String::new(),
    });
    for (tag, expected) in [("N1", true), ("N2", false)] {
        state.apply_tagged(&TaggedResponse {
            tag: tag.into(),
            status: StatusKind::Ok,
            code: None,
            text: String::new(),
        });
        assert_eq!(state.notify().list, expected);
    }
}

#[test]
fn every_pending_effect_survives_foreign_tags_and_is_consumed_by_own_refusals() {
    use crate::types::Command;
    for foreign in [StatusKind::Ok, StatusKind::No, StatusKind::Bad] {
        for refusal in [StatusKind::No, StatusKind::Bad] {
            for command in [
                Command::Logout,
                Command::Authenticate {
                    mechanism: "PLAIN".into(),
                    initial_response: None,
                },
                Command::Select {
                    mailbox: MailboxName::new("INBOX").unwrap(),
                    condstore: false,
                    qresync: None,
                },
                Command::Close,
                Command::Unauthenticate,
                Command::NotifyNone,
            ] {
                let mut state = ProtocolState::new();
                state.arm_effect(&command, "own");
                let mut reply = TaggedResponse {
                    tag: "foreign".into(),
                    status: foreign,
                    code: None,
                    text: String::new(),
                };
                state.apply_tagged(&reply);
                assert_eq!(state.pending.len(), 1);
                assert_eq!(state.session_state(), SessionState::NotAuthenticated);
                reply.tag = "own".into();
                reply.status = refusal;
                state.apply_tagged(&reply);
                assert!(state.pending.is_empty());
                let state_after_refusal = state.session_state();
                reply.status = StatusKind::Ok;
                state.apply_tagged(&reply);
                assert_eq!(state.session_state(), state_after_refusal);
            }
        }
    }
}

#[test]
fn disarming_one_literal_command_preserves_other_armed_effects() {
    use crate::types::Command;

    let mut state = ProtocolState::new();
    state.arm_effect(&Command::Close, "first");
    state.arm_effect(&Command::NotifyNone, "literal");
    state.disarm_effect("literal");
    assert!(matches!(
        state.pending.get("first"),
        Some(PendingEffect::Close)
    ));
    assert!(!state.pending.contains_key("literal"));
    state.apply_tagged(&TaggedResponse {
        tag: "first".into(),
        status: StatusKind::Ok,
        code: Some(ResponseCode::NotificationOverflow(None)),
        text: String::new(),
    });
    assert_eq!(state.session_state(), SessionState::Authenticated);
    assert!(state.pending.is_empty());
}

#[test]
fn notification_overflow_cancels_only_pending_registrations() {
    use crate::types::Command;

    let mut state = ProtocolState::new();
    state.arm_effect(&Command::Close, "close");
    state.arm_effect(&Command::NotifyNone, "notify");
    state.apply_tagged(&TaggedResponse {
        tag: "foreign".into(),
        status: StatusKind::Ok,
        code: Some(ResponseCode::NotificationOverflow(None)),
        text: String::new(),
    });
    assert!(matches!(
        state.pending.get("close"),
        Some(PendingEffect::Close)
    ));
    assert!(!state.pending.contains_key("notify"));
}
