//! Unit tests for the pipeline builder.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::error::Error;
use crate::types::{Capability, SequenceSet};

#[tokio::test]
async fn typed_list_pipeline_returns_the_consumer_value() {
    use crate::connection::test_support;
    let (conn, mut peer) =
        test_support::driver_pair(&test_support::preauth_greeting("IMAP4rev1")).await;
    let request = conn
        .pipeline()
        .list(String::new(), "*".into())
        .execute(std::time::Duration::from_secs(1));
    let script = async {
        let line = test_support::read_line(&mut peer).await;
        test_support::respond(
            &mut peer,
            &format!(
                "* LIST () \"/\" INBOX\r\n{} OK listed\r\n",
                test_support::tag_of(&line)
            ),
        )
        .await;
    };
    let (result, ()) = tokio::join!(request, script);
    let (mailboxes,) = result.unwrap();
    assert_eq!(mailboxes.unwrap()[0].name.as_str(), "INBOX");
}

/// Verify that a 2-command pipeline builds correctly at the type level.
///
/// The test constructs a `Pipeline` with `noop` then `capability` but
/// does NOT execute it (no driver, no wire). It only proves that the
/// typestate accumulation compiles and the internal state is correct.
#[test]
fn two_command_pipeline_builds() {
    // We cannot construct an ImapConnection without a real driver, but
    // we CAN verify the Pipeline type machinery by checking that the
    // builder methods chain and produce the expected `commands` and
    // `pending` vecs. We use a helper that creates a "detached"
    // pipeline without a connection reference.
    let pipeline: Pipeline<'_, (Vec<Capability>, ((), ()))> =
        DetachedPipeline::create().noop().capability();
    assert_eq!(pipeline.commands.len(), 2);
    assert_eq!(pipeline.pending.len(), 2);
}

/// Verify that `UnfoldTuple` for a 2-element nested tuple works correctly
/// with both `Ok` and `Err` results.
#[test]
fn unfold_two_element_tuple_ok() {
    // Accumulated type: (Vec<Capability>, ((), ()))
    // Results vec: [Ok(Box<()>), Ok(Box<Vec<Capability>>)]
    let results: Vec<Result<Box<dyn std::any::Any + Send>, Error>> =
        vec![Ok(Box::new(())), Ok(Box::new(vec![Capability::Imap4Rev1]))];

    let (r0, r1) = <(Vec<Capability>, ((), ()))>::unfold(results).unwrap();
    assert!(r0.is_ok());
    assert_eq!(r0.unwrap(), ());
    let caps = r1.unwrap();
    assert_eq!(caps, vec![Capability::Imap4Rev1]);
}

/// Verify `UnfoldTuple` propagates per-command errors without
/// `TypeMismatch`.
#[test]
fn unfold_two_element_tuple_with_command_error() {
    let results: Vec<Result<Box<dyn std::any::Any + Send>, Error>> = vec![
        Err(Error::Provider(crate::error::ProviderFailure::Protocol(
            "test error".into(),
        ))),
        Ok(Box::new(vec![Capability::Imap4Rev1])),
    ];

    let (r0, r1) = <(Vec<Capability>, ((), ()))>::unfold(results).unwrap();
    assert!(r0.is_err());
    assert!(r1.is_ok());
}

/// Verify `UnfoldTuple` returns `TypeMismatch` on wrong downcast type.
#[test]
fn unfold_type_mismatch() {
    // Put a String where () is expected.
    let results: Vec<Result<Box<dyn std::any::Any + Send>, Error>> = vec![
        Ok(Box::new("wrong type".to_string())),
        Ok(Box::new(vec![Capability::Imap4Rev1])),
    ];

    let err = <(Vec<Capability>, ((), ()))>::unfold(results).unwrap_err();
    assert!(matches!(err, PipelineError::TypeMismatch { index: 0 }));
}

/// Verify `UnfoldTuple` for the empty pipeline.
#[test]
fn unfold_empty() {
    let results: Vec<Result<Box<dyn std::any::Any + Send>, Error>> = vec![];
    let () = <()>::unfold(results).unwrap();
}

/// Verify `UnfoldTuple` for a single-element pipeline.
#[test]
fn unfold_single() {
    let results: Vec<Result<Box<dyn std::any::Any + Send>, Error>> = vec![Ok(Box::new(42u32))];
    let (r0,) = <(u32, ())>::unfold(results).unwrap();
    assert_eq!(r0.unwrap(), 42u32);
}

/// Verify that a 3-command pipeline type-checks and tracks state.
#[test]
fn three_command_pipeline_builds() {
    let pipeline = DetachedPipeline::create().noop().capability().fetch(
        SequenceSet::new("1:*").unwrap(),
        "FLAGS".to_string(),
        None,
    );
    // Type: Pipeline<'_, (Vec<FetchResponse>, (Vec<Capability>, ((), ())))>
    assert_eq!(pipeline.commands.len(), 3);
    assert_eq!(pipeline.pending.len(), 3);
}

// ---------------------------------------------------------------------------
// Helper: detached pipeline (no real connection)
// ---------------------------------------------------------------------------

/// A wrapper that creates a `Pipeline` without a live `ImapConnection`.
///
/// The pipeline holds a reference to a dummy connection. This is safe
/// because detached builder tests never call execution methods (which need
/// the driver channel). Only builder methods and type-level assertions
/// are exercised.
struct DetachedPipeline;

impl DetachedPipeline {
    /// Create a pipeline backed by a leaked dummy connection.
    ///
    /// The "connection" is a zero-sized reference that satisfies the
    /// borrow checker. Since we never execute the pipeline, the
    /// channel/driver state is irrelevant.
    fn create<'a>() -> Pipeline<'a, ()> {
        // Create a dummy ImapConnection with real channels but no driver.
        let (cmd_tx, _cmd_rx) = tokio::sync::mpsc::channel(1);
        let (_state_tx, state_rx) =
            tokio::sync::watch::channel(super::super::driver::ConnectionStateSnapshot::default());
        let (_events_tx, events_rx) = tokio::sync::mpsc::channel(1);
        let conn = super::super::ImapConnection {
            cmd_tx,
            state_rx,
            events_rx: tokio::sync::Mutex::new(events_rx),
            driver_handle: std::sync::Arc::new(crate::connection::DriverTask::absent()),
            tls_active: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            abandoned: std::sync::atomic::AtomicBool::new(false),
            host: String::new(),
        };
        // Leak the connection so the reference lives 'a.
        // Fine for tests  -  the leak is bounded.
        let conn_ref: &'a super::super::ImapConnection = Box::leak(Box::new(conn));
        Pipeline::new(conn_ref)
    }
}

#[tokio::test(start_paused = true)]
async fn pipeline_zero_deadline_submits_nothing() {
    let (conn, _server) = crate::connection::test_support::driver_pair(
        &crate::connection::test_support::preauth_greeting("IMAP4rev1"),
    )
    .await;
    let capacity = conn.cmd_tx.capacity();
    let error = conn
        .pipeline()
        .noop()
        .execute(std::time::Duration::ZERO)
        .await
        .unwrap_err();
    let PipelineError::Timeout(error) = error else {
        panic!("expected pipeline timeout, got {error:?}");
    };
    assert_eq!(
        error.attempt(),
        Some(bifrost_types::TransmissionState::Unsent)
    );
    assert_eq!(conn.cmd_tx.capacity(), capacity);
    assert!(conn.is_reusable());
}

#[tokio::test(start_paused = true)]
async fn pipeline_deadline_after_write_is_inflight() {
    use crate::connection::test_support::{driver_pair, preauth_greeting, read_line};
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let batch = conn
        .pipeline()
        .noop()
        .execute(std::time::Duration::from_secs(1));
    let server_read = async {
        let line = read_line(&mut server).await;
        assert!(line.contains(" NOOP"));
    };
    let (result, ()) = tokio::join!(batch, server_read);
    let PipelineError::Timeout(error) = result.unwrap_err() else {
        panic!("expected pipeline timeout");
    };
    assert_eq!(
        error.attempt(),
        Some(bifrost_types::TransmissionState::InFlight)
    );
    assert!(!conn.is_reusable());
}

/// The peer sends only a declaration, never its payload. Both read loops
/// must reject it before waiting for or allocating the declared body.
#[tokio::test]
async fn pipeline_literal_budget_applies_during_responses_and_continuations() {
    use crate::connection::test_support::{
        driver_pair, preauth_greeting, read_line, respond, select_inbox,
    };
    for continuation in [false, true] {
        let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 ACL")).await;
        select_inbox(&conn, &mut server).await;
        let mut batch = conn
            .pipeline()
            .fetch(SequenceSet::new("1").unwrap(), "(BODY[])", None);
        batch.pending[0] = Box::new(dispatch::FetchConsumer::with_limit(3));
        // Dynamic execution deliberately permits replacing the consumer here:
        // its output is Vec<FetchResponse>, with no typed tuple to unfold.
        let commands = async {
            let results = if continuation {
                batch
                    .list_rights(MailboxName::new("INBOX").unwrap(), "\u{fc}ser".into())
                    .execute_dynamic(std::time::Duration::from_secs(30))
                    .await
            } else {
                batch
                    .execute_dynamic(std::time::Duration::from_secs(30))
                    .await
            }
            .unwrap();
            assert!(matches!(
                results[0],
                Err(Error::ResultLimit(
                    crate::error::ResultLimit::FetchLiteral { .. }
                ))
            ));
        };
        let peer = async {
            let fetch = read_line(&mut server).await;
            assert!(fetch.contains(" FETCH "));
            if continuation {
                let literal = read_line(&mut server).await;
                assert!(literal.ends_with("{5}\r\n"));
            }
            respond(&mut server, "* 1 FETCH (BODY[] {4}\r\n").await;
        };
        tokio::join!(commands, peer);
    }
}
