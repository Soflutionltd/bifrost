//! Unit tests for the pipeline builder.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::error::Error;
use crate::types::{Capability, SequenceSet};

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
        Err(Error::Protocol("test error".into())),
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
/// because tests never call `execute` or `execute_dynamic` (which need
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
