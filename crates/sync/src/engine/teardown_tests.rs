use super::*;
use std::fmt::Write as _;
use std::sync::Mutex;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

/// A subscriber that keeps every event's fields as one rendered line.
///
/// Hand-rolled because this crate carries no log-capturing dev-dependency,
/// and the trait is small enough that a capture needs nothing else.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<String>>>);

impl Captured {
    fn lines(&self) -> Vec<String> {
        self.0.lock().map(|lines| lines.clone()).unwrap_or_default()
    }
}

struct Rendered(String);

impl Visit for Rendered {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let _ = write!(self.0, "{}={value:?} ", field.name());
    }
}

impl Subscriber for Captured {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut rendered = Rendered(String::new());
        event.record(&mut rendered);
        if let Ok(mut lines) = self.0.lock() {
            lines.push(rendered.0);
        }
    }
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}

/// A worker reaching the phase after its shared deadline is already spent
/// is aborted unawaited, and that abort is attributed to its role.
///
/// Against the code before the warn line, this captures nothing at all:
/// the spent-deadline branch aborted and returned silently, so only the
/// first straggler of a slow detach was ever named.
#[tokio::test]
async fn a_worker_aborted_on_a_spent_deadline_is_named() {
    let captured = Captured::default();
    let _guard = tracing::subscriber::set_default(captured.clone());

    let join = tokio::spawn(std::future::pending::<()>());
    let watch = join.abort_handle();
    let worker = WorkerTask {
        role: crate::engine::context::WorkerRole::Multiplexer,
        abort: join.abort_handle(),
        join,
    };
    let spent = tokio::time::Instant::now();

    await_worker_until(spent, worker, &AccountId("acct".to_owned())).await;

    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert!(watch.is_finished(), "the worker must still be aborted");
    let lines = captured.lines();
    assert!(
        lines
            .iter()
            .any(|line| line.contains("role=Multiplexer") && line.contains("aborted")),
        "the spent-deadline abort must name the worker's role: {lines:?}"
    );
}
