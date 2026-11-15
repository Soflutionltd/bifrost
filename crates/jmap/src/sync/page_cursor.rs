//! The persisted cursor codec for mail search and the anchored page mechanics
//! shared by the contact and calendar
//! page walks (`ContactCard/query`, `CalendarEvent/query`).
//!
//! Everything here is protocol mechanics that is identical for every object
//! type: the type-tagged payload, anchor paging, the `queryState` pin, the
//! termination rules and the inputs to the central error boundary.
//! What varies per object type is a [`PageKind`]: the id
//! marker, the query-builder type, how a query is pointed at its start or at
//! an anchor, and the diagnostic wording. Nothing about behaviour is a kind
//! parameter - two kinds cannot disagree on termination, on an absent
//! `total`, or on error classification.
//!
//! Callers implement `PageKind` on a unit struct and call the provided
//! associated functions, e.g. `ContactPages::next_cursor(..)`.
//!
//! Anchored payloads carry their query method and a JSON `[anchor, queryState]` pair.
//! Ambiguous legacy payloads require restarting the caller-owned walk.

use bifrost_types::{AccountError, AccountOperation};

use super::error::page_cursor_error as cursor_error;

use crate::core::id::Id;

/// Persisted cursor payloads have one codec, with disjoint tags for anchored
/// PIM walks and positional mail search. Consumers may retain payloads across
/// upgrades. Retired or cross-codec payloads use the
/// caller-owned restart-paging signal, never an engine cursor reset.
/// An Outlook user expects a cursor from another collection to restart paging,
/// never silently skip contacts or events. Legacy untyped anchors cannot be
/// attributed safely, so they are retired rather than guessed.
enum CursorPayload {
    Anchored {
        method: String,
        anchor: String,
        query_state: String,
    },
    Search {
        position: u64,
        query_state: String,
    },
}

impl CursorPayload {
    fn encode(&self) -> Result<String, serde_json::Error> {
        match self {
            Self::Anchored {
                method,
                anchor,
                query_state,
            } => Ok(format!(
                "pim-3:{method}:{}",
                serde_json::to_string(&(anchor, query_state))?
            )),
            Self::Search {
                position,
                query_state,
            } => Ok(format!("search-3:{position}:{query_state}")),
        }
    }

    fn decode(text: &str, operation: AccountOperation) -> Result<Self, AccountError> {
        if let Some((method, payload)) = text
            .strip_prefix("pim-3:")
            .and_then(|value| value.split_once(':'))
        {
            let (anchor, query_state): (String, String) = serde_json::from_str(payload)
                .map_err(|error| cursor_error(operation, error.to_string()))?;
            if !anchor.is_empty() {
                return Ok(Self::Anchored {
                    method: method.to_owned(),
                    anchor,
                    query_state,
                });
            }
        } else if let Some(payload) = text.strip_prefix("search-3:")
            && let Some((position, query_state)) = payload.split_once(':')
            && let Ok(position) = position.parse::<u64>()
        {
            return Ok(Self::Search {
                position,
                query_state: query_state.to_owned(),
            });
        }

        Err(cursor_error(
            operation,
            "page cursor is malformed, retired or out of range".into(),
        ))
    }
}

pub(super) fn encode_search(position: u64, query_state: &str, owner: Option<&str>) -> Vec<u8> {
    let payload = CursorPayload::Search {
        position,
        query_state: query_state.to_owned(),
    }
    .encode()
    .expect("search cursor formatting cannot fail");
    match owner {
        Some(owner) => super::foreign::encode_object(owner, &payload).into_bytes(),
        None => payload.into_bytes(),
    }
}

pub(super) fn decode_search(
    cursor: Option<&[u8]>,
    owner: Option<&str>,
    operation: AccountOperation,
) -> Result<(u64, Option<String>), AccountError> {
    let Some(bytes) = cursor else {
        return Ok((0, None));
    };
    let text =
        std::str::from_utf8(bytes).map_err(|error| cursor_error(operation, error.to_string()))?;
    let (cursor_owner, payload) = match super::foreign::parse_object(text) {
        Some((owner, payload)) => (Some(owner), payload),
        None => (None, text),
    };
    if cursor_owner != owner {
        return Err(super::error::search_cursor_account_mismatch(
            operation,
            cursor_owner,
            owner,
        ));
    }
    match CursorPayload::decode(payload, operation)? {
        CursorPayload::Search {
            position,
            query_state,
        } => Ok((position, Some(query_state))),
        CursorPayload::Anchored { .. } => Err(cursor_error(
            operation,
            "search page cursor has an anchored payload".into(),
        )),
    }
}

/// A decoded page cursor: the item the next page resumes strictly after, plus
/// the `queryState` the order that anchor was chosen from belonged to. Both
/// fields are mandatory - see [`PageKind::decode_page_cursor`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PageCursor {
    pub(super) anchor: String,
    pub(super) query_state: String,
}

/// A typed JMAP id a page walk can anchor on. Blanket-implemented for every
/// `Id<_>`, so a kind names its own id type without naming the marker.
pub(super) trait PageItemId {
    fn page_str(&self) -> &str;
}

impl<T: ?Sized> PageItemId for Id<T> {
    fn page_str(&self) -> &str {
        self.as_str()
    }
}

/// One object type's page walk: its id marker, its query builder, and the
/// wording its diagnostics use. Every provided function is the shared
/// behaviour.
pub(super) trait PageKind {
    /// The object type's typed id (`Id<_>` over a module-private marker,
    /// which is why the marker itself cannot be named here).
    type ItemId: PageItemId;
    /// The typed query builder the walk pages.
    type Query;

    /// Names the collection in cursor and result-set diagnostics
    /// ("contact", "calendar").
    const COLLECTION: &'static str;
    /// Names the item whose removal `anchorNotFound` reports ("contact",
    /// "event").
    const ITEM: &'static str;
    /// Names an id in the empty-id diagnostic ("card", "event").
    const ID_LABEL: &'static str;
    /// The JMAP query method, for the persisted type tag and diagnostics.
    const METHOD: &'static str;
    /// What the consumer is told to repeat ("listing", "walk").
    const REPEAT: &'static str;

    /// Point the query at the start of the result list (`position: 0`).
    fn query_from_start(query: Self::Query) -> Self::Query;

    /// Point the query strictly after `anchor` (`anchor` plus
    /// `anchorOffset: 1`, RFC 8620 s5.5), with no position.
    fn query_after(query: Self::Query, anchor: &str) -> Self::Query;

    /// Point a page query at its continuation.
    ///
    /// The first page starts at position zero; every later page resolves its
    /// start server-side from the previous page's last id rather than from an
    /// integer offset.
    ///
    /// An integer position is only meaningful if the result ORDER is the same
    /// list it was when the cursor was minted. These queries are served
    /// without an explicit comparator, so their order is server-defined and
    /// guaranteed stable across calls by nothing at all: an item created or
    /// destroyed BEHIND the cursor shifts every later position by one, and
    /// the consumer's next page silently skips or repeats an item. The anchor
    /// closes that half: the server resolves it against whatever order it is
    /// serving now, so churn behind the cursor cannot move the window.
    ///
    /// The anchor alone is NOT sufficient, which is why every page also
    /// carries the `queryState` ([`PageKind::verify_query_state`]). An anchor
    /// survives REORDERING AROUND IT: an item ahead of the anchor that moves
    /// behind it is returned twice, one behind the anchor that moves ahead of
    /// it is never returned, and neither is visible from the anchor, because
    /// the anchor is still exactly where the server says it is.
    fn anchor_query(query: Self::Query, cursor: Option<&PageCursor>) -> Self::Query {
        match cursor {
            Some(cursor) => Self::query_after(query, cursor.anchor.as_str()),
            None => Self::query_from_start(query),
        }
    }

    /// Decode a page cursor into the continuation it names.
    ///
    /// Anything without this kind's current method tag is refused. That
    /// covers the v1 bare integer (a position, which under anchored paging
    /// would mean either a stale offset or an id named "100") and, just as
    /// deliberately, an anchor-only payload: a cursor with no pinned
    /// `queryState` cannot be checked for reordering, so honouring it would
    /// be exactly the unchecked paging the pin exists to end.
    /// The refusal is [`cursor_error`]: `Request(Malformed)` with
    /// `RequestCause::InvalidArgument { field: Some("page_cursor") }`, the
    /// crate's standing answer for an older cursor payload version. The `page_cursor` field, not the kind, is what
    /// tells the consumer to restart paging from the first page.
    fn decode_page_cursor(
        page_cursor: Option<bifrost_types::PageCursor>,
        operation: AccountOperation,
    ) -> Result<Option<PageCursor>, AccountError> {
        let Some(cursor) = page_cursor else {
            return Ok(None);
        };
        let cursor = String::from_utf8(cursor.into_bytes())
            .map_err(|error| cursor_error(operation, error.to_string()))?;
        match CursorPayload::decode(&cursor, operation)? {
            CursorPayload::Anchored {
                method,
                anchor,
                query_state,
            } if method == Self::METHOD => Ok(Some(PageCursor {
                anchor,
                query_state,
            })),
            _ => Err(cursor_error(
                operation,
                format!("{} page cursor belongs to another walk", Self::COLLECTION),
            )),
        }
    }

    /// Refuse a continuation whose result set moved under it.
    ///
    /// `queryState` identifies the ordered list of matching ids (RFC 8620
    /// s5.5). If it differs from the one the cursor was minted against, the
    /// anchor is being resolved in a DIFFERENT list than the one the earlier
    /// page came from, and neither the anchor nor the position can tell us
    /// what moved across it. An item that overtook the anchor is lost; one
    /// that fell behind it is repeated.
    ///
    /// Be precise about what refusing buys. It prevents SILENT acceptance of
    /// an inconsistent continuation - the caller learns the walk broke
    /// instead of receiving a page it cannot tell is short. It does NOT
    /// recover the missing items, and it does not guarantee the walk ever
    /// finishes: a busy collection can move the state on every attempt and
    /// fail repeatedly, the same limitation mail search already carries. A
    /// restart re-reads the earlier pages, so a consumer must replace its
    /// prior result set or deduplicate against it. And a stable `queryState`
    /// pins the ordered ID LIST only - the hydrated properties of those items
    /// can still have changed underneath it.
    ///
    /// A server is not required to move the state for every edit either: it
    /// describes the matching ids in order, so an unrelated property change
    /// need not touch it, though RFC 8620 s5.5 permits a server that cannot
    /// tell to invalidate conservatively.
    ///
    /// This runs BEFORE the page's total, cursor and hydration, including on
    /// an empty or apparently final page: an implementation that checks
    /// afterwards has already handed the caller items from a list it just
    /// decided was the wrong one.
    fn verify_query_state(
        cursor: Option<&PageCursor>,
        served: &str,
        operation: AccountOperation,
    ) -> Result<(), AccountError> {
        match cursor {
            Some(cursor) if cursor.query_state != served => {
                Err(Self::result_set_superseded(operation))
            }
            _ => Ok(()),
        }
    }

    /// Mint the cursor for the page after this one, `Ok(None)` when this page
    /// reached the end, and `Err` when the envelope it reached that
    /// conclusion from does not hold together.
    ///
    /// Termination has two rules, and which one applies depends on whether
    /// the server answered with a `total`. With one (these queries always
    /// ask, via `calculateTotal: true`), `position + served == total` is the
    /// end and is exact. WITHOUT one the walk continues until an EMPTY page:
    /// every nonempty page mints a successor.
    ///
    /// Page fullness is deliberately NOT the fallback. RFC 8620 s5.5 does not
    /// permit an arbitrary short non-final page: `ids` runs to the end of the
    /// result list or to the effective limit, and a server that clamps the
    /// requested limit must RETURN the `limit` it actually used. The honest
    /// argument is the other one: an absent `total` is not evidence of
    /// completion (treating it as "end of walk" truncates the walk at page
    /// ONE), while a conforming EMPTY page is conclusive. Fullness would
    /// additionally have to trust a `limit` echo that a server may omit, for
    /// no gain over asking once more. The cost of continue-until-empty is one
    /// extra round trip on a `total`-less server whose final page happened to
    /// land exactly on the end.
    ///
    /// `total` is the RAW server total: a caller that suppresses it from what
    /// the consumer is told (a client-side filter makes it a different
    /// number) still passes the raw one here, because the suppression is
    /// about what the consumer is told, not about where the server-side
    /// result set ends.
    ///
    /// The three refusals, all `Protocol(ContractViolation)`
    /// (`page_envelope_violation`), all checked BEFORE any termination test
    /// so that a broken envelope can never present as a completed walk:
    ///
    /// - A NEGATIVE position. RFC 8620 s5.5 types the response `position` as
    ///   an UnsignedInt; a negative one indexes nothing.
    /// - `position + served > total` on a NONEMPTY page. `position` is the
    ///   index of the first returned id and `total` is the length of the
    ///   whole result list, so the last id sits at index
    ///   `position + served - 1` and `position + served <= total` must hold.
    ///   A response that claims to have served past the end of the list it
    ///   just measured is CONTRADICTORY, and `>= total` alone reads that
    ///   contradiction as a clean completion.
    /// - A continuation page that CONTAINS the anchor it resumed after.
    ///   `anchorOffset: 1` means strictly after, and `verify_query_state` has
    ///   already pinned the ordered result list, so the anchor cannot have
    ///   moved into this window. This is what makes a non-advancing walk
    ///   detectable rather than indistinguishable from an unbounded result
    ///   set: under an unmoved `queryState` the list is stable and finite, so
    ///   a server that re-serves the page the anchor came from is violating
    ///   its own contract. It subsumes the narrower "the successor anchor
    ///   equals the incoming anchor" rule, which is the same condition
    ///   restricted to the last id.
    ///
    /// The anchor is this page's LAST QUERY-RESULT id - taken from the ids the
    /// query answered with, never from whichever of them survived hydration
    /// or local filtering. The cursor addresses the server-side result set,
    /// so a page that hydrated nothing still has to name where the server
    /// continues from. `position` is the server's echo of where it actually
    /// served from, so a server that clamped the anchored start still reports
    /// a truthful base for the "is there more" test.
    ///
    /// `query_state` is the state THIS response was served under, which by
    /// the time this is called `verify_query_state` has already confirmed
    /// matches the incoming cursor's pin (on a continuation) or is the walk's
    /// first observation (on a first page).
    fn next_cursor(
        cursor: Option<&PageCursor>,
        position: u64,
        ids: &[Self::ItemId],
        total: Option<u64>,
        query_state: &str,
        operation: AccountOperation,
    ) -> Result<Option<bifrost_types::PageCursor>, AccountError> {
        let base = position;
        let served = u64::try_from(ids.len()).map_err(|_| {
            page_envelope_violation(operation, "query page length exceeds UnsignedInt".into())
        })?;
        let next = base.checked_add(served).ok_or_else(|| {
            page_envelope_violation(operation, "query page end exceeds UnsignedInt".into())
        })?;
        if let Some(cursor) = cursor
            && ids.iter().any(|id| id.page_str() == cursor.anchor)
        {
            return Err(page_envelope_violation(
                operation,
                format!(
                    "{} returned the anchor {} it was asked to resume strictly after, \
                     under an unmoved queryState",
                    Self::METHOD,
                    cursor.anchor
                ),
            ));
        }
        if let Some(total) = total {
            if served > 0 && next > total {
                return Err(page_envelope_violation(
                    operation,
                    format!(
                        "{} served {served} ids from position {position} of a result list \
                         it reports as {total} long",
                        Self::METHOD
                    ),
                ));
            }
            if next >= total {
                return Ok(None);
            }
        }
        let Some(anchor) = ids.last().map(PageItemId::page_str) else {
            return Ok(None);
        };
        // A JMAP id is at least one character (RFC 8620 s1.2), so a
        // conforming server never reaches this arm. An empty id is a
        // malformed envelope, and it gets the same treatment as the other
        // three: ending the walk here would report a response we cannot page
        // from as a completed walk.
        if anchor.is_empty() {
            return Err(page_envelope_violation(
                operation,
                format!("{} returned an empty {} id", Self::METHOD, Self::ID_LABEL),
            ));
        }
        let payload = CursorPayload::Anchored {
            method: Self::METHOD.to_owned(),
            anchor: anchor.to_owned(),
            query_state: query_state.to_owned(),
        }
        .encode()
        .map_err(|error| {
            page_envelope_violation(
                operation,
                format!("{} page cursor is not encodable: {error}", Self::COLLECTION),
            )
        })?;
        Ok(Some(payload.into_bytes().into()))
    }

    /// The result set this page cursor addresses is not the one it was minted
    /// against. Ordinary concurrent activity, not a server defect:
    /// `ConcurrencyConflict` derives `Retry(AfterStateRefresh)`, and
    /// refreshing here means repeating from the first page.
    fn result_set_superseded(operation: AccountOperation) -> AccountError {
        super::error::page_concurrency_conflict(
            operation,
            format!(
                "{} result set changed between pages ({} queryState moved); repeat the {}",
                Self::COLLECTION,
                Self::METHOD,
                Self::REPEAT
            ),
        )
    }

    /// Error mapping for a paged query call, aware of whether the call
    /// carried an anchor.
    ///
    /// `anchorNotFound` means the item this cursor resumed after was
    /// destroyed between the two pages. The crate's central mapping
    /// classifies that as `Protocol(ContractViolation)` outside a cursor
    /// scope, which is right for a walk that never asked for an anchor and
    /// wrong here: an item deleted while a consumer pages is ordinary
    /// concurrent activity, not a server defect. `ConcurrencyConflict`
    /// derives `Retry(AfterStateRefresh)`, and refreshing this caller's state
    /// means repeating from the first page - which then succeeds.
    fn page_call_err(
        operation: AccountOperation,
        anchored: bool,
    ) -> impl Fn(crate::Error) -> AccountError {
        move |error| {
            super::error::page_call_error(
                error,
                operation,
                anchored,
                Self::ITEM,
                Self::METHOD,
                Self::REPEAT,
            )
        }
    }
}

/// The server's own page envelope is internally inconsistent, or it
/// contradicts the result list the pinned `queryState` promises is stable.
///
/// `Protocol(ContractViolation)` -> `RecoveryClass::ProviderContractViolation`.
/// The three classifications it is deliberately not:
///
/// - `None` (walk complete) is what these cases used to produce, and it is
///   the silent-truncation shape the anchored cursor exists to end.
/// - `ConcurrencyConflict` (what a moved `queryState` gets) says "repeat the
///   walk and it will work". Here the state did NOT move, so a repeat
///   re-issues the identical request and gets the identical broken envelope;
///   the caller would spin.
/// - `Request(Malformed)` with field `page_cursor` (what a stale cursor
///   payload gets) directs a restart from page one, and it points the blame
///   at the caller's cursor when the defect is in the response.
///
/// `ProviderContractViolation` is the one a consumer can act on: it is
/// terminal for this walk and it names the server.
fn page_envelope_violation(operation: AccountOperation, message: String) -> AccountError {
    super::error::contract_violation(operation, None, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_codec_preserves_search_bytes_and_rejects_cross_codec_payloads() {
        let op = AccountOperation::Search;
        for owner in [None, Some("shared")] {
            let encoded = encode_search(42, "q:1", owner);
            let expected = match owner {
                Some(owner) => super::super::foreign::encode_object(owner, "search-3:42:q:1"),
                None => "search-3:42:q:1".to_owned(),
            };
            assert_eq!(encoded, expected.into_bytes());
            assert_eq!(
                decode_search(Some(&encoded), owner, op).unwrap(),
                (42, Some("q:1".into()))
            );
        }
        for bytes in [
            b"search-3:-1:q".as_slice(),
            b"2:[\"a\",\"q\"]",
            b"7",
            &[0xff],
        ] {
            let error = decode_search(Some(bytes), None, op).expect_err("restart paging");
            let expected =
                AccountError::invalid_page_cursor(op, bifrost_types::Protocol::Jmap, "invalid");
            assert_eq!(error.kind(), expected.kind());
            assert_eq!(error.recovery(), expected.recovery());
        }
        assert!(decode(Some(b"search-3:42:q".to_vec().into())).is_err());
    }

    #[test]
    fn invalid_page_state_has_no_engine_recovery_directive() {
        let error = cursor_error(AccountOperation::Search, "retired cursor".to_string());
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert!(matches!(
            error.recovery(),
            bifrost_types::RecoveryClass::ClientBug
        ));
    }

    enum TestMarker {}

    /// Records what the walk asked of the query, so anchor paging is
    /// observable without a real builder.
    #[derive(Debug, Default, PartialEq, Eq)]
    struct TestQuery {
        position: Option<i32>,
        anchor: Option<String>,
        anchor_offset: Option<i32>,
    }

    struct TestPages;

    impl PageKind for TestPages {
        type ItemId = Id<TestMarker>;
        type Query = TestQuery;

        const COLLECTION: &'static str = "widget";
        const ITEM: &'static str = "gadget";
        const ID_LABEL: &'static str = "sprocket";
        const METHOD: &'static str = "Widget/query";
        const REPEAT: &'static str = "trawl";

        fn query_from_start(mut query: TestQuery) -> TestQuery {
            query.position = Some(0);
            query
        }

        fn query_after(mut query: TestQuery, anchor: &str) -> TestQuery {
            query.anchor = Some(anchor.to_string());
            query.anchor_offset = Some(1);
            query
        }
    }

    const OP: AccountOperation = AccountOperation::ContactsList;

    fn ids(values: &[&str]) -> Vec<Id<TestMarker>> {
        values.iter().map(|id| Id::new(*id)).collect()
    }

    fn cursor(anchor: &str, query_state: &str) -> PageCursor {
        PageCursor {
            anchor: anchor.to_string(),
            query_state: query_state.to_string(),
        }
    }

    fn decode(
        bytes: Option<bifrost_types::PageCursor>,
    ) -> Result<Option<PageCursor>, AccountError> {
        TestPages::decode_page_cursor(bytes, OP)
    }

    /// `next_cursor` for a FIRST page (no incoming anchor), which is what
    /// most of these cases exercise.
    fn mint(
        position: u64,
        page: &[Id<TestMarker>],
        total: Option<u64>,
        query_state: &str,
    ) -> Result<Option<bifrost_types::PageCursor>, AccountError> {
        TestPages::next_cursor(None, position, page, total, query_state, OP)
    }

    fn is_contract_violation(error: &AccountError) -> bool {
        matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ContractViolation
            )
        )
    }

    /// Literal bytes pin the versioned wire format independently of the encoder.
    #[test]
    fn typed_encoding_is_stable() {
        assert_eq!(
            decode(Some(br#"pim-3:Widget/query:["c2","q1"]"#.to_vec().into())).expect("decodes"),
            Some(cursor("c2", "q1"))
        );
        assert_eq!(
            decode(Some(
                br#"pim-3:Widget/query:["a:b\",\"c","[\"q:1\"]"]"#.to_vec().into()
            ))
            .expect("decodes"),
            Some(cursor("a:b\",\"c", "[\"q:1\"]"))
        );
        assert_eq!(
            mint(0, &ids(&["c1", "c2"]), Some(250), "q1")
                .expect("valid envelope")
                .expect("cursor"),
            bifrost_types::PageCursor::from(br#"pim-3:Widget/query:["c2","q1"]"#.to_vec())
        );
    }

    #[test]
    #[cfg(all(feature = "contacts", feature = "calendars"))]
    fn contact_and_calendar_cursors_are_disjoint_and_legacy_cursors_restart() {
        {
            use crate::sync::calendar_ops::CalendarPages;
            use crate::sync::contacts::ContactPages;
            for method in ["ContactCard/query", "CalendarEvent/query"] {
                let bytes = format!("pim-3:{method}:[\"same-id\",\"same-state\"]").into_bytes();
                assert_eq!(
                    ContactPages::decode_page_cursor(Some(bytes.clone().into()), OP).is_ok(),
                    method == "ContactCard/query",
                );
                assert_eq!(
                    CalendarPages::decode_page_cursor(Some(bytes.into()), OP).is_ok(),
                    method == "CalendarEvent/query",
                );
            }
            for decode in [
                ContactPages::decode_page_cursor,
                CalendarPages::decode_page_cursor,
            ] {
                let error = decode(Some(br#"2:["same-id","same-state"]"#.to_vec().into()), OP)
                    .expect_err("legacy payload has no provable collection");
                let expected = AccountError::invalid_page_cursor(
                    OP,
                    bifrost_types::Protocol::Jmap,
                    "retired cursor",
                );
                assert_eq!(error.kind(), expected.kind());
                assert_eq!(error.recovery(), expected.recovery());
            }
        }
    }

    /// The minted cursor names the page's LAST id and pins the state that
    /// order was served under; when the server sent a total, that total
    /// decides whether there is a next page at all.
    #[test]
    fn next_cursor_anchors_on_the_last_id_and_pins_the_state() {
        let minted = mint(0, &ids(&["c1", "c2"]), Some(250), "q1")
            .expect("valid envelope")
            .expect("cursor");
        assert_eq!(
            decode(Some(minted)).expect("decodes"),
            Some(cursor("c2", "q1"))
        );
        assert_eq!(
            mint(200, &ids(&["c9"]), Some(201), "q1").expect("valid envelope"),
            None
        );
    }

    /// A server that ignores `calculateTotal` must not truncate the walk. With
    /// no `total`, a NONEMPTY page mints a successor anchored on its last id
    /// (whatever the page's length - fullness is not a completion test), and
    /// only an EMPTY page ends the walk.
    ///
    /// Reverting the fallback to `let total = total...?` fails the first two
    /// assertions. The short page is here to state the rule, not to catch a
    /// fullness fallback: `next_cursor` is not handed the limit, so fullness
    /// is not expressible at this seam at all - which is itself part of why
    /// the rule is sound here.
    #[test]
    fn without_a_total_the_walk_continues_until_an_empty_page() {
        assert_eq!(
            decode(mint(0, &ids(&["c1", "c2"]), None, "q1").expect("valid envelope"))
                .expect("decodes"),
            Some(cursor("c2", "q1")),
            "a full page with no total continues"
        );
        assert_eq!(
            decode(mint(40, &ids(&["c9"]), None, "q1").expect("valid envelope")).expect("decodes"),
            Some(cursor("c9", "q1")),
            "a SHORT page with no total is not evidence of the end"
        );
        assert_eq!(
            mint(80, &ids(&[]), None, "q1").expect("valid envelope"),
            None,
            "the empty page is what ends it"
        );
    }

    /// BITES. `position` is the index of a nonempty page's FIRST id and
    /// `total` is the length of the whole result list, so
    /// `position + served > total` is a contradiction, not an ending. The
    /// old `next >= total` arm read every one of these as a cleanly
    /// completed walk; restoring it turns all four `expect_err` calls into
    /// `Ok(None)` and the test fails.
    ///
    /// The empty-page row is the boundary the rule must NOT catch: with no
    /// first id there is no index for `position` to be, so a server that
    /// echoes a position past the end of an empty page is left alone and
    /// terminates normally.
    #[test]
    fn an_envelope_that_contradicts_its_own_total_is_a_contract_violation() {
        for (position, page, total) in [
            (0, ids(&["c1", "c2"]), 1_u64),
            (5, ids(&["c1"]), 5),
            (0, ids(&["c1"]), 0),
            (u64::MAX, ids(&["c1"]), 9),
        ] {
            let error = mint(position, &page, Some(total), "q1")
                .expect_err("a contradictory envelope is not a completed walk");
            assert!(
                is_contract_violation(&error),
                "position {position} + {} ids against total {total} must be refused",
                page.len()
            );
        }
        assert_eq!(
            mint(90, &ids(&[]), Some(4), "q1").expect("an empty page indexes nothing"),
            None
        );
    }

    #[test]
    fn a_negative_wire_position_is_rejected_before_paging() {
        let value = serde_json::json!({
            "accountId": "a", "queryState": "q", "position": -5, "ids": []
        });
        assert!(serde_json::from_value::<crate::core::query::QueryResponse<crate::principal::Principal>>(value).is_err());
    }

    /// BITES the non-termination detection. `anchorOffset: 1` resumes
    /// STRICTLY after the anchor, and `verify_query_state` has already
    /// established that the ordered result list did not move, so a page that
    /// contains the anchor again is a server re-serving the window it was
    /// asked to leave - the shape that used to page forever without
    /// terminating. Deleting the containment check makes rows one and two
    /// mint a successor instead of failing.
    ///
    /// Row two is the narrower "the successor anchor equals the incoming
    /// anchor" case; it is a strict subset of containment, which is why one
    /// rule covers both. Row three is the control: the same walk, a page
    /// that genuinely moved past the anchor, still mints.
    #[test]
    fn a_continuation_that_re_serves_its_own_anchor_is_a_contract_violation() {
        let incoming = cursor("c2", "q1");
        for page in [ids(&["c2", "c3"]), ids(&["c3", "c2"])] {
            let error = TestPages::next_cursor(Some(&incoming), 2, &page, None, "q1", OP)
                .expect_err("a page must not contain the anchor it resumes after");
            assert!(is_contract_violation(&error));
        }
        assert!(
            TestPages::next_cursor(Some(&incoming), 2, &ids(&["c3", "c4"]), None, "q1", OP)
                .expect("an advancing page is fine")
                .is_some()
        );
    }

    /// An empty id cannot be an anchor, and ending the walk on it would
    /// report a response we cannot page from as a completed listing.
    #[test]
    fn an_empty_last_id_is_a_contract_violation() {
        let error = mint(0, &ids(&["c1", ""]), None, "q1")
            .expect_err("an empty id cannot anchor a successor");
        assert!(is_contract_violation(&error));
    }

    /// Large totals and positions remain unsigned through page termination.
    #[test]
    fn a_total_above_i32_max_is_not_read_as_an_absent_total() {
        let total = u64::from(u32::MAX) + 7;
        assert!(
            mint(u64::from(u32::MAX), &ids(&["c1", "c2"]), Some(total), "q1")
                .expect("valid envelope")
                .is_some(),
            "two ids out of four billion is not the end of the list"
        );
    }

    /// Every payload without the matching type tag and anchor-plus-state is refused
    /// rather than reinterpreted. Two cases carry the weight: the v1 bare
    /// position (reading it as an anchor would page from an item named
    /// "100"), and the anchor-ONLY v2 shape - a cursor with no pinned state
    /// cannot be checked for reordering, so accepting it would reinstate the
    /// hole the pin closes.
    #[test]
    fn a_page_cursor_refuses_every_shape_without_a_pinned_state() {
        for refused in [
            "2:[\"c2\",\"q1\"]",
            "pim-3:ContactCard/query:[\"c2\",\"q1\"]",
            "pim-3:CalendarEvent/query:[\"c2\",\"q1\"]",
            "100",
            "-1",
            "not-a-number",
            "2:c2",
            "pim-3:Widget/query:[\"c2\"]",
            "pim-3:Widget/query:[\"c2\",\"q1\",\"extra\"]",
            "pim-3:Widget/query:[\"\",\"q1\"]",
            "pim-3:Widget/query:",
            "2:[\"c2\"]",
            "2:[\"c2\",\"q1\",\"extra\"]",
            "2:[\"\",\"q1\"]",
            "3:[\"c2\",\"q1\"]",
            "2:",
        ] {
            let error = decode(Some(Vec::from(refused).into()))
                .expect_err("older or malformed cursor should fail");
            assert!(
                matches!(
                    error.kind(),
                    bifrost_types::AccountErrorKind::Request(
                        bifrost_types::RequestErrorKind::Malformed
                    )
                ),
                "{refused} must be refused as a caller error"
            );
        }
        let non_utf8 = decode(Some(vec![0xff, 0xfe].into())).expect_err("not UTF-8");
        assert!(matches!(
            non_utf8.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert_eq!(decode(None).expect("no cursor is a first page"), None);
    }

    /// Both halves are opaque strings that may contain anything a delimiter
    /// could be. JSON quoting is what makes the split unambiguous, so an id
    /// and a state full of separators, quotes and brackets round-trip.
    #[test]
    fn a_page_cursor_round_trips_opaque_halves() {
        let minted = mint(0, &ids(&["a:b\",\"c"]), Some(9), "[\"q:1\"]")
            .expect("valid envelope")
            .expect("cursor");
        assert_eq!(
            decode(Some(minted)).expect("decodes"),
            Some(cursor("a:b\",\"c", "[\"q:1\"]"))
        );
    }

    /// The state check refuses a moved state, accepts an unmoved one, and
    /// has nothing to compare on a first page.
    #[test]
    fn verify_query_state_refuses_only_a_moved_state() {
        assert!(TestPages::verify_query_state(None, "q1", OP).is_ok());
        assert!(TestPages::verify_query_state(Some(&cursor("c2", "q1")), "q1", OP).is_ok());
        let error = TestPages::verify_query_state(Some(&cursor("c2", "q1")), "q2", OP)
            .expect_err("a moved state must be refused");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::ConcurrencyConflict
        );
    }

    /// The first page positions at zero; a continuation carries the anchor
    /// and NO position, so an item destroyed behind the cursor cannot shift
    /// the window.
    #[test]
    fn a_continuation_page_queries_by_anchor_not_position() {
        assert_eq!(
            TestPages::anchor_query(TestQuery::default(), None),
            TestQuery {
                position: Some(0),
                anchor: None,
                anchor_offset: None,
            }
        );
        assert_eq!(
            TestPages::anchor_query(TestQuery::default(), Some(&cursor("c2", "q1"))),
            TestQuery {
                position: None,
                anchor: Some("c2".to_string()),
                anchor_offset: Some(1),
            }
        );
    }

    fn anchor_not_found() -> crate::Error {
        crate::Error::Method(
            serde_json::from_value(serde_json::json!({"type": "anchorNotFound"}))
                .expect("method error parses"),
        )
    }

    /// `anchorNotFound` on an anchored call is a concurrency conflict (repeat
    /// from the first page), not the contract violation the central mapping
    /// gives it; on a call that carried no anchor the central mapping stands,
    /// because there the server really is at fault.
    #[test]
    fn anchor_not_found_is_a_conflict_only_when_the_call_was_anchored() {
        let anchored = TestPages::page_call_err(OP, true)(anchor_not_found());
        assert_eq!(
            anchored.kind(),
            &bifrost_types::AccountErrorKind::ConcurrencyConflict
        );
        let unanchored = TestPages::page_call_err(OP, false)(anchor_not_found());
        assert_eq!(
            unanchored.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ContractViolation
            )
        );
    }

    /// The per-kind wording reaches the diagnostics: the parameters are
    /// live, not decoration. Refusals name the kind's query method; the
    /// consumer-facing repeat instruction uses the kind's own noun.
    #[test]
    fn diagnostics_carry_the_kinds_own_wording() {
        fn text(error: &AccountError) -> String {
            error.support_consented().support_text.join(" ")
        }

        let superseded = text(&TestPages::result_set_superseded(OP));
        assert!(superseded.contains("widget"), "{superseded}");
        assert!(superseded.contains("Widget/query"), "{superseded}");
        assert!(superseded.contains("trawl"), "{superseded}");

        let lost = text(&TestPages::page_call_err(OP, true)(anchor_not_found()));
        assert!(lost.contains("gadget"), "{lost}");
        assert!(lost.contains("Widget/query"), "{lost}");
        assert!(lost.contains("trawl"), "{lost}");

        // An envelope violation carries its message in the cause's
        // `MalformedResponse` detail, not in the error's own diagnostic text,
        // so it is read from the full debug form.
        let empty = format!(
            "{:?}",
            mint(0, &ids(&[""]), None, "q1").expect_err("an empty id")
        );
        assert!(empty.contains("Widget/query"), "{empty}");
        assert!(empty.contains("sprocket"), "{empty}");

        // Like the envelope violation, a refused cursor now carries its
        // message in the `InvalidArgument` cause, so read the debug form.
        let undecodable = format!(
            "{:?}",
            decode(Some(b"100".to_vec().into())).expect_err("v1 cursor")
        );
        assert!(undecodable.contains("page cursor"), "{undecodable}");
    }
}
