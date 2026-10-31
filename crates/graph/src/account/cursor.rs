use bifrost_types::{
    ChangeCursor, CursorScope, FolderId, ObjectType, OpaqueChangeState, OpaqueProgressBytes,
    ProtocolKind,
};
use serde::{Deserialize, Serialize};

use crate::origin::AdmittedUrl;

/// Internal cursor-layer errors. Callers translate these to
/// `AccountError` via `cursor_error_to_account_error` before
/// emitting them at the account boundary.
#[derive(Debug)]
pub(crate) enum CursorError {
    /// Cursor belongs to a different protocol.
    ProtocolMismatch,
    /// Cursor envelope version is ahead of what this build understands.
    EnvelopeUnknown,
    /// Cursor envelope version is older than this build can migrate.
    SchemaIncompatible,
    /// Cursor operation is not supported for this scope.
    Unsupported,
    /// A calendarView cursor's fixed end bound is too near. It must be
    /// reseeded so Graph mints a delta link over a fresh window.
    CalendarWindowExpired,
    /// A mid-inventory page cursor was handed to the changes walk, which
    /// has no delta link to resume from. The cursor itself is well formed,
    /// so this restarts the one scope rather than declaring the stored
    /// schema incompatible account-wide.
    InventoryInProgress,
    /// A persisted foreign scope refers to a shared mailbox no longer
    /// configured on this account.
    Configuration(String),
    /// Serializing a cursor this crate built failed - a local invariant.
    Encode(String),
    /// The cursor or page-marker bytes the caller handed back do not decode.
    /// Refused before any request, and the bytes are the caller's durable
    /// state rather than anything Graph sent, so this is never a provider
    /// fault: it maps with the other unreadable-cursor arms to
    /// `SyncState(SchemaIncompatible)`, which clears and re-establishes.
    Decode(String),
}

impl std::fmt::Display for CursorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProtocolMismatch => f.write_str("cursor protocol mismatch"),
            Self::EnvelopeUnknown => f.write_str("cursor envelope version unknown"),
            Self::SchemaIncompatible => f.write_str("cursor schema incompatible"),
            Self::Unsupported => f.write_str("unsupported cursor scope"),
            Self::CalendarWindowExpired => f.write_str("calendar cursor window expired"),
            Self::InventoryInProgress => {
                f.write_str("cursor is a mid-inventory page position, not a changes cursor")
            }
            Self::Configuration(msg) => write!(f, "cursor configuration mismatch: {msg}"),
            Self::Encode(msg) => write!(f, "cursor encode error: {msg}"),
            Self::Decode(msg) => write!(f, "cursor decode error: {msg}"),
        }
    }
}

/// Project a scope-routing failure onto the cursor vocabulary.
///
/// `client_for_scope` refuses only for configuration reasons - it resolves
/// a map entry and issues no request - so `Configuration` is the arm that
/// matters, and it must reach `ScopeRevoked` so the engine disables just
/// the stale scope. `GraphError` is the crate-wide type, though, so the
/// remaining arms fold into `Encode`: a transport or response error
/// arriving from a purely local lookup would be a contract violation of
/// this function's own precondition, not a syncable condition.
///
/// Shared by both cursor entry points. `initial_delta_url` rejects the
/// scope before the first request; `changes_stream` rejects it before
/// resuming a persisted `@odata.deltaLink`, which is namespace-correct and
/// would otherwise keep succeeding for a mailbox this account no longer
/// configures - leaving a live scope whose every object id fails hydration
/// and mutation terminally, because those paths DO consult the map.
pub(crate) fn routing_error(error: crate::error::GraphError) -> CursorError {
    match error {
        crate::error::GraphError::Configuration { message } => CursorError::Configuration(message),
        other => CursorError::Encode(format!("{other:?}")),
    }
}

/// Graph's inner (protocol-owned) cursor envelope version.
///
/// v2: `ThreadId`s minted from a shared mailbox carry the owner tag
/// (`"{mailbox}\u{1f}{conversationId}"`) the same way message ids already
/// did. That is an OBJECT-ID encoding change, not a payload-shape change,
/// so it is not expressible as an additive `serde(default)` field: a v1
/// account resumes its delta link, never re-runs inventory, and keeps
/// emitting bare thread ids that still parse - as PRIMARY - and therefore
/// route thread hydration and thread-targeted writes at the wrong mailbox.
/// Bumping forces `decode_cursor` to refuse a v1 cursor as
/// `SchemaIncompatible`, which derives `Engine(SchemaIncompatible)`: the
/// engine drops every durable cursor and re-establishes each scope through
/// a full `inventory_stream` pass, which re-mints the ids under the new
/// encoding. Reseeding is the cheapest correct migration here - the ids are
/// server-issued and not reconstructable from the stored bytes.
///
/// v3: calendarView delta links retain their original end bound, so the
/// payload records that horizon and forces a scoped reseed before it expires.
/// v4: an acknowledged initial-inventory page has a next link but no delta
/// link; the payload distinguishes that resumable inventory position from a
/// live changes cursor so bifrost-sync can restart the right stream.
/// v5: delta links minted with `$top` carry that cap into every later round,
/// and the inventory behind them stopped after one page. Refusing them
/// reseeds every scope with paging through `Prefer: odata.maxpagesize`.
pub(crate) const GRAPH_CURSOR_ENVELOPE_VERSION: u32 = 5;
pub(crate) const CHANGE_CURSOR_ENVELOPE_VERSION: u32 =
    bifrost_types::CHANGE_CURSOR_ENVELOPE_VERSION;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub(crate) enum GraphCursorKind {
    Messages {
        folder_id: String,
    },
    Events {
        calendar_id: String,
    },
    Contacts {
        folder_id: String,
    },
    /// Public folder: no delta token. Routing + poll state live here.
    /// The enclosing `GraphCursorPayload`'s `delta_link` /
    /// `advanced_through` fields are unused for this kind (a public
    /// folder is never a server-issued delta cursor).
    PublicFolder(PublicFolderCursor),
}

/// The maximum live-id snapshot a public-folder cursor carries. The
/// snapshot rides inside the opaque cursor the consumer re-persists on
/// every checkpoint, so an unbounded vector would be a steady write-
/// amplification tax. Above the cap the folder degrades to additions-
/// only (no deletion reconcile) and stores an empty snapshot.
///
/// Nothing is broken in that degraded mode: additions-only is correct for
/// what it emits and is covered by tests. It is a capability ceiling, not a
/// defect - a folder above the cap simply stops reporting deletions (and
/// in-place edits, since `live_versions` empties with the snapshot).
/// Restoring reconcile for huge folders means moving the baseline OUT of the
/// cursor and into a durable side table, which bifrost does not have: the
/// engine's checkpoint store is engine-held, is never handed to an `Account`
/// impl, exposes only change cursors and backfill checkpoints keyed by
/// account/scope, and is not even nameable from this crate. Raising the cap
/// instead is not the fix - the snapshot rides in the opaque cursor the
/// consumer re-persists on every checkpoint, so the cap IS the write-
/// amplification bound. Do not schedule this until a generic side-table
/// surface exists and a handle to it is threaded into account open.
pub(crate) const PUBLIC_FOLDER_LIVE_IDS_CAP: usize = 10_000;

/// How long to wait between full-id deletion reconcile scans, in
/// seconds (ported from ratatoskr `DELETION_SCAN_INTERVAL_SECS`).
pub(crate) const FULL_SCAN_INTERVAL_SECS: u64 = 3600;

/// Public-folder cursor: the entire sync state for a no-delta-token
/// public folder. The watermark drives the incremental timestamp poll;
/// `last_full_scan_at` throttles the deletion reconcile; `live_ids` is
/// the deletion baseline the next scan diffs against. All of it rides
/// in the opaque cursor so a cold resume reconstructs everything with
/// no engine-side side table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PublicFolderCursor {
    /// Native EWS folder id of the public folder.
    pub(crate) folder_id: String,
    /// Routing context, reconstructable on a cold resume.
    pub(crate) routing: PublicFolderRouting,
    /// High-water `DateTimeReceived` (RFC-3339 UTC). `None` before the
    /// first poll.
    pub(crate) watermark: Option<String>,
    /// Unix seconds of the last full-id deletion scan. `None` if never
    /// scanned.
    pub(crate) last_full_scan_at: Option<u64>,
    /// Authoritative live-id snapshot from the last full scan, the
    /// deletion baseline the next scan diffs against. Empty when the
    /// last scan exceeded `PUBLIC_FOLDER_LIVE_IDS_CAP` - in that
    /// degraded state the folder syncs additions only and reconcile is
    /// skipped.
    #[serde(default)]
    pub(crate) live_ids: Vec<String>,
    /// Change keys captured by the last complete scan. This is the
    /// in-place-change baseline for items whose received timestamp does not
    /// move when flags, categories, or read state change.
    #[serde(default)]
    pub(crate) live_versions: Vec<(String, String)>,
    /// Ids whose `received_at` equals the current `watermark`, captured
    /// on the last incremental poll. The incremental `FindItem`
    /// restriction is `>=` (so items sharing the boundary second are not
    /// missed), which means the boundary item(s) re-appear on every
    /// subsequent poll. Skipping ids in this set prevents a quiet folder
    /// from re-emitting its newest item as `Updated`+`Added` forever,
    /// while still admitting a genuinely new item that lands on the same
    /// boundary second. Additive (`serde(default)`); a v1 cursor without
    /// it just re-emits the boundary once, then converges.
    #[serde(default)]
    pub(crate) boundary_ids: Vec<String>,
    /// Whether the folder is in the over-cap additions-only degraded
    /// mode (deletion reconcile disabled). Distinct from "empty
    /// `live_ids`", which is ambiguous (an empty folder also has none).
    /// Tracking it explicitly lets the changes path warn only on the
    /// *transition* into degraded mode instead of re-warning on every
    /// hourly scan. Additive (`serde(default)` -> `false`).
    #[serde(default)]
    pub(crate) degraded: bool,
    /// Item-class local-names (`Task`, `PostItem`, ...) already surfaced
    /// in an unhandled-class `Warning` for this folder. Without this the
    /// same class re-warns on every poll (the operator sees identical
    /// noise hourly); recording the already-reported classes lets a poll
    /// warn only on NEWLY-seen classes. Both the incremental poll and the
    /// full scan contribute observed classes. Additive (`serde(default)`
    /// -> empty; a v1 cursor re-warns once, then converges).
    #[serde(default)]
    pub(crate) warned_classes: Vec<String>,
}

/// Public-folder EWS routing context. Carried in the cursor so the
/// hierarchy/content mailbox pair survives a cold resume. Mirrors the
/// `X-AnchorMailbox` / `X-PublicFolderMailbox` routing pair EWS expects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PublicFolderRouting {
    /// `X-AnchorMailbox` value: the content mailbox SMTP for content
    /// ops, the hierarchy mailbox for hierarchy (FindFolder) ops.
    pub(crate) anchor_mailbox: String,
    /// `X-PublicFolderMailbox` value: the content mailbox (content ops)
    /// or the hierarchy server (hierarchy ops). `None` when unset.
    pub(crate) public_folder_mailbox: Option<String>,
}

impl PublicFolderRouting {
    /// Materialize the EWS routing headers for a request against this
    /// public folder. Owned->owned clone of both fields.
    pub(crate) fn headers(&self) -> crate::ews::EwsHeaders {
        crate::ews::EwsHeaders {
            anchor_mailbox: Some(self.anchor_mailbox.clone()),
            public_folder_mailbox: self.public_folder_mailbox.clone(),
        }
    }
}

/// Apply the live-id cap: returns the scan set when at/below `cap`, or
/// `None` (degrade to additions-only, store no snapshot) when above it.
/// Pure so the cap decision is unit-pinnable.
pub(crate) fn cap_live_ids(ids: Vec<String>, cap: usize) -> Option<Vec<String>> {
    if ids.len() > cap { None } else { Some(ids) }
}

// Graph delta cursors need their nextLink plus last id inside the shared opaque progress bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GraphPageMarker {
    pub(crate) next_link: String,
    pub(crate) last_seen_id: Option<String>,
}

// Graph deltaLink payloads are protocol-specific and intentionally live inside OpaqueChangeState.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GraphCursorPayload {
    pub(crate) kind: GraphCursorKind,
    pub(crate) delta_link: String,
    #[serde(default)]
    pub(crate) advanced_through: Option<GraphPageMarker>,
    /// The marker is an inventory page position rather than a delta page
    /// position. It has no delta link yet and must resume through
    /// `inventory_stream`, never `changes_stream`.
    #[serde(default)]
    pub(crate) inventory_in_progress: bool,
    /// Unix timestamp of the end of the calendarView window that minted this
    /// cursor. Calendar delta links retain their original query bounds, so a
    /// live cursor must be reseeded before this horizon reaches the present.
    ///
    /// v3 is deliberately a reseed boundary rather than an additive read:
    /// v2 cursors have no recoverable window horizon, and treating one as
    /// current would preserve the frozen-window bug indefinitely.
    #[serde(default)]
    pub(crate) calendar_window_end: Option<i64>,
}

/// Re-admit a persisted resume link (a delta link or a page marker's next
/// link) onto the resuming client's api-base.
///
/// Persisted links are the consumer's durable state: serde reads them back
/// as plain text, because the origin they must match belongs to the client,
/// not to the bytes. Every link this crate persists is an admitted absolute
/// server link, so anything else - an empty link, a path, another origin -
/// is a cursor this crate did not produce, refused before any request as
/// `Decode` (`SyncState(SchemaIncompatible)`, which reseeds the scope).
/// Following it instead would send the account bearer wherever it names.
pub(crate) fn admit_resume_url(
    client: &crate::client::GraphClient,
    link: &str,
) -> Result<AdmittedUrl, CursorError> {
    if reqwest::Url::parse(link).is_err() {
        return Err(CursorError::Decode(
            "persisted resume link is not an absolute URL".to_string(),
        ));
    }
    client
        .admit_target(link)
        .map_err(|refusal| CursorError::Decode(format!("persisted resume link {refusal}")))
}

impl GraphCursorPayload {
    /// A live delta cursor. Takes the delta link as an `AdmittedUrl`, so an
    /// unadmitted server link cannot be persisted.
    pub(crate) fn new(
        kind: GraphCursorKind,
        delta_link: &AdmittedUrl,
        advanced_through: Option<GraphPageMarker>,
    ) -> Self {
        Self {
            kind,
            delta_link: delta_link.as_str().to_string(),
            advanced_through,
            inventory_in_progress: false,
            calendar_window_end: None,
        }
    }

    pub(crate) fn with_calendar_window_end_option(mut self, end: Option<i64>) -> Self {
        self.calendar_window_end = end;
        self
    }

    pub(crate) fn inventory_page(
        kind: GraphCursorKind,
        marker: GraphPageMarker,
        calendar_window_end: Option<i64>,
    ) -> Self {
        Self {
            kind,
            delta_link: String::new(),
            advanced_through: Some(marker),
            inventory_in_progress: true,
            calendar_window_end,
        }
    }

    /// Build a public-folder payload. There is no delta link, so the
    /// delta-specific fields are left empty; the entire state lives in
    /// the `PublicFolderCursor` carried by the `kind`.
    pub(crate) fn public_folder(cursor: PublicFolderCursor) -> Self {
        Self {
            kind: GraphCursorKind::PublicFolder(cursor),
            delta_link: String::new(),
            advanced_through: None,
            inventory_in_progress: false,
            calendar_window_end: None,
        }
    }

    pub(crate) fn resume_url(&self) -> &str {
        self.advanced_through
            .as_ref()
            .map_or(self.delta_link.as_str(), |marker| marker.next_link.as_str())
    }
}

pub(crate) fn kind_for_scope(scope: &CursorScope) -> Result<GraphCursorKind, CursorError> {
    match scope {
        CursorScope::FolderType { folder, ty } => match ty {
            ObjectType::Email => Ok(GraphCursorKind::Messages {
                folder_id: folder.0.clone(),
            }),
            ObjectType::Event | ObjectType::CalendarEvent => Ok(GraphCursorKind::Events {
                calendar_id: folder.0.clone(),
            }),
            ObjectType::Contact => Ok(GraphCursorKind::Contacts {
                folder_id: folder.0.clone(),
            }),
            _ => Err(CursorError::Unsupported),
        },
        _ => Err(CursorError::Unsupported),
    }
}

pub(crate) fn scope_for_kind(kind: &GraphCursorKind) -> CursorScope {
    match kind {
        GraphCursorKind::Messages { folder_id } => CursorScope::FolderType {
            folder: FolderId(folder_id.clone()),
            ty: ObjectType::Email,
        },
        GraphCursorKind::Events { calendar_id } => CursorScope::FolderType {
            folder: FolderId(calendar_id.clone()),
            ty: ObjectType::Event,
        },
        GraphCursorKind::Contacts { folder_id } => CursorScope::FolderType {
            folder: FolderId(folder_id.clone()),
            ty: ObjectType::Contact,
        },
        // A public folder surfaces as a bare `CursorScope::Folder`
        // carrying the native EWS folder id; all routing rides in the
        // cursor payload, not the scope.
        GraphCursorKind::PublicFolder(pf) => CursorScope::Folder(FolderId(pf.folder_id.clone())),
    }
}

pub(crate) fn encode_cursor(
    scope: CursorScope,
    payload: GraphCursorPayload,
) -> Result<ChangeCursor, CursorError> {
    let progress = payload
        .advanced_through
        .as_ref()
        .map(encode_page_marker)
        .transpose()?;
    let bytes =
        serde_json::to_vec(&payload).map_err(|error| CursorError::Encode(error.to_string()))?;
    Ok(ChangeCursor {
        scope,
        server_state: OpaqueChangeState {
            protocol: ProtocolKind::Graph,
            envelope_version: GRAPH_CURSOR_ENVELOPE_VERSION,
            bytes,
        },
        advanced_through: progress,
        envelope_version: CHANGE_CURSOR_ENVELOPE_VERSION,
    })
}

pub(crate) fn decode_cursor(cursor: &ChangeCursor) -> Result<GraphCursorPayload, CursorError> {
    if cursor.server_state.protocol != ProtocolKind::Graph {
        return Err(CursorError::ProtocolMismatch);
    }
    if cursor.server_state.envelope_version > GRAPH_CURSOR_ENVELOPE_VERSION {
        return Err(CursorError::EnvelopeUnknown);
    }
    if cursor.server_state.envelope_version < GRAPH_CURSOR_ENVELOPE_VERSION {
        return Err(CursorError::SchemaIncompatible);
    }

    let mut payload: GraphCursorPayload = serde_json::from_slice(&cursor.server_state.bytes)
        .map_err(|error| CursorError::Decode(error.to_string()))?;
    // The outer `ChangeCursor::advanced_through` is the single source of
    // truth for page progress, in BOTH directions: the engine persists it
    // separately from the opaque payload bytes and may have acked a later
    // page than the bytes recorded, so it overrides; and it may equally have
    // acked no page at all, so its absence must clear the payload's copy
    // rather than let a marker the engine never acknowledged pick the resume
    // URL. Overriding on presence but deferring on absence made one field
    // have two sources with a silent precedence rule; `encode_cursor` writes
    // both from the same value, so honoring the outer unconditionally loses
    // nothing.
    payload.advanced_through = cursor
        .advanced_through
        .as_ref()
        .map(decode_page_marker)
        .transpose()?;
    Ok(payload)
}

pub(crate) fn encode_page_marker(
    marker: &GraphPageMarker,
) -> Result<OpaqueProgressBytes, CursorError> {
    serde_json::to_vec(marker)
        .map(OpaqueProgressBytes)
        .map_err(|error| CursorError::Encode(error.to_string()))
}

pub(crate) fn decode_page_marker(
    progress: &OpaqueProgressBytes,
) -> Result<GraphPageMarker, CursorError> {
    serde_json::from_slice(&progress.0).map_err(|error| CursorError::Decode(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_cursor_round_trips() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let marker = GraphPageMarker {
            next_link: "https://graph.microsoft.com/v1.0/next".to_string(),
            last_seen_id: Some("m1".to_string()),
        };
        let payload = GraphCursorPayload::new(
            kind_for_scope(&scope).expect("scope should map"),
            &AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
            Some(marker.clone()),
        );

        let cursor = encode_cursor(scope.clone(), payload).expect("cursor should encode");
        assert_eq!(cursor.scope, scope);
        assert_eq!(cursor.server_state.protocol, ProtocolKind::Graph);
        assert_eq!(
            cursor.server_state.envelope_version,
            GRAPH_CURSOR_ENVELOPE_VERSION
        );

        let decoded = decode_cursor(&cursor).expect("cursor should decode");
        assert_eq!(decoded.advanced_through, Some(marker));
        assert_eq!(
            decoded.resume_url(),
            "https://graph.microsoft.com/v1.0/next"
        );
    }

    #[test]
    fn rejects_other_protocol_cursor() {
        let cursor = ChangeCursor {
            scope: CursorScope::Account,
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Gmail,
                envelope_version: GRAPH_CURSOR_ENVELOPE_VERSION,
                bytes: Vec::new(),
            },
            advanced_through: None,
            envelope_version: CHANGE_CURSOR_ENVELOPE_VERSION,
        };

        assert!(matches!(
            decode_cursor(&cursor),
            Err(CursorError::ProtocolMismatch)
        ));
    }

    #[test]
    fn rejects_future_envelope_version() {
        let cursor = ChangeCursor {
            scope: CursorScope::Account,
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Graph,
                envelope_version: GRAPH_CURSOR_ENVELOPE_VERSION + 1,
                bytes: b"{}".to_vec(),
            },
            advanced_through: None,
            envelope_version: CHANGE_CURSOR_ENVELOPE_VERSION,
        };

        assert!(matches!(
            decode_cursor(&cursor),
            Err(CursorError::EnvelopeUnknown)
        ));
    }

    #[test]
    fn rejects_past_envelope_version_as_schema_incompatible() {
        let cursor = ChangeCursor {
            scope: CursorScope::Account,
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Graph,
                envelope_version: 0,
                bytes: b"{}".to_vec(),
            },
            advanced_through: None,
            envelope_version: CHANGE_CURSOR_ENVELOPE_VERSION,
        };

        assert!(matches!(
            decode_cursor(&cursor),
            Err(CursorError::SchemaIncompatible)
        ));
    }

    #[test]
    fn rejects_truncated_payload_bytes() {
        let cursor = ChangeCursor {
            scope: CursorScope::Account,
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Graph,
                envelope_version: GRAPH_CURSOR_ENVELOPE_VERSION,
                bytes: b"{not-json".to_vec(),
            },
            advanced_through: None,
            envelope_version: CHANGE_CURSOR_ENVELOPE_VERSION,
        };

        assert!(matches!(
            decode_cursor(&cursor),
            Err(CursorError::Decode(_))
        ));
    }

    #[test]
    fn rejects_truncated_progress_bytes() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let payload = GraphCursorPayload::new(
            kind_for_scope(&scope).expect("scope should map"),
            &AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
            None,
        );
        let cursor = ChangeCursor {
            scope: scope.clone(),
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Graph,
                envelope_version: GRAPH_CURSOR_ENVELOPE_VERSION,
                bytes: serde_json::to_vec(&payload).expect("serialize"),
            },
            advanced_through: Some(OpaqueProgressBytes(b"{nope".to_vec())),
            envelope_version: CHANGE_CURSOR_ENVELOPE_VERSION,
        };

        assert!(matches!(
            decode_cursor(&cursor),
            Err(CursorError::Decode(_))
        ));
    }

    #[test]
    fn kind_for_scope_rejects_unsupported_object_type() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Mailbox,
        };
        assert!(matches!(
            kind_for_scope(&scope),
            Err(CursorError::Unsupported)
        ));
    }

    #[test]
    fn kind_for_scope_rejects_non_folder_type_scope() {
        assert!(matches!(
            kind_for_scope(&CursorScope::Account),
            Err(CursorError::Unsupported)
        ));
    }

    fn public_folder_cursor() -> PublicFolderCursor {
        PublicFolderCursor {
            folder_id: "AAMkPF=".to_string(),
            routing: PublicFolderRouting {
                anchor_mailbox: "content@contoso.com".to_string(),
                public_folder_mailbox: Some("pf@contoso.com".to_string()),
            },
            watermark: Some("2026-03-01T10:00:00Z".to_string()),
            last_full_scan_at: Some(1_700_000_000),
            live_ids: vec!["a".to_string(), "b".to_string()],
            live_versions: Vec::new(),
            boundary_ids: vec!["b".to_string()],
            degraded: false,
            warned_classes: vec!["Task".to_string()],
        }
    }

    #[test]
    fn public_folder_cursor_round_trips() {
        let pf = public_folder_cursor();
        let scope = CursorScope::Folder(FolderId(pf.folder_id.clone()));
        let payload = GraphCursorPayload::public_folder(pf.clone());

        let cursor = encode_cursor(scope.clone(), payload).expect("encode");
        assert_eq!(cursor.scope, scope);
        assert_eq!(cursor.server_state.protocol, ProtocolKind::Graph);
        assert_eq!(
            cursor.server_state.envelope_version,
            GRAPH_CURSOR_ENVELOPE_VERSION
        );

        let decoded = decode_cursor(&cursor).expect("decode");
        match decoded.kind {
            GraphCursorKind::PublicFolder(got) => assert_eq!(got, pf),
            other => panic!("expected PublicFolder kind, got {other:?}"),
        }
        // `scope_for_kind` round-trips the native folder id.
        let kind = GraphCursorKind::PublicFolder(pf.clone());
        assert_eq!(scope_for_kind(&kind), scope);
    }

    #[test]
    fn old_message_cursor_still_decodes_after_public_folder_variant() {
        // A `Messages` cursor encoded with the same envelope version
        // still decodes unchanged - proof the additive `PublicFolder`
        // variant did not break the existing kinds (no version bump).
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let payload = GraphCursorPayload::new(
            kind_for_scope(&scope).expect("scope should map"),
            &AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
            None,
        );
        let cursor = encode_cursor(scope, payload).expect("encode");
        let decoded = decode_cursor(&cursor).expect("decode");
        assert!(matches!(decoded.kind, GraphCursorKind::Messages { .. }));
    }

    #[test]
    fn progress_bytes_win_over_the_page_marker_serialized_in_the_payload() {
        // The engine persists `ChangeCursor::advanced_through` separately
        // from the opaque payload bytes and may ack a later page than the
        // bytes recorded. `decode_cursor` therefore lets the outer progress
        // override the inner marker, so a resume follows the checkpoint the
        // engine actually acknowledged.
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let stale = GraphPageMarker {
            next_link: "https://graph.microsoft.com/v1.0/page-1".to_string(),
            last_seen_id: Some("m1".to_string()),
        };
        let acked = GraphPageMarker {
            next_link: "https://graph.microsoft.com/v1.0/page-9".to_string(),
            last_seen_id: Some("m9".to_string()),
        };
        let payload = GraphCursorPayload::new(
            kind_for_scope(&scope).expect("scope should map"),
            &AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
            Some(stale),
        );
        let mut cursor = encode_cursor(scope, payload).expect("encode");
        cursor.advanced_through = Some(encode_page_marker(&acked).expect("encode marker"));

        let decoded = decode_cursor(&cursor).expect("decode");
        assert_eq!(decoded.advanced_through, Some(acked));
        assert_eq!(
            decoded.resume_url(),
            "https://graph.microsoft.com/v1.0/page-9"
        );
    }

    /// The other half of the same rule: the outer progress is authoritative
    /// when ABSENT too. A payload marker the engine never acknowledged must
    /// not survive to pick the resume URL, or the field has two sources with
    /// a silent precedence between them.
    #[test]
    fn an_absent_outer_progress_clears_the_payload_marker() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let unacked = GraphPageMarker {
            next_link: "https://graph.microsoft.com/v1.0/page-1".to_string(),
            last_seen_id: Some("m1".to_string()),
        };
        let payload = GraphCursorPayload::new(
            kind_for_scope(&scope).expect("scope should map"),
            &AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
            Some(unacked),
        );
        let mut cursor = encode_cursor(scope, payload).expect("encode");
        cursor.advanced_through = None;

        let decoded = decode_cursor(&cursor).expect("decode");
        assert_eq!(decoded.advanced_through, None);
        assert_eq!(
            decoded.resume_url(),
            "https://graph.microsoft.com/v1.0/delta"
        );
    }

    #[test]
    fn resume_url_falls_back_to_the_delta_link_without_a_marker() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let payload = GraphCursorPayload::new(
            kind_for_scope(&scope).expect("scope should map"),
            &AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
            None,
        );
        assert_eq!(
            payload.resume_url(),
            "https://graph.microsoft.com/v1.0/delta"
        );
    }

    #[test]
    fn encode_does_not_cross_check_the_scope_against_the_payload_kind() {
        // The scope/kind agreement guard lives in
        // `inventory::scope_matches_payload`, asserted by `changes_stream`,
        // NOT in the codec. Pin the split so a future reader does not
        // assume `decode_cursor` alone is enough to trust a stored cursor:
        // a persisted cursor whose bytes name a different collection than
        // its scope decodes cleanly here and is rejected one layer up.
        let email_scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let contact_scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Contact,
        };
        let payload = GraphCursorPayload::new(
            kind_for_scope(&contact_scope).expect("contact scope maps"),
            &AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
            None,
        );
        let cursor = encode_cursor(email_scope.clone(), payload).expect("encode");
        assert_eq!(cursor.scope, email_scope);

        let decoded = decode_cursor(&cursor).expect("codec accepts the mismatch");
        assert!(matches!(decoded.kind, GraphCursorKind::Contacts { .. }));
        assert_ne!(scope_for_kind(&decoded.kind), email_scope);
    }

    #[test]
    fn an_empty_delta_link_survives_the_round_trip() {
        // Nothing in the codec rejects a delta-less delta cursor. It is
        // reachable only from a corrupted or hand-written cursor, and the
        // resume refuses it before any request (`admit_resume_url`), so this
        // pins only that the codec itself stays permissive, not that empty
        // is meaningful.
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let mut payload = GraphCursorPayload::new(
            kind_for_scope(&scope).expect("scope should map"),
            &AdmittedUrl::for_tests("https://graph.microsoft.com/v1.0/delta"),
            None,
        );
        payload.delta_link = String::new();
        let cursor = encode_cursor(scope, payload).expect("encode");
        let decoded = decode_cursor(&cursor).expect("decode");
        assert_eq!(decoded.resume_url(), "");
    }

    #[test]
    fn public_folder_kind_ignores_the_envelope_delta_fields() {
        // The payload IS the sync state for a public folder; the
        // delta-specific fields must stay empty so no code path mistakes it
        // for a server-issued cursor.
        let payload = GraphCursorPayload::public_folder(public_folder_cursor());
        assert!(payload.delta_link.is_empty());
        assert!(payload.advanced_through.is_none());
        assert_eq!(payload.resume_url(), "");
    }

    #[test]
    fn a_v1_public_folder_cursor_without_the_additive_fields_decodes() {
        // `boundary_ids` / `degraded` / `warned_classes` / `live_ids` are
        // all `serde(default)` additions. A cursor persisted before they
        // existed must still decode, or every public folder would reseed.
        let bytes = serde_json::to_vec(&serde_json::json!({
            "kind": {
                "PublicFolder": {
                    "folder_id": "AAMkPF=",
                    "routing": {
                        "anchor_mailbox": "content@contoso.com",
                        "public_folder_mailbox": null
                    },
                    "watermark": "2026-03-01T10:00:00Z",
                    "last_full_scan_at": null
                }
            },
            "delta_link": "",
            "issued_at_unix_secs": 1700000000
        }))
        .expect("serialize legacy payload");
        let cursor = ChangeCursor {
            scope: CursorScope::Folder(FolderId("AAMkPF=".to_string())),
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Graph,
                envelope_version: GRAPH_CURSOR_ENVELOPE_VERSION,
                bytes,
            },
            advanced_through: None,
            envelope_version: CHANGE_CURSOR_ENVELOPE_VERSION,
        };

        let decoded = decode_cursor(&cursor).expect("legacy public-folder cursor decodes");
        match decoded.kind {
            GraphCursorKind::PublicFolder(pf) => {
                assert!(pf.live_ids.is_empty());
                assert!(pf.boundary_ids.is_empty());
                assert!(pf.warned_classes.is_empty());
                assert!(!pf.degraded);
                assert_eq!(pf.routing.public_folder_mailbox, None);
            }
            other => panic!("expected PublicFolder, got {other:?}"),
        }
    }

    #[test]
    fn scope_for_kind_collapses_calendar_event_onto_event() {
        // Documented lossiness: the Events cursor cannot record whether the
        // scope asked for `Event` or `CalendarEvent`, so `scope_for_kind`
        // always answers `Event` and `scope_matches_payload` compensates
        // with the alias rule.
        let scope = CursorScope::FolderType {
            folder: FolderId("calendar".to_string()),
            ty: ObjectType::CalendarEvent,
        };
        let kind = kind_for_scope(&scope).expect("calendar event scope maps");
        assert_eq!(
            scope_for_kind(&kind),
            CursorScope::FolderType {
                folder: FolderId("calendar".to_string()),
                ty: ObjectType::Event,
            }
        );
    }

    #[test]
    fn live_ids_capped_degrades_to_additions_only() {
        let under: Vec<String> = (0..3).map(|i| i.to_string()).collect();
        assert_eq!(cap_live_ids(under.clone(), 3), Some(under));

        let over: Vec<String> = (0..5).map(|i| i.to_string()).collect();
        assert_eq!(cap_live_ids(over, 3), None);
    }
}
