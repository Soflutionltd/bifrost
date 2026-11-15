use bifrost_types::{
    ChangeCursor, CursorScope, ObjectType, OpaqueChangeState, ProtocolKind, QueryId,
};

/// JMAP's INNER (protocol-owned) cursor payload envelope version - the one
/// stamped on `OpaqueChangeState::envelope_version`. Distinct axis from
/// `OUTER_CURSOR_ENVELOPE_VERSION`, which versions the `ChangeCursor` wrapper
/// `bifrost-types` owns; the two move independently.
///
/// v2: a foreign (shared/delegate) account's `ThreadId`s carry the owning
/// JMAP `accountId` (`"{accountId}\u{1f}{threadId}"`), the same object
/// namespace their `Email` ids and `blobId`s already used. That is an
/// OBJECT-ID encoding change, not a payload-shape change, so it cannot be
/// expressed as an additive field: a v1 bare foreign thread id still
/// parses - as PRIMARY - and there is no way to tell the two apart after
/// the fact. A v1 account would resume its `Email/changes` state, never
/// re-run inventory, and keep handing the consumer bare foreign thread
/// ids, so `thread_hydrate` and every thread-keyed mutation
/// (`set_keyword`/`set_is_read`/`set_importance` on
/// `MutationTarget::Thread`, `move_thread`, `delete_thread`) would resolve
/// an unrelated PRIMARY thread through `Thread/get` on an id collision and
/// apply the write to its messages - `delete_thread` destroys them.
///
/// Bumping makes `decode` refuse a v1 cursor as `SchemaIncompatible`,
/// which derives `Engine(SchemaIncompatible)`: the engine drops every
/// durable cursor and re-establishes each scope through a full
/// `inventory_stream` pass, which re-mints the ids under the new
/// encoding. The ids are server-issued and not reconstructable from the
/// stored bytes, so reseeding IS the migration (identical reasoning to
/// bifrost-graph's v2 bump).
pub(crate) const PAYLOAD_ENVELOPE_VERSION: u32 = 2;

/// The OUTER envelope version, stamped on `ChangeCursor::envelope_version`.
/// This versions the `bifrost-types` cursor wrapper (scope + opaque payload +
/// `advanced_through`), not the JMAP payload inside it, and has never needed a
/// bump. Bumping `PAYLOAD_ENVELOPE_VERSION` does not touch this one.
pub(crate) const OUTER_CURSOR_ENVELOPE_VERSION: u32 = bifrost_types::CHANGE_CURSOR_ENVELOPE_VERSION;

const STATE_TAG_V1: u8 = 1;

const SCOPE_TAG_EMAIL: u8 = 1;
const SCOPE_TAG_MAILBOX: u8 = 2;
const SCOPE_TAG_THREAD: u8 = 3;
const SCOPE_TAG_QUERY: u8 = 4;
// Additive: foreign (shared/delegate) account mailbox scope. The tag
// itself needed no envelope-version bump when it landed (tags 1-4 kept
// decoding); the LATER v2 bump was forced by the object-id encoding
// change (bare foreign thread ids), not by this tag.
const SCOPE_TAG_FOLDER: u8 = 5;

// types: protocol-owned cursor payload inside bifrost-types::OpaqueChangeState.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum JmapCursorState {
    V1 {
        scope: JmapScopeRepr,
        state_string: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum JmapScopeRepr {
    Email,
    Mailbox,
    Thread,
    Query(String),
    /// A foreign (shared/delegate) account scope. The owning JMAP
    /// `accountId` (and mailbox part) are recovered from the
    /// `CursorScope::Folder` codec so a cold resume routes to the same
    /// account. The seeded shape is account-level (`mailbox_id` empty,
    /// via `encode_foreign_account`); a legacy per-mailbox cursor
    /// (non-empty `mailbox_id`) still decodes under the same tag.
    Folder {
        account_id: String,
        mailbox_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum JmapCursorError {
    UnsupportedScope,
    CursorProtocolMismatch,
    CursorEnvelopeUnknown,
    SchemaIncompatible,
    Other(String),
}

impl JmapScopeRepr {
    /// Keep the persisted representation separate from discovery topology.
    /// Thread and Query tags remain readable but are not drivable. Foreign
    /// per-mailbox cursors remain drivable: inventory filters by inMailbox,
    /// while changes follows the owning account's Email state. Replacing this
    /// representation with only the three currently seeded shapes would remove
    /// that existing capability and reject readable v2 rows without a migration.
    pub(crate) fn from_cursor_scope(scope: &CursorScope) -> Result<Self, JmapCursorError> {
        match scope {
            CursorScope::Type(ObjectType::Email) => Ok(Self::Email),
            CursorScope::Type(ObjectType::Mailbox) => Ok(Self::Mailbox),
            CursorScope::Type(ObjectType::Thread) => Ok(Self::Thread),
            CursorScope::Query(query) => Ok(Self::Query(query.0.clone())),
            CursorScope::Folder(folder) => super::foreign::parse_foreign(folder)
                .map(|parsed| Self::Folder {
                    account_id: parsed.account_id,
                    mailbox_id: parsed.mailbox_id,
                })
                .ok_or(JmapCursorError::UnsupportedScope),
            _ => Err(JmapCursorError::UnsupportedScope),
        }
    }

    /// One exhaustive account-drive classification, shared by descriptors,
    /// establishment and push. A valid folder encoding is required; an arbitrary
    /// Folder id must never acquire an Email subscription by its shape alone.
    pub(crate) fn data_type(&self) -> Option<crate::DataType> {
        match self {
            Self::Email | Self::Folder { .. } => Some(crate::DataType::Email),
            Self::Mailbox => Some(crate::DataType::Mailbox),
            Self::Thread | Self::Query(_) => None,
        }
    }

    pub(crate) fn to_cursor_scope(&self) -> CursorScope {
        match self {
            Self::Email => CursorScope::Type(ObjectType::Email),
            Self::Mailbox => CursorScope::Type(ObjectType::Mailbox),
            Self::Thread => CursorScope::Type(ObjectType::Thread),
            Self::Query(query) => CursorScope::Query(QueryId(query.clone())),
            Self::Folder {
                account_id,
                mailbox_id,
            } => CursorScope::Folder(super::foreign::encode_foreign(account_id, mailbox_id)),
        }
    }
}

pub(crate) fn encode(state: &JmapCursorState) -> OpaqueChangeState {
    OpaqueChangeState {
        protocol: ProtocolKind::Jmap,
        envelope_version: PAYLOAD_ENVELOPE_VERSION,
        bytes: encode_state(state),
    }
}

pub(crate) fn encode_for_scope(
    scope: &CursorScope,
    state_string: impl Into<String>,
) -> Result<OpaqueChangeState, JmapCursorError> {
    Ok(encode(&JmapCursorState::V1 {
        scope: JmapScopeRepr::from_cursor_scope(scope)?,
        state_string: state_string.into(),
    }))
}

pub(crate) fn decode(raw: &OpaqueChangeState) -> Result<JmapCursorState, JmapCursorError> {
    if raw.protocol != ProtocolKind::Jmap {
        return Err(JmapCursorError::CursorProtocolMismatch);
    }
    // A FUTURE envelope is unknown - this build cannot read it and cannot
    // reason about what it means. An OLDER envelope is understood exactly
    // well enough to know it must not be resumed: its foreign thread ids
    // were minted bare (see `PAYLOAD_ENVELOPE_VERSION`), so it is reported as
    // `SchemaIncompatible` and the engine reseeds through inventory.
    if raw.envelope_version > PAYLOAD_ENVELOPE_VERSION {
        return Err(JmapCursorError::CursorEnvelopeUnknown);
    }
    if raw.envelope_version < PAYLOAD_ENVELOPE_VERSION {
        return Err(JmapCursorError::SchemaIncompatible);
    }

    decode_state(&raw.bytes)
}

fn encode_state(state: &JmapCursorState) -> Vec<u8> {
    let mut out = Vec::new();
    match state {
        JmapCursorState::V1 {
            scope,
            state_string,
        } => {
            out.push(STATE_TAG_V1);
            encode_scope(scope, &mut out);
            encode_string(state_string, &mut out);
        }
    }
    out
}

fn encode_scope(scope: &JmapScopeRepr, out: &mut Vec<u8>) {
    match scope {
        JmapScopeRepr::Email => out.push(SCOPE_TAG_EMAIL),
        JmapScopeRepr::Mailbox => out.push(SCOPE_TAG_MAILBOX),
        JmapScopeRepr::Thread => out.push(SCOPE_TAG_THREAD),
        JmapScopeRepr::Query(query) => {
            out.push(SCOPE_TAG_QUERY);
            encode_string(query, out);
        }
        JmapScopeRepr::Folder {
            account_id,
            mailbox_id,
        } => {
            out.push(SCOPE_TAG_FOLDER);
            encode_string(account_id, out);
            encode_string(mailbox_id, out);
        }
    }
}

fn encode_string(value: &str, out: &mut Vec<u8>) {
    let bytes = value.as_bytes();
    let len: u32 = bytes
        .len()
        .try_into()
        .expect("JMAP cursor string longer than 4 GiB");
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
}

fn decode_state(bytes: &[u8]) -> Result<JmapCursorState, JmapCursorError> {
    let mut input = bytes;
    let tag = read_u8(&mut input)?;
    let state = match tag {
        STATE_TAG_V1 => {
            let scope = decode_scope(&mut input)?;
            let state_string = decode_string(&mut input)?;
            JmapCursorState::V1 {
                scope,
                state_string,
            }
        }
        _ => return Err(JmapCursorError::SchemaIncompatible),
    };
    if !input.is_empty() {
        return Err(JmapCursorError::SchemaIncompatible);
    }
    Ok(state)
}

fn decode_scope(input: &mut &[u8]) -> Result<JmapScopeRepr, JmapCursorError> {
    let tag = read_u8(input)?;
    match tag {
        SCOPE_TAG_EMAIL => Ok(JmapScopeRepr::Email),
        SCOPE_TAG_MAILBOX => Ok(JmapScopeRepr::Mailbox),
        SCOPE_TAG_THREAD => Ok(JmapScopeRepr::Thread),
        SCOPE_TAG_QUERY => Ok(JmapScopeRepr::Query(decode_string(input)?)),
        SCOPE_TAG_FOLDER => {
            let account_id = decode_string(input)?;
            let mailbox_id = decode_string(input)?;
            Ok(JmapScopeRepr::Folder {
                account_id,
                mailbox_id,
            })
        }
        _ => Err(JmapCursorError::SchemaIncompatible),
    }
}

fn decode_string(input: &mut &[u8]) -> Result<String, JmapCursorError> {
    let len = read_u32_le(input)? as usize;
    if input.len() < len {
        return Err(JmapCursorError::SchemaIncompatible);
    }
    let (bytes, rest) = input.split_at(len);
    *input = rest;
    String::from_utf8(bytes.to_vec()).map_err(|_| JmapCursorError::SchemaIncompatible)
}

fn read_u8(input: &mut &[u8]) -> Result<u8, JmapCursorError> {
    let (first, rest) = input
        .split_first()
        .ok_or(JmapCursorError::SchemaIncompatible)?;
    *input = rest;
    Ok(*first)
}

fn read_u32_le(input: &mut &[u8]) -> Result<u32, JmapCursorError> {
    if input.len() < 4 {
        return Err(JmapCursorError::SchemaIncompatible);
    }
    let (head, rest) = input.split_at(4);
    *input = rest;
    let mut buf = [0u8; 4];
    buf.copy_from_slice(head);
    Ok(u32::from_le_bytes(buf))
}

pub(crate) fn cursor_for_scope(
    scope: CursorScope,
    state_string: impl Into<String>,
) -> Result<ChangeCursor, JmapCursorError> {
    let server_state = encode_for_scope(&scope, state_string)?;
    Ok(ChangeCursor {
        scope,
        server_state,
        advanced_through: None,
        envelope_version: OUTER_CURSOR_ENVELOPE_VERSION,
    })
}

pub(crate) fn decode_cursor(
    cursor: &ChangeCursor,
) -> Result<(JmapScopeRepr, String), JmapCursorError> {
    match decode(&cursor.server_state)? {
        JmapCursorState::V1 {
            scope,
            state_string,
        } => {
            if scope.to_cursor_scope() != cursor.scope {
                return Err(JmapCursorError::Other(
                    "JMAP cursor payload scope does not match ChangeCursor scope".to_string(),
                ));
            }
            Ok((scope, state_string))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_state_round_trips_through_opaque_bytes() {
        let state = JmapCursorState::V1 {
            scope: JmapScopeRepr::Email,
            state_string: "s123".to_string(),
        };

        let encoded = encode(&state);
        assert_eq!(encoded.protocol, ProtocolKind::Jmap);
        assert_eq!(encoded.envelope_version, PAYLOAD_ENVELOPE_VERSION);
        assert_eq!(decode(&encoded).unwrap(), state);
    }

    #[test]
    fn rejects_other_protocol_cursor() {
        let raw = OpaqueChangeState {
            protocol: ProtocolKind::Gmail,
            envelope_version: PAYLOAD_ENVELOPE_VERSION,
            bytes: Vec::new(),
        };

        assert!(matches!(
            decode(&raw),
            Err(JmapCursorError::CursorProtocolMismatch)
        ));
    }

    #[test]
    fn rejects_unknown_envelope_version() {
        let mut raw = encode(&JmapCursorState::V1 {
            scope: JmapScopeRepr::Mailbox,
            state_string: "s456".to_string(),
        });
        raw.envelope_version = PAYLOAD_ENVELOPE_VERSION + 1;

        assert!(matches!(
            decode(&raw),
            Err(JmapCursorError::CursorEnvelopeUnknown)
        ));
    }

    #[test]
    fn refuses_a_v1_cursor_as_schema_incompatible_rather_than_resuming_it() {
        // The v1 -> v2 bump exists because v1 minted foreign thread ids
        // BARE, and a bare foreign thread id is indistinguishable from a
        // primary one. Resuming a v1 cursor would skip the inventory pass
        // that re-mints them, so the payload must be refused as
        // `SchemaIncompatible` (which derives
        // `Engine(SchemaIncompatible)`), not as merely "unknown".
        let mut raw = encode(&JmapCursorState::V1 {
            scope: JmapScopeRepr::Folder {
                account_id: "acct-9".to_string(),
                mailbox_id: String::new(),
            },
            state_string: "s-old".to_string(),
        });
        raw.envelope_version = 1;

        assert!(matches!(
            decode(&raw),
            Err(JmapCursorError::SchemaIncompatible)
        ));
    }

    #[test]
    fn query_scope_round_trips() {
        let state = JmapCursorState::V1 {
            scope: JmapScopeRepr::Query("q-789".to_string()),
            state_string: "s-789".to_string(),
        };
        assert_eq!(decode(&encode(&state)).unwrap(), state);
    }

    #[test]
    fn foreign_folder_scope_round_trips_through_cursor() {
        // A foreign `Folder` scope survives encode_for_scope ->
        // decode_cursor and reconstructs the same account + mailbox.
        let scope = CursorScope::Folder(super::super::foreign::encode_foreign("acct-9", "mbx-3"));
        let cursor = cursor_for_scope(scope.clone(), "fstate").expect("foreign scope encodes");
        let (repr, state_string) = decode_cursor(&cursor).expect("foreign cursor decodes");
        assert_eq!(state_string, "fstate");
        assert_eq!(
            repr,
            JmapScopeRepr::Folder {
                account_id: "acct-9".to_string(),
                mailbox_id: "mbx-3".to_string(),
            }
        );
        // The decoded repr reconstructs the original scope (the
        // decode_cursor scope-equality guard holds).
        assert_eq!(repr.to_cursor_scope(), scope);
    }

    #[test]
    fn account_level_foreign_scope_round_trips_through_cursor() {
        // The seeded foreign shape: account-level, empty mailbox part.
        // It rides the same SCOPE_TAG_FOLDER envelope (an empty string
        // is a legal length-prefixed field), so no version bump.
        let scope = CursorScope::Folder(super::super::foreign::encode_foreign_account("acct-9"));
        let cursor = cursor_for_scope(scope.clone(), "astate").expect("account scope encodes");
        let (repr, state_string) = decode_cursor(&cursor).expect("account cursor decodes");
        assert_eq!(state_string, "astate");
        assert_eq!(
            repr,
            JmapScopeRepr::Folder {
                account_id: "acct-9".to_string(),
                mailbox_id: String::new(),
            }
        );
        assert_eq!(repr.to_cursor_scope(), scope);
    }

    #[test]
    fn folder_scope_byte_round_trips() {
        let state = JmapCursorState::V1 {
            scope: JmapScopeRepr::Folder {
                account_id: "acct-9".to_string(),
                mailbox_id: "mbx-3".to_string(),
            },
            state_string: "s".to_string(),
        };
        assert_eq!(decode(&encode(&state)).unwrap(), state);
    }

    #[test]
    fn rejects_unknown_state_tag() {
        let raw = OpaqueChangeState {
            protocol: ProtocolKind::Jmap,
            envelope_version: PAYLOAD_ENVELOPE_VERSION,
            bytes: vec![0xff],
        };
        assert!(matches!(
            decode(&raw),
            Err(JmapCursorError::SchemaIncompatible)
        ));
    }

    #[test]
    fn rejects_unknown_scope_tag() {
        let raw = OpaqueChangeState {
            protocol: ProtocolKind::Jmap,
            envelope_version: PAYLOAD_ENVELOPE_VERSION,
            bytes: vec![STATE_TAG_V1, 0xff, 0, 0, 0, 0],
        };
        assert!(matches!(
            decode(&raw),
            Err(JmapCursorError::SchemaIncompatible)
        ));
    }

    #[test]
    fn rejects_truncated_payload() {
        let mut bytes = encode(&JmapCursorState::V1 {
            scope: JmapScopeRepr::Email,
            state_string: "abc".to_string(),
        })
        .bytes;
        bytes.pop();
        let raw = OpaqueChangeState {
            protocol: ProtocolKind::Jmap,
            envelope_version: PAYLOAD_ENVELOPE_VERSION,
            bytes,
        };
        assert!(matches!(
            decode(&raw),
            Err(JmapCursorError::SchemaIncompatible)
        ));
    }

    #[test]
    fn rejects_trailing_garbage() {
        let mut bytes = encode(&JmapCursorState::V1 {
            scope: JmapScopeRepr::Email,
            state_string: "abc".to_string(),
        })
        .bytes;
        bytes.push(0);
        let raw = OpaqueChangeState {
            protocol: ProtocolKind::Jmap,
            envelope_version: PAYLOAD_ENVELOPE_VERSION,
            bytes,
        };
        assert!(matches!(
            decode(&raw),
            Err(JmapCursorError::SchemaIncompatible)
        ));
    }

    #[test]
    fn rejects_a_cursor_whose_payload_scope_disagrees_with_its_envelope() {
        // A cursor is a (scope, payload) pair and the scope is recorded in
        // both halves. If the engine hands back a `ChangeCursor` whose
        // outer scope was rewritten - a mis-keyed checkpoint store, a
        // scope-renaming migration - decoding must refuse rather than
        // silently sync the payload's scope against the envelope's.
        let email = cursor_for_scope(CursorScope::Type(ObjectType::Email), "s1")
            .expect("email scope encodes");
        let crossed = ChangeCursor {
            scope: CursorScope::Type(ObjectType::Mailbox),
            server_state: email.server_state,
            advanced_through: None,
            envelope_version: OUTER_CURSOR_ENVELOPE_VERSION,
        };
        assert!(matches!(
            decode_cursor(&crossed),
            Err(JmapCursorError::Other(_))
        ));
    }

    #[test]
    fn a_folder_scope_naming_a_different_mailbox_is_also_rejected() {
        // Same guard, but for the foreign shape where the mismatch is
        // inside the codec-encoded FolderId rather than the variant.
        let seeded = cursor_for_scope(
            CursorScope::Folder(super::super::foreign::encode_foreign("acct-9", "mbx-1")),
            "s1",
        )
        .expect("foreign scope encodes");
        let crossed = ChangeCursor {
            scope: CursorScope::Folder(super::super::foreign::encode_foreign("acct-9", "mbx-2")),
            server_state: seeded.server_state,
            advanced_through: None,
            envelope_version: OUTER_CURSOR_ENVELOPE_VERSION,
        };
        assert!(matches!(
            decode_cursor(&crossed),
            Err(JmapCursorError::Other(_))
        ));
    }

    #[test]
    fn an_unsupported_scope_cannot_be_encoded_at_all() {
        // A `Folder` id with no foreign separator is not a JMAP scope: it
        // has no account to route to, so encoding must fail rather than
        // mint a cursor that decodes to something else.
        assert!(matches!(
            JmapScopeRepr::from_cursor_scope(&CursorScope::Folder(bifrost_types::FolderId(
                "bare-mailbox".to_string()
            ))),
            Err(JmapCursorError::UnsupportedScope)
        ));
    }

    #[test]
    fn rejects_non_utf8_string() {
        let raw = OpaqueChangeState {
            protocol: ProtocolKind::Jmap,
            envelope_version: PAYLOAD_ENVELOPE_VERSION,
            bytes: vec![STATE_TAG_V1, SCOPE_TAG_EMAIL, 1, 0, 0, 0, 0xff],
        };
        assert!(matches!(
            decode(&raw),
            Err(JmapCursorError::SchemaIncompatible)
        ));
    }
}
