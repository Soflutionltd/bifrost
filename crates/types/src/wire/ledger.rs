//! Versioned ledger vocabulary, independent of engine-owned ledger state.
//!
//! Tags are append-only. Preserve their historical minimum versions on decode.
//! Adding a tag requires a vocabulary version bump and an enclosing sync ledger
//! version at least that high; sync asserts this relationship at compile time.

use super::cursor::{decode_scope, encode_scope};
use super::{DecodeError as Error, Reader, write_bytes, write_string};
use crate::{
    AccessErrorKind, AccountErrorKind, AccountOperation, AuthErrorKind, CoverageCoordinate,
    CoverageDomain, DetailVisibility, DiagnosticInfo, DiagnosticText, ErrorScope,
    InternalErrorKind, InventoryRepairTarget, MailboxUnavailableKind, ObjectId, Protocol,
    ProtocolErrorKind, Provider, RequestErrorKind, ResourceKind, ServerErrorKind, SnapshotIdentity,
    SyncStateErrorKind, TelemetryToken, TransportErrorKind,
};

/// Latest ledger version that introduced a shared vocabulary tag.
/// Advance beyond the current sync ledger version when adding new tags.
pub const VOCABULARY_VERSION: u32 = 13;

/// Append a coverage domain, including its scope and snapshot identity.
pub fn encode_domain(out: &mut Vec<u8>, domain: &CoverageDomain) {
    // The scope goes in length-prefixed because `decode_scope` reads a
    // self-delimiting payload rather than reporting how much it consumed.
    write_bytes(out, &encode_scope(&domain.scope));
    match &domain.coordinate {
        CoverageCoordinate::Full => out.push(0),
        CoverageCoordinate::TimeRange(window) => {
            out.push(1);
            write_opt_i64(out, window.from());
            write_opt_i64(out, window.to());
        }
        CoverageCoordinate::UidRange(window) => {
            out.push(2);
            out.extend_from_slice(&window.uid_validity().to_le_bytes());
            out.extend_from_slice(&window.from().to_le_bytes());
            out.extend_from_slice(&window.to().to_le_bytes());
        }
        CoverageCoordinate::PageRange(window) => {
            out.push(3);
            out.extend_from_slice(&window.from().to_le_bytes());
            out.extend_from_slice(&window.to().to_le_bytes());
        }
        CoverageCoordinate::ProviderRegion { namespace, region } => {
            out.push(4);
            write_string(out, namespace);
            write_bytes(out, region);
        }
    }
    match &domain.snapshot.0 {
        None => out.push(0),
        Some(snapshot) => {
            out.push(1);
            write_bytes(out, snapshot);
        }
    }
}

/// Read and validate a coverage domain.
pub fn decode_domain(reader: &mut Reader<'_>) -> Result<CoverageDomain, Error> {
    let scope = decode_scope(&reader.bytes()?)?;
    let coordinate = match reader.u8()? {
        0 => CoverageCoordinate::Full,
        1 => CoverageCoordinate::TimeRange(
            crate::TimeWindow::new(read_opt_i64(reader)?, read_opt_i64(reader)?).ok_or_else(
                || Error::Malformed("ledger envelope: reversed or out-of-range window".into()),
            )?,
        ),
        2 => CoverageCoordinate::UidRange(
            crate::UidWindow::new(reader.u32()?, reader.u64()?, reader.u64()?).ok_or_else(
                || Error::Malformed("ledger envelope: reversed or out-of-range window".into()),
            )?,
        ),
        3 => CoverageCoordinate::PageRange(
            crate::PageWindow::new(reader.u32()?, reader.u32()?).ok_or_else(|| {
                Error::Malformed("ledger envelope: reversed or out-of-range window".into())
            })?,
        ),
        4 => CoverageCoordinate::ProviderRegion {
            namespace: reader.string()?,
            region: reader.bytes()?,
        },
        other => {
            return Err(Error::Malformed(format!(
                "ledger envelope: unknown coverage coordinate tag {other}"
            )));
        }
    };
    let snapshot = match reader.u8()? {
        0 => SnapshotIdentity(None),
        1 => SnapshotIdentity(Some(reader.bytes()?)),
        other => {
            return Err(Error::Malformed(format!(
                "ledger envelope: unknown snapshot tag {other}"
            )));
        }
    };
    Ok(CoverageDomain {
        scope,
        coordinate,
        snapshot,
    })
}

/// Append an optional retained repair target.
pub fn encode_repair_target(out: &mut Vec<u8>, target: Option<&InventoryRepairTarget>) {
    match target {
        None => out.push(0),
        Some(crate::InventoryRepairTarget::Object { id, repair }) => {
            out.push(1);
            write_string(out, &id.0);
            write_bytes(out, repair);
        }
        Some(crate::InventoryRepairTarget::Region { replay }) => {
            out.push(2);
            write_bytes(out, replay);
        }
    }
}

/// Read an optional retained repair target.
pub fn decode_repair_target(
    reader: &mut Reader<'_>,
) -> Result<Option<InventoryRepairTarget>, Error> {
    Ok(match reader.u8()? {
        0 => None,
        1 => Some(crate::InventoryRepairTarget::Object {
            id: ObjectId(reader.string()?),
            repair: reader.bytes()?,
        }),
        2 => Some(crate::InventoryRepairTarget::Region {
            replay: reader.bytes()?,
        }),
        other => {
            return Err(Error::Malformed(format!(
                "ledger envelope: unknown repair target tag {other}"
            )));
        }
    })
}
/// Append full historical diagnostics, including support-only text.
pub fn encode_diagnostics(out: &mut Vec<u8>, diagnostics: &DiagnosticInfo) {
    write_opt_u16(out, diagnostics.status);
    write_opt_str(
        out,
        diagnostics.request_id.as_ref().map(TelemetryToken::as_str),
    );
    write_opt_str(
        out,
        diagnostics.trace_id.as_ref().map(TelemetryToken::as_str),
    );
    write_opt_str(
        out,
        diagnostics.native_code.as_ref().map(TelemetryToken::as_str),
    );
    write_count(out, diagnostics.text.len(), "diagnostic text");
    for text in &diagnostics.text {
        out.push(match text.visibility {
            DetailVisibility::UserSafe => 0,
            DetailVisibility::SupportOnly => 1,
        });
        write_string(out, text.as_str());
    }
}
/// Read historical diagnostics, rejecting malformed telemetry tokens.
pub fn decode_diagnostics(reader: &mut Reader<'_>) -> Result<DiagnosticInfo, Error> {
    let status = read_opt_u16(reader)?;
    let request_id = read_opt_string(reader)?;
    let trace_id = read_opt_string(reader)?;
    let native_code = read_opt_string(reader)?;
    let text_count = reader.u32()? as usize;
    let mut text = Vec::with_capacity(text_count.min(1024));
    for _ in 0..text_count {
        let visibility = match reader.u8()? {
            0 => DetailVisibility::UserSafe,
            1 => DetailVisibility::SupportOnly,
            other => {
                return Err(Error::Malformed(format!(
                    "ledger envelope: unknown diagnostic visibility tag {other}"
                )));
            }
        };
        text.push((visibility, reader.string()?));
    }

    let mut diagnostics = DiagnosticInfo {
        status,
        ..DiagnosticInfo::default()
    };
    let token = |value: Option<String>| -> Result<Option<TelemetryToken>, Error> {
        value
            .map(|value| {
                TelemetryToken::new(value).ok_or_else(|| {
                    Error::Malformed("ledger envelope: invalid diagnostic token".into())
                })
            })
            .transpose()
    };
    diagnostics.request_id = token(request_id)?;
    diagnostics.trace_id = token(trace_id)?;
    diagnostics.native_code = token(native_code)?;
    diagnostics.text = text
        .into_iter()
        .map(|(visibility, value)| match visibility {
            DetailVisibility::UserSafe => DiagnosticText::user_safe(value),
            DetailVisibility::SupportOnly => DiagnosticText::support_only(value),
        })
        .collect();
    Ok(diagnostics)
}

/// Append an error classification using stable tags.
pub fn encode_error_kind(out: &mut Vec<u8>, kind: &AccountErrorKind) {
    match kind {
        AccountErrorKind::Transport(kind) => {
            out.push(0);
            out.push(match kind {
                TransportErrorKind::Network => 0,
                TransportErrorKind::Timeout => 1,
                TransportErrorKind::Tls => 2,
            });
        }
        AccountErrorKind::Authentication(kind) => {
            out.push(1);
            out.push(match kind {
                AuthErrorKind::Expired => 0,
                AuthErrorKind::RefreshTransient => 1,
                AuthErrorKind::Revoked => 2,
                AuthErrorKind::ReauthorizationRequired => 3,
            });
        }
        AccountErrorKind::Authorization(kind) => {
            out.push(2);
            match kind {
                AccessErrorKind::AdminConsentRequired => out.push(0),
                AccessErrorKind::ConditionalAccessBlocked => out.push(1),
                AccessErrorKind::PolicyBlocked => out.push(2),
                AccessErrorKind::InsufficientScope => out.push(3),
                AccessErrorKind::PermissionDenied => out.push(4),
                AccessErrorKind::AccountDisabled => out.push(5),
                AccessErrorKind::MailboxUnavailable { kind } => {
                    out.push(6);
                    out.push(match kind {
                        MailboxUnavailableKind::Transient => 0,
                        MailboxUnavailableKind::Permanent => 1,
                    });
                }
                AccessErrorKind::MailboxNotLicensed => out.push(7),
            }
        }
        AccountErrorKind::Server(kind) => {
            out.push(3);
            match kind {
                ServerErrorKind::Unavailable => out.push(0),
                ServerErrorKind::RateLimited => out.push(1),
                ServerErrorKind::QuotaExhausted => out.push(2),
                ServerErrorKind::Error { status } => {
                    out.push(3);
                    write_opt_u16_value(out, *status);
                }
            }
        }
        AccountErrorKind::SyncState(kind) => {
            out.push(4);
            out.push(match kind {
                SyncStateErrorKind::CursorInvalid => 0,
                SyncStateErrorKind::StrategyFailure => 1,
                SyncStateErrorKind::ScopeCapabilityLost => 2,
                SyncStateErrorKind::SchemaIncompatible => 3,
                SyncStateErrorKind::CapabilityChanged => 4,
                SyncStateErrorKind::OperatorOverrideNeeded => 5,
                SyncStateErrorKind::ScopeRevoked => 6,
            });
        }
        AccountErrorKind::ConcurrencyConflict => out.push(5),
        AccountErrorKind::Request(kind) => {
            out.push(6);
            out.push(match kind {
                RequestErrorKind::Malformed => 0,
                RequestErrorKind::BatchInputInvalid => 1,
            });
        }
        AccountErrorKind::NotFound(resource) => {
            out.push(7);
            out.push(encode_resource(*resource));
        }
        AccountErrorKind::Unsupported(operation) => {
            out.push(8);
            out.push(encode_operation(*operation));
        }
        AccountErrorKind::Protocol(kind) => {
            out.push(9);
            out.push(match kind {
                ProtocolErrorKind::ParseFailed => 0,
                ProtocolErrorKind::MissingField => 1,
                ProtocolErrorKind::ContractViolation => 2,
                ProtocolErrorKind::PartialResponse => 3,
                ProtocolErrorKind::Unknown => 4,
            });
        }
        AccountErrorKind::Internal(kind) => {
            out.push(10);
            out.push(match kind {
                InternalErrorKind::InvariantViolated => 0,
                InternalErrorKind::RuntimeFailure => 1,
                InternalErrorKind::Panicked => 2,
                InternalErrorKind::AccountContract => 3,
                InternalErrorKind::LimitExceeded => 4,
            });
        }
        AccountErrorKind::ControlWaitEnded => out.push(11),
        AccountErrorKind::AccountClosed => out.push(12),
    }
}

/// Read an error classification under the enclosing ledger version.
pub fn decode_error_kind(reader: &mut Reader<'_>) -> Result<AccountErrorKind, Error> {
    let kind = match reader.u8()? {
        0 => AccountErrorKind::Transport(match reader.u8()? {
            0 => TransportErrorKind::Network,
            1 => TransportErrorKind::Timeout,
            2 => TransportErrorKind::Tls,
            other => return Err(unknown_tag("transport error kind", other)),
        }),
        1 => AccountErrorKind::Authentication(match reader.u8()? {
            0 => AuthErrorKind::Expired,
            1 => AuthErrorKind::RefreshTransient,
            2 => AuthErrorKind::Revoked,
            3 => AuthErrorKind::ReauthorizationRequired,
            other => return Err(unknown_tag("auth error kind", other)),
        }),
        2 => AccountErrorKind::Authorization(match reader.u8()? {
            0 => AccessErrorKind::AdminConsentRequired,
            1 => AccessErrorKind::ConditionalAccessBlocked,
            2 => AccessErrorKind::PolicyBlocked,
            3 => AccessErrorKind::InsufficientScope,
            4 => AccessErrorKind::PermissionDenied,
            5 => AccessErrorKind::AccountDisabled,
            6 => AccessErrorKind::MailboxUnavailable {
                kind: match reader.u8()? {
                    0 => MailboxUnavailableKind::Transient,
                    1 => MailboxUnavailableKind::Permanent,
                    other => return Err(unknown_tag("mailbox unavailable kind", other)),
                },
            },
            7 => AccessErrorKind::MailboxNotLicensed,
            other => return Err(unknown_tag("access error kind", other)),
        }),
        3 => AccountErrorKind::Server(match reader.u8()? {
            0 => ServerErrorKind::Unavailable,
            1 => ServerErrorKind::RateLimited,
            2 => ServerErrorKind::QuotaExhausted,
            3 => ServerErrorKind::Error {
                status: read_opt_u16(reader)?,
            },
            other => return Err(unknown_tag("server error kind", other)),
        }),
        4 => AccountErrorKind::SyncState(match reader.u8()? {
            0 => SyncStateErrorKind::CursorInvalid,
            1 => SyncStateErrorKind::StrategyFailure,
            2 => SyncStateErrorKind::ScopeCapabilityLost,
            3 => SyncStateErrorKind::SchemaIncompatible,
            4 => SyncStateErrorKind::CapabilityChanged,
            5 => SyncStateErrorKind::OperatorOverrideNeeded,
            6 => SyncStateErrorKind::ScopeRevoked,
            other => return Err(unknown_tag("sync state error kind", other)),
        }),
        5 => AccountErrorKind::ConcurrencyConflict,
        6 => AccountErrorKind::Request(match reader.u8()? {
            0 => RequestErrorKind::Malformed,
            1 => RequestErrorKind::BatchInputInvalid,
            other => return Err(unknown_tag("request error kind", other)),
        }),
        7 => AccountErrorKind::NotFound(decode_resource(reader.u8()?, reader.version)?),
        8 => AccountErrorKind::Unsupported(decode_operation(reader.u8()?)?),
        9 => AccountErrorKind::Protocol(match reader.u8()? {
            0 => ProtocolErrorKind::ParseFailed,
            1 => ProtocolErrorKind::MissingField,
            2 => ProtocolErrorKind::ContractViolation,
            3 => ProtocolErrorKind::PartialResponse,
            4 => ProtocolErrorKind::Unknown,
            other => return Err(unknown_tag("protocol error kind", other)),
        }),
        // Introduced in version 4; a version-3 writer never produced it.
        10 if reader.version >= 4 => AccountErrorKind::Internal(match reader.u8()? {
            0 => InternalErrorKind::InvariantViolated,
            1 => InternalErrorKind::RuntimeFailure,
            2 => InternalErrorKind::Panicked,
            3 => InternalErrorKind::AccountContract,
            4 => InternalErrorKind::LimitExceeded,
            other => return Err(unknown_tag("internal error kind", other)),
        }),
        // Introduced in version 8; no older writer produced it.
        11 if reader.version >= 8 => AccountErrorKind::ControlWaitEnded,
        12 if reader.version >= 10 => AccountErrorKind::AccountClosed,
        other => return Err(unknown_tag("account error kind", other)),
    };
    Ok(kind)
}

/// Append an error scope using stable tags.
pub fn encode_error_scope(out: &mut Vec<u8>, scope: &ErrorScope) {
    match scope {
        ErrorScope::Submission { id } => {
            out.push(11);
            write_string(out, &id.0);
        }
        ErrorScope::CloudFile { id } => {
            out.push(12);
            write_string(out, id);
        }
        ErrorScope::UploadSession => out.push(13),
        ErrorScope::Account => out.push(0),
        ErrorScope::Namespace { prefix } => {
            out.push(14);
            write_string(out, prefix);
        }
        ErrorScope::Cursor(cursor) => {
            out.push(1);
            write_bytes(out, &encode_scope(cursor));
        }
        ErrorScope::Owner { id } => {
            out.push(15);
            write_string(out, &id.0);
        }
        ErrorScope::Mailbox { id } => {
            out.push(2);
            write_string(out, &id.0);
        }
        ErrorScope::Message { id } => {
            out.push(3);
            write_string(out, &id.0);
        }
        ErrorScope::Thread { id } => {
            out.push(4);
            write_string(out, &id.0);
        }
        ErrorScope::Calendar { id } => {
            out.push(5);
            write_string(out, &id.0);
        }
        ErrorScope::CalendarCollection => out.push(6),
        ErrorScope::Contact { id } => {
            out.push(7);
            write_string(out, &id.0);
        }
        ErrorScope::ContactCollection => out.push(8),
        ErrorScope::DirectoryGateway { url } => {
            out.push(10);
            write_string(out, url);
        }
        ErrorScope::Event { id } => {
            out.push(9);
            write_string(out, &id.0);
        }
    }
}

/// Read an error scope under the enclosing ledger version.
pub fn decode_error_scope(reader: &mut Reader<'_>) -> Result<ErrorScope, Error> {
    Ok(match reader.u8()? {
        0 => ErrorScope::Account,
        1 => ErrorScope::Cursor(decode_scope(&reader.bytes()?)?),
        2 => ErrorScope::Mailbox {
            id: crate::MailboxId(reader.string()?),
        },
        3 => ErrorScope::Message {
            id: ObjectId(reader.string()?),
        },
        4 => ErrorScope::Thread {
            id: crate::ThreadId(reader.string()?),
        },
        5 => ErrorScope::Calendar {
            id: crate::CalendarId(reader.string()?),
        },
        6 => ErrorScope::CalendarCollection,
        7 => ErrorScope::Contact {
            id: crate::ContactId(reader.string()?),
        },
        8 => ErrorScope::ContactCollection,
        9 if reader.version >= 5 => ErrorScope::Event {
            id: crate::EventId(reader.string()?),
        },
        10 if reader.version >= 6 => ErrorScope::DirectoryGateway {
            url: reader.string()?,
        },
        11 if reader.version >= 7 => ErrorScope::Submission {
            id: ObjectId(reader.string()?),
        },
        12 if reader.version >= 7 => ErrorScope::CloudFile {
            id: reader.string()?,
        },
        13 if reader.version >= 7 => ErrorScope::UploadSession,
        14 if reader.version >= 11 => ErrorScope::Namespace {
            prefix: reader.string()?,
        },
        15 if reader.version >= 13 => ErrorScope::Owner {
            id: crate::PrincipalId(reader.string()?),
        },
        other => return Err(unknown_tag("error scope", other)),
    })
}

/// Stable provider tag in the ledger vocabulary.
#[must_use]
pub fn encode_provider(provider: Provider) -> u8 {
    match provider {
        Provider::Fastmail => 0,
        Provider::Gmail => 1,
        Provider::Microsoft => 2,
        Provider::Icloud => 3,
        Provider::Yahoo => 4,
        Provider::Custom => 5,
        Provider::Google => 6,
    }
}

/// Decode a provider tag valid for the given ledger version.
pub fn decode_provider(tag: u8, version: u32) -> Result<Provider, Error> {
    Ok(match tag {
        0 => Provider::Fastmail,
        1 => Provider::Gmail,
        2 => Provider::Microsoft,
        3 => Provider::Icloud,
        4 => Provider::Yahoo,
        5 => Provider::Custom,
        6 if version >= 9 => Provider::Google,
        other => return Err(unknown_tag("provider", other)),
    })
}

/// Stable protocol tag in the ledger vocabulary.
#[must_use]
pub fn encode_protocol(protocol: Protocol) -> u8 {
    match protocol {
        Protocol::Jmap => 0,
        Protocol::Imap => 1,
        Protocol::CardDav => 2,
        Protocol::Smtp => 3,
        Protocol::Lmtp => 4,
        Protocol::Gmail => 5,
        Protocol::Graph => 6,
        Protocol::Ews => 7,
        Protocol::CalDav => 8,
        Protocol::Unknown => 9,
        Protocol::GoogleCalendar => 10,
        Protocol::GooglePeople => 11,
        Protocol::GoogleDrive => 12,
    }
}

/// Decode a protocol tag valid for the given ledger version.
pub fn decode_protocol(tag: u8, version: u32) -> Result<Protocol, Error> {
    Ok(match tag {
        0 => Protocol::Jmap,
        1 => Protocol::Imap,
        2 => Protocol::CardDav,
        3 => Protocol::Smtp,
        4 => Protocol::Lmtp,
        5 => Protocol::Gmail,
        6 => Protocol::Graph,
        7 => Protocol::Ews,
        8 => Protocol::CalDav,
        9 => Protocol::Unknown,
        10 if version >= 9 => Protocol::GoogleCalendar,
        11 if version >= 9 => Protocol::GooglePeople,
        12 if version >= 9 => Protocol::GoogleDrive,
        other => return Err(unknown_tag("protocol", other)),
    })
}

/// Stable resource tag in the ledger vocabulary.
#[must_use]
pub fn encode_resource(resource: ResourceKind) -> u8 {
    match resource {
        ResourceKind::Message => 0,
        ResourceKind::Mailbox => 1,
        ResourceKind::Thread => 2,
        ResourceKind::Calendar => 3,
        ResourceKind::Contact => 4,
        ResourceKind::Draft => 5,
        ResourceKind::Identity => 6,
        ResourceKind::Vacation => 7,
        ResourceKind::PushSubscription => 8,
        ResourceKind::Account => 9,
        ResourceKind::Filter => 10,
        ResourceKind::Event => 11,
    }
}

/// Decode a resource tag valid for the given ledger version.
pub fn decode_resource(tag: u8, version: u32) -> Result<ResourceKind, Error> {
    Ok(match tag {
        0 => ResourceKind::Message,
        1 => ResourceKind::Mailbox,
        2 => ResourceKind::Thread,
        3 => ResourceKind::Calendar,
        4 => ResourceKind::Contact,
        5 => ResourceKind::Draft,
        6 => ResourceKind::Identity,
        7 => ResourceKind::Vacation,
        8 => ResourceKind::PushSubscription,
        9 => ResourceKind::Account,
        10 => ResourceKind::Filter,
        11 if version >= 5 => ResourceKind::Event,
        other => return Err(unknown_tag("resource kind", other)),
    })
}

/// Historical decode table for `AccountOperation`, in wire-tag order.
///
/// Encoding is an exhaustive types-owned match. The round-trip test pins this
/// decode table against those tags, preserving old rows when variants grow.
const OPERATIONS: &[AccountOperation] = &[
    AccountOperation::CategoryDefinitionsList,
    AccountOperation::MessageReactionsRead,
    AccountOperation::Discover,
    AccountOperation::EstablishCursor,
    AccountOperation::DiscoverCursorScopes,
    AccountOperation::DiscoverMemberships,
    AccountOperation::ScopeLifecycle,
    AccountOperation::SyncInventory,
    AccountOperation::SyncChanges,
    AccountOperation::Hydrate,
    AccountOperation::HydrateThread,
    AccountOperation::HydrateMessage,
    AccountOperation::OpenBlob,
    AccountOperation::OpenBlobRange,
    AccountOperation::OpenRawRfc822,
    AccountOperation::PushSubscribe,
    AccountOperation::PushUnsubscribe,
    AccountOperation::PushStream,
    AccountOperation::UpdateFlags,
    AccountOperation::SetStarred,
    AccountOperation::MarkReplied,
    AccountOperation::MarkForwarded,
    AccountOperation::MarkMdnSent,
    AccountOperation::BulkMove,
    AccountOperation::BulkDestroy,
    AccountOperation::MoveThread,
    AccountOperation::DeleteThread,
    AccountOperation::AddToContainer,
    AccountOperation::RemoveFromContainer,
    AccountOperation::SetKeyword,
    AccountOperation::SetLabelMembership,
    AccountOperation::SetCategory,
    AccountOperation::SetExtendedProperty,
    AccountOperation::SetImportance,
    AccountOperation::SetIsRead,
    AccountOperation::Send,
    AccountOperation::AttachmentUpload,
    AccountOperation::HostAttachment,
    AccountOperation::DraftCreate,
    AccountOperation::DraftUpdate,
    AccountOperation::DraftDiscard,
    AccountOperation::DraftSend,
    AccountOperation::CancelScheduledSend,
    AccountOperation::RescheduleSend,
    AccountOperation::Search,
    AccountOperation::SearchMessages,
    AccountOperation::ContainersList,
    AccountOperation::ContainerCreate,
    AccountOperation::ContainerRename,
    AccountOperation::ContainerMove,
    AccountOperation::ContainerDelete,
    AccountOperation::IdentitiesList,
    AccountOperation::IdentityUpdate,
    AccountOperation::VacationGet,
    AccountOperation::VacationSet,
    AccountOperation::QuotaGet,
    AccountOperation::FiltersList,
    AccountOperation::FilterCreate,
    AccountOperation::FilterUpdate,
    AccountOperation::FilterDelete,
    AccountOperation::FilterValidate,
    AccountOperation::AddressBooksList,
    AccountOperation::ContactsList,
    AccountOperation::ContactGet,
    AccountOperation::ContactCreate,
    AccountOperation::ContactUpdate,
    AccountOperation::ContactDelete,
    AccountOperation::ContactSearch,
    AccountOperation::ContactAutocomplete,
    AccountOperation::DirectorySearch,
    AccountOperation::DirectoryGroupsList,
    AccountOperation::DirectoryGroupExpand,
    AccountOperation::CalendarsList,
    AccountOperation::EventsInRange,
    AccountOperation::EventGet,
    AccountOperation::EventCreate,
    AccountOperation::EventUpdate,
    AccountOperation::EventDelete,
    AccountOperation::EventRsvp,
    AccountOperation::EventSearch,
    AccountOperation::EventAutocomplete,
    AccountOperation::Close,
    AccountOperation::Expunge,
];

/// Stable operation tag in the ledger vocabulary.
#[must_use]
pub fn encode_operation(operation: AccountOperation) -> u8 {
    operation.ledger_tag()
}

/// Decode an operation tag in the ledger vocabulary.
pub fn decode_operation(tag: u8) -> Result<AccountOperation, Error> {
    OPERATIONS
        .get(tag as usize)
        .copied()
        .ok_or_else(|| unknown_tag("account operation", tag))
}

fn unknown_tag(what: &str, tag: u8) -> Error {
    Error::Malformed(format!("ledger envelope: unknown {what} tag {tag}"))
}

fn write_count(out: &mut Vec<u8>, count: usize, what: &str) {
    let count = u32::try_from(count)
        .unwrap_or_else(|_| panic!("ledger envelope: {what} exceed the u32 count limit"));
    out.extend_from_slice(&count.to_le_bytes());
}

fn write_opt_i64(out: &mut Vec<u8>, value: Option<i64>) {
    match value {
        None => out.push(0),
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_le_bytes());
        }
    }
}

fn read_opt_i64(reader: &mut Reader<'_>) -> Result<Option<i64>, Error> {
    match reader.u8()? {
        0 => Ok(None),
        1 => Ok(Some(reader.i64()?)),
        other => Err(unknown_tag("optional i64", other)),
    }
}

fn write_opt_u16(out: &mut Vec<u8>, value: Option<u16>) {
    write_opt_u16_value(out, value);
}

fn write_opt_u16_value(out: &mut Vec<u8>, value: Option<u16>) {
    match value {
        None => out.push(0),
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_le_bytes());
        }
    }
}

fn read_opt_u16(reader: &mut Reader<'_>) -> Result<Option<u16>, Error> {
    match reader.u8()? {
        0 => Ok(None),
        1 => Ok(Some(reader.u16()?)),
        other => Err(unknown_tag("optional u16", other)),
    }
}

fn write_opt_str(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        None => out.push(0),
        Some(value) => {
            out.push(1);
            write_string(out, value);
        }
    }
}

fn read_opt_string(reader: &mut Reader<'_>) -> Result<Option<String>, Error> {
    match reader.u8()? {
        0 => Ok(None),
        1 => Ok(Some(reader.string()?)),
        other => Err(unknown_tag("optional string", other)),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn owner_scope_wire_identity_is_distinct_and_versioned() {
        let owner = crate::ErrorScope::Owner {
            id: crate::PrincipalId("a".into()),
        };
        let mailbox = crate::ErrorScope::Mailbox {
            id: crate::MailboxId("a".into()),
        };
        let mut bytes = Vec::new();
        super::encode_error_scope(&mut bytes, &owner);
        assert_eq!(
            super::decode_error_scope(&mut super::Reader::new(&bytes, 13)).unwrap(),
            owner
        );
        assert!(super::decode_error_scope(&mut super::Reader::new(&bytes, 12)).is_err());
        let mut native = Vec::new();
        super::encode_error_scope(&mut native, &mailbox);
        assert_ne!(bytes, native);
        assert_eq!(
            super::decode_error_scope(&mut super::Reader::new(&native, 12)).unwrap(),
            mailbox
        );
        assert_eq!(serde_json::to_value(&owner).unwrap()["kind"], "owner");
        assert_eq!(owner.resource(), None);
    }

    /// Every operation tag must survive the table round trip. A transposed pair
    /// in the table would relabel an operation in a durable record without ever
    /// failing to compile.
    #[test]
    fn every_account_operation_tag_round_trips() {
        for (index, operation) in super::OPERATIONS.iter().enumerate() {
            let tag = super::encode_operation(*operation);
            assert_eq!(usize::from(tag), index, "table order defines the tag");
            assert_eq!(
                super::decode_operation(tag).expect("known tag"),
                *operation,
                "the two directions must agree"
            );
        }
        let past_end = u8::try_from(super::OPERATIONS.len()).expect("table fits a u8 tag");
        assert!(super::decode_operation(past_end).is_err());
    }
}
