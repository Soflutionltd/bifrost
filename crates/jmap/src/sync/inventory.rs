use std::time::Instant;

use bifrost_types::{
    AccountStream, Batch, BlobId, CursorScope, Fingerprint, InventoryEntry, InventoryPartition,
    MailboxId as TypesMailboxId, MembershipScope, ObjectId, ObjectType, PageBoundary,
    ServerVersion, SyncEvent, ThreadId,
};

use crate::core::query;
use crate::core::transport::HttpTransport;
use crate::email::{Email, EmailGet, EmailQuery, Property as EmailProperty};
use crate::mailbox::{Mailbox, MailboxGet, MailboxQuery, Property as MailboxProperty};

use super::capabilities::CoreLimits;
use super::state_walk::StateWalk;

type MailAccount<T> = crate::account::Account<T>;

pub(crate) fn stream<T: HttpTransport>(
    mail: MailAccount<T>,
    limits: CoreLimits,
    scope: CursorScope,
    owner: Option<TypesMailboxId>,
) -> AccountStream<SyncEvent<InventoryEntry>> {
    match scope {
        CursorScope::Type(ObjectType::Email) => email_inventory(mail, limits),
        CursorScope::Type(ObjectType::Mailbox) => {
            mailbox_inventory(mail, limits.max_objects_in_get.max(1))
        }
        // A foreign (shared/delegate) account scope: page its emails via
        // `Email/query` against the foreign account, hydrating inventory
        // entries there. The seeded shape is the ACCOUNT-LEVEL scope
        // (empty mailbox part), which walks the whole account unfiltered
        // - one walk per share, mirroring the primary `Type(Email)`
        // inventory. A legacy per-mailbox scope still decodes and takes
        // the `inMailbox`-filtered walk.
        CursorScope::Folder(ref folder) => match super::foreign::parse_foreign(folder) {
            Some(parsed) => {
                foreign_email_inventory(mail, limits, scope.clone(), parsed.mailbox_id, owner)
            }
            None => Box::pin(async_stream::stream! {
                yield super::error::terminated_unsupported(
                    bifrost_types::AccountOperation::SyncInventory,
                    Some(bifrost_types::ErrorScope::Cursor(scope.clone())),
                    "JMAP folder scope is not a foreign-mailbox scope",
                );
            }),
        },
        CursorScope::Type(ObjectType::Thread) => Box::pin(async_stream::stream! {
                yield super::error::terminated_unsupported(
                    bifrost_types::AccountOperation::SyncInventory,
                    Some(bifrost_types::ErrorScope::Cursor(CursorScope::Type(ObjectType::Thread))),
                    "JMAP thread inventory is derived from Email inventory in this implementation",
                );
        }),
        CursorScope::Query(_) => Box::pin(async_stream::stream! {
                yield super::error::terminated_unsupported(
                    bifrost_types::AccountOperation::SyncInventory,
                    None,
                    "JMAP query inventory requires registered query definitions outside the v1 Account trait",
                );
        }),
        _ => Box::pin(async_stream::stream! {
                yield super::error::terminated_unsupported(
                    bifrost_types::AccountOperation::SyncInventory,
                    None,
                    "cursor scope is not supported by JMAP",
                );
        }),
    }
}

pub(crate) fn stream_partition<T: HttpTransport>(
    mail: MailAccount<T>,
    limits: CoreLimits,
    scope: CursorScope,
    partition: InventoryPartition,
    owner: Option<TypesMailboxId>,
) -> AccountStream<SyncEvent<InventoryEntry>> {
    match partition {
        InventoryPartition::Full => stream(mail, limits, scope, owner),
        _ => Box::pin(async_stream::stream! {
            yield super::error::terminated_unsupported(
                bifrost_types::AccountOperation::SyncInventory,
                None,
                "JMAP inventory partition is not supported for this cursor scope",
            );
        }),
    }
}

fn foreign_email_inventory<T: HttpTransport>(
    mail: MailAccount<T>,
    limits: CoreLimits,
    scope: CursorScope,
    mailbox_id: String,
    owner: Option<TypesMailboxId>,
) -> AccountStream<SyncEvent<InventoryEntry>> {
    let filter =
        (!mailbox_id.is_empty()).then(|| crate::email::query::Filter::in_mailbox(mailbox_id));
    email_inventory_loop(mail, scope, filter, owner, limits.max_objects_in_get.max(1))
}

/// The paged `Email/query` + `Email/get` inventory walk, shared by the
/// primary `Type(Email)` scope and every foreign (shared/delegate)
/// account scope. The only thing that varies is `owner`: `Some(accountId)`
/// for a foreign scope, which both qualifies the emitted ids and
/// memberships into the owning account's namespace and routes a permission
/// denial into a per-scope quarantine; `None` for the primary walk, where
/// `shared_scope_error` reduces to the plain JMAP error mapping.
fn email_inventory_loop<T: HttpTransport>(
    mail: MailAccount<T>,
    scope: CursorScope,
    filter: Option<crate::email::query::Filter>,
    owner: Option<TypesMailboxId>,
    full_limit: usize,
) -> AccountStream<SyncEvent<InventoryEntry>> {
    Box::pin(async_stream::stream! {
        // One accumulator for the paged walk. Each page is a `Query`
        // POST followed by a `Get` POST, and each emitted batch takes
        // and clears the accumulator, so a batch's `bytes_in` covers
        // both requests of its page.
        let (mail, tally) = mail.metered();

        let mut walk = StateWalk::inventory(scope.clone());
        loop {
            let started = Instant::now();
            let mut query = EmailQuery::new();
            if let Some(filter) = filter.clone() {
                query = query.filter(filter);
            }
            query = query
                .sort([query::Comparator::new(crate::email::query::Comparator::ReceivedAt).descending()])
                .limit(full_limit);
            // Anchoring on the previous page's last id resolves the next
            // page's start server-side against the CURRENT result set, so a
            // message deleted behind the cursor cannot shift an unread
            // message into a position the walk already passed. Integer
            // position over `receivedAt desc` would skip ids after such a
            // deletion.
            if let Some(anchor) = walk.inventory_anchor() {
                query = query.anchor(anchor.to_string()).anchor_offset(1);
            } else {
                query = query.position(0);
            }
            let query_response = mail.call(query).await;

            let query_response = match query_response {
                Ok(response) => response,
                Err(err) => {
                    yield super::error::terminated(super::error::shared_scope_error(
                        err,
                        &scope,
                        owner.as_ref(),
                        walk.error_context(bifrost_types::AccountOperation::SyncInventory),
                    ));
                    return;
                }
            };

            // Do not pin queryState: ordinary delivery changes it, and a
            // restart from position zero can starve inventory indefinitely.
            // receivedAt is immutable, and RFC 8620 s5.5 requires stable
            // tie ordering between queries. Existing survivors therefore
            // cannot cross this anchor. New arrivals and membership moves
            // behind it are covered by the change cursor seeded BEFORE
            // inventory. If the anchor itself disappears, anchorNotFound
            // takes the cursor-invalid restart lane through the context above.
            let ids = query_response.ids().to_vec();
            if ids.is_empty() {
                break;
            }
            // RFC 8620 s5.5 permits a server to clamp the query limit.
            // Only an empty page proves completion, never a short page.
            // anchorOffset: 1 excludes the anchor even when queryState moves.
            // Echoing it would otherwise hot-loop without proving coverage.
            if let Some(fault) = walk.observe_inventory_page(&ids) {
                yield super::error::terminated_contract_violation(
                    bifrost_types::AccountOperation::SyncInventory,
                    Some(bifrost_types::ErrorScope::Cursor(scope.clone())),
                    fault.describe("Email/query"),
                );
                return;
            }
            let get_response = mail
                .call(EmailGet::new().ids(ids).properties(inventory_properties()))
                .await;

            let get_response = match get_response {
                Ok(response) => response,
                Err(err) => {
                    yield super::error::terminated(super::error::shared_scope_error(
                        err,
                        &scope,
                        owner.as_ref(),
                        walk.error_context(bifrost_types::AccountOperation::SyncInventory),
                    ));
                    return;
                }
            };

            let state = get_response.state().to_string();
            let mut items = Vec::new();
            for email in get_response.into_list().into_iter().filter(email_has_id) {
                let mut entry = email_to_inventory(email, &state);
                // Each foreign (shared/delegate) inventory item carries
                // its owning account's `Mailbox(accountId)` membership in
                // addition to its native mailbox memberships, so the
                // consumer maps the item to its shared-account owner (the
                // A5c-established owner-tag pattern).
                if let Some(owner) = &owner {
                    qualify_foreign_memberships(&mut entry.memberships, owner);
                    qualify_foreign_ids(&mut entry, owner);
                }
                items.push(entry);
            }

            // A page that materialized nothing does NOT end the walk: only an
            // empty `Email/query` answer does. So a run of ids the `Email/get`
            // could not answer (destroyed between the query and the get, or
            // returned without an id) is walked through silently, emitting no
            // batch, and the walk overshoots by however many such pages it
            // meets. That overshoot is unbounded in principle - nothing here
            // caps how far a barren stretch may run - and in practice ends at
            // the first surviving message, since the ids come from a query the
            // server just answered.
            //
            // Accepted rather than bounded, because a cap would have to choose
            // between two wrong answers: ending the walk early reports
            // coverage the walk does not have (the same error the missing
            // and re-served-anchor exits refuse to make), and terminating
            // without `Done` restarts a scope that is behaving correctly. The
            // real cost is wasted round trips against a mailbox being emptied
            // underneath the walk, not lost or duplicated coverage.
            if !items.is_empty() {
                yield SyncEvent::Batch(Batch {
                    items,
                    page_boundary: PageBoundary::Page,
                    server_latency: started.elapsed(),
                    bytes_in: tally.take(),
                    checkpoint: None,
                });
            }
        }
        yield SyncEvent::Done(None);
    })
}

/// Qualify a foreign (shared/delegate) item's memberships with its
/// owning account. The `owner` tag IS the foreign JMAP accountId.
/// Each native `Mailbox(native)` membership is re-encoded as
/// `Folder(encode_foreign(accountId, native))` - byte-identical to the
/// container ids `containers_list` mints for the share - and the owner
/// `Mailbox(accountId)` tag is appended. Without the re-encoding, a
/// foreign native mailbox id (e.g. `inbox`) collides with the primary's
/// identical id in the engine's membership index and the two accounts'
/// messages conflate. Shared by the foreign inventory walk and by
/// `hydrate`'s Metadata projection: hydration is where the consumer
/// learns which folder a foreign change landed in (the account-level
/// change stream cannot know), so the two must speak one namespace.
pub(crate) fn qualify_foreign_memberships(
    memberships: &mut Vec<MembershipScope>,
    owner: &TypesMailboxId,
) {
    for membership in memberships.iter_mut() {
        if let MembershipScope::Mailbox(native) = membership {
            *membership =
                MembershipScope::Folder(super::foreign::encode_foreign(&owner.0, &native.0));
        }
    }
    memberships.push(MembershipScope::Mailbox(owner.clone()));
}

/// Qualify a foreign inventory item's OBJECT ids with its owning account.
///
/// `Email/get` and blob download are accountId-scoped, but `get_stream` /
/// `open_blob` receive only an id - no scope - so a bare native id would
/// route hydration and blob reads through the PRIMARY account and fail (or
/// silently resolve a same-id primary object). Encoding the owning
/// accountId into both the object id and the whole-message `blobId` is what
/// makes those reads self-routing, exactly as the folder codec makes the
/// cursor scope self-routing on a cold resume.
///
/// The `thread_id` rides the same object namespace. It is not a read-only
/// grouping key: the consumer hands it straight back as
/// `thread_hydrate(thread)` and as `MutationTarget::Thread`, and every one
/// of those doors expands it through an accountId-scoped `Thread/get`.
/// Left bare, a foreign thread id is indistinguishable from a primary one,
/// so on an id collision the primary `Thread/get` resolves an UNRELATED
/// thread and the mutation lands on its messages - `delete_thread`
/// destroys them.
pub(crate) fn qualify_foreign_ids(entry: &mut InventoryEntry, owner: &TypesMailboxId) {
    entry.id = ObjectId(super::foreign::encode_object(&owner.0, &entry.id.0));
    if let Some(blob) = entry.blob_id.take() {
        entry.blob_id = Some(BlobId(super::foreign::encode_object(&owner.0, &blob.0)));
    }
    if let Some(thread) = entry.thread_id.take() {
        entry.thread_id = Some(ThreadId(super::foreign::encode_object(&owner.0, &thread.0)));
    }
}

fn email_inventory<T: HttpTransport>(
    mail: MailAccount<T>,
    limits: CoreLimits,
) -> AccountStream<SyncEvent<InventoryEntry>> {
    email_inventory_loop(
        mail,
        CursorScope::Type(ObjectType::Email),
        None,
        None,
        limits.max_objects_in_get.max(1),
    )
}

pub(crate) fn inventory_properties() -> Vec<EmailProperty> {
    vec![
        EmailProperty::Id,
        EmailProperty::MailboxIds,
        EmailProperty::ThreadId,
        EmailProperty::BlobId,
        EmailProperty::Size,
        EmailProperty::Keywords,
        EmailProperty::MessageId,
        EmailProperty::References,
        EmailProperty::InReplyTo,
        EmailProperty::ReceivedAt,
    ]
}

fn mailbox_inventory<T: HttpTransport>(
    mail: MailAccount<T>,
    page_size: usize,
) -> AccountStream<SyncEvent<InventoryEntry>> {
    Box::pin(async_stream::stream! {
        let (mail, tally) = mail.metered();
        let scope = CursorScope::Type(ObjectType::Mailbox);
        let mut walk = StateWalk::inventory(scope.clone());
        loop {
            let started = Instant::now();
            let mut query = MailboxQuery::new()
                .sort([
                    crate::mailbox::query::Comparator::sort_order().ascending(),
                    crate::mailbox::query::Comparator::name().ascending(),
                ])
                .limit(page_size);
            if let Some(anchor) = walk.inventory_anchor() {
                query = query.anchor(anchor.to_string()).anchor_offset(1);
            } else {
                query = query.position(0);
            }
            let query_response = match mail.call(query).await {
                Ok(response) => response,
                Err(err) => {
                    yield super::error::terminated(super::error::shared_scope_error(
                        err,
                        &scope,
                        None,
                        walk.error_context(bifrost_types::AccountOperation::SyncInventory),
                    ));
                    return;
                }
            };

            let ids = query_response.ids().to_vec();
            if ids.is_empty() {
                break;
            }
            if let Some(fault) = walk.observe_inventory_page(&ids) {
                yield super::error::terminated_contract_violation(
                    bifrost_types::AccountOperation::SyncInventory,
                    Some(bifrost_types::ErrorScope::Cursor(scope.clone())),
                    fault.describe("Mailbox/query"),
                );
                return;
            }

            let response = match mail
                .call(MailboxGet::new().ids(ids).properties(mailbox_inventory_properties()))
                .await
            {
                Ok(response) => response,
                Err(err) => {
                    yield super::error::terminated(super::error::shared_scope_error(
                        err,
                        &scope,
                        None,
                        walk.error_context(bifrost_types::AccountOperation::SyncInventory),
                    ));
                    return;
                }
            };

            let state = response.state().to_string();
            let items = response
                .into_list()
                .into_iter()
                .filter(mailbox_has_id)
                .map(|mailbox| mailbox_to_inventory(mailbox, &state))
                .collect::<Vec<_>>();

            if !items.is_empty() {
                yield SyncEvent::Batch(Batch {
                    items,
                    page_boundary: PageBoundary::Page,
                    server_latency: started.elapsed(),
                    bytes_in: tally.take(),
                    checkpoint: None,
                });
            }
        }
        yield SyncEvent::Done(None);
    })
}

fn mailbox_inventory_properties() -> Vec<MailboxProperty> {
    vec![
        MailboxProperty::Id,
        MailboxProperty::Name,
        MailboxProperty::ParentId,
        MailboxProperty::Role,
        MailboxProperty::SortOrder,
        MailboxProperty::TotalEmails,
        MailboxProperty::UnreadEmails,
        MailboxProperty::TotalThreads,
        MailboxProperty::UnreadThreads,
        MailboxProperty::IsSubscribed,
    ]
}

/// Whether a fetched object carries an id we can hand to a consumer.
///
/// An object arriving with no `id`, or an empty one, cannot become an
/// inventory row: `ObjectId("")` is a handle to nothing, and once qualified
/// for a foreign account it becomes the equally bogus `"acct-9\u{1f}"`, which
/// then routes later reads at whatever that string happens to collide with.
/// The rest of the crate already refuses the shape - hydration drops the
/// object so the submitted id falls through to the `PartialResponse` lane, and
/// container discovery filters it - so the inventory walks, the one path that
/// used to manufacture the row instead, drop it here. Under the crate's closed
/// per-item accounting an unidentifiable object is not an item.
fn email_has_id(email: &Email) -> bool {
    email.id().is_some_and(|id| !id.as_str().is_empty())
}

/// Mailbox counterpart of [`email_has_id`]; same reasoning.
fn mailbox_has_id(mailbox: &Mailbox) -> bool {
    mailbox.id().is_some_and(|id| !id.as_str().is_empty())
}

pub(crate) fn email_to_inventory(email: Email, state: &str) -> InventoryEntry {
    let id = email.id().map(ToString::to_string).unwrap_or_default();
    let memberships = email
        .mailbox_ids()
        .into_iter()
        .map(|mailbox| MembershipScope::Mailbox(bifrost_types::MailboxId(mailbox.to_string())))
        .collect::<Vec<_>>();
    let size = u64::try_from(email.size()).ok();
    let blob_id = email.blob_id().map(|id| BlobId(id.to_string()));
    let thread_id = email.thread_id().map(|id| ThreadId(id.to_string()));
    let message_id = email.message_id().and_then(|ids| ids.first().cloned());
    let references = email.references().map(<[_]>::to_vec).unwrap_or_default();
    let in_reply_to = email.in_reply_to().and_then(|ids| ids.first().cloned());

    InventoryEntry {
        id: ObjectId(id),
        memberships,
        size,
        blob_id,
        fingerprint: Fingerprint {
            server_version: ServerVersion::StateAt(state.to_string()),
            size,
            flags_hash: flags_hash(&email),
        },
        thread_id,
        message_id,
        references,
        in_reply_to,
    }
}

fn mailbox_to_inventory(mailbox: Mailbox, state: &str) -> InventoryEntry {
    let id = mailbox.id().map(ToString::to_string).unwrap_or_default();
    let memberships = mailbox
        .parent_id()
        .map(|parent| MembershipScope::Mailbox(bifrost_types::MailboxId(parent.to_string())))
        .into_iter()
        .collect::<Vec<_>>();
    let flags_hash = mailbox_flags_hash(&mailbox);

    InventoryEntry {
        id: ObjectId(id),
        memberships,
        size: None,
        blob_id: None,
        fingerprint: Fingerprint {
            server_version: ServerVersion::StateAt(state.to_string()),
            size: None,
            flags_hash,
        },
        thread_id: None,
        message_id: None,
        references: Vec::new(),
        in_reply_to: None,
    }
}

/// Container state condensed into the `flags_hash` slot of a mailbox
/// `Fingerprint`. A mailbox has no flag set, so the tracked properties are
/// encoded as `key=value` pseudo-flags and run through the same crate-owned
/// derivation every other `flags_hash` producer uses - the point of finding 3
/// is that no producer of this field invents its own hash. Set semantics are
/// harmless here because the keys are distinct by construction.
fn mailbox_flags_hash(mailbox: &Mailbox) -> u64 {
    let mut parts = Vec::new();
    if let Some(name) = mailbox.name() {
        parts.push(format!("name={name}"));
    }
    if let Some(parent) = mailbox.parent_id() {
        parts.push(format!("parent={parent}"));
    }
    if let Some(role) = mailbox.role() {
        parts.push(format!("role={role:?}"));
    }
    if let Some(sort_order) = mailbox.sort_order() {
        parts.push(format!("sort={sort_order}"));
    }
    if let Some(total) = mailbox.total_emails() {
        parts.push(format!("totalEmails={total}"));
    }
    if let Some(unread) = mailbox.unread_emails() {
        parts.push(format!("unreadEmails={unread}"));
    }
    if let Some(total) = mailbox.total_threads() {
        parts.push(format!("totalThreads={total}"));
    }
    if let Some(unread) = mailbox.unread_threads() {
        parts.push(format!("unreadThreads={unread}"));
    }
    if let Some(is_subscribed) = mailbox.is_subscribed() {
        parts.push(format!("subscribed={is_subscribed}"));
    }
    bifrost_types::canonical_flags_hash(parts)
}

/// The email's keyword set, and only that.
///
/// This used to fold `mailboxIds`, `receivedAt` and `size` into the same u64,
/// which made a field named `flags_hash` a general change detector. Each of
/// those is now carried where the consumer can actually read it:
/// `InventoryEntry::memberships` holds the mailbox set, `Fingerprint::size`
/// holds the size, and `receivedAt` is immutable in JMAP. Narrowing loses no
/// diff signal and makes the field mean what its contract says.
pub(crate) fn flags_hash(email: &Email) -> u64 {
    bifrost_types::canonical_flags_hash(email.keywords())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::MailboxId;

    #[test]
    fn foreign_memberships_are_qualified_with_owner_account() {
        let owner = TypesMailboxId("acct-9".to_string());
        let mut memberships = vec![
            MembershipScope::Mailbox(MailboxId("inbox".to_string())),
            MembershipScope::Mailbox(MailboxId("mbx-2".to_string())),
        ];
        qualify_foreign_memberships(&mut memberships, &owner);

        // Each native membership is re-encoded into the foreign account's
        // namespace; a bare `Mailbox(inbox)` (which would collide with
        // the primary's `inbox`) must not survive.
        let inbox_qualified = super::super::foreign::encode_foreign("acct-9", "inbox");
        assert!(memberships.contains(&MembershipScope::Folder(inbox_qualified)));
        assert!(
            !memberships.contains(&MembershipScope::Mailbox(MailboxId("inbox".to_string()))),
            "native mailbox membership must be qualified, not bare"
        );
        // The owner tag is appended.
        assert!(memberships.contains(&MembershipScope::Mailbox(owner.clone())));
        // Two native memberships qualified + one owner tag.
        assert_eq!(memberships.len(), 3);
    }

    #[test]
    fn foreign_object_and_blob_ids_are_qualified_with_owner_account() {
        let owner = TypesMailboxId("acct-9".to_string());
        let mut entry = InventoryEntry {
            id: ObjectId("M1".to_string()),
            memberships: Vec::new(),
            size: Some(10),
            blob_id: Some(BlobId("B1".to_string())),
            fingerprint: Fingerprint {
                server_version: ServerVersion::StateAt("s1".to_string()),
                size: Some(10),
                flags_hash: bifrost_types::canonical_flags_hash(std::iter::empty::<&str>()),
            },
            thread_id: Some(ThreadId("T1".to_string())),
            message_id: None,
            references: Vec::new(),
            in_reply_to: None,
        };
        qualify_foreign_ids(&mut entry, &owner);

        // Both ids carry the owning account so hydration / blob reads route
        // there instead of the primary account.
        assert_eq!(
            entry.id.0,
            super::super::foreign::encode_object("acct-9", "M1")
        );
        assert_eq!(super::super::foreign::native_object(&entry.id.0), "M1");
        let blob = entry.blob_id.expect("blob id present");
        assert_eq!(blob.0, super::super::foreign::encode_object("acct-9", "B1"));
        assert_eq!(super::super::foreign::native_object(&blob.0), "B1");

        // The thread id is qualified in the same namespace and decodes
        // back to the native id the foreign `Thread/get` expects. Left
        // bare it would assert PRIMARY ownership, and a thread-keyed
        // mutation on an id collision would rewrite an unrelated primary
        // thread's messages.
        let thread = entry.thread_id.expect("thread id present");
        assert_eq!(
            thread.0,
            super::super::foreign::encode_object("acct-9", "T1")
        );
        assert_eq!(super::super::foreign::native_object(&thread.0), "T1");
    }

    /// A primary entry keeps every id bare: one logical object, one wire
    /// form, and a bare thread id is exactly the assertion "this is the
    /// primary account's thread".
    #[test]
    fn a_primary_entry_is_never_qualified() {
        let email: Email = serde_json::from_value(serde_json::json!({
            "id": "M1",
            "blobId": "B1",
            "threadId": "T1",
            "size": 10,
            "mailboxIds": {"inbox": true},
            "keywords": {}
        }))
        .expect("email deserializes");
        let entry = email_to_inventory(email, "s1");

        assert_eq!(entry.id.0, "M1");
        assert_eq!(
            entry.blob_id.as_ref().map(|blob| blob.0.as_str()),
            Some("B1")
        );
        assert_eq!(
            entry.thread_id.as_ref().map(|thread| thread.0.as_str()),
            Some("T1")
        );
    }

    /// An object the server returned without a usable `id` is not an
    /// inventory item: the walks filter it rather than minting
    /// `ObjectId("")`, which would qualify into `"acct-9\u{1f}"` for a
    /// foreign share and route later reads at nothing.
    #[test]
    fn an_object_without_an_id_is_not_an_inventory_item() {
        let missing: Email = serde_json::from_value(serde_json::json!({
            "blobId": "B1",
            "size": 10,
            "mailboxIds": {"inbox": true},
            "keywords": {}
        }))
        .expect("email deserializes");
        assert!(!super::email_has_id(&missing));

        let empty: Email = serde_json::from_value(serde_json::json!({
            "id": "",
            "blobId": "B1",
            "size": 10,
            "mailboxIds": {"inbox": true},
            "keywords": {}
        }))
        .expect("email deserializes");
        assert!(!super::email_has_id(&empty));

        let present: Email = serde_json::from_value(serde_json::json!({
            "id": "M1",
            "blobId": "B1",
            "size": 10,
            "mailboxIds": {"inbox": true},
            "keywords": {}
        }))
        .expect("email deserializes");
        assert!(super::email_has_id(&present));

        let mailbox: Mailbox = serde_json::from_value(serde_json::json!({"name": "Inbox"}))
            .expect("mailbox deserializes");
        assert!(!super::mailbox_has_id(&mailbox));
        let mailbox: Mailbox =
            serde_json::from_value(serde_json::json!({"id": "X1", "name": "Inbox"}))
                .expect("mailbox deserializes");
        assert!(super::mailbox_has_id(&mailbox));
    }

    /// In-process query scripts for delivery churn, anchor loss, and a
    /// provider that ignores anchorOffset and would spin the walk forever.
    #[derive(Clone, Copy)]
    enum InventoryScenario {
        Stuck,
        Churn,
        Clamped,
        MissingAnchor,
    }

    struct InventoryTransport {
        scenario: InventoryScenario,
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    #[derive(Clone)]
    struct MailboxInventoryTransport {
        calls: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    }

    impl crate::core::transport::HttpTransport for MailboxInventoryTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            let request: serde_json::Value = serde_json::from_slice(&body).expect("request json");
            let call = request["methodCalls"][0].clone();
            self.calls.lock().expect("calls").push(call.clone());
            let name = call[0].as_str().expect("method name").to_string();
            let call_id = call[2].as_str().expect("call id").to_string();
            let arguments = match name.as_str() {
                "Mailbox/query" => {
                    assert_eq!(call[1]["limit"], 2);
                    let ids = match call[1]["anchor"].as_str() {
                        None => {
                            assert_eq!(call[1]["position"], 0);
                            serde_json::json!(["X1", "X2"])
                        }
                        Some("X2") => {
                            assert_eq!(call[1]["anchorOffset"], 1);
                            serde_json::json!(["X3"])
                        }
                        Some("X3") => {
                            assert_eq!(call[1]["anchorOffset"], 1);
                            serde_json::json!([])
                        }
                        other => panic!("unexpected mailbox query anchor {other:?}"),
                    };
                    serde_json::json!({
                        "accountId": "primary",
                        "queryState": "mailbox-query-state",
                        "position": 0,
                        "ids": ids
                    })
                }
                "Mailbox/get" => {
                    let ids = call[1]["ids"].as_array().expect("bounded get ids");
                    assert!(!ids.is_empty());
                    assert!(ids.len() <= 2);
                    serde_json::json!({
                        "accountId": "primary",
                        "state": "mailbox-state",
                        "list": ids.iter().map(|id| serde_json::json!({
                            "id": id,
                            "name": format!("mailbox-{}", id.as_str().expect("mailbox id"))
                        })).collect::<Vec<_>>(),
                        "notFound": []
                    })
                }
                other => panic!("unexpected method {other}"),
            };
            Ok(bytes::Bytes::from(
                serde_json::json!({
                    "sessionState": "session-1",
                    "methodResponses": [[name, arguments, call_id]]
                })
                .to_string(),
            ))
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no upload"))
        }

        async fn download(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no download"))
        }

        async fn get_session(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no session"))
        }
    }

    impl crate::core::transport::HttpTransport for InventoryTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let request: serde_json::Value = serde_json::from_slice(&body).expect("request json");
            let call = request["methodCalls"][0].clone();
            let name = call[0].as_str().expect("method name").to_string();
            let call_id = call[2].as_str().expect("call id").to_string();
            let anchor = call[1]["anchor"].as_str();
            if name == "Email/query" && anchor.is_some() {
                assert_eq!(call[1]["anchorOffset"], 1);
                if matches!(self.scenario, InventoryScenario::MissingAnchor) {
                    return Ok(bytes::Bytes::from(
                        serde_json::json!({
                            "sessionState": "session-1",
                            "methodResponses": [["error", {"type": "anchorNotFound"}, call_id]]
                        })
                        .to_string(),
                    ));
                }
            }
            let arguments = match name.as_str() {
                "Email/query" if matches!(self.scenario, InventoryScenario::Clamped) => {
                    assert_eq!(call[1]["limit"], 2);
                    let ids = match anchor {
                        None => {
                            assert_eq!(call[1]["position"], 0);
                            vec!["M1"]
                        }
                        Some("M1") => vec!["M2"],
                        Some("M2") => vec![],
                        other => panic!("unexpected anchor {other:?}"),
                    };
                    serde_json::json!({
                        "accountId": "primary", "queryState": "q1",
                        "canCalculateChanges": false, "position": 0, "ids": ids
                    })
                }
                "Email/query" if matches!(self.scenario, InventoryScenario::Churn) => {
                    // New messages land before each surviving anchor. Tied
                    // receivedAt values retain their server-defined order.
                    let (state, position, ids) = match anchor {
                        None => ("q1", 0, vec!["M1", "M2"]),
                        Some("M2") => ("q2", 3, vec!["M3"]),
                        Some("M3") => ("q3", 5, vec![]),
                        other => panic!("unexpected anchor {other:?}"),
                    };
                    serde_json::json!({
                        "accountId": "primary", "queryState": state,
                        "canCalculateChanges": false, "position": position, "ids": ids
                    })
                }
                "Email/get"
                    if matches!(
                        self.scenario,
                        InventoryScenario::Churn | InventoryScenario::Clamped
                    ) =>
                {
                    let ids = call[1]["ids"].as_array().expect("get ids");
                    let list: Vec<_> = ids
                        .iter()
                        .map(|id| {
                            serde_json::json!({
                                "id": id, "blobId": "B", "size": 1,
                                "mailboxIds": {"inbox": true}, "keywords": {},
                                "receivedAt": "2026-01-01T00:00:00Z"
                            })
                        })
                        .collect();
                    serde_json::json!({
                        "accountId": "primary", "state": "s1", "list": list, "notFound": []
                    })
                }

                "Email/query" => serde_json::json!({
                    "accountId": "primary",
                    "queryState": "q1",
                    "canCalculateChanges": false,
                    "position": 0,
                    "ids": ["M1", "M2"]
                }),
                "Email/get" => serde_json::json!({
                    "accountId": "primary",
                    "state": "s1",
                    "list": [
                        {"id": "M1", "blobId": "B1", "size": 1, "mailboxIds": {"inbox": true}, "keywords": {}},
                        {"id": "M2", "blobId": "B2", "size": 1, "mailboxIds": {"inbox": true}, "keywords": {}}
                    ],
                    "notFound": []
                }),
                other => panic!("unexpected method {other}"),
            };
            let response = serde_json::json!({
                "sessionState": "session-1",
                "methodResponses": [[name, arguments, call_id]]
            });
            Ok(bytes::Bytes::from(response.to_string()))
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no upload"))
        }

        async fn download(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no download"))
        }

        async fn get_session(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no session"))
        }
    }

    fn inventory_test_session() -> crate::core::session::Session {
        serde_json::from_value(serde_json::json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 1000,
                    "maxConcurrentUpload": 2,
                    "maxSizeRequest": 100_000,
                    "maxConcurrentRequests": 4,
                    "maxCallsInRequest": 8,
                    "maxObjectsInGet": 2,
                    "maxObjectsInSet": 2,
                    "collationAlgorithms": []
                },
                "urn:ietf:params:jmap:mail": {}
            },
            "accounts": {
                "primary": {"name": "Primary", "isPersonal": true, "isReadOnly": false,
                    "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}}
            },
            "primaryAccounts": {"urn:ietf:params:jmap:mail": "primary"},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("session parses")
    }

    #[tokio::test]
    async fn inventory_completes_despite_query_state_churn() {
        use futures::StreamExt as _;

        // All routes share the same walk: primary, legacy filtered mailbox,
        // and foreign account inventory must each tolerate query-state churn.
        for (filter, owner) in [
            (None, None),
            (Some(crate::email::query::Filter::in_mailbox("inbox")), None),
            (None, Some(TypesMailboxId("share".to_string()))),
        ] {
            let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let client = crate::client::Client::with_transport(
                InventoryTransport {
                    scenario: InventoryScenario::Churn,
                    calls: std::sync::Arc::clone(&calls),
                },
                inventory_test_session(),
                "https://example.test/.well-known/jmap",
            )
            .expect("client builds");
            let events = email_inventory_loop(
                crate::account::Account::new(client, "primary"),
                CursorScope::Type(ObjectType::Email),
                filter,
                owner.clone(),
                2,
            )
            .collect::<Vec<_>>()
            .await;
            assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 5);
            assert!(matches!(events.last(), Some(SyncEvent::Done(None))));
            let ids: Vec<_> = events
                .iter()
                .flat_map(|event| match event {
                    SyncEvent::Batch(batch) => {
                        batch.items.iter().map(|item| item.id.0.clone()).collect()
                    }
                    SyncEvent::Done(_) => Vec::new(),
                    other => panic!("unexpected inventory event {other:?}"),
                })
                .collect();
            let expected: Vec<_> = ["M1", "M2", "M3"]
                .into_iter()
                .map(|id| {
                    owner.as_ref().map_or_else(
                        || id.to_string(),
                        |owner| super::super::foreign::encode_object(&owner.0, id),
                    )
                })
                .collect();
            assert_eq!(ids, expected);
        }
    }

    #[tokio::test]
    async fn email_inventory_continues_after_server_clamped_pages() {
        use futures::StreamExt as _;

        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let client = crate::client::Client::with_transport(
            InventoryTransport {
                scenario: InventoryScenario::Clamped,
                calls: std::sync::Arc::clone(&calls),
            },
            inventory_test_session(),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        let events = email_inventory_loop(
            crate::account::Account::new(client, "primary"),
            CursorScope::Type(ObjectType::Email),
            None,
            None,
            2,
        )
        .collect::<Vec<_>>()
        .await;

        // The server serves one id per query despite the requested limit of
        // two. Both pages must be hydrated before the third, empty query.
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 5);
        let ids = events
            .iter()
            .flat_map(|event| match event {
                SyncEvent::Batch(batch) => {
                    batch.items.iter().map(|item| item.id.0.clone()).collect()
                }
                SyncEvent::Done(_) => Vec::new(),
                other => panic!("unexpected inventory event {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["M1", "M2"]);
        assert!(matches!(events.last(), Some(SyncEvent::Done(None))));
    }

    #[tokio::test]
    async fn a_missing_inventory_anchor_restarts_without_done() {
        use futures::StreamExt as _;

        let client = crate::client::Client::with_transport(
            InventoryTransport {
                scenario: InventoryScenario::MissingAnchor,
                calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            },
            inventory_test_session(),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        let scope = CursorScope::Type(ObjectType::Email);
        let events = email_inventory_loop(
            crate::account::Account::new(client, "primary"),
            scope.clone(),
            None,
            None,
            2,
        )
        .collect::<Vec<_>>()
        .await;
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], SyncEvent::Batch(_)));
        let SyncEvent::Terminated(error) = &events[1] else {
            panic!("missing anchor must terminate, got {events:?}");
        };
        assert_eq!(
            error.scope(),
            Some(&bifrost_types::ErrorScope::Cursor(scope.clone()))
        );
        assert_eq!(
            error.recovery(),
            &bifrost_types::RecoveryClass::Engine(bifrost_types::EngineDirective::RestartScope(
                scope
            ),)
        );
    }

    #[tokio::test]
    async fn a_re_served_inventory_anchor_terminates_instead_of_looping() {
        use futures::StreamExt as _;

        let session = inventory_test_session();
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let client = crate::client::Client::with_transport(
            InventoryTransport {
                scenario: InventoryScenario::Stuck,
                calls: std::sync::Arc::clone(&calls),
            },
            session,
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        let mail = crate::account::Account::new(client, "primary");
        let events = stream(
            mail,
            CoreLimits {
                max_objects_in_get: 2,
                max_objects_in_set: 2,
            },
            CursorScope::Type(ObjectType::Email),
            None,
        )
        .collect::<Vec<_>>()
        .await;

        // Page one is served (query + get); page two re-serves the anchor
        // and is refused before another `Email/get`.
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 3);
        let error = events
            .iter()
            .find_map(|event| match event {
                SyncEvent::Terminated(error) => Some(error),
                _ => None,
            })
            .unwrap_or_else(|| panic!("expected Terminated, got {events:?}"));
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ContractViolation
            )
        );
        // No `Done`: the walk's coverage was never established, so the
        // consumer must not record a completed inventory.
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, SyncEvent::Done(_))),
            "a refused walk must not report completion"
        );
    }

    #[tokio::test]
    async fn mailbox_inventory_batches_get_by_the_advertised_limit() {
        use futures::StreamExt as _;

        let transport = MailboxInventoryTransport {
            calls: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        };
        let client = crate::client::Client::with_transport(
            transport.clone(),
            inventory_test_session(),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        let events = mailbox_inventory(crate::account::Account::new(client, "primary"), 2)
            .collect::<Vec<_>>()
            .await;

        let calls = transport.calls.lock().expect("calls").clone();
        assert_eq!(
            calls
                .iter()
                .map(|call| call[0].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec![
                "Mailbox/query",
                "Mailbox/get",
                "Mailbox/query",
                "Mailbox/get",
                "Mailbox/query",
            ]
        );
        for call in &calls {
            if call[0] == "Mailbox/get" {
                assert!(call[1]["ids"].is_array());
                assert!(call[1]["ids"].as_array().unwrap().len() <= 2);
            }
        }
        let ids = events
            .iter()
            .flat_map(|event| match event {
                SyncEvent::Batch(batch) => {
                    batch.items.iter().map(|item| item.id.0.clone()).collect()
                }
                SyncEvent::Done(_) => Vec::new(),
                other => panic!("unexpected inventory event {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["X1", "X2", "X3"]);
        assert!(matches!(events.last(), Some(SyncEvent::Done(None))));
    }
}
