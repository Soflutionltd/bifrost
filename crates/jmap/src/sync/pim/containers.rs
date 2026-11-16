//! Mailbox CRUD, role lookup and shared-owner container discovery.

use super::{MailAccount, to_acct_err};
use crate::core::SetCreate;
use crate::core::transport::HttpTransport;
use crate::mailbox::{
    Mailbox, MailboxGet, MailboxId, MailboxRights, MailboxSet, Property as MailboxProperty, Role,
};
use crate::sync::error::{answer_error, missing_shared_rights};
use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, Container, ContainerId, ContainerKind,
    ContainerList, ContainerNamespace, ContainerRights, FolderRole, Provenance, SkippedScope,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Every container the account exposes: the primary account's mailboxes
/// plus each foreign (shared/delegate) account's, namespaced by owner.
///
/// A per-share enumeration failure degrades to a `SkippedScope` on the
/// returned `ContainerList` plus the remaining containers: one
/// unreachable share must not blank the whole sidebar, and it must not
/// vanish from it silently either - the skip entry names the foreign
/// account and carries the classified error, so the consumer can tell
/// "share degraded" from "share deleted". A primary enumeration
/// failure still fails the call.
pub(crate) fn containers_list<T: HttpTransport>(
    foreign_mail: Arc<crate::sync::registry::Registry<T>>,
) -> AccountFuture<Result<ContainerList, AccountError>> {
    let mail = foreign_mail.primary.clone();

    Box::pin(async move {
        let mut containers = fetch_containers(&mail, AccountOperation::ContainersList).await?;
        // Owner emails are resolved once per foreign account per call
        // (never per container) and are best-effort metadata: any
        // resolution failure yields `None` for that account and MUST NOT
        // propagate out of `containers_list` - the consumer treats a
        // container-listing error as an attach failure, and a missing
        // cosmetic email must never cause an account outage.
        let owner_emails = resolve_owner_emails(&mail, &foreign_mail).await;
        let (foreign, skipped_scopes) = fetch_foreign_containers(
            &foreign_mail,
            &owner_emails,
            AccountOperation::ContainersList,
        )
        .await;
        containers.extend(foreign);
        Ok(ContainerList {
            containers,
            skipped_scopes,
        })
    })
}

pub(super) fn container_wire_id<T: HttpTransport>(
    id: &ContainerId,
    slot: &MailAccount<T>,
) -> MailboxId {
    MailboxId::new(slot.wire(&id.0))
}

pub(crate) fn container_create<T: HttpTransport>(
    mail: MailAccount<T>,
    kind: ContainerKind,
    name: String,
    parent: Option<ContainerId>,
    // JMAP mailboxes carry no container color; accepted for trait
    // parity with the colorable (Gmail) path and ignored.
    _style: Option<bifrost_types::ContainerStyle>,
) -> AccountFuture<Result<ContainerId, AccountError>> {
    Box::pin(async move {
        if !matches!(kind, ContainerKind::Folder) {
            return Err(crate::sync::error::unsupported_error(
                AccountOperation::ContainerCreate,
                None,
                bifrost_types::UnsupportedReason::ContainerKind,
                "JMAP only supports folder containers",
            ));
        }
        // Child creation follows the shared parent. Users expect the new row
        // to remain in that share, including the returned container identity.
        let mut set = MailboxSet::new();
        let mut create = crate::mailbox::MailboxCreate::new(None);
        create.name(name);
        create.parent_id(parent.map(|id| container_wire_id(&id, &mail)));
        let create_id = set.create_item(create);
        let mut response = mail
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::ContainerCreate))?;
        let mailbox = response
            .created(&create_id)
            .map_err(answer_error(AccountOperation::ContainerCreate))?;
        // An id-less success entry is refused, never handed back as an empty
        // ContainerId.
        crate::sync::error::require_created_id(
            mailbox.id().map(MailboxId::as_str),
            AccountOperation::ContainerCreate,
            "Mailbox/set",
        )
        .map(|id| ContainerId(crate::sync::foreign::JmapRef::encode(mail.owner(), &id)))
    })
}

pub(crate) fn container_rename<T: HttpTransport>(
    mail: MailAccount<T>,
    container: ContainerId,
    name: String,
    // JMAP has no mailbox recolor; accepted for trait parity and ignored.
    _style: Option<bifrost_types::ContainerStyle>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let mailbox = container_wire_id(&container, &mail);
        let mut set = MailboxSet::new();
        set.update(mailbox.clone()).name(name);
        let mut response = mail
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::ContainerRename))?;
        // Silence about this id is an unconfirmed write, not a successful
        // folder change. Keep the sidebar honest even if newState advanced.
        response
            .updated(&mailbox)
            .map_err(answer_error(AccountOperation::ContainerRename))?;
        Ok(())
    })
}

pub(crate) fn container_move<T: HttpTransport>(
    mail: MailAccount<T>,
    container: ContainerId,
    new_parent: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        // A root move retains the share. A parent in another account cannot
        // be expressed by Mailbox/set; never risk resolving a colliding id.
        if let Some(parent) = &new_parent
            && !crate::sync::foreign::JmapRef::parse(&container.0)
                .same_owner(crate::sync::foreign::JmapRef::parse(&parent.0))
        {
            return Err(crate::sync::error::invalid_argument(
                AccountOperation::ContainerMove,
                "parent",
                "Mailbox/set cannot move a folder between accounts".to_string(),
            ));
        }
        let mailbox = container_wire_id(&container, &mail);
        let mut set = MailboxSet::new();
        set.update(mailbox.clone())
            .parent_id(new_parent.map(|id| container_wire_id(&id, &mail)));
        let mut response = mail
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::ContainerMove))?;
        // Silence about this id is an unconfirmed write, not a successful
        // folder change. Keep the sidebar honest even if newState advanced.
        response
            .updated(&mailbox)
            .map_err(answer_error(AccountOperation::ContainerMove))?;
        Ok(())
    })
}

pub(crate) fn container_delete<T: HttpTransport>(
    mail: MailAccount<T>,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let mailbox = container_wire_id(&container, &mail);
        let mut response = mail
            .call(
                MailboxSet::new()
                    .destroy([mailbox.clone()])
                    .on_destroy_remove_emails(false),
            )
            .await
            .map_err(to_acct_err(AccountOperation::ContainerDelete))?;
        response
            .destroyed(&mailbox)
            .map_err(crate::sync::error::answer_error(
                AccountOperation::ContainerDelete,
            ))?;
        Ok(())
    })
}

pub(super) async fn role_mailbox<T: HttpTransport>(
    mail: &MailAccount<T>,
    role: FolderRole,
    op: AccountOperation,
) -> Result<MailboxId, AccountError> {
    role_mailboxes(mail, &[role], op)
        .await?
        .remove(&role)
        .ok_or_else(|| missing_role_mailbox(op))
}

/// Resolve several role mailboxes from ONE `Mailbox/get`. Roles the
/// account does not expose are simply absent from the map, so the
/// caller decides which of them are required. Batching matters because
/// the submission paths need Drafts and Sent together and
/// `fetch_mailboxes` is an uncached round trip.
pub(super) async fn role_mailboxes<T: HttpTransport>(
    mail: &MailAccount<T>,
    roles: &[FolderRole],
    op: AccountOperation,
) -> Result<HashMap<FolderRole, MailboxId>, AccountError> {
    let mut found = HashMap::new();
    for mut mailbox in fetch_mailboxes(mail, op).await? {
        let Some(role) = map_role(mailbox.role()) else {
            continue;
        };
        if !roles.contains(&role) {
            continue;
        }
        let id = mailbox.take_id();
        if id.as_str().is_empty() {
            continue;
        }
        found.entry(role).or_insert(id);
    }
    Ok(found)
}

pub(super) fn missing_role_mailbox(op: AccountOperation) -> AccountError {
    crate::sync::error::unsupported_error(
        op,
        None,
        bifrost_types::UnsupportedReason::MissingMailboxRole,
        "JMAP required mailbox role not found",
    )
}

async fn fetch_containers<T: HttpTransport>(
    mail: &MailAccount<T>,
    op: AccountOperation,
) -> Result<Vec<Container>, AccountError> {
    Ok(fetch_mailboxes(mail, op)
        .await?
        .into_iter()
        .filter_map(|mailbox| container_from_mailbox(mailbox, None, None))
        .collect())
}

/// The JMAP principals capabilities (RFC 9670) gating owner-email
/// resolution.
const PRINCIPALS_CAPABILITY: &str = "urn:ietf:params:jmap:principals";
const PRINCIPALS_OWNER_CAPABILITY: &str = "urn:ietf:params:jmap:principals:owner";

/// How one foreign account's owner email resolves. Pure decision core of
/// `resolve_owner_emails`, split out so the gate is unit-pinnable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum OwnerEmailPlan {
    /// No email for this account. Also the fail-soft landing for every
    /// error path.
    Skip,
    /// The account advertises `urn:ietf:params:jmap:principals:owner`
    /// with a principal id: resolve via `Principal/get`, and if that
    /// yields nothing, degrade to `name_fallback` when the session name
    /// is itself an address.
    FromPrincipal {
        principal_id: String,
        name_fallback: Option<String>,
    },
    /// No owner principal, but the account's session NAME is an address:
    /// use it directly. Only reachable when the SESSION advertises the
    /// principals capability - the caller gates on that first.
    FromName(String),
}

/// Decide how a foreign account's owner email resolves, given the
/// account-level owner principal id (if advertised) and the account's
/// session name.
///
/// A principal id is always preferred: `Principal/get` is authoritative
/// and the session name is only a label. A principal the server answers
/// for and does not know is the case the fallback exists for, but the
/// fallback applies ONLY when the lookup actually completed - see
/// `PrincipalEmail`. A lookup that failed teaches nothing, and guessing
/// over it would overwrite real ownership with metadata.
pub(super) fn owner_email_plan(
    owner_principal_id: Option<&str>,
    account_name: &str,
) -> OwnerEmailPlan {
    let name_email = account_name_as_address(account_name);
    if let Some(principal_id) = owner_principal_id {
        return OwnerEmailPlan::FromPrincipal {
            principal_id: principal_id.to_string(),
            name_fallback: name_email,
        };
    }
    match name_email {
        Some(email) => OwnerEmailPlan::FromName(email),
        None => OwnerEmailPlan::Skip,
    }
}

/// Interpret an `Account.name` as an owner address by PARSING it, not
/// by sniffing for an `@`.
///
/// RFC 8620 defines `Account.name` as a user-facing label ("e.g. the
/// email address"); an address is one possible shape, not a contract.
/// A containment test accepted anything with an `@` anywhere in it and
/// stored the whole string, so `Support <support@example.com>` became
/// an owner "address" verbatim.
///
/// Only a bare addr-spec is accepted. Pulling the address out of a
/// `Name <addr>` form is deliberately NOT done: that guesses which
/// address inside a free-form label identifies the owner, and a wrong
/// owner is worse than no owner. Requiring a dotted domain rejects
/// local-only labels that could never route.
pub(super) fn account_name_as_address(name: &str) -> Option<String> {
    let candidate = name.trim();
    if candidate.is_empty() || candidate.chars().any(char::is_whitespace) {
        return None;
    }
    if candidate.contains(['<', '>', ',', ';', '"', '\\']) {
        return None;
    }
    let (local, domain) = candidate.split_once('@')?;
    if local.is_empty() || domain.is_empty() || domain.contains('@') {
        return None;
    }
    if !domain.contains('.') || domain.starts_with('.') || domain.ends_with('.') {
        return None;
    }
    Some(candidate.to_string())
}

/// Plan owner-email resolution for every foreign account in the session.
///
/// Level ONE of the two-level gate lives here: a session that does not
/// advertise `urn:ietf:params:jmap:principals` yields no plans at all -
/// not even the name fallback. Resolving from the account name on a
/// session with no principals capability would populate owner emails
/// where the legacy behavior left them NULL.
pub(super) fn owner_email_plans(
    session: &crate::core::session::Session,
    foreign_account_ids: &[&String],
) -> Vec<(String, OwnerEmailPlan)> {
    if !session.has_capability(PRINCIPALS_CAPABILITY) {
        return Vec::new();
    }
    foreign_account_ids
        .iter()
        .filter_map(|account_id| {
            let account = session.account(account_id)?;
            // Level TWO: the individual account's owner capability.
            let owner_principal_id =
                account
                    .capability(PRINCIPALS_OWNER_CAPABILITY)
                    .and_then(|capability| match capability {
                        crate::core::session::Capabilities::PrincipalsOwner(owner) => {
                            owner.principal_id().map(|id| id.as_str().to_string())
                        }
                        _ => None,
                    });
            Some((
                (*account_id).clone(),
                owner_email_plan(owner_principal_id.as_deref(), account.name()),
            ))
        })
        .collect()
}

/// Resolve owner emails for the foreign accounts, once per account.
/// Returns only the resolved entries; every failure lands as an absent
/// key (fail-soft, mandatory - see `containers_list`).
async fn resolve_owner_emails<T: HttpTransport>(
    mail: &MailAccount<T>,
    foreign_mail: &crate::sync::registry::Registry<T>,
) -> HashMap<String, String> {
    let mut resolved = HashMap::new();
    if foreign_mail.shared().next().is_none() {
        return resolved;
    }
    let session = mail.client().session();
    // Deterministic order so the resolution (and its logging, if any is
    // ever added) is stable across calls.
    let foreign_ids: Vec<String> = foreign_mail
        .shared()
        .map(|(id, _)| id.to_string())
        .collect();
    let foreign_ids: Vec<&String> = foreign_ids.iter().collect();
    let principals_account_id = session
        .principals_capabilities()
        .and_then(|capabilities| capabilities.account_id_for_principal())
        .map(|id| id.as_str().to_string());
    for (account_id, plan) in owner_email_plans(&session, &foreign_ids) {
        match plan {
            OwnerEmailPlan::Skip => {}
            OwnerEmailPlan::FromName(email) => {
                resolved.insert(account_id, email);
            }
            OwnerEmailPlan::FromPrincipal {
                principal_id,
                name_fallback,
            } => {
                let outcome =
                    fetch_principal_email(mail, principals_account_id.as_deref(), &principal_id)
                        .await;
                if let Some(email) = owner_email_from_lookup(outcome, name_fallback) {
                    resolved.insert(account_id, email);
                }
            }
        }
    }
    resolved
}

/// Outcome of one owner-principal lookup.
///
/// The distinction that matters is between "the server answered and
/// there is no email" and "we never got an answer". Collapsing both to
/// `None` meant a transport blip or an unimplemented `Principal/get`
/// was indistinguishable from genuine absence, and the name fallback
/// fired on all of them - so a transient failure could overwrite real
/// ownership with a guess derived from a display label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PrincipalEmail {
    /// Authoritative address from `Principal/get`.
    Resolved(String),
    /// The call succeeded and yielded no address: either the principal
    /// is not one the server knows, or it carries no email property.
    /// There is nothing better to be had, so a parsed account name may
    /// stand in.
    Absent,
    /// The call did not complete. This is not evidence of anything;
    /// never fall back on it, or a blip rewrites ownership.
    Unavailable,
}

/// Combine a principal lookup with the plan's parsed name fallback.
///
/// The whole point of classifying the lookup: only a COMPLETED one
/// licenses the fallback. `Absent` means the server answered and there
/// is no address, so the account name is the best remaining evidence.
/// `Unavailable` means we never got an answer, and substituting a guess
/// there would overwrite authoritative ownership with metadata on every
/// transient failure.
pub(super) fn owner_email_from_lookup(
    outcome: PrincipalEmail,
    name_fallback: Option<String>,
) -> Option<String> {
    match outcome {
        PrincipalEmail::Resolved(email) => Some(email),
        PrincipalEmail::Absent => name_fallback,
        PrincipalEmail::Unavailable => None,
    }
}

/// `Principal/get` for one owner principal, against the session's
/// principals account when it names one (falling back to the mail
/// account's id, matching the legacy default-account behavior).
///
/// Still fail-soft - no error escapes - but the failure is now
/// classified rather than flattened.
async fn fetch_principal_email<T: HttpTransport>(
    mail: &MailAccount<T>,
    principals_account_id: Option<&str>,
    principal_id: &str,
) -> PrincipalEmail {
    let account = match principals_account_id {
        Some(id) => mail
            .client()
            .account::<crate::core::capability::Principals>(id),
        None => mail
            .view::<crate::core::capability::Principals>()
            .map(|slot| slot.mail),
    };
    let Ok(account) = account else {
        return PrincipalEmail::Unavailable;
    };
    let get = crate::principal::PrincipalGet::new()
        .ids([crate::principal::PrincipalId::new(principal_id)])
        .properties([
            crate::principal::Property::Email,
            crate::principal::Property::Name,
        ]);
    match account.call(get).await {
        Ok(response) => response
            .into_list()
            .into_iter()
            .next()
            .and_then(|principal| principal.email().map(String::from))
            .map_or(PrincipalEmail::Absent, PrincipalEmail::Resolved),
        // Transport failure, method error, unimplemented method: we
        // learned nothing about who owns this account.
        Err(_) => PrincipalEmail::Unavailable,
    }
}

/// Enumerate every foreign (shared/delegate) account's mailboxes as
/// `Shared`-namespace containers.
///
/// Each foreign account is independent, so a per-account `Mailbox/get`
/// failure degrades to a `SkippedScope` (naming the foreign accountId,
/// carrying the classified error) plus the remaining containers -
/// matching the shape open-time seeding uses for an unreachable share.
/// One unreachable share must not blank the whole sidebar.
pub(super) async fn fetch_foreign_containers<T: HttpTransport>(
    foreign_mail: &crate::sync::registry::Registry<T>,
    owner_emails: &HashMap<String, String>,
    op: AccountOperation,
) -> (Vec<Container>, Vec<SkippedScope>) {
    let mut containers = Vec::new();
    let mut skipped = Vec::new();
    // Deterministic order so the projection is stable across calls.
    for (account_id, mail) in foreign_mail.shared() {
        let owner_email = owner_emails.get(account_id.as_str()).map(String::as_str);
        match fetch_mailboxes(mail, op).await {
            Ok(mailboxes) => {
                for mailbox in mailboxes {
                    // Read before projection: `container_from_mailbox`
                    // maps the absence onto `rights: None`, which is
                    // also what a protocol that never reports rights
                    // produces, so the omission is unrecoverable after.
                    let rights_missing = mailbox.my_rights().is_none();
                    let Some(container) =
                        container_from_mailbox(mailbox, Some(account_id), owner_email)
                    else {
                        continue;
                    };
                    if rights_missing {
                        skipped.push(SkippedScope {
                            scope: bifrost_types::ErrorScope::Mailbox {
                                id: container.id.0.clone().into(),
                            },
                            error: missing_shared_rights(&container.id.0, op),
                        });
                    }
                    containers.push(container);
                }
            }
            Err(error) => skipped.push(SkippedScope {
                scope: bifrost_types::ErrorScope::Owner {
                    id: account_id.to_string().into(),
                },
                error,
            }),
        }
    }
    (containers, skipped)
}

pub(super) async fn fetch_mailboxes<T: HttpTransport>(
    mail: &MailAccount<T>,
    op: AccountOperation,
) -> Result<Vec<Mailbox>, AccountError> {
    Ok(mail
        .call(MailboxGet::new().properties([
            MailboxProperty::Id,
            MailboxProperty::Name,
            MailboxProperty::ParentId,
            MailboxProperty::Role,
            MailboxProperty::MyRights,
            MailboxProperty::IsSubscribed,
        ]))
        .await
        .map_err(to_acct_err(op))?
        .into_list())
}

/// Project one `Mailbox` onto a `Container`.
///
/// `owner_account` is `Some(accountId)` for a foreign (shared/delegate)
/// account's mailbox. In that case the container's `native_id` is
/// `encode_foreign(accountId, mailboxId)` - byte-identical to the
/// `MembershipScope::Folder` qualification the foreign inventory and
/// hydration stamp on that account's messages, which is what lets the
/// consumer join a message's membership to its container. (The account's
/// SYNC scope is coarser: one account-level `Folder` scope per share,
/// since `Email/changes` cannot be filtered by mailbox.) `owner_local_id`
/// keeps the bare mailbox id for calls made against the owner's own
/// account. The parent is re-encoded in the same namespace so a foreign
/// child never points at a primary mailbox that happens to share the
/// parent's id.
pub(super) fn container_from_mailbox(
    mut mailbox: Mailbox,
    owner_account: Option<&str>,
    owner_email: Option<&str>,
) -> Option<Container> {
    let id = mailbox.take_id();
    if id.as_str().is_empty() {
        return None;
    }
    let local = id.into_string();
    let native = match owner_account {
        Some(account) => crate::sync::foreign::encode_foreign(account, &local).0,
        None => local.clone(),
    };
    let parent = mailbox.parent_id().map(|parent| {
        let parent = parent.to_string();
        ContainerId(match owner_account {
            Some(account) => crate::sync::foreign::encode_foreign(account, &parent).0,
            None => parent,
        })
    });
    let role = map_role(mailbox.role());
    Some(
        Container::new(
            ContainerId(native.clone()),
            ContainerKind::Folder,
            role,
            Provenance {
                provider: bifrost_types::ProtocolKind::Jmap,
                kind: ContainerKind::Folder,
                native,
            },
            mailbox.name().unwrap_or("").to_string(),
            parent,
        )
        // JMAP mailboxes carry no container color, so `style` keeps its
        // `Container::new` default.
        //
        // A role-bearing mailbox is JMAP's native system notion; for
        // folder-shaped JMAP that is exactly what `role` already captures,
        // so there is no hidden split to surface.
        .with_system(role.is_some())
        // JMAP's `Mailbox.myRights` / `Mailbox.isSubscribed` carry the
        // per-folder ACL and subscription state the shared-mailbox sidebar
        // gates submit on.
        .with_rights(mailbox.my_rights().map(rights_from_mailbox))
        .with_subscription(mailbox.is_subscribed())
        .with_namespace(match owner_account {
            Some(_) => ContainerNamespace::Shared,
            None => ContainerNamespace::Personal,
        })
        .with_owner(owner_account.map(|account| bifrost_types::PrincipalId(account.to_string())))
        // Best-effort metadata resolved once per foreign account by
        // `resolve_owner_emails`; `None` for a personal mailbox and for
        // every unresolvable owner (fail-soft).
        .with_owner_email(owner_email.map(str::to_string))
        .with_owner_local_id(owner_account.map(|_| local)),
    )
}

/// Map the JMAP `Mailbox/myRights` object onto the unified
/// [`ContainerRights`]. Each decoded boolean maps to `Some(_)`; older
/// servers omitting `mayShare` grant no sharing authority, so it is false.
fn rights_from_mailbox(rights: &MailboxRights) -> ContainerRights {
    ContainerRights {
        may_read_items: Some(rights.may_read_items()),
        may_add_items: Some(rights.may_add_items()),
        may_remove_items: Some(rights.may_remove_items()),
        may_set_seen: Some(rights.may_set_seen()),
        may_set_keywords: Some(rights.may_set_keywords()),
        may_create_child: Some(rights.may_create_child()),
        may_rename: Some(rights.may_rename()),
        may_delete: Some(rights.may_delete()),
        may_submit: Some(rights.may_submit()),
        may_share: Some(rights.may_share()),
    }
}

fn map_role(role: Option<&Role>) -> Option<FolderRole> {
    match role {
        Some(Role::Inbox) => Some(FolderRole::Inbox),
        Some(Role::Sent) => Some(FolderRole::Sent),
        Some(Role::Drafts) => Some(FolderRole::Drafts),
        Some(Role::Archive) => Some(FolderRole::Archive),
        Some(Role::Trash) => Some(FolderRole::Trash),
        Some(Role::Junk) => Some(FolderRole::Spam),
        _ => None,
    }
}

#[cfg(test)]
mod sharing_rights_tests {
    #[test]
    fn sharing_authority_reaches_container_rights() {
        for allowed in [true, false] {
            let rights = serde_json::from_value(serde_json::json!({"mayShare": allowed})).unwrap();
            assert_eq!(super::rights_from_mailbox(&rights).may_share, Some(allowed));
        }
    }
}
