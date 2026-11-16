//! Identity, vacation and quota settings.

use super::body::{address_from_jmap, address_to_jmap};
use super::checked_timestamp;
use super::unix_to_system_time;
use super::{MailAccount, QuotaAccount, SubmissionAccount, VacationAccount, to_acct_err};
use crate::core::transport::HttpTransport;
use crate::identity::{IdentityGet, IdentityId as JmapIdentityId, IdentitySet};
use crate::quota::{Property as QuotaProperty, QuotaGet};
use crate::sync::error::answer_error;
use crate::vacation_response::{VacationResponseGet, VacationResponseId, VacationResponseSet};
use bifrost_types::{AccountError, AccountFuture, AccountOperation, QuotaInfo, VacationConfig};

pub(crate) fn identities_list<T: HttpTransport>(
    submission: Option<SubmissionAccount<T>>,
) -> AccountFuture<Result<Vec<bifrost_types::Identity>, AccountError>> {
    Box::pin(async move {
        let Some(account) = submission else {
            return Err(crate::sync::error::unsupported_error(
                AccountOperation::IdentitiesList,
                None,
                bifrost_types::UnsupportedReason::SubmissionUnavailable,
                "JMAP submission capability not available",
            ));
        };
        let response = account
            .call(IdentityGet::new().properties([
                crate::identity::Property::Id,
                crate::identity::Property::Name,
                crate::identity::Property::Email,
                crate::identity::Property::ReplyTo,
                crate::identity::Property::TextSignature,
                crate::identity::Property::HtmlSignature,
            ]))
            .await
            .map_err(to_acct_err(AccountOperation::IdentitiesList))?;
        let mut identities = Vec::new();
        for mut identity in response.into_list() {
            let id = identity.take_id().into_string();
            let reply_to = identity
                .reply_to()
                .and_then(|values| values.first())
                .map(address_from_jmap);
            identities.push(bifrost_types::Identity {
                id: bifrost_types::IdentityId(id),
                name: identity.name().unwrap_or("").to_string(),
                address: identity.email().unwrap_or("").to_string(),
                signature_text: identity.text_signature().map(str::to_string),
                signature_html: identity.html_signature().map(str::to_string),
                reply_to,
                // JMAP has no default-identity concept (identity_update
                // refuses is_default for the same reason) and Identity/get
                // order is server-arbitrary, so no row is reported default.
                is_default: false,
            });
        }
        Ok(identities)
    })
}

#[cfg_attr(test, derive(Debug))]
pub(super) struct SendingIdentity {
    pub(super) id: String,
    pub(super) name: Option<String>,
    pub(super) email: String,
}

/// Select a concrete foreign identity. JMAP does not designate a default
/// identity, so the first concrete address returned by the server is the
/// documented best-effort convention. Wildcard identities cannot be used in
/// an RFC 5322 From header.
pub(super) async fn foreign_sending_identity<T: HttpTransport>(
    mail: &MailAccount<T>,
    requested: Option<&bifrost_types::IdentityId>,
) -> Result<SendingIdentity, AccountError> {
    let get = IdentityGet::new().properties([
        crate::identity::Property::Id,
        crate::identity::Property::Name,
        crate::identity::Property::Email,
    ]);
    let get = match requested {
        Some(requested) => get.ids([JmapIdentityId::new(requested.0.clone())]),
        None => get,
    };
    let submission = mail
        .view::<crate::core::capability::Submission>()
        .map_err(to_acct_err(AccountOperation::Send))?;
    let response = submission
        .call(get)
        .await
        .map_err(to_acct_err(AccountOperation::Send))?;
    select_concrete_identity(
        response.into_list(),
        if requested.is_some() {
            "requested foreign sending identity is absent or has no concrete email"
        } else {
            "foreign account advertises no sending identity"
        },
    )
}

/// Resolve only a caller-selected primary identity for scheduled envelope
/// fallback. JMAP has no default identity, and Identity/get order is not a
/// selection contract.
pub(super) async fn primary_sending_identity<T: HttpTransport>(
    mail: &MailAccount<T>,
    requested: &bifrost_types::IdentityId,
) -> Result<SendingIdentity, AccountError> {
    let submission = mail
        .view::<crate::core::capability::Submission>()
        .map_err(to_acct_err(AccountOperation::Send))?;
    let response = submission
        .call(
            IdentityGet::new()
                .ids([JmapIdentityId::new(requested.0.clone())])
                .properties([
                    crate::identity::Property::Id,
                    crate::identity::Property::Name,
                    crate::identity::Property::Email,
                ]),
        )
        .await
        .map_err(to_acct_err(AccountOperation::Send))?;
    select_concrete_identity(
        response.into_list(),
        "requested sending identity is absent or has no concrete email",
    )
}

/// Pure identity selection: first row with a concrete (non-empty,
/// non-wildcard) email. Callers provide the detail for an empty or
/// wildcard-only result, since a by-id read and a foreign account listing
/// describe different failures.
pub(super) fn select_concrete_identity(
    rows: Vec<crate::identity::Identity>,
    missing_detail: &str,
) -> Result<SendingIdentity, AccountError> {
    let mut concrete = rows.into_iter().filter_map(|mut identity| {
        let id = identity.take_id().into_string();
        let email = identity.email()?.to_string();
        (!id.is_empty() && !email.is_empty() && !email.contains('*')).then(|| SendingIdentity {
            id,
            name: identity.name().map(str::to_string),
            email,
        })
    });
    if let Some(identity) = concrete.next() {
        return Ok(identity);
    }
    Err(crate::sync::error::unsupported_error(
        AccountOperation::Send,
        None,
        bifrost_types::UnsupportedReason::IdentitySelection,
        missing_detail,
    ))
}

pub(crate) fn identity_update<T: HttpTransport>(
    submission: Option<SubmissionAccount<T>>,
    identity: bifrost_types::IdentityId,
    patch: bifrost_types::IdentityPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let Some(account) = submission else {
            return Err(crate::sync::error::unsupported_error(
                AccountOperation::IdentityUpdate,
                None,
                bifrost_types::UnsupportedReason::SubmissionUnavailable,
                "JMAP submission capability not available",
            ));
        };
        if patch.is_default.is_some() {
            return Err(crate::sync::error::unsupported_error(
                AccountOperation::IdentityUpdate,
                None,
                bifrost_types::UnsupportedReason::RequestShape,
                "JMAP does not support setting default identity",
            ));
        }
        let mut set = IdentitySet::new();
        let identity = JmapIdentityId::new(identity.0);
        let item = set.update(identity.clone());
        if let Some(name) = patch.name {
            item.name(name);
        }
        if let Some(signature) = patch.signature_text {
            if let Some(value) = signature {
                item.text_signature(value);
            } else {
                item.text_signature("");
            }
        }
        if let Some(signature) = patch.signature_html {
            if let Some(value) = signature {
                item.html_signature(value);
            } else {
                item.html_signature("");
            }
        }
        if let Some(reply_to) = patch.reply_to {
            let values = reply_to.into_iter().map(address_to_jmap);
            item.reply_to(Some(values));
        }
        let mut response = account
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::IdentityUpdate))?;
        // An Outlook user must not see settings saved without confirmation
        // for the requested id; silence is an unanswered write.
        response
            .updated(&identity)
            .map(drop)
            .map_err(answer_error(AccountOperation::IdentityUpdate))
    })
}

pub(crate) fn vacation_get<T: HttpTransport>(
    vacation: Option<VacationAccount<T>>,
) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
    Box::pin(async move {
        let Some(account) = vacation else {
            return Err(crate::sync::error::unsupported_error(
                AccountOperation::VacationGet,
                None,
                bifrost_types::UnsupportedReason::CapabilityUnavailable,
                "JMAP vacation capability not available",
            ));
        };
        let mut response = account
            .call(VacationResponseGet::new().ids([VacationResponseId::new("singleton")]))
            .await
            .map_err(to_acct_err(AccountOperation::VacationGet))?;
        let Some(vacation) = response.pop() else {
            return Ok(None);
        };
        Ok(Some(VacationConfig {
            is_enabled: vacation.is_enabled(),
            subject: vacation.subject().map(str::to_string),
            body_text: vacation.text_body().map(str::to_string),
            body_html: vacation.html_body().map(str::to_string),
            starts_at: vacation.from_date().and_then(unix_to_system_time),
            ends_at: vacation.to_date().and_then(unix_to_system_time),
        }))
    })
}

pub(crate) fn vacation_set<T: HttpTransport>(
    vacation: Option<VacationAccount<T>>,
    config: VacationConfig,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let Some(account) = vacation else {
            return Err(crate::sync::error::unsupported_error(
                AccountOperation::VacationSet,
                None,
                bifrost_types::UnsupportedReason::CapabilityUnavailable,
                "JMAP vacation capability not available",
            ));
        };
        let mut set = VacationResponseSet::new();
        let patch = set.update(VacationResponseId::new("singleton"));
        patch.is_enabled(config.is_enabled);
        patch.subject(config.subject);
        patch.text_body(config.body_text);
        patch.html_body(config.body_html);
        patch.from_date(
            config
                .starts_at
                .map(|time| checked_timestamp(time, AccountOperation::VacationSet))
                .transpose()?,
        );
        patch.to_date(
            config
                .ends_at
                .map(|time| checked_timestamp(time, AccountOperation::VacationSet))
                .transpose()?,
        );
        let mut response = account
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::VacationSet))?;
        // An Outlook user must not see settings saved without confirmation
        // for the requested id; silence is an unanswered write.
        response
            .updated(&VacationResponseId::new("singleton"))
            .map(drop)
            .map_err(answer_error(AccountOperation::VacationSet))
    })
}

pub(crate) fn quota_get<T: HttpTransport>(
    quota: Option<QuotaAccount<T>>,
) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
    Box::pin(async move {
        let Some(account) = quota else {
            return Err(crate::sync::error::unsupported_error(
                AccountOperation::QuotaGet,
                None,
                bifrost_types::UnsupportedReason::CapabilityUnavailable,
                "JMAP quota capability not available",
            ));
        };
        let response = account
            .call(QuotaGet::new().properties([
                QuotaProperty::ResourceType,
                QuotaProperty::Used,
                QuotaProperty::HardLimit,
            ]))
            .await
            .map_err(to_acct_err(AccountOperation::QuotaGet))?;
        let mut fallback = None;
        for quota in response.into_list() {
            let info = QuotaInfo {
                used_bytes: quota.used().unwrap_or(0),
                total_bytes: quota.hard_limit(),
            };
            if matches!(quota.resource_type(), Some("octets" | "storage")) {
                return Ok(Some(info));
            }
            fallback.get_or_insert(info);
        }
        Ok(fallback)
    })
}
