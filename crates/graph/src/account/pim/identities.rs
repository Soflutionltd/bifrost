//! Send-as identities and the mailbox vacation (automatic replies)
//! settings, with their Graph datetime projections.

use crate::account::GraphAccount;
use crate::account::graph_error::{GraphErrorContext, into_account_error};
use crate::client::Resource;
use bifrost_types::{AccountError, AccountOperation, Identity, IdentityId, VacationConfig};
use serde::Deserialize;
use serde_json::{Value, json};
use std::time::SystemTime;

use super::common::*;

pub(crate) async fn identities_list(account: GraphAccount) -> Result<Vec<Identity>, AccountError> {
    let profile = account.client.get_profile().await.map_err(|e| {
        into_account_error(
            e,
            GraphErrorContext::graph(AccountOperation::IdentitiesList),
        )
    })?;
    let address = profile.mail.or(profile.user_principal_name);
    let Some(address) = address else {
        return Ok(Vec::new());
    };
    Ok(vec![Identity {
        id: IdentityId(address.clone()),
        name: profile.display_name.unwrap_or_default(),
        address,
        signature_text: None,
        signature_html: None,
        reply_to: None,
        is_default: true,
    }])
}

pub(crate) async fn vacation_get(
    account: GraphAccount,
) -> Result<Option<VacationConfig>, AccountError> {
    let path = format!(
        "{}/mailboxSettings?$select=automaticRepliesSetting",
        account.client.api_path_prefix()
    );
    let settings: MailboxSettings = account
        .client
        .get_json(Resource::Outlook, &path)
        .await
        .map_err(|e| {
            into_account_error(e, GraphErrorContext::graph(AccountOperation::VacationGet))
        })?;
    Ok(settings.automatic_replies_setting.map(vacation_from_graph))
}

pub(crate) async fn vacation_set(
    account: GraphAccount,
    config: VacationConfig,
) -> Result<(), AccountError> {
    let status = if config.is_enabled {
        if config.starts_at.is_some() || config.ends_at.is_some() {
            "scheduled"
        } else {
            "alwaysEnabled"
        }
    } else {
        "disabled"
    };
    let body = config
        .body_html
        .clone()
        .or(config.body_text.clone())
        .unwrap_or_default();
    let setting = json!({
        "automaticRepliesSetting": {
            "status": status,
            "internalReplyMessage": body,
            "externalReplyMessage": body,
            "externalAudience": "all",
            "scheduledStartDateTime": graph_datetime_or_default(config.starts_at),
            "scheduledEndDateTime": graph_datetime_or_default(config.ends_at)
        }
    });
    let path = format!("{}/mailboxSettings", account.client.api_path_prefix());
    account
        .client
        .patch(
            Resource::Outlook,
            &path,
            &setting,
            bifrost_net::ReplayPolicy::Idempotent,
        )
        .await
        .map_err(|e| into_account_error(e, GraphErrorContext::graph(AccountOperation::VacationSet)))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct MailboxSettings {
    pub(super) automatic_replies_setting: Option<AutomaticRepliesSetting>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AutomaticRepliesSetting {
    pub(super) status: Option<String>,
    pub(super) internal_reply_message: Option<String>,
    pub(super) external_reply_message: Option<String>,
    pub(super) scheduled_start_date_time: Option<DateTimeTimeZone>,
    pub(super) scheduled_end_date_time: Option<DateTimeTimeZone>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct DateTimeTimeZone {
    pub(super) date_time: Option<String>,
    pub(super) time_zone: Option<String>,
}

pub(super) fn vacation_from_graph(setting: AutomaticRepliesSetting) -> VacationConfig {
    let status = setting.status.unwrap_or_else(|| "disabled".to_string());
    let body_html = setting
        .internal_reply_message
        .or(setting.external_reply_message)
        .filter(|body| !body.is_empty());
    VacationConfig {
        is_enabled: !status.eq_ignore_ascii_case("disabled"),
        subject: None,
        body_text: None,
        body_html,
        starts_at: setting
            .scheduled_start_date_time
            .and_then(|dt| graph_datetime_to_system_time(&dt)),
        ends_at: setting
            .scheduled_end_date_time
            .and_then(|dt| graph_datetime_to_system_time(&dt)),
    }
}

pub(super) fn graph_datetime_or_default(time: Option<SystemTime>) -> Value {
    json!({
        "dateTime": time
            .map(system_time_naive_utc)
            .unwrap_or_else(|| "0001-01-01T00:00:00".to_string()),
        "timeZone": "UTC"
    })
}

pub(super) fn graph_datetime_to_system_time(value: &DateTimeTimeZone) -> Option<SystemTime> {
    let date_time = value.date_time.as_deref()?;
    let _time_zone = value.time_zone.as_deref().unwrap_or("UTC");
    parse_graph_datetime(date_time)
}
