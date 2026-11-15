//! Gmail settings operations.

use super::super::flags::{ARCHIVE_ID, is_archive_id};
use super::super::scopes::{ScopeCache, invalidate_scope_snapshot, refresh_scope_snapshot};
use super::super::{error, flags};
use super::{account_error_for, unsupported};
use crate::client::GmailClient;
use crate::types::{GmailLabel, GmailLabelColor, GmailVacationSettings};
use bifrost_types::{
    AccountError, AccountFuture, Address, Container, ContainerId, ContainerKind, ContainerList,
    ContainerStyle, FolderRole, Identity, IdentityId, IdentityPatch, ProtocolKind, Provenance,
    VacationConfig,
};
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) fn containers_list(
    client: Arc<GmailClient>,
    cache: ScopeCache,
) -> AccountFuture<Result<ContainerList, AccountError>> {
    Box::pin(async move {
        let snapshot = refresh_scope_snapshot(&client, &cache)
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::containers_list()))?;
        let mut containers = Vec::with_capacity(snapshot.labels.len() + 1);
        containers.push(archive_container());
        containers.extend(snapshot.labels.iter().map(container_from_label));
        // Single-namespace enumeration: one `labels.list` answers for
        // the whole account, so there is never a skipped namespace.
        Ok(ContainerList::complete(containers))
    })
}

pub(crate) fn container_create(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    kind: ContainerKind,
    name: String,
    parent: Option<ContainerId>,
    style: Option<ContainerStyle>,
) -> AccountFuture<Result<ContainerId, AccountError>> {
    Box::pin(async move {
        if parent.is_some() || !matches!(kind, ContainerKind::Label) {
            return Err(unsupported(
                bifrost_types::AccountOperation::ContainerCreate,
            ));
        }
        // Gmail's create takes (text_color, background_color).
        let color = style
            .as_ref()
            .map(|s| (s.color_fg.as_str(), s.color_bg.as_str()));
        // A transport error can arrive after Gmail applied the mutation, so
        // invalidate after every dispatched attempt, not only after a 2xx.
        let result = client.create_label(&name, color).await;
        invalidate_scope_snapshot(&cache).await;
        let label = result.map_err(|e| {
            account_error_for(
                e,
                error::GmailErrorContext::container(
                    bifrost_types::AccountOperation::ContainerCreate,
                ),
            )
        })?;
        Ok(ContainerId(label.id))
    })
}

pub(crate) fn container_rename(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    container: ContainerId,
    name: String,
    style: Option<ContainerStyle>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        if is_archive_id(&container.0) {
            return Err(unsupported(
                bifrost_types::AccountOperation::ContainerRename,
            ));
        }
        // `update_label`'s color is `Option<Option<(text, bg)>>`: outer
        // `None` leaves the color untouched, `Some(Some(..))` recolors.
        // A recolor with no name change still rides this rename path.
        let color = style
            .as_ref()
            .map(|s| Some((s.color_fg.as_str(), s.color_bg.as_str())));
        // A transport error can arrive after Gmail applied the mutation, so
        // invalidate after every dispatched attempt, not only after a 2xx.
        let result = client.update_label(&container.0, Some(&name), color).await;
        invalidate_scope_snapshot(&cache).await;
        result.map_err(|e| {
            account_error_for(
                e,
                error::GmailErrorContext::container(
                    bifrost_types::AccountOperation::ContainerRename,
                ),
            )
        })?;
        Ok(())
    })
}

pub(crate) fn container_move(
    _container: ContainerId,
    _new_parent: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContainerMove)) })
}

pub(crate) fn container_delete(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        if is_archive_id(&container.0) {
            return Err(unsupported(
                bifrost_types::AccountOperation::ContainerDelete,
            ));
        }
        // A transport error can arrive after Gmail applied the mutation, so
        // invalidate after every dispatched attempt, not only after a 2xx.
        let result = client.delete_label(&container.0).await;
        invalidate_scope_snapshot(&cache).await;
        result.map_err(|e| {
            account_error_for(
                e,
                error::GmailErrorContext::container(
                    bifrost_types::AccountOperation::ContainerDelete,
                ),
            )
        })
    })
}

pub(crate) fn identities_list(
    client: Arc<GmailClient>,
) -> AccountFuture<Result<Vec<Identity>, AccountError>> {
    Box::pin(async move {
        let identities = client
            .list_send_as()
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::identities_list()))?
            .into_iter()
            .map(|send_as| Identity {
                id: IdentityId(send_as.send_as_email.clone()),
                name: send_as.display_name.unwrap_or_default(),
                address: send_as.send_as_email,
                signature_text: None,
                signature_html: send_as.signature,
                reply_to: send_as.reply_to_address.map(Address::bare),
                is_default: send_as.is_default.unwrap_or(false),
            })
            .collect();
        Ok(identities)
    })
}

pub(crate) fn identity_update(
    client: Arc<GmailClient>,
    identity: IdentityId,
    patch: IdentityPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let body = identity_update_body(patch);
        if body.as_object().is_none_or(serde_json::Map::is_empty) {
            return Ok(());
        }
        client
            .patch_send_as(&identity.0, &body)
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::identity_update()))?;
        Ok(())
    })
}

fn identity_update_body(patch: IdentityPatch) -> serde_json::Value {
    let mut body = json!({});
    if let Some(name) = patch.name {
        body["displayName"] = json!(name);
    }
    if let Some(signature) = patch.signature_html {
        body["signature"] = signature.map_or(serde_json::Value::Null, serde_json::Value::String);
    } else if let Some(signature) = patch.signature_text {
        // Gmail stores this field as HTML. Preserve the shared patch's
        // plain-text semantics by escaping content and converting line breaks.
        body["signature"] = signature.map_or(serde_json::Value::Null, |text| {
            serde_json::Value::String(signature_text_as_html(&text))
        });
    }
    if let Some(reply_to) = patch.reply_to {
        body["replyToAddress"] =
            reply_to.map_or(serde_json::Value::Null, |addr| json!(addr.address));
    }
    if let Some(is_default) = patch.is_default {
        body["isDefault"] = json!(is_default);
    }
    body
}

fn signature_text_as_html(text: &str) -> String {
    let mut html = String::with_capacity(text.len());
    let mut characters = text.chars().peekable();
    while let Some(character) = characters.next() {
        match character {
            '&' => html.push_str("&amp;"),
            '<' => html.push_str("&lt;"),
            '>' => html.push_str("&gt;"),
            '"' => html.push_str("&quot;"),
            '\'' => html.push_str("&#39;"),
            '\r' => {
                if characters.peek() == Some(&'\n') {
                    characters.next();
                }
                html.push_str("<br>");
            }
            '\n' => html.push_str("<br>"),
            other => html.push(other),
        }
    }
    html
}

pub(crate) fn vacation_get(
    client: Arc<GmailClient>,
) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
    Box::pin(async move {
        let settings = client
            .get_vacation()
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::vacation_get()))?;
        Ok(Some(VacationConfig {
            is_enabled: settings.enable_auto_reply.unwrap_or(false),
            subject: settings.response_subject,
            body_text: settings.response_body_plain_text,
            body_html: settings.response_body_html,
            starts_at: settings
                .start_time
                .as_deref()
                .and_then(system_time_from_millis),
            ends_at: settings
                .end_time
                .as_deref()
                .and_then(system_time_from_millis),
        }))
    })
}

pub(crate) fn vacation_set(
    client: Arc<GmailClient>,
    config: VacationConfig,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let settings = GmailVacationSettings {
            enable_auto_reply: Some(config.is_enabled),
            response_subject: config.subject,
            response_body_plain_text: config.body_text,
            response_body_html: config.body_html,
            start_time: config.starts_at.map(system_time_to_millis_string),
            end_time: config.ends_at.map(system_time_to_millis_string),
        };
        client
            .update_vacation(&settings)
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::vacation_set()))?;
        Ok(())
    })
}

fn archive_container() -> Container {
    // Synthetic bifrost container (Gmail models archive as the absence of
    // INBOX, not a real label), so it takes every `Container::new`
    // default: no Gmail color, not a Gmail-native system label, no ACL /
    // subscription / namespace metadata (Gmail is personal-only).
    Container::new(
        ContainerId(ARCHIVE_ID.to_string()),
        ContainerKind::Label,
        Some(FolderRole::Archive),
        Provenance {
            provider: ProtocolKind::Gmail,
            kind: ContainerKind::Label,
            native: ARCHIVE_ID.to_string(),
        },
        "Archive".to_string(),
        None,
    )
}

fn container_from_label(label: &GmailLabel) -> Container {
    let role = flags::GmailLabelId::parse(&label.id).role();
    // Gmail is personal-only and has no per-folder ACL / subscription
    // model, so namespace / owner / rights / subscription all stay at
    // their `Container::new` defaults.
    Container::new(
        ContainerId(label.id.clone()),
        ContainerKind::Label,
        role,
        Provenance {
            provider: ProtocolKind::Gmail,
            kind: ContainerKind::Label,
            native: label.id.clone(),
        },
        label.name.clone(),
        None,
    )
    .with_style(style_from_label_color(label.color.as_ref()))
    // Gmail tags far more labels as system (CATEGORY_*, IMPORTANT, CHAT,
    // ...) than ever receive a `role`, so carry the native
    // `type == "system"` bit through so the consumer can reproduce
    // Gmail's system-label-as-folder split, which `role` alone
    // (INBOX/SENT/DRAFT/TRASH/SPAM only) cannot.
    .with_system(label.is_system())
}

/// Map a Gmail label `color` object into a `ContainerStyle`. Yields
/// `None` when the label has no color or the pair is incomplete; Gmail
/// always emits both members when it emits the object at all, but we
/// stay defensive against a partial payload.
fn style_from_label_color(color: Option<&GmailLabelColor>) -> Option<ContainerStyle> {
    let color = color?;
    let bg = color.background_color.clone()?;
    let fg = color.text_color.clone()?;
    Some(ContainerStyle {
        color_bg: bg,
        color_fg: fg,
    })
}

fn system_time_to_millis_string(time: SystemTime) -> String {
    let millis = time
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis();
    millis.to_string()
}

fn system_time_from_millis(value: &str) -> Option<SystemTime> {
    value
        .parse::<u64>()
        .ok()
        .map(|millis| UNIX_EPOCH + Duration::from_millis(millis))
}

#[cfg(test)]
mod tests {
    use super::super::super::flags::LABEL_INBOX;
    use super::super::super::scopes::{labels_for_flags, snapshot};
    use super::super::test_support::cache_with_label;
    use super::super::test_support::ok_json;
    use super::super::test_support::scripted_client;
    use super::*;
    use bifrost_net::test_support::Canned;
    use bifrost_types::FlagOp;
    use bytes::Bytes;
    use reqwest::StatusCode;
    use std::collections::HashSet;

    #[tokio::test]
    async fn creating_a_label_invalidates_the_vocabulary_cache() {
        let (client, script) = scripted_client(vec![
            ok_json(br#"{"id":"Label_2","name":"New","type":"user"}"#),
            ok_json(
                br#"{"labels":[
                {"id":"Label_1","name":"Work","type":"user"},
                {"id":"Label_2","name":"New","type":"user"}
            ]}"#,
            ),
        ]);
        let cache = cache_with_label();

        let id = container_create(
            Arc::clone(&client),
            Arc::clone(&cache),
            ContainerKind::Label,
            "New".to_string(),
            None,
            None,
        )
        .await
        .expect("label creation succeeds");

        assert_eq!(id.0, "Label_2");
        assert!(snapshot(&cache).is_stale());
        let labels = labels_for_flags(&client, &cache)
            .await
            .expect("the next vocabulary read refreshes");
        let op = FlagOp::Add(HashSet::from(["$gmail-label:Label_2:New".to_string()]));
        let patch = flags::translate_flag_op(&op, &labels);
        assert!(patch.unsupported_flags.is_empty());
        assert_eq!(patch.add_label_ids, vec!["Label_2".to_string()]);
        assert_eq!(script.requests().len(), 2);
    }

    #[tokio::test]
    async fn renaming_a_label_invalidates_the_vocabulary_cache() {
        let (client, script) = scripted_client(vec![
            ok_json(br#"{"id":"Label_1","name":"Renamed","type":"user"}"#),
            ok_json(
                br#"{"labels":[
                {"id":"Label_1","name":"Renamed","type":"user"}
            ]}"#,
            ),
        ]);
        let cache = cache_with_label();

        container_rename(
            Arc::clone(&client),
            Arc::clone(&cache),
            ContainerId("Label_1".to_string()),
            "Renamed".to_string(),
            None,
        )
        .await
        .expect("label rename succeeds");

        assert!(snapshot(&cache).is_stale());
        let labels = labels_for_flags(&client, &cache)
            .await
            .expect("the next vocabulary read refreshes");
        assert_eq!(labels[0].name, "Renamed");
        assert_eq!(script.requests().len(), 2);
    }

    #[tokio::test]
    async fn deleting_a_label_invalidates_the_vocabulary_cache() {
        let (client, script) = scripted_client(vec![
            Canned::Response {
                status: StatusCode::NO_CONTENT,
                headers: reqwest::header::HeaderMap::new(),
                body: Bytes::new(),
            },
            ok_json(br#"{"labels":[]}"#),
        ]);
        let cache = cache_with_label();

        container_delete(
            Arc::clone(&client),
            Arc::clone(&cache),
            ContainerId("Label_1".to_string()),
        )
        .await
        .expect("label deletion succeeds");

        assert!(snapshot(&cache).is_stale());
        assert!(
            labels_for_flags(&client, &cache)
                .await
                .expect("the next vocabulary read refreshes")
                .is_empty()
        );
        assert_eq!(script.requests().len(), 2);
    }

    #[test]
    fn plain_text_identity_signatures_are_escaped_and_keep_line_breaks() {
        assert_eq!(
            signature_text_as_html("A <B> & \"C\" 'D'\r\nnext\n"),
            "A &lt;B&gt; &amp; &quot;C&quot; &#39;D&#39;<br>next<br>"
        );

        let mut patch = IdentityPatch::default();
        patch.signature_text = Some(Some("A <B>\n& C".to_string()));
        let body = identity_update_body(patch);
        assert_eq!(body["signature"].as_str(), Some("A &lt;B&gt;<br>&amp; C"));
    }

    #[test]
    fn an_explicit_html_identity_signature_takes_precedence() {
        let mut patch = IdentityPatch::default();
        patch.signature_html = Some(Some("<b>trusted HTML</b>".to_string()));
        patch.signature_text = Some(Some("plain text".to_string()));

        let body = identity_update_body(patch);
        assert_eq!(body["signature"].as_str(), Some("<b>trusted HTML</b>"));
    }

    #[test]
    fn container_roles_map_gmail_system_labels() {
        let label = GmailLabel {
            id: LABEL_INBOX.to_string(),
            name: "Inbox".to_string(),
            label_type: Some(crate::types::GmailLabelType::System),
            color: None,
        };
        let container = container_from_label(&label);
        assert_eq!(container.role, Some(FolderRole::Inbox));
        assert_eq!(container.native_id(), LABEL_INBOX);
        assert!(matches!(container.kind, ContainerKind::Label));
        let lower = GmailLabel {
            id: "inbox".into(),
            ..label
        };
        assert_eq!(container_from_label(&lower).role, Some(FolderRole::Inbox));
    }

    #[test]
    fn user_label_color_round_trips_into_container_style() {
        // Representative Gmail user-label payload carrying a color.
        let raw = r##"{
            "id": "Label_42",
            "name": "Work",
            "type": "user",
            "color": { "backgroundColor": "#fb4c2f", "textColor": "#ffffff" }
        }"##;
        let label: GmailLabel = serde_json::from_str(raw).expect("parse label");

        let container = container_from_label(&label);
        let style = container.style.expect("user label color carries a style");
        assert_eq!(style.color_bg, "#fb4c2f");
        assert_eq!(style.color_fg, "#ffffff");
        // A user label is not a Gmail system label.
        assert!(!container.system);
    }

    #[test]
    fn system_label_flagged_system_and_user_label_not() {
        let system = GmailLabel {
            id: "CATEGORY_PROMOTIONS".to_string(),
            name: "Promotions".to_string(),
            label_type: Some(crate::types::GmailLabelType::System),
            color: None,
        };
        let user = GmailLabel {
            id: "Label_42".to_string(),
            name: "Work".to_string(),
            label_type: Some(crate::types::GmailLabelType::User),
            color: None,
        };
        // A CATEGORY_* label has no `role` yet is a Gmail system label:
        // exactly the split that `system` carries and `role` cannot.
        let system_container = container_from_label(&system);
        assert!(system_container.system);
        assert_eq!(system_container.role, None);
        assert!(!container_from_label(&user).system);
        // An uncolored label yields no style.
        assert!(container_from_label(&user).style.is_none());
    }

    #[test]
    fn archive_container_is_explicit() {
        let container = archive_container();
        assert_eq!(container.id.0, ARCHIVE_ID);
        assert_eq!(container.role, Some(FolderRole::Archive));
        assert!(container.style.is_none());
        assert!(!container.system);
    }
}
