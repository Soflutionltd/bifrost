//! Organization-directory groups: the mail-enabled groups the
//! authenticated mailbox belongs to, and their transitive member
//! expansion.
//!
//! Listing walks `GET {prefix}/memberOf/microsoft.graph.group` filtered
//! to mail-enabled groups; expansion walks
//! `GET /groups/{id}/transitiveMembers/microsoft.graph.user`, which
//! flattens nested groups and handles cycle detection server-side. Both
//! need directory-group read consent (`GroupMember.Read.All`-class), a
//! harder tenant grant than the mail scopes: an ungranted tenant
//! surfaces a real `NoPermission` error through `into_error`, exactly
//! like `directory_search` - support is protocol-level, consent is
//! per-tenant runtime state.

use bifrost_types::{
    AccountError, AccountOperation, DirectoryGroup, DirectoryGroupId, DirectoryGroupKind,
    DirectoryGroupMember, Page, ProtocolKind,
};
use serde::Deserialize;

use crate::client::Resource;
use crate::origin::AdmittedUrl;
use crate::types::ODataCollection;

use super::GraphAccount;
use super::contacts::{admit_next, decode_link_cursor, get_page, link_cursor};
use super::graph_error;

const GROUP_SELECT: &str = "id,displayName,mail,groupTypes,mailEnabled,securityEnabled";
const MEMBER_SELECT: &str = "displayName,mail,userPrincipalName";

pub(crate) async fn directory_groups_list(
    account: GraphAccount,
    page_cursor: Option<Vec<u8>>,
) -> Result<Page<DirectoryGroup>, AccountError> {
    let operation = AccountOperation::DirectoryGroupsList;
    let url = match decode_link_cursor(&account, page_cursor, operation)? {
        Some(next) => next,
        None => {
            let prefix = account.client.api_path_prefix();
            first_page(&account, &groups_list_path(&prefix), operation)?
        }
    };
    let page: ODataCollection<GraphDirectoryGroup> =
        get_page(&account, Resource::Unmetered, &url, operation).await?;
    let next_cursor = link_cursor(admit_next(&account, &page, operation)?);
    Ok(Page {
        items: page.value.into_iter().filter_map(classify_group).collect(),
        next_cursor,
        estimated_total: None,
        failed_ids: Vec::new(),
        skipped_scopes: Vec::new(),
    })
}

pub(crate) async fn directory_group_expand(
    account: GraphAccount,
    group: DirectoryGroupId,
    page_cursor: Option<Vec<u8>>,
) -> Result<Page<DirectoryGroupMember>, AccountError> {
    let operation = AccountOperation::DirectoryGroupExpand;
    let url = match decode_link_cursor(&account, page_cursor, operation)? {
        Some(next) => next,
        None => first_page(&account, &transitive_members_path(&group), operation)?,
    };
    let page: ODataCollection<GraphGroupMember> =
        get_page(&account, Resource::Unmetered, &url, operation).await?;
    let next_cursor = link_cursor(admit_next(&account, &page, operation)?);
    Ok(Page {
        items: page.value.into_iter().filter_map(member_to_row).collect(),
        next_cursor,
        estimated_total: None,
        failed_ids: Vec::new(),
        skipped_scopes: Vec::new(),
    })
}

/// A page cursor is the verbatim `@odata.nextLink` URL as bytes, the same
/// convention `contacts_list` uses, decoded and admitted the same way.
fn first_page(
    account: &GraphAccount,
    path: &str,
    operation: AccountOperation,
) -> Result<AdmittedUrl, AccountError> {
    account.client.api_url(path).map_err(|error| {
        graph_error::into_account_error(error, graph_error::GraphErrorContext::graph(operation))
    })
}

fn groups_list_path(prefix: &str) -> String {
    let filter = bifrost_net::url::encode_query_value("mailEnabled eq true");
    format!(
        "{prefix}/memberOf/microsoft.graph.group?$filter={filter}&$select={GROUP_SELECT}&$top=999"
    )
}

fn transitive_members_path(group: &DirectoryGroupId) -> String {
    let encoded = bifrost_net::url::encode_path_component(&group.0);
    format!(
        "/groups/{encoded}/transitiveMembers/microsoft.graph.user?$select={MEMBER_SELECT}&$top=999"
    )
}

/// Classify one Graph group row, dropping groups that are not
/// mail-enabled (pure security groups are not an address surface):
/// `Unified` in `groupTypes` wins, then `securityEnabled`, else a
/// classic distribution list.
fn classify_group(group: GraphDirectoryGroup) -> Option<DirectoryGroup> {
    if !group.mail_enabled.unwrap_or(false) {
        return None;
    }
    let group_types = group.group_types.as_deref().unwrap_or(&[]);
    let kind = if group_types.iter().any(|ty| ty == "Unified") {
        DirectoryGroupKind::Unified
    } else if group.security_enabled.unwrap_or(false) {
        DirectoryGroupKind::MailEnabledSecurity
    } else {
        DirectoryGroupKind::DistributionList
    };
    Some(DirectoryGroup {
        id: DirectoryGroupId(group.id),
        display_name: group.display_name.unwrap_or_default(),
        email: group.mail.filter(|mail| !mail.is_empty()),
        kind,
        provider: ProtocolKind::Graph,
    })
}

/// Project one member row, preferring `mail` over `userPrincipalName`
/// and dropping members with neither. Addresses are lowercased so the
/// consumer's email keying is case-stable.
fn member_to_row(member: GraphGroupMember) -> Option<DirectoryGroupMember> {
    let email = member
        .mail
        .as_deref()
        .filter(|mail| !mail.is_empty())
        .or_else(|| {
            member
                .user_principal_name
                .as_deref()
                .filter(|upn| !upn.is_empty())
        })?
        .to_lowercase();
    Some(DirectoryGroupMember {
        email,
        display_name: member.display_name,
    })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphDirectoryGroup {
    id: String,
    display_name: Option<String>,
    mail: Option<String>,
    group_types: Option<Vec<String>>,
    mail_enabled: Option<bool>,
    security_enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphGroupMember {
    display_name: Option<String>,
    mail: Option<String>,
    user_principal_name: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn group(
        group_types: Option<Vec<&str>>,
        mail_enabled: Option<bool>,
        security_enabled: Option<bool>,
    ) -> GraphDirectoryGroup {
        GraphDirectoryGroup {
            id: "g1".to_string(),
            display_name: Some("Engineering".to_string()),
            mail: Some("eng@contoso.test".to_string()),
            group_types: group_types.map(|types| types.into_iter().map(str::to_string).collect()),
            mail_enabled,
            security_enabled,
        }
    }

    #[test]
    fn classify_unified_group() {
        let row = classify_group(group(Some(vec!["Unified"]), Some(true), Some(false)))
            .expect("mail-enabled group classifies");
        assert_eq!(row.kind, DirectoryGroupKind::Unified);
        assert_eq!(row.id, DirectoryGroupId("g1".to_string()));
        assert_eq!(row.display_name, "Engineering");
        assert_eq!(row.email.as_deref(), Some("eng@contoso.test"));
        assert_eq!(row.provider, ProtocolKind::Graph);
    }

    #[test]
    fn classify_distribution_list_and_security_group() {
        // No Unified marker, not security-enabled -> distribution list.
        // `groupTypes` absent entirely (legacy DLs) takes the same path.
        for types in [Some(vec![]), None] {
            let row = classify_group(group(types, Some(true), Some(false)))
                .expect("mail-enabled group classifies");
            assert_eq!(row.kind, DirectoryGroupKind::DistributionList);
        }
        let row = classify_group(group(Some(vec![]), Some(true), Some(true)))
            .expect("mail-enabled group classifies");
        assert_eq!(row.kind, DirectoryGroupKind::MailEnabledSecurity);
    }

    #[test]
    fn classify_drops_mail_disabled_group() {
        assert!(classify_group(group(Some(vec![]), Some(false), Some(true))).is_none());
        assert!(classify_group(group(Some(vec![]), None, Some(true))).is_none());
    }

    #[test]
    fn member_prefers_mail_lowercased_over_upn() {
        let member = GraphGroupMember {
            display_name: Some("Alice".to_string()),
            mail: Some("Alice@Contoso.test".to_string()),
            user_principal_name: Some("alice@contoso.onmicrosoft.test".to_string()),
        };
        let row = member_to_row(member).expect("member with mail resolves");
        assert_eq!(row.email, "alice@contoso.test");
        assert_eq!(row.display_name.as_deref(), Some("Alice"));
    }

    #[test]
    fn member_falls_back_to_upn_and_drops_unresolvable() {
        let upn_only = GraphGroupMember {
            display_name: None,
            mail: Some(String::new()),
            user_principal_name: Some("Bob@Contoso.test".to_string()),
        };
        let row = member_to_row(upn_only).expect("empty mail falls back to UPN");
        assert_eq!(row.email, "bob@contoso.test");

        let unresolvable = GraphGroupMember {
            display_name: Some("Ghost".to_string()),
            mail: None,
            user_principal_name: None,
        };
        assert!(member_to_row(unresolvable).is_none());
    }

    #[test]
    fn groups_list_path_filters_mail_enabled_under_prefix() {
        let path = groups_list_path("/me");
        assert!(path.starts_with("/me/memberOf/microsoft.graph.group?$filter="));
        assert!(path.contains(&bifrost_net::url::encode_query_value("mailEnabled eq true")));
        assert!(path.contains(&format!("$select={GROUP_SELECT}")));
    }

    #[test]
    fn transitive_members_path_casts_to_user_and_encodes_id() {
        let path = transitive_members_path(&DirectoryGroupId("a b".to_string()));
        assert!(path.starts_with("/groups/a%20b/transitiveMembers/microsoft.graph.user?"));
        assert!(path.contains(&format!("$select={MEMBER_SELECT}")));
    }
}
