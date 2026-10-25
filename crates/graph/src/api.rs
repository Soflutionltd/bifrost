use crate::client::{GraphClient, Resource};
use crate::error::GraphError;
use crate::paging::PageWalk;
use crate::types::{GraphMailFolder, GraphMessageRule, GraphProfile, ODataCollection};

impl GraphClient {
    pub(crate) async fn get_profile(&self) -> Result<GraphProfile, GraphError> {
        let prefix = self.api_path_prefix();
        self.get_json(
            Resource::Unmetered,
            &format!("{prefix}?$select=displayName,mail,userPrincipalName"),
        )
        .await
    }

    pub(crate) async fn list_mail_folders(&self) -> Result<Vec<GraphMailFolder>, GraphError> {
        let prefix = self.api_path_prefix();
        let mut folders = Vec::new();
        let mut walk = PageWalk::new("mailFolders");
        let mut next_url = Some(self.api_url(&format!(
            "{prefix}/mailFolders?$select=id,displayName,parentFolderId,childFolderCount&$top=100"
        ))?);

        while let Some(url) = next_url {
            walk.enter(&url)?;
            let page: ODataCollection<GraphMailFolder> = self.get(Resource::Outlook, &url).await?;
            folders.extend(page.value);
            next_url = self.admit_next(page.next_link.as_ref())?;
        }

        Ok(folders)
    }

    pub(crate) async fn list_mail_folders_recursive(
        &self,
    ) -> Result<Vec<GraphMailFolder>, GraphError> {
        let prefix = self.api_path_prefix();
        let mut folders = self.list_mail_folders().await?;
        let mut queue: std::collections::VecDeque<String> = folders
            .iter()
            .filter(|folder| folder.child_folder_count.unwrap_or(0) > 0)
            .map(|folder| folder.id.clone())
            .collect();
        // The descent needs its own guard, separate from the per-collection
        // page walks below. Folder parentage is server-supplied, so a cycle
        // (A claims B as a child, B claims A) or a folder reported under two
        // parents re-enqueues ids forever and grows `folders` without bound -
        // a second unbounded loop, and one no amount of pagination guarding
        // would catch.
        let mut expanded: std::collections::HashSet<String> = std::collections::HashSet::new();

        while let Some(parent_id) = queue.pop_front() {
            if !expanded.insert(parent_id.clone()) {
                continue;
            }
            let enc_parent_id = bifrost_net::url::encode_path_component(&parent_id);
            let mut walk = PageWalk::new("childFolders");
            let mut next_url = Some(self.api_url(&format!(
                "{prefix}/mailFolders/{enc_parent_id}/childFolders?$select=id,displayName,parentFolderId,childFolderCount&$top=100"
            ))?);

            while let Some(url) = next_url {
                walk.enter(&url)?;
                let page: ODataCollection<GraphMailFolder> =
                    self.get(Resource::Outlook, &url).await?;
                for folder in page.value {
                    if folder.child_folder_count.unwrap_or(0) > 0 {
                        queue.push_back(folder.id.clone());
                    }
                    folders.push(folder);
                }
                next_url = self.admit_next(page.next_link.as_ref())?;
            }
        }

        Ok(folders)
    }

    pub(crate) async fn list_message_rules(&self) -> Result<Vec<GraphMessageRule>, GraphError> {
        let prefix = self.api_path_prefix();
        let mut rules = Vec::new();
        let mut walk = PageWalk::new("messageRules");
        let mut next_url = Some(self.api_url(&format!("{prefix}/mailFolders/inbox/messageRules"))?);

        while let Some(url) = next_url {
            walk.enter(&url)?;
            let page: ODataCollection<GraphMessageRule> = self.get(Resource::Outlook, &url).await?;
            rules.extend(page.value);
            next_url = self.admit_next(page.next_link.as_ref())?;
        }

        Ok(rules)
    }

    pub(crate) async fn create_message_rule(
        &self,
        rule: &GraphMessageRule,
    ) -> Result<GraphMessageRule, GraphError> {
        let prefix = self.api_path_prefix();
        self.post(
            Resource::Outlook,
            &format!("{prefix}/mailFolders/inbox/messageRules"),
            rule,
            bifrost_net::ReplayPolicy::Never,
        )
        .await
    }

    pub(crate) async fn update_message_rule(
        &self,
        rule_id: &str,
        patch: &serde_json::Value,
    ) -> Result<GraphMessageRule, GraphError> {
        let prefix = self.api_path_prefix();
        let encoded = bifrost_net::url::encode_path_component(rule_id);
        self.patch_json(
            Resource::Outlook,
            &format!("{prefix}/mailFolders/inbox/messageRules/{encoded}"),
            patch,
            bifrost_net::ReplayPolicy::Idempotent,
        )
        .await
    }

    pub(crate) async fn delete_message_rule(&self, rule_id: &str) -> Result<(), GraphError> {
        let prefix = self.api_path_prefix();
        let encoded = bifrost_net::url::encode_path_component(rule_id);
        self.delete(
            Resource::Outlook,
            &format!("{prefix}/mailFolders/inbox/messageRules/{encoded}"),
            bifrost_net::ReplayPolicy::Never,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::client::{GraphClient, ScriptedRestResponse};
    use serde_json::json;

    fn folder_page(id: &str, children: u32, next: Option<&str>) -> ScriptedRestResponse {
        let mut body = json!({
            "value": [{
                "id": id,
                "displayName": id,
                "childFolderCount": children,
            }]
        });
        if let Some(link) = next {
            body["@odata.nextLink"] = json!(link);
        }
        ScriptedRestResponse::json(reqwest::StatusCode::OK, body)
    }

    /// A server that keeps handing back the SAME `nextLink` is refused rather
    /// than walked forever.
    ///
    /// This loop had no bound of any kind - `while let Some(url) = next_url`
    /// with `next_url = page.next_link` - so a server repeating a link spun it
    /// indefinitely while the accumulating `Vec` grew without limit.
    #[tokio::test]
    async fn a_repeated_next_link_refuses_the_folder_walk() {
        let client = GraphClient::new("token");
        let link = "https://graph.microsoft.com/v1.0/me/mailFolders?page=2";
        client.script_rest([
            folder_page("a", 0, Some(link)),
            folder_page("b", 0, Some(link)),
            folder_page("c", 0, Some(link)),
        ]);

        let error = client
            .list_mail_folders()
            .await
            .expect_err("a repeated page link must refuse the walk");
        assert!(
            format!("{error:?}").contains("repeated a page link"),
            "the refusal must name the contract violation: {error:?}"
        );
    }

    /// A parentage CYCLE terminates too, and it is a different hazard from
    /// pagination: every page is well-formed and no link repeats, but the
    /// folders name each other as children so ids are re-enqueued forever. No
    /// amount of page-link guarding catches this - the descent needs its own
    /// visited set.
    #[tokio::test]
    async fn a_folder_parentage_cycle_terminates_the_recursive_walk() {
        let client = GraphClient::new("token");
        // Root listing yields "a", which claims a child. Every childFolders
        // request then answers with a folder that also claims a child, and the
        // ids repeat - a cycle that re-enqueues forever without the guard.
        client.script_rest([
            folder_page("a", 1, None),
            folder_page("b", 1, None),
            folder_page("a", 1, None),
            folder_page("b", 1, None),
        ]);

        let folders = client
            .list_mail_folders_recursive()
            .await
            .expect("a cycle must terminate, not loop");
        assert!(
            folders.len() <= 4,
            "the visited set must stop re-expanding a folder: {folders:?}"
        );
    }
}
