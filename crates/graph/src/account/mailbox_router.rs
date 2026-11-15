//! Mailbox routing, budgets and the per-mailbox Trash cache.
use super::{foreign, scope};
use crate::client::{GraphClient, Resource};
use bifrost_types::{CursorScope, ObjectId};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub(crate) struct MailboxRouter {
    primary: GraphClient,
    shared: Arc<HashMap<String, GraphClient>>,
    trash: Arc<RwLock<HashMap<String, String>>>,
}
impl MailboxRouter {
    pub(crate) fn new(primary: GraphClient, mailboxes: &[String]) -> Self {
        let shared = Arc::new(shared_clients_map(&primary, mailboxes));
        Self {
            primary,
            shared,
            trash: Arc::default(),
        }
    }
    pub(crate) fn primary(&self) -> &GraphClient {
        &self.primary
    }
    pub(crate) fn shared(&self) -> &HashMap<String, GraphClient> {
        &self.shared
    }
    pub(crate) fn shared_client(&self, mailbox: &str) -> Option<&GraphClient> {
        shared_client(&self.shared, mailbox)
    }
    pub(crate) fn set_shared_mailboxes(&mut self, mailboxes: &[String]) {
        self.shared = Arc::new(shared_clients_map(&self.primary, mailboxes));
    }
    #[cfg(test)]
    pub(crate) fn set_shared_clients(&mut self, clients: HashMap<String, GraphClient>) {
        self.shared = Arc::new(clients);
    }
    pub(crate) fn metered(&self) -> (Self, crate::client::ByteTally) {
        let (primary, tally) = self.primary.metered();
        let shared = self
            .shared
            .iter()
            .map(|(key, client)| (key.clone(), client.with_tally(tally.clone())))
            .collect();
        (
            Self {
                primary,
                shared: Arc::new(shared),
                trash: Arc::clone(&self.trash),
            },
            tally,
        )
    }
    pub(crate) async fn cached_trash(&self, owner: Option<&str>) -> Option<String> {
        self.trash
            .read()
            .await
            .get(owner.unwrap_or_default())
            .cloned()
    }
    pub(crate) async fn remember_trash(&self, owner: Option<&str>, native: String) {
        self.trash
            .write()
            .await
            .insert(owner.unwrap_or_default().to_owned(), native);
    }
    /// Select the `GraphClient` for an owning mailbox decoded from a
    /// foreign-encoded message/blob id. `None` (a primary-mailbox item)
    /// routes through `/me`; a configured shared mailbox routes through
    /// its `/users/{mailbox}` client. An unconfigured foreign owner is a
    /// stale account configuration, never a primary-mailbox id.
    pub(crate) fn client_for_owner(
        &self,
        owner: Option<&str>,
    ) -> Result<&GraphClient, crate::error::GraphError> {
        match owner {
            None => Ok(&self.primary),
            Some(mailbox) => shared_client(&self.shared, mailbox).ok_or_else(|| {
                crate::error::GraphError::Configuration {
                    message: format!("shared mailbox not configured on this account: {mailbox}"),
                }
            }),
        }
    }

    /// The Outlook mailbox budget a REST request for `owner` is charged to:
    /// the budget of the very client `client_for_owner` routes it through,
    /// so the declaration and the stamp the REST funnel puts on that
    /// client's throttles are one function of one client. `None` for an
    /// unconfigured owner, which never reaches the wire.
    pub(crate) fn budget_for_owner(
        &self,
        owner: Option<&str>,
    ) -> Option<bifrost_types::ThrottleBudget> {
        self.client_for_owner(owner)
            .ok()
            .map(GraphClient::mailbox_budget)
    }

    /// The mailbox budget a REST request for message `id` is charged to.
    /// A public-folder item is read over EWS, whose throttles carry no
    /// mailbox budget, so it declares none.
    pub(crate) fn budget_for_object(&self, id: &ObjectId) -> Option<bifrost_types::ThrottleBudget> {
        let parsed = foreign::parse_message_id(id);
        if parsed.public_folder().is_some() {
            return None;
        }
        self.budget_for_owner(parsed.owner())
    }

    /// Keep one owner per subrequest; the REST funnel merges final keys
    /// before applying the per-mailbox batch weight.
    pub(crate) fn batch_resource(
        &self,
        ids: &[ObjectId],
    ) -> Result<Resource, crate::error::GraphError> {
        let clients = ids
            .iter()
            .map(|id| {
                let parsed = foreign::parse_message_id(id);
                self.client_for_owner(parsed.owner()).cloned()
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Resource::Batch(clients))
    }

    /// The mailbox budget a drive of `scope` is charged to. Use the same
    /// scope admission and client routing as sync. Public-folder polling
    /// uses EWS rather than a REST mailbox budget.
    pub(crate) fn budget_for_scope(
        &self,
        scope: &CursorScope,
    ) -> Option<bifrost_types::ThrottleBudget> {
        let parsed = scope::GraphScope::parse(scope).ok()?;
        if matches!(parsed, scope::GraphScope::PublicFolder(_)) {
            return None;
        }
        self.client_for_owner(parsed.owner())
            .ok()
            .map(GraphClient::mailbox_budget)
    }
}

/// Build the `/users/{id}`-keyed foreign-mailbox client map from a list
/// of routing keys (SMTP addresses or user ids), dropping empty keys. The
/// map key is the exact string later looked up by `client_for_scope` /
/// `client_for_owner` and minted into foreign scope/owner tags by
/// `discover_cursor_scopes`, so seeding a key here is self-consistent with
/// every routing site.
///
/// `with_shared_mailbox("")` is constructible, and an empty key used to
/// install a client whose prefix is the malformed `/users/` - which every
/// foreign id with an empty owner then routed to, producing an opaque
/// remote 400 for what is a local configuration error. Dropping it here
/// makes that id fall to the same local `Configuration` rejection every
/// other unconfigured owner gets. `merge_shared_mailboxes` already applies
/// the identical rule on the Autodiscover leg; this is the config leg,
/// which had no such filter.
fn shared_clients_map(client: &GraphClient, mailboxes: &[String]) -> HashMap<String, GraphClient> {
    mailboxes
        .iter()
        .filter(|mailbox| !mailbox.is_empty())
        // Outlook users expect one shared mailbox regardless of address case,
        // and stable ids for the mail in it. The FIRST configured spelling is
        // the mailbox's canonical spelling: it keys this map and is the owner
        // written into every id, so ids never change spelling. Later case
        // aliases fold into it; lookups (`shared_client`) are ASCII
        // case-insensitive, and the client routes and budgets in lowercase.
        .fold(HashMap::new(), |mut map, mailbox| {
            if !map
                .keys()
                .any(|existing: &String| existing.eq_ignore_ascii_case(mailbox))
            {
                map.insert(mailbox.clone(), client.for_shared_mailbox(mailbox.clone()));
            }
            map
        })
}

/// The configured client for a shared mailbox, matched ASCII
/// case-insensitively so an id or send-as address spelled differently from
/// the configuration still routes to the one mailbox.
fn shared_client<'a>(
    clients: &'a HashMap<String, GraphClient>,
    mailbox: &str,
) -> Option<&'a GraphClient> {
    clients.get(mailbox).or_else(|| {
        clients
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(mailbox))
            .map(|(_, client)| client)
    })
}
