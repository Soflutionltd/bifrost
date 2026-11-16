//! One interpretation of a Graph sync collection, retained for the whole operation.

use bifrost_types::{CursorScope, FolderId, MembershipScope, ObjectId, ObjectType, ThreadId};

use super::GraphAccount;
use super::cursor::{CursorError, GraphCursorKind};
use super::foreign::{self, ParsedFolder};

#[derive(Debug, Clone)]
pub(crate) enum GraphScope {
    Mail {
        folder: FolderId,
        route: ParsedFolder,
    },
    Events {
        calendar: FolderId,
    },
    Contacts {
        folder: FolderId,
    },
    PublicFolder(FolderId),
}

impl GraphScope {
    pub(crate) fn parse(scope: &CursorScope) -> Result<Self, CursorError> {
        match scope {
            CursorScope::Folder(folder) => Ok(Self::PublicFolder(folder.clone())),
            CursorScope::FolderType { folder, ty } => {
                let route = foreign::parse_folder(folder);
                match ty {
                    ObjectType::Email => Ok(Self::Mail {
                        folder: folder.clone(),
                        route,
                    }),
                    // Outlook users expect a collection's notifications and reads to
                    // address the same mailbox. Typed PIM reads currently use the
                    // primary client, so shared non-mail collections are refused at
                    // admission, including push, rather than claiming false coverage.
                    ObjectType::CalendarEvent if route.foreign().is_none() => Ok(Self::Events {
                        calendar: folder.clone(),
                    }),
                    ObjectType::Contact if route.foreign().is_none() => Ok(Self::Contacts {
                        folder: folder.clone(),
                    }),
                    _ => Err(CursorError::Unsupported),
                }
            }
            _ => Err(CursorError::Unsupported),
        }
    }

    pub(crate) fn folder(&self) -> &FolderId {
        match self {
            Self::Mail { folder, .. } | Self::Contacts { folder } | Self::PublicFolder(folder) => {
                folder
            }
            Self::Events { calendar } => calendar,
        }
    }

    pub(crate) fn native_id(&self) -> &str {
        match self {
            Self::Mail { route, .. } => route.native_id(),
            _ => &self.folder().0,
        }
    }

    pub(crate) fn owner(&self) -> Option<&str> {
        match self {
            Self::Mail { route, .. } => route.foreign().map(|foreign| foreign.mailbox.as_str()),
            _ => None,
        }
    }

    pub(crate) fn client<'a>(
        &self,
        account: &'a GraphAccount,
    ) -> Result<&'a crate::client::GraphClient, crate::error::GraphError> {
        account.mailboxes.client_for_owner(self.owner())
    }

    pub(crate) fn kind(&self) -> Result<GraphCursorKind, CursorError> {
        match self {
            Self::Mail { folder, .. } => Ok(GraphCursorKind::Messages {
                folder_id: folder.0.clone(),
            }),
            Self::Events { calendar } => Ok(GraphCursorKind::Events {
                calendar_id: calendar.0.clone(),
            }),
            Self::Contacts { folder } => Ok(GraphCursorKind::Contacts {
                folder_id: folder.0.clone(),
            }),
            // A public cursor also needs the account's async routing-map lookup.
            Self::PublicFolder(_) => Err(CursorError::Unsupported),
        }
    }

    pub(crate) fn matches_payload(&self, payload: &super::cursor::GraphCursorPayload) -> bool {
        match (self, payload.kind()) {
            (Self::PublicFolder(folder), GraphCursorKind::PublicFolder(public)) => {
                folder.0 == public.folder_id
            }
            _ => self.kind().is_ok_and(|kind| kind == payload.kind()),
        }
    }

    pub(crate) fn etag_cached(&self) -> bool {
        matches!(self, Self::Mail { .. })
    }

    // calendarView delta has no $select; retain its page-size and UTC
    // preference on every inventory and changes page, including resumes.
    // https://learn.microsoft.com/en-us/graph/delta-query-events
    // https://learn.microsoft.com/en-us/graph/api/event-get?view=graph-rest-1.0
    pub(crate) fn delta_prefer(&self) -> Option<&'static str> {
        matches!(self, Self::Events { .. })
            .then_some("odata.maxpagesize=50, outlook.timezone=\"UTC\"")
    }

    pub(crate) fn delta_path(
        &self,
        account: &GraphAccount,
        now: jiff::Timestamp,
    ) -> Result<(String, Option<i64>), CursorError> {
        use super::inventory::{
            MESSAGE_SYNC_SELECT, calendar_view_bound, calendar_view_timestamp,
            format_calendar_timestamp,
        };
        use crate::types::CONTACT_SYNC_SELECT;
        let prefix = self
            .client(account)
            .map_err(super::cursor::routing_error)?
            .api_path_prefix();
        let encoded = bifrost_net::url::encode_path_component(self.native_id());
        match self {
            GraphScope::Mail { .. } => Ok((
                format!(
                    "{prefix}/mailFolders/{encoded}/messages/delta?$select={MESSAGE_SYNC_SELECT}&$top=50"
                ),
                None,
            )),
            GraphScope::Events { .. } => {
                let start = calendar_view_bound(now, -90);
                let end_at = calendar_view_timestamp(now, 365);
                let end = format_calendar_timestamp(end_at);
                Ok((
                    format!(
                        "{prefix}/calendars/{encoded}/calendarView/delta?startDateTime={start}&endDateTime={end}"
                    ),
                    Some(end_at.as_second()),
                ))
            }
            GraphScope::Contacts { .. } => Ok((
                format!(
                    "{prefix}/contactFolders/{encoded}/contacts/delta?$select={CONTACT_SYNC_SELECT}&$top=250"
                ),
                None,
            )),
            GraphScope::PublicFolder(_) => Err(super::cursor::CursorError::Unsupported),
        }
    }

    pub(crate) fn subscription_resource(
        &self,
        account: &GraphAccount,
    ) -> Result<Option<String>, crate::error::GraphError> {
        // Calendar ids stay in the resource: using /events for every
        // calendar would subscribe every scope to the default calendar.
        let (collection, items) = match self {
            Self::Mail { .. } => ("mailFolders", "messages"),
            Self::Events { .. } => ("calendars", "events"),
            Self::Contacts { .. } => ("contactFolders", "contacts"),
            Self::PublicFolder(_) => return Ok(None),
        };
        let prefix = self.client(account)?.api_path_prefix();
        let id = bifrost_net::url::encode_path_component(self.native_id());
        Ok(Some(format!("{prefix}/{collection}/{id}/{items}")))
    }

    pub(crate) fn ews_folder_id(&self) -> Option<String> {
        // The EWS session translates and subscribes in the primary mailbox.
        // Shared mail remains readable and webhook-capable, but pretending it
        // has EWS coverage would leave an Outlook user with silently stale mail.
        (!matches!(self, Self::PublicFolder(_)) && self.owner().is_none())
            .then(|| self.native_id().to_string())
    }

    pub(crate) fn mint_object_id(&self, id: &str) -> ObjectId {
        match self {
            Self::Events { calendar } => ObjectId(super::calendar::join_event_id(&calendar.0, id)),
            Self::Mail { .. } => ObjectId(foreign::qualify_with_owner(self.owner(), id)),
            Self::Contacts { .. } => ObjectId(id.to_string()),
            Self::PublicFolder(folder) => foreign::encode_public_item_id(folder, id),
        }
    }

    pub(crate) fn thread_id(&self, id: &str) -> ThreadId {
        ThreadId(foreign::qualify_with_owner(self.owner(), id))
    }

    pub(crate) fn membership(&self, native: Option<&str>) -> MembershipScope {
        // Graph returns native parent ids. Discovery uses owner-qualified
        // shared folder ids; emit that same identity so membership coverage
        // cannot conflate a shared inbox with the primary inbox.
        let native = native
            .filter(|id| !id.is_empty())
            .unwrap_or(self.native_id());
        MembershipScope::Folder(match self.owner() {
            Some(owner) => foreign::encode_foreign(owner, native),
            None => FolderId(native.to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::PushMode;
    use crate::client::GraphClient;

    fn typed(folder: FolderId, ty: ObjectType) -> CursorScope {
        CursorScope::FolderType { folder, ty }
    }

    #[test]
    fn shared_mail_uses_one_owner_for_reads_push_ids_and_membership() {
        let account = GraphAccount::new_for_tests_with_shared(
            GraphClient::new("token"),
            PushMode::GraphSubscriptions,
            &["shared@example.com".to_string()],
        );
        let folder = foreign::encode_foreign("SHARED@example.com", "opaque/id");
        let scope = GraphScope::parse(&typed(folder.clone(), ObjectType::Email)).expect("mail");
        assert_eq!(scope.owner(), Some("SHARED@example.com"));
        assert_eq!(scope.native_id(), "opaque/id");
        let client = scope
            .client(&account)
            .expect("case insensitive owner lookup");
        let prefix = client.api_path_prefix();
        assert_eq!(
            scope.subscription_resource(&account).expect("resource"),
            Some(format!("{prefix}/mailFolders/opaque%2Fid/messages"))
        );
        let (path, end) = scope
            .delta_path(&account, jiff::Timestamp::UNIX_EPOCH)
            .expect("delta");
        assert!(path.starts_with(&format!("{prefix}/mailFolders/opaque%2Fid/messages/delta?")));
        assert!(end.is_none());
        assert!(scope.ews_folder_id().is_none());
        assert!(scope.etag_cached());
        assert!(scope.delta_prefer().is_none());
        assert_eq!(
            scope.mint_object_id("item").0,
            "SHARED@example.com\u{1f}item"
        );
        assert_eq!(
            scope.thread_id("thread").0,
            "SHARED@example.com\u{1f}thread"
        );
        assert_eq!(scope.membership(None), MembershipScope::Folder(folder));
        assert_eq!(
            scope.membership(Some("parent")),
            MembershipScope::Folder(foreign::encode_foreign("SHARED@example.com", "parent"))
        );
    }

    #[test]
    fn shared_non_mail_and_unknown_types_never_reach_a_transport_arm() {
        for ty in [ObjectType::CalendarEvent, ObjectType::Contact] {
            assert!(matches!(
                GraphScope::parse(&typed(
                    foreign::encode_foreign("shared@example.com", "collection"),
                    ty,
                )),
                Err(CursorError::Unsupported)
            ));
        }
        assert!(matches!(
            GraphScope::parse(&typed(FolderId("inbox".into()), ObjectType::Mailbox)),
            Err(CursorError::Unsupported)
        ));
        assert!(matches!(
            GraphScope::parse(&CursorScope::Account),
            Err(CursorError::Unsupported)
        ));
    }

    #[test]
    fn primary_collections_share_admission_but_keep_their_native_semantics() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
        for (ty, resource, object, cached, prefer) in [
            (
                ObjectType::Email,
                "/me/mailFolders/collection/messages",
                "item",
                true,
                None,
            ),
            (
                ObjectType::CalendarEvent,
                "/me/calendars/collection/events",
                "collection::item",
                false,
                Some("odata.maxpagesize=50, outlook.timezone=\"UTC\""),
            ),
            (
                ObjectType::Contact,
                "/me/contactFolders/collection/contacts",
                "item",
                false,
                None,
            ),
        ] {
            let scope =
                GraphScope::parse(&typed(FolderId("collection".into()), ty)).expect("primary");
            assert_eq!(
                scope
                    .subscription_resource(&account)
                    .expect("resource")
                    .as_deref(),
                Some(resource)
            );
            assert_eq!(scope.ews_folder_id().as_deref(), Some("collection"));
            assert_eq!(scope.mint_object_id("item").0, object);
            assert_eq!(scope.etag_cached(), cached);
            assert_eq!(scope.delta_prefer(), prefer);
            assert!(scope.kind().is_ok());
        }
    }

    #[test]
    fn public_folder_is_a_poll_candidate_not_a_rest_or_push_collection() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
        let scope =
            GraphScope::parse(&CursorScope::Folder(FolderId("public".into()))).expect("candidate");
        assert!(matches!(scope, GraphScope::PublicFolder(_)));
        assert!(scope.kind().is_err());
        assert!(
            scope
                .delta_path(&account, jiff::Timestamp::UNIX_EPOCH)
                .is_err()
        );
        assert_eq!(
            scope.subscription_resource(&account).expect("poll only"),
            None
        );
        assert_eq!(scope.ews_folder_id(), None);
        assert!(!scope.etag_cached());
    }
}
