/// Quota units charged by one Gmail API method, keyed on the path
/// suffix under the `users/me` base and the HTTP method.
///
/// PROVENANCE: transcribed from Google's published "Usage limits" table
/// for the Gmail API (`developers.google.com/gmail/api/reference/quota`),
/// as it stood in May 2026 - the revision that raised `messages.get` to
/// 20 units and `threads.get` to 40. Checked twice, independently, in
/// August 2026. Google restates these numbers periodically; when they
/// move, re-read that table rather than re-deriving costs from observed
/// 429s, and update this note with the date checked.
///
/// The catch-all is deliberately NOT one unit. An unlisted method is a
/// method we have not checked, and under-charging it is the failure mode
/// that produced sustained 429 backoff on the hydration loop, so it is
/// billed like a `messages.get` until someone looks it up. Requests that
/// do not resolve against the Gmail base at all - Calendar, Drive and
/// People - keep their separate surface's `cost_default` of one request.
///
/// One Gmail request identity owns quota, control admission and cursor semantics.
/// Resolve only after the typed Gmail handle selects its service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GmailEndpoint {
    Profile,
    History,
    Watch,
    Stop,
    LabelsGet,
    LabelsCreate,
    LabelsWrite,
    ThreadsList,
    ThreadsGet,
    ThreadsModify,
    ThreadsDelete,
    MessagesList,
    AttachmentGet,
    MessagesGet,
    MessagesSend,
    MessagesModify,
    MessagesDelete,
    MessagesBatchWrite,
    DraftsList,
    DraftsCreate,
    DraftsGet,
    DraftsUpdate,
    DraftsDelete,
    DraftsSend,
    SettingsGet,
    FiltersWrite,
    SendAsUpdate,
    VacationUpdate,
    Unlisted,
}

impl GmailEndpoint {
    pub(crate) fn from_request(path: &str, method: &reqwest::Method) -> Self {
        let path = path.split('?').next().unwrap_or(path);
        let segments = path.trim_matches('/').split('/').collect::<Vec<_>>();

        match (method.as_str(), segments.as_slice()) {
            ("GET", ["profile"]) => Self::Profile,
            ("GET", ["history"]) => Self::History,
            ("POST", ["watch"]) => Self::Watch,
            ("POST", ["stop"]) => Self::Stop,
            ("GET", ["labels"] | ["labels", _]) => Self::LabelsGet,
            ("POST", ["labels"]) => Self::LabelsCreate,
            ("PATCH", ["labels", _]) | ("DELETE", ["labels", _]) => Self::LabelsWrite,
            ("GET", ["threads"]) => Self::ThreadsList,
            ("GET", ["threads", _]) => Self::ThreadsGet,
            ("POST", ["threads", _, "modify"]) => Self::ThreadsModify,
            ("DELETE", ["threads", _]) => Self::ThreadsDelete,
            ("GET", ["messages"]) => Self::MessagesList,
            ("GET", ["messages", _, "attachments", _]) => Self::AttachmentGet,
            ("GET", ["messages", _]) => Self::MessagesGet,
            ("POST", ["messages", "send"]) => Self::MessagesSend,
            ("POST", ["messages", _, "modify"]) => Self::MessagesModify,
            ("DELETE", ["messages", _]) => Self::MessagesDelete,
            ("POST", ["messages", "batchModify"] | ["messages", "batchDelete"]) => {
                Self::MessagesBatchWrite
            }
            ("GET", ["drafts"]) => Self::DraftsList,
            ("POST", ["drafts"]) => Self::DraftsCreate,
            ("GET", ["drafts", _]) => Self::DraftsGet,
            ("PUT", ["drafts", _]) => Self::DraftsUpdate,
            ("DELETE", ["drafts", _]) => Self::DraftsDelete,
            ("POST", ["drafts", "send"]) => Self::DraftsSend,
            ("GET", ["settings", "filters"] | ["settings", "sendAs"])
            | ("GET", ["settings", "vacation"])
            | ("GET", ["settings", "filters", _] | ["settings", "sendAs", _]) => Self::SettingsGet,
            ("POST", ["settings", "filters"]) | ("DELETE", ["settings", "filters", _]) => {
                Self::FiltersWrite
            }
            ("PATCH", ["settings", "sendAs", _]) => Self::SendAsUpdate,
            ("PUT", ["settings", "vacation"]) => Self::VacationUpdate,
            _ => Self::Unlisted,
        }
    }

    pub(crate) fn quota_cost(self) -> u32 {
        match self {
            Self::Profile => 1,
            Self::History => 2,
            Self::Watch => 100,
            Self::Stop => 50,
            Self::LabelsGet => 1,
            Self::LabelsCreate => 5,
            Self::LabelsWrite => 5,
            Self::ThreadsList => 10,
            Self::ThreadsGet => 40,
            Self::ThreadsModify => 10,
            Self::ThreadsDelete => 20,
            Self::MessagesList => 5,
            Self::AttachmentGet => 20,
            Self::MessagesGet => 20,
            Self::MessagesSend => 100,
            Self::MessagesModify => 5,
            Self::MessagesDelete => 10,
            Self::MessagesBatchWrite => 50,
            Self::DraftsList => 5,
            Self::DraftsCreate => 10,
            Self::DraftsGet => 20,
            Self::DraftsUpdate => 15,
            Self::DraftsDelete => 10,
            Self::DraftsSend => 100,
            Self::SettingsGet => 1,
            Self::FiltersWrite => 5,
            Self::SendAsUpdate => 100,
            Self::VacationUpdate => 5,
            Self::Unlisted => 20,
        }
    }

    pub(crate) fn is_history(self) -> bool {
        self == Self::History
    }

    pub(crate) fn uses_data_concurrency(self) -> bool {
        !matches!(self, Self::Watch | Self::Stop)
    }
}

#[cfg(test)]
fn gmail_quota_cost(path: &str, method: reqwest::Method) -> u32 {
    GmailEndpoint::from_request(path, &method).quota_cost()
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::GOOGLE_API_BURST;
    #[test]
    fn gmail_method_costs_match_the_published_quota_table() {
        assert_eq!(gmail_quota_cost("/profile", reqwest::Method::GET), 1);
        assert_eq!(
            gmail_quota_cost("/history?startHistoryId=1", reqwest::Method::GET),
            2
        );
        assert_eq!(gmail_quota_cost("/messages", reqwest::Method::GET), 5);
        assert_eq!(
            gmail_quota_cost("/messages/m1?format=metadata", reqwest::Method::GET),
            20
        );
        assert_eq!(
            gmail_quota_cost("/messages/m1/attachments/a1", reqwest::Method::GET),
            20
        );
        assert_eq!(gmail_quota_cost("/threads/t1", reqwest::Method::GET), 40);
        assert_eq!(
            gmail_quota_cost("/messages/send", reqwest::Method::POST),
            100
        );
        assert_eq!(
            gmail_quota_cost("/messages/batchModify", reqwest::Method::POST),
            50
        );
        assert_eq!(gmail_quota_cost("/watch", reqwest::Method::POST), 100);
        assert_eq!(gmail_quota_cost("/stop", reqwest::Method::POST), 50);
    }

    #[test]
    fn no_method_costs_more_than_the_burst_ceiling() {
        for (path, method) in [
            ("/messages/send", reqwest::Method::POST),
            ("/drafts/send", reqwest::Method::POST),
            ("/watch", reqwest::Method::POST),
            ("/settings/sendAs/a", reqwest::Method::PATCH),
            ("/unlisted", reqwest::Method::POST),
        ] {
            assert!(
                gmail_quota_cost(path, method.clone()) <= GOOGLE_API_BURST,
                "{method} {path} cannot be admitted by a {GOOGLE_API_BURST}-unit bucket",
            );
        }
    }

    #[test]
    fn unlisted_gmail_methods_are_billed_conservatively() {
        assert_eq!(
            gmail_quota_cost("/messages/m1/unknownVerb", reqwest::Method::POST),
            20
        );
        assert_eq!(gmail_quota_cost("/somethingNew", reqwest::Method::GET), 20);
        // Cheap reads this crate actually issues stay at their real cost
        // rather than being swept into the conservative default.
        assert_eq!(gmail_quota_cost("/labels/Label_1", reqwest::Method::GET), 1);
        assert_eq!(gmail_quota_cost("/drafts", reqwest::Method::GET), 5);
        assert_eq!(
            gmail_quota_cost("/settings/sendAs/a%40b.test", reqwest::Method::GET),
            1
        );
    }
}
