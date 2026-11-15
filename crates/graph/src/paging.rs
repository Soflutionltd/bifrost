//! Bounded `@odata.nextLink` traversal.
//!
//! Every Graph collection walk follows server-supplied `nextLink` values until
//! the server stops sending them. That is an unbounded loop against a remote:
//! a server that keeps emitting a link - through a bug, a proxy, or a hostile
//! response - spins the walk forever while the accumulating `Vec` grows without
//! limit. Six such loops existed here with no bound of any kind.
//!
//! `PageWalk` adapts bifrost-net's shared pagination admission to Graph URLs
//! and errors. Both repeated-link detection and a finite page budget are needed:
//! a repeat catches cycles, while the budget also bounds endless fresh links.
//!
//! The two refusals blame different parties, so they travel as different
//! errors, and neither is a parse failure (every page parsed):
//!
//! - A repeated link is the provider's fault and nothing else's: an honest
//!   server never hands back a link it already served. It is
//!   `GraphError::ContractViolation` -> `Protocol(ContractViolation)`, with
//!   `Acknowledged` evidence, since the link arrived in a response.
//! - An exhausted page budget is NOT evidence against the provider. A server
//!   issuing fresh links forever and an honestly enormous collection look
//!   identical from here; what is certain is only that this crate declined to
//!   go further. It is `GraphError::LimitExceeded` ->
//!   `Internal(LimitExceeded)`, the same split `bifrost-google`'s walks make.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::client::GraphClient;
use crate::error::GraphError;
use crate::origin::AdmittedUrl;

use bifrost_types::{AccountOperation, PageCursor};

/// One versioned, surface-tagged boundary for every caller-owned Graph walk.
/// A saved cursor from another surface must reload that walk, never fetch an
/// unrelated collection with the account bearer. Inner positions remain owned
/// by their walkers (including public-folder and Search API positions).
#[derive(Serialize, Deserialize)]
#[serde(tag = "surface", content = "position", rename_all = "snake_case")]
enum GraphPageCursor {
    ContactsList(serde_json::Value),
    ContactSearch(serde_json::Value),
    DirectorySearch(serde_json::Value),
    DirectoryGroupsList(serde_json::Value),
    DirectoryGroupExpand(serde_json::Value),
    EventsInRange(serde_json::Value),
    EventSearch(serde_json::Value),
    Search(serde_json::Value),
    SearchMessages(serde_json::Value),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorEnvelope {
    version: u8,
    cursor: GraphPageCursor,
}

impl GraphPageCursor {
    fn operation(&self) -> AccountOperation {
        match self {
            Self::ContactsList(_) => AccountOperation::ContactsList,
            Self::ContactSearch(_) => AccountOperation::ContactSearch,
            Self::DirectorySearch(_) => AccountOperation::DirectorySearch,
            Self::DirectoryGroupsList(_) => AccountOperation::DirectoryGroupsList,
            Self::DirectoryGroupExpand(_) => AccountOperation::DirectoryGroupExpand,
            Self::EventsInRange(_) => AccountOperation::EventsInRange,
            Self::EventSearch(_) => AccountOperation::EventSearch,
            Self::Search(_) => AccountOperation::Search,
            Self::SearchMessages(_) => AccountOperation::SearchMessages,
        }
    }

    fn into_position(self) -> serde_json::Value {
        match self {
            Self::ContactsList(value)
            | Self::ContactSearch(value)
            | Self::DirectorySearch(value)
            | Self::DirectoryGroupsList(value)
            | Self::DirectoryGroupExpand(value)
            | Self::EventsInRange(value)
            | Self::EventSearch(value)
            | Self::Search(value)
            | Self::SearchMessages(value) => value,
        }
    }
}

pub(crate) fn encode_cursor<T: Serialize>(operation: AccountOperation, position: &T) -> PageCursor {
    let position = serde_json::to_value(position).expect("Graph page position is serializable");
    let cursor = match operation {
        AccountOperation::ContactsList => GraphPageCursor::ContactsList(position),
        AccountOperation::ContactSearch => GraphPageCursor::ContactSearch(position),
        AccountOperation::DirectorySearch => GraphPageCursor::DirectorySearch(position),
        AccountOperation::DirectoryGroupsList => GraphPageCursor::DirectoryGroupsList(position),
        AccountOperation::DirectoryGroupExpand => GraphPageCursor::DirectoryGroupExpand(position),
        AccountOperation::EventsInRange => GraphPageCursor::EventsInRange(position),
        AccountOperation::EventSearch => GraphPageCursor::EventSearch(position),
        AccountOperation::Search => GraphPageCursor::Search(position),
        AccountOperation::SearchMessages => GraphPageCursor::SearchMessages(position),
        _ => unreachable!("only page operations encode Graph page cursors"),
    };
    serde_json::to_vec(&CursorEnvelope { version: 1, cursor })
        .expect("Graph page cursor is serializable")
        .into()
}

pub(crate) fn decode_cursor<T: serde::de::DeserializeOwned>(
    cursor: &[u8],
    operation: AccountOperation,
) -> Result<T, String> {
    let envelope: CursorEnvelope = serde_json::from_slice(cursor)
        .map_err(|_| "malformed or retired Graph page cursor".to_string())?;
    if envelope.version != 1 || envelope.cursor.operation() != operation {
        return Err("Graph page cursor version or surface mismatch".to_string());
    }
    serde_json::from_value(envelope.cursor.into_position())
        .map_err(|_| "malformed Graph page position".to_string())
}

/// Maximum pages one collection walk may request.
///
/// Deliberately generous: at Graph's typical `$top` of 100-250 this is
/// 1,000,000+ objects, so no real mailbox, calendar set, or contact folder
/// tree approaches it. The budget exists to bound a MISBEHAVING server, not to
/// limit legitimate data - but a walk that hits it cannot tell which it met,
/// which is why the refusal is this crate's own limit and not a provider
/// fault.
const MAX_PAGES: usize = 10_000;

/// Guard for a single `nextLink` traversal.
///
/// Call [`PageWalk::enter`] with each URL before fetching it, including the
/// first. Checking the first URL too means a server that echoes the request URI
/// back as its own `nextLink` is caught on the second pass rather than looping.
pub(crate) struct PageWalk {
    seen: HashSet<String>,
    pages: usize,
    /// Names the collection in a refusal, so a support report says WHICH walk
    /// the server misbehaved on.
    what: &'static str,
}

impl PageWalk {
    pub(crate) fn new(what: &'static str) -> Self {
        Self {
            seen: HashSet::new(),
            pages: 0,
            what,
        }
    }

    /// Admit one page URL, or refuse the walk. Keyed on the admitted URL's
    /// serialization, so two spellings of one link count as a repeat.
    pub(crate) fn enter(&mut self, url: &AdmittedUrl) -> Result<(), GraphError> {
        match bifrost_net::pagination::check_next(
            &mut self.seen,
            self.pages,
            MAX_PAGES,
            Some(url.as_str().to_string()),
        ) {
            Ok(()) => self.pages += 1,
            Err(bifrost_net::pagination::PageRefusal::Repeated) => {
                return Err(GraphError::ContractViolation {
                    message: format!("Graph {} pagination repeated a page link", self.what),
                });
            }
            Err(_) => {
                return Err(GraphError::LimitExceeded {
                    message: format!("Graph {} pagination exceeded {MAX_PAGES} pages", self.what),
                });
            }
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct PagedCursor {
    url: String,
    skip: usize,
}

#[derive(Serialize, Deserialize)]
struct DirectoryCursor {
    url: String,
    skip: usize,
    mode: crate::account::DirectoryMode,
}

/// Admit the decoded position onto this client's API origin before using its bearer.
fn admit_cursor_url(client: &GraphClient, url: &str) -> Result<AdmittedUrl, String> {
    if !url.starts_with('/') && reqwest::Url::parse(url).is_err() {
        return Err("page cursor url is neither absolute nor a path".to_string());
    }
    client
        .admit_target(url)
        .map_err(|refusal| format!("page cursor {refusal}"))
}

pub(crate) fn decode_paged_cursor(
    client: &GraphClient,
    cursor: Option<PageCursor>,
    operation: AccountOperation,
) -> Result<Option<(AdmittedUrl, usize)>, String> {
    cursor
        .map(|cursor| {
            let PagedCursor { url, skip } = decode_cursor(&cursor, operation)?;
            Ok((admit_cursor_url(client, &url)?, skip))
        })
        .transpose()
}

pub(crate) fn decode_directory_cursor(
    client: &GraphClient,
    cursor: Option<PageCursor>,
) -> Result<Option<(AdmittedUrl, usize, crate::account::DirectoryMode)>, String> {
    cursor
        .map(|cursor| {
            let DirectoryCursor { url, skip, mode } =
                decode_cursor(&cursor, AccountOperation::DirectorySearch)?;
            Ok((admit_cursor_url(client, &url)?, skip, mode))
        })
        .transpose()
}

pub(crate) fn encode_directory_cursor(
    url: &AdmittedUrl,
    skip: usize,
    mode: crate::account::DirectoryMode,
) -> PageCursor {
    encode_cursor(
        AccountOperation::DirectorySearch,
        &DirectoryCursor {
            url: url.as_str().to_string(),
            skip,
            mode,
        },
    )
}

pub(crate) fn decode_link_cursor(
    client: &GraphClient,
    cursor: Option<PageCursor>,
    operation: AccountOperation,
) -> Result<Option<AdmittedUrl>, String> {
    cursor
        .map(|cursor| {
            let text: String = decode_cursor(&cursor, operation)?;
            if reqwest::Url::parse(&text).is_err() {
                return Err("page cursor is not an absolute URL".to_string());
            }
            admit_cursor_url(client, &text)
        })
        .transpose()
}

pub(crate) fn encode_paged_cursor(
    url: &AdmittedUrl,
    skip: usize,
    operation: AccountOperation,
) -> PageCursor {
    encode_cursor(
        operation,
        &PagedCursor {
            url: url.as_str().to_string(),
            skip,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_PAGES, PageWalk, PagedCursor, decode_cursor, decode_link_cursor, decode_paged_cursor,
        encode_cursor, encode_paged_cursor,
    };
    use crate::client::GraphClient;
    use crate::error::GraphError;
    use crate::origin::{AdmittedUrl, Base};
    use bifrost_types::AccountOperation;

    fn url(link: &str) -> AdmittedUrl {
        Base::parse("https://graph.test")
            .admit(link)
            .expect("same origin")
    }

    #[test]
    fn search_cursor_resumes_inside_an_overdelivered_page() {
        let client = GraphClient::new("token");
        let link = client
            .admit_target("https://graph.microsoft.com/v1.0/users?page=4")
            .expect("same origin");
        let encoded = encode_paged_cursor(&link, 17, AccountOperation::ContactSearch);
        let (url, skip) =
            decode_paged_cursor(&client, Some(encoded), AccountOperation::ContactSearch)
                .expect("valid cursor")
                .expect("present cursor");
        assert_eq!(url, link);
        assert_eq!(skip, 17);
    }

    /// An encoded first-page path resumes on this client's own base.
    #[test]
    fn a_path_paged_cursor_resumes_on_the_api_base() {
        let client = GraphClient::new("token");
        let cursor = encode_cursor(
            AccountOperation::EventSearch,
            &PagedCursor {
                url: "/me/events?$top=5".into(),
                skip: 2,
            },
        );
        let (url, skip) = decode_paged_cursor(&client, Some(cursor), AccountOperation::EventSearch)
            .expect("valid cursor")
            .expect("present cursor");
        assert_eq!(
            url.as_str(),
            "https://graph.microsoft.com/v1.0/me/events?$top=5"
        );
        assert_eq!(skip, 2);
    }

    /// The token leak: a caller's cursor bytes naming another origin were
    /// turned into a request URL verbatim and fetched with the bearer.
    #[test]
    fn a_cursor_off_the_api_origin_is_refused() {
        let client = GraphClient::new("token");
        for cursor in [
            br#"{"url":"https://elsewhere.example/users","skip":0}"#.to_vec(),
            br#"{"url":"HTTPS://elsewhere.example/users","skip":0}"#.to_vec(),
            br#"{"url":"https://graph.microsoft.com@elsewhere.example/v1.0","skip":0}"#.to_vec(),
            b"https://elsewhere.example/users".to_vec(),
            b"HTTPS://elsewhere.example/users".to_vec(),
        ] {
            let text = String::from_utf8_lossy(&cursor);
            let position = serde_json::from_slice::<PagedCursor>(&cursor).unwrap_or(PagedCursor {
                url: text.into_owned(),
                skip: 0,
            });
            let link = encode_cursor(AccountOperation::ContactsList, &position.url);
            assert!(
                decode_link_cursor(&client, Some(link), AccountOperation::ContactsList).is_err()
            );
            let encoded = encode_cursor(AccountOperation::ContactSearch, &position);
            assert!(
                decode_paged_cursor(&client, Some(encoded), AccountOperation::ContactSearch)
                    .is_err()
            );
            assert!(
                decode_paged_cursor(
                    &client,
                    Some(cursor.clone().into()),
                    AccountOperation::ContactSearch
                )
                .is_err()
            );
            assert!(
                decode_link_cursor(&client, Some(cursor.into()), AccountOperation::ContactsList)
                    .is_err()
            );
        }
    }

    /// Bytes this crate could not have minted are refused, not turned into a
    /// same-origin request for whatever path they spell.
    #[test]
    fn unmintable_cursor_bytes_are_refused() {
        let client = GraphClient::new("token");
        for cursor in [
            b"garbage".to_vec(),
            b"me/messages".to_vec(),
            vec![0xff, 0xfe],
            br#"{"url":"garbage","skip":0}"#.to_vec(),
        ] {
            assert!(
                decode_paged_cursor(
                    &client,
                    Some(cursor.clone().into()),
                    AccountOperation::ContactSearch
                )
                .is_err()
            );
            assert!(
                decode_link_cursor(&client, Some(cursor.into()), AccountOperation::ContactsList)
                    .is_err()
            );
        }
    }

    #[test]
    fn graph_page_cursors_reject_every_other_surface_and_version() {
        let operations = [
            AccountOperation::ContactsList,
            AccountOperation::ContactSearch,
            AccountOperation::DirectorySearch,
            AccountOperation::DirectoryGroupsList,
            AccountOperation::DirectoryGroupExpand,
            AccountOperation::EventsInRange,
            AccountOperation::EventSearch,
            AccountOperation::Search,
            AccountOperation::SearchMessages,
        ];
        for minted in operations {
            let cursor = encode_cursor(minted, &"opaque");
            assert_eq!(decode_cursor::<String>(&cursor, minted).unwrap(), "opaque");
            for used in operations.into_iter().filter(|used| *used != minted) {
                assert!(
                    decode_cursor::<String>(&cursor, used).is_err(),
                    "{minted:?} used on {used:?}"
                );
            }
            let mut value: serde_json::Value = serde_json::from_slice(&cursor).unwrap();
            value["version"] = 2.into();
            assert!(decode_cursor::<String>(&serde_json::to_vec(&value).unwrap(), minted).is_err());
        }
        let legacy = br#"{"url":"https://graph.microsoft.com/v1.0/users","skip":0}"#;
        assert!(decode_cursor::<PagedCursor>(legacy, AccountOperation::ContactSearch).is_err());
    }

    #[test]
    fn a_repeated_link_is_refused() {
        let mut walk = PageWalk::new("calendars");
        walk.enter(&url("https://graph.test/calendars"))
            .expect("first");
        walk.enter(&url("https://graph.test/calendars?page=2"))
            .expect("second");
        let error = walk
            .enter(&url("https://graph.test/calendars"))
            .expect_err("a link already walked must be refused");
        // The provider's contract breach: never a parse failure (the page
        // parsed) and never this crate's own limit.
        match error {
            GraphError::ContractViolation { message } => {
                assert!(message.contains("repeated a page link"), "{message}");
            }
            other => panic!("a repeated link must be a contract violation: {other:?}"),
        }
    }

    /// The budget is the half a repeated-link check cannot cover: a server
    /// handing out a FRESH link every page never repeats itself, so only a
    /// finite budget stops it. This is exactly how the same defect was closed
    /// in bifrost-google, and why both guards are here rather than one.
    #[test]
    fn a_server_issuing_endless_fresh_links_is_refused() {
        let mut walk = PageWalk::new("contactFolders");
        for page in 0..MAX_PAGES {
            walk.enter(&url(&format!("https://graph.test/c?page={page}")))
                .expect("within budget");
        }
        let error = walk
            .enter(&url("https://graph.test/c?page=never-seen-before"))
            .expect_err("an unrepeated but endless walk must still be bounded");
        // An honestly huge collection looks the same as an endless one, so the
        // refusal is this crate's limit, not a charge against the provider.
        match error {
            GraphError::LimitExceeded { message } => {
                assert!(message.contains("exceeded"), "{message}");
            }
            other => panic!("an exhausted budget must be LimitExceeded: {other:?}"),
        }
    }

    /// A proven provider cycle is more specific than the local budget limit.
    #[test]
    fn the_repeat_is_checked_before_the_budget() {
        let mut walk = PageWalk::new("c");
        for page in 0..MAX_PAGES {
            walk.enter(&url(&format!("https://graph.test/c?page={page}")))
                .expect("within budget");
        }
        let error = walk
            .enter(&url("https://graph.test/c?page=0"))
            .expect_err("past the budget");
        assert!(
            matches!(error, GraphError::ContractViolation { .. }),
            "{error:?}"
        );
    }
}
