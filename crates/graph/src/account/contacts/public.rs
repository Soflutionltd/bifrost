//! Read-only typed contact doors for discovered public folders.
use super::*;
use crate::account::foreign;
use crate::ews::{self, contacts::Node};
use bifrost_types::{ErrorScope, FolderId};

pub(super) async fn books(account: &GraphAccount) -> Vec<AddressBook> {
    let routing = account.routing_map.read().await;
    let meta = account.public_folder_meta.read().await;
    let mut books: Vec<_> = meta
        .iter()
        .filter(|(folder, meta)| {
            routing.contains_key(*folder)
                && meta.effective_rights.read
                && ews::EwsItemKind::from_folder_class(meta.folder_class.as_deref().unwrap_or(""))
                    == ews::EwsItemKind::Contact
        })
        .map(|(folder, meta)| AddressBook {
            id: foreign::public_address_book(&folder.0),
            native_id: folder.0.clone(),
            name: meta.display_name.clone(),
            provenance: ContactProvenance {
                provider: ProtocolKind::Graph,
                native: folder.0.clone(),
                address_book_native: None,
            },
            corpus: ContactCorpus::Main,
            is_default: false,
            can_create_contacts: false,
            can_update_contacts: false,
            can_delete_contacts: false,
        })
        .collect();
    books.sort_by(|a, b| a.id.0.cmp(&b.id.0));
    books
}

async fn routing(
    account: &GraphAccount,
    folder: &str,
    operation: AccountOperation,
) -> Result<ews::EwsHeaders, AccountError> {
    account
        .public_folder_routing(&FolderId(folder.to_string()))
        .await
        .map(|route| route.headers())
        .ok_or_else(|| {
            graph_error::stale_local_state_error(
                operation,
                ErrorScope::ContactCollection,
                format!("public contact folder {folder} has no routing entry"),
            )
        })
}

fn ews_error(error: ews::EwsError, operation: AccountOperation) -> AccountError {
    graph_error::ews_error_to_account_error(
        error,
        GraphErrorContext::ews(operation).with_scope(ErrorScope::ContactCollection),
    )
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Position {
    public_contacts_v1: String,
    offset: u32,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    skip: usize,
}

pub(super) async fn list(
    account: &GraphAccount,
    folder: &str,
    cursor: Option<Vec<u8>>,
) -> Result<Page<ContactCard>, AccountError> {
    page(
        account,
        folder,
        cursor,
        None,
        usize::MAX,
        AccountOperation::ContactsList,
    )
    .await
}

pub(super) async fn search(
    account: &GraphAccount,
    folder: &str,
    request: &ContactSearchRequest,
) -> Result<Page<ContactCard>, AccountError> {
    page(
        account,
        folder,
        request.page_cursor.clone(),
        Some(&request.query),
        request.limit.unwrap_or(250).max(1) as usize,
        AccountOperation::ContactSearch,
    )
    .await
}

async fn page(
    account: &GraphAccount,
    folder: &str,
    cursor: Option<Vec<u8>>,
    query: Option<&str>,
    limit: usize,
    operation: AccountOperation,
) -> Result<Page<ContactCard>, AccountError> {
    let headers = routing(account, folder, operation).await?;
    let (offset, skip) = if let Some(cursor) = cursor {
        let position: Position = serde_json::from_slice(&cursor)
            .map_err(|error| graph_error::invalid_account_error(operation, error.to_string()))?;
        if position.public_contacts_v1 != folder || position.query.as_deref() != query {
            return Err(graph_error::invalid_account_error(
                operation,
                "public contact cursor belongs to another folder",
            ));
        }
        (position.offset, position.skip)
    } else {
        (0, 0)
    };
    let client = super::super::public_folder::ews_client(account)
        .map_err(|error| ews_error(error, operation))?;
    let xml = client
        .execute(
            &ews::contacts::find_body(folder, offset, query),
            &headers,
            bifrost_net::ReplayPolicy::Idempotent,
        )
        .await
        .map_err(|error| ews_error(error, operation))?;
    let page = ews::parse_find_items_response(&xml).map_err(|error| ews_error(error, operation))?;
    let next_offset = if page.includes_last {
        None
    } else {
        let next = page
            .next_offset
            .filter(|next| *next > offset)
            .ok_or_else(|| {
                graph_error::into_account_error(
                    crate::error::GraphError::ContractViolation {
                        message: "public contact page has no advancing indexed offset".into(),
                    },
                    GraphErrorContext::ews(operation).with_scope(ErrorScope::ContactCollection),
                )
            })?;
        Some(next)
    };
    let nodes = ews::contacts::contact_nodes(&xml).map_err(|error| ews_error(error, operation))?;
    let skipped_dist_lists = nodes
        .iter()
        .filter(|node| {
            node.name == "DistributionList"
                || node
                    .value("ItemClass")
                    .is_some_and(|class| ews::is_subclass(&class, "IPM.DistList"))
        })
        .count();
    if skipped_dist_lists > 0 {
        tracing::warn!(
            skipped_dist_lists,
            "public contact page skipped distribution lists: ContactCard has no group representation"
        );
    }
    if skip > nodes.len() {
        return Err(graph_error::invalid_account_error(
            operation,
            "public contact cursor skips beyond its page",
        ));
    }
    let mut items = Vec::new();
    let mut resume = next_offset.map(|offset| (offset, 0));
    let page_len = nodes.len();
    for (index, node) in nodes.into_iter().enumerate().skip(skip) {
        if is_contact(&node) {
            items.push(project(folder, &node).map_err(|error| ews_error(error, operation))?);
            if items.len() == limit {
                if index + 1 < page_len {
                    resume = Some((offset, index + 1));
                }
                break;
            }
        }
    }
    let next_cursor = resume.map(|(offset, skip)| {
        serde_json::to_vec(&Position {
            public_contacts_v1: folder.to_string(),
            offset,
            query: query.map(str::to_string),
            skip,
        })
        .expect("serializable public contact position")
    });
    Ok(Page {
        truncated: false,
        items,
        next_cursor,
        estimated_total: None,
        failed_ids: Vec::new(),
        skipped_scopes: Vec::new(),
    })
}

pub(super) async fn get(
    account: &GraphAccount,
    folder: &str,
    item: &str,
) -> Result<ContactCard, AccountError> {
    let operation = AccountOperation::ContactGet;
    let map_error = |error| {
        graph_error::ews_error_to_account_error(
            error,
            GraphErrorContext::ews(operation).with_scope(ErrorScope::Contact {
                id: foreign::public_contact(folder, item),
            }),
        )
    };
    let headers = routing(account, folder, operation).await?;
    let client = super::super::public_folder::ews_client(account).map_err(map_error)?;
    let xml = client
        .execute(
            &ews::contacts::get_body(item),
            &headers,
            bifrost_net::ReplayPolicy::Idempotent,
        )
        .await
        .map_err(map_error)?;
    let nodes = ews::contacts::contact_nodes(&xml).map_err(map_error)?;
    if nodes.len() != 1 || nodes[0].child("ItemId").and_then(|id| id.id.as_deref()) != Some(item) {
        return Err(graph_error::into_account_error(
            crate::error::GraphError::ContractViolation {
                message: "public contact GetItem returned a different identity".into(),
            },
            GraphErrorContext::ews(operation).with_scope(ErrorScope::Contact {
                id: foreign::public_contact(folder, item),
            }),
        ));
    }
    let node = &nodes[0];
    if !is_contact(node) {
        return Err(graph_error::unsupported_item_error(
            operation,
            ErrorScope::Contact {
                id: foreign::public_contact(folder, item),
            },
        ));
    }
    project(folder, node).map_err(map_error)
}

fn is_contact(node: &Node) -> bool {
    node.name == "Contact"
        && node
            .value("ItemClass")
            .is_some_and(|class| ews::is_subclass(&class, "IPM.Contact"))
}

fn project(folder: &str, node: &Node) -> Result<ContactCard, ews::EwsError> {
    let identity = node
        .child("ItemId")
        .filter(|id| id.id.is_some())
        .ok_or_else(|| {
            ews::EwsError::MalformedXml(bifrost_types::DiagnosticText::support_only(
                "public contact has no ItemId".to_string(),
            ))
        })?;
    let native = identity.id.clone().expect("checked identity");
    let entries = |name| {
        node.child(name)
            .map(|dictionary| dictionary.children.as_slice())
            .unwrap_or_default()
    };
    let display_name = node.value("DisplayName").or_else(|| {
        let name = [node.value("GivenName"), node.value("Surname")]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(" ");
        (!name.is_empty()).then_some(name)
    });
    Ok(ContactCard {
        id: foreign::public_contact(folder, &native),
        address_book_id: Some(foreign::public_address_book(folder)),
        native_id: native.clone(),
        etag: identity.change_key.clone(),
        provenance: ContactProvenance {
            provider: ProtocolKind::Graph,
            native,
            address_book_native: Some(folder.to_string()),
        },
        corpus: ContactCorpus::Main,
        display_name,
        emails: contact_emails(entries("EmailAddresses")),
        phones: entries("PhoneNumbers")
            .iter()
            .filter(|entry| entry.name == "Entry" && !entry.text.trim().is_empty())
            .map(|entry| ContactPhone {
                value: entry.text.clone(),
                kind: entry.key.clone(),
                is_primary: false,
            })
            .collect(),
        organizations: node
            .value("CompanyName")
            .or_else(|| node.value("JobTitle").map(|_| String::new()))
            .map(|name| {
                vec![ContactOrganization {
                    name,
                    title: node.value("JobTitle"),
                }]
            })
            .unwrap_or_default(),
        addresses: entries("PhysicalAddresses")
            .iter()
            .filter(|entry| entry.name == "Entry")
            .map(|entry| ContactAddress {
                kind: entry.key.clone(),
                formatted: None,
                street: entry
                    .value("Street")
                    .map(|text| text.lines().map(str::to_string).collect())
                    .unwrap_or_default(),
                locality: entry.value("City"),
                region: entry.value("State"),
                postal_code: entry.value("PostalCode"),
                country: entry.value("CountryOrRegion"),
                is_primary: false,
            })
            .collect(),
        notes: node.value("Body"),
        photo_url: None,
        photo: None,
    })
}

/// EX and other Exchange routing addresses are not SMTP recipients. Missing
/// routing metadata is admitted only when a complete addr-spec parses.
fn contact_emails(entries: &[Node]) -> Vec<ContactEmail> {
    let mut skipped_email_addresses = 0_u64;
    let emails = entries
        .iter()
        .filter(|entry| entry.name == "Entry")
        .filter_map(|entry| {
            let value = entry.text.trim();
            let usable = match entry.routing_type.as_deref() {
                Some(kind) => kind.eq_ignore_ascii_case("SMTP") && !value.is_empty(),
                None => email_address::EmailAddress::parse_with_options(
                    value,
                    email_address::Options::default().without_display_text(),
                )
                .is_ok(),
            };
            if !usable {
                skipped_email_addresses += 1;
                return None;
            }
            Some(ContactEmail {
                value: value.to_string(),
                kind: entry.key.clone(),
                is_primary: false,
            })
        })
        .collect();
    if skipped_email_addresses > 0 {
        tracing::warn!(
            skipped_email_addresses,
            "public contact omitted non-SMTP or invalid untyped email addresses"
        );
    }
    emails
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use bifrost_net::test_support::{Canned, ScriptedDispatch, scripted_account};
    use bifrost_net::{NetConfig, RetryPolicy, StaticTokenSource, TokenSource};
    use bifrost_types::AccountErrorKind;
    use std::sync::Arc;

    #[derive(Clone, Default)]
    struct GroupWarnings(
        Arc<std::sync::atomic::AtomicU64>,
        Arc<std::sync::atomic::AtomicU64>,
    );

    impl tracing::field::Visit for GroupWarnings {
        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            if field.name() == "skipped_email_addresses" {
                self.1
                    .fetch_add(value, std::sync::atomic::Ordering::Relaxed);
            }
            if field.name() == "skipped_dist_lists" {
                self.0
                    .fetch_add(value, std::sync::atomic::Ordering::Relaxed);
            }
        }
        fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
    }

    impl tracing::Subscriber for GroupWarnings {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            event.record(&mut self.clone());
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    fn account(answers: Vec<String>) -> (Arc<ScriptedDispatch>, GraphAccount) {
        let script = ScriptedDispatch::new(answers.into_iter().map(|answer| Canned::Response {
            status: reqwest::StatusCode::OK,
            headers: reqwest::header::HeaderMap::new(),
            body: bytes::Bytes::from(answer),
        }));
        let token: Arc<dyn TokenSource> = Arc::new(StaticTokenSource::new("token", None));
        let net = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            Arc::clone(&token),
            RetryPolicy::disabled(),
        );
        let client =
            crate::client::GraphClient::with_account_net(net, "https://graph.test/v1.0", token);
        (
            script,
            GraphAccount::new_for_tests(client, super::super::super::PushMode::GraphSubscriptions),
        )
    }

    async fn seed(account: &GraphAccount, folder: &str, class: &str, read: bool) {
        account
            .seed_public_folder_meta_for_tests(
                FolderId(folder.into()),
                super::super::super::cursor::PublicFolderRouting {
                    anchor_mailbox: "anchor@example.test".into(),
                    public_folder_mailbox: Some("content@example.test".into()),
                },
                super::super::super::public_folder::PublicFolderMeta {
                    display_name: "Public people".into(),
                    folder_class: Some(class.into()),
                    parent: None,
                    effective_rights: ews::EwsEffectiveRights {
                        read,
                        ..Default::default()
                    },
                },
            )
            .await;
    }

    const CONTACT: &str = r#"<t:Contact><t:ItemId Id="person&amp;1" ChangeKey="version"/><t:ItemClass>IPM.Contact.Custom</t:ItemClass><t:DisplayName>Ada &amp; Grace</t:DisplayName><t:GivenName>Ada</t:GivenName><t:Surname>Lovelace</t:Surname><t:EmailAddresses><t:Entry Key="EmailAddress1">ada@example.test</t:Entry><t:Entry Key="EmailAddress2">other@example.test</t:Entry></t:EmailAddresses><t:PhoneNumbers><t:Entry Key="BusinessPhone">123</t:Entry><t:Entry Key="MobilePhone">456</t:Entry></t:PhoneNumbers><t:CompanyName>Engine</t:CompanyName><t:JobTitle>Programmer</t:JobTitle><t:PhysicalAddresses><t:Entry Key="Business"><t:Street>First
Second</t:Street><t:City>London</t:City><t:State>Region</t:State><t:PostalCode>12345</t:PostalCode><t:CountryOrRegion>UK</t:CountryOrRegion></t:Entry></t:PhysicalAddresses><t:Body>Notes &amp; details</t:Body></t:Contact>"#;
    fn soap(items: &str, last: bool, offset: Option<u32>) -> String {
        let offset = offset
            .map(|offset| format!(r#" IndexedPagingOffset="{offset}""#))
            .unwrap_or_default();
        format!(
            r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"><s:Body><m:FindItemResponse><m:ResponseMessages><m:FindItemResponseMessage ResponseClass="Success"><m:ResponseCode>NoError</m:ResponseCode><m:RootFolder IncludesLastItemInRange="{last}" TotalItemsInView="3"{offset}><t:Items>{items}</t:Items></m:RootFolder></m:FindItemResponseMessage></m:ResponseMessages></m:FindItemResponse></s:Body></s:Envelope>"#
        )
    }
    fn get_soap(items: &str) -> String {
        soap(items, true, None)
            .replace("FindItem", "GetItem")
            .replace(
                "<m:RootFolder IncludesLastItemInRange=\"true\" TotalItemsInView=\"3\">",
                "",
            )
            .replace("</m:RootFolder>", "")
            .replace("t:Items", "m:Items")
    }

    #[tokio::test]
    async fn nc2b_public_books_are_discoverable_read_only_without_pins() {
        let (script, mut account) = account(vec![r#"{"value":[]}"#.into()]);
        account.public_folders = Some(super::super::super::PublicFolderScope::hierarchy_only());
        for (folder, class, read) in [
            ("people", "IPF.Contact.Custom", true),
            ("lookalike", "IPF.Contactless", true),
            ("mail", "IPF.Note", true),
            ("denied", "IPF.Contact", false),
        ] {
            seed(&account, folder, class, read).await;
        }
        let books = super::super::address_books_list(account).await.unwrap();
        assert_eq!(books.len(), 2);
        let book = &books[1];
        assert_eq!(book.id, foreign::public_address_book("people"));
        assert_eq!(book.native_id, "people");
        assert_eq!(book.provenance.native, "people");
        assert!(
            !book.is_default
                && !book.can_create_contacts
                && !book.can_update_contacts
                && !book.can_delete_contacts
        );
        assert_eq!(script.requests().len(), 1);
    }

    #[tokio::test]
    async fn nc2b_public_contacts_page_project_and_skip_groups() {
        let warnings = GroupWarnings::default();
        let _subscriber = tracing::subscriber::set_default(warnings.clone());
        let group = r#"<t:DistributionList><t:ItemId Id="group"/><t:ItemClass>IPM.DistList</t:ItemClass></t:DistributionList>"#;
        let (script, account) = account(vec![
            soap(group, false, Some(17)),
            soap(CONTACT, true, None),
        ]);
        seed(&account, "people", "IPF.Contact", true).await;
        let first = super::super::list(
            account.clone(),
            Some(foreign::public_address_book("people")),
            None,
        )
        .await
        .unwrap();
        assert!(first.items.is_empty());
        assert_eq!(warnings.0.load(std::sync::atomic::Ordering::Relaxed), 1);
        let second = super::super::list(
            account,
            Some(foreign::public_address_book("people")),
            Some(first.next_cursor.unwrap()),
        )
        .await
        .unwrap();
        assert!(second.next_cursor.is_none());
        assert_eq!(second.items.len(), 1);
        let card = &second.items[0];
        assert_eq!(card.id, foreign::public_contact("people", "person&1"));
        assert_eq!(card.native_id, "person&1");
        assert_eq!(
            card.provenance.address_book_native.as_deref(),
            Some("people")
        );
        assert_eq!(
            card.address_book_id,
            Some(foreign::public_address_book("people"))
        );
        assert_eq!(card.display_name.as_deref(), Some("Ada & Grace"));
        assert_eq!(card.etag.as_deref(), Some("version"));
        assert_eq!(card.emails.len(), 2);
        assert_eq!(card.emails[0].value, "ada@example.test");
        assert_eq!(card.phones[1].value, "456");
        assert_eq!(card.phones[1].kind.as_deref(), Some("MobilePhone"));
        assert_eq!(card.organizations[0].name, "Engine");
        assert_eq!(card.organizations[0].title.as_deref(), Some("Programmer"));
        let address = &card.addresses[0];
        assert_eq!(address.street, ["First", "Second"]);
        assert_eq!(address.locality.as_deref(), Some("London"));
        assert_eq!(address.region.as_deref(), Some("Region"));
        assert_eq!(address.postal_code.as_deref(), Some("12345"));
        assert_eq!(address.country.as_deref(), Some("UK"));
        assert_eq!(card.notes.as_deref(), Some("Notes & details"));
        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        for (index, request) in requests.iter().enumerate() {
            assert_eq!(request.url.path(), "/EWS/Exchange.asmx");
            assert_eq!(request.headers["X-AnchorMailbox"], "anchor@example.test");
            assert_eq!(
                request.headers["X-PublicFolderMailbox"],
                "content@example.test"
            );
            let body = std::str::from_utf8(request.body.as_ref().unwrap()).unwrap();
            assert!(body.contains(if index == 0 {
                "Offset=\"0\""
            } else {
                "Offset=\"17\""
            }));
            for field in [
                "item:ItemClass",
                "contacts:DisplayName",
                "contacts:GivenName",
                "contacts:Surname",
                "contacts:EmailAddresses",
                "contacts:PhoneNumbers",
                "contacts:PhysicalAddresses",
                "contacts:CompanyName",
                "contacts:JobTitle",
            ] {
                assert!(
                    body.contains(&format!("FieldURI=\"{field}\"")),
                    "missing {field}"
                );
            }
            assert!(!body.contains("message:") && !body.contains("item:DateTimeReceived"));
        }
    }

    #[tokio::test]
    async fn nc2b_truncated_contact_pages_require_advancing_offsets() {
        for offset in [None, Some(0)] {
            let (script, account) = account(vec![soap(CONTACT, false, offset)]);
            seed(&account, "people", "IPF.Contact", true).await;
            let error =
                super::super::list(account, Some(foreign::public_address_book("people")), None)
                    .await
                    .unwrap_err();
            assert!(matches!(error.kind(), AccountErrorKind::Protocol(_)));
            let requests = script.requests();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].url.path(), "/EWS/Exchange.asmx");
        }
    }

    #[tokio::test]
    async fn nc2b_public_get_checks_kind_and_projects_contact_shape() {
        for (wire, class) in [
            ("Contact", "IPM.Contact"),
            ("Message", "IPM.Note"),
            ("Contact", "IPM.Contactless"),
            ("DistributionList", "IPM.DistList"),
            ("Task", "IPM.Task"),
        ] {
            let contact = CONTACT
                .replace("t:Contact", &format!("t:{wire}"))
                .replace("IPM.Contact.Custom", class);
            let (script, account) = account(vec![get_soap(&contact)]);
            seed(&account, "people", "IPF.Contact", true).await;
            let result =
                super::super::get(account, foreign::public_contact("people", "person&1")).await;
            if wire == "Contact" && class == "IPM.Contact" {
                let card = result.unwrap();
                assert_eq!(card.notes.as_deref(), Some("Notes & details"));
                assert_eq!(card.emails.len(), 2);
            } else {
                assert!(matches!(
                    result.unwrap_err().kind(),
                    AccountErrorKind::Unsupported(AccountOperation::ContactGet)
                ));
            }
            let requests = script.requests();
            assert_eq!(requests.len(), 1);
            let body = std::str::from_utf8(requests[0].body.as_ref().unwrap()).unwrap();
            assert!(body.contains("<m:GetItem>"));
            assert!(body.contains("contacts:GivenName") && body.contains("contacts:Surname"));
            assert!(body.contains("item:Body") && body.contains("<t:BodyType>Text"));
            assert!(!body.contains("message:"));
        }
    }

    #[tokio::test]
    async fn nc2b_missing_routing_and_all_public_write_doors_refuse_without_wire() {
        let (script, account) = account(vec![]);
        let book = foreign::public_address_book("people");
        let id = foreign::public_contact("people", "person");
        for error in [
            super::super::list(account.clone(), Some(book.clone()), None)
                .await
                .unwrap_err(),
            super::super::get(account.clone(), id.clone())
                .await
                .unwrap_err(),
        ] {
            assert_eq!(error.kind(), &AccountErrorKind::ConcurrencyConflict);
        }
        for seeded in [false, true] {
            if seeded {
                seed(&account, "people", "IPF.Contact", true).await;
            }
            let errors = [
                (
                    super::super::create(
                        account.clone(),
                        ContactCreate {
                            address_book_id: Some(book.clone()),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap_err(),
                    AccountOperation::ContactCreate,
                ),
                (
                    super::super::update(account.clone(), id.clone(), ContactPatch::default())
                        .await
                        .unwrap_err(),
                    AccountOperation::ContactUpdate,
                ),
                (
                    super::super::update(
                        account.clone(),
                        ContactId("rest".into()),
                        ContactPatch {
                            address_book_id: Some(book.clone()),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap_err(),
                    AccountOperation::ContactUpdate,
                ),
                (
                    super::super::delete(account.clone(), id.clone())
                        .await
                        .unwrap_err(),
                    AccountOperation::ContactDelete,
                ),
            ];
            for (error, operation) in errors {
                assert_eq!(error.kind(), &AccountErrorKind::Unsupported(operation));
            }
        }
        assert!(script.requests().is_empty());
    }

    #[tokio::test]
    async fn nc2b_public_cursor_cannot_change_collection_or_transport() {
        let (script, account) = account(vec![]);
        seed(&account, "people", "IPF.Contact", true).await;
        for cursor in [
            br#"{"public_contacts_v1":"another","offset":17}"#.to_vec(),
            b"https://graph.test/v1.0/me/contacts".to_vec(),
        ] {
            let error = super::super::list(
                account.clone(),
                Some(foreign::public_address_book("people")),
                Some(cursor),
            )
            .await
            .unwrap_err();
            assert!(matches!(error.kind(), AccountErrorKind::Request(_)));
        }
        assert!(script.requests().is_empty());
    }

    #[tokio::test]
    async fn nc2b_public_contact_get_refuses_wrong_identity_and_truncated_xml() {
        for (xml, expected) in [
            (
                get_soap(&CONTACT.replace("person&amp;1", "somebody-else")),
                bifrost_types::ProtocolErrorKind::ContractViolation,
            ),
            (
                get_soap(CONTACT)
                    .trim_end_matches("</s:Envelope>")
                    .to_string(),
                bifrost_types::ProtocolErrorKind::ParseFailed,
            ),
        ] {
            let (script, account) = account(vec![xml]);
            seed(&account, "people", "IPF.Contact", true).await;
            let error = super::super::get(account, foreign::public_contact("people", "person&1"))
                .await
                .unwrap_err();
            assert_eq!(error.kind(), &AccountErrorKind::Protocol(expected));
            let requests = script.requests();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].url.path(), "/EWS/Exchange.asmx");
        }
    }

    #[tokio::test]
    async fn nc2b_review_email_routing_types_never_expose_legacy_addresses() {
        let entries = r#"<t:Entry Key="EmailAddress1" RoutingType="EX">/o=Org/ou=Site/cn=Recipients/cn=Ada</t:Entry>
<t:Entry Key="EmailAddress2" RoutingType="EX">looks.smtp@example.test</t:Entry>
<t:Entry Key="EmailAddress3" RoutingType="SMTP">smtp@example.test</t:Entry>
<t:Entry Key="EmailAddress1" RoutingType="smtp">lower@example.test</t:Entry>
<t:Entry Key="EmailAddress2">untyped@example.test</t:Entry>
<t:Entry Key="EmailAddress2">&quot;quoted local&quot;@example.test</t:Entry>
<t:Entry Key="EmailAddress3">/o=Org/cn=Legacy</t:Entry>
<t:Entry Key="EmailAddress3">bad@@example.test</t:Entry>
<t:Entry Key="EmailAddress3">Name &lt;display@example.test&gt;</t:Entry>
<t:Entry Key="EmailAddress3" RoutingType="X400">other@example.test</t:Entry>
<t:Entry Key="EmailAddress3" RoutingType="">emptytype@example.test</t:Entry>"#;
        let start = CONTACT.find("<t:EmailAddresses>").unwrap();
        let end = CONTACT.find("</t:EmailAddresses>").unwrap() + "</t:EmailAddresses>".len();
        let mut contact = CONTACT.to_string();
        contact.replace_range(
            start..end,
            &format!("<t:EmailAddresses>{entries}</t:EmailAddresses>"),
        );
        let warnings = GroupWarnings::default();
        let _subscriber = tracing::subscriber::set_default(warnings.clone());
        let (script, account) = account(vec![soap(&contact, true, None), get_soap(&contact)]);
        seed(&account, "people", "IPF.Contact", true).await;
        let list = super::super::list(
            account.clone(),
            Some(foreign::public_address_book("people")),
            None,
        )
        .await
        .unwrap();
        let get = super::super::get(account, foreign::public_contact("people", "person&1"))
            .await
            .unwrap();
        for card in [&list.items[0], &get] {
            assert_eq!(
                card.emails
                    .iter()
                    .map(|email| email.value.as_str())
                    .collect::<Vec<_>>(),
                [
                    "smtp@example.test",
                    "lower@example.test",
                    "untyped@example.test",
                    "\"quoted local\"@example.test"
                ]
            );
        }
        assert_eq!(warnings.1.load(std::sync::atomic::Ordering::Relaxed), 14);
        assert_eq!(script.requests().len(), 2);
    }

    #[tokio::test]
    async fn nc2b_review_contact_not_found_preserves_resource_and_id() {
        let xml = r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"><s:Body><m:GetItemResponse><m:ResponseMessages><m:GetItemResponseMessage ResponseClass="Error"><m:MessageText>gone</m:MessageText><m:ResponseCode>ErrorItemNotFound</m:ResponseCode></m:GetItemResponseMessage></m:ResponseMessages></m:GetItemResponse></s:Body></s:Envelope>"#;
        let (script, account) = account(vec![xml.into()]);
        seed(&account, "people", "IPF.Contact", true).await;
        let id = foreign::public_contact("people", "person&1");
        let error = super::super::get(account, id.clone()).await.unwrap_err();
        assert_eq!(
            error.kind(),
            &AccountErrorKind::NotFound(bifrost_types::ResourceKind::Contact)
        );
        assert_eq!(error.scope(), Some(&ErrorScope::Contact { id: id.clone() }));
        assert!(error.chain().iter().any(|cause| matches!(cause,
            bifrost_types::Cause::Request(bifrost_types::RequestCause::NotFound { what: bifrost_types::ResourceKind::Contact, id: Some(native) }) if native == &id.0)));
        assert_eq!(script.requests().len(), 1);
    }

    #[tokio::test]
    async fn nc2b_review_public_search_restricts_all_six_fields_and_pages_with_limit() {
        let contact2 = CONTACT.replace("person&amp;1", "person2");
        let group = r#"<t:DistributionList><t:ItemId Id="group"/><t:ItemClass>IPM.DistList</t:ItemClass></t:DistributionList>"#;
        let mixed = format!("{CONTACT}{group}{contact2}");
        let (script, account) = account(vec![
            soap(&mixed, false, Some(17)),
            soap(&mixed, false, Some(17)),
            soap(CONTACT, true, None),
        ]);
        seed(&account, "people", "IPF.Contact", true).await;
        let request = ContactSearchRequest {
            query: "A<&\"'".into(),
            address_book_id: Some(foreign::public_address_book("people")),
            limit: Some(1),
            page_cursor: None,
        };
        let first = super::super::search(account.clone(), request.clone())
            .await
            .unwrap();
        assert_eq!(first.items[0].native_id, "person&1");
        assert_eq!(first.items[0].emails.len(), 2);
        let second = super::super::search(
            account.clone(),
            ContactSearchRequest {
                page_cursor: first.next_cursor,
                ..request.clone()
            },
        )
        .await
        .unwrap();
        assert_eq!(second.items[0].native_id, "person2");
        let third = super::super::search(
            account,
            ContactSearchRequest {
                page_cursor: second.next_cursor,
                ..request
            },
        )
        .await
        .unwrap();
        assert_eq!(third.items.len(), 1);
        assert!(third.next_cursor.is_none());
        let requests = script.requests();
        assert_eq!(requests.len(), 3);
        for (index, request) in requests.iter().enumerate() {
            assert_eq!(request.url.path(), "/EWS/Exchange.asmx");
            assert_eq!(
                request.headers["X-PublicFolderMailbox"],
                "content@example.test"
            );
            let body = std::str::from_utf8(request.body.as_ref().unwrap()).unwrap();
            assert!(body.contains(if index < 2 {
                "Offset=\"0\""
            } else {
                "Offset=\"17\""
            }));
            assert!(body.contains("<m:Restriction><t:Or>"));
            assert_eq!(
                body.matches("ContainmentMode=\"Substring\" ContainmentComparison=\"IgnoreCase\"")
                    .count(),
                6
            );
            for field in ["DisplayName", "GivenName", "Surname"] {
                assert!(body.contains(&format!("<t:FieldURI FieldURI=\"contacts:{field}\"/>")));
            }
            for index in 1..=3 {
                assert!(body.contains(&format!("<t:IndexedFieldURI FieldURI=\"contacts:EmailAddress\" FieldIndex=\"EmailAddress{index}\"/>")));
            }
            assert_eq!(
                body.matches("<t:Constant Value=\"A&lt;&amp;&quot;&apos;\"/>")
                    .count(),
                6
            );
        }
    }

    #[tokio::test]
    async fn nc2b_review_search_cursor_is_query_bound_and_never_falls_back_to_rest() {
        let (script, account) = account(vec![
            soap(CONTACT, false, Some(17)),
            soap(CONTACT, false, None),
        ]);
        let request = ContactSearchRequest {
            address_book_id: Some(foreign::public_address_book("people")),
            ..ContactSearchRequest::new("ada")
        };
        let error = super::super::search(account.clone(), request.clone())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), &AccountErrorKind::ConcurrencyConflict);
        assert!(script.requests().is_empty());
        seed(&account, "people", "IPF.Contact", true).await;
        let first = super::super::search(account.clone(), request.clone())
            .await
            .unwrap();
        for cursor in [
            first.next_cursor.clone().unwrap(),
            b"https://graph.test/v1.0/rest".to_vec(),
        ] {
            let error = super::super::search(
                account.clone(),
                ContactSearchRequest {
                    query: "different".into(),
                    page_cursor: Some(cursor),
                    ..request.clone()
                },
            )
            .await
            .unwrap_err();
            assert!(matches!(error.kind(), AccountErrorKind::Request(_)));
        }
        let error = super::super::search(
            account,
            ContactSearchRequest {
                page_cursor: first.next_cursor,
                ..request
            },
        )
        .await
        .unwrap_err();
        assert_eq!(
            error.kind(),
            &AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::ContractViolation)
        );
        assert_eq!(script.requests().len(), 2);
    }

    #[test]
    fn nc2b_contact_name_fallback_and_codec_are_native_safe() {
        let xml = get_soap(&CONTACT.replace("<t:DisplayName>Ada &amp; Grace</t:DisplayName>", ""));
        let nodes = ews::contacts::contact_nodes(&xml).unwrap();
        let card = project("people", &nodes[0]).unwrap();
        assert_eq!(card.display_name.as_deref(), Some("Ada Lovelace"));
        assert_eq!(
            foreign::parse_public_contact(&card.id.0),
            Some(("people", "person&1"))
        );
        assert_eq!(foreign::parse_public_address_book("people"), None);
        assert_eq!(foreign::parse_public_contact("rest-contact"), None);
    }
}
