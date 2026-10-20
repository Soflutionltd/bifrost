use super::*;
use bifrost_dav_core::DavResponse;
use bifrost_dav_core::test_support::{
    dav_redirect, dav_script, dav_script_yielding, scripted_dav_net, transcripts,
};
use bifrost_net::test_support::Canned;
use bifrost_types::{RecoveryClass, RequestErrorKind};
use reqwest::{StatusCode, header::HeaderMap};

const BASE: &str = "https://dav.example.test";

fn answer(status: StatusCode, body: impl Into<String>) -> Canned {
    DavResponse {
        status,
        headers: HeaderMap::new(),
        body: body.into(),
        url: String::new(),
    }
    .into()
}

fn multistatus(rows: &str) -> String {
    format!(
        r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">{rows}</D:multistatus>"#
    )
}

fn book(href: &str, marker: &str) -> String {
    format!(
        r#"<D:response><D:href>{href}</D:href><D:propstat><D:prop><D:resourcetype><D:collection/>{marker}</D:resourcetype></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>"#
    )
}

fn principal(gateways: &[&str]) -> String {
    let hrefs = gateways
        .iter()
        .map(|url| format!("<D:href>{url}</D:href>"))
        .collect::<String>();
    multistatus(&format!(
        r#"<D:response><D:href>/principal/</D:href><D:propstat><D:prop><C:addressbook-home-set><D:href>/books/</D:href></C:addressbook-home-set><C:directory-gateway>{hrefs}</C:directory-gateway></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>"#
    ))
}

fn discovery(gateways: &[&str], books: &str) -> Vec<Canned> {
    vec![
        answer(
            StatusCode::MULTI_STATUS,
            r#"<D:current-user-principal xmlns:D="DAV:"><D:href>/principal/</D:href></D:current-user-principal>"#,
        ),
        answer(StatusCode::MULTI_STATUS, principal(gateways)),
        answer(StatusCode::MULTI_STATUS, multistatus(books)),
    ]
}

fn card(href: &str, fields: &str) -> String {
    format!(
        r#"<D:response><D:href>{href}</D:href><D:propstat><D:prop><C:address-data>BEGIN:VCARD
VERSION:4.0
{fields}
END:VCARD</C:address-data></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>"#
    )
}

#[tokio::test]
async fn directory_discovery_admits_only_gateways_and_excludes_them_from_personal_surfaces() {
    for (gateways, expected) in [
        (vec![], 0),
        (vec!["/directory/"], 1),
        (
            vec![
                "/directory/",
                "https://other.example.test/gal/",
                "/directory/",
            ],
            2,
        ),
        (vec!["http://unsafe.example.test/gal/"], 0),
    ] {
        let books =
            book("/books/personal/", "<C:addressbook/>") + &book("/directory/", "<C:addressbook/>");
        let mut steps = discovery(&gateways, &books);
        steps.push(answer(StatusCode::MULTI_STATUS, multistatus(&books)));
        let script = dav_script(steps);
        let client = CardDavClient::with_account_net(BASE, scripted_dav_net(&script));
        let account = CardDavAccount::open_with_client(client)
            .await
            .expect("open");
        assert_eq!(account.directory_gateways.len(), expected);
        assert_eq!(
            account.capabilities().pim_methods.directory_search,
            expected > 0
        );
        assert!(!account.capabilities().pim_methods.directory_enumerate);
        assert_eq!(
            account.default_addressbook_url.as_deref(),
            Some("https://dav.example.test/books/personal/")
        );
        let listed = account.address_books_list().await.expect("books");
        assert_eq!(
            listed.len(),
            if gateways.contains(&"/directory/") {
                1
            } else {
                2
            }
        );
        assert_eq!(account.addressbook_urls.len(), listed.len());
        let scopes = account.discover_cursor_scopes().collect::<Vec<_>>().await;
        let SyncEvent::Batch(batch) = &scopes[0] else {
            panic!("scope batch");
        };
        assert_eq!(batch.items.len(), listed.len());
        assert!(batch.items.iter().all(|scope| matches!(scope, CursorScope::Folder(id) if !account.client.is_directory_url(&id.0))));

        let requests = transcripts(&script);
        assert!(requests[1].body.contains("<C:directory-gateway/>"));
        assert_eq!(requests.len(), 4);
    }
}

#[tokio::test]
async fn directory_marker_alone_opens_without_a_personal_default() {
    let books = book("/directory/", "<C:directory/>");
    let mut steps = discovery(&[], &books);
    steps.push(answer(StatusCode::MULTI_STATUS, multistatus(&books)));
    let script = dav_script(steps);
    let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
        BASE,
        scripted_dav_net(&script),
    ))
    .await
    .expect("open");
    assert_eq!(account.directory_gateways, [format!("{BASE}/directory/")]);
    assert!(account.default_addressbook_url.is_none());
    assert!(account.addressbook_urls.is_empty());
    assert!(
        account
            .address_books_list()
            .await
            .expect("books")
            .is_empty()
    );
    assert!(account.capabilities().pim_methods.directory_search);
}

#[tokio::test]
async fn directory_discovery_resolves_plural_hrefs_against_effective_principal_url() {
    let script = dav_script([
        answer(
            StatusCode::MULTI_STATUS,
            r#"<D:current-user-principal xmlns:D="DAV:"><D:href>/principal/</D:href></D:current-user-principal>"#,
        ),
        dav_redirect(StatusCode::TEMPORARY_REDIRECT, "/effective/principal/"),
        answer(
            StatusCode::MULTI_STATUS,
            principal(&["gal/", "../second/", "gal/"]),
        ),
    ]);
    let client = CardDavClient::with_account_net(BASE, scripted_dav_net(&script));
    let found = client.discover_addressbook_home().await.expect("discovery");
    assert_eq!(found.home, format!("{BASE}/books/"));
    assert_eq!(
        found.gateways,
        [
            format!("{BASE}/effective/principal/gal/"),
            format!("{BASE}/effective/second/")
        ]
    );
}

#[tokio::test]
async fn directory_query_projects_server_matches_and_sends_one_bounded_report() {
    let rows = card(
        "first.vcf",
        "FN:Server nickname match\nEMAIL:alias@example.test\nEMAIL;PREF=2:preferred@example.test\nEMAIL;PREF=9:other@example.test\nTEL:+123\nORG:Acme\\; Research;Engineering;London\nTITLE:Developer",
    ) + &card("no-email.vcf", "FN:No email");
    let mut steps = discovery(&["/directory/"], "");
    steps.push(answer(StatusCode::MULTI_STATUS, multistatus(&rows)));
    let script = dav_script(steps);
    let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
        BASE,
        scripted_dav_net(&script),
    ))
    .await
    .expect("open");
    let page = account
        .directory_search("nick & <name>".into(), Some(7), None)
        .await
        .expect("search");
    assert_eq!(page.items.len(), 1, "do not locally filter server matches");
    let row = &page.items[0];
    assert_eq!(row.email, "preferred@example.test");
    assert_eq!(
        row.additional_emails,
        ["alias@example.test", "other@example.test"]
    );
    assert_eq!(row.company.as_deref(), Some("Acme; Research"));
    assert_eq!(row.department.as_deref(), Some("Engineering; London"));
    assert_eq!(row.title.as_deref(), Some("Developer"));
    assert_eq!(row.phones, ["+123"]);
    assert_eq!(row.provider, ProtocolKind::CardDav);
    assert!(!page.truncated);
    assert!(page.next_cursor.is_none() && page.estimated_total.is_none());
    let requests = transcripts(&script);
    assert_eq!(requests.len(), 4);
    let request = &requests[3];
    assert_eq!(request.method.as_str(), "REPORT");
    assert_eq!(request.headers["depth"], "1");
    assert!(request.body.contains("<C:address-data/>"));
    assert!(request.body.contains("<C:filter test=\"anyof\">"));
    assert_eq!(request.body.matches("<C:prop-filter").count(), 4);
    for name in ["FN", "EMAIL", "N", "NICKNAME"] {
        assert!(request.body.contains(&format!("name=\"{name}\"")));
    }
    assert_eq!(
        request
            .body
            .matches("collation=\"i;unicode-casemap\" match-type=\"contains\"")
            .count(),
        4
    );
    assert!(request.body.contains("nick &amp; &lt;name&gt;"));
    assert!(
        request
            .body
            .contains("<C:limit><C:nresults>7</C:nresults></C:limit>")
    );
}

#[tokio::test]
async fn directory_collection_507_is_truncation_with_rows_or_without_precondition() {
    for with_precondition in [false, true] {
        for with_rows in [false, true] {
            let rows = if with_rows {
                card("one.vcf", "EMAIL:one@example.test")
            } else {
                String::new()
            };
            let error = if with_precondition {
                "<D:error><D:number-of-matches-within-limits/></D:error>"
            } else {
                ""
            };
            let body = multistatus(&format!(
                "{rows}<D:response><D:href>/directory/</D:href><D:status>HTTP/1.1 507 Insufficient Storage</D:status>{error}</D:response>"
            ));
            let mut steps = discovery(&["/directory/"], "");
            steps.push(answer(StatusCode::MULTI_STATUS, body));
            let script = dav_script(steps);
            let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
                BASE,
                scripted_dav_net(&script),
            ))
            .await
            .expect("open");
            let page = account
                .directory_search("one".into(), Some(1), None)
                .await
                .expect("truncated success");
            assert_eq!(page.items.len(), usize::from(with_rows));
            assert!(page.truncated);
            assert!(page.failed_ids.is_empty() && page.skipped_scopes.is_empty());
            assert!(page.next_cursor.is_none() && page.estimated_total.is_none());
        }
    }
}

#[tokio::test]
async fn directory_local_limit_and_multi_gateway_dedup_are_global_and_deterministic() {
    let mut steps = discovery(&["/z/", "/a/"], "");
    steps.extend([
        answer(
            StatusCode::MULTI_STATUS,
            multistatus(
                &(card(
                    "one.vcf",
                    "EMAIL: ONE@Example.test \nEMAIL:shared@example.test\nFN:First",
                ) + &card(
                    "two.vcf",
                    "EMAIL:two@example.test\nEMAIL:shared@example.test",
                )),
            ),
        ),
        answer(
            StatusCode::MULTI_STATUS,
            multistatus(
                &(card("duplicate.vcf", "EMAIL:one@example.test\nFN:Duplicate")
                    + &card("three.vcf", "EMAIL:three@example.test")
                    + &card("four.vcf", "EMAIL:four@example.test")),
            ),
        ),
    ]);
    let script = dav_script_yielding(steps);
    let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
        BASE,
        scripted_dav_net(&script),
    ))
    .await
    .expect("open");
    let page = account
        .directory_search("anything".into(), Some(3), None)
        .await
        .expect("merge");
    assert_eq!(
        page.items
            .iter()
            .map(|c| c.email.as_str())
            .collect::<Vec<_>>(),
        ["ONE@Example.test", "two@example.test", "three@example.test"]
    );
    assert_eq!(page.items[0].display_name.as_deref(), Some("First"));
    assert!(page.truncated);
    let requests = transcripts(&script);
    assert_eq!(
        [requests[3].url.as_str(), requests[4].url.as_str()],
        ["https://dav.example.test/a/", "https://dav.example.test/z/"]
    );
    assert!(script.peak_in_flight() > 1);
}

#[tokio::test]
async fn directory_fanout_is_bounded() {
    let mut steps = discovery(&["/1/", "/2/", "/3/", "/4/", "/5/", "/6/"], "");
    steps.extend((0..6).map(|_| answer(StatusCode::MULTI_STATUS, multistatus(""))));
    let script = dav_script_yielding(steps);
    let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
        BASE,
        scripted_dav_net(&script),
    ))
    .await
    .expect("open");
    account
        .directory_search("q".into(), None, None)
        .await
        .expect("search");
    assert_eq!(script.peak_in_flight(), 4);
    assert_eq!(transcripts(&script).len(), 9);
}

#[tokio::test]
async fn directory_partial_failure_identifies_gateway_and_all_failed_is_error() {
    for all_failed in [false, true] {
        let mut steps = discovery(&["/a/", "/b/"], "");
        steps.push(answer(StatusCode::FORBIDDEN, "permission denied"));
        steps.push(if all_failed {
            answer(StatusCode::UNAUTHORIZED, "expired")
        } else {
            answer(
                StatusCode::MULTI_STATUS,
                multistatus(&card("one.vcf", "EMAIL:one@example.test")),
            )
        });
        if all_failed {
            // DAV retries one bearer-authenticated 401 after refreshing the
            // token. The second 401 is the terminal gateway error.
            steps.push(answer(StatusCode::UNAUTHORIZED, "expired"));
        }
        let script = dav_script(steps);
        let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
            BASE,
            scripted_dav_net(&script),
        ))
        .await
        .expect("open");
        let result = account.directory_search("one".into(), None, None).await;
        if all_failed {
            assert!(result.is_err());
        } else {
            let page = result.expect("partial success");
            assert_eq!(page.items.len(), 1);
            assert_eq!(page.skipped_scopes.len(), 1);
            assert_eq!(
                page.skipped_scopes[0].scope,
                ErrorScope::DirectoryGateway {
                    url: format!("{BASE}/a/")
                }
            );
            assert!(matches!(
                page.skipped_scopes[0].error.recovery(),
                RecoveryClass::NoPermission { .. }
            ));
        }
    }
}

#[tokio::test]
async fn directory_filter_refusals_never_enumerate() {
    for (status, body) in [
        (StatusCode::BAD_REQUEST, ""),
        (StatusCode::METHOD_NOT_ALLOWED, ""),
        (StatusCode::NOT_IMPLEMENTED, ""),
        (
            StatusCode::FORBIDDEN,
            r#"<D:error xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav"><C:supported-filter/></D:error>"#,
        ),
    ] {
        let mut steps = discovery(&["/directory/"], "");
        // The transport retries a 501 before directory classification.
        let attempts = if status == StatusCode::NOT_IMPLEMENTED {
            bifrost_dav_core::test_support::dav_retried(DavResponse {
                status,
                headers: HeaderMap::new(),
                body: body.into(),
                url: String::new(),
            })
        } else {
            vec![answer(status, body)]
        };
        steps.extend(attempts);
        let script = dav_script(steps);
        let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
            BASE,
            scripted_dav_net(&script),
        ))
        .await
        .expect("open");
        let error = account
            .directory_search("q".into(), None, None)
            .await
            .expect_err("filter refusal");
        assert!(matches!(
            error.kind(),
            AccountErrorKind::Unsupported(AccountOperation::DirectorySearch)
        ));
        assert!(
            transcripts(&script)[3..]
                .iter()
                .all(|r| r.method.as_str() == "REPORT")
        );
    }
}

#[tokio::test]
async fn directory_empty_cursor_and_false_capability_refuse_locally() {
    for enabled in [false, true] {
        let steps = discovery(if enabled { &["/directory/"] } else { &[] }, "");
        let script = dav_script(steps);
        let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
            BASE,
            scripted_dav_net(&script),
        ))
        .await
        .expect("open");
        for query in ["", "   ", "q"] {
            let error = account
                .directory_search(query.into(), Some(3), Some(vec![255]))
                .await
                .expect_err("local refusal");
            if !enabled || query.trim().is_empty() {
                assert!(matches!(
                    error.kind(),
                    AccountErrorKind::Unsupported(AccountOperation::DirectorySearch)
                ));
            } else {
                assert!(matches!(
                    error.kind(),
                    AccountErrorKind::Request(RequestErrorKind::Malformed)
                ));
            }
        }
        assert_eq!(transcripts(&script).len(), 3);
    }
}

#[tokio::test]
async fn directory_every_write_door_refuses_before_any_request_and_respects_boundaries() {
    let books = book("/marked/", "<C:directory/>") + &book("/books/personal/", "<C:addressbook/>");
    let script = dav_script(discovery(
        &["/directory/", "http://unsafe.example.test/gal/"],
        &books,
    ));
    let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
        BASE,
        scripted_dav_net(&script),
    ))
    .await
    .expect("open");
    for gateway in ["/directory/", "/marked/", "http://unsafe.example.test/gal/"] {
        let created = account
            .contact_create(ContactCreate {
                address_book_id: Some(AddressBookId(gateway.into())),
                ..Default::default()
            })
            .await;
        let updated = account
            .contact_update(
                ContactId(format!("{gateway}one.vcf")),
                ContactPatch::default(),
            )
            .await;
        let moved = account
            .contact_update(
                ContactId("/books/personal/one.vcf".into()),
                ContactPatch {
                    address_book_id: Some(AddressBookId(gateway.into())),
                    ..Default::default()
                },
            )
            .await;
        let deleted = account
            .contact_delete(ContactId(format!("{gateway}one.vcf")))
            .await;
        for error in [created.map(|_| ()), updated, moved, deleted] {
            assert!(matches!(
                error.expect_err("read-only gateway").kind(),
                AccountErrorKind::Unsupported(_)
            ));
        }
    }
    for url in [
        "/directory",
        "/directory/sub/one.vcf",
        "/marked/one.vcf",
        "/directory/../directory/one.vcf",
    ] {
        assert!(account.client.is_directory_url(url), "{url}");
    }
    for url in [
        "/directory-other/one.vcf",
        "https://other.example.test/directory/one.vcf",
        "/books/personal/one.vcf",
    ] {
        assert!(!account.client.is_directory_url(url), "{url}");
    }
    assert_eq!(transcripts(&script).len(), 3);
}

#[tokio::test]
async fn directory_single_gateway_local_overflow_and_zero_limit() {
    for limit in [0, 1] {
        let mut steps = discovery(&["/directory/"], "");
        if limit > 0 {
            steps.push(answer(
                StatusCode::MULTI_STATUS,
                multistatus(
                    &(card("one.vcf", "EMAIL:one@example.test")
                        + &card("two.vcf", "EMAIL:two@example.test")),
                ),
            ));
        }
        let script = dav_script(steps);
        let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
            BASE,
            scripted_dav_net(&script),
        ))
        .await
        .expect("open");
        let page = account
            .directory_search("q".into(), Some(limit), None)
            .await
            .expect("search");
        assert_eq!(page.items.len(), limit as usize);
        assert_eq!(page.truncated, limit > 0);
        assert!(page.next_cursor.is_none());
        assert_eq!(transcripts(&script).len(), if limit > 0 { 4 } else { 3 });
    }
}

#[tokio::test]
async fn directory_preference_skips_blank_emails_and_keeps_failed_resource_ids() {
    let rows = card(
        "preferred.vcf",
        "EMAIL;PREF=1:   \nEMAIL:first@example.test\nEMAIL;PREF=2:preferred@example.test",
    ) + &card("broken.vcf", "EMAIL;TYPE=\"unterminated:value")
        + r#"<D:response><D:href>missing.vcf</D:href><D:propstat><D:prop/><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>"#
        + r#"<D:response><D:href>refused.vcf</D:href><D:status>HTTP/1.1 403 Forbidden</D:status></D:response>"#;
    let mut steps = discovery(&["/directory/"], "");
    steps.push(answer(StatusCode::MULTI_STATUS, multistatus(&rows)));
    let script = dav_script(steps);
    let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
        BASE,
        scripted_dav_net(&script),
    ))
    .await
    .expect("open");
    let page = account
        .directory_search("q".into(), None, None)
        .await
        .expect("search");
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].email, "preferred@example.test");
    assert_eq!(page.items[0].additional_emails, ["first@example.test"]);
    let mut failed = page.failed_ids;
    failed.sort();
    assert_eq!(
        failed,
        [
            format!("{BASE}/directory/broken.vcf"),
            format!("{BASE}/directory/missing.vcf"),
            format!("{BASE}/directory/refused.vcf")
        ]
    );
}

#[tokio::test]
async fn directory_member_507_remains_an_ordinary_quota_failure() {
    let mut steps = discovery(&["/directory/"], "");
    steps.push(answer(StatusCode::MULTI_STATUS, multistatus(r#"<D:response><D:href>one.vcf</D:href><D:status>HTTP/1.1 507 Insufficient Storage</D:status></D:response>"#)));
    let script = dav_script(steps);
    let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
        BASE,
        scripted_dav_net(&script),
    ))
    .await
    .expect("open");
    let error = account
        .directory_search("q".into(), None, None)
        .await
        .expect_err("resource failure");
    assert!(matches!(
        error.kind(),
        AccountErrorKind::Server(bifrost_types::ServerErrorKind::QuotaExhausted)
    ));
}

#[test]
fn directory_url_dedup_and_membership_use_resolved_dav_identity() {
    let mut urls = vec![
        format!("{BASE}/%64irectory/"),
        format!("{BASE}/between/"),
        format!("{BASE}/directory/"),
    ];
    crate::client::dedup_directory_urls(&mut urls);
    assert_eq!(urls.len(), 2);
    let script = bifrost_dav_core::test_support::dav_script_empty();
    let mut client = CardDavClient::with_account_net(BASE, scripted_dav_net(&script));
    client.initialize_directories([format!("{BASE}/%64irectory/")], Vec::new());
    for uri in [
        "/directory/one.vcf",
        "/%64irectory/one.vcf",
        "/directory?query=1",
    ] {
        assert!(client.is_directory_url(uri));
    }
    assert!(!client.is_directory_url("/directory%2Fother/one.vcf"));
}

#[tokio::test]
async fn directory_review_foreign_listing_marker_never_delegates_credentials() {
    for advertise_principal_gateway in [false, true] {
        let books = book("https://unrelated.example/gal/", "<C:directory/>");
        let mut steps = discovery(
            if advertise_principal_gateway {
                &["/directory/"]
            } else {
                &[]
            },
            &books,
        );
        if advertise_principal_gateway {
            steps.push(answer(
                StatusCode::MULTI_STATUS,
                multistatus(&card("one.vcf", "EMAIL:one@example.test")),
            ));
        }
        let script = dav_script(steps);
        let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
            BASE,
            scripted_dav_net(&script),
        ))
        .await
        .expect("open");
        assert!(
            !account
                .client
                .is_trusted_url("https://unrelated.example/gal/")
        );
        assert!(
            account
                .client
                .is_directory_url("https://unrelated.example/gal/one.vcf")
        );
        assert!(account.default_addressbook_url.is_none() && account.addressbook_urls.is_empty());
        assert_eq!(
            account.capabilities().pim_methods.directory_search,
            advertise_principal_gateway
        );
        let result = account.directory_search("q".into(), None, None).await;
        if advertise_principal_gateway {
            assert_eq!(result.expect("principal gateway works").items.len(), 1);
        } else {
            assert!(matches!(
                result.expect_err("no trusted gateway").kind(),
                AccountErrorKind::Unsupported(AccountOperation::DirectorySearch)
            ));
        }
        assert!(
            transcripts(&script)
                .iter()
                .all(|request| request.url.starts_with(BASE)),
            "no request or credential reaches the foreign listing origin"
        );
    }
}

#[tokio::test]
async fn directory_round_two_later_markers_tighten_access_without_widening_snapshots() {
    let personal = book("/books/personal/", "<C:addressbook/>");
    let changed = book("/books/personal/", "<C:addressbook/><C:directory/>")
        + &book("https://unrelated.example/late/", "<C:directory/>");
    let mut steps = discovery(&["/directory/"], &personal);
    steps.extend([
        answer(StatusCode::MULTI_STATUS, multistatus(&changed)),
        answer(StatusCode::MULTI_STATUS, multistatus(&personal)),
        answer(StatusCode::MULTI_STATUS, multistatus("")),
    ]);
    let script = dav_script(steps);
    let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
        BASE,
        scripted_dav_net(&script),
    ))
    .await
    .expect("open");
    let initial_default = account.default_addressbook_url.clone();
    let initial_scopes = account.addressbook_urls.clone();
    let initial_gateways = account.directory_gateways.clone();
    let initial_capability = account.capabilities().pim_methods.directory_search;
    // Live observations tighten access; origin trust and search stay frozen.
    assert!(
        account
            .address_books_list()
            .await
            .expect("listing")
            .is_empty()
    );
    assert!(account.client.is_directory_url("/books/personal/one.vcf"));
    let refused = account
        .contact_delete(ContactId("/directory/one.vcf".into()))
        .await
        .expect_err("original gateway stays read-only");
    assert!(matches!(
        refused.kind(),
        AccountErrorKind::Unsupported(AccountOperation::ContactDelete)
    ));

    assert!(
        account
            .client
            .is_directory_url("https://unrelated.example/late/one.vcf")
    );
    for after_marker_disappears in [false, true] {
        if after_marker_disappears {
            assert!(
                account
                    .address_books_list()
                    .await
                    .expect("marker disappeared")
                    .is_empty()
            );
        }
        let error = account
            .contact_delete(ContactId("/books/personal/one.vcf".into()))
            .await
            .expect_err("cached contact is now read-only");
        assert!(matches!(
            error.kind(),
            AccountErrorKind::Unsupported(AccountOperation::ContactDelete)
        ));
        let error = account
            .contact_update(
                ContactId("/books/personal/one.vcf".into()),
                ContactPatch::default(),
            )
            .await
            .expect_err("no GET or PUT after a marker");
        assert!(matches!(
            error.kind(),
            AccountErrorKind::Unsupported(AccountOperation::ContactUpdate)
        ));
        assert!(matches!(
            account
                .contacts_list(None, None)
                .await
                .expect_err("cached personal default is excluded")
                .kind(),
            AccountErrorKind::Unsupported(AccountOperation::ContactsList)
        ));
        assert!(matches!(
            account
                .contact_get(ContactId("/books/personal/one.vcf".into()))
                .await
                .expect_err("cached personal id is excluded")
                .kind(),
            AccountErrorKind::Unsupported(AccountOperation::ContactGet)
        ));
        assert!(matches!(
            account
                .contact_search(ContactSearchRequest::new(""))
                .await
                .expect_err("cached search default is excluded")
                .kind(),
            AccountErrorKind::Unsupported(AccountOperation::ContactSearch)
        ));
        let error = account
            .contact_create(ContactCreate::default())
            .await
            .expect_err("cached default is now read-only");
        assert!(matches!(
            error.kind(),
            AccountErrorKind::Unsupported(AccountOperation::ContactCreate)
        ));
    }
    account
        .directory_search("q".into(), None, None)
        .await
        .expect("original gateway stays live");
    assert_eq!(account.default_addressbook_url, initial_default);
    assert_eq!(account.addressbook_urls, initial_scopes);
    assert_eq!(account.directory_gateways, initial_gateways);
    assert_eq!(
        account.capabilities().pim_methods.directory_search,
        initial_capability
    );
    let requests = transcripts(&script);
    assert_eq!(requests.len(), 6);
    assert_eq!(requests[4].method.as_str(), "PROPFIND");
    assert_eq!(requests[5].url, format!("{BASE}/directory/"));
    assert!(
        !account
            .client
            .is_trusted_url("https://unrelated.example/late/")
    );
}

#[tokio::test]
async fn directory_review_redirected_writes_check_every_source_and_rebased_destination() {
    for status in [
        StatusCode::TEMPORARY_REDIRECT,
        StatusCode::PERMANENT_REDIRECT,
    ] {
        for door in [
            "create-put",
            "update-put",
            "delete",
            "move-source",
            "move-destination",
        ] {
            let gateway = if door == "move-destination" {
                "/blocked/b/"
            } else {
                "/blocked/a/"
            };
            let mut steps = discovery(&[gateway], "");
            steps.extend([
                dav_redirect(status, "/stage/a/one.vcf"),
                dav_redirect(status, "/blocked/a/one.vcf"),
            ]);
            let script = dav_script(steps);
            let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
                BASE,
                scripted_dav_net(&script),
            ))
            .await
            .expect("open");
            let source = format!("{BASE}/books/a/one.vcf");
            let error = match door {
                "create-put" | "update-put" => account
                    .client
                    .put_vcard(
                        &source,
                        "BEGIN:VCARD\nEND:VCARD".into(),
                        if door == "create-put" {
                            PutCondition::IfNoneMatch
                        } else {
                            PutCondition::IfMatch("v1")
                        },
                        AccountOperation::ContactUpdate,
                    )
                    .await
                    .map(|_| ()),
                "delete" => account
                    .client
                    .delete_vcard(&source, AccountOperation::ContactDelete)
                    .await
                    .map(|_| ()),
                _ => account
                    .client
                    .move_resource(
                        &source,
                        &format!("{BASE}/books/b/one.vcf"),
                        AccountOperation::ContactUpdate,
                    )
                    .await
                    .map(|_| ()),
            }
            .expect_err("refuse before the gateway hop");
            let unreplayable = matches!(door, "create-put" | "move-source" | "move-destination");
            if unreplayable {
                assert!(
                    matches!(
                        error.kind(),
                        AccountErrorKind::Protocol(
                            bifrost_types::ProtocolErrorKind::PartialResponse
                        )
                    ),
                    "{door}: {error:?}"
                );
                assert!(
                    matches!(error.recovery(), RecoveryClass::Reconcile(_)),
                    "an unreplayable redirected write requires reconciliation: {error:?}"
                );
                assert!(
                    error.chain().iter().any(|cause| matches!(
                        cause,
                        bifrost_types::Cause::Request(
                            bifrost_types::RequestCause::Unsupported { .. }
                        )
                    )),
                    "the directory refusal remains in the evidence: {error:?}"
                );
            } else {
                assert!(
                    matches!(error.kind(), AccountErrorKind::Unsupported(_)),
                    "{door}: {error:?}"
                );
            }
            assert!(error.chain().iter().any(|cause| matches!(cause, Cause::Attempt(attempt) if attempt.transmission_state == TransmissionState::Acknowledged)), "answered redirect evidence: {door}");
            let requests = transcripts(&script);
            assert_eq!(requests.len(), 5, "discovery plus two safe hops: {door}");
            assert_eq!(requests[3].url, source);
            assert_eq!(requests[4].url, format!("{BASE}/stage/a/one.vcf"));
            assert_eq!(requests[4].headers["authorization"], "Bearer token");
            if door.starts_with("move") {
                assert_eq!(
                    requests[4].headers["destination"],
                    format!("{BASE}/stage/b/one.vcf")
                );
            }
        }
    }
}

#[tokio::test]
async fn directory_review_redirected_writes_to_siblings_still_work() {
    for door in ["put", "delete", "move"] {
        let mut steps = discovery(&["/protected/"], "");
        steps.extend([
            dav_redirect(StatusCode::TEMPORARY_REDIRECT, "/protected-other/a/one.vcf"),
            answer(StatusCode::NO_CONTENT, ""),
        ]);
        let script = dav_script(steps);
        let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
            BASE,
            scripted_dav_net(&script),
        ))
        .await
        .expect("open");
        let source = format!("{BASE}/books/a/one.vcf");
        match door {
            "put" => account
                .client
                .put_vcard(
                    &source,
                    "card".into(),
                    PutCondition::IfMatch("v1"),
                    AccountOperation::ContactUpdate,
                )
                .await
                .map(|_| ()),
            "delete" => account
                .client
                .delete_vcard(&source, AccountOperation::ContactDelete)
                .await
                .map(|_| ()),
            _ => account
                .client
                .move_resource(
                    &source,
                    &format!("{BASE}/books/b/one.vcf"),
                    AccountOperation::ContactUpdate,
                )
                .await
                .map(|_| ()),
        }
        .expect("safe redirected write");
        let requests = transcripts(&script);
        assert_eq!(requests.len(), 5);
        assert_eq!(requests[4].url, format!("{BASE}/protected-other/a/one.vcf"));
        if door == "move" {
            assert_eq!(
                requests[4].headers["destination"],
                format!("{BASE}/protected-other/b/one.vcf")
            );
        }
    }
}

#[test]
fn directory_round_two_projection_keeps_first_title_without_changing_personal_policy() {
    for fields in [
        "ORG:Acme\nTITLE:Engineer\nTITLE:Manager",
        "TITLE:Engineer\nTITLE:Manager",
        "ORG:Acme\nTITLE:Engineer\nTITLE:Manager\nORG:Other\nTITLE:Director",
    ] {
        let data =
            format!("BEGIN:VCARD\nVERSION:4.0\nEMAIL:person@example.test\n{fields}\nEND:VCARD\n");
        let directory = crate::vcard::directory_from_vcard(&data)
            .expect("decode")
            .expect("email");
        assert_eq!(directory.title.as_deref(), Some("Engineer"));
        let personal = contact_from_vcard(
            "https://dav.example.test/book/one.vcf".into(),
            None,
            None,
            &data,
        )
        .expect("personal");
        assert_eq!(personal.organizations[0].title.as_deref(), Some("Manager"));
    }
}

#[tokio::test]
async fn directory_round_two_personal_read_doors_refuse_gateway_members_before_io() {
    let books = book("/marked/", "<C:directory/>");
    let mut steps = discovery(&["/directory/", "http://unsafe.example.test/gal/"], &books);
    steps.extend((0..2).map(|_| answer(StatusCode::MULTI_STATUS, multistatus(""))));
    let script = dav_script(steps);
    let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
        BASE,
        scripted_dav_net(&script),
    ))
    .await
    .expect("open");
    for gateway in [
        "/directory",
        "/directory/nested/",
        "/%64irectory/",
        "https://DAV.EXAMPLE.test:443/directory/",
        "/marked/",
        "http://unsafe.example.test/gal/",
    ] {
        let listed = account
            .contacts_list(Some(AddressBookId(gateway.into())), None)
            .await
            .expect_err("no personal listing");
        assert!(matches!(
            listed.kind(),
            AccountErrorKind::Unsupported(AccountOperation::ContactsList)
        ));
        let got = account
            .contact_get(ContactId(gateway.into()))
            .await
            .expect_err("no personal get");
        assert!(matches!(
            got.kind(),
            AccountErrorKind::Unsupported(AccountOperation::ContactGet)
        ));
        for query in ["", "needle"] {
            let mut request = ContactSearchRequest::new(query);
            request.address_book_id = Some(AddressBookId(gateway.into()));
            let searched = account
                .contact_search(request)
                .await
                .expect_err("no personal search");
            assert!(matches!(
                searched.kind(),
                AccountErrorKind::Unsupported(AccountOperation::ContactSearch)
            ));
        }
    }
    assert_eq!(transcripts(&script).len(), 3);
    account
        .directory_search("needle".into(), None, None)
        .await
        .expect("bounded directory search stays available");
    assert_eq!(transcripts(&script).len(), 5);
}

#[tokio::test]
async fn directory_round_two_personal_read_redirects_cannot_enter_gateways() {
    for door in ["list", "get", "search"] {
        let mut steps = discovery(
            &["/directory/"],
            &book("/books/personal/", "<C:addressbook/>"),
        );
        steps.push(dav_redirect(StatusCode::TEMPORARY_REDIRECT, "/directory/"));
        let script = dav_script(steps);
        let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
            BASE,
            scripted_dav_net(&script),
        ))
        .await
        .expect("open");
        let result = match door {
            "list" => account
                .contacts_list(Some(AddressBookId("/books/personal/".into())), None)
                .await
                .map(|_| ()),
            "get" => account
                .contact_get(ContactId("/books/personal/one.vcf".into()))
                .await
                .map(|_| ()),
            _ => {
                let mut request = ContactSearchRequest::new("");
                request.address_book_id = Some(AddressBookId("/books/personal/".into()));
                account.contact_search(request).await.map(|_| ())
            }
        };
        assert!(matches!(
            result.expect_err("refuse redirected personal read").kind(),
            AccountErrorKind::Unsupported(_)
        ));
        assert_eq!(transcripts(&script).len(), 4, "no gateway hop for {door}");
    }
}

#[tokio::test]
async fn directory_round_two_personal_reads_still_admit_sibling_paths() {
    for door in ["list", "get", "search"] {
        let mut steps = discovery(&["/directory/"], "");
        steps.push(if door == "get" {
            answer(StatusCode::OK, "BEGIN:VCARD\nFN:Sibling\nEND:VCARD")
        } else {
            answer(StatusCode::MULTI_STATUS, multistatus(""))
        });
        let script = dav_script(steps);
        let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
            BASE,
            scripted_dav_net(&script),
        ))
        .await
        .expect("open");
        match door {
            "list" => account
                .contacts_list(Some(AddressBookId("/directory-other/".into())), None)
                .await
                .map(|_| ()),
            "get" => account
                .contact_get(ContactId("/directory-other/one.vcf".into()))
                .await
                .map(|_| ()),
            _ => {
                let mut request = ContactSearchRequest::new("");
                request.address_book_id = Some(AddressBookId("/directory-other/".into()));
                account.contact_search(request).await.map(|_| ())
            }
        }
        .expect("sibling is personal");
        assert_eq!(transcripts(&script).len(), 4);
    }
}

#[tokio::test]
async fn directory_round_two_late_markers_reach_the_redirect_guard() {
    for door in ["put", "delete", "move-source", "move-destination"] {
        let personal = book("/books/a/", "<C:addressbook/>");
        let marked = book(
            if door == "move-destination" {
                "/late/b/"
            } else {
                "/late/a/"
            },
            "<C:directory/>",
        );
        let mut steps = discovery(&["/directory/"], &personal);
        steps.extend([
            answer(StatusCode::MULTI_STATUS, multistatus(&(personal + &marked))),
            dav_redirect(StatusCode::TEMPORARY_REDIRECT, "/late/a/one.vcf"),
        ]);
        let script = dav_script(steps);
        let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
            BASE,
            scripted_dav_net(&script),
        ))
        .await
        .expect("open");
        account
            .address_books_list()
            .await
            .expect("observe late marker");
        let source = format!("{BASE}/books/a/one.vcf");
        let result = match door {
            "put" => account
                .client
                .put_vcard(
                    &source,
                    "card".into(),
                    PutCondition::IfMatch("v1"),
                    AccountOperation::ContactUpdate,
                )
                .await
                .map(|_| ()),
            "delete" => account
                .client
                .delete_vcard(&source, AccountOperation::ContactDelete)
                .await
                .map(|_| ()),
            _ => account
                .client
                .move_resource(
                    &source,
                    &format!("{BASE}/books/b/one.vcf"),
                    AccountOperation::ContactUpdate,
                )
                .await
                .map(|_| ()),
        };
        let error = result.expect_err("later marker is shared with every redirect guard");
        if door.starts_with("move") {
            assert!(matches!(
                error.kind(),
                AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::PartialResponse)
            ));
            assert!(matches!(error.recovery(), RecoveryClass::Reconcile(_)));
            assert!(error.chain().iter().any(|cause| matches!(
                cause,
                bifrost_types::Cause::Request(bifrost_types::RequestCause::Unsupported { .. })
            )));
        } else {
            assert!(matches!(error.kind(), AccountErrorKind::Unsupported(_)));
        }
        assert!(error.chain().iter().any(|cause| matches!(
            cause,
            bifrost_types::Cause::Attempt(attempt)
                if attempt.transmission_state == bifrost_types::TransmissionState::Acknowledged
        )));
        assert_eq!(
            transcripts(&script).len(),
            5,
            "no write to newly marked directory: {door}"
        );
    }
}

#[tokio::test]
async fn directory_round_three_update_prefetch_redirect_is_refused() {
    let mut steps = discovery(
        &["/directory/"],
        &book("/books/personal/", "<C:addressbook/>"),
    );
    steps.push(dav_redirect(
        StatusCode::TEMPORARY_REDIRECT,
        "/directory/one.vcf",
    ));
    let script = dav_script(steps);
    let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
        BASE,
        scripted_dav_net(&script),
    ))
    .await
    .expect("open");
    let error = account
        .contact_update(
            ContactId("/books/personal/one.vcf".into()),
            ContactPatch::default(),
        )
        .await
        .expect_err("no prefetch GET to a directory");
    assert!(matches!(
        error.kind(),
        AccountErrorKind::Unsupported(AccountOperation::ContactUpdate)
    ));
    let requests = transcripts(&script);
    assert_eq!(requests.len(), 4);
    assert_eq!(requests[3].method, reqwest::Method::GET);
    assert_eq!(requests[3].url, format!("{BASE}/books/personal/one.vcf"));
    assert!(error.chain().iter().any(|cause| matches!(cause, Cause::Attempt(attempt) if attempt.transmission_state == TransmissionState::Acknowledged)));
}

fn assert_directory_scope_retired(
    error: &AccountError,
    scope: &CursorScope,
    operation: AccountOperation,
) {
    assert!(matches!(
        error.kind(),
        AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::ScopeRevoked)
    ));
    assert_eq!(error.operation(), Some(operation));
    assert_eq!(error.scope(), Some(&ErrorScope::Cursor(scope.clone())));
    assert!(
        matches!(error.recovery(), RecoveryClass::Engine(bifrost_types::EngineDirective::DisableScope(retired)) if retired == scope)
    );
}

#[tokio::test]
async fn directory_round_three_inventory_and_establishment_retire_after_later_marker() {
    let personal = book("/books/personal/", "<C:addressbook/>");
    let marked = book("/books/personal/", "<C:addressbook/><C:directory/>");
    for learn_inside_snapshot in [false, true] {
        let mut steps = discovery(&[], &personal);
        steps.push(answer(StatusCode::MULTI_STATUS, multistatus(&marked)));
        let script = dav_script(steps);
        let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
            BASE,
            scripted_dav_net(&script),
        ))
        .await
        .expect("open");
        let scope = CursorScope::Folder(bifrost_types::FolderId(format!("{BASE}/books/personal/")));
        if !learn_inside_snapshot {
            account
                .address_books_list()
                .await
                .expect("observe directory");
        }
        let events = account
            .inventory_stream(scope.clone())
            .collect::<Vec<_>>()
            .await;
        let [InventoryEvent::Terminated(error)] = events.as_slice() else {
            panic!(
                "directory inventory must terminate without entries, coverage or checkpoint: {events:?}"
            );
        };
        assert_directory_scope_retired(error, &scope, AccountOperation::SyncInventory);
        let discovery = account.discover_cursor_scopes().collect::<Vec<_>>().await;
        let SyncEvent::Batch(batch) = &discovery[0] else {
            panic!("scope discovery batch");
        };
        assert!(
            batch.items.is_empty(),
            "retired scopes must not be advertised again"
        );

        let error = account
            .establish_initial_cursor(scope.clone())
            .await
            .expect_err("no new personal cursor for a directory");
        assert_directory_scope_retired(&error, &scope, AccountOperation::EstablishCursor);
        assert_eq!(
            transcripts(&script).len(),
            4,
            "no member listing after the marker"
        );
    }
}

#[tokio::test]
async fn directory_round_three_existing_change_cursor_retires_after_later_marker() {
    let personal = book("/books/personal/", "<C:addressbook/>");
    let marked = book("/books/personal/", "<C:addressbook/><C:directory/>");
    for scope in [
        CursorScope::Folder(bifrost_types::FolderId(format!("{BASE}/books/personal/"))),
        CursorScope::Type(ObjectType::Contact),
    ] {
        let mut steps = discovery(&[], &personal);
        steps.push(answer(StatusCode::MULTI_STATUS, multistatus(&marked)));
        let script = dav_script(steps);
        let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
            BASE,
            scripted_dav_net(&script),
        ))
        .await
        .expect("open");
        let cursor = cursor_from_snapshot(
            scope.clone(),
            &ContactSnapshot {
                addressbook_url: format!("{BASE}/books/personal/"),
                ctag: Some("unchanged".into()),
                entries: vec![ContactSnapshotEntry {
                    uri: format!("{BASE}/books/personal/cached.vcf"),
                    etag: Some("v1".into()),
                }],
                failed_hrefs: Vec::new(),
            },
        );
        account
            .address_books_list()
            .await
            .expect("observe directory");
        let events = account.changes_stream(cursor).collect::<Vec<_>>().await;
        let [SyncEvent::Terminated(error)] = events.as_slice() else {
            panic!("no poll, personal changes or checkpoint: {events:?}");
        };
        assert_directory_scope_retired(error, &scope, AccountOperation::SyncChanges);
        assert_eq!(
            transcripts(&script).len(),
            4,
            "not even a ctag probe may reach the directory"
        );
    }
}

#[tokio::test]
async fn directory_round_three_sync_redirect_retires_the_original_scope() {
    let mut steps = discovery(
        &["/directory/"],
        &book("/books/personal/", "<C:addressbook/>"),
    );
    steps.push(dav_redirect(StatusCode::PERMANENT_REDIRECT, "/directory/"));
    let script = dav_script(steps);
    let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
        BASE,
        scripted_dav_net(&script),
    ))
    .await
    .expect("open");
    let scope = CursorScope::Folder(bifrost_types::FolderId(format!("{BASE}/books/personal/")));
    let cursor = cursor_from_snapshot(
        scope.clone(),
        &ContactSnapshot {
            addressbook_url: format!("{BASE}/books/personal/"),
            ctag: None,
            entries: Vec::new(),
            failed_hrefs: Vec::new(),
        },
    );
    let events = account.changes_stream(cursor).collect::<Vec<_>>().await;
    let [SyncEvent::Terminated(error)] = events.as_slice() else {
        panic!("directory redirect must terminate");
    };
    assert_directory_scope_retired(error, &scope, AccountOperation::SyncChanges);
    assert!(error.chain().iter().any(|cause| matches!(cause, Cause::Attempt(attempt) if attempt.transmission_state == TransmissionState::Acknowledged)));
    assert_eq!(transcripts(&script).len(), 4);
}

#[tokio::test]
async fn directory_round_three_discovery_never_enumerates_an_advertised_directory_home() {
    for gateway in ["/books/", "/"] {
        let mut steps = discovery(&[gateway], "");
        steps.pop(); // No home listing is legal once the principal names it a directory.
        steps.push(answer(StatusCode::MULTI_STATUS, multistatus("")));
        let script = dav_script(steps);
        let account = CardDavAccount::open_with_client(CardDavClient::with_account_net(
            BASE,
            scripted_dav_net(&script),
        ))
        .await
        .expect("directory-only account");
        assert_eq!(
            transcripts(&script).len(),
            2,
            "no personal home enumeration"
        );
        assert!(account.default_addressbook_url.is_none() && account.addressbook_urls.is_empty());
        assert!(account.capabilities().pim_methods.directory_search);
        account
            .directory_search("q".into(), None, None)
            .await
            .expect("bounded lookup");
        let sent = transcripts(&script);
        assert_eq!(sent.len(), 3);
        assert_eq!(sent[2].method.as_str(), "REPORT");
        assert_eq!(sent[2].url, format!("{BASE}{gateway}"));
    }
}
