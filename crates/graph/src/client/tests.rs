use bifrost_types::TransmissionState;

use super::*;

/// The `Json` diagnostic label is format-neutral because Autodiscover
/// XML and OneDrive upload failures share the variant, so a Graph REST
/// body that does not decode must say so itself. It used to carry only
/// serde's bare message, naming neither Graph nor JSON.
#[test]
fn a_graph_rest_parse_failure_names_graph_json() {
    let response = RestResponse {
        status: reqwest::StatusCode::OK,
        headers: reqwest::header::HeaderMap::new(),
        body: Bytes::from_static(b"{not json"),
    };
    match parse_json_response::<serde_json::Value>(response) {
        Err(GraphError::Json { message, body }) => {
            assert!(message.starts_with("Graph REST JSON body: "), "{message}");
            assert_eq!(body.as_deref(), Some(&b"{not json"[..]));
        }
        other => panic!("expected Json, got {other:?}"),
    }
}

#[tokio::test]
async fn trims_api_base() {
    let client = GraphClient::with_api_base("https://example.test/v1.0/", "token");
    assert_eq!(client.api_base(), "https://example.test/v1.0");
    assert_eq!(client.access_token().await, "token");
}

#[test]
fn harness_api_base_redirects_autodiscover_and_ews() {
    // A harness pointing the Graph api-base at its mock must get the
    // Autodiscover + EWS surfaces redirected too: in production they
    // live on `outlook.office365.com`, a different host from Graph, so
    // an api-base-only override left them hitting the real service.
    let harness = GraphClient::with_api_base("http://127.0.0.1:8181/v1.0", "token");
    assert_eq!(harness.outlook_base(), "http://127.0.0.1:8181");
    assert_eq!(
        crate::ews::ews_url(harness.outlook_base()),
        "http://127.0.0.1:8181/EWS/Exchange.asmx"
    );
    assert_eq!(
        crate::account::autodiscover_soap_url(harness.outlook_base()),
        "http://127.0.0.1:8181/autodiscover/autodiscover.svc"
    );
    assert_eq!(
        crate::account::autodiscover_xml_url(harness.outlook_base()),
        "http://127.0.0.1:8181/autodiscover/autodiscover.xml"
    );

    // The production Graph base keeps the production Outlook origin -
    // Autodiscover/EWS are NOT on the Graph host.
    let production = GraphClient::new("token");
    assert_eq!(production.outlook_base(), OUTLOOK_BASE);
    assert_eq!(
        crate::ews::ews_url(production.outlook_base()),
        "https://outlook.office365.com/EWS/Exchange.asmx"
    );

    // A shared-mailbox client inherits the override.
    assert_eq!(
        harness
            .for_shared_mailbox("shared@contoso.com")
            .outlook_base(),
        "http://127.0.0.1:8181"
    );

    // An explicit override wins over the derivation.
    assert_eq!(
        production
            .with_outlook_base("https://ews.test/")
            .outlook_base(),
        "https://ews.test"
    );
}

#[tokio::test]
async fn rotated_token_source_is_read() {
    use bifrost_net::AccessToken;

    let source = StaticTokenSource::new("old-token", None);
    let client = GraphClient::with_source(GRAPH_API_BASE, Arc::new(source.clone()));
    assert_eq!(client.access_token().await, "old-token");
    source.set(AccessToken::new("new-token", None));
    assert_eq!(client.access_token().await, "new-token");
}

#[tokio::test]
async fn outlook_credentials_survive_derivation_without_replacing_graph_credentials() {
    let source = StaticTokenSource::new("exchange-token", None);
    let client = GraphClient::new("graph-token").with_outlook_source(Arc::new(source.clone()));
    let derived = client
        .for_shared_mailbox("shared@example.com")
        .with_outlook_base(OUTLOOK_BASE)
        .metered()
        .0;
    let url = reqwest::Url::parse("https://outlook.office365.com/EWS/Exchange.asmx")
        .expect("fixture URL");
    let policy = derived.outlook_auth();
    let bifrost_net::HopCredential::Bearer(selected) =
        policy.admit(None, &url).expect("configured Outlook source")
    else {
        panic!("Outlook must select a bearer source");
    };
    assert_eq!(
        selected.current().await.expect("token").as_str(),
        "exchange-token"
    );
    source.set(bifrost_net::AccessToken::new(
        "rotated-exchange-token",
        None,
    ));
    assert_eq!(
        selected.current().await.expect("rotated token").as_str(),
        "rotated-exchange-token"
    );
    assert_eq!(client.access_token().await, "graph-token");
    let foreign = reqwest::Url::parse("https://graph.microsoft.com/v1.0/me").expect("fixture URL");
    assert!(policy.admit(Some(&url), &foreign).is_err());
}

#[tokio::test]
async fn autodiscover_wire_uses_outlook_source() {
    let client = GraphClient::new("graph-token")
        .with_outlook_source(Arc::new(StaticTokenSource::new("exchange-token", None)));
    client.script_aux([ScriptedRestResponse::text(
        reqwest::StatusCode::OK,
        "<root/>",
    )]);
    let url = client
        .admit_outlook_target("https://outlook.office365.com/autodiscover/autodiscover.xml")
        .expect("configured origin");
    client
        .execute_aux(
            reqwest::Method::POST,
            AuxTarget::Bearer(&url),
            &[],
            Bytes::new(),
            None,
            bifrost_net::ReplayPolicy::Idempotent,
        )
        .await
        .expect("scripted response");
    let requests = client.rest_requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0]
            .headers
            .get(reqwest::header::AUTHORIZATION)
            .expect("bearer"),
        "Bearer exchange-token"
    );
}

#[test]
fn separate_outlook_origin_requires_its_own_source() {
    let client = GraphClient::new("graph-token");
    let url = reqwest::Url::parse("https://outlook.office365.com/EWS/Exchange.asmx")
        .expect("fixture URL");
    assert!(matches!(
        client.outlook_auth().admit(None, &url),
        Err(bifrost_net::Error::HopPolicy(error))
            if matches!(error.kind(), bifrost_types::AccountErrorKind::SyncState(
                bifrost_types::SyncStateErrorKind::OperatorOverrideNeeded
            ))
    ));
}

#[test]
fn invalid_outlook_origin_requires_operator_configuration() {
    let policy = OutlookAuth {
        base: Base::parse("not a URL"),
        source: None,
    };
    let url = reqwest::Url::parse("https://outlook.office365.com/EWS/Exchange.asmx")
        .expect("fixture URL");
    assert!(matches!(
        bifrost_net::HopPolicy::admit(&policy, None, &url),
        Err(bifrost_net::Error::HopPolicy(error))
            if matches!(error.kind(), bifrost_types::AccountErrorKind::SyncState(
                bifrost_types::SyncStateErrorKind::OperatorOverrideNeeded
            ))
    ));
}

#[tokio::test]
async fn same_origin_exchange_retains_single_service_source() {
    let client = GraphClient::with_api_base("https://mail.example.com/v1.0", "single-token");
    let url =
        reqwest::Url::parse("https://mail.example.com/EWS/Exchange.asmx").expect("fixture URL");
    let bifrost_net::HopCredential::Bearer(source) = client
        .outlook_auth()
        .admit(None, &url)
        .expect("same service origin")
    else {
        panic!("same service must select a bearer source");
    };
    assert_eq!(
        source.current().await.expect("token").as_str(),
        "single-token"
    );
}

#[test]
fn api_path_prefix_returns_me_for_primary_mailbox() {
    let client = GraphClient::new("token");
    assert_eq!(client.api_path_prefix(), "/me");
    assert!(!client.is_shared_mailbox());
}

#[test]
fn for_shared_mailbox_creates_scoped_client() {
    let client = GraphClient::new("token");
    let scoped = client.for_shared_mailbox("shared@example.com");
    assert_eq!(scoped.api_path_prefix(), "/users/shared%40example.com");
    assert_eq!(scoped.mailbox_id(), Some("shared@example.com"));
    assert!(scoped.is_shared_mailbox());
}

#[test]
fn non_versioned_api_base_keeps_its_path() {
    let client = GraphClient::with_api_base("http://127.0.0.1:8181/graph", "token");
    assert_eq!(client.api_base(), "http://127.0.0.1:8181/graph");
    // The Outlook origin, by contrast, correctly follows the redirect.
    assert_eq!(client.outlook_base(), "http://127.0.0.1:8181");
}

/// A client with no attached transport is the client's own invariant
/// failing, on every send path: it used to be minted as a
/// `Network(Unsent)` error, which the engine retried as a network fault
/// forever, though a retry of the same request never attaches anything.
#[tokio::test]
async fn an_unattached_client_is_an_internal_failure_on_every_send_path() {
    let client = GraphClient::new("token");
    let url = client.api_url("/me").expect("api url");
    let is_internal = |error: GraphError| matches!(error, GraphError::Internal { .. });

    assert!(is_internal(
        client
            .get::<serde_json::Value>(Resource::Outlook, &url)
            .await
            .expect_err("rest")
    ));
    assert!(is_internal(
        client
            .execute_aux(
                reqwest::Method::POST,
                AuxTarget::Bearer(&url),
                &[],
                Bytes::new(),
                None,
                bifrost_net::ReplayPolicy::Idempotent
            )
            .await
            .err()
            .expect("aux")
    ));
    assert!(is_internal(
        client
            .download_stream(Resource::Outlook, &url, None)
            .await
            .err()
            .expect("download")
    ));
}

#[test]
fn api_url_joins_paths_and_admits_only_the_api_origin() {
    // The `@odata.nextLink` / `@odata.deltaLink` walk feeds absolute
    // URLs back in; rewriting them onto the api-base would break
    // pagination, so a same-origin absolute URL passes through. One on
    // any other origin is refused: the request would carry the bearer.
    let client = GraphClient::with_api_base("https://x/v1.0", "token");
    assert_eq!(
        client.api_url("/me/messages").expect("path").as_str(),
        "https://x/v1.0/me/messages"
    );
    assert_eq!(
        client.api_url("me/messages").expect("bare path").as_str(),
        "https://x/v1.0/me/messages"
    );
    assert_eq!(
        client
            .api_url("https://x/v1.0/next?$skiptoken=a")
            .expect("same origin")
            .as_str(),
        "https://x/v1.0/next?$skiptoken=a"
    );
    assert!(matches!(
        client.api_url("https://graph.example/next?$skiptoken=a"),
        Err(GraphError::Configuration { .. })
    ));
    assert!(matches!(
        client.admit_link(&ProviderLink::for_tests("http://x/v1.0/next")),
        Err(GraphError::ProviderLinkRefused { .. })
    ));
}

#[test]
fn rate_limit_host_tracks_the_api_base_host() {
    // The per-host token bucket must follow a redirected base, or a
    // harness run would meter against `graph.microsoft.com`.
    let host = |base: &str| host_from_api_base(&Base::parse(base));
    assert_eq!(host("https://graph.microsoft.com/v1.0"), GRAPH_HOST);
    assert_eq!(host("http://127.0.0.1:8181/v1.0"), "127.0.0.1");
    // An unparseable base falls back to production rather than panicking.
    assert_eq!(host("not a url"), GRAPH_HOST);
}

/// nc-9's shape: `with_account_net` was the one constructor that
/// hardcoded `rate_limit_host` to the production Graph host, so an
/// injected net against a redirected base metered under the wrong
/// bucket. It must derive from the supplied base like the others.
#[test]
fn with_account_net_derives_the_rate_limit_host_from_its_base() {
    let donor = GraphClient::new("token");
    donor.attach_account(AccountId("with-account-net-host-probe".to_string()));
    let net = donor.account_net().expect("account net attached");
    let client = GraphClient::with_account_net(
        net,
        "http://127.0.0.1:8181/v1.0",
        Arc::new(StaticTokenSource::new("token", None)),
    );
    assert_eq!(client.inner.rate_limit_host, "127.0.0.1");
}

#[test]
fn outlook_base_keeps_the_scheme_and_port_of_a_redirected_api_base() {
    let derive = |base: &str| derive_outlook_base(&Base::parse(base));
    assert_eq!(
        derive("https://graph.contoso-cloud.test/v1.0").as_str(),
        "https://graph.contoso-cloud.test"
    );
    assert_eq!(
        derive("http://127.0.0.1:8181/v1.0").as_str(),
        "http://127.0.0.1:8181"
    );
    // An unusable Graph base derives an unusable Outlook origin rather
    // than falling back to production: the fallback would have left
    // Autodiscover and EWS sending the bearer to the real service.
    assert!(derive("nonsense").url().is_none());
    assert!(
        GraphClient::with_api_base("nonsense", "token")
            .ews_url()
            .is_err()
    );
    // An explicit Outlook origin still repairs it.
    assert!(
        GraphClient::with_api_base("nonsense", "token")
            .with_outlook_base("https://ews.test")
            .ews_url()
            .is_ok()
    );
}

#[test]
fn shared_mailbox_prefix_percent_encodes_the_routing_key() {
    // The routing key is an SMTP address; `@` and any `+` tag must not
    // leak into the path unencoded.
    let client = GraphClient::new("token").for_shared_mailbox("a+tag@contoso.com");
    let prefix = client.api_path_prefix();
    assert!(!prefix.contains('@'), "{prefix}");
    assert!(!prefix.contains('+'), "{prefix}");
    assert!(prefix.starts_with("/users/"), "{prefix}");
    // The stored key uses the same ASCII canonical form as routing maps.
    assert_eq!(client.mailbox_id(), Some("a+tag@contoso.com"));
    assert!(!client.uses_default_mailbox());
}

#[test]
fn routed_error_context_leaves_throttle_decisions_to_classification() {
    let client = GraphClient::new("token").for_shared_mailbox("Shared@Contoso.com");
    let budget = client.mailbox_budget();
    for (status, body, throttled) in [
        (503, "", false),
        (429, "", true),
        (507, "", true),
        (400, r#"{"error":{"code":"TooManyRequests"}}"#, true),
        (503, r#"{"error":{"code":"TooManyRequests"}}"#, true),
    ] {
        let source = GraphError::Response(crate::error::GraphResponseError::from_response(
            reqwest::StatusCode::from_u16(status).unwrap(),
            reqwest::header::HeaderMap::new(),
            Bytes::from(body),
        ));
        let error = crate::account::graph_error::into_account_error(
            client.with_mailbox_context(&Resource::Outlook, source),
            crate::account::graph_error::GraphErrorContext::graph(
                bifrost_types::AccountOperation::SyncChanges,
            ),
        );
        assert_eq!(
            !error.throttle_budgets().is_empty(),
            throttled,
            "{status}: {body}"
        );
        if throttled {
            assert_eq!(error.throttle_budgets(), std::slice::from_ref(&budget));
        } else {
            assert!(matches!(
                error.kind(),
                bifrost_types::AccountErrorKind::Server(
                    bifrost_types::ServerErrorKind::Unavailable
                )
            ));
        }
    }
}

/// A throttle is tagged with the budget of the mailbox the request was
/// routed to, and only when the request was routed to one.
///
/// The shared client's 429 under `/users/{key}` carries that mailbox's
/// budget, distinct from the primary's. A 429 on `/subscriptions` (an
/// app-and-tenant budget, not a mailbox one) and on `$batch` (which can
/// mix mailboxes) is left as the transport shaped it.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_throttle_is_tagged_with_the_mailbox_it_was_routed_to() {
    use reqwest::StatusCode;
    let primary = GraphClient::new("token");
    primary.set_primary_address(Some("Me@Contoso.com".into()));
    let shared = primary.for_shared_mailbox("Shared@Contoso.com");
    let throttle =
        || {
            [ScriptedRestResponse::empty(StatusCode::TOO_MANY_REQUESTS)
                .with_header("Retry-After", "5")]
        };

    shared.script_rest(throttle());
    let url = shared
        .api_url(&format!("{}/messages", shared.api_path_prefix()))
        .expect("api url");
    match shared
        .execute_wire(
            Resource::Outlook,
            &url,
            reqwest::Method::GET,
            None,
            RequestOptions::default(),
            None,
            bifrost_net::ReplayPolicy::Idempotent,
        )
        .await
    {
        Err(GraphError::MailboxContext { budget, .. }) => {
            assert_eq!(budget, shared.mailbox_budget());
            assert!(
                budget.as_str().ends_with("mailbox:shared@contoso.com"),
                "{budget:?}"
            );
            assert_ne!(budget, primary.mailbox_budget());
        }
        other => panic!("a shared-mailbox 429 carries its budget: {:?}", other.err()),
    }
    assert!(
        primary
            .mailbox_budget()
            .as_str()
            .ends_with("mailbox:me@contoso.com")
    );

    // Another spelling of the same origin names the same budget, and
    // still attributes its own requests.
    let shouted = GraphClient::with_api_base("HTTPS://GRAPH.Microsoft.com:443/v1.0", "token")
        .for_shared_mailbox("shared@contoso.com");
    assert_eq!(shouted.mailbox_budget(), shared.mailbox_budget());
    // And another API version against the same mailbox is the same
    // provider budget.
    let beta = GraphClient::with_api_base("https://graph.microsoft.com/beta", "token")
        .for_shared_mailbox("shared@contoso.com");
    assert_eq!(beta.mailbox_budget(), shared.mailbox_budget());
    shouted.script_rest(throttle());
    let url = shouted
        .api_url(&format!("{}/messages", shouted.api_path_prefix()))
        .expect("api url");
    assert!(matches!(
        shouted
            .execute_wire(
                Resource::Outlook,
                &url,
                reqwest::Method::GET,
                None,
                RequestOptions::default(),
                None,
                bifrost_net::ReplayPolicy::Idempotent
            )
            .await,
        Err(GraphError::MailboxContext { .. })
    ));

    for path in ["/subscriptions", "/$batch"] {
        primary.script_rest(throttle());
        let url = primary.api_url(path).expect("api url");
        assert!(
            matches!(
                primary
                    .execute_wire(
                        Resource::Unmetered,
                        &url,
                        reqwest::Method::GET,
                        None,
                        RequestOptions::default(),
                        None,
                        bifrost_net::ReplayPolicy::Idempotent
                    )
                    .await,
                Err(GraphError::Net(_))
            ),
            "{path} is not charged to one mailbox and must stay untagged"
        );
    }
}

// A reserved concurrency claim is not yet a recorded wire request:
// admission, rate and token futures precede polling ScriptedDispatch.
// Drive those stages with a bounded poll/yield loop before inspecting
// counts. The operation must stay pending on a claim or canned response.
async fn drive_pending<F: std::future::Future>(mut request: std::pin::Pin<&mut F>) {
    for _ in 0..64 {
        assert!(futures::FutureExt::now_or_never(request.as_mut()).is_none());
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn outlook_admission_is_per_mailbox_and_unmetered_work_bypasses_it() {
    let primary = GraphClient::new("token");
    primary.script_aux_pending(7);
    let shared = primary.for_shared_mailbox("shared@contoso.com");
    let net = primary.attached_net().expect("scripted net");
    let limit = net.shared_concurrency_limit(primary.concurrency_key(&net), 4);
    let other = net.shared_concurrency_limit(shared.concurrency_key(&net), 4);
    let url = primary.api_url("/me/messages").expect("url");
    let shared_path = format!("{}/messages", shared.api_path_prefix());
    let shared_url = shared.api_url(&shared_path).expect("shared url");
    let mut active = Vec::new();
    for _ in 0..4 {
        let mut request = Box::pin(primary.get::<serde_json::Value>(Resource::Outlook, &url));
        drive_pending(request.as_mut()).await;
        active.push(request);
    }
    assert_eq!(limit.in_flight(), 4);
    assert_eq!(primary.wire_attempts(), 4);
    let mut fifth = Box::pin(primary.get::<serde_json::Value>(Resource::Outlook, &url));
    drive_pending(fifth.as_mut()).await;
    assert_eq!(primary.wire_attempts(), 4);
    let mut foreign = Box::pin(shared.get::<serde_json::Value>(Resource::Outlook, &shared_url));
    drive_pending(foreign.as_mut()).await;
    assert_eq!(other.in_flight(), 1);
    let requests = primary.take_rest_requests();
    // The request log includes the fifth primary call while its claim waits.
    assert_eq!(requests.len(), 6);
    assert!(
        requests[..5]
            .iter()
            .all(|request| request.url.ends_with("/me/messages"))
    );
    assert!(
        requests[5]
            .url
            .ends_with("/users/shared%40contoso.com/messages")
    );
    let mut unmetered = Box::pin(primary.get::<serde_json::Value>(Resource::Unmetered, &url));
    drive_pending(unmetered.as_mut()).await;
    assert_eq!(primary.wire_attempts(), 6);
    assert_eq!(limit.in_flight(), 4);
    drop(active.pop());
    drive_pending(fifth.as_mut()).await;
    assert_eq!(primary.wire_attempts(), 7);
    assert_eq!(limit.in_flight(), 4);
    drop((active, fifth, foreign, unmetered));
    assert_eq!(limit.in_flight(), 0);
    assert_eq!(other.in_flight(), 0);
}

#[tokio::test]
async fn a_primary_address_alias_shares_the_fixed_primary_limit() {
    let primary = GraphClient::new("token");
    primary.script_aux_pending(5);
    let alias = primary.for_shared_mailbox("ME@contoso.com");
    let net = primary.attached_net().expect("scripted net");
    let key = primary.concurrency_key(&net);
    primary.set_primary_address(Some("me@Contoso.com".into()));
    assert_eq!(primary.concurrency_key(&net), key);
    assert_eq!(alias.concurrency_key(&net), key);
    let limit = net.shared_concurrency_limit(key, 4);
    let url = primary.api_url("/me/messages").expect("url");
    let mut active = Vec::new();
    for _ in 0..4 {
        let mut request = Box::pin(primary.get::<serde_json::Value>(Resource::Outlook, &url));
        drive_pending(request.as_mut()).await;
        active.push(request);
    }
    let mut fifth = std::pin::pin!(alias.get::<serde_json::Value>(Resource::Outlook, &url));
    drive_pending(fifth.as_mut()).await;
    assert_eq!(primary.wire_attempts(), 4);
    assert_eq!(limit.in_flight(), 4);
    drop(active);
    drive_pending(fifth.as_mut()).await;
    assert_eq!(primary.wire_attempts(), 5);
    assert_eq!(limit.in_flight(), 1);
}

#[tokio::test]
async fn directory_queries_bypass_a_saturated_mailbox_without_throttle_tags() {
    use bifrost_types::Account;
    let client = GraphClient::new("token");
    client.script_aux_pending(4);
    let net = client.attached_net().expect("net");
    let limit = net.shared_concurrency_limit(client.concurrency_key(&net), 4);
    let url = client.api_url("/me/messages").expect("url");
    let mut active = Vec::new();
    for _ in 0..4 {
        let mut request = Box::pin(client.get::<serde_json::Value>(Resource::Outlook, &url));
        drive_pending(request.as_mut()).await;
        active.push(request);
    }
    client.script_rest([ScriptedRestResponse::empty(
        reqwest::StatusCode::TOO_MANY_REQUESTS,
    )]);
    let account = crate::account::GraphAccount::new_for_tests(
        client.clone(),
        crate::account::PushMode::GraphSubscriptions,
    );
    let mut query = Account::directory_search(&account, "ada".into(), None, None);
    let mut result = None;
    for _ in 0..64 {
        if let Some(value) = futures::FutureExt::now_or_never(query.as_mut()) {
            result = Some(value);
            break;
        }
        tokio::task::yield_now().await;
    }
    let error = result
        .expect("directory query must bypass mailbox admission")
        .expect_err("429");
    assert!(error.throttle_budgets().is_empty());
    assert_eq!(limit.in_flight(), 4);
    assert!(
        client
            .take_rest_requests()
            .last()
            .expect("query")
            .url
            .contains("/users?")
    );
}

#[tokio::test]
async fn opening_a_derived_factory_preserves_primary_identity() {
    use bifrost_types::AccountFactory;
    let primary = GraphClient::new("token");
    primary.set_primary_address(Some("a@contoso.com".into()));
    primary.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            serde_json::json!({"mail": "b@contoso.com"}),
        ),
        ScriptedRestResponse::json(reqwest::StatusCode::OK, serde_json::json!({"value": []})),
    ]);
    let shared = primary
        .for_shared_mailbox("b@contoso.com")
        .with_outlook_base("https://outlook.office365.com");
    let alias = primary.for_shared_mailbox("A@CONTOSO.COM");
    let budget = primary.mailbox_budget();
    crate::account::GraphAccountFactory::new(shared.clone())
        .open(AccountId("primary".into()))
        .await
        .expect("open shared factory");
    let net = primary.attached_net().expect("net");
    assert_eq!(primary.mailbox_budget(), budget);
    assert_eq!(alias.concurrency_key(&net), primary.concurrency_key(&net));
    assert_ne!(shared.concurrency_key(&net), primary.concurrency_key(&net));
}

#[tokio::test]
async fn mixed_owner_hydration_claims_both_mailbox_limits() {
    use bifrost_types::{Account, ObjectId, Projection};
    use futures::StreamExt;
    let primary = GraphClient::new("token");
    primary.script_aux_pending(1);
    let shared = primary.for_shared_mailbox("shared@contoso.com");
    let net = primary.attached_net().expect("net");
    let limit = net.shared_concurrency_limit(primary.concurrency_key(&net), 4);
    let other = net.shared_concurrency_limit(shared.concurrency_key(&net), 4);
    let account = crate::account::GraphAccount::new_for_tests_with_shared(
        primary.clone(),
        crate::account::PushMode::GraphSubscriptions,
        &["shared@contoso.com".into()],
    );
    let ids = vec![
        ObjectId("p1".into()),
        ObjectId("p2".into()),
        ObjectId("shared@contoso.com\u{1f}s1".into()),
    ];
    let mut stream = Account::get_stream(
        &account,
        Box::pin(futures::stream::iter(ids)),
        Projection::Metadata,
    );
    let mut next = Box::pin(stream.next());
    drive_pending(next.as_mut()).await;
    assert_eq!((limit.in_flight(), other.in_flight()), (2, 1));
    let requests = primary.take_rest_requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "POST");
    assert!(requests[0].url.ends_with("/$batch"));
    let body = requests[0].body.as_ref().expect("batch body");
    let urls: Vec<_> = body["requests"]
        .as_array()
        .expect("items")
        .iter()
        .map(|item| {
            item["url"]
                .as_str()
                .expect("url")
                .split('?')
                .next()
                .expect("path")
        })
        .collect();
    assert_eq!(
        urls,
        vec![
            "/me/messages/p1",
            "/me/messages/p2",
            "/users/shared%40contoso.com/messages/s1"
        ]
    );
    drop(next);
    drop(stream);
    assert_eq!((limit.in_flight(), other.in_flight()), (0, 0));
}

#[tokio::test]
async fn shared_mailbox_hydration_uses_its_limit_while_pending() {
    use bifrost_types::{Account, HydrationProjection, ObjectId};

    let primary = GraphClient::new("token");
    primary.set_primary_address(Some("me@contoso.com".into()));
    primary.script_aux_pending(1);
    let shared = primary.for_shared_mailbox("shared@contoso.com");
    let net = primary.attached_net().expect("scripted net");
    let primary_limit = net.shared_concurrency_limit(primary.concurrency_key(&net), 4);
    let shared_limit = net.shared_concurrency_limit(shared.concurrency_key(&net), 4);
    let account = crate::account::GraphAccount::new_for_tests_with_shared(
        primary.clone(),
        crate::account::PushMode::GraphSubscriptions,
        &["shared@contoso.com".into()],
    );
    let id = ObjectId("shared@contoso.com\u{1f}AAMkshared".into());

    let mut request = Box::pin(Account::message_hydrate(
        &account,
        id,
        HydrationProjection::Headers,
    ));
    drive_pending(request.as_mut()).await;

    assert_eq!(
        (shared_limit.in_flight(), primary_limit.in_flight()),
        (1, 0)
    );
    let requests = primary.take_rest_requests();
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0]
            .url
            .contains("/users/shared%40contoso.com/messages/AAMkshared")
    );

    drop(request);
    assert_eq!(
        (shared_limit.in_flight(), primary_limit.in_flight()),
        (0, 0)
    );
}

#[tokio::test]
async fn batches_claim_capped_weights_after_merging_mailbox_aliases() {
    let primary = GraphClient::new("token");
    primary.script_aux_pending(3);
    primary.set_primary_address(Some("me@contoso.com".into()));
    let shared = primary.for_shared_mailbox("shared@contoso.com");
    let alias = primary.for_shared_mailbox("ME@CONTOSO.COM");
    let net = primary.attached_net().expect("scripted net");
    let limit = net.shared_concurrency_limit(primary.concurrency_key(&net), 4);
    let other = net.shared_concurrency_limit(shared.concurrency_key(&net), 4);
    for (clients, expected, expected_urls) in [
        (
            vec![
                primary.clone(),
                primary.clone(),
                primary.clone(),
                shared.clone(),
                shared.clone(),
            ],
            (3, 2),
            vec![
                "/me/messages",
                "/me/messages",
                "/me/messages",
                "/users/shared%40contoso.com/messages",
                "/users/shared%40contoso.com/messages",
            ],
        ),
        (vec![primary.clone(); 6], (4, 0), vec!["/me/messages"; 6]),
        (
            vec![
                primary.clone(),
                primary.clone(),
                primary.clone(),
                alias.clone(),
                alias.clone(),
                alias,
            ],
            (4, 0),
            vec![
                "/me/messages",
                "/me/messages",
                "/me/messages",
                "/users/me%40contoso.com/messages",
                "/users/me%40contoso.com/messages",
                "/users/me%40contoso.com/messages",
            ],
        ),
    ] {
        let batch = crate::types::BatchRequest {
            requests: clients
                .iter()
                .enumerate()
                .map(|(index, client)| crate::types::BatchRequestItem {
                    id: index.to_string(),
                    method: "GET".into(),
                    url: format!("{}/messages", client.api_path_prefix()),
                    body: None,
                    headers: None,
                })
                .collect(),
        };
        let mut request = Box::pin(primary.post_batch(
            Resource::Batch(clients),
            &batch,
            bifrost_net::ReplayPolicy::Never,
        ));
        drive_pending(request.as_mut()).await;
        assert_eq!((limit.in_flight(), other.in_flight()), expected);
        let recorded = primary.take_rest_requests();
        assert_eq!(recorded.len(), 1);
        let body = recorded[0].body.as_ref().expect("batch body");
        let urls: Vec<_> = body["requests"]
            .as_array()
            .expect("batch items")
            .iter()
            .map(|item| item["url"].as_str().expect("subrequest url"))
            .collect();
        assert_eq!(urls, expected_urls);
        drop(request);
        assert_eq!((limit.in_flight(), other.in_flight()), (0, 0));
    }
    assert_eq!(primary.wire_attempts(), 3);
}

#[tokio::test]
async fn continuation_spelling_keeps_the_walk_mailbox_on_a_throttle() {
    let primary = GraphClient::new("token");
    let shared = primary.for_shared_mailbox("shared@contoso.com");
    shared.script_rest([
        ScriptedRestResponse::json(reqwest::StatusCode::OK, serde_json::json!({
            "value": [],
            "@odata.nextLink": "https://graph.microsoft.com/v1.0/users('shared@contoso.com')/mailFolders?$skiptoken=next",
        })),
        ScriptedRestResponse::empty(reqwest::StatusCode::TOO_MANY_REQUESTS),
    ]);
    match shared.list_mail_folders().await {
        Err(GraphError::MailboxContext { budget, .. }) => {
            assert_eq!(budget, shared.mailbox_budget());
            assert_ne!(budget, primary.mailbox_budget());
        }
        other => panic!("the walk retains its owning mailbox: {other:?}"),
    }
    assert_eq!(shared.wire_attempts(), 2);
}

#[test]
fn attach_account_uses_engine_account_id() {
    let client = GraphClient::new("token");
    client.attach_account(AccountId("engine-account".to_string()));
    let account_net = client.account_net().expect("account net attached");
    assert_eq!(
        account_net.account(),
        &AccountId("engine-account".to_string())
    );
}

fn opened_shape_account(client: &GraphClient) -> crate::account::GraphAccount {
    crate::account::GraphAccount::new_for_tests(
        client.clone(),
        crate::account::PushMode::GraphSubscriptions,
    )
}

#[test]
fn opened_account_forwards_bandwidth_cap() {
    use bifrost_types::Account;

    let client = GraphClient::new("token");
    client.attach_account(AccountId("graph-cap-forward".to_string()));
    let account = opened_shape_account(&client);
    account.set_bandwidth_cap(bifrost_types::BandwidthCap::new(1234));
    assert_eq!(client.account_net().unwrap().bandwidth_cap(), Some(1234));
    account.set_bandwidth_cap(None);
    assert_eq!(client.account_net().unwrap().bandwidth_cap(), None);
}

#[test]
fn same_id_reattach_preserves_forwarded_bandwidth_cap() {
    use bifrost_types::Account;

    let client = GraphClient::new("token");
    let id = AccountId("graph-cap-reattach".to_string());
    client.attach_account(id.clone());
    let account = opened_shape_account(&client);
    account.set_bandwidth_cap(bifrost_types::BandwidthCap::new(2345));
    assert_eq!(client.account_net().unwrap().bandwidth_cap(), Some(2345));
    // Keep no AccountNet clone alive across replacement: only the slot
    // may hold the registry's weakly retained budget alive.
    client.attach_account(id);
    assert_eq!(client.account_net().unwrap().bandwidth_cap(), Some(2345));
    account.set_bandwidth_cap(bifrost_types::BandwidthCap::new(3456));
    assert_eq!(client.account_net().unwrap().bandwidth_cap(), Some(3456));
}

#[test]
fn injected_client_retag_uses_destination_bandwidth_budget() {
    use bifrost_types::Account;

    let wire = bifrost_net::test_support::ScriptedDispatch::new([]);
    let net = bifrost_net::test_support::scripted_net(&wire, bifrost_net::NetConfig::default());
    let source = net.attach_account(
        AccountId("graph-cap-source".to_string()),
        AccountSpec::new(None),
    );
    source.set_bandwidth_cap(bifrost_types::BandwidthCap::new(1111));
    let destination_id = AccountId("graph-cap-destination".to_string());
    let destination = net.attach_account(destination_id.clone(), AccountSpec::new(None));
    destination.set_bandwidth_cap(bifrost_types::BandwidthCap::new(2222));
    let client = GraphClient::with_account_net(
        source.clone(),
        "https://graph.microsoft.com/v1.0",
        Arc::new(StaticTokenSource::new("token", None)),
    );
    let budget = client.mailbox_budget();
    let key = client.concurrency_key(&source);
    client.attach_account(destination_id.clone());
    // Retagging changes accounting, not which mailbox this client names.
    assert_eq!(client.mailbox_budget(), budget);
    assert_eq!(client.concurrency_key(&source), key);
    let attached = client.account_net().unwrap();
    assert_eq!(attached.account(), &destination_id);
    assert_eq!(attached.bandwidth_cap(), Some(2222));
    let account = opened_shape_account(&client);
    account.set_bandwidth_cap(bifrost_types::BandwidthCap::new(3333));
    assert_eq!(attached.bandwidth_cap(), Some(3333));
    assert_eq!(destination.bandwidth_cap(), Some(3333));
    assert_eq!(source.bandwidth_cap(), Some(1111));
}

/// Every reopen calls `attach_account` again with the same engine
/// id. `Net` mints a fresh registration token per attach and no
/// longer unregisters a previous one, so the displaced handle has
/// to be detached explicitly or the host bucket leaks an attach
/// count per cycle and is never reclaimed.
/// `GraphClient::new` rides the process-wide `Net`, so this keys
/// on a meter entry for an id no other test touches rather than on
/// the shared `GRAPH_HOST` bucket.
#[test]
fn reattaching_the_same_engine_id_does_not_leak_registrations() {
    let client = GraphClient::new("token");
    let id = AccountId("graph-reattach-leak-probe".to_string());
    let net = client.inner.net.clone().expect("default client owns a Net");

    for _ in 0..3 {
        client.attach_account(id.clone());
    }
    bifrost_net::MeterSink::record_bytes_in(net.meter().as_ref(), &id, 5);
    assert_eq!(net.meter().account(id.clone()).bytes_in(), 5);

    client.account_net().expect("account net attached").detach();

    assert_eq!(
        net.meter().account(id).bytes_in(),
        0,
        "three attaches must leave exactly one live registration to detach"
    );
}

#[tokio::test]
async fn scripted_rest_records_the_graph_owned_request_and_returns_its_response() {
    let client = GraphClient::new("token");
    client.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        serde_json::json!({"displayName":"Ada"}),
    )]);

    let profile = client.get_profile().await.expect("scripted profile");
    assert_eq!(profile.display_name.as_deref(), Some("Ada"));
    let requests = client.take_rest_requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "GET");
    assert!(
        requests[0]
            .url
            .ends_with("/me?$select=displayName,mail,userPrincipalName")
    );
    assert_eq!(requests[0].if_match, None);
    assert_eq!(requests[0].prefer, None);
    assert_eq!(requests[0].body, None);

    let response = ScriptedRestResponse::empty(reqwest::StatusCode::TOO_MANY_REQUESTS)
        .with_header("Retry-After", "5");
    assert_eq!(response.headers["retry-after"], "5");
}

#[tokio::test]
async fn bodyless_rest_requests_omit_content_type() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::json(reqwest::StatusCode::OK, serde_json::json!({"id":"m1"})),
        ScriptedRestResponse::empty(reqwest::StatusCode::NO_CONTENT),
        ScriptedRestResponse::empty(reqwest::StatusCode::NO_CONTENT),
        ScriptedRestResponse::json(reqwest::StatusCode::CREATED, serde_json::json!({"id":"m2"})),
    ]);

    let _: serde_json::Value = client
        .get_json(Resource::Outlook, "/me/messages/m1")
        .await
        .expect("GET");
    client
        .delete(
            Resource::Outlook,
            "/me/messages/m1",
            bifrost_net::ReplayPolicy::Never,
        )
        .await
        .expect("DELETE");
    client
        .post_empty(
            Resource::Outlook,
            "/me/messages/m1/accept",
            bifrost_net::ReplayPolicy::Never,
        )
        .await
        .expect("bodyless POST");
    let _: serde_json::Value = client
        .post(
            Resource::Outlook,
            "/me/messages",
            &serde_json::json!({"subject":"created"}),
            bifrost_net::ReplayPolicy::Never,
        )
        .await
        .expect("JSON POST");

    let requests = client.rest_requests();
    assert_eq!(requests.len(), 4);
    assert!(requests[0].headers.get("content-type").is_none());
    assert!(requests[1].headers.get("content-type").is_none());
    assert!(requests[2].headers.get("content-type").is_none());
    assert_eq!(
        requests[3]
            .headers
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("application/json")
    );
}

/// Issue one GET against a scripted status and return what the
/// caller actually sees. Every response here travels the production
/// retry loop, so these outcomes are the transport's, not a local
/// restatement of them.
async fn outcome_for(
    responses: impl IntoIterator<Item = ScriptedRestResponse>,
) -> Result<RestResponse, GraphError> {
    let client = GraphClient::new("token");
    client.script_rest(responses);
    let url = client.api_url("/me/messages").expect("api url");
    client
        .execute_wire(
            Resource::Outlook,
            &url,
            reqwest::Method::GET,
            None,
            RequestOptions::default(),
            None,
            bifrost_net::ReplayPolicy::Idempotent,
        )
        .await
}

/// `bifrost-net` returns `Ok(Response)` for 2xx and a passed-through
/// 3xx ONLY; every other status leaves its retry loop as a typed
/// `Error`. This used to assert against a local reimplementation of
/// that rule, which could agree with itself while disagreeing with
/// the transport. It now drives the transport.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_scripted_status_takes_the_shape_bifrost_net_produces() {
    use reqwest::StatusCode;

    assert!(
        outcome_for([ScriptedRestResponse::empty(StatusCode::OK)])
            .await
            .is_ok()
    );
    // A 304 on a conditional read is passed through as a response.
    assert!(
        outcome_for([ScriptedRestResponse::empty(StatusCode::NOT_MODIFIED)])
            .await
            .is_ok()
    );

    match outcome_for([ScriptedRestResponse::empty(StatusCode::NOT_FOUND)])
        .await
        .as_ref()
        .map_err(GraphError::terminal)
    {
        Err(GraphError::Net(bifrost_net::Error::Status { code, .. })) => {
            assert_eq!(*code, StatusCode::NOT_FOUND);
        }
        _ => panic!("a terminal 4xx is Error::Status"),
    }

    // The funnel tags an Outlook throttle with the client's mailbox
    // budget; the transport's shape
    // survives underneath it unchanged.
    match outcome_for([
        ScriptedRestResponse::empty(StatusCode::TOO_MANY_REQUESTS).with_header("Retry-After", "5")
    ])
    .await
    {
        Err(GraphError::MailboxContext { source, .. }) => match *source {
            GraphError::Net(bifrost_net::Error::RateLimited {
                retry_after,
                final_response,
            }) => {
                assert_eq!(
                    retry_after,
                    Some(bifrost_types::RetryHint::After(
                        std::time::Duration::from_secs(5)
                    ))
                );
                assert_eq!(final_response.status, StatusCode::TOO_MANY_REQUESTS);
            }
            _ => panic!("429 past the budget is Error::RateLimited"),
        },
        _ => panic!("an Outlook 429 carries the mailbox budget"),
    }

    match outcome_for([ScriptedRestResponse::empty(
        StatusCode::INTERNAL_SERVER_ERROR,
    )])
    .await
    .as_ref()
    .map_err(GraphError::terminal)
    {
        Err(GraphError::Net(bifrost_net::Error::RetryBudgetExhausted {
            final_response: Some(final_response),
            ..
        })) => assert_eq!(final_response.status, StatusCode::INTERNAL_SERVER_ERROR),
        _ => panic!("5xx past the budget is Error::RetryBudgetExhausted"),
    }
}

/// A fact the local simulation could not have told anyone, because it
/// answered every request from a single scripted entry: a 401 does not
/// become `AuthLost` on its own. bifrost-net forces a token refresh and
/// reissues the request on a budget SEPARATE from `max_attempts`, so
/// the caller sees `AuthLost` only when the second attempt is also
/// rejected - and a Graph path that meets a transient 401 recovers
/// without ever surfacing an error.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_401_is_retried_after_a_forced_refresh_before_it_becomes_auth_lost() {
    use reqwest::StatusCode;

    let recovered = outcome_for([
        ScriptedRestResponse::empty(StatusCode::UNAUTHORIZED),
        ScriptedRestResponse::json(StatusCode::OK, serde_json::json!({"ok": true})),
    ])
    .await
    .expect("the refreshed retry succeeds");
    assert_eq!(recovered.status, StatusCode::OK);

    match outcome_for([
        ScriptedRestResponse::empty(StatusCode::UNAUTHORIZED),
        ScriptedRestResponse::empty(StatusCode::UNAUTHORIZED),
    ])
    .await
    .as_ref()
    .map_err(GraphError::terminal)
    {
        Err(GraphError::Net(error)) => {
            let bifrost_net::Error::AuthLost {
                transmission_state,
                final_response: Some(_),
                ..
            } = error.terminal()
            else {
                panic!("a 401 surviving forced refresh retains AuthLost: {error:?}");
            };
            assert_eq!(*transmission_state, Some(TransmissionState::Acknowledged));
        }
        _ => panic!("a 401 surviving the forced refresh is Error::AuthLost"),
    }
}

/// What no Graph-local seam could reach, and what graph-T1 was filed
/// for: the retry loop runs BELOW this crate's funnel, so a seam that
/// answered at the funnel could pin which outcome a status produced
/// but never how many attempts it cost. Scripting at the wire makes
/// the attempt count observable - one funnel call, two wire requests,
/// and a transient 503 the caller never sees.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_transient_5xx_is_retried_below_the_graph_funnel() {
    use reqwest::StatusCode;

    let client = GraphClient::new("token");
    client.script_rest_with_retries([
        ScriptedRestResponse::empty(StatusCode::SERVICE_UNAVAILABLE)
            .with_header("Retry-After", "0"),
        ScriptedRestResponse::json(StatusCode::OK, serde_json::json!({"ok": true})),
    ]);

    let url = client.api_url("/me").expect("api url");
    let response = client
        .execute_wire(
            Resource::Outlook,
            &url,
            reqwest::Method::GET,
            None,
            RequestOptions::default(),
            None,
            bifrost_net::ReplayPolicy::Idempotent,
        )
        .await
        .expect("the retried 503 succeeds on the second attempt");

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(
        client.wire_attempts(),
        2,
        "the transport made two wire attempts"
    );
    assert_eq!(
        client.take_rest_requests().len(),
        1,
        "the retry is invisible at the Graph funnel - it issued one request"
    );
}

/// A script that runs out must fail the test at the request that
/// exceeded it. Falling through would put a real socket behind an
/// unexpected request and leave it out of the recorded list, so a test
/// asserting "exactly N requests" could not see the N+1th.
///
/// The panic now comes from bifrost-net's dispatcher rather than a
/// Graph-local queue, which is the point of the consolidation: the
/// guarantee is enforced once, for every crate riding the transport.
#[tokio::test]
#[should_panic(expected = "scripted dispatch exhausted")]
async fn an_exhausted_script_fails_loudly_instead_of_reaching_the_network() {
    let client = GraphClient::new("token");
    client.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        serde_json::json!({}),
    )]);
    let _ = client.get_profile().await;
    let _ = client.get_profile().await;
}
