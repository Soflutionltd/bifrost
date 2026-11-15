//! Shared assertions instantiated with each protocol crate's real dialect and
//! parsers. The wire remains scripted below bifrost-net, with no listener.

use bifrost_types::{AccountErrorKind, AccountOperation, ProtocolErrorKind};
use reqwest::{StatusCode, header::HeaderMap};

use super::{dav_retried, dav_script, dav_script_yielding, scripted_dav_net, transcripts};
use crate::collection::{
    DavCollectionClient, Dialect, MULTIGET_BATCH_SIZE, MULTIGET_LEG_CONCURRENCY,
};
use crate::{DavCredentials, DavDispatch, DavResponse};

const BASE: &str = "https://dav.example.test";
const COLLECTION: &str = "https://dav.example.test/collection/";

fn dispatch<D: Dialect>(
    script: &std::sync::Arc<bifrost_net::test_support::ScriptedDispatch>,
) -> DavDispatch {
    DavDispatch::with_account_net(
        scripted_dav_net(script),
        BASE,
        DavCredentials::Basic {
            username: "user".into(),
            password: "pass".into(),
        },
        D::DAV,
    )
}

fn response(body: String) -> DavResponse {
    DavResponse {
        status: StatusCode::MULTI_STATUS,
        headers: HeaderMap::new(),
        body,
        url: crate::DavHref::parse(COLLECTION).expect("test DAV URL"),
    }
}

fn good_member<D: Dialect>() -> DavResponse {
    response(format!(
        "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"{}\"><D:response><D:href>one.{}</D:href><D:propstat><D:prop><C:{}>payload</C:{}></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>",
        D::NAMESPACE,
        D::FILE_EXTENSION,
        D::DATA_PROPERTY,
        D::DATA_PROPERTY
    ))
}

fn malformed() -> DavResponse {
    response("<D:multistatus xmlns:D=\"DAV:\"><D:response></D:multistatus>".into())
}

/// The bound must apply to real in-flight dispatches, not merely input chunks.
pub async fn multiget_concurrency<D: Dialect>() {
    let chunks = 10;
    let script = dav_script_yielding(
        (0..chunks).map(|_| response("<D:multistatus xmlns:D=\"DAV:\"/>".into())),
    );
    let dispatch = dispatch::<D>(&script);
    let uris = uris::<D>(MULTIGET_BATCH_SIZE * chunks);
    let result = DavCollectionClient::<D>::new(&dispatch)
        .fetch(COLLECTION, &uris, AccountOperation::SyncInventory)
        .await;
    assert!(result.is_ok());
    assert_eq!(transcripts(&script).len(), chunks);
    assert_eq!(script.peak_in_flight(), MULTIGET_LEG_CONCURRENCY);
    for request in transcripts(&script) {
        assert_eq!(
            request
                .headers
                .get("Depth")
                .and_then(|value| value.to_str().ok()),
            Some("0")
        );
        assert!(request.body.contains(D::MULTIGET_ELEMENT));
        assert!(request.body.contains(D::NAMESPACE));
        assert!(request.body.contains(D::DATA_PROPERTY));
    }
}

fn uris<D: Dialect>(count: usize) -> Vec<String> {
    (0..count)
        .map(|index| format!("{COLLECTION}{index}.{}", D::FILE_EXTENSION))
        .collect()
}

/// All failure paths must retain earlier observations and the failed leg's
/// classification, including a body that will not parse.
pub async fn partial_multiget<D: Dialect>(http_failure: bool) {
    let good = bifrost_net::test_support::Canned::from(good_member::<D>());
    let later = if http_failure {
        dav_retried(DavResponse {
            status: StatusCode::SERVICE_UNAVAILABLE,
            ..response(String::new())
        })
    } else {
        vec![malformed().into()]
    };
    let script = dav_script(std::iter::once(good).chain(later));
    let dispatch = dispatch::<D>(&script);
    let result = DavCollectionClient::<D>::new(&dispatch)
        .fetch(
            COLLECTION,
            &uris::<D>(MULTIGET_BATCH_SIZE + 1),
            AccountOperation::SyncInventory,
        )
        .await;
    let fetched =
        result.unwrap_or_else(|error| panic!("an earlier observation must survive: {error:?}"));
    assert_eq!(fetched.report.items.len(), 1);
    assert_eq!(
        fetched.report.items[0].uri,
        format!("{COLLECTION}one.{}", D::FILE_EXTENSION)
    );
    let error = fetched
        .degraded
        .expect("the refused sibling leg is reported");
    if !http_failure {
        assert_eq!(
            error.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed)
        );
    }
    assert_eq!(error.protocol(), Some(D::DAV.protocol()));
}

/// Nothing usable anywhere remains a failure rather than an empty success.
pub async fn wholly_malformed_multiget<D: Dialect>() {
    let script = dav_script([malformed()]);
    let dispatch = dispatch::<D>(&script);
    let result = DavCollectionClient::<D>::new(&dispatch)
        .fetch(COLLECTION, &uris::<D>(1), AccountOperation::SyncInventory)
        .await;
    let error = result.err().expect("no observations must stay an error");
    assert_eq!(
        error.kind(),
        &AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed)
    );
    assert_eq!(error.protocol(), Some(D::DAV.protocol()));
    assert_eq!(error.operation(), Some(AccountOperation::SyncInventory));
}
