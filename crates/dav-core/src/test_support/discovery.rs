//! Credential discovery scenarios shared by the two dialect suites.
//!
//! The callbacks deliberately invoke each real client's discovery and listing
//! methods. Moving those adapters into dav-core would require a dependency
//! cycle, while replacing them with dispatcher calls would stop testing the
//! parser-to-admission boundary that can expose an enterprise user's secret.

use std::future::Future;

use bifrost_net::AccountNet;
use bifrost_types::AccountError;
use reqwest::StatusCode;
use reqwest::header::AUTHORIZATION;

use super::{dav_redirect, dav_script, scripted_dav_net, transcripts};
use crate::{DavHref, DavProtocol, DavResponse};

const BASE: &str = "https://dav.example.test";
const HOME: &str = "https://homes.example.test/homes/ada/";
const PLAIN_HOME: &str = "http://homes.example.test/homes/ada/";
const REDIRECT_HOME: &str = "https://homes.example.test/dav/homes/ada/";

fn response(body: String) -> DavResponse {
    DavResponse {
        status: StatusCode::MULTI_STATUS,
        headers: reqwest::header::HeaderMap::new(),
        body,
        url: DavHref::parse(BASE).expect("test DAV URL"),
    }
}

fn properties(protocol: DavProtocol) -> (&'static str, &'static str, &'static str) {
    match protocol {
        DavProtocol::CalDav => (
            "urn:ietf:params:xml:ns:caldav",
            "calendar-home-set",
            "calendar",
        ),
        DavProtocol::CardDav => (
            "urn:ietf:params:xml:ns:carddav",
            "addressbook-home-set",
            "addressbook",
        ),
    }
}

/// Exercise authenticated home discovery and subsequent credential admission.
///
/// The adapter discovers the home, admits it and lists its collections, then
/// returns the discovered home. Both adapters use `Bearer token` credentials.
///
/// # Panics
/// Panics if discovery, admission or wire credential isolation regresses.
pub async fn discovered_home<F, Fut>(protocol: DavProtocol, plaintext: bool, run: F)
where
    F: FnOnce(AccountNet, &'static str) -> Fut,
    Fut: Future<Output = Result<String, AccountError>>,
{
    let home = if plaintext { PLAIN_HOME } else { HOME };
    let (namespace, property, _) = properties(protocol);
    let script = dav_script([
        response("<D:current-user-principal xmlns:D=\"DAV:\"><D:href>/principals/ada/</D:href></D:current-user-principal>".into()),
        response(format!("<C:{property} xmlns:D=\"DAV:\" xmlns:C=\"{namespace}\"><D:href>{home}</D:href></C:{property}>")),
        response("<D:multistatus xmlns:D=\"DAV:\"/>".into()),
    ]);
    let result = run(scripted_dav_net(&script), home).await;
    let requests = transcripts(&script);
    if plaintext {
        assert!(result.is_err(), "a downgraded discovered origin is refused");
        assert_eq!(requests.len(), 2);
        assert!(requests.iter().all(|request| request.url.starts_with(BASE)));
    } else {
        assert_eq!(
            result.expect("cross-origin home is credential-bearing"),
            home
        );
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[2].url, home);
        assert_eq!(requests[2].headers[AUTHORIZATION], "Bearer token");
    }
}

/// Exercise a real dialect listing through an admitted cross-origin redirect.
///
/// The adapter admits `home`, lists from the configured origin, and returns
/// the first collection href. Relative hrefs must use the effective wire URL.
///
/// # Panics
/// Panics if redirect credentials, method preservation or href rebasing regress.
pub async fn redirected_home<F, Fut>(protocol: DavProtocol, run: F)
where
    F: FnOnce(AccountNet, &'static str) -> Fut,
    Fut: Future<Output = Result<String, AccountError>>,
{
    let (namespace, _, collection) = properties(protocol);
    let script = dav_script([
        dav_redirect(StatusCode::MOVED_PERMANENTLY, REDIRECT_HOME),
        response(format!("<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"{namespace}\"><D:response><D:href>team/</D:href><D:propstat><D:prop><D:resourcetype><D:collection/><C:{collection}/></D:resourcetype></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>")).into(),
    ]);
    let href = run(scripted_dav_net(&script), HOME)
        .await
        .expect("cross-origin redirect is followed with credentials");
    let requests = transcripts(&script);
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1].url, REDIRECT_HOME);
    assert_eq!(requests[1].method, *crate::PROPFIND);
    assert_eq!(requests[1].headers[AUTHORIZATION], "Bearer token");
    assert_eq!(href, format!("{REDIRECT_HOME}team/"));
}
