#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::types::validated::MailboxName;
use crate::types::{FetchResponse, Flag, MailboxInfo, StatusItem};

// --- GreetingStatus tests ---

#[test]
fn greeting_status_variants() {
    assert_eq!(GreetingStatus::Ok, GreetingStatus::Ok);
    assert_ne!(GreetingStatus::Ok, GreetingStatus::PreAuth);
    assert_ne!(GreetingStatus::PreAuth, GreetingStatus::Bye);
}

#[test]
fn greeting_status_copy() {
    let s = GreetingStatus::PreAuth;
    let s2 = s; // Copy
    assert_eq!(s, s2);
}

// --- StatusKind tests ---

#[test]
fn status_kind_variants() {
    assert_eq!(StatusKind::Ok, StatusKind::Ok);
    assert_ne!(StatusKind::Ok, StatusKind::No);
    assert_ne!(StatusKind::No, StatusKind::Bad);
}

#[test]
fn status_kind_copy() {
    let s = StatusKind::Bad;
    let s2 = s;
    assert_eq!(s, s2);
}

// --- UntaggedStatus tests ---

#[test]
fn untagged_status_variants() {
    assert_ne!(UntaggedStatus::Ok, UntaggedStatus::No);
    assert_ne!(UntaggedStatus::Bad, UntaggedStatus::Bye);
}

// --- GreetingResponse tests ---

#[test]
fn greeting_response_ok() {
    let greeting = GreetingResponse {
        status: GreetingStatus::Ok,
        code: None,
        text: "Dovecot ready.".into(),
    };
    assert_eq!(greeting.status, GreetingStatus::Ok);
    assert!(greeting.code.is_none());
    assert_eq!(greeting.text, "Dovecot ready.");
}

#[test]
fn greeting_response_preauth_with_code() {
    let greeting = GreetingResponse {
        status: GreetingStatus::PreAuth,
        code: Some(ResponseCode::Alert),
        text: "Authenticated via TLS cert".into(),
    };
    assert_eq!(greeting.status, GreetingStatus::PreAuth);
    assert_eq!(greeting.code, Some(ResponseCode::Alert));
}

#[test]
fn greeting_response_bye() {
    let greeting = GreetingResponse {
        status: GreetingStatus::Bye,
        code: None,
        text: "Server shutting down".into(),
    };
    assert_eq!(greeting.status, GreetingStatus::Bye);
}

// --- TaggedResponse tests ---

#[test]
fn tagged_response_ok() {
    let resp = TaggedResponse {
        tag: "A001".into(),
        status: StatusKind::Ok,
        code: None,
        text: "LOGIN completed".into(),
    };
    assert_eq!(resp.tag, "A001");
    assert_eq!(resp.status, StatusKind::Ok);
    assert!(resp.code.is_none());
}

#[test]
fn tagged_response_no_with_code() {
    let resp = TaggedResponse {
        tag: "A002".into(),
        status: StatusKind::No,
        code: Some(ResponseCode::AuthenticationFailed),
        text: "Invalid credentials".into(),
    };
    assert_eq!(resp.status, StatusKind::No);
    assert_eq!(resp.code, Some(ResponseCode::AuthenticationFailed));
}

#[test]
fn tagged_response_bad() {
    let resp = TaggedResponse {
        tag: "A003".into(),
        status: StatusKind::Bad,
        code: Some(ResponseCode::ClientBug),
        text: "Syntax error".into(),
    };
    assert_eq!(resp.status, StatusKind::Bad);
    assert_eq!(resp.code, Some(ResponseCode::ClientBug));
}

// --- ContinuationRequest tests ---

#[test]
fn continuation_request() {
    let cont = ContinuationRequest {
        code: None,
        data: "Ready for literal data".into(),
    };
    assert_eq!(cont.data, "Ready for literal data");
    assert_eq!(cont.code, None);
}

#[test]
fn continuation_request_empty() {
    let cont = ContinuationRequest {
        code: None,
        data: String::new(),
    };
    assert!(cont.data.is_empty());
    assert_eq!(cont.code, None);
}

// --- ResponseCode tests ---

#[test]
fn response_code_simple_variants() {
    assert_eq!(ResponseCode::Alert, ResponseCode::Alert);
    assert_eq!(ResponseCode::Parse, ResponseCode::Parse);
    assert_eq!(ResponseCode::ReadOnly, ResponseCode::ReadOnly);
    assert_eq!(ResponseCode::ReadWrite, ResponseCode::ReadWrite);
    assert_eq!(ResponseCode::TryCreate, ResponseCode::TryCreate);
    assert_eq!(ResponseCode::NoModSeq, ResponseCode::NoModSeq);
    assert_eq!(ResponseCode::Closed, ResponseCode::Closed);
    assert_eq!(
        ResponseCode::MailboxId("abc".into()),
        ResponseCode::MailboxId("abc".into())
    );
}

#[test]
fn response_code_uid_next() {
    let code = ResponseCode::UidNext(42);
    assert_eq!(code, ResponseCode::UidNext(42));
    assert_ne!(code, ResponseCode::UidNext(99));
}

#[test]
fn response_code_uid_validity() {
    let code = ResponseCode::UidValidity(1_234_567_890);
    assert_eq!(code, ResponseCode::UidValidity(1_234_567_890));
}

#[test]
fn response_code_unseen() {
    let code = ResponseCode::Unseen(5);
    assert_eq!(code, ResponseCode::Unseen(5));
}

#[test]
fn response_code_append_uid() {
    let code = ResponseCode::AppendUid {
        uid_validity: 100,
        uids: vec![UidRange::single(200)],
    };
    match &code {
        ResponseCode::AppendUid { uid_validity, uids } => {
            assert_eq!(*uid_validity, 100);
            assert_eq!(uids, &[UidRange::single(200)]);
        }
        _ => panic!("expected AppendUid"),
    }
}

#[test]
fn response_code_copy_uid() {
    let code = ResponseCode::CopyUid {
        uid_validity: 100,
        source_uids: vec![UidRange::single(1), UidRange::range(3, 5)],
        dest_uids: vec![UidRange::single(10)],
    };
    match &code {
        ResponseCode::CopyUid {
            uid_validity,
            source_uids,
            dest_uids,
        } => {
            assert_eq!(*uid_validity, 100);
            assert_eq!(source_uids.len(), 2);
            assert_eq!(dest_uids.len(), 1);
        }
        _ => panic!("expected CopyUid"),
    }
}

#[test]
fn response_code_highest_mod_seq() {
    let code = ResponseCode::HighestModSeq(9999);
    assert_eq!(code, ResponseCode::HighestModSeq(9999));
}

#[test]
fn response_code_bad_charset() {
    let code = ResponseCode::BadCharset(vec!["utf-8".into(), "us-ascii".into()]);
    match &code {
        ResponseCode::BadCharset(charsets) => {
            assert_eq!(charsets.len(), 2);
            assert_eq!(charsets[0], "utf-8");
        }
        _ => panic!("expected BadCharset"),
    }
}

#[test]
fn response_code_permanent_flags() {
    let code = ResponseCode::PermanentFlags(vec![Flag::Seen, Flag::Flagged]);
    match &code {
        ResponseCode::PermanentFlags(flags) => {
            assert_eq!(flags.len(), 2);
            assert_eq!(flags[0], Flag::Seen);
        }
        _ => panic!("expected PermanentFlags"),
    }
}

#[test]
fn response_code_capability() {
    let code = ResponseCode::Capability(vec![Capability::Imap4Rev1, Capability::Idle]);
    match &code {
        ResponseCode::Capability(caps) => {
            assert_eq!(caps.len(), 2);
            assert_eq!(caps[0], Capability::Imap4Rev1);
        }
        _ => panic!("expected Capability"),
    }
}

#[test]
fn response_code_modified() {
    let code = ResponseCode::Modified(vec![UidRange::range(1, 10)]);
    match &code {
        ResponseCode::Modified(ranges) => {
            assert_eq!(ranges.len(), 1);
            assert_eq!(ranges[0], UidRange::range(1, 10));
        }
        _ => panic!("expected Modified"),
    }
}

#[test]
fn response_code_rfc5530_variants() {
    // Ensure the RFC 5530 extended codes are distinct.
    let codes = [
        ResponseCode::Unavailable,
        ResponseCode::AuthenticationFailed,
        ResponseCode::AuthorizationFailed,
        ResponseCode::Expired,
        ResponseCode::PrivacyRequired,
        ResponseCode::ContactAdmin,
        ResponseCode::NoPerm,
        ResponseCode::InUse,
        ResponseCode::ExpungeIssued,
        ResponseCode::Corruption,
        ResponseCode::ServerBug,
        ResponseCode::ClientBug,
        ResponseCode::Cannot,
        ResponseCode::Limit,
        ResponseCode::OverQuota,
        ResponseCode::AlreadyExists,
        ResponseCode::NonExistent,
    ];
    // Each should equal itself.
    for code in &codes {
        assert_eq!(code, &code.clone());
    }
    // First and second should differ.
    assert_ne!(codes[0], codes[1]);
}

#[test]
fn response_code_other() {
    let code = ResponseCode::Other {
        name: "CUSTOM".into(),
        value: Some("data".into()),
    };
    match &code {
        ResponseCode::Other { name, value } => {
            assert_eq!(name, "CUSTOM");
            assert_eq!(value.as_deref(), Some("data"));
        }
        _ => panic!("expected Other"),
    }
}

#[test]
fn response_code_other_no_value() {
    let code = ResponseCode::Other {
        name: "XFOO".into(),
        value: None,
    };
    match &code {
        ResponseCode::Other { name, value } => {
            assert_eq!(name, "XFOO");
            assert!(value.is_none());
        }
        _ => panic!("expected Other"),
    }
}

// --- Response enum tests ---

#[test]
fn response_greeting_variant() {
    let resp = Response::Greeting(GreetingResponse {
        status: GreetingStatus::Ok,
        code: None,
        text: "Ready".into(),
    });
    assert!(matches!(resp, Response::Greeting(_)));
}

#[test]
fn response_tagged_variant() {
    let resp = Response::Tagged(TaggedResponse {
        tag: "T1".into(),
        status: StatusKind::Ok,
        code: None,
        text: "Done".into(),
    });
    assert!(matches!(resp, Response::Tagged(_)));
}

#[test]
fn response_continuation_variant() {
    let resp = Response::Continuation(ContinuationRequest {
        code: None,
        data: "+".into(),
    });
    assert!(matches!(resp, Response::Continuation(_)));
}

// --- UntaggedResponse tests ---

#[test]
fn untagged_exists() {
    let resp = UntaggedResponse::Exists(42);
    assert_eq!(resp, UntaggedResponse::Exists(42));
    assert_ne!(resp, UntaggedResponse::Exists(0));
}

#[test]
fn untagged_recent() {
    let resp = UntaggedResponse::Recent(5);
    assert_eq!(resp, UntaggedResponse::Recent(5));
}

#[test]
fn untagged_expunge() {
    let resp = UntaggedResponse::Expunge(7);
    assert_eq!(resp, UntaggedResponse::Expunge(7));
}

#[test]
fn untagged_flags() {
    let resp = UntaggedResponse::Flags(vec![
        Flag::Seen,
        Flag::Answered,
        Flag::Custom("$Junk".into()),
    ]);
    match &resp {
        UntaggedResponse::Flags(flags) => {
            assert_eq!(flags.len(), 3);
            assert_eq!(flags[2], Flag::Custom("$Junk".into()));
        }
        _ => panic!("expected Flags"),
    }
}

#[test]
fn untagged_search() {
    let resp = UntaggedResponse::Search {
        uids: vec![1, 5, 10, 42],
        mod_seq: None,
    };
    match &resp {
        UntaggedResponse::Search { uids, mod_seq } => {
            assert_eq!(uids, &[1, 5, 10, 42]);
            assert!(mod_seq.is_none());
        }
        _ => panic!("expected Search"),
    }
}

#[test]
fn untagged_search_empty() {
    let resp = UntaggedResponse::Search {
        uids: vec![],
        mod_seq: None,
    };
    match &resp {
        UntaggedResponse::Search { uids, .. } => assert!(uids.is_empty()),
        _ => panic!("expected Search"),
    }
}

#[test]
fn untagged_status() {
    let resp = UntaggedResponse::Status {
        status: UntaggedStatus::Ok,
        code: Some(ResponseCode::UidValidity(12345)),
        text: "selected".into(),
    };
    match &resp {
        UntaggedResponse::Status { status, code, text } => {
            assert_eq!(*status, UntaggedStatus::Ok);
            assert_eq!(code, &Some(ResponseCode::UidValidity(12345)));
            assert_eq!(text, "selected");
        }
        _ => panic!("expected Status"),
    }
}

#[test]
fn untagged_mailbox_status() {
    let resp = UntaggedResponse::MailboxStatus {
        mailbox: MailboxName::new("INBOX").unwrap(),
        items: vec![StatusItem::Messages(42), StatusItem::Unseen(3)],
    };
    match &resp {
        UntaggedResponse::MailboxStatus { mailbox, items } => {
            assert_eq!(mailbox.as_str(), "INBOX");
            assert_eq!(items.len(), 2);
        }
        _ => panic!("expected MailboxStatus"),
    }
}

#[test]
fn untagged_capability() {
    let resp = UntaggedResponse::Capability(vec![
        Capability::Imap4Rev1,
        Capability::Idle,
        Capability::Auth("PLAIN".into()),
    ]);
    match &resp {
        UntaggedResponse::Capability(caps) => {
            assert_eq!(caps.len(), 3);
            assert_eq!(caps[2], Capability::Auth("PLAIN".into()));
        }
        _ => panic!("expected Capability"),
    }
}

#[test]
fn untagged_enabled() {
    let resp = UntaggedResponse::Enabled(vec!["CONDSTORE".into(), "QRESYNC".into()]);
    match &resp {
        UntaggedResponse::Enabled(exts) => {
            assert_eq!(exts, &["CONDSTORE", "QRESYNC"]);
        }
        _ => panic!("expected Enabled"),
    }
}

#[test]
fn untagged_fetch() {
    let fetch = FetchResponse {
        seq: 1,
        uid: Some(100),
        ..Default::default()
    };
    let resp = UntaggedResponse::Fetch(Box::new(fetch));
    match &resp {
        UntaggedResponse::Fetch(f) => {
            assert_eq!(f.seq, 1);
            assert_eq!(f.uid, Some(100));
        }
        _ => panic!("expected Fetch"),
    }
}

#[test]
fn untagged_list() {
    let info = MailboxInfo {
        name: MailboxName::new("INBOX").unwrap(),
        delimiter: Some('/'),
        attributes: vec![],
        ..Default::default()
    };
    let resp = UntaggedResponse::List(info);
    match &resp {
        UntaggedResponse::List(i) => {
            assert_eq!(i.name.as_str(), "INBOX");
            assert_eq!(i.delimiter, Some('/'));
        }
        _ => panic!("expected List"),
    }
}

#[test]
fn untagged_vanished() {
    let resp = UntaggedResponse::Vanished {
        earlier: true,
        uids: vec![UidRange::range(1, 5), UidRange::single(10)],
    };
    match &resp {
        UntaggedResponse::Vanished { earlier, uids } => {
            assert!(*earlier);
            assert_eq!(uids.len(), 2);
        }
        _ => panic!("expected Vanished"),
    }
}

#[test]
fn untagged_id() {
    let resp = UntaggedResponse::Id(vec![
        ("name".into(), Some("Dovecot".into())),
        ("version".into(), None),
    ]);
    match &resp {
        UntaggedResponse::Id(pairs) => {
            assert_eq!(pairs.len(), 2);
            assert_eq!(pairs[0].1.as_deref(), Some("Dovecot"));
            assert!(pairs[1].1.is_none());
        }
        _ => panic!("expected Id"),
    }
}

#[test]
fn untagged_namespace() {
    let resp = UntaggedResponse::Namespace {
        personal: vec![NamespaceDescriptor {
            prefix: String::new(),
            delimiter: Some('/'),
            extensions: vec![],
        }],
        other: vec![],
        shared: vec![NamespaceDescriptor {
            prefix: "#shared.".into(),
            delimiter: Some('.'),
            extensions: vec![],
        }],
    };
    match &resp {
        UntaggedResponse::Namespace {
            personal,
            other,
            shared,
        } => {
            assert_eq!(personal.len(), 1);
            assert!(other.is_empty());
            assert_eq!(shared.len(), 1);
            assert_eq!(shared[0].prefix, "#shared.");
        }
        _ => panic!("expected Namespace"),
    }
}

#[test]
fn untagged_quota() {
    let resp = UntaggedResponse::Quota {
        root: String::new(),
        resources: vec![QuotaResource {
            name: "STORAGE".into(),
            usage: 1024,
            limit: 10240,
        }],
    };
    match &resp {
        UntaggedResponse::Quota { root, resources } => {
            assert!(root.is_empty());
            assert_eq!(resources.len(), 1);
            assert_eq!(resources[0].usage, 1024);
            assert_eq!(resources[0].limit, 10240);
        }
        _ => panic!("expected Quota"),
    }
}

#[test]
fn untagged_quota_root() {
    let resp = UntaggedResponse::QuotaRoot {
        mailbox: MailboxName::new("INBOX").unwrap(),
        roots: vec![String::new(), "user.alice".into()],
    };
    match &resp {
        UntaggedResponse::QuotaRoot { mailbox, roots } => {
            assert_eq!(mailbox.as_str(), "INBOX");
            assert_eq!(roots.len(), 2);
        }
        _ => panic!("expected QuotaRoot"),
    }
}

#[test]
fn untagged_acl() {
    let resp = UntaggedResponse::Acl {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![AclEntry {
            identifier: "alice".into(),
            rights: "lrswipkxte".into(),
        }],
    };
    match &resp {
        UntaggedResponse::Acl { mailbox, entries } => {
            assert_eq!(mailbox.as_str(), "INBOX");
            assert_eq!(entries[0].identifier, "alice");
        }
        _ => panic!("expected Acl"),
    }
}

#[test]
fn untagged_my_rights() {
    let resp = UntaggedResponse::MyRights {
        mailbox: MailboxName::new("INBOX").unwrap(),
        rights: "lrs".into(),
    };
    match &resp {
        UntaggedResponse::MyRights { mailbox, rights } => {
            assert_eq!(mailbox.as_str(), "INBOX");
            assert_eq!(rights, "lrs");
        }
        _ => panic!("expected MyRights"),
    }
}

#[test]
fn untagged_list_rights() {
    let resp = UntaggedResponse::ListRights {
        mailbox: MailboxName::new("INBOX").unwrap(),
        identifier: "bob".into(),
        required: "l".into(),
        optional: vec!["r".into(), "s".into()],
    };
    match &resp {
        UntaggedResponse::ListRights {
            mailbox,
            identifier,
            required,
            optional,
        } => {
            assert_eq!(mailbox.as_str(), "INBOX");
            assert_eq!(identifier, "bob");
            assert_eq!(required, "l");
            assert_eq!(optional.len(), 2);
        }
        _ => panic!("expected ListRights"),
    }
}

#[test]
fn untagged_metadata() {
    let resp = UntaggedResponse::Metadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![
            MetadataEntry {
                name: "/private/comment".into(),
                value: Some(b"My notes".to_vec()),
            },
            MetadataEntry {
                name: "/shared/vendor/foo".into(),
                value: None,
            },
        ],
    };
    match &resp {
        UntaggedResponse::Metadata { mailbox, entries } => {
            assert_eq!(mailbox.as_str(), "INBOX");
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].value.as_deref(), Some(b"My notes".as_slice()));
            assert!(entries[1].value.is_none());
        }
        _ => panic!("expected Metadata"),
    }
}

#[test]
fn untagged_thread() {
    let resp = UntaggedResponse::Thread(vec![ThreadNode {
        id: Some(1),
        children: vec![
            ThreadNode {
                id: Some(2),
                children: vec![],
            },
            ThreadNode {
                id: Some(3),
                children: vec![ThreadNode {
                    id: Some(4),
                    children: vec![],
                }],
            },
        ],
    }]);
    match &resp {
        UntaggedResponse::Thread(nodes) => {
            assert_eq!(nodes.len(), 1);
            assert_eq!(nodes[0].id, Some(1));
            assert_eq!(nodes[0].children.len(), 2);
            assert_eq!(nodes[0].children[1].children[0].id, Some(4));
        }
        _ => panic!("expected Thread"),
    }
}

// --- Capability tests ---

#[test]
fn capability_variants() {
    assert_eq!(Capability::Imap4Rev1, Capability::Imap4Rev1);
    assert_ne!(Capability::Imap4Rev1, Capability::Imap4Rev2);
    assert_eq!(
        Capability::Auth("PLAIN".into()),
        Capability::Auth("PLAIN".into())
    );
    assert_ne!(
        Capability::Auth("PLAIN".into()),
        Capability::Auth("XOAUTH2".into())
    );
}

#[test]
fn capability_other() {
    let cap = Capability::Other("XSPECIAL".into());
    assert_eq!(cap, Capability::Other("XSPECIAL".into()));
}

// ===== RFC 3501 Section 7.2.1 audit: capability names are case-insensitive =====

#[test]
fn capability_other_case_insensitive() {
    // RFC 3501 Section 7.2.1: capability names are case-insensitive.
    // Two `Other` variants differing only in case must compare equal.
    assert_eq!(
        Capability::Other("XYZZY".into()),
        Capability::Other("xyzzy".into()),
        "Capability::Other must compare case-insensitively per RFC 3501 Section 7.2.1"
    );
}

#[test]
fn capability_auth_case_insensitive() {
    // RFC 3501 Section 7.2.1: capability names are case-insensitive.
    assert_eq!(
        Capability::Auth("PLAIN".into()),
        Capability::Auth("plain".into()),
        "Capability::Auth must compare case-insensitively per RFC 3501 Section 7.2.1"
    );
}

#[test]
fn capability_thread_case_insensitive() {
    // RFC 3501 Section 7.2.1: capability names are case-insensitive.
    assert_eq!(
        Capability::Thread("REFERENCES".into()),
        Capability::Thread("references".into()),
        "Capability::Thread must compare case-insensitively per RFC 3501 Section 7.2.1"
    );
}

#[test]
fn capability_other_case_insensitive_hash() {
    // RFC 3501 Section 7.2.1: case-insensitively equal capabilities must hash the same.
    use std::collections::HashSet;
    let mut set = HashSet::new();
    set.insert(Capability::Other("XYZZY".into()));
    set.insert(Capability::Other("xyzzy".into()));
    assert_eq!(
        set.len(),
        1,
        "Case-insensitively equal Capability::Other must have the same Hash \
             per RFC 3501 Section 7.2.1"
    );
}

#[test]
fn capability_thread() {
    let cap = Capability::Thread("REFERENCES".into());
    assert_eq!(cap, Capability::Thread("REFERENCES".into()));
    assert_ne!(cap, Capability::Thread("ORDEREDSUBJECT".into()));
}

#[test]
fn capability_append_limit() {
    assert_eq!(
        Capability::AppendLimit(Some(1024)),
        Capability::AppendLimit(Some(1024))
    );
    assert_eq!(Capability::AppendLimit(None), Capability::AppendLimit(None));
    assert_ne!(
        Capability::AppendLimit(Some(1024)),
        Capability::AppendLimit(None)
    );
}

#[test]
fn capability_hash() {
    use std::collections::HashSet;
    let mut set = HashSet::new();
    set.insert(Capability::Imap4Rev1);
    set.insert(Capability::Idle);
    set.insert(Capability::Imap4Rev1); // duplicate
    assert_eq!(set.len(), 2);
}

// --- UidRange tests ---

#[test]
fn uid_range_single() {
    let r = UidRange::single(42);
    assert_eq!(r.start, 42);
    assert!(r.end.is_none());
}

#[test]
fn uid_range_range() {
    let r = UidRange::range(1, 100);
    assert_eq!(r.start, 1);
    assert_eq!(r.end, Some(100));
}

#[test]
fn uid_range_copy() {
    let r = UidRange::range(5, 10);
    let r2 = r; // Copy
    assert_eq!(r, r2);
}

// --- UidRange nz-number validation tests (RFC 3501 Section 9) ---

#[test]
fn uid_range_try_single_zero_returns_none() {
    // UIDs are nz-number per RFC 3501 Section 9: 0 is invalid.
    assert!(UidRange::try_single(0).is_none());
}

#[test]
fn uid_range_try_single_nonzero_returns_some() {
    let r = UidRange::try_single(1).unwrap();
    assert_eq!(r.start, 1);
    assert!(r.end.is_none());
}

#[test]
fn uid_range_try_range_zero_start_returns_none() {
    // start=0 violates nz-number (RFC 3501 Section 9).
    assert!(UidRange::try_range(0, 5).is_none());
}

#[test]
fn uid_range_try_range_zero_end_returns_none() {
    // end=0 violates nz-number (RFC 3501 Section 9).
    assert!(UidRange::try_range(5, 0).is_none());
}

#[test]
fn uid_range_try_range_nonzero_returns_some() {
    let r = UidRange::try_range(1, 5).unwrap();
    assert_eq!(r.start, 1);
    assert_eq!(r.end, Some(5));
}

// --- NamespaceDescriptor tests ---

#[test]
fn namespace_descriptor() {
    let ns = NamespaceDescriptor {
        prefix: "INBOX.".into(),
        delimiter: Some('.'),
        extensions: vec![],
    };
    assert_eq!(ns.prefix, "INBOX.");
    assert_eq!(ns.delimiter, Some('.'));
    assert!(ns.extensions.is_empty());
}

#[test]
fn namespace_descriptor_no_delimiter() {
    let ns = NamespaceDescriptor {
        prefix: String::new(),
        delimiter: None,
        extensions: vec![],
    };
    assert!(ns.prefix.is_empty());
    assert!(ns.delimiter.is_none());
    assert!(ns.extensions.is_empty());
}

// --- QuotaResource tests ---

#[test]
fn quota_resource() {
    let qr = QuotaResource {
        name: "MESSAGE".into(),
        usage: 50,
        limit: 1000,
    };
    assert_eq!(qr.name, "MESSAGE");
    assert_eq!(qr.usage, 50);
    assert_eq!(qr.limit, 1000);
}

// --- AclEntry tests ---

#[test]
fn acl_entry() {
    let entry = AclEntry {
        identifier: "admin".into(),
        rights: "lrswipkxtea".into(),
    };
    assert_eq!(entry.identifier, "admin");
    assert_eq!(entry.rights, "lrswipkxtea");
}

// --- MetadataEntry tests ---

#[test]
fn metadata_entry_with_value() {
    let entry = MetadataEntry {
        name: "/private/comment".into(),
        value: Some(b"hello".to_vec()),
    };
    assert_eq!(entry.name, "/private/comment");
    assert_eq!(entry.value.as_deref(), Some(b"hello".as_slice()));
}

#[test]
fn metadata_entry_nil_value() {
    let entry = MetadataEntry {
        name: "/shared/something".into(),
        value: None,
    };
    assert!(entry.value.is_none());
}

// --- ThreadNode tests ---

#[test]
fn thread_node_leaf() {
    let node = ThreadNode {
        id: Some(5),
        children: vec![],
    };
    assert_eq!(node.id, Some(5));
    assert!(node.children.is_empty());
}

#[test]
fn thread_node_dummy_parent() {
    let node = ThreadNode {
        id: None,
        children: vec![ThreadNode {
            id: Some(1),
            children: vec![],
        }],
    };
    assert_eq!(node.id, None);
    assert_eq!(node.children.len(), 1);
}

// --- Response wrapping via boxed UntaggedResponse ---

#[test]
fn response_untagged_boxed() {
    let untagged = UntaggedResponse::Exists(10);
    let resp = Response::Untagged(Box::new(untagged));
    match resp {
        Response::Untagged(inner) => {
            assert_eq!(*inner, UntaggedResponse::Exists(10));
        }
        _ => panic!("expected Untagged"),
    }
}

// --- EsearchResponse ---

#[test]
fn esearch_response_default() {
    let esearch = EsearchResponse::default();
    assert_eq!(esearch.tag, None);
    assert!(!esearch.uid);
    assert_eq!(esearch.min, None);
    assert_eq!(esearch.max, None);
    assert_eq!(esearch.count, None);
    assert!(esearch.all.is_empty());
}

#[test]
fn esearch_response_clone_eq() {
    let esearch = EsearchResponse {
        tag: Some("A001".into()),
        uid: true,
        min: Some(1),
        max: Some(100),
        count: Some(50),
        all: vec![UidRange::range(1, 50), UidRange::range(51, 100)],
        mod_seq: None,
    };
    let cloned = esearch.clone();
    assert_eq!(esearch, cloned);
}

#[test]
fn esearch_response_ne() {
    let a = EsearchResponse {
        min: Some(1),
        ..EsearchResponse::default()
    };
    let b = EsearchResponse {
        min: Some(2),
        ..EsearchResponse::default()
    };
    assert_ne!(a, b);
}

// ===== Spec audit: failing tests for known deviations =====

// ===== RFC 3501 Section 7.2.1 audit: cross-representation equality =====

#[test]
fn custom_idle_equals_idle_variant() {
    // RFC 3501 Section 7.2.1: capability names are case-insensitive atoms.
    // Custom("IDLE") represents the same capability as Capability::Idle.
    assert_eq!(
        Capability::Other("IDLE".into()),
        Capability::Idle,
        "Other(\"IDLE\") must equal Capability::Idle per RFC 3501 Section 7.2.1"
    );
}

#[test]
fn custom_capability_cross_representation_hash() {
    // RFC 3501 Section 7.2.1: equal capabilities must hash identically.
    use std::collections::HashSet;
    let mut set = HashSet::new();
    set.insert(Capability::Idle);
    set.insert(Capability::Other("IDLE".into()));
    assert_eq!(
        set.len(),
        1,
        "Other(\"IDLE\") and Capability::Idle must hash the same \
             per RFC 3501 Section 7.2.1"
    );
}

#[test]
fn custom_starttls_equals_starttls_variant() {
    // RFC 3501 Section 7.2.1: capability names are case-insensitive atoms.
    assert_eq!(
        Capability::Other("STARTTLS".into()),
        Capability::StartTls,
        "Other(\"STARTTLS\") must equal Capability::StartTls \
             per RFC 3501 Section 7.2.1"
    );
}

#[test]
fn custom_auth_plain_equals_auth_variant() {
    // RFC 3501 Section 7.2.1: AUTH=PLAIN as Other must equal Auth("PLAIN").
    assert_eq!(
        Capability::Other("AUTH=PLAIN".into()),
        Capability::Auth("PLAIN".into()),
        "Other(\"AUTH=PLAIN\") must equal Auth(\"PLAIN\") \
             per RFC 3501 Section 7.2.1"
    );
}

#[test]
fn sort_display_as_imap_str_round_trip() {
    // RFC 5957 defines the capability as `SORT=DISPLAY`. The parser stores the
    // token as the unit variant `SortDisplay`.
    // `as_imap_str()` must reconstruct the original wire form `SORT=DISPLAY`,
    // not `SORT=DISPLAYDISPLAY`.
    let cap = Capability::SortDisplay;
    assert_eq!(cap.as_imap_str(), "SORT=DISPLAY");
}

#[test]
fn sort_display_cross_representation_equality() {
    // Cross-representation: Other("SORT=DISPLAY") must equal SortDisplay.
    let structured = Capability::SortDisplay;
    let other = Capability::Other("SORT=DISPLAY".to_owned());
    assert_eq!(structured, other);
    assert_eq!(other, structured);
}

#[test]
fn sort_display_hash_consistency() {
    // Equal capabilities must produce the same hash (Hash/Eq contract).
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let a = Capability::SortDisplay;
    let b = Capability::Other("SORT=DISPLAY".to_owned());

    let hash = |c: &Capability| {
        let mut h = DefaultHasher::new();
        c.hash(&mut h);
        h.finish()
    };
    assert_eq!(a, b);
    assert_eq!(hash(&a), hash(&b));
}

#[test]
fn spec_audit_l12_literal_minus_capability() {
    // RFC 7888 Section 5 defines the LITERAL- capability, which indicates
    // the server supports non-synchronizing literals only for small payloads
    // (up to 4096 bytes). It should have a dedicated Capability::LiteralMinus
    // variant, not Capability::Other("LITERAL-").
    let input = b"* CAPABILITY IMAP4rev1 LITERAL-\r\n";
    let (_, resp) = crate::codec::decode::parse_response(input).unwrap();
    match resp {
        Response::Untagged(inner) => match *inner {
            UntaggedResponse::Capability(ref caps) => {
                // LITERAL- should NOT be an Other variant.
                let has_dedicated = caps.iter().any(|c| {
                    // If a dedicated LiteralMinus variant existed, it would
                    // NOT match Capability::Other(_).
                    !matches!(c, Capability::Other(_)) && !matches!(c, Capability::Imap4Rev1)
                });
                assert!(
                    has_dedicated,
                    "LITERAL- should have a dedicated Capability variant, \
                         not Capability::Other; got {caps:?}"
                );
            }
            other => panic!("expected Capability, got {other:?}"),
        },
        other => panic!("expected Untagged, got {other:?}"),
    }
}

// --- TaggedResponse status dispatch tests ---
// Pre-refactor: validate the three-arm Ok/No/Bad dispatch that will be
// extracted into TaggedResponse::require_ok().

#[test]
fn tagged_ok_returns_no_error() {
    use crate::error::Error;
    let tagged = TaggedResponse {
        tag: "A001".into(),
        status: StatusKind::Ok,
        code: None,
        text: "completed".into(),
    };
    let result: Result<(), Error> = match tagged.status {
        StatusKind::Ok => Ok(()),
        StatusKind::No => Err(Error::no_with_code(tagged.text, tagged.code)),
        StatusKind::Bad => Err(Error::bad_with_code(tagged.text, tagged.code)),
    };
    assert!(result.is_ok());
}

#[test]
fn tagged_no_returns_no_error_variant() {
    use crate::error::Error;
    let tagged = TaggedResponse {
        tag: "A002".into(),
        status: StatusKind::No,
        code: Some(ResponseCode::NonExistent),
        text: "mailbox not found".into(),
    };
    let result: Result<(), Error> = match tagged.status {
        StatusKind::Ok => Ok(()),
        StatusKind::No => Err(Error::no_with_code(tagged.text, tagged.code)),
        StatusKind::Bad => Err(Error::bad_with_code(tagged.text, tagged.code)),
    };
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::No { text, code, .. } => {
            assert_eq!(text, "mailbox not found");
            assert_eq!(code, Some(ResponseCode::NonExistent));
        }
        other => panic!("expected Error::No, got {other:?}"),
    }
}

#[test]
fn tagged_bad_returns_bad_error() {
    use crate::error::Error;
    let tagged = TaggedResponse {
        tag: "A003".into(),
        status: StatusKind::Bad,
        code: None,
        text: "syntax error".into(),
    };
    let result: Result<(), Error> = match tagged.status {
        StatusKind::Ok => Ok(()),
        StatusKind::No => Err(Error::no_with_code(tagged.text, tagged.code)),
        StatusKind::Bad => Err(Error::bad_with_code(tagged.text, tagged.code)),
    };
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::Bad { text, code, .. } => {
            assert_eq!(text, "syntax error");
            assert!(code.is_none());
        }
        other => panic!("expected Error::Bad, got {other:?}"),
    }
}

#[test]
fn tagged_ok_with_response_code_succeeds() {
    use crate::error::Error;
    let tagged = TaggedResponse {
        tag: "A004".into(),
        status: StatusKind::Ok,
        code: Some(ResponseCode::ReadWrite),
        text: "SELECT completed".into(),
    };
    let result: Result<(), Error> = match tagged.status {
        StatusKind::Ok => Ok(()),
        StatusKind::No => Err(Error::no_with_code(tagged.text, tagged.code)),
        StatusKind::Bad => Err(Error::bad_with_code(tagged.text, tagged.code)),
    };
    assert!(result.is_ok());
}

// --- Capability::as_imap_str() tests ---
// RFC 3501 Section 7.2.1 / RFC 9051 Section 7.2.1: wire representation.

#[test]
fn capability_as_imap_str_all_variants() {
    // RFC 3501 Section 7.2.1: each capability has a defined wire form.
    assert_eq!(Capability::Imap4Rev1.as_imap_str(), "IMAP4rev1");
    assert_eq!(Capability::Imap4Rev2.as_imap_str(), "IMAP4rev2");
    assert_eq!(Capability::Acl.as_imap_str(), "ACL");
    assert_eq!(
        Capability::AppendLimit(Some(1024)).as_imap_str(),
        "APPENDLIMIT=1024"
    );
    assert_eq!(Capability::AppendLimit(None).as_imap_str(), "APPENDLIMIT");
    assert_eq!(Capability::Binary.as_imap_str(), "BINARY");
    assert_eq!(Capability::Children.as_imap_str(), "CHILDREN");
    assert_eq!(
        Capability::CompressDeflate.as_imap_str(),
        "COMPRESS=DEFLATE"
    );
    assert_eq!(Capability::Condstore.as_imap_str(), "CONDSTORE");
    assert_eq!(
        Capability::CreateSpecialUse.as_imap_str(),
        "CREATE-SPECIAL-USE"
    );
    assert_eq!(Capability::Enable.as_imap_str(), "ENABLE");
    assert_eq!(Capability::Esearch.as_imap_str(), "ESEARCH");
    assert_eq!(Capability::Id.as_imap_str(), "ID");
    assert_eq!(Capability::Idle.as_imap_str(), "IDLE");
    assert_eq!(Capability::ListExtended.as_imap_str(), "LIST-EXTENDED");
    assert_eq!(Capability::ListStatus.as_imap_str(), "LIST-STATUS");
    assert_eq!(Capability::LiteralPlus.as_imap_str(), "LITERAL+");
    assert_eq!(Capability::LoginDisabled.as_imap_str(), "LOGINDISABLED");
    assert_eq!(Capability::LiteralMinus.as_imap_str(), "LITERAL-");
    assert_eq!(Capability::Metadata.as_imap_str(), "METADATA");
    assert_eq!(Capability::MetadataServer.as_imap_str(), "METADATA-SERVER");
    assert_eq!(Capability::Move.as_imap_str(), "MOVE");
    assert_eq!(Capability::MultiAppend.as_imap_str(), "MULTIAPPEND");
    assert_eq!(Capability::Namespace.as_imap_str(), "NAMESPACE");
    assert_eq!(Capability::ObjectId.as_imap_str(), "OBJECTID");
    assert_eq!(Capability::Preview.as_imap_str(), "PREVIEW");
    assert_eq!(Capability::QResync.as_imap_str(), "QRESYNC");
    assert_eq!(Capability::Quota.as_imap_str(), "QUOTA");
    assert_eq!(
        Capability::QuotaResource("STORAGE".into()).as_imap_str(),
        "QUOTA=RES-STORAGE"
    );
    assert_eq!(Capability::QuotaSet.as_imap_str(), "QUOTASET");
    assert_eq!(
        Capability::Rights("texk".into()).as_imap_str(),
        "RIGHTS=texk"
    );
    assert_eq!(Capability::SaslIr.as_imap_str(), "SASL-IR");
    assert_eq!(Capability::SaveDate.as_imap_str(), "SAVEDATE");
    assert_eq!(Capability::SearchRes.as_imap_str(), "SEARCHRES");
    assert_eq!(Capability::Sort.as_imap_str(), "SORT");
    assert_eq!(Capability::SortDisplay.as_imap_str(), "SORT=DISPLAY");
    assert_eq!(Capability::StartTls.as_imap_str(), "STARTTLS");
    assert_eq!(Capability::SpecialUse.as_imap_str(), "SPECIAL-USE");
    assert_eq!(
        Capability::Thread("REFERENCES".into()).as_imap_str(),
        "THREAD=REFERENCES"
    );
    assert_eq!(Capability::StatusSize.as_imap_str(), "STATUS=SIZE");
    assert_eq!(Capability::Unauthenticate.as_imap_str(), "UNAUTHENTICATE");
    assert_eq!(Capability::UidPlus.as_imap_str(), "UIDPLUS");
    assert_eq!(Capability::Unselect.as_imap_str(), "UNSELECT");
    assert_eq!(Capability::Within.as_imap_str(), "WITHIN");
    assert_eq!(Capability::XGmExt1.as_imap_str(), "X-GM-EXT-1");
    assert_eq!(Capability::Utf8Accept.as_imap_str(), "UTF8=ACCEPT");
    assert_eq!(Capability::Utf8Only.as_imap_str(), "UTF8=ONLY");
    assert_eq!(
        Capability::Auth("XOAUTH2".into()).as_imap_str(),
        "AUTH=XOAUTH2"
    );
    assert_eq!(
        Capability::Other("XSPECIAL".into()).as_imap_str(),
        "XSPECIAL"
    );
}

// --- Capability PartialEq for string-carrying variants ---

#[test]
fn capability_sort_display_equality() {
    // RFC 5957: SORT=DISPLAY capability.
    // The parser recognizes the fixed token case-insensitively.
    assert_eq!(
        Capability::SortDisplay,
        Capability::from_imap_str("sort=display"),
        "SortDisplay must compare case-insensitively per RFC 3501 Section 7.2.1"
    );
    assert_ne!(
        Capability::SortDisplay,
        Capability::from_imap_str("SORT=OTHER"),
    );
}

#[test]
fn capability_rights_equality() {
    // RFC 4314 Section 6: RIGHTS=<chars> capability.
    // Rights variants with same payload must be equal (case-insensitive).
    assert_eq!(
        Capability::Rights("texk".into()),
        Capability::Rights("TEXK".into()),
        "Rights must compare case-insensitively per RFC 3501 Section 7.2.1"
    );
    assert_ne!(
        Capability::Rights("texk".into()),
        Capability::Rights("abc".into()),
    );
}

// --- TaggedResponse::require_ok() tests ---
// RFC 3501 Section 7.1 / RFC 9051 Section 7.1: OK/NO/BAD status dispatch.

#[test]
fn require_ok_succeeds_on_ok() {
    // RFC 3501 Section 7.1: OK indicates success.
    let tagged = TaggedResponse {
        tag: "A001".into(),
        status: StatusKind::Ok,
        code: Some(ResponseCode::ReadWrite),
        text: "SELECT completed".into(),
    };
    let result = tagged.require_ok();
    assert!(result.is_ok());
    let resp = result.unwrap();
    assert_eq!(resp.tag, "A001");
    assert_eq!(resp.code, Some(ResponseCode::ReadWrite));
}

#[test]
fn require_ok_returns_error_on_no() {
    use crate::error::Error;
    // RFC 3501 Section 7.1: NO indicates an operational error.
    let tagged = TaggedResponse {
        tag: "A002".into(),
        status: StatusKind::No,
        code: Some(ResponseCode::NonExistent),
        text: "mailbox not found".into(),
    };
    let result = tagged.require_ok();
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::No { text, code, .. } => {
            assert_eq!(text, "mailbox not found");
            assert_eq!(code, Some(ResponseCode::NonExistent));
        }
        other => panic!("expected Error::No, got {other:?}"),
    }
}

#[test]
fn require_ok_returns_error_on_bad() {
    use crate::error::Error;
    // RFC 3501 Section 7.1: BAD indicates a protocol-level error.
    let tagged = TaggedResponse {
        tag: "A003".into(),
        status: StatusKind::Bad,
        code: None,
        text: "syntax error in command".into(),
    };
    let result = tagged.require_ok();
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::Bad { text, code, .. } => {
            assert_eq!(text, "syntax error in command");
            assert!(code.is_none());
        }
        other => panic!("expected Error::Bad, got {other:?}"),
    }
}

#[test]
fn require_ok_no_without_code() {
    use crate::error::Error;
    // RFC 3501 Section 7.1: NO without a response code.
    let tagged = TaggedResponse {
        tag: "A004".into(),
        status: StatusKind::No,
        code: None,
        text: "operation failed".into(),
    };
    let result = tagged.require_ok();
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::No { text, code, .. } => {
            assert_eq!(text, "operation failed");
            assert!(code.is_none());
        }
        other => panic!("expected Error::No, got {other:?}"),
    }
}

#[test]
fn require_ok_bad_with_code() {
    use crate::error::Error;
    // RFC 3501 Section 7.1: BAD with a response code.
    let tagged = TaggedResponse {
        tag: "A005".into(),
        status: StatusKind::Bad,
        code: Some(ResponseCode::ClientBug),
        text: "invalid arguments".into(),
    };
    let result = tagged.require_ok();
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::Bad { text, code, .. } => {
            assert_eq!(text, "invalid arguments");
            assert_eq!(code, Some(ResponseCode::ClientBug));
        }
        other => panic!("expected Error::Bad, got {other:?}"),
    }
}

// ===== Capability::from_imap_str / From<String> / From<&str> =====

#[test]
fn capability_from_str_known_variants() {
    // RFC 3501 Section 7.2.1: known capability tokens.
    assert_eq!(Capability::from("IMAP4rev1"), Capability::Imap4Rev1);
    assert_eq!(Capability::from("IMAP4rev2"), Capability::Imap4Rev2);
    assert_eq!(Capability::from("IDLE"), Capability::Idle);
    assert_eq!(Capability::from("STARTTLS"), Capability::StartTls);
    assert_eq!(
        Capability::from("COMPRESS=DEFLATE"),
        Capability::CompressDeflate
    );
    assert_eq!(Capability::from("UIDPLUS"), Capability::UidPlus);
    assert_eq!(Capability::from("CONDSTORE"), Capability::Condstore);
    assert_eq!(Capability::from("QRESYNC"), Capability::QResync);
    assert_eq!(Capability::from("MOVE"), Capability::Move);
    assert_eq!(Capability::from("NAMESPACE"), Capability::Namespace);
    assert_eq!(
        Capability::from("STATUS=DELETED"),
        Capability::StatusDeleted
    );
    assert_eq!(Capability::from("X-GM-EXT-1"), Capability::XGmExt1);
}

#[test]
fn capability_from_string_known_variant() {
    // RFC 3501 Section 7.2.1: via owned String.
    assert_eq!(Capability::from("IDLE".to_owned()), Capability::Idle);
    assert_eq!(
        Capability::from("STARTTLS".to_owned()),
        Capability::StartTls
    );
}

#[test]
fn capability_from_str_case_insensitive() {
    // RFC 3501 Section 7.2.1: capability names are case-insensitive.
    assert_eq!(Capability::from("idle"), Capability::Idle);
    assert_eq!(Capability::from("Idle"), Capability::Idle);
    assert_eq!(Capability::from("starttls"), Capability::StartTls);
    assert_eq!(Capability::from("imap4rev1"), Capability::Imap4Rev1);
}

#[test]
fn capability_from_str_auth() {
    // RFC 3501 Section 7.2.1: AUTH=mechanism.
    assert_eq!(
        Capability::from("AUTH=PLAIN"),
        Capability::Auth("PLAIN".to_owned())
    );
    assert_eq!(
        Capability::from("AUTH=XOAUTH2"),
        Capability::Auth("XOAUTH2".to_owned())
    );
}

#[test]
fn capability_from_str_appendlimit() {
    // RFC 7889: APPENDLIMIT with and without value.
    assert_eq!(
        Capability::from("APPENDLIMIT"),
        Capability::AppendLimit(None)
    );
    assert_eq!(
        Capability::from("APPENDLIMIT=1048576"),
        Capability::AppendLimit(Some(1_048_576))
    );
}

#[test]
fn capability_from_str_thread() {
    // RFC 5256: THREAD=REFERENCES.
    assert_eq!(
        Capability::from("THREAD=REFERENCES"),
        Capability::Thread("REFERENCES".to_owned())
    );
}

#[test]
fn capability_from_str_unknown() {
    // Unknown capabilities preserved verbatim.
    assert_eq!(
        Capability::from("X-CUSTOM-CAP"),
        Capability::Other("X-CUSTOM-CAP".to_owned())
    );
}

#[test]
fn capability_parser_canonicalizes_suffixes() {
    assert!(matches!(
        Capability::from_imap_str("quota=res-storage"),
        Capability::QuotaResource(value) if value == "STORAGE"
    ));
    assert!(matches!(
        Capability::from_imap_str("rights=TEXK"),
        Capability::Rights(value) if value == "texk"
    ));
    assert!(matches!(
        Capability::from_imap_str("auth=plain"),
        Capability::Auth(value) if value == "PLAIN"
    ));
    assert!(matches!(
        Capability::from_imap_str("thread=references"),
        Capability::Thread(value) if value == "REFERENCES"
    ));
}
