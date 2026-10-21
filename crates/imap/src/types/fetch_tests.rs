#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;

// --- FetchResponse tests ---

#[test]
fn fetch_response_default() {
    let resp = FetchResponse::default();
    assert_eq!(resp.seq, 0);
    assert!(resp.uid.is_none());
    assert!(resp.flags.is_none());
    assert!(resp.envelope.is_none());
    assert!(resp.body_structure.is_none());
    assert!(resp.rfc822_size.is_none());
    assert!(resp.internal_date.is_none());
    assert!(resp.body_sections.is_empty());
    assert!(resp.mod_seq.is_none());
    assert!(resp.save_date.is_none());
    assert!(resp.binary_sections.is_empty());
    assert!(resp.binary_sizes.is_empty());
    assert!(resp.preview.is_none());
    assert!(resp.email_id.is_none());
    assert!(resp.thread_id.is_none());
    assert!(resp.gmail_msg_id.is_none());
    assert!(resp.gmail_thread_id.is_none());
    assert!(resp.gmail_labels.is_none());
}

#[test]
fn fetch_response_with_seq_and_uid() {
    let resp = FetchResponse {
        seq: 5,
        uid: Some(1234),
        ..Default::default()
    };
    assert_eq!(resp.seq, 5);
    assert_eq!(resp.uid, Some(1234));
}

#[test]
fn fetch_response_with_flags() {
    let resp = FetchResponse {
        seq: 1,
        flags: Some(vec![Flag::Seen, Flag::Flagged]),
        ..Default::default()
    };
    let flags = resp.flags.unwrap();
    assert_eq!(flags.len(), 2);
    assert_eq!(flags[0], Flag::Seen);
    assert_eq!(flags[1], Flag::Flagged);
}

#[test]
fn fetch_response_with_envelope() {
    let env = Envelope {
        subject: Some("Test subject".into()),
        ..Default::default()
    };
    let resp = FetchResponse {
        seq: 1,
        envelope: Some(env),
        ..Default::default()
    };
    assert_eq!(
        resp.envelope.unwrap().subject.as_deref(),
        Some("Test subject")
    );
}

#[test]
fn fetch_response_with_rfc822_size() {
    let resp = FetchResponse {
        seq: 1,
        rfc822_size: Some(4096),
        ..Default::default()
    };
    assert_eq!(resp.rfc822_size, Some(4096));
}

#[test]
fn fetch_response_with_internal_date() {
    let resp = FetchResponse {
        seq: 1,
        internal_date: Some("17-Jul-1996 02:44:25 -0700".into()),
        ..Default::default()
    };
    assert_eq!(
        resp.internal_date.as_deref(),
        Some("17-Jul-1996 02:44:25 -0700")
    );
}

#[test]
fn fetch_response_with_mod_seq() {
    let resp = FetchResponse {
        seq: 1,
        mod_seq: Some(99999),
        ..Default::default()
    };
    assert_eq!(resp.mod_seq, Some(99999));
}

#[test]
fn fetch_response_with_save_date() {
    let resp = FetchResponse {
        seq: 1,
        save_date: Some("01-Jan-2024 00:00:00 +0000".into()),
        ..Default::default()
    };
    assert_eq!(
        resp.save_date.as_deref(),
        Some("01-Jan-2024 00:00:00 +0000")
    );
}

// --- BodySection tests ---

#[test]
fn body_section_full() {
    let section = BodySection {
        section: "HEADER".into(),
        origin: None,
        data: Some(b"Subject: Hi\r\n".to_vec().into()),
    };
    assert_eq!(section.section, "HEADER");
    assert!(section.origin.is_none());
    assert_eq!(section.data.as_deref(), Some(b"Subject: Hi\r\n".as_slice()));
}

#[test]
fn body_section_with_origin() {
    let section = BodySection {
        section: "1".into(),
        origin: Some(100),
        data: Some(b"partial data".to_vec().into()),
    };
    assert_eq!(section.origin, Some(100));
}

#[test]
fn body_section_nil_data() {
    let section = BodySection {
        section: "TEXT".into(),
        origin: None,
        data: None,
    };
    assert!(section.data.is_none());
}

#[test]
fn body_section_empty_section_string() {
    let section = BodySection {
        section: String::new(),
        origin: None,
        data: Some(b"full message".to_vec().into()),
    };
    assert!(section.section.is_empty());
}

// --- BinarySection tests ---

#[test]
fn binary_section_basic() {
    let section = BinarySection {
        section: vec![1, 2, 3],
        origin: None,
        data: Some(vec![0xFF, 0xD8, 0xFF].into()),
    };
    assert_eq!(section.section, [1, 2, 3]);
    assert!(section.origin.is_none());
    assert_eq!(section.data.as_deref(), Some([0xFF, 0xD8, 0xFF].as_slice()));
}

#[test]
fn binary_section_with_origin() {
    let section = BinarySection {
        section: vec![1],
        origin: Some(512),
        data: Some(vec![0x00].into()),
    };
    assert_eq!(section.origin, Some(512));
}

#[test]
fn binary_section_nil_data() {
    let section = BinarySection {
        section: vec![],
        origin: None,
        data: None,
    };
    assert!(section.data.is_none());
}

// --- FetchResponse with body sections ---

#[test]
fn fetch_response_multiple_body_sections() {
    let resp = FetchResponse {
        seq: 3,
        body_sections: vec![
            BodySection {
                section: "HEADER".into(),
                origin: None,
                data: Some(b"From: alice@example.com\r\n".to_vec().into()),
            },
            BodySection {
                section: "TEXT".into(),
                origin: None,
                data: Some(b"Hello world".to_vec().into()),
            },
        ],
        ..Default::default()
    };
    assert_eq!(resp.body_sections.len(), 2);
    assert_eq!(resp.body_sections[0].section, "HEADER");
    assert_eq!(resp.body_sections[1].section, "TEXT");
}

#[test]
fn fetch_response_binary_sizes() {
    let resp = FetchResponse {
        seq: 1,
        binary_sizes: vec![(vec![1], 1024), (vec![2, 1], 512)],
        ..Default::default()
    };
    assert_eq!(resp.binary_sizes.len(), 2);
    assert_eq!(resp.binary_sizes[0], (vec![1], 1024));
    assert_eq!(resp.binary_sizes[1], (vec![2, 1], 512));
}

// --- FetchAttr tests ---

#[test]
fn fetch_attr_simple_variants() {
    assert_eq!(FetchAttr::Uid, FetchAttr::Uid);
    assert_eq!(FetchAttr::Flags, FetchAttr::Flags);
    assert_eq!(FetchAttr::Envelope, FetchAttr::Envelope);
    assert_eq!(FetchAttr::BodyStructure, FetchAttr::BodyStructure);
    assert_eq!(FetchAttr::Rfc822Size, FetchAttr::Rfc822Size);
    assert_eq!(FetchAttr::InternalDate, FetchAttr::InternalDate);
    assert_eq!(FetchAttr::Rfc822, FetchAttr::Rfc822);
    assert_eq!(FetchAttr::Rfc822Header, FetchAttr::Rfc822Header);
    assert_eq!(FetchAttr::Rfc822Text, FetchAttr::Rfc822Text);
    assert_eq!(FetchAttr::ModSeq, FetchAttr::ModSeq);
    assert_eq!(FetchAttr::SaveDate, FetchAttr::SaveDate);
    assert_eq!(FetchAttr::Preview, FetchAttr::Preview);
    assert_eq!(FetchAttr::PreviewLazy, FetchAttr::PreviewLazy);
    assert_eq!(FetchAttr::EmailId, FetchAttr::EmailId);
    assert_eq!(FetchAttr::ThreadId, FetchAttr::ThreadId);
}

#[test]
fn fetch_response_with_preview() {
    // RFC 8970 Section 3: PREVIEW data item in FETCH response.
    let resp = FetchResponse {
        seq: 1,
        preview: Some("Meeting tomorrow at 3pm to discuss...".into()),
        ..Default::default()
    };
    assert_eq!(
        resp.preview.as_deref(),
        Some("Meeting tomorrow at 3pm to discuss...")
    );
}

#[test]
fn fetch_response_with_preview_nil() {
    // RFC 8970 Section 3: PREVIEW can be NIL (e.g., LAZY mode).
    let resp = FetchResponse {
        seq: 1,
        preview: None,
        ..Default::default()
    };
    assert!(resp.preview.is_none());
}

#[test]
fn fetch_response_with_email_id() {
    let resp = FetchResponse {
        seq: 1,
        email_id: Some("M6d99ac3275826486".into()),
        ..Default::default()
    };
    assert_eq!(resp.email_id.as_deref(), Some("M6d99ac3275826486"));
}

#[test]
fn fetch_response_with_thread_id() {
    let resp = FetchResponse {
        seq: 1,
        thread_id: Some("T64b478a75b7ea9fd".into()),
        ..Default::default()
    };
    assert_eq!(resp.thread_id.as_deref(), Some("T64b478a75b7ea9fd"));
}

#[test]
fn fetch_response_with_gmail_ids() {
    let resp = FetchResponse {
        seq: 1,
        gmail_msg_id: Some(1_278_455_344_230_334_864),
        gmail_thread_id: Some(1_278_455_344_230_334_865),
        gmail_labels: Some(vec!["\\Inbox".to_owned(), "Work".to_owned()]),
        ..Default::default()
    };
    assert_eq!(resp.gmail_msg_id, Some(1_278_455_344_230_334_864));
    assert_eq!(resp.gmail_thread_id, Some(1_278_455_344_230_334_865));
    assert_eq!(
        resp.gmail_labels,
        Some(vec!["\\Inbox".to_owned(), "Work".to_owned()])
    );
}

#[test]
fn fetch_attr_body_section_peek() {
    let attr = FetchAttr::BodySection {
        peek: true,
        section: Some("HEADER".into()),
        partial: None,
    };
    match &attr {
        FetchAttr::BodySection {
            peek,
            section,
            partial,
        } => {
            assert!(*peek);
            assert_eq!(section.as_deref(), Some("HEADER"));
            assert!(partial.is_none());
        }
        _ => panic!("expected BodySection"),
    }
}

#[test]
fn fetch_attr_body_section_with_partial() {
    let attr = FetchAttr::BodySection {
        peek: false,
        section: Some("1".into()),
        partial: Some((0, 1024)),
    };
    match &attr {
        FetchAttr::BodySection { partial, .. } => {
            assert_eq!(*partial, Some((0, 1024)));
        }
        _ => panic!("expected BodySection"),
    }
}

#[test]
fn fetch_attr_body_section_no_section() {
    let attr = FetchAttr::BodySection {
        peek: false,
        section: None,
        partial: None,
    };
    match &attr {
        FetchAttr::BodySection { section, .. } => {
            assert!(section.is_none());
        }
        _ => panic!("expected BodySection"),
    }
}

#[test]
fn fetch_attr_binary() {
    let attr = FetchAttr::Binary {
        peek: true,
        section: vec![1, 2],
        partial: Some((0, 512)),
    };
    match &attr {
        FetchAttr::Binary {
            peek,
            section,
            partial,
        } => {
            assert!(*peek);
            assert_eq!(section, &[1, 2]);
            assert_eq!(*partial, Some((0, 512)));
        }
        _ => panic!("expected Binary"),
    }
}

#[test]
fn fetch_attr_binary_size() {
    let attr = FetchAttr::BinarySize {
        section: vec![3, 1],
    };
    match &attr {
        FetchAttr::BinarySize { section } => {
            assert_eq!(section, &[3, 1]);
        }
        _ => panic!("expected BinarySize"),
    }
}

// --- StoreOperation tests ---

#[test]
fn store_operation_variants() {
    assert_eq!(StoreOperation::Add, StoreOperation::Add);
    assert_eq!(StoreOperation::Remove, StoreOperation::Remove);
    assert_eq!(StoreOperation::Replace, StoreOperation::Replace);
    assert_ne!(StoreOperation::Add, StoreOperation::Remove);
    assert_ne!(StoreOperation::Remove, StoreOperation::Replace);
}

#[test]
fn store_operation_copy() {
    let op = StoreOperation::Add;
    let op2 = op; // Copy
    assert_eq!(op, op2);
}

// --- AppendMessage tests ---

#[test]
fn append_message_basic() {
    let msg = AppendMessage {
        flags: vec![Flag::Seen],
        date: Some("01-Jan-2024 00:00:00 +0000".into()),
        data: bytes::Bytes::from_static(b"From: test@example.com\r\nSubject: Hi\r\n\r\nBody"),
    };
    assert_eq!(msg.flags.len(), 1);
    assert_eq!(msg.flags[0], Flag::Seen);
    assert!(msg.date.is_some());
    assert!(!msg.data.is_empty());
}

#[test]
fn append_message_no_flags_no_date() {
    let msg = AppendMessage {
        flags: vec![],
        date: None,
        data: bytes::Bytes::from_static(b"minimal message"),
    };
    assert!(msg.flags.is_empty());
    assert!(msg.date.is_none());
}

/// `AppendMessage::new` must take an owned buffer over without copying it:
/// the whole point of the `Bytes` field is that the caller's allocation is the
/// one that reaches the socket.
#[test]
fn append_message_new_does_not_copy_an_owned_buffer() {
    let body = b"From: a@example.com\r\n\r\nbody".to_vec();
    let ptr = body.as_ptr();
    let msg = AppendMessage::new(body);
    assert_eq!(
        msg.data.as_ptr(),
        ptr,
        "Vec<u8> must be adopted, not copied"
    );
    assert!(msg.flags.is_empty());
    assert!(msg.date.is_none());

    let shared = bytes::Bytes::from_static(b"shared");
    let msg = AppendMessage::new(shared.clone());
    assert_eq!(msg.data.as_ptr(), shared.as_ptr());
}

// --- Clone / Eq ---

#[test]
fn fetch_response_clone_eq() {
    let resp = FetchResponse {
        seq: 10,
        uid: Some(500),
        flags: Some(vec![Flag::Draft]),
        ..Default::default()
    };
    let cloned = resp.clone();
    assert_eq!(resp, cloned);
}

#[test]
fn body_section_clone_eq() {
    let section = BodySection {
        section: "1.2".into(),
        origin: Some(0),
        data: Some(b"data".to_vec().into()),
    };
    let cloned = section.clone();
    assert_eq!(section, cloned);
}

// --- StoreResult field access tests ---

#[test]
fn store_result_provides_access_to_fetches() {
    // RFC 3501 Section 6.4.6: STORE returns implicit FETCH responses.
    let result = StoreResult {
        fetches: vec![
            FetchResponse {
                seq: 1,
                uid: Some(100),
                flags: Some(vec![Flag::Seen]),
                ..Default::default()
            },
            FetchResponse {
                seq: 2,
                uid: Some(101),
                flags: Some(vec![Flag::Flagged]),
                ..Default::default()
            },
        ],
        status: crate::types::response::StatusKind::Ok,
        code: None,
    };
    // Access via .fetches field.
    assert_eq!(result.fetches.len(), 2);
    assert_eq!(result.fetches[0].seq, 1);
    assert_eq!(result.fetches[1].uid, Some(101));
    // Iterate via .fetches.
    let seqs: Vec<u32> = result.fetches.iter().map(|f| f.seq).collect();
    assert_eq!(seqs, vec![1, 2]);
}

#[test]
fn store_result_empty() {
    // RFC 3501 Section 6.4.6: STORE with .SILENT returns no FETCH responses.
    let result = StoreResult {
        fetches: vec![],
        status: crate::types::response::StatusKind::Ok,
        code: None,
    };
    assert!(result.fetches.is_empty());
}

#[test]
fn fetch_response_debug() {
    let resp = FetchResponse {
        seq: 1,
        uid: Some(42),
        ..Default::default()
    };
    let debug = format!("{resp:?}");
    assert!(debug.contains("seq: 1"));
    assert!(debug.contains("uid: Some(42)"));
}

// --- FetchAttr::to_imap_string tests ---
// RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5

#[test]
fn fetch_attr_simple_items() {
    assert_eq!(FetchAttr::Uid.to_imap_string(), "UID");
    assert_eq!(FetchAttr::Flags.to_imap_string(), "FLAGS");
    assert_eq!(FetchAttr::Envelope.to_imap_string(), "ENVELOPE");
    assert_eq!(FetchAttr::BodyStructure.to_imap_string(), "BODYSTRUCTURE");
    assert_eq!(FetchAttr::Rfc822Size.to_imap_string(), "RFC822.SIZE");
    assert_eq!(FetchAttr::InternalDate.to_imap_string(), "INTERNALDATE");
    assert_eq!(FetchAttr::Rfc822.to_imap_string(), "RFC822");
    assert_eq!(FetchAttr::Rfc822Header.to_imap_string(), "RFC822.HEADER");
    assert_eq!(FetchAttr::Rfc822Text.to_imap_string(), "RFC822.TEXT");
    assert_eq!(FetchAttr::ModSeq.to_imap_string(), "MODSEQ");
    assert_eq!(FetchAttr::SaveDate.to_imap_string(), "SAVEDATE");
    assert_eq!(FetchAttr::Preview.to_imap_string(), "PREVIEW");
    assert_eq!(FetchAttr::PreviewLazy.to_imap_string(), "PREVIEW (LAZY)");
    assert_eq!(FetchAttr::EmailId.to_imap_string(), "EMAILID");
    assert_eq!(FetchAttr::ThreadId.to_imap_string(), "THREADID");
    assert_eq!(FetchAttr::GmailMsgId.to_imap_string(), "X-GM-MSGID");
    assert_eq!(FetchAttr::GmailThreadId.to_imap_string(), "X-GM-THRID");
    assert_eq!(FetchAttr::GmailLabels.to_imap_string(), "X-GM-LABELS");
}

#[test]
fn fetch_attr_body_section_no_section_no_partial() {
    let attr = FetchAttr::BodySection {
        peek: false,
        section: None,
        partial: None,
    };
    assert_eq!(attr.to_imap_string(), "BODY[]");
}

#[test]
fn fetch_attr_body_section_peek_with_section() {
    let attr = FetchAttr::BodySection {
        peek: true,
        section: Some("HEADER".into()),
        partial: None,
    };
    assert_eq!(attr.to_imap_string(), "BODY.PEEK[HEADER]");
}

#[test]
fn fetch_attr_body_section_wire_format_with_partial() {
    // RFC 3501 Section 6.4.5: partial range <offset.count>.
    let attr = FetchAttr::BodySection {
        peek: false,
        section: Some("TEXT".into()),
        partial: Some((0, 1024)),
    };
    assert_eq!(attr.to_imap_string(), "BODY[TEXT]<0.1024>");
}

#[test]
fn fetch_attr_binary_section() {
    // RFC 3516 Section 4.5.1: BINARY[section].
    let attr = FetchAttr::Binary {
        peek: false,
        section: vec![1, 2],
        partial: None,
    };
    assert_eq!(attr.to_imap_string(), "BINARY[1.2]");
}

#[test]
fn fetch_attr_binary_peek_with_partial() {
    // RFC 3516 Section 4.5.1: BINARY.PEEK[section]<partial>.
    let attr = FetchAttr::Binary {
        peek: true,
        section: vec![3],
        partial: Some((100, 200)),
    };
    assert_eq!(attr.to_imap_string(), "BINARY.PEEK[3]<100.200>");
}

#[test]
fn fetch_attr_binary_size_wire_format() {
    // RFC 3516 Section 4.5.2: BINARY.SIZE[section].
    let attr = FetchAttr::BinarySize {
        section: vec![1, 2, 3],
    };
    assert_eq!(attr.to_imap_string(), "BINARY.SIZE[1.2.3]");
}

// --- format_fetch_attrs tests ---
// RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5

#[test]
fn format_single_attr_no_parens() {
    // A single attribute is serialized without parentheses.
    assert_eq!(format_fetch_attrs(&[FetchAttr::Flags]), "FLAGS");
}

#[test]
fn format_multiple_attrs_with_parens() {
    // Multiple attributes are joined with spaces inside parentheses.
    let result = format_fetch_attrs(&[FetchAttr::Uid, FetchAttr::Flags, FetchAttr::Envelope]);
    assert_eq!(result, "(UID FLAGS ENVELOPE)");
}
