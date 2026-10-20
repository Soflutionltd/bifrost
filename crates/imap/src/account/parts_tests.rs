//! Part-traversal and part-handle tests.
//!
//! The BODYSTRUCTURE inputs here are wire bytes fed through the crate's own
//! parser, not hand-built enum values: a traversal validated against
//! structures the test author invented proves only that two of the author's
//! beliefs agree. Every case pins the exact part paths, which is the half a
//! wrong implementation gets subtly wrong while looking right. The one
//! deliberately hand-built structure is the truncation case, which by
//! construction is one the parser refuses to produce.
//!
//! One fixture hazard, since it cost a round: a `\` line continuation in a
//! byte-string literal eats the newline AND the next line's leading
//! whitespace, so a `)` ending one line and a `"SUBTYPE"` starting the next
//! end up adjacent. RFC 3501 `body-type-mpart` is `1*body SP media-subtype`,
//! and with the SP gone the parser reads the subtype as a third body and
//! fails. Every multipart subtype below is therefore introduced by an
//! explicit `\x20` rather than by layout whitespace, which is also immune to
//! an editor stripping a trailing space before the backslash. `parse` now
//! asserts the parser consumed the whole fixture, which is what catches this
//! class of mistake instead of quietly walking a prefix.

use bifrost_types::mime::TransferEncoding;
use bifrost_types::{AccountErrorKind, AccountOperation, RequestErrorKind};

use super::*;
use crate::codec::decode::bodystructure::body_structure;

const OP: AccountOperation = AccountOperation::OpenBlob;

/// Parse a whole BODYSTRUCTURE, insisting the parser consumed ALL of it.
///
/// Ignoring the remainder would let a fixture with a malformed suffix, or one
/// the parser only read a prefix of, pass while pinning the traversal of a
/// structure nobody meant to write.
fn parse(input: &[u8]) -> BodyStructure {
    let (rest, structure) = body_structure(input, false, 0).expect("BODYSTRUCTURE parses");
    assert!(
        rest.is_empty(),
        "fixture left {} unparsed bytes: {:?}",
        rest.len(),
        String::from_utf8_lossy(rest)
    );
    structure
}

/// Walk a wire fixture, asserting the walk was complete. Anything the parser
/// accepted is within the traversal's bound by construction, so a truncated
/// walk here is a bug in the bound, not a property of the fixture.
fn walk(input: &[u8]) -> Vec<MessagePart> {
    let walked = walk_bodystructure(&parse(input));
    assert!(
        !walked.truncated,
        "parser-produced structure walked as truncated"
    );
    walked.parts
}

fn paths(input: &[u8]) -> Vec<String> {
    walk(input)
        .into_iter()
        .map(|part| part.path.section())
        .collect()
}

fn folder() -> MailboxName {
    MailboxName::new("INBOX".to_owned()).expect("valid mailbox")
}

fn canonical(encoding: TransferEncoding) -> PartEncoding {
    PartEncoding::canonical(encoding)
}

#[track_caller]
fn assert_malformed(result: Result<DecodedPartHandle, AccountError>, context: &str) {
    let error = match result {
        Ok(_) => panic!("expected refusal for {context}"),
        Err(error) => error,
    };
    assert!(
        matches!(
            error.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        ),
        "expected Request(Malformed) for {context}, got {:?}",
        error.kind()
    );
}

// ---------------------------------------------------------------- traversal

/// A message whose top-level body is not multipart has ONE part, numbered
/// `1` - not zero parts, and not a part with an empty path.
#[test]
fn single_part_message_is_part_one() {
    let parts = walk(b"(\"TEXT\" \"PLAIN\" (\"CHARSET\" \"utf-8\") NIL NIL \"7BIT\" 100 5)");
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].path.section(), "1");
    assert_eq!(parts[0].media_type, "text");
    assert_eq!(parts[0].media_subtype, "plain");
    assert_eq!(parts[0].encoding.classified, TransferEncoding::SevenBit);
    // Lowercased by the BODYSTRUCTURE parser (RFC 2045 case-insensitivity),
    // which is the only normalization applied to the token anywhere.
    assert_eq!(parts[0].encoding.token, "7bit");
    assert_eq!(parts[0].size, 100);
    assert!(!parts[0].is_embedded_message);
}

/// A `multipart/*` container takes no number of its own; its children are
/// `1` and `2`.
#[test]
fn multipart_alternative_numbers_children_not_container() {
    let parts = walk(
        b"((\"TEXT\" \"PLAIN\" (\"CHARSET\" \"utf-8\") NIL NIL \"QUOTED-PRINTABLE\" 100 5)\
          (\"TEXT\" \"HTML\" (\"CHARSET\" \"utf-8\") NIL NIL \"QUOTED-PRINTABLE\" 200 8)\
          \x20\"ALTERNATIVE\")",
    );
    assert_eq!(
        parts.iter().map(|p| p.path.section()).collect::<Vec<_>>(),
        vec!["1", "2"]
    );
    assert_eq!(parts[1].media_subtype, "html");
    assert_eq!(
        parts[1].encoding.classified,
        TransferEncoding::QuotedPrintable
    );
    assert_eq!(parts[1].size, 200);
}

/// `multipart/mixed` with an attachment: identity comes off the disposition,
/// and the encoding and octet size are the ones a range reader needs.
#[test]
fn multipart_mixed_attachment_carries_identity() {
    let parts = walk(
        b"((\"TEXT\" \"PLAIN\" (\"CHARSET\" \"utf-8\") NIL NIL \"7BIT\" 100 5)\
          (\"APPLICATION\" \"PDF\" (\"NAME\" \"report.pdf\") NIL NIL \"BASE64\" 4096 NIL \
           (\"ATTACHMENT\" (\"FILENAME\" \"report.pdf\")) NIL NIL)\
          \x20\"MIXED\")",
    );
    assert_eq!(
        parts.iter().map(|p| p.path.section()).collect::<Vec<_>>(),
        vec!["1", "2"]
    );
    let pdf = &parts[1];
    assert_eq!(pdf.media_type, "application");
    assert_eq!(pdf.media_subtype, "pdf");
    assert_eq!(pdf.encoding.classified, TransferEncoding::Base64);
    assert_eq!(pdf.size, 4096);
    assert_eq!(pdf.filename.as_deref(), Some("report.pdf"));
    assert_eq!(pdf.disposition.as_deref(), Some("attachment"));
}

/// A nested `multipart/*` at `1` numbers its children `1.1` / `1.2`, and the
/// sibling after it is `2` - the container's own path is a prefix, never a
/// part number.
#[test]
fn nested_multipart_prefixes_children() {
    let parts = walk(
        b"(((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 10 1)\
           (\"TEXT\" \"HTML\" NIL NIL NIL \"7BIT\" 20 1) \"ALTERNATIVE\")\
          (\"IMAGE\" \"PNG\" (\"NAME\" \"logo.png\") \"<logo@example.com>\" NIL \"BASE64\" 512 NIL \
           (\"INLINE\" (\"FILENAME\" \"logo.png\")) NIL \"cid:logo\")\
          \x20\"MIXED\")",
    );
    assert_eq!(
        parts.iter().map(|p| p.path.section()).collect::<Vec<_>>(),
        vec!["1.1", "1.2", "2"]
    );
    let png = &parts[2];
    assert_eq!(png.content_id.as_deref(), Some("<logo@example.com>"));
    assert_eq!(png.content_location.as_deref(), Some("cid:logo"));
    assert_eq!(png.disposition.as_deref(), Some("inline"));
}

/// `message/rfc822` is itself fetchable at its own path AND opens a fresh
/// numbering context: an inner multipart numbers `2.1` / `2.2`.
#[test]
fn embedded_message_with_multipart_body_renumbers_inside() {
    let parts = walk(
        b"((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 10 1)\
          (\"MESSAGE\" \"RFC822\" NIL NIL NIL \"7BIT\" 900 \
           (NIL \"fwd\" NIL NIL NIL NIL NIL NIL NIL NIL) \
           ((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 30 2)\
            (\"TEXT\" \"HTML\" NIL NIL NIL \"7BIT\" 40 2) \"ALTERNATIVE\") 40)\
          \x20\"MIXED\")",
    );
    assert_eq!(
        parts.iter().map(|p| p.path.section()).collect::<Vec<_>>(),
        vec!["1", "2", "2.1", "2.2"]
    );
    assert!(parts[1].is_embedded_message);
    assert_eq!(parts[1].size, 900);
    assert!(!parts[2].is_embedded_message);
}

/// The composition case implementations get wrong: an embedded message whose
/// body is SINGLE-part. The single-part rule applies inside the embedded
/// message too, so the inner body is `2.1`, not `2` and not absent.
#[test]
fn embedded_message_with_single_part_body_is_dot_one() {
    assert_eq!(
        paths(
            b"((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 10 1)\
              (\"MESSAGE\" \"RFC822\" NIL NIL NIL \"7BIT\" 300 \
               (NIL \"fwd\" NIL NIL NIL NIL NIL NIL NIL NIL) \
               (\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 50 3) 20)\
              \x20\"MIXED\")"
        ),
        vec!["1", "2", "2.1"]
    );
}

/// A whole message that IS a `message/rfc822`: the message is part `1` by the
/// single-part rule, and its inner single-part body is `1.1`.
#[test]
fn top_level_embedded_message_numbers_one_and_one_dot_one() {
    assert_eq!(
        paths(
            b"(\"MESSAGE\" \"RFC822\" NIL NIL NIL \"7BIT\" 500 \
              (NIL \"embedded\" NIL NIL NIL NIL NIL NIL NIL NIL) \
              (\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 50 3) 20)"
        ),
        vec!["1", "1.1"]
    );
}

/// `multipart/signed` is numbered like any other multipart: the signature is
/// an ordinary sibling at `2`, which is what makes it fetchable for
/// verification.
#[test]
fn multipart_signed_numbers_signature_as_sibling() {
    let parts = walk(
        b"((\"TEXT\" \"PLAIN\" (\"CHARSET\" \"utf-8\") NIL NIL \"7BIT\" 100 5)\
          (\"APPLICATION\" \"PGP-SIGNATURE\" NIL NIL NIL \"7BIT\" 200)\
          \x20\"SIGNED\" (\"PROTOCOL\" \"application/pgp-signature\" \"MICALG\" \"pgp-sha256\"))",
    );
    assert_eq!(
        parts.iter().map(|p| p.path.section()).collect::<Vec<_>>(),
        vec!["1", "2"]
    );
    assert_eq!(parts[1].media_subtype, "pgp-signature");
}

/// A `Content-Transfer-Encoding` this crate does not model classifies as
/// `Unknown` - never guessed at - but the server's spelling SURVIVES, because
/// for such a part it is the only description of how the octets are encoded
/// that exists anywhere. Losing it here would leave a later layer unable to
/// tell a consumer either how to decode the bytes or what the server said.
#[test]
fn unmodelled_encoding_keeps_the_servers_token() {
    let parts = walk(b"(\"APPLICATION\" \"OCTET-STREAM\" NIL NIL NIL \"X-UUENCODE\" 42)");
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].encoding.classified, TransferEncoding::Unknown);
    assert_eq!(parts[0].encoding.token, "x-uuencode");
    assert!(parts[0].encoding.is_unmodelled());
}

/// A modelled encoding carries its token too. Case is the ONE thing not
/// preserved end to end: the BODYSTRUCTURE parser lowercases the token before
/// this module ever sees it, which RFC 2045 Section 5.1 makes lossless, and
/// this pins that as the deliberate boundary rather than leaving a reader to
/// assume the spelling survives byte-for-byte.
#[test]
fn modelled_encoding_keeps_the_servers_token_lowercased() {
    let parts = walk(b"(\"APPLICATION\" \"PDF\" NIL NIL NIL \"BaSe64\" 10)");
    assert_eq!(parts[0].encoding.classified, TransferEncoding::Base64);
    assert_eq!(parts[0].encoding.token, "base64");
    assert!(!parts[0].encoding.is_unmodelled());
}

/// Servers echo the sender's spelling, including RFC 2047 encoded words in a
/// disposition filename. The BODYSTRUCTURE parser reports them verbatim, so
/// the decode has to happen here.
#[test]
fn filename_decodes_encoded_words_and_falls_back_to_name() {
    let encoded = walk(
        b"(\"APPLICATION\" \"PDF\" NIL NIL NIL \"BASE64\" 10 NIL \
          (\"ATTACHMENT\" (\"FILENAME\" \"=?utf-8?q?na=C3=AFve.pdf?=\")) NIL NIL)",
    );
    assert_eq!(encoded[0].filename.as_deref(), Some("naïve.pdf"));

    // No disposition at all: the content-type `name` parameter is the
    // legacy pre-RFC-2183 spelling and is still the only identity many
    // servers report.
    let fallback =
        walk(b"(\"APPLICATION\" \"PDF\" (\"NAME\" \"legacy.pdf\") NIL NIL \"BASE64\" 10)");
    assert_eq!(fallback[0].filename.as_deref(), Some("legacy.pdf"));
    assert_eq!(fallback[0].disposition, None);
}

// ----------------------------------------------------------------- nesting

/// A chain of `levels` nested `message/rfc822` bodies, innermost body
/// `text/plain`. Parser depth for the innermost node is exactly `levels`.
fn nested_message_chain(levels: usize) -> Vec<u8> {
    let mut wire = String::new();
    for _ in 0..levels {
        wire.push_str(
            "(\"MESSAGE\" \"RFC822\" NIL NIL NIL \"7BIT\" 10 \
             (NIL \"fwd\" NIL NIL NIL NIL NIL NIL NIL NIL) ",
        );
    }
    wire.push_str("(\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 10 1)");
    for _ in 0..levels {
        wire.push_str(" 1)");
    }
    wire.into_bytes()
}

/// The traversal shares the BODYSTRUCTURE parser's nesting bound, so the
/// deepest structure the parser ACCEPTS still walks complete. This is the
/// half of the pairing that would break if the traversal bound were lowered
/// again: a shorter bound would silently drop the deepest parts.
#[test]
fn deepest_accepted_nesting_walks_complete() {
    let walked = walk_bodystructure(&parse(&nested_message_chain(64)));
    assert!(!walked.truncated);
    // 64 embedded messages plus the innermost text body.
    assert_eq!(walked.parts.len(), 65);
    assert_eq!(walked.parts[64].path.section(), "1.".repeat(64) + "1");
}

/// The other half: one level deeper is refused by the PARSER, so the
/// traversal never sees it. That is the guarantee the shared bound buys -
/// the traversal is never the component that gives up first, and a server
/// cannot provoke a truncated walk at all.
#[test]
fn one_level_deeper_is_refused_by_the_parser() {
    assert!(
        body_structure(&nested_message_chain(65), false, 0).is_err(),
        "parser accepted nesting deeper than the traversal bound"
    );
}

/// Exhaustion is REPORTED, not silent. Only a structure that did not come
/// from the parser can reach the bound, so this one is built by hand; the
/// point is that a caller can tell a truncated part list from a complete one
/// instead of publishing a prefix as if it were the whole message.
#[test]
fn overdeep_structure_reports_truncation() {
    fn text() -> BodyStructure {
        BodyStructure::Text {
            media_subtype: "plain".to_owned(),
            params: Vec::new(),
            id: None,
            description: None,
            encoding: "7BIT".to_owned(),
            size: 10,
            lines: 1,
            md5: None,
            disposition: None,
            language: None,
            location: None,
        }
    }

    let mut node = text();
    for _ in 0..(MAX_PART_DEPTH + 2) {
        node = BodyStructure::Multipart {
            media_subtype: "mixed".to_owned(),
            bodies: vec![node],
            params: Vec::new(),
            disposition: None,
            language: None,
            location: None,
        };
    }
    let walked = walk_bodystructure(&node);
    assert!(walked.truncated, "over-deep walk reported as complete");
    assert!(walked.parts.is_empty(), "no leaf is reachable within bound");

    // ... and a structure right at the bound is complete, so the flag tracks
    // the bound rather than being stuck on.
    let mut shallow = text();
    for _ in 0..MAX_PART_DEPTH {
        shallow = BodyStructure::Multipart {
            media_subtype: "mixed".to_owned(),
            bodies: vec![shallow],
            params: Vec::new(),
            disposition: None,
            language: None,
            location: None,
        };
    }
    let walked = walk_bodystructure(&shallow);
    assert!(!walked.truncated);
    assert_eq!(walked.parts.len(), 1);
}

// ------------------------------------------------------------- handle codec

#[test]
fn handle_round_trips() {
    let path = PartPath(vec![2, 1]);
    let encoded = encode_part_handle(
        &folder(),
        7,
        42,
        &path,
        &canonical(TransferEncoding::Base64),
    );
    let decoded = decode_part_handle(&encoded, OP).expect("round trip");
    assert_eq!(decoded.folder.as_str(), "INBOX");
    assert_eq!(decoded.uidvalidity, 7);
    assert_eq!(decoded.uid, 42);
    assert_eq!(decoded.path, path);
    assert_eq!(decoded.encoding.classified, TransferEncoding::Base64);
    assert_eq!(decoded.encoding.token, "base64");
}

/// The whole point of P2 (b): an unmodelled encoding survives the handle
/// round trip with the server's spelling intact, so the byte path stage two
/// builds can still say what the server claimed instead of only `unknown`.
#[test]
fn handle_preserves_an_unmodelled_encoding_token() {
    let part = &walk(b"(\"APPLICATION\" \"OCTET-STREAM\" NIL NIL NIL \"X-UUENCODE\" 42)")[0];
    let encoded = encode_part_handle(&folder(), 7, 42, &part.path, &part.encoding);
    let decoded = decode_part_handle(&encoded, OP).expect("round trip");
    assert_eq!(decoded.encoding.classified, TransferEncoding::Unknown);
    assert_eq!(decoded.encoding.token, "x-uuencode");
    assert_eq!(decoded.encoding, part.encoding);
}

/// The token is length-prefixed for the same reason the folder is: it comes
/// off the wire as a quoted string and can contain the field separator.
#[test]
fn handle_survives_a_colon_in_the_encoding_token() {
    let encoding = PartEncoding {
        classified: TransferEncoding::Unknown,
        token: "x-weird:token".to_owned(),
    };
    let encoded = encode_part_handle(&folder(), 3, 9, &PartPath(vec![1]), &encoding);
    let decoded = decode_part_handle(&encoded, OP).expect("round trip");
    assert_eq!(decoded.encoding, encoding);
    assert_eq!(decoded.uid, 9);
}

/// The folder is length-prefixed, not escaped: a mailbox name containing the
/// field separator must decode back byte-identically rather than being split
/// into fields and read as a different mailbox.
#[test]
fn handle_survives_a_colon_in_the_mailbox_name() {
    let odd = MailboxName::new("Archive:2026:Q1".to_owned()).expect("valid mailbox");
    let encoded = encode_part_handle(
        &odd,
        3,
        9,
        &PartPath(vec![1]),
        &canonical(TransferEncoding::QuotedPrintable),
    );
    let decoded = decode_part_handle(&encoded, OP).expect("round trip");
    assert_eq!(decoded.folder.as_str(), "Archive:2026:Q1");
    assert_eq!(decoded.path.section(), "1");
}

/// An unrecognised version is REFUSED, never reinterpreted under this
/// version's field rules - which would produce a well-formed-looking handle
/// naming the wrong bytes. Each refusal is checked for its CLASSIFICATION,
/// not merely for being an error: these all have to land as
/// `Request(Malformed)`, which is what tells the engine the input is bad
/// rather than the server or the network.
#[test]
fn unrecognised_version_is_refused() {
    let current = encode_part_handle(
        &folder(),
        7,
        42,
        &PartPath(vec![1]),
        &canonical(TransferEncoding::Base64),
    );
    let future = current.replacen("imappart3:", "imappart4:", 1);
    assert_ne!(future, current);
    assert_malformed(decode_part_handle(&future, OP), "future version");
    // A v1 handle is refused rather than upgraded. Nothing ever minted one -
    // this module is unwired - so there is no stored handle to keep working.
    assert_malformed(
        decode_part_handle("imappart1:5:INBOX:7:42:1:base64", OP),
        "v1 handle",
    );
    // ... and the prefix is still recognised as a part handle rather than as
    // unrelated text.
    assert_malformed(decode_part_handle("imappart", OP), "bare prefix");
    assert_malformed(
        decode_part_handle("imapblob1:5:INBOX:7:42:1:base64:6:base64", OP),
        "foreign prefix",
    );
}

#[test]
fn uidvalidity_mismatch_is_reported_not_fetched() {
    let encoded = encode_part_handle(
        &folder(),
        7,
        42,
        &PartPath(vec![1]),
        &canonical(TransferEncoding::Base64),
    );
    let decoded = decode_part_handle(&encoded, OP).expect("round trip");
    assert!(decoded.verify_uidvalidity(7, OP).is_ok());
    let error = decoded
        .verify_uidvalidity(8, OP)
        .expect_err("stale epoch refused");
    assert!(matches!(
        error.kind(),
        AccountErrorKind::Request(RequestErrorKind::Malformed)
    ));
}

#[test]
fn malformed_handles_are_refused() {
    for bad in [
        // zero uidvalidity / uid: this crate never mints an `nz-number` 0.
        "imappart2:5:INBOX:0:42:1:base64:6:base64",
        "imappart2:5:INBOX:7:0:1:base64:6:base64",
        // zero component in the part path.
        "imappart2:5:INBOX:7:42:1.0:base64:6:base64",
        // empty path.
        "imappart2:5:INBOX:7:42::base64:6:base64",
        // folder length that runs past the string.
        "imappart2:99:INBOX:7:42:1:base64:6:base64",
        // length not followed by a separator at the declared offset.
        "imappart2:5:INBOXX:7:42:1:base64:6:base64",
        // an encoding token the codec does not define. `from_token` would
        // silently fold this into `Unknown`; the handle decoder must not.
        "imappart2:5:INBOX:7:42:1:b64:3:b64",
        // classification and original token disagree: a corrupted or edited
        // handle, since minting derives one from the other.
        "imappart2:5:INBOX:7:42:1:base64:3:b64",
        // token length that runs past the string.
        "imappart2:5:INBOX:7:42:1:base64:9:base64",
        // token length short of the field, leaving a stray tail.
        "imappart2:5:INBOX:7:42:1:base64:3:base64",
        // missing the token field entirely (a v1-shaped tail under v2).
        "imappart2:5:INBOX:7:42:1:base64",
        // trailing field: a v2 handle has exactly these fields.
        "imappart2:5:INBOX:7:42:1:base64:6:base64:extra",
        // non-numeric version.
        "imappartx:5:INBOX:7:42:1:base64:6:base64",
        // missing fields.
        "imappart2:5:INBOX:7:42",
    ] {
        assert_malformed(decode_part_handle(bad, OP), bad);
    }
}

/// Every encoding this crate models today survives the token round trip.
///
/// Note what this does NOT do: `TransferEncoding` is `#[non_exhaustive]`, so
/// `encoding_token` needs a wildcard arm and a variant added upstream will
/// fold silently into `unknown` rather than failing to compile or failing
/// here. No test in this crate can force that decision. The list below is
/// therefore a pin on today's vocabulary, and the reason a missed variant is
/// survivable is the original token carried beside the classification.
#[test]
fn every_modelled_encoding_token_round_trips() {
    for encoding in [
        TransferEncoding::SevenBit,
        TransferEncoding::EightBit,
        TransferEncoding::Binary,
        TransferEncoding::Base64,
        TransferEncoding::QuotedPrintable,
        TransferEncoding::Unknown,
    ] {
        let token = encoding_token(encoding);
        assert_eq!(encoding_from_token(token), Some(encoding), "token {token}");
    }
}
