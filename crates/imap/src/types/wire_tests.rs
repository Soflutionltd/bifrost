#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::types::validated::MailboxName;

/// RFC 3501 Section 5.1.3 example: plain ASCII mailbox.
#[test]
fn ascii_passthrough() {
    assert_eq!(encode_utf7("INBOX"), "INBOX");
    assert_eq!(decode_utf7(b"INBOX"), "INBOX");
}

/// RFC 3501 Section 5.1.3: `&` encodes as `&-`.
#[test]
fn ampersand_encoding() {
    assert_eq!(encode_utf7("A&B"), "A&-B");
    assert_eq!(decode_utf7(b"A&-B"), "A&B");
}

/// Round-trip for non-ASCII mailbox names.
#[test]
fn non_ascii_roundtrip() {
    let names = [
        "Langstrumpf",            // ASCII only
        "Strstrumpf",             // ASCII only
        "Français",               // Latin-1 supplement
        "日本語",                 // CJK
        "Strstrumpf/Langstrumpf", // With slash
        "Папка",                  // Cyrillic
        "مجلد",                   // Arabic
        "フォルダ",               // Katakana
    ];

    for name in &names {
        let encoded = encode_utf7(name);
        let decoded = decode_utf7(encoded.as_bytes());
        assert_eq!(
            &decoded, name,
            "round-trip failed for {name:?}: encoded={encoded:?}"
        );
    }
}

/// Verify specific known encodings.
#[test]
fn known_encodings() {
    // "Langstrumpf" is pure ASCII
    assert_eq!(encode_utf7("Langstrumpf"), "Langstrumpf");

    // "Français"  -  the `ç` (U+00E7) is non-ASCII
    let encoded = encode_utf7("Français");
    assert!(encoded.starts_with("Fran"));
    assert!(encoded.contains('&'));
    assert_eq!(decode_utf7(encoded.as_bytes()), "Français");
}

/// Mixed ASCII and non-ASCII in same name.
#[test]
fn mixed_ascii_non_ascii() {
    let name = "INBOX/日本語/test";
    let encoded = encode_utf7(name);
    let decoded = decode_utf7(encoded.as_bytes());
    assert_eq!(decoded, name);
}

/// Supplementary plane characters (surrogate pairs in UTF-16).
#[test]
fn supplementary_plane() {
    let name = "Emoji\u{1F4E7}Folder"; // U+1F4E7 E-MAIL SYMBOL
    let encoded = encode_utf7(name);
    let decoded = decode_utf7(encoded.as_bytes());
    assert_eq!(decoded, name);
}

/// Empty string.
#[test]
fn empty_string() {
    assert_eq!(encode_utf7(""), "");
    assert_eq!(decode_utf7(b""), "");
}

/// Multiple `&` characters.
#[test]
fn multiple_ampersands() {
    assert_eq!(encode_utf7("A&B&C"), "A&-B&-C");
    assert_eq!(decode_utf7(b"A&-B&-C"), "A&B&C");
}

/// Malformed modified Base64 is handled gracefully.
#[test]
fn malformed_base64_graceful() {
    // Unterminated shift sequence  -  no closing `-`
    let result = decode_utf7(b"&INVALID");
    // Should not panic; produces some output.
    assert!(!result.is_empty());
}

// L8: Decoder passes through non-ASCII bytes outside Base64 segments.
// RFC 3501 Section 5.1.3 says bytes 0x80-0xFF should be Base64-encoded,
// but as a client library we accept them gracefully (Postel's law).
#[test]
fn non_ascii_passthrough_outside_base64() {
    // 0xC3 0xA9 is UTF-8 for 'é'  -  not valid in modified UTF-7 but
    // should be accepted gracefully by the decoder.
    let result = decode_utf7(&[b'A', 0xC3, 0xA9, b'B']);
    assert!(result.contains('A'));
    assert!(result.contains('B'));
    // Should not panic or error
}

// An unterminated shifted segment is invalid MUTF-7. Preserve its raw bytes
// rather than decoding a prefix and changing the mailbox identity.
#[test]
fn unterminated_base64_segment() {
    // &- is the encoding of '&'. &AE4- is '日'. Without trailing '-':
    let result = decode_utf7(b"test&AE4");
    assert_eq!(result, "test&AE4");
}

/// RFC 3501 Section5.1.3 / Postel's law: non-conformant servers send raw UTF-8
/// bytes outside Base64 segments. The decoder should interpret consecutive
/// high bytes as UTF-8 rather than treating each byte as Latin-1.
#[test]
fn spec_audit_raw_utf8_outside_base64() {
    // 0xC3 0xA9 is UTF-8 for 'é' (U+00E9).
    // A non-conformant server might send: INBOX/café
    let input = b"INBOX/caf\xc3\xa9";
    let result = decode_utf7(input);
    assert_eq!(
        result, "INBOX/café",
        "raw UTF-8 bytes should be decoded as UTF-8, not Latin-1"
    );
}

#[test]
fn spec_audit_raw_utf8_cjk_outside_base64() {
    // 0xE6 0x97 0xA5 0xE6 0x9C 0xAC 0xE8 0xAA 0x9E is UTF-8 for '日本語'.
    let input = b"\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e";
    let result = decode_utf7(input);
    assert_eq!(
        result, "日本語",
        "raw UTF-8 CJK should be decoded correctly"
    );
}

/// RFC 3501 Section 5.1.3: "Modified BASE64 MUST NOT be used to represent
/// any printing US-ASCII character which can represent itself."
/// This is a requirement on the *encoder*. Per Postel's law the decoder
/// must accept `&AEE-` (U+0041 'A') and produce the correct character.
#[test]
fn accepts_base64_encoded_printable_ascii_per_postels_law() {
    // &AEE- encodes U+0041 ('A') which is printable ASCII.
    let result = decode_utf7(b"&AEE-");
    assert_eq!(
        result, "A",
        "Decoder must accept Base64-encoded printable ASCII per Postel's law"
    );
}

/// RFC 3501 Section 5.1.3: Modified UTF-7 uses UTF-16BE encoding, which
/// requires byte pairs. A Base64 segment that decodes to an odd number of
/// bytes has an orphaned trailing byte  -  this must emit U+FFFD (replacement
/// character), not silently drop the corrupt byte.
#[test]
fn spec_audit_utf16be_trailing_odd_byte_produces_replacement() {
    // Test decode_utf16be directly: [0x4E, 0x16] = U+4E16 ('世'),
    // 0xFF is the orphan byte that cannot form a code unit.
    let mut out = String::new();
    decode_utf16be(&[0x4E, 0x16, 0xFF], &mut out);
    assert_eq!(
        out, "世\u{FFFD}",
        "orphan trailing byte must produce U+FFFD, not be silently dropped"
    );

    // Test via decode_utf7: "&Thb,-" -> Base64 "Thb," decodes to
    // [0x4E, 0x16, 0xFF] (one valid code unit + one orphan byte).
    let result = decode_utf7(b"&Thb,-");
    assert!(
        result.contains('\u{FFFD}'),
        "decode_utf7 must emit U+FFFD for orphan byte in Base64 segment"
    );
    assert!(
        result.contains('世'),
        "valid code unit before orphan byte must still decode"
    );
}

/// RFC 3501 Section 5.1.3: "Modified BASE64 MUST NOT be used to represent
/// ANY printing US-ASCII character which can represent itself."
/// This is a requirement on the *encoder*. Per Postel's law the decoder
/// must accept a Base64 segment containing a mix of printable ASCII and
/// non-ASCII characters and produce the correct decoded text.
#[test]
fn regression_mixed_ascii_nonascii_base64_segment() {
    // &AEFl5Q- encodes UTF-16BE [0x00, 0x41, 0x65, 0xE5] = 'A' + '日'
    // in a single Base64 segment. 'A' is printable ASCII and MUST NOT be
    // Base64-encoded per RFC 3501 Section 5.1.3, but the decoder accepts
    // it per Postel's law.
    let result = decode_utf7(b"&AEFl5Q-");
    assert_eq!(
        result, "A\u{65E5}",
        "Decoder must accept Base64-encoded printable ASCII mixed with \
             non-ASCII per Postel's law (RFC 3501 Section 5.1.3)"
    );
}

/// RFC 3501 Section 5.1.3 / Postel's law: a non-conformant server
/// encodes both printable ASCII and non-ASCII characters in a single
/// Base64 segment. The MUST NOT rule applies to encoders, not decoders.
/// The decoder must accept and correctly decode such input.
#[test]
fn decode_utf7_mixed_ascii_nonascii_accepts_per_postels_law() {
    // &AEFl5Q- encodes UTF-16BE [0x00, 0x41, 0x65, 0xE5] = 'A' (U+0041) + '日' (U+65E5)
    let result = decode_utf7(b"&AEFl5Q-");
    assert_eq!(
        result, "A\u{65E5}",
        "Decoder must accept Base64 with mixed ASCII/non-ASCII per Postel's law"
    );
}

/// RFC 3501 Section 5.1.3 / Postel's law: a non-conformant server
/// encodes a single printable ASCII character ('A' = U+0041) in Base64.
/// The decoder must accept this and produce the correct character.
#[test]
fn decode_utf7_pure_ascii_in_base64_accepts_per_postels_law() {
    // &AEE- encodes U+0041 ('A') in Base64.
    let result = decode_utf7(b"&AEE-");
    assert_eq!(
        result, "A",
        "Decoder must accept Base64-encoded printable ASCII per Postel's law"
    );
}

/// RFC 3501 Section 5.1.3 / Postel's law: a non-conformant server
/// encodes '&' (U+0026) in Base64 as `&ACY-` instead of the required `&-`.
/// The decoder must accept this and produce the correct '&' character.
#[test]
fn decode_utf7_base64_encoded_ampersand_accepts_per_postels_law() {
    // &ACY- encodes U+0026 ('&') in Base64.
    let result = decode_utf7(b"&ACY-");
    assert_eq!(
        result, "&",
        "Decoder must accept Base64-encoded '&' per Postel's law"
    );
}

/// RFC 3501 Section 5.1.3  -  the modified Base64 engine must
/// tolerate non-zero trailing bits in the final Base64 character. When a
/// non-conformant encoder sets unused trailing bits, the decoder should
/// still produce the correct UTF-16BE output.
#[test]
fn regression_nonzero_trailing_bits_accepted() {
    // U+00E9 ('é') = UTF-16BE [0x00, 0xE9] = 16 bits.
    // Base64 groups of 6: 000000 001110 1001xx (18 bits, 2 trailing).
    // Standard encoding (trailing=00): 'A','O','k' -> &AOk-
    // Non-zero trailing (trailing=11): 'A','O','n' -> &AOn-
    // The decoder must accept &AOn- and produce 'é'.
    assert_eq!(
        decode_utf7(b"&AOn-"),
        "\u{00E9}",
        "Non-zero trailing bits in Base64 must be accepted (RFC 3501 Section 5.1.3)"
    );
}

/// RFC 3501 Section 5.1.3  -  the modified Base64 alphabet omits
/// padding ('='), but the decoder should tolerate padding from
/// non-conformant servers per Postel's law.
#[test]
fn regression_base64_padding_tolerated() {
    // &AOk=- encodes U+00E9 ('é') with padding. The decoder must accept it.
    assert_eq!(
        decode_utf7(b"&AOk=-"),
        "\u{00E9}",
        "Base64 padding must be tolerated (RFC 3501 Section 5.1.3, Postel's law)"
    );
}

/// RFC 3501 Section 5.1.3  -  `&` MUST be represented as `&-`,
/// NOT Base64-encoded. This is a requirement on the *encoder*.
/// Per Postel's law the decoder must accept `&ACY-` (Base64 encoding
/// of U+0026 '&') and produce the correct '&' character.
#[test]
fn accepts_base64_encoded_ampersand_per_postels_law() {
    // &ACY- encodes U+0026 ('&') in Base64. Per RFC 3501 Section 5.1.3,
    // '&' MUST be represented as '&-', not Base64-encoded  -  but the
    // decoder accepts it per Postel's law.
    let result = decode_utf7(b"&ACY-");
    assert_eq!(
        result, "&",
        "Decoder must accept Base64-encoded '&' per Postel's law (RFC 3501 Section 5.1.3)"
    );
}

/// control characters (0x00-0x1F, 0x7F) in the input are not
/// printable US-ASCII per RFC 3501 Section 5.1.3 and must be replaced
/// with U+FFFD, not silently passed through.
#[test]
fn regression_control_chars_replaced() {
    // NUL (0x00), BEL (0x07), DEL (0x7F) in input stream
    let input = b"\x00hello\x07world\x7F";
    let result = decode_utf7(input);
    assert!(
        !result.contains('\0'),
        "NUL (0x00) must not pass through verbatim (RFC 3501 Section 5.1.3)"
    );
    assert!(
        !result.contains('\x07'),
        "BEL (0x07) must not pass through verbatim (RFC 3501 Section 5.1.3)"
    );
    assert!(
        !result.contains('\x7F'),
        "DEL (0x7F) must not pass through verbatim (RFC 3501 Section 5.1.3)"
    );
    // The printable ASCII parts should still be decoded.
    assert!(
        result.contains("hello"),
        "printable ASCII must be preserved"
    );
    assert!(
        result.contains("world"),
        "printable ASCII must be preserved"
    );
    // Control chars should be replaced with U+FFFD.
    assert!(
        result.contains('\u{FFFD}'),
        "control characters must be replaced with U+FFFD"
    );
}

/// RFC 3501 Section 5.1.3: when the Base64 data inside a `&...-`
/// shift sequence is not valid modified Base64, the decoder falls back
/// to emitting the raw `&<base64>-` literally (Postel's law).
#[test]
fn invalid_base64_in_shift_falls_back_to_raw() {
    // `!!!` is not valid Base64  -  none of those characters appear in the
    // modified Base64 alphabet. engine.decode() will return Err,
    // triggering the fallback at L151-156.
    let result = decode_utf7(b"test&!!!-end");
    assert_eq!(
        result, "test&!!!-end",
        "Invalid Base64 within shift must emit raw fallback (RFC 3501 Section 5.1.3)"
    );
}

/// RFC 3501 Section 5.1.3: when the Base64 data inside a shift
/// decodes to bytes but cannot form valid characters, the raw
/// fallback must still fire. Here we use `@#$` which
/// is not in the modified Base64 alphabet.
#[test]
fn invalid_base64_chars_in_shift_falls_back() {
    let result = decode_utf7(b"&@#$-");
    assert_eq!(
        result, "&@#$-",
        "Base64 with invalid alphabet chars must produce raw fallback"
    );
}

/// RFC 3501 Section 5.1.3: bytes 0x80-0xFF outside a Base64 segment
/// that do not form valid UTF-8 must be handled via `from_utf8_lossy`,
/// producing U+FFFD replacement characters.
#[test]
fn invalid_utf8_high_bytes_outside_base64() {
    // 0xFF is not a valid start byte for any UTF-8 sequence.
    // 0xFE is similarly invalid. A sequence of such bytes triggers
    // the from_utf8_lossy fallback.
    let result = decode_utf7(&[b'A', 0xFF, 0xFE, b'B']);
    assert!(
        result.starts_with('A'),
        "printable ASCII before invalid bytes must be preserved"
    );
    assert!(
        result.ends_with('B'),
        "printable ASCII after invalid bytes must be preserved"
    );
    assert!(
        result.contains('\u{FFFD}'),
        "invalid UTF-8 high bytes must produce U+FFFD (RFC 3501 Section 5.1.3)"
    );
}

/// RFC 3501 Section 5.1.3: a lone continuation byte (0x80-0xBF) without
/// a leading byte is invalid UTF-8 and must produce U+FFFD via the
/// `from_utf8_lossy` path.
#[test]
fn lone_continuation_byte_outside_base64() {
    // 0x80 is a continuation byte that doesn't follow a valid start byte.
    let result = decode_utf7(&[0x80]);
    assert_eq!(
        result, "\u{FFFD}",
        "lone continuation byte must produce U+FFFD"
    );
}

/// RFC 3501 Section 5.1.3: UTF-16BE requires valid code units.
/// An unpaired high surrogate (0xD800) cannot be decoded to a
/// character, so `decode_utf16be` must emit U+FFFD.
#[test]
fn unpaired_high_surrogate_produces_replacement() {
    // 0xD800 is a high surrogate. Without a following low surrogate,
    // char::decode_utf16 yields Err, triggering the replacement path.
    let mut out = String::new();
    decode_utf16be(&[0xD8, 0x00], &mut out);
    assert_eq!(
        out, "\u{FFFD}",
        "unpaired high surrogate must produce U+FFFD (RFC 3501 Section 5.1.3)"
    );
}

/// RFC 3501 Section 5.1.3: an unpaired low surrogate (0xDC00)
/// is also invalid UTF-16 and must produce U+FFFD.
#[test]
fn unpaired_low_surrogate_produces_replacement() {
    // 0xDC00 is a low surrogate without a preceding high surrogate.
    let mut out = String::new();
    decode_utf16be(&[0xDC, 0x00], &mut out);
    assert_eq!(
        out, "\u{FFFD}",
        "unpaired low surrogate must produce U+FFFD (RFC 3501 Section 5.1.3)"
    );
}

/// RFC 3501 Section 5.1.3 / Postel's law: a trailing `&` at the end of
/// input with no closing `-` should be emitted as a literal `&` character.
/// Since no Base64 content follows the `&`, it is ambiguous whether the
/// sender intended a shift sequence or a literal `&`. Per Postel's law,
/// silently dropping the character loses data, so we preserve it.
#[test]
fn trailing_ampersand_preserved_as_literal() {
    assert_eq!(
        decode_utf7(b"test&"),
        "test&",
        "trailing `&` with no closing `-` must be preserved as literal (Postel's law)"
    );
    assert_eq!(
        decode_utf7(b"&"),
        "&",
        "lone `&` with no closing `-` must be preserved as literal (Postel's law)"
    );
}

/// RFC 3501 Section 5.1.3: a high surrogate followed by another high
/// surrogate (instead of a low surrogate) must produce two U+FFFD
/// replacement characters via the `decode_utf16be` error path.
#[test]
fn two_high_surrogates_produce_two_replacements() {
    // Two consecutive high surrogates: 0xD800, 0xD800.
    let mut out = String::new();
    decode_utf16be(&[0xD8, 0x00, 0xD8, 0x00], &mut out);
    assert_eq!(
        out, "\u{FFFD}\u{FFFD}",
        "two unpaired high surrogates must each produce U+FFFD"
    );
}

/// RFC 3501 Section 5.1.3: verify the surrogate replacement path
/// works end-to-end via `decode_utf7`. We manually encode a lone
/// high surrogate (0xD800) as modified Base64 inside a `&...-` block.
#[test]
fn unpaired_surrogate_in_base64_segment_produces_replacement() {
    // UTF-16BE for lone high surrogate 0xD800 = [0xD8, 0x00].
    // Modified Base64 encoding of those bytes via the IMAP engine.
    let engine = &*IMAP_B64_ENGINE;
    let encoded_b64 = engine.encode([0xD8, 0x00]);
    let mut input = Vec::new();
    input.push(b'&');
    input.extend_from_slice(encoded_b64.as_bytes());
    input.push(b'-');

    let result = decode_utf7(&input);
    assert!(
        result.contains('\u{FFFD}'),
        "unpaired surrogate in Base64 segment must produce U+FFFD (RFC 3501 Section 5.1.3)"
    );
}

mod prop_invariants {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1000))]

        /// I15 property: MUTF-7 encode/decode is a round-trip identity for
        /// all valid UTF-8 strings without NUL, CR, or LF (the characters
        /// forbidden in IMAP mailbox names by RFC 3501 Section 9 /
        /// RFC 9051 Section 9).
        ///
        /// RFC 3501 Section 5.1.3: Modified UTF-7 encoding.
        #[test]
        fn i15_roundtrip_encode_decode_utf7(name in "[^\0\r\n]*") {
            let encoded = encode_utf7(&name);
            let decoded = decode_utf7(encoded.as_bytes());
            prop_assert_eq!(
                &decoded, &name,
                "round-trip failed: encode_utf7({:?}) = {:?}, decode_utf7({:?}) = {:?}",
                name, encoded, encoded, decoded
            );
        }
    }
}

/// Validate that the modified Base64 alphabet constant is exactly 64
/// unique, valid Base64 characters with `,` replacing `/`
/// (RFC 3501 Section 5.1.3).
#[test]
fn imap_b64_alphabet_is_valid() {
    assert_eq!(
        IMAP_B64_ALPHABET_STR.len(),
        64,
        "alphabet must be exactly 64 characters"
    );
    assert!(
        !IMAP_B64_ALPHABET_STR.contains('/'),
        "IMAP modified Base64 must replace / with ,"
    );
    assert!(
        IMAP_B64_ALPHABET_STR.contains(','),
        "IMAP modified Base64 must contain , in place of /"
    );
    // Verify no duplicates.
    let mut chars: Vec<char> = IMAP_B64_ALPHABET_STR.chars().collect();
    chars.sort_unstable();
    chars.dedup();
    assert_eq!(chars.len(), 64, "alphabet must have 64 unique characters");
    // Verify the engine initializes without error.
    base64::alphabet::Alphabet::new(IMAP_B64_ALPHABET_STR)
        .expect("alphabet must be accepted by the base64 crate");
}

/// RFC 3501 Section 5.1.3 replaces raw control octets with U+FFFD, but a
/// control character carried *inside* a modified-Base64 shift segment is
/// decoded literally: `&AAoACQ-` is UTF-16BE `U+000A U+0009`.
///
/// The two paths therefore disagree, and deliberately: a raw control octet is
/// a framing error in the wire form and is replaced, while a Base64 shift
/// segment is an unambiguous encoding of a code point the server chose for its
/// mailbox, so it is decoded and carried through `MailboxName::from_decoded`
/// unchanged.
#[test]
fn base64_encoded_control_characters_are_decoded_literally() {
    assert_eq!(
        decode_utf7(b"&AAoACQ-"),
        "\n\t",
        "control characters inside a Base64 shift segment survive decoding, \
         unlike raw control octets which become U+FFFD"
    );
    // Contrast: the same octets sent raw are replaced.
    assert_eq!(decode_utf7(b"\n\t"), "\u{FFFD}\u{FFFD}");
    assert_eq!(
        MailboxName::from_decoded(decode_utf7(b"&AAoACQ-")).as_str(),
        "\n\t"
    );
    // Re-encoding is what contains the control characters: MUTF-7 folds them
    // back into a shift segment, so nothing reaches the wire unescaped.
    assert_eq!(encode_utf7("\n\t"), "&AAoACQ-");
}

mod prop_decode_invariants {
    use super::*;
    use proptest::prelude::*;

    /// Arbitrary Rust strings, *including* control characters. The existing
    /// `i15_roundtrip_encode_decode_utf7` property deliberately excludes NUL,
    /// CR, and LF because they are invalid in a user-supplied mailbox name;
    /// this generator covers the names `decode_utf7` can itself produce.
    fn arb_any_string() -> impl Strategy<Value = String> {
        prop::collection::vec(any::<char>(), 0..40)
            .prop_map(|chars| chars.into_iter().collect::<String>())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// `decode_utf7` must not panic on arbitrary wire bytes. Mailbox
        /// names are fully server-controlled and are decoded before any
        /// validation runs (RFC 3501 Section 5.1.3).
        #[test]
        fn decode_utf7_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..200)) {
            let _ = decode_utf7(&bytes);
        }

        /// RFC 3501 Section 5.1.3: encode/decode is an identity for *every*
        /// Rust string, not only for names free of NUL/CR/LF. This matters
        /// operationally because `decode_utf7` can emit control characters
        /// (see `base64_encoded_control_characters_are_decoded_literally`),
        /// and the resulting `MailboxName` is handed straight back to
        /// `encode_mailbox_str` on the next command.
        #[test]
        fn roundtrip_identity_including_control_characters(name in arb_any_string()) {
            let encoded = encode_utf7(&name);
            prop_assert_eq!(decode_utf7(encoded.as_bytes()), name);
        }

        /// Whatever `decode_utf7` produced from hostile wire bytes must
        /// survive the round trip the account layer actually performs:
        /// decode on parse, re-encode on the next command. A non-identity
        /// here would make the client address a different mailbox than the
        /// server named (RFC 3501 Section 5.1.3).
        #[test]
        fn decode_is_stable_under_reencode(bytes in prop::collection::vec(any::<u8>(), 0..200)) {
            let once = decode_utf7(&bytes);
            let twice = decode_utf7(encode_utf7(&once).as_bytes());
            prop_assert_eq!(twice, once);
        }

        /// Encoding never emits an octet outside printable US-ASCII
        /// (RFC 3501 Section 5.1.3: everything else goes through the
        /// modified-Base64 shift). This is what lets `encode_mailbox_str`
        /// hand the result to the quoted-string encoder without escaping.
        #[test]
        fn encode_emits_only_printable_ascii(name in arb_any_string()) {
            let encoded = encode_utf7(&name);
            for b in encoded.bytes() {
                prop_assert!(
                    (0x20..=0x7E).contains(&b),
                    "encode_utf7 emitted non-printable octet {:#04x} for {:?}",
                    b, name
                );
            }
        }
    }
}
