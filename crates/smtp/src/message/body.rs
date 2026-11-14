use std::{mem, ops::Deref};

use crate::message::header::{ContentTransferEncoding, ContentType};

/// A [`Message`][super::Message] or [`SinglePart`][super::SinglePart] body that has already been encoded.
#[derive(Debug, Clone)]
pub struct Body {
    buf: Vec<u8>,
    encoding: ContentTransferEncoding,
    default_content_type: Option<ContentType>,
}

/// Either a `Vec<u8>` or a `String`.
///
/// If the content is valid utf-8 a `String` should be passed, as it
/// makes for a more efficient `Content-Transfer-Encoding` to be chosen.
#[derive(Debug, Clone)]
pub enum MaybeString {
    /// Binary data
    Binary(Vec<u8>),
    /// UTF-8 string
    String(String),
}

impl Body {
    /// Encode the supplied `buf`, making it ready to be sent as a body.
    ///
    /// Takes a `Vec<u8>` or a `String`.
    ///
    /// Automatically chooses the most efficient encoding between
    /// `7bit`, `quoted-printable` and `base64`.
    ///
    /// If `String` is passed, line endings are converted to `CRLF`.
    ///
    /// If `buf` is valid utf-8 a `String` should be supplied, as `String`s
    /// can be encoded as `7bit` or `quoted-printable`, while `Vec<u8>` always
    /// get encoded as `base64`.
    pub fn new<B: Into<MaybeString>>(buf: B) -> Self {
        let mut buf: MaybeString = buf.into();
        let default_content_type = buf.default_content_type();

        let encoding = buf.encoding(false);
        buf.encode_crlf();
        Self::new_impl(buf.into(), encoding, default_content_type)
    }

    /// Encode the supplied `buf`, using the provided `encoding`.
    ///
    /// [`Body::new`] is generally the better option.
    ///
    /// If `String` is passed, line endings are converted to `CRLF`.
    ///
    /// Returns an [`Err`] giving back the supplied `buf`, in case the chosen
    /// encoding would have resulted into `buf` being encoded
    /// into an invalid body.
    pub fn new_with_encoding<B: Into<MaybeString>>(
        buf: B,
        encoding: ContentTransferEncoding,
    ) -> Result<Self, Vec<u8>> {
        let mut buf: MaybeString = buf.into();
        let default_content_type = buf.default_content_type();

        let best_encoding = buf.encoding(true);
        let ok = match (encoding, best_encoding) {
            (ContentTransferEncoding::SevenBit, ContentTransferEncoding::SevenBit) => true,
            (
                ContentTransferEncoding::EightBit,
                ContentTransferEncoding::SevenBit | ContentTransferEncoding::EightBit,
            ) => true,
            (ContentTransferEncoding::SevenBit | ContentTransferEncoding::EightBit, _) => false,
            (
                ContentTransferEncoding::QuotedPrintable
                | ContentTransferEncoding::Base64
                | ContentTransferEncoding::Binary,
                _,
            ) => true,
        };
        if !ok {
            return Err(buf.into());
        }

        buf.encode_crlf();
        Ok(Self::new_impl(buf.into(), encoding, default_content_type))
    }

    /// Builds a new `Body` using a pre-encoded buffer.
    ///
    /// **Generally not you want.**
    ///
    /// `buf` shouldn't contain non-ascii characters, lines longer than 1000 characters or nul bytes.
    #[inline]
    pub fn dangerous_pre_encoded(buf: Vec<u8>, encoding: ContentTransferEncoding) -> Self {
        Self {
            buf,
            encoding,
            default_content_type: None,
        }
    }

    /// Encodes the supplied `buf` using the provided `encoding`
    fn new_impl(
        buf: Vec<u8>,
        encoding: ContentTransferEncoding,
        default_content_type: Option<ContentType>,
    ) -> Self {
        match encoding {
            ContentTransferEncoding::SevenBit
            | ContentTransferEncoding::EightBit
            | ContentTransferEncoding::Binary => Self {
                buf,
                encoding,
                default_content_type,
            },
            ContentTransferEncoding::QuotedPrintable => {
                // Quoted-printable text mode only treats CRLF as a hard line
                // break and escapes a bare LF as `=0A` (quoted_printable 0.5.2
                // onward), so a bare LF is turned into CRLF first. That keeps
                // a bare LF as a line break, which is how it was always
                // emitted before.
                let mut buf = buf;
                in_place_crlf_bytes(&mut buf);
                let encoded = quoted_printable::encode(buf);

                Self {
                    buf: encoded,
                    encoding: ContentTransferEncoding::QuotedPrintable,
                    default_content_type,
                }
            }
            ContentTransferEncoding::Base64 => {
                let len = email_encoding::body::base64::encoded_len(buf.len());

                let mut out = String::with_capacity(len);
                email_encoding::body::base64::encode(&buf, &mut out)
                    .expect("encode body as base64");

                Self {
                    buf: out.into_bytes(),
                    encoding: ContentTransferEncoding::Base64,
                    default_content_type,
                }
            }
        }
    }

    /// Returns the length of this `Body` in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Returns `true` if this `Body` has a length of zero, `false` otherwise.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Returns the `Content-Transfer-Encoding` of this `Body`.
    #[inline]
    pub fn encoding(&self) -> ContentTransferEncoding {
        self.encoding
    }

    pub(crate) fn default_content_type(&self) -> Option<ContentType> {
        self.default_content_type.clone()
    }

    /// Consumes `Body` and returns the inner `Vec<u8>`
    #[inline]
    pub fn into_vec(self) -> Vec<u8> {
        self.buf
    }
}

impl MaybeString {
    fn default_content_type(&self) -> Option<ContentType> {
        match self {
            Self::String(_) => Some(ContentType::TEXT_PLAIN),
            Self::Binary(_) => None,
        }
    }

    /// Suggests the best `Content-Transfer-Encoding` to be used for this `MaybeString`
    ///
    /// The `binary` encoding is never returned
    fn encoding(&self, supports_utf8: bool) -> ContentTransferEncoding {
        use email_encoding::body::Encoding;

        let output = match self {
            Self::String(s) => Encoding::choose(s.as_str(), supports_utf8),
            Self::Binary(b) => Encoding::choose(b.as_slice(), supports_utf8),
        };

        match output {
            Encoding::SevenBit => ContentTransferEncoding::SevenBit,
            Encoding::EightBit => ContentTransferEncoding::EightBit,
            Encoding::QuotedPrintable => ContentTransferEncoding::QuotedPrintable,
            Encoding::Base64 => ContentTransferEncoding::Base64,
        }
    }

    /// Encode line endings to CRLF if the variant is `String`
    fn encode_crlf(&mut self) {
        match self {
            Self::String(string) => in_place_crlf_line_endings(string),
            Self::Binary(_) => {}
        }
    }

    /// Normalize a top-level message body before it reaches the SMTP DATA
    /// encoder.
    ///
    /// Raw byte input that ends up on the wire as literal `7bit`/`8bit` text
    /// is line-oriented data, so it must obey the same CRLF boundary as
    /// `String` input: otherwise a bare `LF` reaches the DATA writer, which
    /// dot-stuffs it defensively and a CRLF-strict relay delivers the extra
    /// dot as content. Byte input that gets re-encoded (base64, and the
    /// `binary` extension) is opaque payload - rewriting `0x0A` there would
    /// corrupt what the peer decodes, so it is left exactly as supplied.
    fn encode_message_crlf(&mut self, encoding: ContentTransferEncoding) {
        match self {
            Self::String(string) => in_place_crlf_line_endings(string),
            Self::Binary(bytes) => {
                if matches!(
                    encoding,
                    ContentTransferEncoding::SevenBit | ContentTransferEncoding::EightBit
                ) {
                    in_place_crlf_bytes(bytes);
                }
            }
        }
    }
}

/// A trait for something that takes an encoded [`Body`].
///
/// Used by [`MessageBuilder::body`][super::MessageBuilder::body] and
/// [`SinglePartBuilder::body`][super::SinglePartBuilder::body],
/// which can either take something that can be encoded into [`Body`]
/// or a pre-encoded [`Body`].
///
/// If `encoding` is `None` the best encoding between `7bit`, `quoted-printable`
/// and `base64` is chosen based on the input body. **Best option.**
///
/// If `encoding` is `Some` the supplied encoding is used.
/// **NOTE:** if using the specified `encoding` would result into a malformed
/// body, this will panic!
pub trait IntoBody {
    /// Encode as valid body
    fn into_body(self, encoding: Option<ContentTransferEncoding>) -> Body;

    /// Encode a top-level message body.
    fn into_message_body(self, encoding: Option<ContentTransferEncoding>) -> Body
    where
        Self: Sized,
    {
        self.into_body(encoding)
    }
}

impl<T> IntoBody for T
where
    T: Into<MaybeString>,
{
    fn into_body(self, encoding: Option<ContentTransferEncoding>) -> Body {
        match encoding {
            Some(encoding) => Body::new_with_encoding(self, encoding).expect("invalid encoding"),
            None => Body::new(self),
        }
    }

    fn into_message_body(self, encoding: Option<ContentTransferEncoding>) -> Body {
        let mut body: MaybeString = self.into();
        // The encoding that will actually be applied below decides whether the
        // buffer is line-oriented text or opaque payload.
        let effective = encoding.unwrap_or_else(|| body.encoding(false));
        body.encode_message_crlf(effective);

        // ONE decision governs both steps. `Body::new` would choose again,
        // from the already-normalized bytes, and a buffer that was CRLF
        // rewritten because `effective` said "line-oriented text" must not
        // then be encoded as opaque payload - that rewrite would have mutated
        // bytes the caller handed over untouchable. The two choices cannot
        // currently disagree (normalization only ever splits or lengthens
        // terminators, never a line), which
        // `crlf_normalization_never_changes_the_chosen_encoding` pins; passing
        // `effective` through means a future change to `MaybeString::encoding`
        // cannot quietly reintroduce the gap either.
        let chosen = encoding.unwrap_or(effective);
        Body::new_with_encoding(body, chosen).expect("invalid encoding")
    }
}

impl IntoBody for Body {
    fn into_body(self, encoding: Option<ContentTransferEncoding>) -> Body {
        let _ = encoding;

        self
    }
}

impl AsRef<[u8]> for Body {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.buf.as_ref()
    }
}

impl From<Vec<u8>> for MaybeString {
    #[inline]
    fn from(b: Vec<u8>) -> Self {
        Self::Binary(b)
    }
}

impl From<String> for MaybeString {
    #[inline]
    fn from(s: String) -> Self {
        Self::String(s)
    }
}

impl From<MaybeString> for Vec<u8> {
    #[inline]
    fn from(s: MaybeString) -> Self {
        match s {
            MaybeString::Binary(b) => b,
            MaybeString::String(s) => s.into(),
        }
    }
}

impl Deref for MaybeString {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Binary(b) => b.as_ref(),
            Self::String(s) => s.as_ref(),
        }
    }
}

/// In place conversion to CRLF line endings.
///
/// Single-pass rebuild like the byte sibling below: per-LF `String::insert`
/// shifts the whole tail each time, which is quadratic on a large LF-only
/// body.
fn in_place_crlf_line_endings(string: &mut String) {
    let bare_lf_count = find_all_lf_char_indices(string).len();
    if bare_lf_count == 0 {
        return;
    }
    let mut normalized = String::with_capacity(string.len() + bare_lf_count);
    let mut previous = None;
    for ch in string.chars() {
        if ch == '\n' && previous != Some('\r') {
            normalized.push('\r');
        }
        normalized.push(ch);
        previous = Some(ch);
    }
    *string = normalized;
}

/// Convert bare LF bytes to CRLF without interpreting the rest of a raw body.
fn in_place_crlf_bytes(bytes: &mut Vec<u8>) {
    if !bytes
        .iter()
        .enumerate()
        .any(|(index, byte)| *byte == b'\n' && (index == 0 || bytes[index - 1] != b'\r'))
    {
        return;
    }

    let mut normalized = Vec::with_capacity(bytes.len());
    let mut previous = None;
    for byte in bytes.drain(..) {
        if byte == b'\n' && previous != Some(b'\r') {
            normalized.push(b'\r');
        }
        normalized.push(byte);
        previous = Some(byte);
    }
    *bytes = normalized;
}

/// Find indices to all places where `\r` should be inserted
/// in order to make `s` have CRLF line endings
///
/// The list is reversed, which is more efficient.
fn find_all_lf_char_indices(s: &str) -> Vec<usize> {
    let mut indices = Vec::new();

    let mut found_lf = false;
    for (i, c) in s.char_indices().rev() {
        if mem::take(&mut found_lf) && c != '\r' {
            // the previous character was `\n`, but this isn't a `\r`
            indices.push(i + c.len_utf8());
        }

        found_lf = c == '\n';
    }

    if found_lf {
        // the first character is `\n`
        indices.push(0);
    }

    indices
}

#[cfg(test)]
mod test {

    use super::{Body, ContentTransferEncoding, in_place_crlf_bytes, in_place_crlf_line_endings};

    #[test]
    fn raw_byte_message_normalization_converts_only_bare_lf() {
        let mut bytes = b"first\nsecond\r\nthird\n".to_vec();

        in_place_crlf_bytes(&mut bytes);

        assert_eq!(bytes, b"first\r\nsecond\r\nthird\r\n");
    }

    /// Finding 8: `into_message_body` decides whether the buffer is
    /// line-oriented text from the PRE-normalization bytes, then normalizes.
    /// If a second choice made from the post-normalization bytes could
    /// disagree - text going in, opaque payload coming out - the normalization
    /// would have mutated bytes the caller handed over untouchable. The driver
    /// now carries the one decision through, and this pins the property that
    /// made the old two-decision shape safe in the first place, so a change to
    /// `MaybeString::encoding` that breaks it is caught here rather than in
    /// somebody's mail.
    #[test]
    fn crlf_normalization_never_changes_the_chosen_encoding() {
        use super::MaybeString;

        let long_line = "a".repeat(1500);
        let long_line_high = "\u{e9}".repeat(700);
        let cases: Vec<Vec<u8>> = vec![
            b"plain ascii".to_vec(),
            b"bare\nlf\nlines".to_vec(),
            b"bare\rcr\rlines".to_vec(),
            b"mixed\r\nand\nand\rterminators\n".to_vec(),
            b"high \xc3\xa9 bytes\nwith bare lf\n".to_vec(),
            b"high \xc3\xa9 bytes\rwith bare cr\r".to_vec(),
            b"trailing bare cr\r".to_vec(),
            b"trailing bare lf\n".to_vec(),
            b"\r".to_vec(),
            b"\n".to_vec(),
            b"\0nul byte\n".to_vec(),
            long_line.clone().into_bytes(),
            format!("{long_line}\nsecond line").into_bytes(),
            format!("{long_line}\rsecond line").into_bytes(),
            long_line_high.clone().into_bytes(),
            format!("{long_line_high}\nsecond").into_bytes(),
            Vec::new(),
        ];

        for case in cases {
            let before = MaybeString::Binary(case.clone()).encoding(false);
            let mut normalized = MaybeString::Binary(case.clone());
            normalized.encode_message_crlf(before);
            let after = normalized.encoding(false);
            assert_eq!(
                before,
                after,
                "normalization changed the chosen encoding for {:?}",
                String::from_utf8_lossy(&case)
            );
        }
    }

    #[test]
    fn seven_bit_detect() {
        let encoded = Body::new(String::from("Hello, world!"));

        assert_eq!(encoded.encoding(), ContentTransferEncoding::SevenBit);
        assert_eq!(encoded.as_ref(), b"Hello, world!");
    }

    #[test]
    fn seven_bit_encode() {
        let encoded = Body::new_with_encoding(
            String::from("Hello, world!"),
            ContentTransferEncoding::SevenBit,
        )
        .unwrap();

        assert_eq!(encoded.encoding(), ContentTransferEncoding::SevenBit);
        assert_eq!(encoded.as_ref(), b"Hello, world!");
    }

    #[test]
    fn seven_bit_too_long_detect() {
        let encoded = Body::new("Hello, world!".repeat(100));

        assert_eq!(encoded.encoding(), ContentTransferEncoding::QuotedPrintable);
        assert_eq!(
            encoded.as_ref(),
            concat!(
                "Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, wor=\r\n",
                "ld!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, =\r\n",
                "world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hell=\r\n",
                "o, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!H=\r\n",
                "ello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, worl=\r\n",
                "d!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, w=\r\n",
                "orld!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello=\r\n",
                ", world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!He=\r\n",
                "llo, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world=\r\n",
                "!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, wo=\r\n",
                "rld!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello,=\r\n",
                " world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hel=\r\n",
                "lo, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!=\r\n",
                "Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, wor=\r\n",
                "ld!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, =\r\n",
                "world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hell=\r\n",
                "o, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!H=\r\n",
                "ello, world!Hello, world!"
            )
            .as_bytes()
        );
    }

    #[test]
    fn seven_bit_too_long_fail() {
        let result = Body::new_with_encoding(
            "Hello, world!".repeat(100),
            ContentTransferEncoding::SevenBit,
        );

        assert!(result.is_err());
    }

    #[test]
    fn seven_bit_too_long_encode_quotedprintable() {
        let encoded = Body::new_with_encoding(
            "Hello, world!".repeat(100),
            ContentTransferEncoding::QuotedPrintable,
        )
        .unwrap();

        assert_eq!(encoded.encoding(), ContentTransferEncoding::QuotedPrintable);
        assert_eq!(
            encoded.as_ref(),
            concat!(
                "Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, wor=\r\n",
                "ld!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, =\r\n",
                "world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hell=\r\n",
                "o, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!H=\r\n",
                "ello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, worl=\r\n",
                "d!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, w=\r\n",
                "orld!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello=\r\n",
                ", world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!He=\r\n",
                "llo, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world=\r\n",
                "!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, wo=\r\n",
                "rld!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello,=\r\n",
                " world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hel=\r\n",
                "lo, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!=\r\n",
                "Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, wor=\r\n",
                "ld!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, =\r\n",
                "world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!Hell=\r\n",
                "o, world!Hello, world!Hello, world!Hello, world!Hello, world!Hello, world!H=\r\n",
                "ello, world!Hello, world!"
            )
            .as_bytes()
        );
    }

    #[test]
    fn seven_bit_invalid() {
        let result = Body::new_with_encoding(
            String::from("Привет, мир!"),
            ContentTransferEncoding::SevenBit,
        );

        assert!(result.is_err());
    }

    #[test]
    fn eight_bit_encode() {
        let encoded = Body::new_with_encoding(
            String::from("Привет, мир!"),
            ContentTransferEncoding::EightBit,
        )
        .unwrap();

        assert_eq!(encoded.encoding(), ContentTransferEncoding::EightBit);
        assert_eq!(encoded.as_ref(), "Привет, мир!".as_bytes());
    }

    #[test]
    fn eight_bit_too_long_fail() {
        let result = Body::new_with_encoding(
            "Привет, мир!".repeat(200),
            ContentTransferEncoding::EightBit,
        );

        assert!(result.is_err());
    }

    #[test]
    fn quoted_printable_detect() {
        let encoded = Body::new(String::from("Questo messaggio è corto"));

        assert_eq!(encoded.encoding(), ContentTransferEncoding::QuotedPrintable);
        assert_eq!(encoded.as_ref(), b"Questo messaggio =C3=A8 corto");
    }

    #[test]
    fn quoted_printable_encode_ascii() {
        let encoded = Body::new_with_encoding(
            String::from("Hello, world!"),
            ContentTransferEncoding::QuotedPrintable,
        )
        .unwrap();

        assert_eq!(encoded.encoding(), ContentTransferEncoding::QuotedPrintable);
        assert_eq!(encoded.as_ref(), b"Hello, world!");
    }

    #[test]
    fn quoted_printable_encode_utf8() {
        let encoded = Body::new_with_encoding(
            String::from("Привет, мир!"),
            ContentTransferEncoding::QuotedPrintable,
        )
        .unwrap();

        assert_eq!(encoded.encoding(), ContentTransferEncoding::QuotedPrintable);
        assert_eq!(
            encoded.as_ref(),
            b"=D0=9F=D1=80=D0=B8=D0=B2=D0=B5=D1=82, =D0=BC=D0=B8=D1=80!".as_ref()
        );
    }

    #[test]
    fn quoted_printable_encode_line_wrap() {
        let encoded = Body::new(String::from(
            "Se lo standard \u{1F4EC} fosse stato più semplice avremmo finito molto prima.",
        ));

        assert_eq!(encoded.encoding(), ContentTransferEncoding::QuotedPrintable);
        println!("{}", std::str::from_utf8(encoded.as_ref()).unwrap());
        assert_eq!(
            encoded.as_ref(),
            concat!(
                "Se lo standard =F0=9F=93=AC fosse stato pi=C3=B9 semplice avremmo finito mo=\r\n",
                "lto prima."
            )
            .as_bytes()
        );
    }

    #[test]
    fn base64_detect() {
        let input = Body::new(vec![0; 80]);
        let encoding = input.encoding();
        assert_eq!(encoding, ContentTransferEncoding::Base64);
    }

    #[test]
    fn base64_encode_bytes() {
        let encoded =
            Body::new_with_encoding(vec![0; 80], ContentTransferEncoding::Base64).unwrap();

        assert_eq!(encoded.encoding(), ContentTransferEncoding::Base64);
        assert_eq!(
            encoded.as_ref(),
            concat!(
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\r\n",
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
            )
            .as_bytes()
        );
    }

    #[test]
    fn base64_encode_bytes_wrapping() {
        let encoded = Body::new_with_encoding(
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9].repeat(20),
            ContentTransferEncoding::Base64,
        )
        .unwrap();

        assert_eq!(encoded.encoding(), ContentTransferEncoding::Base64);
        assert_eq!(
            encoded.as_ref(),
            concat!(
                "AAECAwQFBgcICQABAgMEBQYHCAkAAQIDBAUGBwgJAAECAwQFBgcICQABAgMEBQYHCAkAAQIDBAUG\r\n",
                "BwgJAAECAwQFBgcICQABAgMEBQYHCAkAAQIDBAUGBwgJAAECAwQFBgcICQABAgMEBQYHCAkAAQID\r\n",
                "BAUGBwgJAAECAwQFBgcICQABAgMEBQYHCAkAAQIDBAUGBwgJAAECAwQFBgcICQABAgMEBQYHCAkA\r\n",
                "AQIDBAUGBwgJAAECAwQFBgcICQABAgMEBQYHCAk="
            )
            .as_bytes()
        );
    }

    #[test]
    fn base64_encode_ascii() {
        let encoded = Body::new_with_encoding(
            String::from("Hello World!"),
            ContentTransferEncoding::Base64,
        )
        .unwrap();

        assert_eq!(encoded.encoding(), ContentTransferEncoding::Base64);
        assert_eq!(encoded.as_ref(), b"SGVsbG8gV29ybGQh");
    }

    #[test]
    fn base64_encode_ascii_wrapping() {
        let encoded =
            Body::new_with_encoding("Hello World!".repeat(20), ContentTransferEncoding::Base64)
                .unwrap();

        assert_eq!(encoded.encoding(), ContentTransferEncoding::Base64);
        assert_eq!(
            encoded.as_ref(),
            concat!(
                "SGVsbG8gV29ybGQhSGVsbG8gV29ybGQhSGVsbG8gV29ybGQhSGVsbG8gV29ybGQhSGVsbG8gV29y\r\n",
                "bGQhSGVsbG8gV29ybGQhSGVsbG8gV29ybGQhSGVsbG8gV29ybGQhSGVsbG8gV29ybGQhSGVsbG8g\r\n",
                "V29ybGQhSGVsbG8gV29ybGQhSGVsbG8gV29ybGQhSGVsbG8gV29ybGQhSGVsbG8gV29ybGQhSGVs\r\n",
                "bG8gV29ybGQhSGVsbG8gV29ybGQhSGVsbG8gV29ybGQhSGVsbG8gV29ybGQhSGVsbG8gV29ybGQh\r\n",
                "SGVsbG8gV29ybGQh"
            )
            .as_bytes()
        );
    }

    #[test]
    fn crlf() {
        let mut string = String::from("Send me a \u{2709}\u{FE0F}\nwith\nbifrost_smtp!\n\u{1F600}");

        in_place_crlf_line_endings(&mut string);
        assert_eq!(
            string,
            "Send me a \u{2709}\u{FE0F}\r\nwith\r\nbifrost_smtp!\r\n\u{1F600}"
        );
    }

    #[test]
    fn harsh_crlf() {
        let mut string = String::from(
            "\n\nSend me a \u{2709}\u{FE0F}\r\n\nwith\n\nbifrost_smtp!\n\r\n\u{1F600}",
        );

        in_place_crlf_line_endings(&mut string);
        assert_eq!(
            string,
            "\r\n\r\nSend me a \u{2709}\u{FE0F}\r\n\r\nwith\r\n\r\nbifrost_smtp!\r\n\r\n\u{1F600}"
        );
    }

    #[test]
    fn crlf_noop() {
        let mut string =
            String::from("\r\nSend me a \u{2709}\u{FE0F}\r\nwith\r\nbifrost_smtp!\r\n\u{1F600}");

        in_place_crlf_line_endings(&mut string);
        assert_eq!(
            string,
            "\r\nSend me a \u{2709}\u{FE0F}\r\nwith\r\nbifrost_smtp!\r\n\u{1F600}"
        );
    }
}
