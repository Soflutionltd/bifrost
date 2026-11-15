//! SMTP response, containing a mandatory return code and an optional text
//! message

use std::{
    fmt::{Display, Formatter, Result},
    result,
    str::FromStr,
};

use nom::{
    IResult, Parser,
    branch::alt,
    bytes::streaming::{tag, take_until},
    combinator::{complete, map, peek, value},
    multi::many0,
    sequence::preceded,
};

use crate::transport::smtp::{Error, error};

/// The first digit indicates severity
#[derive(PartialEq, Eq, Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
// pub: users inspect and construct typed SMTP status codes.
pub enum Severity {
    /// 2yx
    PositiveCompletion = 2,
    /// 3yz
    PositiveIntermediate = 3,
    /// 4yz
    TransientNegativeCompletion = 4,
    /// 5yz
    PermanentNegativeCompletion = 5,
}

impl Display for Severity {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", *self as u8)
    }
}

/// Second digit
#[derive(PartialEq, Eq, Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
// pub: users inspect and construct typed SMTP status codes.
pub enum Category {
    /// x0z
    Syntax = 0,
    /// x1z
    Information = 1,
    /// x2z
    Connections = 2,
    /// x3z
    Unspecified3 = 3,
    /// x4z
    Unspecified4 = 4,
    /// x5z
    MailSystem = 5,
}

impl Display for Category {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", *self as u8)
    }
}

/// The detail digit of a response code (third digit)
#[derive(PartialEq, Eq, Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
// pub: users inspect and construct typed SMTP status codes.
pub enum Detail {
    #[allow(missing_docs)]
    Zero = 0,
    #[allow(missing_docs)]
    One = 1,
    #[allow(missing_docs)]
    Two = 2,
    #[allow(missing_docs)]
    Three = 3,
    #[allow(missing_docs)]
    Four = 4,
    #[allow(missing_docs)]
    Five = 5,
    #[allow(missing_docs)]
    Six = 6,
    #[allow(missing_docs)]
    Seven = 7,
    #[allow(missing_docs)]
    Eight = 8,
    #[allow(missing_docs)]
    Nine = 9,
}

impl Display for Detail {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", *self as u8)
    }
}

/// Represents a 3 digit SMTP response code
#[derive(PartialEq, Eq, Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
// pub: Response and Error expose this typed SMTP status code.
pub struct Code {
    /// First digit of the response code
    pub severity: Severity,
    /// Second digit of the response code
    pub category: Category,
    /// Third digit
    pub detail: Detail,
}

impl Display for Code {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}{}{}", self.severity, self.category, self.detail)
    }
}

impl Code {
    /// Creates a new `Code` structure
    pub fn new(severity: Severity, category: Category, detail: Detail) -> Code {
        Code {
            severity,
            category,
            detail,
        }
    }

    /// Whether this is a 2yz completion reply.
    pub fn is_completion(self) -> bool {
        matches!(self.severity, Severity::PositiveCompletion)
    }

    /// Whether this is a 3yz intermediate reply.
    pub fn is_intermediate(self) -> bool {
        matches!(self.severity, Severity::PositiveIntermediate)
    }

    /// Tells if the response is positive
    pub fn is_positive(self) -> bool {
        matches!(
            self.severity,
            Severity::PositiveCompletion | Severity::PositiveIntermediate
        )
    }
}

impl From<Code> for u16 {
    fn from(code: Code) -> Self {
        code.detail as u16 + 10 * code.category as u16 + 100 * code.severity as u16
    }
}

/// Represents an enhanced SMTP status code.
///
/// Enhanced status codes are defined by RFC 2034 and appear in response text as
/// `class.subject.detail`, for example `5.1.1`.
#[derive(PartialEq, Eq, Copy, Clone, Debug, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
// pub: users inspect RFC 2034 enhanced status codes on replies.
pub struct EnhancedStatusCode {
    /// Status class: 2 for success, 4 for transient failure, 5 for permanent
    /// failure.
    pub class: u8,
    /// Subject component.
    pub subject: u16,
    /// Detail component.
    pub detail: u16,
}

impl Display for EnhancedStatusCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}.{}.{}", self.class, self.subject, self.detail)
    }
}

impl FromStr for EnhancedStatusCode {
    type Err = Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        parse_enhanced_status_code(s).ok_or_else(|| error::parse("invalid enhanced status code"))
    }
}

/// Contains an SMTP reply, with separated code and message
///
/// The text message is optional, only the code is mandatory
#[derive(PartialEq, Eq, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
// pub: transports return full SMTP replies for success and status errors.
pub struct Response {
    /// Response code
    code: Code,
    /// Server response string (optional)
    /// Handle multiline responses
    message: Vec<String>,
}

impl FromStr for Response {
    type Err = Error;

    fn from_str(s: &str) -> result::Result<Response, Error> {
        parse_response(s)
            .map(|(_, r)| r)
            .map_err(|e| error::parse(e.to_owned()))
    }
}

impl Response {
    /// Creates a new `Response`
    pub fn new(code: Code, message: Vec<String>) -> Response {
        Response { code, message }
    }

    /// Whether this is a 2yz completion reply.
    pub fn is_completion(&self) -> bool {
        self.code.is_completion()
    }

    /// Whether this is a 3yz intermediate reply.
    pub fn is_intermediate(&self) -> bool {
        self.code.is_intermediate()
    }

    /// Tells if the response is positive
    pub fn is_positive(&self) -> bool {
        self.code.is_positive()
    }

    /// Tests code equality
    pub fn has_code(&self, code: u16) -> bool {
        u16::from(self.code) == code
    }

    /// Returns only the first word of the message if possible
    pub fn first_word(&self) -> Option<&str> {
        self.message
            .first()
            .and_then(|line| line.split_whitespace().next())
    }

    /// Returns only the line of the message if possible
    pub fn first_line(&self) -> Option<&str> {
        self.message.first().map(String::as_str)
    }

    /// Response code
    pub fn code(&self) -> Code {
        self.code
    }

    /// Returns the first enhanced status code from the response text.
    ///
    /// RFC 2034 puts enhanced status codes at the start of response text lines.
    /// Bifrost accepts the first well-formed code whose class matches the
    /// normal SMTP reply class.
    pub fn enhanced_status_code(&self) -> Option<EnhancedStatusCode> {
        self.enhanced_status_codes().next()
    }

    /// Returns all enhanced status codes from response text lines.
    ///
    /// Only well-formed codes whose class matches the normal SMTP reply class
    /// are returned.
    pub fn enhanced_status_codes(&self) -> impl Iterator<Item = EnhancedStatusCode> + '_ {
        let expected_class = self.code.severity as u8;
        self.message.iter().filter_map(move |line| {
            line.split_whitespace()
                .next()
                .and_then(parse_enhanced_status_code)
                .filter(|code| code.class == expected_class)
        })
    }

    /// Server response string (array of lines)
    pub fn message(&self) -> impl Iterator<Item = &str> {
        self.message.iter().map(String::as_str)
    }
}

// Parsers (originally from tokio-smtp)

fn parse_code(i: &str) -> IResult<&str, Code> {
    let (i, severity) = parse_severity(i)?;
    let (i, category) = parse_category(i)?;
    let (i, detail) = parse_detail(i)?;
    Ok((
        i,
        Code {
            severity,
            category,
            detail,
        },
    ))
}

fn parse_severity(i: &str) -> IResult<&str, Severity> {
    alt((
        map(tag("2"), |_| Severity::PositiveCompletion),
        map(tag("3"), |_| Severity::PositiveIntermediate),
        map(tag("4"), |_| Severity::TransientNegativeCompletion),
        map(tag("5"), |_| Severity::PermanentNegativeCompletion),
    ))
    .parse(i)
}

fn parse_category(i: &str) -> IResult<&str, Category> {
    alt((
        map(tag("0"), |_| Category::Syntax),
        map(tag("1"), |_| Category::Information),
        map(tag("2"), |_| Category::Connections),
        map(tag("3"), |_| Category::Unspecified3),
        map(tag("4"), |_| Category::Unspecified4),
        map(tag("5"), |_| Category::MailSystem),
    ))
    .parse(i)
}

fn parse_detail(i: &str) -> IResult<&str, Detail> {
    alt((
        map(tag("0"), |_| Detail::Zero),
        map(tag("1"), |_| Detail::One),
        map(tag("2"), |_| Detail::Two),
        map(tag("3"), |_| Detail::Three),
        map(tag("4"), |_| Detail::Four),
        map(tag("5"), |_| Detail::Five),
        map(tag("6"), |_| Detail::Six),
        map(tag("7"), |_| Detail::Seven),
        map(tag("8"), |_| Detail::Eight),
        map(tag("9"), |_| Detail::Nine),
    ))
    .parse(i)
}

fn parse_enhanced_status_code(value: &str) -> Option<EnhancedStatusCode> {
    let mut parts = value.split('.');
    let class = match parts.next()? {
        "2" => 2,
        "4" => 4,
        "5" => 5,
        _ => return None,
    };
    let subject = parse_enhanced_status_component(parts.next()?)?;
    let detail = parse_enhanced_status_component(parts.next()?)?;

    if parts.next().is_some() {
        return None;
    }

    Some(EnhancedStatusCode {
        class,
        subject,
        detail,
    })
}

fn parse_enhanced_status_component(value: &str) -> Option<u16> {
    if value.is_empty()
        || value.len() > 3
        || value.starts_with('0') && value.len() > 1
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }

    value.parse().ok()
}

pub(crate) fn parse_response(i: &str) -> IResult<&str, Response> {
    let (i, lines) = many0((
        parse_code,
        preceded(tag("-"), take_until("\r\n")),
        tag("\r\n"),
    ))
    .parse(i)?;
    let (i, (last_code, last_line)) = (
        parse_code,
        alt((
            preceded(tag(" "), take_until("\r\n")),
            value("", peek(complete(tag("\r\n")))),
        )),
    )
        .parse(i)?;
    let (i, _) = complete(tag("\r\n")).parse(i)?;

    // Check that all codes are equal.
    if !lines.iter().all(|&(code, _, _)| code == last_code) {
        return Err(nom::Err::Failure(nom::error::Error::new(
            "",
            nom::error::ErrorKind::Not,
        )));
    }

    // Extract text from lines, and append last line.
    let mut lines: Vec<String> = lines.into_iter().map(|(_, text, _)| text.into()).collect();
    lines.push(last_line.into());

    Ok((
        i,
        Response {
            code: last_code,
            message: lines,
        },
    ))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_severity_fmt() {
        assert_eq!(format!("{}", Severity::PositiveCompletion), "2");
    }

    #[test]
    fn test_category_fmt() {
        assert_eq!(format!("{}", Category::Unspecified4), "4");
    }

    #[test]
    fn test_code_new() {
        assert_eq!(
            Code::new(
                Severity::TransientNegativeCompletion,
                Category::Connections,
                Detail::Zero,
            ),
            Code {
                severity: Severity::TransientNegativeCompletion,
                category: Category::Connections,
                detail: Detail::Zero,
            }
        );
    }

    #[test]
    fn test_code_display() {
        let code = Code {
            severity: Severity::TransientNegativeCompletion,
            category: Category::Connections,
            detail: Detail::One,
        };

        assert_eq!(code.to_string(), "421");
    }

    #[test]
    fn test_code_to_u16() {
        let code = Code {
            severity: Severity::TransientNegativeCompletion,
            category: Category::Connections,
            detail: Detail::One,
        };
        let c: u16 = code.into();
        assert_eq!(c, 421);
    }

    #[test]
    fn test_response_from_str() {
        let raw_response = "250-me\r\n250-8BITMIME\r\n250-SIZE 42\r\n250 AUTH PLAIN CRAM-MD5\r\n";
        assert_eq!(
            raw_response.parse::<Response>().unwrap(),
            Response {
                code: Code {
                    severity: Severity::PositiveCompletion,
                    category: Category::MailSystem,
                    detail: Detail::Zero,
                },
                message: vec![
                    "me".to_owned(),
                    "8BITMIME".to_owned(),
                    "SIZE 42".to_owned(),
                    "AUTH PLAIN CRAM-MD5".to_owned(),
                ],
            }
        );

        let wrong_code = "2506-me\r\n250-8BITMIME\r\n250-SIZE 42\r\n250 AUTH PLAIN CRAM-MD5\r\n";
        assert!(wrong_code.parse::<Response>().is_err());

        let wrong_end = "250-me\r\n250-8BITMIME\r\n250-SIZE 42\r\n250-AUTH PLAIN CRAM-MD5\r\n";
        assert!(wrong_end.parse::<Response>().is_err());
    }

    #[test]
    fn test_response_is_positive() {
        assert!(
            Response::new(
                Code {
                    severity: Severity::PositiveCompletion,
                    category: Category::MailSystem,
                    detail: Detail::Zero,
                },
                vec!["me".to_owned(), "8BITMIME".to_owned(), "SIZE 42".to_owned(),],
            )
            .is_positive()
        );
        assert!(
            !Response::new(
                Code {
                    severity: Severity::TransientNegativeCompletion,
                    category: Category::MailSystem,
                    detail: Detail::Zero,
                },
                vec!["me".to_owned(), "8BITMIME".to_owned(), "SIZE 42".to_owned(),],
            )
            .is_positive()
        );
    }

    #[test]
    fn test_response_has_code() {
        assert!(
            Response::new(
                Code {
                    severity: Severity::TransientNegativeCompletion,
                    category: Category::MailSystem,
                    detail: Detail::One,
                },
                vec!["me".to_owned(), "8BITMIME".to_owned(), "SIZE 42".to_owned(),],
            )
            .has_code(451)
        );
        assert!(
            !Response::new(
                Code {
                    severity: Severity::TransientNegativeCompletion,
                    category: Category::MailSystem,
                    detail: Detail::One,
                },
                vec!["me".to_owned(), "8BITMIME".to_owned(), "SIZE 42".to_owned(),],
            )
            .has_code(251)
        );
    }

    #[test]
    fn test_enhanced_status_code() {
        let response: Response = "550 5.1.1 user unknown\r\n".parse().unwrap();
        assert_eq!(
            response.enhanced_status_code(),
            Some(EnhancedStatusCode {
                class: 5,
                subject: 1,
                detail: 1,
            })
        );

        let mismatched: Response = "550 4.2.0 mailbox full\r\n".parse().unwrap();
        assert_eq!(mismatched.enhanced_status_code(), None);

        let multiline: Response = "250-mail.example\r\n250 2.1.5 recipient ok\r\n"
            .parse()
            .unwrap();
        assert_eq!(
            multiline.enhanced_status_code(),
            Some(EnhancedStatusCode {
                class: 2,
                subject: 1,
                detail: 5,
            })
        );
        assert_eq!(
            multiline
                .enhanced_status_codes()
                .collect::<Vec<EnhancedStatusCode>>(),
            vec![EnhancedStatusCode {
                class: 2,
                subject: 1,
                detail: 5,
            }]
        );

        let varied: Response = concat!(
            "550-5.1.1 first user unknown\r\n",
            "550 5.2.0 second mailbox disabled\r\n"
        )
        .parse()
        .unwrap();
        assert_eq!(
            varied
                .enhanced_status_codes()
                .collect::<Vec<EnhancedStatusCode>>(),
            vec![
                EnhancedStatusCode {
                    class: 5,
                    subject: 1,
                    detail: 1,
                },
                EnhancedStatusCode {
                    class: 5,
                    subject: 2,
                    detail: 0,
                },
            ]
        );

        for invalid in ["5.01.1", "5.1.001", "5.1000.1", "5.1.1000"] {
            assert!(invalid.parse::<EnhancedStatusCode>().is_err());
        }
    }

    #[test]
    fn test_response_first_word() {
        assert_eq!(
            Response::new(
                Code {
                    severity: Severity::TransientNegativeCompletion,
                    category: Category::MailSystem,
                    detail: Detail::One,
                },
                vec!["me".to_owned(), "8BITMIME".to_owned(), "SIZE 42".to_owned(),],
            )
            .first_word(),
            Some("me")
        );
        assert_eq!(
            Response::new(
                Code {
                    severity: Severity::TransientNegativeCompletion,
                    category: Category::MailSystem,
                    detail: Detail::One,
                },
                vec![
                    "me mo".to_owned(),
                    "8BITMIME".to_owned(),
                    "SIZE 42".to_owned(),
                ],
            )
            .first_word(),
            Some("me")
        );
        assert_eq!(
            Response::new(
                Code {
                    severity: Severity::TransientNegativeCompletion,
                    category: Category::MailSystem,
                    detail: Detail::One,
                },
                vec![],
            )
            .first_word(),
            None
        );
        assert_eq!(
            Response::new(
                Code {
                    severity: Severity::TransientNegativeCompletion,
                    category: Category::MailSystem,
                    detail: Detail::One,
                },
                vec![" ".to_owned()],
            )
            .first_word(),
            None
        );
        assert_eq!(
            Response::new(
                Code {
                    severity: Severity::TransientNegativeCompletion,
                    category: Category::MailSystem,
                    detail: Detail::One,
                },
                vec!["  ".to_owned()],
            )
            .first_word(),
            None
        );
        assert_eq!(
            Response::new(
                Code {
                    severity: Severity::TransientNegativeCompletion,
                    category: Category::MailSystem,
                    detail: Detail::One,
                },
                vec![String::new()],
            )
            .first_word(),
            None
        );
    }

    #[test]
    fn test_response_incomplete() {
        let raw_response = "250-smtp.example.org\r\n";
        let res = parse_response(raw_response);
        match res {
            Err(nom::Err::Incomplete(_)) => {}
            _ => panic!("Expected incomplete response, got {res:?}"),
        }
    }

    #[test]
    fn parse_response_leaves_trailing_reply_in_remaining_input() {
        // Pipelined drains rely on `read_line` framing rather than on the
        // parser's remaining slice: `read_response_inner` discards it. Pin the
        // parser contract so a change of framing strategy is visible here.
        let (remaining, response) = parse_response("250 first\r\n250 second\r\n").unwrap();

        assert_eq!(remaining, "250 second\r\n");
        assert_eq!(response.first_line(), Some("first"));
    }

    #[test]
    fn parse_response_accepts_reply_without_a_space_separator() {
        assert!("250\r\n".parse::<Response>().is_ok());
        assert!("250-first\r\n250\r\n".parse::<Response>().is_ok());
    }

    #[test]
    fn parse_response_accepts_empty_text_after_the_code() {
        let response: Response = "250 \r\n".parse().unwrap();

        assert!(response.has_code(250));
        assert_eq!(response.first_line(), Some(""));
        assert_eq!(response.first_word(), None);
    }

    #[test]
    fn parse_response_accepts_empty_continuation_lines() {
        let response: Response = "250-\r\n250 HELP\r\n".parse().unwrap();

        assert_eq!(response.message().collect::<Vec<&str>>(), vec!["", "HELP"]);
        // An empty first line has no first word, which is what
        // `ServerInfo::from_response` reads as the server name.
        assert_eq!(response.first_word(), None);
    }

    #[test]
    fn parse_response_needs_more_input_for_bare_lf_line_endings() {
        // `take_until("\r\n")` never matches a bare-LF terminated line, so the
        // reader keeps asking for more bytes instead of accepting the reply.
        // It terminates on EOF with "incomplete response".
        assert!(matches!(
            parse_response("250 ok\n"),
            Err(nom::Err::Incomplete(_))
        ));
    }

    #[test]
    fn parse_response_rejects_mismatched_continuation_codes() {
        // A hard `Failure`, not a recoverable `Error`: the codes on a
        // multiline reply must all match.
        assert!(matches!(
            parse_response("250-ok\r\n251 ok\r\n"),
            Err(nom::Err::Failure(_))
        ));
    }

    #[test]
    fn multiline_response_preserves_every_line_in_order() {
        let response: Response = concat!(
            "250-mail.example.org\r\n",
            "250-PIPELINING\r\n",
            "250-SIZE 10240000\r\n",
            "250 HELP\r\n",
        )
        .parse()
        .unwrap();

        assert_eq!(
            response.message().collect::<Vec<&str>>(),
            vec!["mail.example.org", "PIPELINING", "SIZE 10240000", "HELP"]
        );
        assert_eq!(response.first_word(), Some("mail.example.org"));
    }

    #[test]
    fn enhanced_status_codes_skip_mismatched_and_malformed_tokens() {
        let response: Response = concat!(
            "550-4.7.1 class does not match the reply\r\n",
            "550-5.7.1.9 too many components\r\n",
            "550 5.7.1 policy rejection\r\n",
        )
        .parse()
        .unwrap();

        assert_eq!(
            response
                .enhanced_status_codes()
                .collect::<Vec<EnhancedStatusCode>>(),
            vec![EnhancedStatusCode {
                class: 5,
                subject: 7,
                detail: 1,
            }]
        );
    }

    #[test]
    fn enhanced_status_code_component_bounds() {
        for invalid in ["5.1", "5.1.1.1", "5..1", "5.1.a", "1.1.1", "5.-1.1", ""] {
            assert!(
                invalid.parse::<EnhancedStatusCode>().is_err(),
                "expected {invalid:?} to be rejected"
            );
        }

        let widest: EnhancedStatusCode = "5.999.999".parse().unwrap();
        assert_eq!(widest.to_string(), "5.999.999");
        // A single zero component is legal; a zero-prefixed one is not.
        assert!("2.0.0".parse::<EnhancedStatusCode>().is_ok());
        assert!("2.00.0".parse::<EnhancedStatusCode>().is_err());
    }

    #[test]
    fn code_converts_to_u16_and_compares_by_digits() {
        let response: Response = "421 4.7.0 too many connections\r\n".parse().unwrap();

        assert_eq!(u16::from(response.code()), 421);
        assert!(!response.is_positive());
        assert!(response.has_code(421));
        assert!(!response.has_code(42));
    }

    #[test]
    fn test_response_first_line() {
        assert_eq!(
            Response::new(
                Code {
                    severity: Severity::TransientNegativeCompletion,
                    category: Category::MailSystem,
                    detail: Detail::One,
                },
                vec!["me".to_owned(), "8BITMIME".to_owned(), "SIZE 42".to_owned(),],
            )
            .first_line(),
            Some("me")
        );
        assert_eq!(
            Response::new(
                Code {
                    severity: Severity::TransientNegativeCompletion,
                    category: Category::MailSystem,
                    detail: Detail::One,
                },
                vec![
                    "me mo".to_owned(),
                    "8BITMIME".to_owned(),
                    "SIZE 42".to_owned(),
                ],
            )
            .first_line(),
            Some("me mo")
        );
        assert_eq!(
            Response::new(
                Code {
                    severity: Severity::TransientNegativeCompletion,
                    category: Category::MailSystem,
                    detail: Detail::One,
                },
                vec![],
            )
            .first_line(),
            None
        );
        assert_eq!(
            Response::new(
                Code {
                    severity: Severity::TransientNegativeCompletion,
                    category: Category::MailSystem,
                    detail: Detail::One,
                },
                vec![" ".to_owned()],
            )
            .first_line(),
            Some(" ")
        );
        assert_eq!(
            Response::new(
                Code {
                    severity: Severity::TransientNegativeCompletion,
                    category: Category::MailSystem,
                    detail: Detail::One,
                },
                vec!["  ".to_owned()],
            )
            .first_line(),
            Some("  ")
        );
        assert_eq!(
            Response::new(
                Code {
                    severity: Severity::TransientNegativeCompletion,
                    category: Category::MailSystem,
                    detail: Detail::One,
                },
                vec![String::new()],
            )
            .first_line(),
            Some("")
        );
    }
}
