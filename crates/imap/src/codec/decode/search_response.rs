//! SEARCH, SORT, and ESEARCH response parsers.

#[allow(clippy::wildcard_imports)]
use super::primitives::*;
#[allow(clippy::wildcard_imports)]
use super::*;

/// Parse `* SEARCH` response (RFC 3501 Section 7.2.5, RFC 7162 Section 3.1.5).
///
/// RFC 7162 Section 3.1.5: when a MODSEQ search criterion is used and the
/// result is non-empty, the server appends `(MODSEQ <n>)`.
/// Example: `* SEARCH 2 5 6 7 11 12 18 19 20 23 (MODSEQ 917162500)`
pub(super) fn parse_untagged_search(input: &[u8]) -> IResult<&[u8], UntaggedResponse> {
    let (input, (nums, mod_seq)) = parse_number_list_with_modseq(input, b"SEARCH")?;
    Ok((
        input,
        UntaggedResponse::Search {
            numbers: nums,
            mod_seq,
        },
    ))
}

/// Parse `* SORT` response (RFC 5256 Section 4, RFC 7162 Section 3.1.6).
///
/// `sort-data = "SORT" *(SP nz-number) [SP search-sort-mod-seq]`; the
/// result is a list of message numbers or UIDs in sorted order. An empty
/// result (no numbers) is valid and means no messages matched.
///
/// RFC 7162 Section 3.1.6: when a MODSEQ search criterion is used, the
/// server appends `(MODSEQ <mod-sequence-value>)` to the SORT response.
pub(super) fn parse_untagged_sort(input: &[u8]) -> IResult<&[u8], UntaggedResponse> {
    let (input, (nums, mod_seq)) = parse_number_list_with_modseq(input, b"SORT")?;
    Ok((input, UntaggedResponse::Sort { nums, mod_seq }))
}

/// Shared parser for SEARCH and SORT responses (RFC 3501 Section 7.2.5,
/// RFC 5256 Section 4, RFC 7162 Section 3.1.5-6).
///
/// Both responses share the same wire format:
/// `keyword *(SP nz-number) [SP "(" "MODSEQ" SP mod-sequence-value ")"] CRLF`
///
/// Some servers send trailing spaces with no numbers; these are tolerated via
/// `parse_optional_modseq_and_crlf` (Postel's law).
fn parse_number_list_with_modseq<'a>(
    input: &'a [u8],
    keyword: &'static [u8],
) -> IResult<&'a [u8], (Vec<u32>, Option<crate::types::ModSeq>)> {
    let (input, _) = tag_no_case(keyword).parse(input)?;
    // RFC 3501 Section 7.2.5 / RFC 5256 Section 4 ABNF specifies
    // `nz-number`, but we use `number` per Postel's law: some
    // non-conformant servers include 0 in SEARCH/SORT results, and using
    // `nz_number` with `many0` silently drops 0 and all subsequent results.
    let (input, nums) = many0(preceded(sp, number)).parse(input)?;
    // Discard 0 values because they are not valid UIDs or sequence numbers.
    let nums: Vec<u32> = nums.into_iter().filter(|&n| n != 0).collect();
    let (input, mod_seq) = parse_optional_modseq_and_crlf(input)?;
    Ok((input, (nums, mod_seq)))
}

/// Parse optional `search-sort-mod-seq` suffix and consume trailing content
/// through CRLF (RFC 7162 Section 3.1.6).
fn parse_optional_modseq_and_crlf(input: &[u8]) -> IResult<&[u8], Option<crate::types::ModSeq>> {
    // A malformed or overflowing MODSEQ is discarded rather than committed:
    // `number64` reports overflow as a Failure, which `opt` would propagate
    // and so lose the already-parsed result numbers. Demote it to a plain
    // error here so the suffix is skipped and the numbers survive.
    fn soft_nz_number64(i: &[u8]) -> IResult<&[u8], u64> {
        nz_number64(i).map_err(|e| match e {
            nom::Err::Failure(inner) => nom::Err::Error(inner),
            other => other,
        })
    }
    let (input, mod_seq) = opt(preceded(
        sp,
        delimited(
            (char('('), tag_no_case(&b"MODSEQ"[..]), sp),
            soft_nz_number64,
            char(')'),
        ),
    ))
    .parse(input)?;
    let (input, _) = take_while(|b: u8| b != b'\r' && b != b'\n').parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, mod_seq.and_then(crate::types::ModSeq::new)))
}

/// Parse `* ESEARCH (TAG "tag") [UID] result-data` (RFC 4731 Section 3.1).
///
/// RFC 4731 Section 3.1 ABNF:
/// `search-return-data = "MIN" SP nz-number / "MAX" SP nz-number /
///                        "ALL" SP sequence-set / "COUNT" SP number`
///
/// Produces `UntaggedResponse::Esearch(EsearchResponse)` with all fields preserved.
pub(super) fn parse_untagged_esearch(input: &[u8]) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag_no_case(&b"ESEARCH"[..]).parse(input)?;

    // Optional search-correlator: (TAG "tag")
    // RFC 4466 Section 2.6.2: `search-correlator = SP "(" "TAG" SP tag-string ")"`
    let (input, tag_val) = opt(preceded(
        sp,
        delimited(
            char('('),
            preceded((tag_no_case(&b"TAG"[..]), sp), astring),
            char(')'),
        ),
    ))
    .parse(input)?;

    // Optional UID indicator (RFC 4731 Section 3.1).
    // Like `nil_token`, we must verify a token boundary after "UID" to
    // prevent greedy prefix matches against atoms like "UIDFOO".
    let (input, uid_indicator) = opt(preceded(
        sp,
        terminated(
            tag_no_case(&b"UID"[..]),
            peek(alt((
                value((), verify(take(1u8), |b: &[u8]| !is_atom_char(b[0]))),
                value((), eof),
            ))),
        ),
    ))
    .parse(input)?;

    let mut esearch = EsearchResponse {
        tag: tag_val.map(|v| String::from_utf8_lossy(&v).into_owned()),
        ..EsearchResponse::default()
    };

    let mut min = None;
    let mut max = None;
    let mut all = Vec::new();
    // Result data: space-separated key-value pairs until CRLF.
    let mut input = input;

    while let Ok((rest, _)) = sp(input) {
        if rest.first() == Some(&b'\r') || rest.first() == Some(&b'\n') {
            input = rest;
            break;
        }

        let (rest, key) = atom(rest)?;
        let key_upper = String::from_utf8_lossy(key).to_ascii_uppercase();

        match key_upper.as_str() {
            "ALL" => {
                // RFC 4731 Section 3.1: "ALL SP sequence-set"
                let (rest2, _) = sp(rest)?;
                let (rest2, set) = sequence_set(rest2)?;
                all = set;
                input = rest2;
            }
            "MIN" => {
                let (rest2, _) = sp(rest)?;
                let (rest2, val) = nz_number(rest2)?;
                min = Some(val);
                input = rest2;
            }
            "MAX" => {
                let (rest2, _) = sp(rest)?;
                let (rest2, val) = nz_number(rest2)?;
                max = Some(val);
                input = rest2;
            }
            "COUNT" => {
                let (rest2, _) = sp(rest)?;
                let (rest2, val) = number(rest2)?;
                esearch.count = Some(val);
                input = rest2;
            }
            "MODSEQ" => {
                // RFC 7162 Section 3.1.10: mod-sequence-value must be >= 1.
                // Malformed MODSEQ values are skipped so already-parsed
                // results are preserved.
                if let Ok((rest2, _)) = sp(rest) {
                    if let Ok((rest2, val)) = nz_number64(rest2) {
                        esearch.mod_seq = crate::types::ModSeq::new(val);
                        input = rest2;
                    } else {
                        let (rest2, _) =
                            take_while(|b: u8| b != b' ' && b != b'\r').parse(rest2)?;
                        input = rest2;
                    }
                } else {
                    let (rest2, _) = take_while(|b: u8| b != b' ' && b != b'\r').parse(rest)?;
                    input = rest2;
                }
            }
            _ => {
                // Unknown keys may or may not have a value. Skip the value
                // if present, preserving known result data already parsed.
                let Ok((rest2, _)) = sp(rest) else {
                    input = rest;
                    continue;
                };
                if rest2.first() == Some(&b'(') {
                    let (rest2, ()) = skip_parenthesized_block(rest2)?;
                    input = rest2;
                } else {
                    let (rest2, ()) =
                        super::envelope_fetch::skip_tagged_ext_simple(|b| b == b' ' || b == b'\r')(
                            rest2,
                        )?;
                    input = rest2;
                }
            }
        }
    }

    let (input, _) = crlf(input)?;
    esearch.numbers =
        crate::types::response::EsearchNumbers::from_wire(uid_indicator.is_some(), min, max, all);
    Ok((input, UntaggedResponse::Esearch(esearch)))
}
