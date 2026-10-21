use crate::types::{FetchAttr, FetchResponse};

use super::pim::PREVIEW_FETCH_BYTES;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BodySelection {
    None,
    Headers,
    Preview(usize),
    Whole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FetchSelection {
    pub flags: bool,
    pub envelope: bool,
    pub size: bool,
    pub modseq: bool,
    pub body: BodySelection,
}

impl FetchSelection {
    pub(super) fn attributes(self) -> Vec<FetchAttr> {
        let mut attrs = vec![FetchAttr::Uid];
        if self.flags {
            attrs.push(FetchAttr::Flags);
        }
        if self.envelope {
            attrs.push(FetchAttr::Envelope);
        }
        if self.size {
            attrs.push(FetchAttr::Rfc822Size);
        }
        if self.modseq {
            attrs.push(FetchAttr::ModSeq);
        }
        match self.body {
            BodySelection::None => {}
            BodySelection::Headers => attrs.push(FetchAttr::Rfc822Header),
            BodySelection::Preview(limit) => attrs.push(FetchAttr::BodySection {
                peek: true,
                section: None,
                partial: Some((
                    0,
                    u64::try_from(limit)
                        .unwrap_or(u64::MAX)
                        .max(PREVIEW_FETCH_BYTES),
                )),
            }),
            BodySelection::Whole => attrs.push(FetchAttr::BodySection {
                peek: true,
                section: None,
                partial: None,
            }),
        }
        attrs
    }

    pub(super) fn needs_body_budget(self) -> bool {
        matches!(self.body, BodySelection::Preview(_) | BodySelection::Whole)
    }
}

/// Decode the bytes selected from the shared FETCH surface. Both account
/// hydration APIs start here; one returns these bytes and the other parses
/// them through `bifrost_types::mime`.
pub(super) fn decode_body(fetch: &mut FetchResponse) -> Option<bytes::Bytes> {
    fetch
        .body_sections
        .iter_mut()
        .find_map(|section| section.data.take())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preview_is_one_shared_whole_message_prefix_policy() {
        let selection = FetchSelection {
            flags: false,
            envelope: false,
            size: false,
            modseq: false,
            body: BodySelection::Preview(64),
        };
        assert_eq!(
            selection.attributes(),
            vec![
                FetchAttr::Uid,
                FetchAttr::BodySection {
                    peek: true,
                    section: None,
                    partial: Some((0, PREVIEW_FETCH_BYTES)),
                }
            ]
        );
    }

    // The two hydration paths this module unified did NOT agree on the
    // metadata prefix, and that disagreement is deliberate: the generic
    // `get` projections ask for exactly what the projection names, while PIM
    // hydration always needs flags/envelope/size to populate a `Message`.
    // Folding them onto one builder is only safe if the flag combination each
    // caller passes still reproduces its old attribute list verbatim, order
    // included - the FETCH attribute order is what the encoder puts on the
    // wire.
    #[test]
    fn pim_hydration_keeps_its_mandatory_metadata_prefix() {
        let selection = FetchSelection {
            flags: true,
            envelope: true,
            size: true,
            modseq: false,
            body: BodySelection::None,
        };
        assert_eq!(
            selection.attributes(),
            vec![
                FetchAttr::Uid,
                FetchAttr::Flags,
                FetchAttr::Envelope,
                FetchAttr::Rfc822Size,
            ],
            "PIM `Headers` hydration reads headers out of ENVELOPE, not RFC822.HEADER"
        );
        assert!(!selection.needs_body_budget());
    }

    // The generic `Headers` projection is the opposite shape: no envelope, but
    // an actual RFC822.HEADER. It is also the arm where the body-budget
    // classification is easiest to get wrong, because the old code decided by
    // scanning for a `BodySection` attribute and RFC822.HEADER is not one -
    // even though the decoder normalises it INTO `body_sections`.
    #[test]
    fn generic_header_projection_asks_for_the_header_literal_without_metadata() {
        let selection = FetchSelection {
            flags: false,
            envelope: false,
            size: false,
            modseq: false,
            body: BodySelection::Headers,
        };
        assert_eq!(
            selection.attributes(),
            vec![FetchAttr::Uid, FetchAttr::Rfc822Header]
        );
    }

    // MODSEQ rides only on the two metadata-shaped generic projections; no
    // body-bearing projection ever asked for it, and PIM hydration never did.
    #[test]
    fn modseq_is_appended_after_metadata_and_before_the_body() {
        let selection = FetchSelection {
            flags: true,
            envelope: true,
            size: true,
            modseq: true,
            body: BodySelection::None,
        };
        assert_eq!(
            selection.attributes(),
            vec![
                FetchAttr::Uid,
                FetchAttr::Flags,
                FetchAttr::Envelope,
                FetchAttr::Rfc822Size,
                FetchAttr::ModSeq,
            ]
        );
    }

    // `Full` on the generic path carries RFC822.SIZE but no flags/envelope,
    // and a whole-message body with no partial range.
    #[test]
    fn whole_message_projection_has_no_partial_range() {
        let selection = FetchSelection {
            flags: false,
            envelope: false,
            size: true,
            modseq: false,
            body: BodySelection::Whole,
        };
        assert_eq!(
            selection.attributes(),
            vec![
                FetchAttr::Uid,
                FetchAttr::Rfc822Size,
                FetchAttr::BodySection {
                    peek: true,
                    section: None,
                    partial: None,
                },
            ]
        );
        assert!(selection.needs_body_budget());
    }

    // The preview floor is a maximum, not a clamp: a caller asking for more
    // than the floor gets what it asked for. The two old paths had already
    // drifted on exactly this policy once.
    #[test]
    fn a_preview_larger_than_the_floor_is_not_reduced_to_it() {
        let big = usize::try_from(PREVIEW_FETCH_BYTES).expect("floor fits usize") * 4;
        let selection = FetchSelection {
            flags: true,
            envelope: true,
            size: true,
            modseq: false,
            body: BodySelection::Preview(big),
        };
        let attrs = selection.attributes();
        let partial = attrs.iter().find_map(|attr| match attr {
            FetchAttr::BodySection { partial, .. } => *partial,
            _ => None,
        });
        assert_eq!(
            partial,
            Some((0, u64::try_from(big).expect("fits u64"))),
            "the floor only raises small requests"
        );
        assert!(selection.needs_body_budget());
    }

    // `decode_body` replaced two different extractors: one consumed
    // `body_sections` by value, the other borrowed the first section's data.
    // Both took the FIRST section carrying data, skipping any that carry none.
    #[test]
    fn decode_body_takes_the_first_section_that_actually_carries_data() {
        use crate::types::fetch::BodySection;

        let mut fetch = FetchResponse {
            body_sections: vec![
                BodySection {
                    section: "HEADER".to_owned(),
                    origin: None,
                    data: None,
                },
                BodySection {
                    section: String::new(),
                    origin: None,
                    data: Some(b"payload".to_vec().into()),
                },
                BodySection {
                    section: "TEXT".to_owned(),
                    origin: None,
                    data: Some(b"later".to_vec().into()),
                },
            ],
            ..FetchResponse::default()
        };
        assert_eq!(
            decode_body(&mut fetch).as_deref(),
            Some(b"payload".as_slice())
        );

        // Nothing to decode is `None`, not an empty body: the generic path
        // turns that into empty bytes while PIM skips MIME parsing entirely.
        let mut empty = FetchResponse::default();
        assert_eq!(decode_body(&mut empty), None);

        let mut all_absent = FetchResponse {
            body_sections: vec![BodySection {
                section: "HEADER".to_owned(),
                origin: None,
                data: None,
            }],
            ..FetchResponse::default()
        };
        assert_eq!(decode_body(&mut all_absent), None);
    }

    #[test]
    fn header_only_fetch_does_not_take_the_body_literal_budget_path() {
        let selection = FetchSelection {
            flags: false,
            envelope: false,
            size: false,
            modseq: false,
            body: BodySelection::Headers,
        };
        assert!(!selection.needs_body_budget());
        assert_eq!(
            selection.attributes(),
            vec![FetchAttr::Uid, FetchAttr::Rfc822Header]
        );
    }
}
