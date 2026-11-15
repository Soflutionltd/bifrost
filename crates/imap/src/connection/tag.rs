//! Per-connection tag generator with random prefix and wrap regeneration.
//!
//! RFC 3501 Section2.2.1 defines command tags as alphanumeric strings chosen
//! by the client. This generator produces tags of the form
//! `{prefix:08x}{counter:08x}`  -  a random 32-bit hex prefix concatenated
//! with a monotonically increasing 32-bit hex counter. The prefix is
//! regenerated via `getrandom` when the counter wraps past `u32::MAX`,
//! making tag collisions across reconnects and across counter cycles
//! statistically impossible (invariant I12).
//!
//! # Invariant I12
//!
//! > Tag collisions are statistically impossible across reconnects and
//! > across `u32` counter wrap. `next` uses a per-connection random
//! > 32-bit prefix + a `u32` counter. Counter wrap regenerates the
//! > prefix. Prefix is initialized via `getrandom`.

// Keep the wire spelling as the correlation identity. Parsed response tags
// and ESEARCH correlators are arbitrary IMAP strings, not necessarily our
// generator's hex format; decoding them as two integers would also equate
// distinct spellings (for example uppercase and lowercase hex). A generated
// tag type would still need exact string comparisons at these boundaries.
/// Generates unique IMAP command tags with a random prefix per
/// connection (RFC 3501 Section2.2.1, invariant I12).
pub(crate) struct TagGenerator {
    prefix: u32,
    counter: u32,
}

impl TagGenerator {
    /// Create a new generator with a random prefix seeded from
    /// `getrandom`.
    #[allow(clippy::expect_used)] // getrandom failure = no OS entropy source; unrecoverable.
    pub(crate) fn new() -> Self {
        let mut bytes = [0u8; 4];
        getrandom::fill(&mut bytes).expect("getrandom failed  -  no OS entropy source");
        let prefix = u32::from_le_bytes(bytes);
        Self { prefix, counter: 0 }
    }

    /// Return the next tag and advance the counter.
    ///
    /// When the counter reaches `u32::MAX`, the prefix is regenerated
    /// from fresh entropy and the counter resets to 0, so the new cycle
    /// cannot collide with tags from the previous cycle.
    #[allow(clippy::expect_used)] // getrandom failure = no OS entropy source; unrecoverable.
    pub(crate) fn next(&mut self) -> String {
        if self.counter == u32::MAX {
            let mut bytes = [0u8; 4];
            getrandom::fill(&mut bytes).expect("getrandom failed  -  no OS entropy source");
            self.prefix = u32::from_le_bytes(bytes);
            self.counter = 0;
        } else {
            self.counter = self.counter.wrapping_add(1);
        }
        format!("{:08x}{:08x}", self.prefix, self.counter)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// Invariant I12  -  counter wrap regenerates the prefix so that the
    /// new tag cycle cannot collide with the previous one.
    #[test]
    fn invariant_i12_tag_wrap_regenerates_prefix() {
        let mut generator = TagGenerator::new();
        let original_prefix = generator.prefix;
        // One fresh prefix can equal the original by chance (1 in 2^32), so
        // wrap repeatedly: a generator that never regenerates stays equal on
        // every wrap, while a working one differs on some wrap with
        // overwhelming (1 - 2^-128) probability.
        let mut regenerated = false;
        for _ in 0..4 {
            generator.counter = u32::MAX;
            let _ = generator.next();
            assert_eq!(generator.counter, 0);
            if generator.prefix != original_prefix {
                regenerated = true;
                break;
            }
        }
        assert!(regenerated, "wrap did not regenerate prefix");
    }

    /// Tags are formatted as 16 hex characters: 8 for prefix, 8 for counter.
    #[test]
    fn tag_format_is_hex() {
        let mut generator = TagGenerator::new();
        let tag = generator.next();
        assert_eq!(tag.len(), 16, "tag should be 16 hex chars");
        assert!(
            tag.chars().all(|c| c.is_ascii_hexdigit()),
            "tag should only contain hex digits, got: {tag}"
        );
    }

    /// Sequential tags share the same prefix and have incrementing counters.
    #[test]
    fn tags_are_sequential() {
        let mut generator = TagGenerator::new();
        let tag1 = generator.next();
        let tag2 = generator.next();
        // Same prefix (first 8 chars).
        assert_eq!(
            &tag1[..8],
            &tag2[..8],
            "prefix should not change between sequential tags"
        );
        // Counter increments by 1.
        let c1 = u32::from_str_radix(&tag1[8..], 16).unwrap();
        let c2 = u32::from_str_radix(&tag2[8..], 16).unwrap();
        assert_eq!(c2, c1 + 1, "counter should increment by 1");
    }

    /// First tag after construction has counter = 1 (counter starts at 0,
    /// incremented before formatting).
    #[test]
    fn first_tag_counter_is_one() {
        let mut generator = TagGenerator::new();
        let tag = generator.next();
        let counter = u32::from_str_radix(&tag[8..], 16).unwrap();
        assert_eq!(counter, 1);
    }
}
