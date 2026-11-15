use serde::{Deserialize, Serialize};

/// RFC 8621 keyword identity is ASCII case insensitive. Store its canonical
/// spelling so set membership, patches and query operands agree.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub(crate) struct Keyword(String);

impl Keyword {
    pub(crate) fn parse(value: impl AsRef<str>) -> crate::Result<Self> {
        let value = value.as_ref();
        if value.is_empty()
            || value.len() > 255
            || value
                .bytes()
                .any(|b| !matches!(b, 0x21..=0x7e) || b"(){%*\"\\]".contains(&b))
        {
            return Err(crate::Error::RequestEncode(serde::ser::Error::custom(
                "JMAP keyword must be an IMAP atom of 1 to 255 ASCII octets",
            )));
        }
        Ok(Self(value.to_ascii_lowercase()))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for Keyword {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::parse(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::Keyword;

    #[test]
    fn grammar_and_canonical_identity_share_one_boundary() {
        assert_eq!(
            Keyword::parse("$Seen").unwrap(),
            Keyword::parse("$seen").unwrap()
        );
        assert!(Keyword::parse("a".repeat(255)).is_ok());
        for invalid in [
            String::new(),
            "a".repeat(256),
            "two words".into(),
            "a]b".into(),
            "a\\b".into(),
            "a\nb".into(),
        ] {
            assert!(Keyword::parse(&invalid).is_err(), "{invalid:?}");
        }
        assert_eq!(
            Keyword::parse("work/urgent~tag}").unwrap().as_str(),
            "work/urgent~tag}"
        );
    }
}
