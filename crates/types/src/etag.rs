//! Entity validators shared by protocol adapters.

/// A validator classified once at its string boundary.
///
/// The original bytes are retained: Graph uses weak wire validators, while
/// Google People uses opaque JSON tokens rather than HTTP header syntax.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum EntityTag {
    Strong(StrongTag),
    Weak(String),
}

/// A validator without a weak marker. Header syntax is checked separately.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StrongTag(String);

impl StrongTag {
    #[must_use]
    pub fn to_header(&self) -> String {
        format!("\"{}\"", self.0.trim().trim_matches('"'))
    }
}

impl EntityTag {
    #[must_use]
    pub fn parse(value: &str) -> Self {
        if value
            .trim()
            .get(..2)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("W/"))
        {
            Self::Weak(value.to_owned())
        } else {
            Self::Strong(StrongTag(value.to_owned()))
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Strong(tag) => &tag.0,
            Self::Weak(value) => value,
        }
    }

    #[must_use]
    pub fn into_string(self) -> String {
        match self {
            Self::Strong(tag) => tag.0,
            Self::Weak(value) => value,
        }
    }

    /// DAV snapshot spelling, preserving the weak marker.
    #[must_use]
    pub fn normalized(self) -> String {
        match self {
            Self::Strong(tag) => tag.0.trim().trim_matches('"').to_owned(),
            Self::Weak(value) => {
                let value = value.trim();
                let value = value.get(2..).unwrap_or_default().trim();
                format!("W/{value}")
            }
        }
    }

    /// Admit an HTTP caller token. DAV additionally accepts unquoted strong
    /// tokens; Graph and Calendar require the quoted wire spelling.
    #[must_use]
    pub fn valid_http(&self, strong: bool) -> bool {
        let value = self.as_str();
        let content = match self {
            Self::Weak(_) if strong => return false,
            Self::Weak(_) => match value.strip_prefix("W/") {
                Some(value) => value,
                None => return false,
            },
            Self::Strong(_) => value,
        };
        let valid = |value: &str| {
            value
                .bytes()
                .all(|byte| byte == 0x21 || (0x23..=0x7e).contains(&byte) || byte >= 0x80)
        };
        if strong && valid(content) {
            return true;
        }
        content
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .is_some_and(valid)
    }
}

impl From<String> for EntityTag {
    fn from(value: String) -> Self {
        Self::parse(&value)
    }
}

impl From<&str> for EntityTag {
    fn from(value: &str) -> Self {
        Self::parse(value)
    }
}

impl std::ops::Deref for EntityTag {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::EntityTag;

    #[test]
    fn classification_preserves_provider_token_bytes() {
        for value in ["\"strong\"", "W/\"weak\"", "w/\"weak\"", "opaque-source"] {
            assert_eq!(EntityTag::parse(value).into_string(), value);
        }
        assert!(matches!(EntityTag::parse("w/\"weak\""), EntityTag::Weak(_)));
        assert_eq!(EntityTag::parse("w/\"weak\"").normalized(), "W/\"weak\"");
        assert_eq!(EntityTag::parse("\"strong\"").normalized(), "strong");
    }

    #[test]
    fn http_admission_keeps_provider_precondition_rules() {
        assert!(EntityTag::parse("normalized").valid_http(true));
        assert!(!EntityTag::parse("normalized").valid_http(false));
        assert!(EntityTag::parse("W/\"weak\"").valid_http(false));
        assert!(!EntityTag::parse("W/\"weak\"").valid_http(true));
        assert!(!EntityTag::parse("w/\"weak\"").valid_http(true));
        assert!(!EntityTag::parse("bad\r\nvalue").valid_http(true));
    }
}
