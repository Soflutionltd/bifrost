//! Graph message flag vocabulary. Category-door sentinels are interpreted only
//! at that door; a literal category named "read" must remain a category.

pub(crate) const READ: &str = "\\seen";
pub(crate) const FLAGGED: &str = "\\flagged";
pub(crate) const CATEGORY_PREFIX: &str = "category:";
pub(crate) const STARRED_CATEGORY: &str = "$flagged";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GraphFlag<'a> {
    Read,
    Flagged,
    Category(&'a str),
}

impl<'a> GraphFlag<'a> {
    pub(crate) fn parse(token: &'a str) -> Option<Self> {
        if token.eq_ignore_ascii_case(READ) || token.eq_ignore_ascii_case("read") {
            Some(Self::Read)
        } else if token.eq_ignore_ascii_case(FLAGGED)
            || token.eq_ignore_ascii_case("flagged")
            || token.eq_ignore_ascii_case("starred")
        {
            Some(Self::Flagged)
        } else {
            // Shared flag identity folds ASCII case. An Outlook user expects
            // equivalent spellings to make the same edit, independent of which
            // spelling a set retained. Preserve the category's display name.
            token
                .get(..CATEGORY_PREFIX.len())
                .filter(|prefix| prefix.eq_ignore_ascii_case(CATEGORY_PREFIX))
                .map(|_| Self::Category(&token[CATEGORY_PREFIX.len()..]))
        }
    }

    pub(crate) fn token(self) -> String {
        match self {
            Self::Read => READ.to_string(),
            Self::Flagged => FLAGGED.to_string(),
            Self::Category(name) => format!("{CATEGORY_PREFIX}{name}"),
        }
    }

    pub(crate) fn is_starred_category(category: &'a str) -> bool {
        // Outlook category names are literal, except for the existing star
        // sentinel. Sharing aliases must not turn the category "read" into a
        // read-state write or extend the bulk flag API with a new sentinel.
        category.eq_ignore_ascii_case(STARRED_CATEGORY)
            || matches!(Self::parse(category), Some(Self::Flagged))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flag_and_category_doors_share_aliases_without_sharing_sentinels() {
        for alias in ["\\Flagged", "FLAGGED", "starred"] {
            assert_eq!(GraphFlag::parse(alias), Some(GraphFlag::Flagged));
            assert!(GraphFlag::is_starred_category(alias));
        }
        assert!(GraphFlag::is_starred_category("$FLAGGED"));
        assert_eq!(GraphFlag::parse("$flagged"), None);
        assert!(!GraphFlag::is_starred_category("read"));
        assert_eq!(GraphFlag::parse("READ"), Some(GraphFlag::Read));
        assert_eq!(
            GraphFlag::parse("Category:Work"),
            Some(GraphFlag::Category("Work"))
        );
        assert_eq!(
            GraphFlag::parse("category:Work").unwrap().token(),
            "category:Work"
        );
        assert_eq!(GraphFlag::Read.token(), "\\seen");
        assert_eq!(GraphFlag::Flagged.token(), "\\flagged");
    }
}
