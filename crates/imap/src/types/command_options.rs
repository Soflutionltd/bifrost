//! Typed option vocabularies for SEARCH RETURN and LIST-EXTENDED.
//!
//! Unknown extension syntax is retained and checked by the encoder. Known
//! spellings always become their named variant, including mixed-case input.

macro_rules! option_type {
    ($name:ident { $($variant:ident => $wire:literal),+ $(,)? }) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub(crate) enum $name {
            $($variant,)+
            Extension(ExtensionOption),
        }
        impl $name {
            pub(crate) fn as_str(&self) -> &str {
                match self {
                    $(Self::$variant => $wire,)+
                    Self::Extension(value) => &value.0,
                }
            }
        }
        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                if value.contains(['\r', '\n']) { return Self::Extension(ExtensionOption(value.to_owned())); }
                $(if value.trim().eq_ignore_ascii_case($wire) { return Self::$variant; })+
                Self::Extension(ExtensionOption(value.to_owned()))
            }
        }
        impl From<String> for $name {
            fn from(value: String) -> Self { value.as_str().into() }
        }
        impl std::ops::Deref for $name {
            type Target = str;
            fn deref(&self) -> &str { self.as_str() }
        }
    };
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ExtensionOption(String);

option_type!(SearchReturn {
    Min => "MIN", Max => "MAX", All => "ALL", Count => "COUNT", Save => "SAVE"
});
option_type!(ListSelect {
    Subscribed => "SUBSCRIBED", Remote => "REMOTE", RecursiveMatch => "RECURSIVEMATCH",
    SpecialUse => "SPECIAL-USE"
});
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum ListReturn {
    Subscribed,
    Children,
    SpecialUse,
    Status(StatusReturn),
    Extension(ExtensionOption),
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct StatusReturn {
    items: StatusItems,
    text: String,
}
impl ListReturn {
    pub(crate) fn as_str(&self) -> &str {
        match self {
            Self::Subscribed => "SUBSCRIBED",
            Self::Children => "CHILDREN",
            Self::SpecialUse => "SPECIAL-USE",
            Self::Status(value) => &value.text,
            Self::Extension(value) => &value.0,
        }
    }
    pub(crate) fn status_items(&self) -> Option<&StatusItems> {
        match self {
            Self::Status(value) => Some(&value.items),
            _ => None,
        }
    }
}
impl From<&str> for ListReturn {
    fn from(text: &str) -> Self {
        if !text.contains(['\r', '\n']) {
            match text.trim().to_ascii_uppercase().as_str() {
                "SUBSCRIBED" => return Self::Subscribed,
                "CHILDREN" => return Self::Children,
                "SPECIAL-USE" => return Self::SpecialUse,
                _ => {}
            }
            if let Some(Ok(items)) = list_status_return_option_items(text.trim())
                && status_item_tokens(items).is_ok()
            {
                return Self::Status(StatusReturn {
                    items: items.into(),
                    text: text.to_owned(),
                });
            }
        }
        Self::Extension(ExtensionOption(text.to_owned()))
    }
}
impl From<String> for ListReturn {
    fn from(text: String) -> Self {
        text.as_str().into()
    }
}
impl std::ops::Deref for ListReturn {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}

option_type!(StatusAttribute {
    Messages => "MESSAGES", Recent => "RECENT", UidNext => "UIDNEXT",
    UidValidity => "UIDVALIDITY", Unseen => "UNSEEN", Deleted => "DELETED",
    DeletedStorage => "DELETED-STORAGE", Size => "SIZE", HighestModSeq => "HIGHESTMODSEQ",
    AppendLimit => "APPENDLIMIT", MailboxId => "MAILBOXID"
});

/// A status request retains its parsed attributes; malformed raw input is kept
/// only so the encoder can report the same fallible command-construction error.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum StatusItems {
    Attributes(Vec<StatusAttribute>),
    Raw(String),
}
impl StatusItems {
    pub(crate) fn attributes(&self) -> Result<Vec<StatusAttribute>, crate::Error> {
        match self {
            Self::Attributes(items) => Ok(items.clone()),
            Self::Raw(text) => Ok(status_item_tokens(text)?
                .into_iter()
                .map(Into::into)
                .collect()),
        }
    }
    pub(crate) fn body(&self, context: &str) -> Result<String, crate::Error> {
        if let Self::Raw(text) = self
            && text.contains(['\r', '\n'])
        {
            return Err(crate::Error::LocalRefusal(
                crate::error::LocalRefusal::InvalidInput(
                    format!("{context} must not contain CR or LF").into(),
                ),
            ));
        }
        let items = self.attributes()?;
        if items.is_empty() {
            return Err(crate::Error::LocalRefusal(
                crate::error::LocalRefusal::InvalidInput(
                    format!("{context} must contain at least one status data item").into(),
                ),
            ));
        }
        for item in &items {
            super::validated::validate_atom_bytes(item.as_str().as_bytes(), context)?;
        }
        Ok(items
            .iter()
            .map(StatusAttribute::as_str)
            .collect::<Vec<_>>()
            .join(" "))
    }
}
impl From<String> for StatusItems {
    fn from(text: String) -> Self {
        if text.contains(['\r', '\n']) {
            return Self::Raw(text);
        }
        match status_item_tokens(&text) {
            Ok(items) => Self::Attributes(items.into_iter().map(Into::into).collect()),
            Err(_) => Self::Raw(text),
        }
    }
}
impl From<&str> for StatusItems {
    fn from(text: &str) -> Self {
        text.to_owned().into()
    }
}
impl From<Vec<StatusAttribute>> for StatusItems {
    fn from(items: Vec<StatusAttribute>) -> Self {
        Self::Attributes(items)
    }
}

pub(crate) fn status_item_tokens(items: &str) -> Result<Vec<&str>, crate::Error> {
    let trimmed = items.trim();
    if trimmed.is_empty() {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(
                "STATUS item list must contain at least one data item \
                 (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
                    .into(),
            ),
        ));
    }

    let body = match (trimmed.strip_prefix('('), trimmed.strip_suffix(')')) {
        (Some(without_open), Some(_)) => {
            let inner = without_open
                .strip_suffix(')')
                .ok_or_else(|| {
                    crate::Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(
                        "STATUS item list must use balanced parentheses \
                             (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
                            .into(),
                    ))
                })?
                .trim();
            if inner.is_empty() {
                return Err(crate::Error::LocalRefusal(
                    crate::error::LocalRefusal::InvalidInput(
                        "STATUS item list must contain at least one data item \
                         (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
                            .into(),
                    ),
                ));
            }
            inner
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(crate::Error::LocalRefusal(
                crate::error::LocalRefusal::InvalidInput(
                    "STATUS item list must use balanced parentheses \
                     (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
                        .into(),
                ),
            ));
        }
        (None, None) => trimmed,
    };

    if body.contains('(') || body.contains(')') {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(
                "STATUS item list must be flat and must not contain nested parentheses \
                 (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
                    .into(),
            ),
        ));
    }

    let tokens: Vec<&str> = body.split_ascii_whitespace().collect();
    if tokens.is_empty() {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(
                "STATUS item list must contain at least one data item \
                 (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
                    .into(),
            ),
        ));
    }
    Ok(tokens)
}

pub(crate) fn list_status_return_option_items(option: &str) -> Option<Result<&str, crate::Error>> {
    if !option
        .get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("STATUS"))
    {
        return None;
    }

    match option.as_bytes().get(6).copied() {
        Some(next) if next != b' ' && !next.is_ascii_whitespace() && next != b'(' => return None,
        _ => {}
    }

    Some(if let Some(suffix) = option[6..].strip_prefix(" (") {
        if let Some(items) = suffix.strip_suffix(')') {
            Ok(items)
        } else {
            Err(crate::Error::LocalRefusal(
                crate::error::LocalRefusal::InvalidInput(
                    "LIST-EXTENDED STATUS return option must be STATUS (<items>) \
                 per RFC 5819 Section 4 / RFC 9051 Section 7"
                        .into(),
                ),
            ))
        }
    } else {
        Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(
                "LIST-EXTENDED STATUS return option must be STATUS (<items>) \
             per RFC 5819 Section 4 / RFC 9051 Section 7"
                    .into(),
            ),
        ))
    })
}
