//! Distinct identities for the transport's independent registry domains.

fn encode_key(namespace: &str, components: &[&str]) -> String {
    use std::fmt::Write;

    let mut key = format!("{}:{namespace}", namespace.len());
    for component in components {
        write!(key, "{}:{component}", component.len()).expect("writing a String cannot fail");
    }
    key
}

macro_rules! registry_key {
    ($name:ident) => {
        /// Opaque registry identity. Use `namespaced` for provider-owned keys.
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            /// Encode components with byte lengths, so delimiters in an account
            /// id or origin cannot alias a different component sequence.
            #[must_use]
            pub fn namespaced(namespace: &str, components: &[&str]) -> Self {
                Self(encode_key(namespace, components))
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        // String admission preserves caller-defined identities. The registries
        // store distinct types; provider code should mint namespaced identities.
        impl From<String> for $name {
            fn from(key: String) -> Self {
                Self(key)
            }
        }
        impl From<&str> for $name {
            fn from(key: &str) -> Self {
                Self(key.to_owned())
            }
        }
        impl From<&String> for $name {
            fn from(key: &String) -> Self {
                Self(key.clone())
            }
        }
        impl From<&$name> for $name {
            fn from(key: &$name) -> Self {
                key.clone()
            }
        }
        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

registry_key!(HoldKey);
registry_key!(LimitKey);

/// Rate bucket scope, independent of the host that owns the bucket.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum QuotaScope {
    /// Share the host-wide compatibility bucket.
    HostOnly,
    /// Caller-defined identity; empty names normalize to HostOnly at lookup.
    Named(String),
}

impl QuotaScope {
    /// Mint a provider scope without delimiter or Debug-format dependencies.
    #[must_use]
    pub fn namespaced(namespace: &str, components: &[&str]) -> Self {
        Self::Named(encode_key(namespace, components))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::HostOnly => "",
            Self::Named(scope) => scope,
        }
    }
}

impl From<String> for QuotaScope {
    fn from(scope: String) -> Self {
        if scope.is_empty() {
            Self::HostOnly
        } else {
            Self::Named(scope)
        }
    }
}

impl From<&str> for QuotaScope {
    fn from(scope: &str) -> Self {
        Self::from(scope.to_owned())
    }
}

impl From<&String> for QuotaScope {
    fn from(scope: &String) -> Self {
        Self::from(scope.clone())
    }
}

impl From<&QuotaScope> for QuotaScope {
    fn from(scope: &QuotaScope) -> Self {
        scope.clone()
    }
}

impl std::fmt::Display for QuotaScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
