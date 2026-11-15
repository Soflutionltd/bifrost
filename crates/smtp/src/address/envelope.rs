use super::Address;
use crate::Error;
use crate::message::header::Headers;

/// Simple email envelope representation
///
/// We only accept mailboxes, and do not support source routes (as per RFC).
#[derive(PartialEq, Eq, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Envelope {
    /// The envelope recipient's addresses
    ///
    /// This can not be empty.
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "serde_forward_path::deserialize")
    )]
    forward_path: Vec<Address>,
    /// The envelope sender address
    reverse_path: Option<Address>,
}

/// just like the default implementation to deserialize `Vec<Address>` but it
/// forbids **de**serializing empty lists
#[cfg(feature = "serde")]
mod serde_forward_path {
    use super::Address;
    /// dummy type required for serde
    /// see example: <https://serde.rs/deserialize-map.html>
    struct CustomVisitor;
    impl<'de> serde::de::Visitor<'de> for CustomVisitor {
        type Value = Vec<Address>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a non-empty list of recipient addresses")
        }

        fn visit_seq<S>(self, mut access: S) -> Result<Self::Value, S::Error>
        where
            S: serde::de::SeqAccess<'de>,
        {
            let mut seq: Vec<Address> = Vec::with_capacity(access.size_hint().unwrap_or(0));
            while let Some(key) = access.next_element()? {
                seq.push(key);
            }
            if seq.is_empty() {
                Err(serde::de::Error::invalid_length(seq.len(), &self))
            } else {
                Ok(seq)
            }
        }
    }
    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<Address>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(CustomVisitor {})
    }

    #[cfg(test)]
    mod tests {
        #[test]
        fn deserializing_empty_recipient_list_returns_error() {
            assert!(
                serde_json::from_str::<crate::address::Envelope>(r#"{"forward_path": []}"#)
                    .is_err()
            );
        }
        #[test]
        fn deserializing_non_empty_recipient_list_is_ok() {
            serde_json::from_str::<crate::address::Envelope>(
                r#"{ "forward_path": [ {"user":"foo", "domain":"example.com"} ] }"#,
            )
            .unwrap();
        }
    }
}

impl Envelope {
    /// Creates a new envelope, which may fail if `to` is empty.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use bifrost_smtp::address::{Address, Envelope};
    /// # use std::error::Error;
    /// # fn main() -> Result<(), Box<dyn Error>> {
    /// let sender = "sender@email.com".parse::<Address>()?;
    /// let recipients = vec!["to@email.com".parse::<Address>()?];
    ///
    /// let envelope = Envelope::new(Some(sender), recipients);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// If `to` has no elements in it.
    pub fn new(from: Option<Address>, to: Vec<Address>) -> Result<Envelope, Error> {
        if to.is_empty() {
            return Err(Error::MissingTo);
        }
        Ok(Envelope {
            forward_path: to,
            reverse_path: from,
        })
    }

    /// Gets the destination addresses of the envelope.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use bifrost_smtp::address::{Address, Envelope};
    /// # use std::error::Error;
    /// # fn main() -> Result<(), Box<dyn Error>> {
    /// let sender = "from@email.com".parse::<Address>()?;
    /// let recipients = vec!["to@email.com".parse::<Address>()?];
    ///
    /// let envelope = Envelope::new(Some(sender), recipients.clone())?;
    /// assert_eq!(envelope.to(), recipients.as_slice());
    /// # Ok(())
    /// # }
    /// ```
    pub fn to(&self) -> &[Address] {
        self.forward_path.as_slice()
    }

    /// Gets the sender of the envelope.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use bifrost_smtp::address::{Address, Envelope};
    /// # use std::error::Error;
    /// # fn main() -> Result<(), Box<dyn Error>> {
    /// let sender = "from@email.com".parse::<Address>()?;
    /// let recipients = vec!["to@email.com".parse::<Address>()?];
    ///
    /// let envelope = Envelope::new(Some(sender), recipients.clone())?;
    /// assert!(envelope.from().is_some());
    ///
    /// let senderless = Envelope::new(None, recipients.clone())?;
    /// assert!(senderless.from().is_none());
    /// # Ok(())
    /// # }
    /// ```
    pub fn from(&self) -> Option<&Address> {
        self.reverse_path.as_ref()
    }

    /// Check if any of the addresses in the envelope contains non-ascii chars
    pub(crate) fn has_non_ascii_addresses(&self) -> bool {
        Self::addresses_need_smtputf8(self.reverse_path.iter().chain(self.forward_path.iter()))
    }

    pub(crate) fn addresses_need_smtputf8<'a>(
        mut addresses: impl Iterator<Item = &'a Address>,
    ) -> bool {
        addresses.any(|address| !address.is_ascii())
    }
}

impl TryFrom<&Headers> for Envelope {
    type Error = Error;

    fn try_from(headers: &Headers) -> Result<Self, Self::Error> {
        // The reverse-path rule lives in bifrost-types so SMTP and IMAP draft
        // submission agree. A single From wins over Sender (a delegate sending
        // for a shared mailbox expects bounces at the mailbox); this reverses
        // the inherited lettre Sender-first rule on purpose.
        let raw = headers.to_string();
        let headers = bifrost_types::mime::HeaderMap::parse(raw.as_bytes());
        let (from, recipients) = headers
            .submission_envelope()
            .map_err(|detail| Error::InvalidInput(detail.to_owned()))?;
        let from = from
            .map(|address| address.address.parse())
            .transpose()
            .map_err(|error: crate::address::AddressError| {
                Error::InvalidInput(error.to_string())
            })?;
        let to = recipients
            .into_iter()
            .map(|address| address.address.parse())
            .collect::<Result<Vec<Address>, _>>()
            .map_err(|error| Error::InvalidInput(error.to_string()))?;
        Self::new(from, to)
    }
}
