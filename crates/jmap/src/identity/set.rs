use crate::core::field::Field;
use crate::email::EmailAddress;

use super::{IdentityCreate, IdentityPatch};

impl IdentityCreate {
    pub(crate) fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = Some(name.into());
        self
    }

    pub(crate) fn email(&mut self, email: impl Into<String>) -> &mut Self {
        self.email = Some(email.into());
        self
    }

    pub(crate) fn bcc<T, U>(&mut self, bcc: Option<T>) -> &mut Self
    where
        T: Iterator<Item = U>,
        U: Into<EmailAddress>,
    {
        self.bcc = Field::from(bcc.map(|s| s.map(std::convert::Into::into).collect()));
        self
    }

    pub(crate) fn reply_to<T, U>(&mut self, reply_to: Option<T>) -> &mut Self
    where
        T: Iterator<Item = U>,
        U: Into<EmailAddress>,
    {
        self.reply_to = Field::from(reply_to.map(|s| s.map(std::convert::Into::into).collect()));
        self
    }

    pub(crate) fn text_signature(&mut self, text_signature: impl Into<String>) -> &mut Self {
        self.text_signature = Some(text_signature.into());
        self
    }

    pub(crate) fn html_signature(&mut self, html_signature: impl Into<String>) -> &mut Self {
        self.html_signature = Some(html_signature.into());
        self
    }
}

impl IdentityPatch {
    // Clearing a recipient list in the mail UI must send an empty list, not
    // silently preserve the old recipients. None remains an explicit null.

    pub(crate) fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = Some(name.into());
        self
    }

    pub(crate) fn bcc<T, U>(&mut self, bcc: Option<T>) -> &mut Self
    where
        T: Iterator<Item = U>,
        U: Into<EmailAddress>,
    {
        self.bcc = Field::from(bcc.map(|s| s.map(std::convert::Into::into).collect::<Vec<_>>()));
        self
    }

    pub(crate) fn reply_to<T, U>(&mut self, reply_to: Option<T>) -> &mut Self
    where
        T: Iterator<Item = U>,
        U: Into<EmailAddress>,
    {
        self.reply_to =
            Field::from(reply_to.map(|s| s.map(std::convert::Into::into).collect::<Vec<_>>()));
        self
    }

    pub(crate) fn text_signature(&mut self, text_signature: impl Into<String>) -> &mut Self {
        self.text_signature = Some(text_signature.into());
        self
    }

    pub(crate) fn html_signature(&mut self, html_signature: impl Into<String>) -> &mut Self {
        self.html_signature = Some(html_signature.into());
        self
    }
}
