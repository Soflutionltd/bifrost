use crate::core::field::Field;
use jiff::Timestamp;

use super::{VacationResponseCreate, VacationResponsePatch};

macro_rules! vacation_setters {
    ($t:ty) => {
        impl $t {
            pub(crate) fn is_enabled(&mut self, is_enabled: bool) -> &mut Self {
                self.is_enabled = Some(is_enabled);
                self
            }

            pub(crate) fn from_date(&mut self, from_date: Option<Timestamp>) -> &mut Self {
                self.from_date = Field::from(from_date);
                self
            }

            pub(crate) fn to_date(&mut self, to_date: Option<Timestamp>) -> &mut Self {
                self.to_date = Field::from(to_date);
                self
            }

            pub(crate) fn subject(&mut self, subject: Option<impl Into<String>>) -> &mut Self {
                self.subject = Field::from(subject.map(std::convert::Into::into));
                self
            }

            pub(crate) fn text_body(&mut self, text_body: Option<impl Into<String>>) -> &mut Self {
                self.text_body = Field::from(text_body.map(std::convert::Into::into));
                self
            }

            pub(crate) fn html_body(&mut self, html_body: Option<impl Into<String>>) -> &mut Self {
                self.html_body = Field::from(html_body.map(std::convert::Into::into));
                self
            }
        }
    };
}

vacation_setters!(VacationResponseCreate);

impl VacationResponsePatch {
    pub(crate) fn is_enabled(&mut self, is_enabled: bool) -> &mut Self {
        self.is_enabled = Some(is_enabled);
        self
    }

    pub(crate) fn from_date(&mut self, from_date: Option<Timestamp>) -> &mut Self {
        self.from_date = match from_date {
            Some(from_date) => Field::Value(from_date),
            None => Field::Null,
        };
        self
    }

    pub(crate) fn to_date(&mut self, to_date: Option<Timestamp>) -> &mut Self {
        self.to_date = match to_date {
            Some(to_date) => Field::Value(to_date),
            None => Field::Null,
        };
        self
    }

    pub(crate) fn subject(&mut self, subject: Option<impl Into<String>>) -> &mut Self {
        self.subject = match subject {
            Some(subject) => Field::Value(subject.into()),
            None => Field::Null,
        };
        self
    }

    pub(crate) fn text_body(&mut self, text_body: Option<impl Into<String>>) -> &mut Self {
        self.text_body = match text_body {
            Some(text_body) => Field::Value(text_body.into()),
            None => Field::Null,
        };
        self
    }

    pub(crate) fn html_body(&mut self, html_body: Option<impl Into<String>>) -> &mut Self {
        self.html_body = match html_body {
            Some(html_body) => Field::Value(html_body.into()),
            None => Field::Null,
        };
        self
    }
}
