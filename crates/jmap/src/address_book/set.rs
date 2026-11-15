use std::collections::HashMap;

use crate::core::field::Field;

use super::{AddressBookCreate, AddressBookPatch, AddressBookRights};

impl AddressBookCreate {
    pub(crate) fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = Field::from(Some(name.into()));
        self
    }

    pub(crate) fn description(&mut self, description: Option<impl Into<String>>) -> &mut Self {
        self.description = match description {
            Some(d) => Field::Value(d.into()),
            None => Field::Null,
        };
        self
    }

    pub(crate) fn sort_order(&mut self, sort_order: u32) -> &mut Self {
        self.sort_order = Some(sort_order);
        self
    }

    pub(crate) fn is_subscribed(&mut self, is_subscribed: bool) -> &mut Self {
        self.is_subscribed = Some(is_subscribed);
        self
    }

    pub(crate) fn share_with(
        &mut self,
        share_with: Option<HashMap<String, AddressBookRights>>,
    ) -> &mut Self {
        self.share_with = match share_with {
            Some(sw) => Field::Value(sw),
            None => Field::Null,
        };
        self
    }
}

impl AddressBookPatch {
    pub(crate) fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = Some(name.into());
        self
    }

    pub(crate) fn description(&mut self, description: Option<impl Into<String>>) -> &mut Self {
        self.description = match description {
            Some(d) => Field::Value(d.into()),
            None => Field::Null,
        };
        self
    }

    pub(crate) fn sort_order(&mut self, sort_order: u32) -> &mut Self {
        self.sort_order = Some(sort_order);
        self
    }

    pub(crate) fn is_subscribed(&mut self, is_subscribed: bool) -> &mut Self {
        self.is_subscribed = Some(is_subscribed);
        self
    }

    pub(crate) fn share_with(
        &mut self,
        share_with: Option<HashMap<String, AddressBookRights>>,
    ) -> &mut Self {
        self.share_with = match share_with {
            Some(sw) => Field::Value(sw),
            None => Field::Null,
        };
        self
    }
}
