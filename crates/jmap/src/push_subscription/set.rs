use crate::{DataType, core::field::Field};

use super::{Keys, PushSubscriptionCreate, PushSubscriptionPatch};

impl PushSubscriptionCreate {
    pub(crate) fn device_client_id(&mut self, device_client_id: impl Into<String>) -> &mut Self {
        self.device_client_id = Some(device_client_id.into());
        self
    }

    pub(crate) fn url(&mut self, url: impl Into<String>) -> &mut Self {
        self.url = Some(url.into());
        self
    }

    pub(crate) fn verification_code(&mut self, verification_code: impl Into<String>) -> &mut Self {
        self.verification_code = Some(verification_code.into());
        self
    }

    pub(crate) fn keys(&mut self, keys: Keys) -> &mut Self {
        self.keys = Some(keys);
        self
    }

    pub(crate) fn expires(&mut self, expires: jiff::Timestamp) -> &mut Self {
        self.expires = Some(expires);
        self
    }

    pub(crate) fn types(&mut self, types: Option<impl IntoIterator<Item = DataType>>) -> &mut Self {
        self.types = Field::from(types.map(|types| types.into_iter().collect()));
        self
    }
}

impl PushSubscriptionPatch {
    pub(crate) fn verification_code(&mut self, verification_code: impl Into<String>) -> &mut Self {
        self.verification_code = Some(verification_code.into());
        self
    }

    pub(crate) fn expires(&mut self, expires: jiff::Timestamp) -> &mut Self {
        self.expires = Some(expires);
        self
    }

    pub(crate) fn types(&mut self, types: Option<impl IntoIterator<Item = DataType>>) -> &mut Self {
        self.types = match types {
            Some(types) => Field::Value(types.into_iter().collect()),
            None => Field::Null,
        };
        self
    }
}

impl Keys {
    pub(crate) fn new(p256dh: &[u8], auth: &[u8]) -> Self {
        use base64::{Engine, engine::general_purpose::URL_SAFE};
        Keys {
            p256dh: URL_SAFE.encode(p256dh),
            auth: URL_SAFE.encode(auth),
        }
    }
}
