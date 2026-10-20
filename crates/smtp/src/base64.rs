use ::base64::{
    DecodeError,
    engine::{Engine, general_purpose::STANDARD},
};
use zeroize::Zeroizing;

#[cfg(any(feature = "dkim", test))]
pub(crate) fn encode<T: AsRef<[u8]>>(input: T) -> String {
    STANDARD.encode(input)
}

pub(crate) fn encode_zeroizing<T: AsRef<[u8]>>(input: T) -> Zeroizing<String> {
    Zeroizing::new(STANDARD.encode(input))
}

pub(crate) fn decode<T: AsRef<[u8]>>(input: T) -> Result<Vec<u8>, DecodeError> {
    STANDARD.decode(input)
}
