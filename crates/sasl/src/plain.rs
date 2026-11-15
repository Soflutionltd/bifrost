//! RFC 4616 PLAIN payload construction.

use zeroize::Zeroizing;

use crate::{SaslError, Secret};

/// Build raw `authzid NUL authcid NUL password` bytes in zeroizing storage.
///
/// NUL is a structural delimiter and cannot appear in any credential field.
/// Refuse it rather than allowing a caller to inject another identity field.
pub fn plain_payload(authzid: &str, authcid: &str, password: &str) -> Result<Secret, SaslError> {
    if [authzid, authcid, password]
        .iter()
        .any(|field| field.as_bytes().contains(&0))
    {
        return Err(SaslError::InvalidCredential(
            "PLAIN credential fields cannot contain NUL".into(),
        ));
    }
    let mut payload = Zeroizing::new(String::with_capacity(
        authzid.len() + authcid.len() + password.len() + 2,
    ));
    payload.push_str(authzid);
    payload.push('\0');
    payload.push_str(authcid);
    payload.push('\0');
    payload.push_str(password);
    Ok(Secret::from(payload))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn plain_payload_frames_fields_in_zeroizing_storage() {
        let payload: Secret = plain_payload("delegate", "user", "password").unwrap();
        let storage: Zeroizing<String> = payload.into_zeroizing();
        assert_eq!(storage.as_bytes(), b"delegate\0user\0password");
        assert_eq!(
            plain_payload("", "user", "pw").unwrap().as_bytes(),
            b"\0user\0pw"
        );
    }

    #[test]
    fn plain_payload_refuses_nul_in_every_field() {
        for fields in [
            ("a\0b", "user", "pw"),
            ("", "a\0b", "pw"),
            ("", "user", "a\0b"),
        ] {
            assert!(matches!(
                plain_payload(fields.0, fields.1, fields.2),
                Err(SaslError::InvalidCredential(_))
            ));
        }
    }
}
