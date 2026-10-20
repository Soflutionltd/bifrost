//! QUOTA and ACL command encoders.

use super::{CommandWriter, validate_atom};

/// Encode SETQUOTA command (RFC 2087 Section 4.1).
///
/// Format: `SETQUOTA "<root>" (<resource> <limit> ...)`.
/// RFC 2087 Section 4.1:
/// `setquota = "SETQUOTA" SP astring SP setquota_list`
/// `setquota_list = "(" 0#setquota_resource ")"`
/// `setquota_resource = atom SP number`
///
/// `number` is defined as `1*DIGIT` in RFC 3501 Section 9, constrained to u32.
/// Returns an error if any resource name is not a valid atom or any limit exceeds `u32::MAX`.
pub(in crate::codec::encode) fn encode_set_quota(
    w: &mut CommandWriter,
    tag: &str,
    root: &str,
    resources: &[(String, u64)],
    utf8: bool,
) -> Result<(), crate::Error> {
    // RFC 2087 Section 4.1: setquota_resource = atom SP number
    // RFC 3501 Section 9: number = 1*DIGIT (u32 range)
    for (resource, limit) in resources {
        // RFC 2087 Section 4.1: resource name must be an atom
        validate_atom(resource, "SETQUOTA resource name")?;
        if *limit > u64::from(u32::MAX) {
            return Err(crate::Error::InvalidInput(format!(
                "SETQUOTA resource limit {limit} for \"{resource}\" exceeds u32::MAX \
                 (RFC 2087 Section 4.1: number is constrained to 32 bits per RFC 3501 Section 9)"
            )));
        }
    }
    w.raw(tag.as_bytes());
    w.raw(b" SETQUOTA ");
    // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
    w.string(root.as_bytes(), utf8);
    w.raw(b" (");
    for (i, (resource, limit)) in resources.iter().enumerate() {
        if i > 0 {
            w.raw(b" ");
        }
        w.raw(resource.as_bytes());
        w.raw(b" ");
        w.raw(limit.to_string().as_bytes());
    }
    w.raw(b")\r\n");
    Ok(())
}

/// Encode SETACL command (RFC 4314 Section 3.1).
///
/// Format: `SETACL <mailbox> <identifier> <rights>`.
pub(in crate::codec::encode) fn encode_set_acl(
    w: &mut CommandWriter,
    tag: &str,
    mailbox: impl AsRef<[u8]>,
    identifier: &str,
    rights: &str,
    utf8: bool,
) {
    w.raw(tag.as_bytes());
    w.raw(b" SETACL ");
    // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
    w.string(mailbox.as_ref(), utf8);
    w.raw(b" ");
    w.string(identifier.as_bytes(), utf8);
    w.raw(b" ");
    w.string(rights.as_bytes(), utf8);
    w.raw(b"\r\n");
}
