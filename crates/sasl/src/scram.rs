//! Pure SCRAM (RFC 5802 / RFC 7677) computation.
//!
//! No I/O, no protocol command flow. The protocol crates drive the `+`
//! continuation sequencing and call these transition functions.

use crate::error::SaslError;
use crate::secret::Secret;
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

/// Upper bound on the SCRAM `i=` iteration count fed to PBKDF2. RFC 7677 sets
/// the floor at 4096; this ceiling sits far above any legitimate deployment
/// (real-world values run 4k-600k) while keeping a single PBKDF2 run bounded so
/// a hostile/misconfigured peer cannot pin a core. 100,000,000 SHA-256 PBKDF2
/// rounds is already seconds of work; anything beyond is treated as an attack.
const MAX_SCRAM_ITERATIONS: u32 = 100_000_000;

/// Lower bound on the SCRAM `i=` iteration count, from RFC 7677 section 4
/// ("the iteration count SHOULD be at least 4096"). Enforced as a hard floor:
/// the count is server-supplied, and a hostile or MITM'd peer answering `i=1`
/// reduces `SaltedPassword` to a single PBKDF2 round, which makes the captured
/// `p=` proof cheap to brute-force offline. Refusing is the same downgrade
/// defence this module applies to `-PLUS` stripping and nonce non-extension:
/// a peer that cannot meet the specified minimum does not get our proof.
const MIN_SCRAM_ITERATIONS: u32 = 4096;

/// The hash function backing a SCRAM mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScramHash {
    /// SCRAM-SHA-1 (RFC 5802).
    Sha1,
    /// SCRAM-SHA-256 (RFC 7677).
    Sha256,
}

/// Whether a SCRAM exchange binds to the TLS channel.
///
/// Names the channel-binding dimension, kept orthogonal to [`ScramHash`] (the
/// hash) so the proof math stays hash-parameterized and the `-PLUS` mechanism
/// name is derived in one place ([`ScramHash::mechanism_name`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ChannelBinding {
    /// No channel binding (GS2 header `n,,`). Server advertises the non-PLUS
    /// mechanism, or the client declines binding.
    None,
    /// `tls-server-end-point` binding (GS2 header
    /// `p=tls-server-end-point,,`). Server advertises the `-PLUS` mechanism.
    TlsServerEndPoint,
}

impl ScramHash {
    /// The SASL mechanism name advertised on the wire, including the `-PLUS`
    /// suffix when the exchange binds to the channel.
    pub fn mechanism_name(self, binding: ChannelBinding) -> &'static str {
        match (self, binding) {
            (Self::Sha1, ChannelBinding::None) => "SCRAM-SHA-1",
            (Self::Sha256, ChannelBinding::None) => "SCRAM-SHA-256",
            (Self::Sha1, ChannelBinding::TlsServerEndPoint) => "SCRAM-SHA-1-PLUS",
            (Self::Sha256, ChannelBinding::TlsServerEndPoint) => "SCRAM-SHA-256-PLUS",
        }
    }
}

/// The GS2 channel-binding input to a SCRAM client-final message.
///
/// Determines the GS2 header the client sends in client-first and the `c=`
/// attribute (base64 of `gs2_header || cbind_data`). A data-carrying enum, not
/// a struct with a discriminant plus a separate data field, so the two never
/// disagree: both the header and the `c=` value derive from the same variant,
/// and the invalid "discriminant says None but data is non-empty" state is
/// unrepresentable.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ScramChannelBinding {
    /// No binding: GS2 header `n,,`, `c=biws`.
    None,
    /// Binding is available but the server did not advertise PLUS: `y,,`, `c=eSws`.
    SupportedButNotAdvertised,
    /// `tls-server-end-point` binding: GS2 header
    /// `p=tls-server-end-point,,`, `c=` over that header plus the raw
    /// [`crate::tls_server_end_point`] bytes.
    TlsServerEndPoint(Vec<u8>),
}

impl ScramChannelBinding {
    /// The GS2 header string the consumer must send in client-first (so the
    /// consumer and the `c=` computation never disagree). A PLUS consumer MUST
    /// build client-first from this, not from a literal, or the desync this
    /// type prevents re-enters at the consumer boundary.
    pub fn gs2_header(&self) -> &'static str {
        match self {
            Self::None => "n,,",
            Self::SupportedButNotAdvertised => "y,,",
            Self::TlsServerEndPoint(_) => "p=tls-server-end-point,,",
        }
    }

    /// The channel-binding discriminant, for callers that select the mechanism
    /// name via [`ScramHash::mechanism_name`].
    pub fn binding(&self) -> ChannelBinding {
        match self {
            Self::None | Self::SupportedButNotAdvertised => ChannelBinding::None,
            Self::TlsServerEndPoint(_) => ChannelBinding::TlsServerEndPoint,
        }
    }
}

/// Select one advertised SCRAM rung and its GS2 input (RFC 5802 section 6).
///
/// Call in preference order; `plus` identifies the requested rung. An advertised
/// PLUS suppresses its unbound counterpart even if local binding is unavailable.
/// `binding_data` is present only when binding is usable on this connection.
/// Certificate parsing errors must be propagated before calling this helper.
pub fn select_scram_rung(
    hash: ScramHash,
    plus: bool,
    advertised: impl Fn(&str) -> bool,
    binding_data: Option<&[u8]>,
) -> Option<(ScramHash, ScramChannelBinding)> {
    let offers_plus = advertised(hash.mechanism_name(ChannelBinding::TlsServerEndPoint));
    let binding = if plus {
        if !offers_plus {
            return None;
        }
        ScramChannelBinding::TlsServerEndPoint(binding_data?.to_vec())
    } else {
        if offers_plus || !advertised(hash.mechanism_name(ChannelBinding::None)) {
            return None;
        }
        // RFC 5802 section 6: `n` would conceal a stripped PLUS advertisement.
        // `y` lets a binding-capable server detect it; no binding bytes accompany y.
        if binding_data.is_some() {
            ScramChannelBinding::SupportedButNotAdvertised
        } else {
            ScramChannelBinding::None
        }
    };
    Some((hash, binding))
}

/// base64-decode a SASL continuation payload to its UTF-8 text form.
pub fn decode_continuation(data: &str) -> Result<String, SaslError> {
    use base64::Engine;

    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data.trim())
        .map_err(|e| SaslError::Protocol(format!("invalid base64 SASL continuation: {e}")))?;
    String::from_utf8(bytes)
        .map_err(|e| SaslError::Protocol(format!("SASL continuation was not UTF-8: {e}")))
}

/// Escape `=` and `,` in a SCRAM username (RFC 5802 saslname rule).
///
/// Crate-private on purpose. Escaping alone is only half of what RFC 5802
/// requires of a `n=` value - the other half is RFC 4013 SASLprep, and a caller
/// that reached for the escaper by name would silently skip it. Protocol crates
/// get [`prepare_scram_username`], which does both. The GS2 identity in
/// `oauth.rs` is the one in-crate caller that needs escaping without SASLprep,
/// because OAuth identities are not SCRAM usernames.
pub(crate) fn escape_username(user: &str) -> String {
    user.replace('=', "=3D").replace(',', "=2C")
}

/// Apply RFC 4013 SASLprep, then RFC 5802 saslname escaping to a SCRAM username.
///
/// SASLprep failure (a prohibited code point, an unassigned one, or a bidi
/// violation) is an `InvalidCredential` error rather than a pass-through:
/// sending the raw bytes would make the client and the server derive
/// `SaltedPassword` over different inputs, and would let visually equivalent
/// identities reach the same account. It is the caller's input, refused
/// before the exchange starts, so it is not a protocol fault.
pub fn prepare_scram_username(user: &str) -> Result<String, SaslError> {
    let prepared = stringprep::saslprep(user).map_err(|e| {
        SaslError::InvalidCredential(format!("SCRAM username failed SASLprep: {e}"))
    })?;
    Ok(escape_username(&prepared))
}

/// A SCRAM password that has already been through RFC 4013 SASLprep.
///
/// The only input [`scram_client_final`] accepts, and constructible only
/// through [`prepare_scram_password`]. That ordering is the point: SASLprep
/// used to run inside `scram_client_final`, after the server-first message,
/// so a password the caller supplied that fails SASLprep was discovered
/// mid-exchange - with AUTHENTICATE already on the wire and the server
/// waiting for a continuation. Preparing it up front makes that refusal a
/// local one, made before any byte is sent. Zeroized on drop.
pub struct ScramPassword(Zeroizing<String>);

impl std::fmt::Debug for ScramPassword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ScramPassword(<redacted>)")
    }
}

/// Apply RFC 4013 SASLprep to a SCRAM password, BEFORE the exchange starts.
///
/// A password that fails SASLprep is an `InvalidCredential` error, for the
/// same reasons as [`prepare_scram_username`].
pub fn prepare_scram_password(password: &str) -> Result<ScramPassword, SaslError> {
    let prepared = stringprep::saslprep(password).map_err(|e| {
        SaslError::InvalidCredential(format!("SCRAM password failed SASLprep: {e}"))
    })?;
    Ok(ScramPassword(Zeroizing::new(prepared.into_owned())))
}

/// Build the base64 client-final message and the expected server signature
/// from the server-first message. Pure; no I/O.
///
/// Every error here is about the server-first message, never the caller's
/// credentials: those were prepared by [`prepare_scram_password`] before the
/// exchange began.
pub fn scram_client_final(
    hash: ScramHash,
    password: &ScramPassword,
    client_nonce: &str,
    client_first_bare: &str,
    server_first: &str,
    binding: &ScramChannelBinding,
) -> Result<(Secret, Vec<u8>), SaslError> {
    use base64::Engine;

    validate_scram_attributes(server_first)?;
    if scram_field(server_first, 'm')?.is_some() {
        return Err(SaslError::Protocol(
            "SCRAM mandatory extension field is not supported".into(),
        ));
    }
    let server_nonce = scram_field(server_first, 'r')?
        .ok_or_else(|| SaslError::Protocol("SCRAM server-first message missing nonce".into()))?;
    // RFC 5802: the server-nonce (`r=`) is the client nonce with server-supplied
    // bytes appended, so it must STRICTLY extend the client nonce. An equal
    // nonce means the server contributed zero entropy (malformed or replayed
    // server-first); reject it.
    if !server_nonce.starts_with(client_nonce) || server_nonce.len() <= client_nonce.len() {
        return Err(SaslError::Protocol(
            "SCRAM server nonce does not extend client nonce".into(),
        ));
    }
    let salt_b64 = scram_field(server_first, 's')?
        .ok_or_else(|| SaslError::Protocol("SCRAM server-first message missing salt".into()))?;
    let salt = base64::engine::general_purpose::STANDARD
        .decode(salt_b64)
        .map_err(|e| SaslError::Protocol(format!("invalid SCRAM salt: {e}")))?;
    let iterations = scram_field(server_first, 'i')?
        .ok_or_else(|| {
            SaslError::Protocol("SCRAM server-first message missing iteration count".into())
        })?
        .parse::<u32>()
        .map_err(|e| SaslError::Protocol(format!("invalid SCRAM iteration count: {e}")))?;
    if iterations < MIN_SCRAM_ITERATIONS {
        return Err(SaslError::Protocol(format!(
            "SCRAM iteration count {iterations} is below the minimum of {MIN_SCRAM_ITERATIONS}"
        )));
    }
    // RFC 7677 mandates a minimum of 4096 and notes the count should stay within
    // reason; an unbounded `i=` is a CPU DoS, since the value flows straight into
    // PBKDF2. A hostile/misconfigured peer sending `i=4000000000` would pin a
    // core for minutes per attempt. Cap at a generous ceiling well above any
    // legitimate deployment (current real-world values are 4k-600k). Capping
    // here also protects the IMAP driver, which passes the value through
    // uncapped.
    if iterations > MAX_SCRAM_ITERATIONS {
        return Err(SaslError::Protocol(format!(
            "SCRAM iteration count {iterations} exceeds the maximum of {MAX_SCRAM_ITERATIONS}"
        )));
    }

    // RFC 5802 Section 6: the `c=` attribute is base64(gs2-header || cbind-data).
    // Without binding data the header is `n,,` (`biws`) or `y,,` (`eSws`).
    // For tls-server-end-point the header is
    // `p=tls-server-end-point,,` followed by the raw binding bytes.
    let mut cbind = binding.gs2_header().as_bytes().to_vec();
    if let ScramChannelBinding::TlsServerEndPoint(data) = binding {
        cbind.extend_from_slice(data);
    }
    let c_value = base64::engine::general_purpose::STANDARD.encode(&cbind);
    let client_final_without_proof = format!("c={c_value},r={server_nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{client_final_without_proof}");
    let (proof, server_signature) = scram_proof_and_server_signature(
        hash,
        password.0.as_bytes(),
        &salt,
        iterations,
        &auth_message,
    )?;
    let proof = zeroize::Zeroizing::new(base64::engine::general_purpose::STANDARD.encode(proof));
    let client_final =
        zeroize::Zeroizing::new(format!("{client_final_without_proof},p={}", proof.as_str()));
    Ok((
        base64::engine::general_purpose::STANDARD
            .encode(client_final.as_bytes())
            .into(),
        server_signature,
    ))
}

/// Verify the server-final message against the expected signature.
pub fn verify_server_final(server_final: &str, expected_signature: &[u8]) -> Result<(), SaslError> {
    use base64::Engine;

    validate_scram_attributes(server_final)?;
    if let Some(error) = scram_field(server_final, 'e')? {
        return Err(SaslError::AuthFailed(format!(
            "SCRAM server error: {error}"
        )));
    }
    let verifier = scram_field(server_final, 'v')?
        .ok_or_else(|| SaslError::Protocol("SCRAM server-final message missing verifier".into()))?;
    let actual = base64::engine::general_purpose::STANDARD
        .decode(verifier)
        .map_err(|e| SaslError::Protocol(format!("invalid SCRAM server verifier: {e}")))?;
    // `actual` is the attacker-controllable `v=` field; compare in constant
    // time so a MITM cannot use a timing oracle to forge the server signature
    // and impersonate the server. Matches the proof compare in `secret.rs`.
    if !bool::from(actual.ct_eq(expected_signature)) {
        return Err(SaslError::Protocol(
            "SCRAM server signature verification failed".into(),
        ));
    }
    Ok(())
}

fn scram_field(message: &str, key: char) -> Result<Option<&str>, SaslError> {
    // Match `<key>=` without rebuilding a `format!("{key}=")` String on every
    // call (4-5 lookups per message). Strip the single `key` char, then the `=`.
    let mut matches = message.split(',').filter_map(|field| {
        field
            .strip_prefix(key)
            .and_then(|rest| rest.strip_prefix('='))
    });
    let value = matches.next();
    if matches.next().is_some() {
        return Err(SaslError::Protocol(format!(
            "SCRAM message contains duplicate {key}= attribute"
        )));
    }
    Ok(value)
}

fn validate_scram_attributes(message: &str) -> Result<(), SaslError> {
    let mut seen = [false; 128];
    for field in message.split(',') {
        let bytes = field.as_bytes();
        if bytes.len() < 2 || !bytes[0].is_ascii_alphabetic() || bytes[1] != b'=' {
            return Err(SaslError::Protocol(
                "SCRAM message contains a malformed attribute".into(),
            ));
        }
        let index = usize::from(bytes[0]);
        if seen[index] {
            return Err(SaslError::Protocol(format!(
                "SCRAM message contains duplicate {}= attribute",
                char::from(bytes[0])
            )));
        }
        seen[index] = true;
    }
    Ok(())
}

fn scram_proof_and_server_signature(
    hash: ScramHash,
    password: &[u8],
    salt: &[u8],
    iterations: u32,
    auth_message: &str,
) -> Result<(Zeroizing<Vec<u8>>, Vec<u8>), SaslError> {
    match hash {
        ScramHash::Sha1 => scram_proof_sha1(password, salt, iterations, auth_message),
        ScramHash::Sha256 => scram_proof_sha256(password, salt, iterations, auth_message),
    }
}

fn scram_proof_sha1(
    password: &[u8],
    salt: &[u8],
    iterations: u32,
    auth_message: &str,
) -> Result<(Zeroizing<Vec<u8>>, Vec<u8>), SaslError> {
    use sha1::Digest;

    let mut salted = Zeroizing::new([0u8; 20]);
    pbkdf2::pbkdf2_hmac::<sha1::Sha1>(password, salt, iterations, &mut salted[..]);
    let client_key = hmac_digest::<hmac::Hmac<sha1::Sha1>>(&salted[..], b"Client Key")?;
    let stored_key = Zeroizing::new(sha1::Sha1::digest(&client_key).to_vec());
    let client_signature =
        hmac_digest::<hmac::Hmac<sha1::Sha1>>(&stored_key, auth_message.as_bytes())?;
    let proof = xor_bytes(&client_key, &client_signature);
    let server_key = hmac_digest::<hmac::Hmac<sha1::Sha1>>(&salted[..], b"Server Key")?;
    let server_signature =
        hmac_digest::<hmac::Hmac<sha1::Sha1>>(&server_key, auth_message.as_bytes())?;
    Ok((proof, server_signature.to_vec()))
}

fn scram_proof_sha256(
    password: &[u8],
    salt: &[u8],
    iterations: u32,
    auth_message: &str,
) -> Result<(Zeroizing<Vec<u8>>, Vec<u8>), SaslError> {
    use sha2::Digest;

    let mut salted = Zeroizing::new([0u8; 32]);
    pbkdf2::pbkdf2_hmac::<sha2::Sha256>(password, salt, iterations, &mut salted[..]);
    let client_key = hmac_digest::<hmac::Hmac<sha2::Sha256>>(&salted[..], b"Client Key")?;
    let stored_key = Zeroizing::new(sha2::Sha256::digest(&client_key).to_vec());
    let client_signature =
        hmac_digest::<hmac::Hmac<sha2::Sha256>>(&stored_key, auth_message.as_bytes())?;
    let proof = xor_bytes(&client_key, &client_signature);
    let server_key = hmac_digest::<hmac::Hmac<sha2::Sha256>>(&salted[..], b"Server Key")?;
    let server_signature =
        hmac_digest::<hmac::Hmac<sha2::Sha256>>(&server_key, auth_message.as_bytes())?;
    Ok((proof, server_signature.to_vec()))
}

fn hmac_digest<M>(key: &[u8], data: &[u8]) -> Result<Zeroizing<Vec<u8>>, SaslError>
where
    M: hmac::Mac + hmac::digest::KeyInit,
{
    let mut mac = <M as hmac::digest::KeyInit>::new_from_slice(key)
        .map_err(|e| SaslError::Protocol(format!("invalid SCRAM HMAC key: {e}")))?;
    mac.update(data);
    Ok(Zeroizing::new(mac.finalize().into_bytes().to_vec()))
}

fn xor_bytes(a: &[u8], b: &[u8]) -> Zeroizing<Vec<u8>> {
    assert_eq!(a.len(), b.len(), "SCRAM digest widths must match");
    Zeroizing::new(a.iter().zip(b.iter()).map(|(a, b)| a ^ b).collect())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn negotiated_binding_headers_and_client_final_attributes() {
        use base64::Engine;

        for hash in [ScramHash::Sha1, ScramHash::Sha256] {
            for (offers_plus, available, plus, header, c_input) in [
                (false, false, false, "n,,", b"n,,".as_slice()),
                (false, true, false, "y,,", b"y,,".as_slice()),
                (
                    true,
                    true,
                    true,
                    "p=tls-server-end-point,,",
                    b"p=tls-server-end-point,,data".as_slice(),
                ),
            ] {
                let advertised = |name: &str| {
                    name == hash.mechanism_name(ChannelBinding::None)
                        || (offers_plus
                            && name == hash.mechanism_name(ChannelBinding::TlsServerEndPoint))
                };
                let (selected, binding) = select_scram_rung(
                    hash,
                    plus,
                    advertised,
                    available.then_some(b"data".as_slice()),
                )
                .unwrap();
                assert_eq!(selected, hash);
                assert_eq!(binding.gs2_header(), header);
                let (message, _) = scram_client_final(
                    hash,
                    &pw("pencil"),
                    "nonce",
                    "n=user,r=nonce",
                    "r=nonceserver,s=QSXCR+Q6sek8bf92,i=4096",
                    &binding,
                )
                .unwrap();
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(message.as_str())
                    .unwrap();
                let decoded = String::from_utf8(decoded).unwrap();
                assert_eq!(
                    decoded.split(',').next().unwrap(),
                    format!(
                        "c={}",
                        base64::engine::general_purpose::STANDARD.encode(c_input)
                    )
                );
            }
            for available in [false, true] {
                let advertised = |name: &str| name.starts_with("SCRAM-");
                assert!(
                    select_scram_rung(hash, false, advertised, available.then_some(&[][..]))
                        .is_none()
                );
            }
            assert!(select_scram_rung(hash, true, |_| true, None).is_none());
            assert!(select_scram_rung(hash, true, |_| false, Some(&[])).is_none());
            assert!(select_scram_rung(hash, false, |_| false, Some(&[])).is_none());
        }
    }

    fn pw(password: &str) -> ScramPassword {
        prepare_scram_password(password).expect("test password prepares")
    }

    #[test]
    fn scram_sha1_client_final_matches_rfc_5802_vector() {
        use base64::Engine;

        let client_nonce = "fyko+d2lbbFgONRv9qkxdawL";
        let client_first_bare = "n=user,r=fyko+d2lbbFgONRv9qkxdawL";
        let server_first = "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096";
        let (client_final, server_signature) = scram_client_final(
            ScramHash::Sha1,
            &pw("pencil"),
            client_nonce,
            client_first_bare,
            server_first,
            &ScramChannelBinding::None,
        )
        .unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(client_final.as_str())
            .unwrap();
        let decoded = String::from_utf8(decoded).unwrap();
        assert_eq!(
            decoded,
            "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts="
        );
        assert_eq!(
            base64::engine::general_purpose::STANDARD.encode(server_signature),
            "rmF9pqV8S7suAoZWja4dJRkFsKQ="
        );
    }

    #[test]
    fn scram_sha256_client_final_matches_rfc_7677_vector() {
        use base64::Engine;

        // RFC 7677 Section 3: the published SCRAM-SHA-256 exchange for
        // user "user", password "pencil".
        let client_nonce = "rOprNGfwEbeRWgbNEkqO";
        let client_first_bare = "n=user,r=rOprNGfwEbeRWgbNEkqO";
        let server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let (client_final, server_signature) = scram_client_final(
            ScramHash::Sha256,
            &pw("pencil"),
            client_nonce,
            client_first_bare,
            server_first,
            &ScramChannelBinding::None,
        )
        .unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(client_final.as_str())
            .unwrap();
        let decoded = String::from_utf8(decoded).unwrap();
        assert_eq!(
            decoded,
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        // The RFC's server-final message verifies against the signature we
        // computed, closing the loop on both directions of the exchange.
        verify_server_final(
            "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=",
            &server_signature,
        )
        .unwrap();
    }

    #[test]
    fn escape_username_escapes_equals_before_comma() {
        assert_eq!(escape_username("a,b=c"), "a=2Cb=3Dc");
        // Order matters: '=' must be escaped before ','. The reverse order
        // would rewrite the '=' introduced by the ',' escape into '=3D',
        // yielding '=3D2C' for a lone comma.
        assert_eq!(escape_username("=,"), "=3D=2C");
        assert_eq!(escape_username("plain"), "plain");
    }

    #[test]
    fn scram_username_saslprep_matches_rfc_4013_examples() {
        // The complete RFC 4013 Section 3 example table, in order. Each row
        // exercises a different SASLprep clause, so pinning the whole table is
        // what proves real SASLprep is wired rather than a mapping subset.

        // #1 SOFT HYPHEN mapped to nothing.
        assert_eq!(prepare_scram_username("I\u{ad}X").unwrap(), "IX");
        // #2 and #3: no transformation, and case is NOT folded.
        assert_eq!(prepare_scram_username("user").unwrap(), "user");
        assert_eq!(prepare_scram_username("USER").unwrap(), "USER");
        // #4 FEMININE ORDINAL INDICATOR: NFKC output is "a".
        assert_eq!(prepare_scram_username("\u{aa}").unwrap(), "a");
        // #5 ROMAN NUMERAL NINE: NFKC output is "IX".
        assert_eq!(prepare_scram_username("\u{2168}").unwrap(), "IX");
        // #6 prohibited character (BELL).
        assert!(
            matches!(
                prepare_scram_username("\u{7}"),
                Err(SaslError::InvalidCredential(_))
            ),
            "prohibited code point must not pass SASLprep"
        );
        // #7 bidirectional check failure: a RandALCat code point followed by an
        // LCat one. Without the bidi rule this string prepares cleanly.
        assert!(
            matches!(
                prepare_scram_username("\u{627}\u{31}"),
                Err(SaslError::InvalidCredential(_))
            ),
            "RFC 3454 bidi rule violation must not pass SASLprep"
        );

        // Escaping still runs after preparation, on the prepared form.
        assert_eq!(prepare_scram_username("a,b=c").unwrap(), "a=2Cb=3Dc");
    }

    #[test]
    fn scram_password_saslprep_removes_rfc_4013_mapped_to_nothing() {
        use base64::Engine;
        let (client_final, _) = scram_client_final(
            ScramHash::Sha1,
            &pw("pen\u{ad}cil"),
            "fyko+d2lbbFgONRv9qkxdawL",
            "n=user,r=fyko+d2lbbFgONRv9qkxdawL",
            "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096",
            &ScramChannelBinding::None,
        )
        .unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(client_final.as_str())
            .unwrap();
        assert_eq!(
            String::from_utf8(decoded).unwrap(),
            "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts="
        );
    }

    /// A password that fails SASLprep is refused when it is PREPARED, which a
    /// protocol crate does before the exchange starts - never by
    /// `scram_client_final`, which runs after the server-first message and so
    /// mid-exchange. It is the caller's credential, so `InvalidCredential`,
    /// not a protocol fault.
    #[test]
    fn scram_password_rejects_prohibited_saslprep_input() {
        let err = prepare_scram_password("bad\u{7}").unwrap_err();
        assert!(
            matches!(err, SaslError::InvalidCredential(_)),
            "got {err:?}"
        );
    }

    #[test]
    fn scram_rejects_malformed_server_first_messages() {
        let nonce = "abc";
        let bare = "n=user,r=abc";
        for server_first in [
            // missing salt
            "r=abcdef,i=4096",
            // missing iteration count
            "r=abcdef,s=QSXCR+Q6sek8bf92",
            // missing server nonce
            "s=QSXCR+Q6sek8bf92,i=4096",
            // RFC 5802 mandatory extension is unsupported
            "m=ext,r=abcdef,s=QSXCR+Q6sek8bf92,i=4096",
            // zero iterations
            "r=abcdef,s=QSXCR+Q6sek8bf92,i=0",
            // below the RFC 7677 floor: an offline-brute-force downgrade
            "r=abcdef,s=QSXCR+Q6sek8bf92,i=1",
            "r=abcdef,s=QSXCR+Q6sek8bf92,i=4095",
            // salt is not base64
            "r=abcdef,s=!!!,i=4096",
            // iteration count is not numeric
            "r=abcdef,s=QSXCR+Q6sek8bf92,i=nope",
            // negative iteration count does not parse as u32
            "r=abcdef,s=QSXCR+Q6sek8bf92,i=-1",
        ] {
            let err = scram_client_final(
                ScramHash::Sha256,
                &pw("pencil"),
                nonce,
                bare,
                server_first,
                &ScramChannelBinding::None,
            )
            .unwrap_err();
            assert!(
                matches!(err, SaslError::Protocol(_)),
                "{server_first}: got {err:?}"
            );
        }
    }

    #[test]
    fn scram_rejects_duplicate_attributes() {
        let err = scram_client_final(
            ScramHash::Sha256,
            &pw("pencil"),
            "abc",
            "n=user,r=abc",
            "r=abcdef,s=QSXCR+Q6sek8bf92,i=4096,x=one,x=two",
            &ScramChannelBinding::None,
        )
        .unwrap_err();
        assert!(matches!(err, SaslError::Protocol(_)), "got {err:?}");

        // A duplicated attribute we DO consume. Taking the first occurrence
        // would let a MITM append a second salt or iteration count that a
        // differently-ordered peer honours instead.
        let err = scram_client_final(
            ScramHash::Sha256,
            &pw("pencil"),
            "abc",
            "n=user,r=abc",
            "r=abcdef,s=QSXCR+Q6sek8bf92,s=AAAAAAAAAAAAAAAA,i=4096",
            &ScramChannelBinding::None,
        )
        .unwrap_err();
        assert!(matches!(err, SaslError::Protocol(_)), "got {err:?}");

        // A field with no `=` at all is not an attribute-value pair.
        let err = verify_server_final("v=AAAA,junk", &[0; 32]).unwrap_err();
        assert!(matches!(err, SaslError::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn scram_rejects_duplicate_verifier_even_when_both_copies_are_correct() {
        use base64::Engine;

        // Both `v=` values are the signature we expect, so every check
        // downstream of the duplicate guard passes. Without that guard this is
        // an accepted server-final, which is precisely the ambiguity a
        // duplicate attribute buys an attacker: one copy for us, another for
        // whatever else parses the message.
        let expected = [7u8; 32];
        let encoded = base64::engine::general_purpose::STANDARD.encode(expected);
        assert!(
            verify_server_final(&format!("v={encoded}"), &expected).is_ok(),
            "single correct verifier must still be accepted"
        );
        let err = verify_server_final(&format!("v={encoded},v={encoded}"), &expected).unwrap_err();
        assert!(matches!(err, SaslError::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn scram_rejects_server_nonce_with_different_prefix() {
        // A server nonce that does not extend the client nonce is a
        // reflection/tamper signal, not merely short entropy.
        let err = scram_client_final(
            ScramHash::Sha256,
            &pw("pencil"),
            "abc",
            "n=user,r=abc",
            "r=xyzdef,s=QSXCR+Q6sek8bf92,i=4096",
            &ScramChannelBinding::None,
        )
        .unwrap_err();
        assert!(matches!(err, SaslError::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn decode_continuation_decodes_text_and_rejects_garbage() {
        assert_eq!(decode_continuation("aGVsbG8=").unwrap(), "hello");
        // Surrounding whitespace (trailing CRLF from the wire) is tolerated.
        assert_eq!(decode_continuation(" aGVsbG8=\r\n").unwrap(), "hello");
        let err = decode_continuation("!!!").unwrap_err();
        assert!(matches!(err, SaslError::Protocol(_)), "got {err:?}");
        // Valid base64 of invalid UTF-8 (a lone 0xFF byte).
        let err = decode_continuation("/w==").unwrap_err();
        assert!(matches!(err, SaslError::Protocol(_)), "got {err:?}");
    }

    /// Decode the base64 client-final and return its `c=` field.
    fn client_final_c_field(client_final: &Secret) -> String {
        use base64::Engine;

        let decoded = base64::engine::general_purpose::STANDARD
            .decode(client_final.as_str())
            .unwrap();
        let decoded = String::from_utf8(decoded).unwrap();
        decoded
            .split(',')
            .find_map(|f| f.strip_prefix("c="))
            .unwrap()
            .to_owned()
    }

    #[test]
    fn scram_sha256_plus_c_field_is_base64_of_header_and_cbind() {
        use base64::Engine;

        let client_nonce = "fyko+d2lbbFgONRv9qkxdawL";
        let client_first_bare = "n=user,r=fyko+d2lbbFgONRv9qkxdawL";
        let server_first = "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096";
        let cbind = vec![0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03, 0x04];

        let (plus_final, plus_sig) = scram_client_final(
            ScramHash::Sha256,
            &pw("pencil"),
            client_nonce,
            client_first_bare,
            server_first,
            &ScramChannelBinding::TlsServerEndPoint(cbind.clone()),
        )
        .unwrap();

        // RFC 5802 Section 6: c= is base64(gs2-header || cbind-data).
        let mut expected_cbind = b"p=tls-server-end-point,,".to_vec();
        expected_cbind.extend_from_slice(&cbind);
        let expected_c = base64::engine::general_purpose::STANDARD.encode(&expected_cbind);
        assert_eq!(client_final_c_field(&plus_final), expected_c);

        // PLUS and non-PLUS must diverge: different c= flows into auth_message,
        // hence a different proof and server signature for the same inputs.
        let (none_final, none_sig) = scram_client_final(
            ScramHash::Sha256,
            &pw("pencil"),
            client_nonce,
            client_first_bare,
            server_first,
            &ScramChannelBinding::None,
        )
        .unwrap();
        assert_eq!(client_final_c_field(&none_final), "biws");
        assert_ne!(plus_sig, none_sig);
        assert_ne!(plus_final.as_str(), none_final.as_str());
    }

    #[test]
    fn scram_channel_binding_gs2_header_and_discriminant() {
        assert_eq!(ScramChannelBinding::None.gs2_header(), "n,,");
        assert_eq!(
            ScramChannelBinding::TlsServerEndPoint(vec![0x01]).gs2_header(),
            "p=tls-server-end-point,,"
        );
        assert_eq!(ScramChannelBinding::None.binding(), ChannelBinding::None);
        assert_eq!(
            ScramChannelBinding::TlsServerEndPoint(vec![0x01]).binding(),
            ChannelBinding::TlsServerEndPoint
        );
    }

    #[test]
    fn scram_rejects_iteration_count_above_ceiling() {
        let client_nonce = "fyko+d2lbbFgONRv9qkxdawL";
        let client_first_bare = "n=user,r=fyko+d2lbbFgONRv9qkxdawL";
        // i= one above the ceiling; must be rejected before PBKDF2 runs.
        let i = u64::from(MAX_SCRAM_ITERATIONS) + 1;
        let server_first =
            format!("r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i={i}");
        let err = scram_client_final(
            ScramHash::Sha256,
            &pw("pencil"),
            client_nonce,
            client_first_bare,
            &server_first,
            &ScramChannelBinding::None,
        )
        .unwrap_err();
        assert!(matches!(err, SaslError::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn scram_rejects_server_nonce_equal_to_client_nonce() {
        let client_nonce = "fyko+d2lbbFgONRv9qkxdawL";
        let client_first_bare = "n=user,r=fyko+d2lbbFgONRv9qkxdawL";
        // server-nonce == client-nonce: server contributed no entropy.
        let server_first = "r=fyko+d2lbbFgONRv9qkxdawL,s=QSXCR+Q6sek8bf92,i=4096";
        let err = scram_client_final(
            ScramHash::Sha1,
            &pw("pencil"),
            client_nonce,
            client_first_bare,
            server_first,
            &ScramChannelBinding::None,
        )
        .unwrap_err();
        assert!(matches!(err, SaslError::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn verify_server_final_accepts_matching_signature() {
        use base64::Engine;
        let sig = vec![0x01, 0x02, 0x03, 0x04];
        let server_final = format!(
            "v={}",
            base64::engine::general_purpose::STANDARD.encode(&sig)
        );
        assert!(verify_server_final(&server_final, &sig).is_ok());
    }

    #[test]
    fn verify_server_final_rejects_mismatched_signature_as_protocol() {
        use base64::Engine;
        let actual = vec![0x01, 0x02, 0x03, 0x04];
        let expected = vec![0x01, 0x02, 0x03, 0x05];
        let server_final = format!(
            "v={}",
            base64::engine::general_purpose::STANDARD.encode(&actual)
        );
        // Signature mismatch is a protocol-class failure, not AuthFailed.
        let err = verify_server_final(&server_final, &expected).unwrap_err();
        assert!(matches!(err, SaslError::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn verify_server_final_surfaces_server_error_as_auth_failed() {
        let err = verify_server_final("e=invalid-proof", &[0x00]).unwrap_err();
        assert_eq!(err.scram_server_error(), Some("invalid-proof"));
        assert!(matches!(err, SaslError::AuthFailed(_)), "got {err:?}");
    }

    #[test]
    fn server_error_token_preserves_policy_and_unknown_values() {
        for token in ["channel-binding-not-supported", "unknown-extension-error"] {
            let err = verify_server_final(&format!("e={token}"), &[]).unwrap_err();
            assert_eq!(err.scram_server_error(), Some(token));
        }
        let malformed = verify_server_final("not-a-server-final", &[]).unwrap_err();
        assert_eq!(malformed.scram_server_error(), None);
    }

    #[test]
    fn mechanism_name_includes_plus_suffix_for_bound_variant() {
        assert_eq!(
            ScramHash::Sha256.mechanism_name(ChannelBinding::None),
            "SCRAM-SHA-256"
        );
        assert_eq!(
            ScramHash::Sha1.mechanism_name(ChannelBinding::None),
            "SCRAM-SHA-1"
        );
        assert_eq!(
            ScramHash::Sha256.mechanism_name(ChannelBinding::TlsServerEndPoint),
            "SCRAM-SHA-256-PLUS"
        );
        assert_eq!(
            ScramHash::Sha1.mechanism_name(ChannelBinding::TlsServerEndPoint),
            "SCRAM-SHA-1-PLUS"
        );
    }
}
