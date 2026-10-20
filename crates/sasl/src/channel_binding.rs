//! RFC 5929 `tls-server-end-point` channel-binding computation.
//!
//! Computation-only: given a DER-encoded server certificate, produce the
//! channel-binding data the GS2 `cbind-data` carries. No TLS, no I/O. The
//! protocol crates pull the peer certificate DER from their transport and
//! call in here; the result is base64-framed by the SCRAM client-final
//! computation in `scram.rs`.
//!
//! Per RFC 5929 Section 4 the binding value is the hash of the entire
//! DER-encoded certificate, using the hash named by the certificate's own
//! `signatureAlgorithm`, EXCEPT that MD5 and SHA-1 are upgraded to SHA-256
//! (RFC 5929 Section 4.1).

use crate::error::SaslError;

/// Compute the RFC 5929 Section 4 `tls-server-end-point` channel-binding
/// data from a DER-encoded server certificate.
///
/// The binding value is the hash of the entire DER-encoded certificate. The
/// hash function is the one named by the certificate's own
/// `signatureAlgorithm`, EXCEPT that MD5 and SHA-1 are upgraded to SHA-256
/// (RFC 5929 Section 4.1). Returns the raw hash bytes; the caller base64-frames
/// them into the GS2 `cbind-data`.
///
/// # Errors
///
/// [`SaslError::Protocol`] for a malformed / truncated DER, an indefinite-form
/// length, or a signature algorithm whose binding hash cannot be determined
/// (unrecognized OID, or EdDSA - see [`HashFamily`] resolution).
pub fn tls_server_end_point(cert_der: &[u8]) -> Result<Vec<u8>, SaslError> {
    let (oid, parameters) = signature_algorithm(cert_der)?;
    let family = if oid == OID_RSASSA_PSS {
        hash_family_for_pss_parameters(parameters)?
    } else {
        hash_family_for_oid(oid)?
    };
    Ok(family.digest(cert_der))
}

/// The resolved hash family used to compute the binding value, after the
/// MD5/SHA-1 -> SHA-256 upgrade has been applied. SHA-1 never appears here:
/// it is always upgraded to SHA-256, so no SHA-1 hashing path exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HashFamily {
    Sha256,
    Sha384,
    Sha512,
}

impl HashFamily {
    fn digest(self, data: &[u8]) -> Vec<u8> {
        use sha2::Digest;
        match self {
            Self::Sha256 => sha2::Sha256::digest(data).to_vec(),
            Self::Sha384 => sha2::Sha384::digest(data).to_vec(),
            Self::Sha512 => sha2::Sha512::digest(data).to_vec(),
        }
    }
}

// Pre-encoded DER OID *content* bytes (the bytes inside the OID's TLV, not
// including the `0x06` tag or the length). Compared as slice equality against
// the parsed signatureAlgorithm OID content, so no OID-to-string decoding is
// needed. Each constant's dotted form is in its comment.

// 1.2.840.113549.1.1.11  sha256WithRSAEncryption
const OID_SHA256_RSA: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0b];
// 1.2.840.113549.1.1.12  sha384WithRSAEncryption
const OID_SHA384_RSA: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0c];
// 1.2.840.113549.1.1.13  sha512WithRSAEncryption
const OID_SHA512_RSA: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0d];
// 1.2.840.113549.1.1.5   sha1WithRSAEncryption
const OID_SHA1_RSA: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x05];
// 1.2.840.113549.1.1.4   md5WithRSAEncryption
const OID_MD5_RSA: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x04];
// 1.2.840.113549.1.1.10  id-RSASSA-PSS
const OID_RSASSA_PSS: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0a];

// 1.2.840.10045.4.3.2    ecdsa-with-SHA256
const OID_ECDSA_SHA256: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x02];
// 1.2.840.10045.4.3.3    ecdsa-with-SHA384
const OID_ECDSA_SHA384: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x03];
// 1.2.840.10045.4.3.4    ecdsa-with-SHA512
const OID_ECDSA_SHA512: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x04];
// 1.2.840.10045.4.1      ecdsa-with-SHA1
const OID_ECDSA_SHA1: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x01];

// Bare digest OIDs. A conforming X.509 `signatureAlgorithm` never carries a
// bare digest OID (it carries a signature-with-digest OID such as those
// above), so these arms cannot fire for a well-formed cert. We accept them
// anyway as a deliberate leniency: mapping a bare digest to its hash family
// is unambiguous and harmless, and rejecting it would be a needless
// channel-binding failure on a marginally non-conforming peer. The
// `signature_oid_maps_to_hash_family` test pins them for that reason.
// 2.16.840.1.101.3.4.2.1 sha256 (bare)
const OID_SHA256_BARE: &[u8] = &[0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x01];
// 2.16.840.1.101.3.4.2.2 sha384 (bare)
const OID_SHA384_BARE: &[u8] = &[0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x02];
// 2.16.840.1.101.3.4.2.3 sha512 (bare)
const OID_SHA512_BARE: &[u8] = &[0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x03];
// 1.3.14.3.2.26          sha1 (bare)
const OID_SHA1_BARE: &[u8] = &[0x2b, 0x0e, 0x03, 0x02, 0x1a];

// 1.3.101.112            Ed25519
const OID_ED25519: &[u8] = &[0x2b, 0x65, 0x70];
// 1.3.101.113            Ed448
const OID_ED448: &[u8] = &[0x2b, 0x65, 0x71];

/// Resolve a signature-algorithm OID to the binding hash family, applying the
/// RFC 5929 Section 4.1 MD5/SHA-1 -> SHA-256 upgrade.
fn hash_family_for_oid(oid: &[u8]) -> Result<HashFamily, SaslError> {
    // SHA-256 family.
    if oid == OID_SHA256_RSA || oid == OID_ECDSA_SHA256 || oid == OID_SHA256_BARE {
        return Ok(HashFamily::Sha256);
    }
    // SHA-384 family.
    if oid == OID_SHA384_RSA || oid == OID_ECDSA_SHA384 || oid == OID_SHA384_BARE {
        return Ok(HashFamily::Sha384);
    }
    // SHA-512 family.
    if oid == OID_SHA512_RSA || oid == OID_ECDSA_SHA512 || oid == OID_SHA512_BARE {
        return Ok(HashFamily::Sha512);
    }
    // MD5 / SHA-1 -> upgraded to SHA-256 (RFC 5929 Section 4.1 exception).
    if oid == OID_SHA1_RSA || oid == OID_ECDSA_SHA1 || oid == OID_SHA1_BARE || oid == OID_MD5_RSA {
        return Ok(HashFamily::Sha256);
    }
    // EdDSA names no signature-algorithm hash in the RFC 5929 Section 4.1
    // sense: the hash is intrinsic to the algorithm (SHA-512 for Ed25519,
    // SHAKE256 for Ed448) rather than the OID's named hash. Guessing SHA-256
    // here would fail silently against a conforming server. Recognize the OID
    // and fail loud rather than guess; if a real deployment binds against an
    // EdDSA leaf cert, the hash that server uses gets pinned by a vector and
    // wired in then, not guessed now.
    if oid == OID_ED25519 || oid == OID_ED448 {
        return Err(SaslError::Protocol(
            "EdDSA (Ed25519/Ed448) certificate channel binding is not \
             supported: EdDSA names no signature hash for the RFC 5929 \
             Section 4.1 binding-hash selection"
                .into(),
        ));
    }
    // Unknown OID: do NOT default to SHA-256. A wrong binding value is a
    // silent auth failure that is far harder to debug than a typed error.
    Err(SaslError::Protocol(format!(
        "unrecognized certificate signature algorithm OID (DER content bytes: \
         {oid:02x?})"
    )))
}

/// Resolve the `hashAlgorithm` field of RFC 4055 `RSASSA-PSS-params`. An
/// absent field carries the ASN.1 default `sha1`, which RFC 5929 Section 4.1
/// upgrades to SHA-256.
///
/// The whole parameter SEQUENCE is parsed before a hash is returned, not just
/// the prefix up to `[0]`. `RSASSA-PSS-params` is
/// `SEQUENCE { [0] hashAlgorithm, [1] maskGenAlgorithm, [2] saltLength,
/// [3] trailerField }`, all DEFAULTed and all explicitly tagged, so DER admits
/// each context tag at most once and only in ascending order. Returning on the
/// first `[0]` would accept a duplicated or misordered `[0]` and silently bind
/// against whichever hash was encoded first - an ambiguity an attacker chooses.
/// Since this module's policy is that anything it cannot read unambiguously is
/// a hard error, the structure is validated in full first.
fn hash_family_for_pss_parameters(parameters: &[u8]) -> Result<HashFamily, SaslError> {
    // `parameters` is OPTIONAL in `AlgorithmIdentifier`; wholly absent means
    // every PSS field takes its default, hash included.
    if parameters.is_empty() {
        return Ok(HashFamily::Sha256);
    }
    let (tag, body, rest) = read_tlv(parameters)?;
    if tag != TAG_SEQUENCE || !rest.is_empty() {
        return Err(SaslError::Protocol(
            "RSASSA-PSS certificate parameters are not one DER SEQUENCE".into(),
        ));
    }

    let mut hash: Option<HashFamily> = None;
    let mut previous_tag: Option<u8> = None;
    let mut fields = body;
    while !fields.is_empty() {
        let (field_tag, field_body, remaining) = read_tlv(fields)?;
        if !(TAG_CONTEXT_0..=TAG_CONTEXT_3).contains(&field_tag) {
            return Err(SaslError::Protocol(format!(
                "RSASSA-PSS parameters contain an unexpected field tag {field_tag:#04x}"
            )));
        }
        // DER: DEFAULTed SEQUENCE members appear at most once, in declaration
        // order. Equal or descending tags mean a duplicate or a reordering.
        if previous_tag.is_some_and(|previous| field_tag <= previous) {
            return Err(SaslError::Protocol(format!(
                "RSASSA-PSS parameters contain a duplicate or out-of-order field \
                 tag {field_tag:#04x}"
            )));
        }
        previous_tag = Some(field_tag);

        if field_tag == TAG_CONTEXT_0 {
            hash = Some(pss_hash_algorithm(field_body)?);
        }
        fields = remaining;
    }
    Ok(hash.unwrap_or(HashFamily::Sha256))
}

/// Read the `[0] hashAlgorithm` content: exactly one `AlgorithmIdentifier`
/// whose first element is the hash OID.
fn pss_hash_algorithm(field_body: &[u8]) -> Result<HashFamily, SaslError> {
    let (alg_tag, alg_body, alg_rest) = read_tlv(field_body)?;
    if alg_tag != TAG_SEQUENCE || !alg_rest.is_empty() {
        return Err(SaslError::Protocol(
            "RSASSA-PSS hashAlgorithm is not one AlgorithmIdentifier".into(),
        ));
    }
    let (oid_tag, oid, _parameters) = read_tlv(alg_body)?;
    if oid_tag != TAG_OID {
        return Err(SaslError::Protocol(
            "RSASSA-PSS hashAlgorithm.algorithm is not an OBJECT IDENTIFIER".into(),
        ));
    }
    hash_family_for_oid(oid)
}

/// Walk the DER `Certificate` and return the `signatureAlgorithm` OID's
/// content bytes (no tag, no length).
///
/// `Certificate ::= SEQUENCE { tbsCertificate, signatureAlgorithm,
/// signatureValue }`. The `signatureAlgorithm` is the second element, an
/// `AlgorithmIdentifier ::= SEQUENCE { algorithm OID, parameters OPTIONAL }`;
/// its first element is the OID.
fn signature_algorithm(cert_der: &[u8]) -> Result<(&[u8], &[u8]), SaslError> {
    // Outer Certificate SEQUENCE.
    let (outer_tag, outer_body, rest) = read_tlv(cert_der)?;
    if outer_tag != TAG_SEQUENCE {
        return Err(SaslError::Protocol(
            "certificate DER outer element is not a SEQUENCE".into(),
        ));
    }
    // `tls_server_end_point` hashes the ENTIRE input buffer, so trailing
    // bytes after the outer SEQUENCE would silently ride into the binding
    // hash and mismatch the server's. Reject them instead: with native-tls
    // as the only source this is theoretical, but a transport handing over
    // padded DER must fail loudly, not bind wrongly.
    if !rest.is_empty() {
        return Err(SaslError::Protocol(
            "certificate DER has trailing bytes after the outer SEQUENCE".into(),
        ));
    }
    // First element: tbsCertificate. Skip over it.
    let (_tbs_tag, _tbs_body, after_tbs) = read_tlv(outer_body)?;
    // Second element: signatureAlgorithm SEQUENCE.
    let (sigalg_tag, sigalg_body, _after_sigalg) = read_tlv(after_tbs)?;
    if sigalg_tag != TAG_SEQUENCE {
        return Err(SaslError::Protocol(
            "certificate signatureAlgorithm is not a SEQUENCE".into(),
        ));
    }
    // First element of signatureAlgorithm: the algorithm OID.
    let (oid_tag, oid_body, after_oid) = read_tlv(sigalg_body)?;
    if oid_tag != TAG_OID {
        return Err(SaslError::Protocol(
            "certificate signatureAlgorithm.algorithm is not an OBJECT IDENTIFIER".into(),
        ));
    }
    Ok((oid_body, after_oid))
}

const TAG_SEQUENCE: u8 = 0x30;
const TAG_OID: u8 = 0x06;
const TAG_CONTEXT_0: u8 = 0xa0;
const TAG_CONTEXT_3: u8 = 0xa3;

/// Read one definite-form DER TLV from the front of `buf`. Returns the tag
/// byte, the content (value) slice, and the remainder past this element.
///
/// Only low-tag-number, definite-form values are accepted. Lengths must use
/// DER's minimal encoding: short form `0x00..=0x7f`, or long form with one to
/// four length bytes for values of at least 128. The indefinite form (`0x80`)
/// is invalid in DER and rejected. Any truncation or length overrun is a
/// `Protocol` error.
fn read_tlv(buf: &[u8]) -> Result<(u8, &[u8], &[u8]), SaslError> {
    let tag = *buf
        .first()
        .ok_or_else(|| SaslError::Protocol("truncated DER: missing tag byte".into()))?;
    if tag & 0x1f == 0x1f {
        return Err(SaslError::Protocol(
            "high-tag-number DER form is not supported".into(),
        ));
    }
    let len_byte = *buf
        .get(1)
        .ok_or_else(|| SaslError::Protocol("truncated DER: missing length byte".into()))?;

    let (len, header_len) = if len_byte & 0x80 == 0 {
        // Short form: the length byte is the length.
        (len_byte as usize, 2)
    } else {
        let num_len_bytes = (len_byte & 0x7f) as usize;
        if num_len_bytes == 0 {
            // 0x80: indefinite form, invalid in DER.
            return Err(SaslError::Protocol(
                "indefinite-form DER length is invalid in DER".into(),
            ));
        }
        if num_len_bytes > 4 {
            // A single certificate element longer than 4 GiB is not a thing we
            // parse; reject rather than risk a usize overflow.
            return Err(SaslError::Protocol("DER length field too large".into()));
        }
        let len_bytes = buf
            .get(2..2 + num_len_bytes)
            .ok_or_else(|| SaslError::Protocol("truncated DER: short long-form length".into()))?;
        if len_bytes.first() == Some(&0) {
            return Err(SaslError::Protocol(
                "non-minimal DER long-form length".into(),
            ));
        }
        // Accumulate into `u64` (the `num_len_bytes <= 4` cap keeps this exact)
        // then narrow to `usize`. Narrowing through `try_from` rather than
        // shifting directly into `usize` means a 4-byte length cannot silently
        // wrap on a 32-bit target into a bogus small value that slips past the
        // later bounds check.
        let mut len: u64 = 0;
        for &b in len_bytes {
            len = (len << 8) | u64::from(b);
        }
        if len < 0x80 {
            return Err(SaslError::Protocol(
                "non-minimal DER long-form length".into(),
            ));
        }
        let len = usize::try_from(len)
            .map_err(|_| SaslError::Protocol("DER length field too large".into()))?;
        (len, 2 + num_len_bytes)
    };

    let end = header_len
        .checked_add(len)
        .ok_or_else(|| SaslError::Protocol("DER length overflow".into()))?;
    let content = buf
        .get(header_len..end)
        .ok_or_else(|| SaslError::Protocol("truncated DER: content overruns buffer".into()))?;
    let rest = &buf[end..];
    Ok((tag, content, rest))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// Build a minimal but structurally valid DER `Certificate` whose
    /// `signatureAlgorithm` carries `sig_oid_content` (the OID content bytes).
    /// The tbsCertificate is an empty SEQUENCE and the signatureValue an empty
    /// BIT STRING - the parser only walks to the signatureAlgorithm OID, and
    /// the binding value is the hash of *these exact whole bytes*, so a minimal
    /// shape is a faithful fixture for both behaviors under test.
    fn build_cert(sig_oid_content: &[u8]) -> Vec<u8> {
        build_cert_with_parameters(sig_oid_content, &[])
    }

    fn build_cert_with_parameters(sig_oid_content: &[u8], parameters: &[u8]) -> Vec<u8> {
        // All fixture bodies are well under 128 bytes, so DER short-form length
        // (a single byte) is always correct; `try_from` pins that invariant
        // rather than truncating silently.
        let short_len = |n: usize| u8::try_from(n).unwrap();

        // signatureAlgorithm: SEQUENCE { OID }
        let mut oid = vec![TAG_OID, short_len(sig_oid_content.len())];
        oid.extend_from_slice(sig_oid_content);
        let sigalg_len = oid.len() + parameters.len();
        let mut sigalg = vec![TAG_SEQUENCE, short_len(sigalg_len)];
        sigalg.extend_from_slice(&oid);
        sigalg.extend_from_slice(parameters);

        // tbsCertificate: empty SEQUENCE.
        let tbs = vec![TAG_SEQUENCE, 0x00];
        // signatureValue: empty BIT STRING (tag 0x03), single zero unused-bits
        // octet.
        let sigval = vec![0x03, 0x01, 0x00];

        let mut body = Vec::new();
        body.extend_from_slice(&tbs);
        body.extend_from_slice(&sigalg);
        body.extend_from_slice(&sigval);

        let mut cert = vec![TAG_SEQUENCE, short_len(body.len())];
        cert.extend_from_slice(&body);
        cert
    }

    fn pss_parameters(hash_oid: &[u8]) -> Vec<u8> {
        let short_len = |n: usize| u8::try_from(n).unwrap();
        let mut oid = vec![TAG_OID, short_len(hash_oid.len())];
        oid.extend_from_slice(hash_oid);
        let mut algorithm = vec![TAG_SEQUENCE, short_len(oid.len())];
        algorithm.extend_from_slice(&oid);
        let mut hash_algorithm = vec![TAG_CONTEXT_0, short_len(algorithm.len())];
        hash_algorithm.extend_from_slice(&algorithm);
        let mut parameters = vec![TAG_SEQUENCE, short_len(hash_algorithm.len())];
        parameters.extend_from_slice(&hash_algorithm);
        parameters
    }

    /// A `[0] hashAlgorithm` element (tag + AlgorithmIdentifier), without the
    /// enclosing `RSASSA-PSS-params` SEQUENCE, so tests can assemble field
    /// sequences the strict encoder would never produce.
    fn pss_hash_field(hash_oid: &[u8]) -> Vec<u8> {
        let inner = pss_parameters(hash_oid);
        // Strip the outer SEQUENCE header that `pss_parameters` added.
        inner[2..].to_vec()
    }

    fn pss_params_from_fields(fields: &[u8]) -> Vec<u8> {
        let mut parameters = vec![TAG_SEQUENCE, u8::try_from(fields.len()).unwrap()];
        parameters.extend_from_slice(fields);
        parameters
    }

    #[test]
    fn tls_server_end_point_rsa_sha256_hashes_whole_der() {
        use sha2::Digest;
        let cert = build_cert(OID_SHA256_RSA);
        let expected = sha2::Sha256::digest(&cert).to_vec();
        let got = tls_server_end_point(&cert).unwrap();
        assert_eq!(got.len(), 32);
        assert_eq!(got, expected);
    }

    #[test]
    fn tls_server_end_point_sha1_cert_upgrades_to_sha256() {
        use sha2::Digest;
        let cert = build_cert(OID_SHA1_RSA);
        let expected = sha2::Sha256::digest(&cert).to_vec();
        let got = tls_server_end_point(&cert).unwrap();
        // RFC 5929 Section 4.1: SHA-1 signature -> SHA-256 binding hash.
        assert_eq!(got.len(), 32);
        assert_eq!(got, expected);
    }

    #[test]
    fn tls_server_end_point_rsassa_pss_uses_parameter_hash() {
        use sha2::Digest;
        let parameters = pss_parameters(OID_SHA384_BARE);
        let cert = build_cert_with_parameters(OID_RSASSA_PSS, &parameters);
        let expected = sha2::Sha384::digest(&cert).to_vec();
        let got = tls_server_end_point(&cert).unwrap();
        assert_eq!(got.len(), 48);
        assert_eq!(got, expected);
    }

    #[test]
    fn tls_server_end_point_rsassa_pss_default_sha1_upgrades_to_sha256() {
        use sha2::Digest;
        let cert = build_cert_with_parameters(OID_RSASSA_PSS, &[TAG_SEQUENCE, 0]);
        let expected = sha2::Sha256::digest(&cert).to_vec();
        assert_eq!(tls_server_end_point(&cert).unwrap(), expected);
    }

    #[test]
    fn tls_server_end_point_rsassa_pss_rejects_duplicate_hash_field() {
        // Two `[0] hashAlgorithm` fields naming different hashes. Taking the
        // first would bind against SHA-384 while the certificate is ambiguous;
        // an ambiguous binding must be refused outright.
        let mut fields = pss_hash_field(OID_SHA384_BARE);
        fields.extend_from_slice(&pss_hash_field(OID_SHA512_BARE));
        let cert = build_cert_with_parameters(OID_RSASSA_PSS, &pss_params_from_fields(&fields));
        let err = tls_server_end_point(&cert).unwrap_err();
        assert!(matches!(err, SaslError::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn tls_server_end_point_rsassa_pss_rejects_out_of_order_hash_field() {
        // `[1] maskGenAlgorithm` (contents irrelevant) placed before
        // `[0] hashAlgorithm`. DER forbids the reordering; a prefix parser that
        // stopped at the first `[0]` would never notice.
        let mut fields = vec![TAG_CONTEXT_0 + 1, 0x00];
        fields.extend_from_slice(&pss_hash_field(OID_SHA384_BARE));
        let cert = build_cert_with_parameters(OID_RSASSA_PSS, &pss_params_from_fields(&fields));
        let err = tls_server_end_point(&cert).unwrap_err();
        assert!(matches!(err, SaslError::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn tls_server_end_point_rsassa_pss_rejects_unknown_field_tag() {
        // `[4]` is not a member of `RSASSA-PSS-params`. Unknown structure is a
        // typed error, never a shrug that keeps the default hash.
        let mut fields = pss_hash_field(OID_SHA384_BARE);
        fields.extend_from_slice(&[TAG_CONTEXT_0 + 4, 0x00]);
        let cert = build_cert_with_parameters(OID_RSASSA_PSS, &pss_params_from_fields(&fields));
        let err = tls_server_end_point(&cert).unwrap_err();
        assert!(matches!(err, SaslError::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn tls_server_end_point_rsassa_pss_trailing_fields_are_validated() {
        // A well-formed `[0]` followed by `[2] saltLength` and `[3]
        // trailerField` still resolves to the `[0]` hash, proving full-sequence
        // parsing did not break the conforming case.
        use sha2::Digest;
        let mut fields = pss_hash_field(OID_SHA384_BARE);
        fields.extend_from_slice(&[TAG_CONTEXT_0 + 2, 0x00, TAG_CONTEXT_3, 0x00]);
        let cert = build_cert_with_parameters(OID_RSASSA_PSS, &pss_params_from_fields(&fields));
        let expected = sha2::Sha384::digest(&cert).to_vec();
        assert_eq!(tls_server_end_point(&cert).unwrap(), expected);
    }

    #[test]
    fn tls_server_end_point_rsassa_pss_absent_parameters_default_to_sha256() {
        // `AlgorithmIdentifier.parameters` omitted entirely: every PSS field
        // takes its default, so hashAlgorithm is sha1, upgraded to SHA-256.
        use sha2::Digest;
        let cert = build_cert_with_parameters(OID_RSASSA_PSS, &[]);
        let expected = sha2::Sha256::digest(&cert).to_vec();
        assert_eq!(tls_server_end_point(&cert).unwrap(), expected);
    }

    #[test]
    fn tls_server_end_point_rsassa_pss_unknown_hash_oid_is_protocol_error() {
        // The named hash is not one we recognize. Falling back to the sha1
        // default here would compute a binding value the server never agrees
        // with, so it has to be an error.
        let parameters = pss_parameters(&[0x2a, 0x03, 0x04]);
        let cert = build_cert_with_parameters(OID_RSASSA_PSS, &parameters);
        let err = tls_server_end_point(&cert).unwrap_err();
        assert!(matches!(err, SaslError::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn tls_server_end_point_truncated_der_is_protocol_error() {
        let cert = build_cert(OID_SHA256_RSA);
        let truncated = &cert[..cert.len() / 2];
        let err = tls_server_end_point(truncated).unwrap_err();
        assert!(matches!(err, SaslError::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn tls_server_end_point_unrecognized_oid_is_protocol_error() {
        // A bogus, structurally valid OID that maps to nothing.
        let bogus = &[0x2a, 0x03, 0x04, 0x05];
        let cert = build_cert(bogus);
        let err = tls_server_end_point(&cert).unwrap_err();
        assert!(matches!(err, SaslError::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn tls_server_end_point_eddsa_oid_is_protocol_error() {
        let cert = build_cert(OID_ED25519);
        let err = tls_server_end_point(&cert).unwrap_err();
        assert!(matches!(err, SaslError::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn signature_oid_maps_to_hash_family() {
        // SHA-256 family.
        assert_eq!(
            hash_family_for_oid(OID_SHA256_RSA).unwrap(),
            HashFamily::Sha256
        );
        assert_eq!(
            hash_family_for_oid(OID_ECDSA_SHA256).unwrap(),
            HashFamily::Sha256
        );
        assert_eq!(
            hash_family_for_oid(OID_SHA256_BARE).unwrap(),
            HashFamily::Sha256
        );
        // SHA-384 family.
        assert_eq!(
            hash_family_for_oid(OID_SHA384_RSA).unwrap(),
            HashFamily::Sha384
        );
        assert_eq!(
            hash_family_for_oid(OID_ECDSA_SHA384).unwrap(),
            HashFamily::Sha384
        );
        assert_eq!(
            hash_family_for_oid(OID_SHA384_BARE).unwrap(),
            HashFamily::Sha384
        );
        // SHA-512 family.
        assert_eq!(
            hash_family_for_oid(OID_SHA512_RSA).unwrap(),
            HashFamily::Sha512
        );
        assert_eq!(
            hash_family_for_oid(OID_ECDSA_SHA512).unwrap(),
            HashFamily::Sha512
        );
        assert_eq!(
            hash_family_for_oid(OID_SHA512_BARE).unwrap(),
            HashFamily::Sha512
        );
        // MD5 / SHA-1 -> SHA-256 (RFC 5929 Section 4.1 upgrade).
        assert_eq!(
            hash_family_for_oid(OID_SHA1_RSA).unwrap(),
            HashFamily::Sha256
        );
        assert_eq!(
            hash_family_for_oid(OID_ECDSA_SHA1).unwrap(),
            HashFamily::Sha256
        );
        assert_eq!(
            hash_family_for_oid(OID_SHA1_BARE).unwrap(),
            HashFamily::Sha256
        );
        assert_eq!(
            hash_family_for_oid(OID_MD5_RSA).unwrap(),
            HashFamily::Sha256
        );
        // EdDSA and unknown -> error.
        assert!(hash_family_for_oid(OID_ED25519).is_err());
        assert!(hash_family_for_oid(OID_ED448).is_err());
        assert!(hash_family_for_oid(&[0x2a, 0x03]).is_err());
    }

    // ---- DER long-form length path (`read_tlv`) ----------------------------
    //
    // The fixtures above all use short-form lengths. These exercise the
    // multi-byte long-form length accumulation, the `num_len_bytes > 4`
    // rejection, the indefinite-form (`0x80`) rejection, and that a long-form
    // header advances `rest` past the full element.

    #[test]
    fn read_tlv_long_form_two_byte_length() {
        // 0x82 => two length bytes follow; 0x01 0x00 == 256 content bytes.
        let mut buf = vec![TAG_SEQUENCE, 0x82, 0x01, 0x00];
        buf.extend(std::iter::repeat_n(0xAA, 256));
        buf.extend_from_slice(&[0xFF, 0xFF]); // trailing bytes -> `rest`
        let (tag, content, rest) = read_tlv(&buf).unwrap();
        assert_eq!(tag, TAG_SEQUENCE);
        assert_eq!(content.len(), 256);
        assert!(content.iter().all(|&b| b == 0xAA));
        assert_eq!(rest, &[0xFF, 0xFF]);
    }

    #[test]
    fn read_tlv_long_form_one_byte_length() {
        // 0x81 => one length byte follows; 0x80 == 128 (> short-form max).
        let mut buf = vec![TAG_OID, 0x81, 0x80];
        buf.extend(std::iter::repeat_n(0x11, 128));
        let (tag, content, rest) = read_tlv(&buf).unwrap();
        assert_eq!(tag, TAG_OID);
        assert_eq!(content.len(), 128);
        assert!(rest.is_empty());
    }

    #[test]
    fn read_tlv_indefinite_form_is_rejected() {
        // 0x80 is the indefinite form, invalid in DER.
        let buf = [TAG_SEQUENCE, 0x80, 0x00, 0x00];
        let err = read_tlv(&buf).unwrap_err();
        assert!(matches!(err, SaslError::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn read_tlv_more_than_four_length_bytes_is_rejected() {
        // 0x85 => five length bytes; rejected before any narrowing.
        let buf = [TAG_SEQUENCE, 0x85, 0x01, 0x00, 0x00, 0x00, 0x00];
        let err = read_tlv(&buf).unwrap_err();
        assert!(matches!(err, SaslError::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn read_tlv_long_form_length_overruns_buffer() {
        // 0x82 declares 256 content bytes but the buffer is short -> truncation.
        let buf = [TAG_SEQUENCE, 0x82, 0x01, 0x00, 0xAA, 0xAA];
        let err = read_tlv(&buf).unwrap_err();
        assert!(matches!(err, SaslError::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn read_tlv_truncated_long_form_length_field() {
        // 0x83 promises three length bytes but only one is present.
        let buf = [TAG_SEQUENCE, 0x83, 0x01];
        let err = read_tlv(&buf).unwrap_err();
        assert!(matches!(err, SaslError::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn read_tlv_rejects_non_minimal_der_lengths() {
        let short_value_in_long_form = [TAG_SEQUENCE, 0x81, 0x7f];
        let leading_zero_in_long_form = [TAG_SEQUENCE, 0x82, 0x00, 0x80];

        assert!(matches!(
            read_tlv(&short_value_in_long_form),
            Err(SaslError::Protocol(_))
        ));
        assert!(matches!(
            read_tlv(&leading_zero_in_long_form),
            Err(SaslError::Protocol(_))
        ));
    }

    #[test]
    fn read_tlv_rejects_high_tag_number_form() {
        let high_tag_number = [0x1f, 0x01, 0x00];

        assert!(matches!(
            read_tlv(&high_tag_number),
            Err(SaslError::Protocol(_))
        ));
    }
}
