# bifrost-sasl

Private shared SASL/SCRAM computation layer for the bifrost protocol crates.
Not in the public API of any protocol crate. Depended on by `bifrost-imap`
and `bifrost-smtp`. No `bifrost-types`, no
`bifrost-net`, no tokio: the crate is sync and computation-only.

## Scope

Pure computation: no I/O, no async, no protocol command flow. The protocol
crates own the wire sequencing - driving `+` continuations, framing commands -
and call into the transition functions here. The SASL crate is invoked from
inside the protocol consumers, never the other way around.

## Surface

- `Secret` - zeroizing string wrapper (`Zeroizing<String>` newtype),
  constant-time `PartialEq`, redacted `Debug`, `as_str` / `as_bytes` /
  `into_zeroizing`, plus `From<String>` / `From<&str>` /
  `From<Zeroizing<String>>` / `Deref<Target=str>` / `AsRef<str>`. Each protocol
  crate can consume it directly; IMAP
  `types/secret.rs::SecretString` is an alias, with no duplicate storage or
  equality implementation.
- `SaslError` - `#[non_exhaustive]`. `Protocol(String)` for malformed or
  unexpected SASL/SCRAM messages and peer-controlled input (the server's
  certificate); `InvalidCredential(String)` for a caller-supplied username or
  password that fails SASLprep or a PLAIN field containing NUL;
  `ServerRejected(ScramServerError)` for a typed SCRAM `e=` refusal;
  `LocalRuntime(String)` for a local cryptographic facility failure.
- `ScramHash` - `#[non_exhaustive]` `{ Sha1, Sha256 }`. Names the hash, not a
  protocol-layer mechanism. `mechanism_name(ChannelBinding)` returns the wire
  name, owning the `-PLUS` suffix in one place:
  `"SCRAM-SHA-1"` / `"SCRAM-SHA-256"` for `ChannelBinding::None`,
  `"SCRAM-SHA-1-PLUS"` / `"SCRAM-SHA-256-PLUS"` for
  `ChannelBinding::TlsServerEndPoint`.
- `ChannelBinding` - `#[non_exhaustive]` `{ None, TlsServerEndPoint }`. The
  channel-binding dimension, orthogonal to `ScramHash`, so the proof math stays
  hash-parameterized.
- `ScramChannelBinding` - `#[non_exhaustive]` data-carrying enum
  `{ None, SupportedButNotAdvertised, TlsServerEndPoint(Vec<u8>) }`: the GS2 channel-binding input to
  `scram_client_final`. `gs2_header()` returns the GS2 header string
  (`"n,,"` / `"y,,"` / `"p=tls-server-end-point,,"`); `binding()` returns the matching
  `ChannelBinding`. A data-carrying enum, not a discriminant-plus-data struct,
  so the GS2 header and the `c=` value never desync. A PLUS consumer MUST build
  its client-first header from `gs2_header()`, not a literal.
- `Mechanism` (`negotiation.rs`) owns the mechanism vocabulary, wire names,
  case-insensitive `from_name`, and SCRAM hash/binding projection. SASL `Login`
  and the legacy command `ImapLogin` are distinct identities. SMTP's public
  eight-variant `Mechanism` is a protocol support adapter to this vocabulary;
  it retains every previously selectable mechanism, including SASL LOGIN.
- `MechanismName` owns open wire identities: known names project to `Mechanism`,
  unknown extensions retain their text. IMAP AUTH capabilities, server profiles
  and AUTHENTICATE commands use it. Wire encoders still validate atom syntax.
- `ChannelBindingData` is opaque certificate-derived data, minted by
  `binding_from_peer_certificate(Option<&[u8]>)`. Absence returns `None`;
  certificate errors propagate rather than becoming a skip.
- `scram_rung(mechanism, advertised, binding)` replaces the old multi-purpose
  selector. Its `ScramRung` carries the mechanism and actual GS2 input;
  `ScramSkip` distinguishes NotAdvertised, PlusWithoutBinding and
  DowngradeForbidden. No synthetic empty binding or echoed input hash is needed.
  Unbound SCRAM with usable binding sends `y,,`; without it, `n,,`.
  A direct IMAP caller explicitly choosing unbound SCRAM retains `n,,`.
- `negotiate(advertised, policy, encrypted, binding)` owns the local ladder.
  `NegotiationPolicy` supplies the supported/allowed set, OAuth versus password
  credentials and explicit insecure opt-in. Results retain typed local skips
  alongside successful selections, whose SCRAM rung carries binding forward.
  IMAP performs binding-dependent resolution under its attempt deadline after
  the preliminary policy ladder; SMTP carries the resolved selection straight
  into the exchange. Neither retries a wire refusal.
- `tls_server_end_point(cert_der) -> Result<Vec<u8>, SaslError>` - RFC 5929
  Section 4 `tls-server-end-point` binding data: the hash of the whole
  DER-encoded certificate, using the hash named by the cert's own
  `signatureAlgorithm` with MD5/SHA-1 upgraded to SHA-256 (Section 4.1). A
  small in-crate TLV walk reads the signatureAlgorithm OID (no X.509 stack, no
  ASN.1 dependency); it rejects high-tag-number forms and non-minimal DER
  lengths. SHA-384/512 hash families are supported via `sha2`, and RSASSA-PSS
  reads its hash from the algorithm parameters.
  Unrecognized OIDs and EdDSA (Ed25519/Ed448) are a hard `Protocol` error
  rather than a guessed binding hash - a wrong binding value is a silent auth
  failure. Returns raw hash bytes; the caller base64-frames them.
- `prepare_scram_password(password) -> Result<ScramPassword, SaslError>` -
  RFC 4013 SASLprep of the password, zeroized on drop. `ScramPassword` is the
  only password type `scram_client_final` accepts, so the preparation, and
  its `InvalidCredential` refusal, necessarily happen before the exchange
  starts rather than after the server-first message.
- `scram_client_final(hash, password: &ScramPassword, client_nonce,
  client_first_bare, server_first, binding: &ScramChannelBinding) ->
  Result<(Secret, Vec<u8>), SaslError>` - parses the server-first message,
  runs PBKDF2/HMAC, returns the base64 client-final and the expected server
  signature. Protocol consumers write this value as returned and do not
  base64-encode it again. Errors describe the server-first message, except
  local HMAC facility failures, which use LocalRuntime. The `c=` attribute is
  `base64(gs2_header || cbind_data)` derived from `binding`; `None` is
  byte-identical to the old `c=biws`.
- `verify_server_final(server_final, expected_signature) -> Result<(),
  SaslError>` - checks for a server `e=` error, then verifies the `v=`
  signature.
- `prepare_scram_username(user) -> Result<String, SaslError>` - RFC 4013
  SASLprep followed by RFC 5802 saslname escaping of `=` / `,`. This is the
  only way a protocol crate builds an `n=` value; the bare escaper is
  crate-private precisely so a caller cannot reach for it and skip SASLprep.
  Prohibited, unassigned, and bidi-violating input is a typed
  `InvalidCredential` error rather than a misleading authentication failure
  or a protocol fault.
- `decode_continuation(data) -> Result<String, SaslError>` - base64-decode a
  SASL continuation to UTF-8.
- `cram_md5_response(user, pass, challenge) -> Result<Secret, SaslError>` -
  RFC 2195 CRAM-MD5 response.
- `plain_payload(authzid, authcid, password) -> Result<Secret, SaslError>`
  (`plain.rs`) - RFC 4616 raw PLAIN bytes, assembled directly in zeroizing
  storage. Refuses NUL in every field as `InvalidCredential` before framing.
  IMAP, ManageSieve and SMTP use it with an empty authorization identity. ManageSieve also uses the shared
  XOAUTH2 builder and keeps its base64 payload and authentication command in
  zeroizing storage.
- `OAuthMechanism` - `#[non_exhaustive]` `{ Xoauth2, OAuthBearer }`.
  `name()` returns the wire token; `error_ack()` returns the raw error answer
  as a static string: empty for XOAUTH2, SOH for OAUTHBEARER (RFC 7628
  Section 3.2.3). Protocol crates base64-frame the answer and append CRLF.
- `xoauth2_payload(user, access_token) -> Secret` /
  `oauthbearer_payload(identity, access_token) -> Secret` (`oauth.rs`) - OAuth
  SASL client payloads as a zeroizing `Secret` of the *raw* (un-base64'd)
  bytes; the protocol crate frames them (IMAP base64-encodes for AUTHENTICATE,
  SMTP's AUTH command base64-encodes downstream). XOAUTH2 has no GS2 framing so
  nothing is escaped; OAUTHBEARER's `a=` identity is GS2-escaped via
  `escape_username` (same RFC-derived escape table as SCRAM saslname) and the
  optional RFC 7628 `host=` / `port=` attributes are deliberately omitted to
  stay transport-agnostic. The frame-delimiter-stripped intermediate copies,
  including the access token when it contains `\x01`, use zeroizing storage.

The per-hash proof helpers, `scram_field`, `escape_username`, and the HMAC/XOR
primitives stay private to the crate. Password-derived SCRAM intermediates and the CRAM-MD5
pre-encoding response are zeroized on drop. Secret and verifier equality use
`subtle::ConstantTimeEq`. Duplicate SCRAM attributes are protocol errors.

## Error mapping contract

`ScramServerError` owns the server-final token vocabulary, preserving extensions
as `Other(String)`. Its `refusal_class()` is the recovery authority for both
protocols. No consumer parses display text to recover a token.

| Server error | Refusal class | Ask for replacement credentials? |
|---|---|---|
| invalid-proof, unknown-user | Credential | Yes, repair password or account identity |
| invalid-encoding, invalid-username-encoding | Protocol | No |
| extensions-not-supported | Policy | No |
| channel-bindings-dont-match | Policy | No |
| server-does-support-channel-binding | Policy | No |
| channel-binding-not-supported | Policy | No |
| unsupported-channel-binding-type | Policy | No |
| no-resources, other-error | Unavailable | No, retry |
| Unknown extension token | Unavailable | No, retry |

Enterprise users expect replacement credentials only when the server gives
credential-loss evidence. An unspecified failure or an unknown extension is
insufficient evidence; retry retains the original token for support. Binding
failures require configuration or security repair and never justify descending
to a weaker mechanism.

Both protocols map Credential to reauthorization, Policy to policy blocked,
Protocol to protocol/parse failure, and Unavailable to server unavailable.
IMAP retains the typed refusal on `Error::Auth` through cancellation completion,
without inventing an IMAP response code; SMTP uses `ErrorKind::ServerUnavailable`
for the retryable case. `Protocol` remains protocol/parse-class,
`InvalidCredential` remains local invalid input, and `LocalRuntime` maps to each
protocol's local-runtime error. HMAC key construction failures use that local
lane rather than blaming peer input. Signature mismatch remains `Protocol`.

Both protocol crates prepare the username AND password while building the
exchange, before the first byte of AUTHENTICATE / AUTH is sent, so an
`InvalidCredential` is always a local refusal: it follows the cross-crate
rule in `reference/error-model.md` ("Local refusals") - never a provider
fault, and never a reason to retire the connection. Everything a protocol
crate still maps from `Protocol` is either the peer's fault or raised
mid-exchange, where retiring the connection is required.

## Correctness pins

Unit tests in the crate are the load-bearing proof that the computation is
correct:

- RFC 5802 SHA-1 client-final + server signature (`scram.rs`), now passing
  `ScramChannelBinding::None` - proves the `None` path stays byte-identical to
  the old `c=biws`.
- Shared policy (`negotiation.rs`): mechanism identity and extension retention,
  TLS refusal for all mechanisms with explicit opt-in, bound SHA-1 preference,
  PLUS allow-list downgrade suppression, and canonical OAuth preference with
  XOAUTH2 still selectable.
- Shared SCRAM negotiation (`scram.rs`): both hashes select `n`, `y`, and `p`
  with the exact GS2 header and `c=` bytes; unavailable, unadvertised, and
  suppressed rungs are rejected. IMAP and SMTP consumer tests independently
  pin client-first headers and client-final `c=` values for all three states.
- PLAIN framing and zeroizing storage (`plain.rs`), plus NUL refusal in each
  of the authorization identity, authentication identity, and password fields.
- RFC 2195 CRAM-MD5 vector (`cram.rs`).
- SCRAM-PLUS construction (`scram.rs`): a constructed deterministic pin (no
  published RFC 5802 PLUS vector exists) that the `c=` field equals
  `base64("p=tls-server-end-point,," || cbind)` and that PLUS and non-PLUS
  diverge in proof + server signature for the same inputs.
- RFC 5929 cert binding (`channel_binding.rs`): `tls_server_end_point` over a
  hand-built DER hashes the whole DER under the family named by the
  signatureAlgorithm OID; an SHA-1-signed cert upgrades to SHA-256; truncated,
  non-minimal length, high-tag-number, unrecognized-OID, and EdDSA certs are
  `Protocol` errors. The OID-to-family table is exercised directly so a
  mistyped OID byte fails a type-level test.
- RSASSA-PSS parameters (`channel_binding.rs`): the hash comes from the RFC
  4055 `RSASSA-PSS-params` `[0] hashAlgorithm`, defaulting to sha1 (upgraded to
  SHA-256) when the field or the whole `parameters` element is absent. The
  entire parameter SEQUENCE is parsed before a hash is returned, not just the
  prefix up to `[0]`: DER admits each context tag at most once and only in
  ascending order, so a duplicate `[0]`, a reordered field, or an out-of-range
  tag is a `Protocol` error rather than a binding computed from whichever hash
  the attacker put first. Pins cover an explicit SHA-384 parameter, the default
  upgrade, absent parameters, trailing `[2]`/`[3]` fields, an unknown hash OID,
  and each of the three rejection cases.
- SCRAM `i=` bounds (`scram.rs`): the server-supplied iteration count is
  rejected below the RFC 7677 floor of 4096 and above a DoS ceiling of
  100,000,000. The floor is a downgrade defence, not hygiene - `i=1` reduces
  `SaltedPassword` to one PBKDF2 round and makes the captured `p=` proof cheap
  to brute-force offline.
- RFC 4013 SASLprep examples (`scram.rs`): the complete RFC 4013 Section 3
  example table is pinned on `prepare_scram_username` - mapped-to-nothing, the
  two no-op rows (case is *not* folded), both NFKC rows, the prohibited code
  point, and the bidi violation. A password containing SOFT HYPHEN, prepared
  through `prepare_scram_password`, maps to the RFC 5802 SHA-1 vector's
  `pencil` and produces that vector's absolute client proof, which is what
  proves real SASLprep runs on the PBKDF2 input.
- Duplicate attributes (`scram.rs`): repeated server-first and server-final
  keys are rejected rather than selecting the first value, including keys the
  client never reads (an unknown `x=` extension). The server-final pin
  duplicates a *correct* verifier, so it fails only on the duplicate guard and
  not on some downstream length or value check.

## Forward note

IMAP and SMTP both drive the PLUS path: their consumers send the
`p=tls-server-end-point,,` GS2 header, pull the peer cert DER, select PLUS when
advertised, and enforce RFC 5802 Section 6 downgrade protection (see
`reference/imap.md` / `reference/smtp.md` and git history). SMTP consumes
`ScramHash` / `ScramChannelBinding` / `scram_client_final` /
`verify_server_final` via `ScramExchange`. OAuth (XOAUTH2 / OAUTHBEARER)
payload construction now lives here (`xoauth2_payload` / `oauthbearer_payload`),
collapsing the formerly duplicated IMAP and SMTP builders. The crate owns SCRAM rung eligibility and GS2 selection via
`scram_rung` and `negotiate`; protocol crates retain wire sequencing.
EdDSA leaf-cert channel binding is deliberately a hard error until an
evidence-driven vector pins the binding hash a real server uses.

## Mechanism preference

`negotiation.rs` is the policy owner for all three protocol consumers.
`authentication_allowed` requires TLS for every automatically selected
mechanism on every protocol, SCRAM included (IMAP SCRAM used to be allowed over
plaintext), unless the caller explicitly opts into insecure authentication.
This is a repository owner ruling: an enterprise client should not send
credentials or a SCRAM exchange over an unencrypted channel by default. SCRAM
hides the password but not the exchange, and it cannot protect the mail or
script traffic that follows. IMAP's `allow_cleartext_without_tls`, SMTP's
`dangerous_allow_insecure_auth` and ManageSieve's auth policy preserve that
explicit escape hatch. Direct IMAP mechanism methods remain explicit choices.

The canonical password order is SCRAM-SHA-256-PLUS, SCRAM-SHA-1-PLUS,
SCRAM-SHA-256, SCRAM-SHA-1, PLAIN, CRAM-MD5, SASL LOGIN, IMAP LOGIN. A bound
SHA-1 exchange outranks unbound SHA-256. Protocol support sets filter this order:
SMTP supports its existing eight mechanisms; IMAP supports its existing nine
choices, with CRAM-MD5 and legacy LOGIN opt-in; ManageSieve currently supports
only PLAIN and XOAUTH2. No new wire exchange is implied by the common vocabulary.
LOGIN is opt-in for SMTP as well. Enterprise users expect a legacy method only
when configured, rather than an automatic downgrade.

OAuth is an orthogonal credential path. `OAUTH_PREFERENCE` chooses standardized
OAUTHBEARER before XOAUTH2, independent of the allow-list order. A caller can
still select XOAUTH2 by allowing only it.

RFC 5802 section 6 suppresses an unbound SCRAM rung whenever the matching PLUS
mechanism is advertised, even if PLUS is excluded by the caller's allow-list.
A missing certificate may skip PLUS; a present certificate that cannot be
processed is a hard error. The real certificate-derived binding travels with
SMTP's selection into the exchange; there is no second selection or assertion
that the selected rung remains eligible. IMAP skips only local refusals and live
capability disappearance; both protocols treat a wire rejection as final.

## Non-goals

- `tls-unique` channel binding. TLS 1.3 removed the extractor and bifrost only
  supports modern TLS, so `tls-server-end-point` is the only binding offered.
- SCRAM-SHA-512 family. Not implemented; revisit if a real deployment requires
  it.
- GSSAPI / Kerberos. Different stack, no demand.
- A typed public auth-outcome surface (which mechanism + binding were used, or a
  typed failure reason) is not built; the protocol crates map into their
  existing error types. Not scheduled.

SCRAM server-final parsing rejects an empty `e=` and a message containing both
`e=` and `v=` as protocol faults before interpreting a typed server refusal.
