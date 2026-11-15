# Hunt: smtp-sasl - wrong shape, wrong place

Scope: `bifrost-smtp` (`crates/smtp/`) and `bifrost-sasl` (`crates/sasl/`), plus
how `bifrost-imap` consumes SASL (`connection/auth.rs`,
`connection/dispatch/auth.rs`, `account/sieve.rs`, `types/auth.rs`,
`types/secret.rs`) and composes SMTP for submission (`account/submission.rs`,
`account/pim.rs`). This is a reading. No code was changed.

The short version: bifrost-sasl owns only the math. Everything around the math
(the mechanism vocabulary, the preference ladder, the cleartext gate, the SCRAM
state machine, nonce generation, the PLAIN and OAuth error-continuation
payloads, the binding-from-certificate policy and the classification of a SASL
refusal) is re-decided in IMAP, in IMAP's ManageSieve client, and in SMTP, held
in step by comments that say "mirrors" and "must not differ". Several have
already drifted. Inside bifrost-smtp the dominant shape problem is a 2x2
product (SMTP/LMTP dialect times blocking/async runtime) that is materialised
four times over: four transports, four builders, four protocol machines, two
connection adapters that each re-implement the session (greeting, EHLO,
STARTTLS, AUTH, reply framing), two pools, and two dial sequences.

Findings are tagged **[defect]** (live or latent wrong behaviour),
**[drift]** (a duplicated decision that already disagrees), **[dup]** (a
duplicated decision that agrees today), **[axis]** (a primitive or prose where a
type belongs) or **[structure]**. Anything that would remove or rename a
published item is marked **[ask owner]**; nothing here should be read as a
mandate to delete published surface.

---

## 0. Ranked headline list

1. **[defect][drift]** IMAP turns every OAuth token-source failure into
   server-acknowledged `AuthLost`; SMTP preserves bifrost-net's typed
   classification. (2.2)
2. **[drift]** A SCRAM `e=` server error other than `invalid-proof` is
   `AuthLost` on IMAP and `PolicyBlocked` on SMTP. (2.3)
3. **[defect][drift]** ManageSieve builds its own XOAUTH2 payload by
   `format!` (no `\x01` strip, no zeroizing) and its own PLAIN payload; IMAP
   PLAIN and ManageSieve PLAIN have no NUL check, SMTP PLAIN refuses NUL.
   The OAuth consolidation commit missed ManageSieve. (2.1)
4. **[drift]** The OAuth error-continuation answer differs: IMAP sends an
   empty line for both XOAUTH2 and OAUTHBEARER, SMTP sends `\x01` for both.
   Each crate is right for one mechanism and wrong for the other. (2.4)
5. **[drift]** SCRAM nonce RNG failure: IMAP `LocalRuntime` (internal), SMTP
   `InvalidInput` + Auth phase, which maps to `PolicyBlocked`. (2.5)
6. **[defect]** SMTP envelope addresses from IMAP submission go through
   `Address::new_dangerous`; only control characters are refused, so a
   recipient string containing a space and `>` can append ESMTP parameters
   (`NOTIFY=`, `ORCPT=`) to `RCPT TO`. (1.6)
7. **[drift]** Reverse-path choice: bifrost-smtp's `Envelope::try_from(&Headers)`
   prefers `Sender`, IMAP's `parse_draft_for_submission` prefers `From`. (2.9)
8. **[defect]** `SendProgress::body_started` / `body_finished` are written by
   the machines and read by nothing; `resolve` decides "may the body have
   left" from `data_response` alone. (1.9)
9. **[structure]** Four near-identical protocol machines in `client/core.rs`;
   one has already drifted and been patched back by comment. (3.2)
10. **[structure]** The sans-IO core stops at the transaction; the session
    (greeting, EHLO, STARTTLS, AUTH ladder, SCRAM exchange, reply framing) is
    written twice, in `connection.rs` and `async_connection.rs`. (3.1)
11. **[structure]** bifrost-sasl should own the SASL client, not just the
    SCRAM arithmetic. (3.5)
12. **[defect]** The `account-error` feature cannot be turned off: bifrost-types
    is used unconditionally, so `--no-default-features` does not compile and no
    sweep checks it. `SmtpTransmissionState` exists only to serve that dead
    gate. (3.7)

---

## 1. Axes that should be types

### 1.1 [axis] The SMTP/LMTP dialect travels as `bifrost_types::error::Protocol`

`SmtpInfo::protocol`, `SmtpConnection`/`AsyncSmtpConnection::protocol`,
`SendProgress::protocol`, `SmtpErrorContext::protocol` and
`ConnectionOptions::protocol` all carry the error-model `Protocol` enum (Imap,
Jmap, Graph, ...), not a two-valued wire dialect. The consequences are visible
in the code:

- `SmtpErrorContext::send` opens with
  `assert!(matches!(protocol, Protocol::Smtp | Protocol::Lmtp))` - a runtime
  check for a fact a type would make unconstructible.
- `SmtpInfo::new` and `SmtpInfo::ensure_can_authenticate` each carry a
  `_ => SMTP_PORT` / `_ => "SMTP"` arm meaning "an IMAP-tagged transport,
  treat it as SMTP".
- `SendProgress::resolve` has a `debug_assert!(protocol == Protocol::Smtp)`
  because SMTP and LMTP resolve `Accepted` differently and the struct cannot
  tell them apart at the type level.

Shape: a crate-local `enum Dialect { Smtp, Lmtp }` (or a type parameter, see
3.2) with `impl From<Dialect> for Protocol` used only at the account-error
boundary. The assert, the wildcard arms and the debug_assert go away.

### 1.2 [axis] Mechanism names as `&'static str` on error stamps

`SmtpCommandPhase::Auth { mechanism: Option<&'static str> }` (smtp `error.rs`)
and IMAP `Error::Auth { mechanism: Option<&'static str> }` carry the mechanism
as a string even though each crate has a mechanism enum (`Mechanism`,
`AuthMechanism`). The comments justify the string as "the name that went out
on the wire", but that is a property of the enum's `name()`, not a reason to
erase the enum. `ScramExchange` stores both `hash` and a separately computed
`mechanism_name: &'static str`; IMAP's `AuthenticateXoauth2Consumer` carries
`mechanism: &'static str` because one consumer serves two mechanisms. Typed,
these become `Option<Mechanism>` and the diagnostic text is derived at the
boundary.

### 1.3 [axis] SCRAM server errors are prose with a magic prefix

`SaslError::AuthFailed(String)` is built in `verify_server_final` as
`format!("SCRAM server error: {error}")`, and
`SaslError::scram_server_error()` recovers the token with
`strip_prefix("SCRAM server error: ")`. SMTP's `From<SaslError>` branches on
`scram_server_error() == Some("invalid-proof")`. Tests on both sides assert on
the exact prose. RFC 5802 section 7 enumerates the `server-error-value`
vocabulary (`invalid-encoding`, `extensions-not-supported`, `invalid-proof`,
`channel-bindings-dont-match`, `server-does-support-channel-binding`,
`channel-binding-not-supported`, `unsupported-channel-binding-type`,
`unknown-user`, `invalid-username-encoding`, `no-resources`,
`other-error`, plus extensions). Shape:

```rust
pub enum SaslError {
    Protocol(String),
    InvalidCredential(String),
    ServerRejected(ScramServerError),   // typed, with Other(String)
}
```

with `ScramServerError::is_credential_rejection()` owned by bifrost-sasl.
This is also what makes 2.3 fixable in one place.

### 1.4 [axis] `select_scram_rung` answers three questions through one signature

`select_scram_rung(hash, plus: bool, advertised: impl Fn(&str) -> bool,
binding_data: Option<&[u8]>) -> Option<(ScramHash, ScramChannelBinding)>` is
called four different ways, and each caller decodes it differently:

- SMTP `password_mechanism` passes `binding_available.then_some(&[][..])` - a
  fake empty binding - only to learn whether a rung is eligible.
- IMAP `password_mechanism_ladder` passes `None` and `plus = false` to ask a
  different question ("is the unbound rung downgrade-forbidden").
- IMAP `authenticate_best` destructures its own resolved binding with
  `_ => unreachable!("certificate resolver returns bound data")` to feed the
  bytes back in.
- SMTP `SmtpConnection::auth` (and the async twin) call it again after
  selection with `.expect("selected SCRAM rung remains advertised")`.

The echoed `ScramHash` in the return is never different from the input. The
`plus: bool` duplicates `ChannelBinding`. The binding bytes are a bare
`Vec<u8>` / `&[u8]` that anything can mint; only `tls_server_end_point` should.

Shape: a `ChannelBindingData` newtype only `tls_server_end_point` constructs,
`binding_from_peer_certificate(Option<&[u8]>) -> Result<Option<ChannelBindingData>>`
(see 2.6), and a negotiation function that returns the ordered, already
filtered rung list with a typed reason per skipped rung:

```rust
pub fn scram_rungs(advertised: &dyn Fn(&str) -> bool,
                   binding: Option<&ChannelBindingData>)
    -> Vec<Result<ScramRung, ScramSkip>>;   // ScramSkip::{NotAdvertised, PlusWithoutBinding, DowngradeForbidden}
```

IMAP's `AuthMechanismRejectionReason::ChannelBindingUnavailable` and SMTP's
`skipped_plus_for_binding` both fall out of `ScramSkip`.

### 1.5 [axis] LOGIN state as `challenge_index: usize`; OAuth token as `Option<&str>`

`Mechanism::response_with_token_at_index(credentials, challenge,
challenge_index: usize, oauth_token: Option<&str>)` is a stateless encoder
pretending to be one. LOGIN's "username then password" is driven by an index
the connection loop must count correctly (the comment warns it must not be
derived from the remaining-challenge budget). The OAuth token is an
`Option<&str>` whose `None` is a "driver bug" error at runtime
(`oauth_identity_and_token`). Both are the same smell as 3.5: a per-mechanism
exchange is a small state machine, and the crate models only SCRAM that way
(`ScramExchange`). Shape: one `SaslClient` trait / enum in bifrost-sasl with
`initial_response()` and `step(challenge) -> Step`, constructed with whatever
it needs (a resolved token for OAuth), so "OAuth mechanism without a token"
cannot be constructed.

### 1.6 [axis][defect] Envelope addresses: `new_dangerous` at the account boundary

`account/submission.rs::to_smtp_address` builds every MAIL FROM and RCPT TO
address with `SmtpAddress::new_dangerous(user, domain)` after
`rsplit_once('@')`, or `new_dangerous(whole, "")` when there is no `@`. The
comment says the server rejects an invalid RCPT. The only client-side guard
left is `commands.rs::validate_single_line_argument`, which rejects control
characters and nothing else. `Rcpt`'s Display is `RCPT TO:<{addr}>{params}`,
so an address string such as `a@b.example> NOTIFY=SUCCESS ORCPT=rfc822;x`
produces a syntactically valid command with caller-chosen DSN parameters. The
string originates in `bifrost_types::Address::address` from a `SendRequest`
(see `types/src/mime/render.rs::SubmissionEnvelope`), i.e. user-typed
recipients in a mail client. `reference/types.md` already says submit paths
must use the checked parsers and propagate rejection before writing; the
conversion to the wire type is the one place that is not checked.

Owner: bifrost-smtp already depends on bifrost-types. A
`impl TryFrom<&bifrost_types::Address> for bifrost_smtp::Address` that runs
`Address::new`'s validation (local part via `email_address`, IDNA domain)
belongs in bifrost-smtp, and `to_smtp_address` should be deleted in favour of
it, with a rejection becoming a per-recipient `failed` lane (or a batch
`BatchInputInvalid`) before MAIL FROM.

### 1.7 [axis] `HOLDUNTIL` is a `String`; IMAP feeds it `unwrap_or_default()`

`FutureReleaseParameter::HoldUntil(String)` and `SendOptions::hold_until(impl
Into<String>)` take free text; validation is the generic
`validate_esmtp_raw_value` at send time (non-empty printable ASCII, no `=`),
never RFC 3339. IMAP's `hold_until_rfc3339` turns a `SystemTime` that
`jiff::Timestamp::try_from` rejects into `""`, which then fails at send time as
"raw ESMTP parameter value must contain printable ASCII without spaces or
'='" - a misleading `Request(Malformed)`. Shape:
`HoldUntil(jiff::Timestamp)` (or a validated newtype) with a
`hold_until(SystemTime) -> Result<Self, Error>` constructor, so the only
failure is at the one place the time is converted. The same applies to
`MailParameter` in general: `DeliverByParameter` and `MtPriorityParameter`
validate in their constructors and keep fields private, while
`MailParameter::EnvelopeId(String)`, `Other`, `OtherRaw` and
`RcptParameter::OriginalRecipient` are public variants validated later by
`validate_syntax`. Two validation models for one enum.

### 1.8 [axis] Batch item identity round-trips through a decimal string

`send_rfc5322` mints `BatchItemId(index.to_string())` and
`not_accepted_recipients` parses it back with `id.0.parse::<usize>()`,
skipping ids that do not parse. The index is the real key; the string is a
lossy carrier. `SmtpBatchRecipient` could carry the caller's own payload (a
generic `BatchItem<T>` already exists in bifrost-types); then IMAP gets its
`bifrost_types::Address` back from the lane directly and the parse disappears.

### 1.9 [axis][defect] `SendProgress` phase flags that nothing reads

`SendProgress { body_started: bool, body_finished: bool, data_response:
Option<Response> }`. `core.rs` calls `set_body_started` / `set_body_finished`;
nothing in production reads either field. `resolve` decides every `Accepted`
recipient's lane from `data_response` alone: `Some(positive)` succeeded,
`Some(negative)` failed, `None` uncertain with an `InFlight`
partial-completion error. That last arm is only correct when the body may have
left; correctness of a pre-body exit depends on the machine having already
called `mark_unresolved_unsent`. The flags look like they were meant to gate
exactly that default and never got wired. The batch tests set the flags,
which makes the suite read as though they matter.

Shape: one transaction-phase enum the machine advances and `resolve` matches:
`Envelope -> DataIssued -> BodySent -> FinalReply(Response)`; `resolve` then
maps `Envelope` to `Unsent`, `BodySent` to `Uncertain`, and so on, and the
three-field split disappears. Audit for bite: with the phase enum in place,
the pre-body exits stop depending on a separate `mark_*` call.

### 1.10 [axis] Enhanced status codes demoted to consent-tier prose at the boundary

bifrost-smtp parses a typed `EnhancedStatusCode { class, subject, detail }`.
At the account boundary `smtp_wire_cause` converts it to
`bifrost_types::error::EnhancedStatusCode { reply: u16, enhanced:
Option<DiagnosticText>, text }`, and `response_to_account_error` sets
`native_code` to the same dotted string. The structured, non-secret protocol
fact `5.1.1` reaches consumers only as support-only text behind the diagnostics
consent tier; a machine caller cannot branch on X.1.1 vs X.2.2 without parsing
prose it may not even be allowed to read. The type named
`EnhancedStatusCode` in bifrost-types should carry the three numbers.
(bifrost-types change, owned there; flagged here because SMTP is its only
producer.)

### 1.11 [axis] "Positive" means two things

`Response::is_positive` (and `Code::is_positive`) is true for 2yz and 3yz.
It is used for "the DATA command was accepted" (wants 354), "the message was
accepted" (wants 250), "RSET was acknowledged" (`Epilogue::ResetReading`) and,
in `SendProgress::resolve`, "the recipient's message was delivered". A 3yz
final reply after end-of-data would resolve as delivered. Each command expects
one class; a `Response::is_completion()` / `is_intermediate()` split, or an
`expect(ReplyClass)` at each read, makes the wrong acceptance impossible.

### 1.12 [axis] Error kind plus phase encodes classification

In `account_error.rs::into_account_error`, `ErrorKind::InvalidInput` means
`Request(Malformed)` except when the phase is `Auth { .. }`, where it means
`Authorization(PolicyBlocked)`. Producers rely on that pairing to steer
classification:

- local "no compatible authentication mechanism" and "channel binding required
  but unavailable" (`password_mechanism`),
- SCRAM server errors other than invalid-proof (`From<SaslError>`),
- "OAuth token source requires refresh or async resolution; use the async SMTP
  transport" (`oauth2_token_blocking`),
- SCRAM nonce RNG failure (`generate_scram_nonce`; see 2.5),
- legacy encoder protocol errors such as "This mechanism does not expect a
  challenge", which `try_smtp!(.., phase)` stamps Auth.

These are five different facts (local negotiation failure, server policy
refusal, wrong transport for this token source, local runtime failure, server
protocol violation) flattened into one (kind, phase) pair, and three of them
land on the wrong recovery class. A typed `ErrorKind::Auth(AuthFailure)` with
variants for each, classified once, would end the coupling. The same file's
`SmtpErrorContext` has four fields (`operation`, `provider`,
`idempotency_override`, `transmission_state`) that the only constructor,
`send()`, always sets to the same constants; the struct is a `Dialect` in
disguise (1.1).

### 1.13 [axis] Two booleans selecting a reply mode

`SmtpConnection::read_response_inner(accept_negative: bool, manage_state:
bool)` (and its async twin) is a four-valued mode as two bools; only three
combinations are used. A `ReplyMode` enum names them.

### 1.14 [axis] `authentication: Vec<Mechanism>` plus `authentication_configured: bool`

`SmtpInfo` stores the mechanism list and a flag saying whether the list was
explicit, and `set_credentials` rewrites the list from
`credentials.preferred_mechanisms()` unless the flag is set. The pair is an
`enum MechanismChoice { Default, Explicit(Vec<Mechanism>) }`, resolved against
the credential kind at use, not at set time (today, setting credentials after
an explicit list keeps the list; setting them before makes the default stick
until `authentication` is called).

---

## 2. Decisions made in more than one place

### 2.1 [defect][drift] SASL payload construction: three PLAIN builders, two XOAUTH2 builders

Question: what bytes does PLAIN / XOAUTH2 put on the wire, and what input is
refused?

| Site | PLAIN | XOAUTH2 |
|---|---|---|
| SMTP `Mechanism::response_with_token_at_index` | `\0user\0pass`, **refuses NUL** in either field | `bifrost_sasl::xoauth2_payload` |
| IMAP `ImapConnection::authenticate_plain` | `\0user\0pass`, **no NUL check** | `bifrost_sasl::xoauth2_payload` |
| IMAP `account/sieve.rs` (ManageSieve AUTHENTICATE) | `\0user\0pass` in a plain `Vec`, **no NUL check**, not zeroizing | **`format!("user={identity}\x01auth=Bearer {}\x01\x01")`**: no `\x01` strip, not zeroizing |

They already disagree. SMTP's comment explains why NUL must be refused (an
`authzid\0authcid` in the username slot is an authorization-identity
injection); IMAP and ManageSieve accept it. The `\x01` frame-injection defence
that commit 26aa7c25 moved into bifrost-sasl was never applied to ManageSieve,
which also bypasses the zeroizing `Secret`. ManageSieve offers only PLAIN or
XOAUTH2 (no OAUTHBEARER, no SCRAM) and gates the cleartext policy after
building the payload.

Owner: bifrost-sasl. Add `plain_payload(authzid, authcid, password) ->
Result<Secret, SaslError>` (with the NUL refusal as `InvalidCredential`) next
to the OAuth builders, and route all three sites through it and through the
existing `xoauth2_payload`.

### 2.2 [defect][drift] Classifying an OAuth token-source failure

Question: what does a failure to read the bearer token mean for recovery?

- SMTP `oauth_token_error` projects the `bifrost_net::Error` through
  `into_account_error_with_throttle_scope` and tunnels the typed result with
  `with_preclassified_account_error`, explicitly so a transient refresh failure
  is not flattened into a terminal one.
- IMAP `authenticate_best` maps it to
  `Error::auth_with_mechanism(format!("failed to read OAuth access token: {e}"),
  None, ..)`. `classify_auth` with no response code yields
  `Authentication(ReauthorizationRequired)` and stamps
  `TransmissionState::Acknowledged` ("auth failures are server-acknowledged
  tagged responses").
- IMAP `account/sieve.rs` does the same via `Error::auth_with_code(.., None)`.

So on IMAP a token endpoint outage, a `RefreshFailed { Network }`, or the
`Cancelled` lost-driver case `reference/net.md` explicitly says must NOT be
`AuthLost`, all become terminal `AuthLost` with false "the server answered"
evidence, sending the user to re-authorize an account whose token is fine.
net.md records fixing exactly this in bifrost-net; the IMAP consumer
reintroduces it one layer up.

Owner: the projection already exists in bifrost-net
(`into_account_error_with_throttle_scope`). IMAP needs an `Error` variant that
carries a preclassified `AccountError` (SMTP's tunnel) or a typed token
failure; the decision "token-source failure maps through net's table" should
be made once. A helper in bifrost-net such as
`token_failure_account_error(error, protocol, operation)` would give IMAP,
ManageSieve and SMTP one call.

Related smell: SMTP's `oauth2_token_blocking` turns
`TokenSnapshot::Contended` into a fabricated
`bifrost_net::Error::RefreshFailed { source: Network { "OAuth token cache is
busy" } }` purely to obtain a transient classification from net's table. The
classification of a snapshot state belongs on `TokenSnapshot` (or in net's
projection) rather than being reverse-engineered by minting a fake network
error.

### 2.3 [drift] Classifying a SCRAM server error (`e=`)

Question: is a SCRAM server-final `e=` value a credential rejection, a policy
refusal, or something else?

- SMTP `From<SaslError>`: `invalid-proof` -> `ErrorKind::Authentication`
  (AuthLost); any other value -> `InvalidInput` + Auth phase ->
  `Authorization(PolicyBlocked)`. Pinned by
  `a_scram_policy_error_carries_its_rung_through_the_policy_lane`.
- IMAP `From<SaslError>` and `AuthenticateScramConsumer::named_sasl_error`:
  every `AuthFailed` -> `Error::Auth` -> `ReauthorizationRequired` -> AuthLost.

`e=channel-binding-not-supported` is a policy block on SMTP and "re-enter your
password" on IMAP, for the same server and the same credential. IMAP's
consumer comment says it "mirrors `bifrost_smtp`'s `named_sasl_error`; the two
crates must not differ" - they differ in the part that matters (the class), and
agree only on the message suffix.

Owner: bifrost-sasl, via the typed `ScramServerError` of 1.3 and a single
`is_credential_rejection()` / `refusal_class()`; each protocol crate then maps
one typed class rather than re-deciding the token table.

### 2.4 [drift] Answering an OAuth error continuation

Question: after a failed XOAUTH2 / OAUTHBEARER, the server sends a
continuation carrying a JSON error; what does the client send back?

- IMAP `AuthenticateXoauth2Consumer` (shared by both mechanisms): an empty
  line.
- SMTP `Mechanism::response_with_token_at_index`: `\x01` (base64 `AQ==`) for
  both.

RFC 7628 section 3.2.3 has the OAUTHBEARER client answer with `%x01`;
Google's XOAUTH2 documentation has the client send an empty response. Each
crate is right for one mechanism and wrong for the other. The commit that
consolidated payload construction explicitly left "the RFC 7628 `\x01`
error-continuation handling" in SMTP as "protocol command flow"; it is a
per-mechanism SASL rule, not protocol flow, and belongs in bifrost-sasl
(`oauth_error_ack(mechanism) -> &'static [u8]`).

### 2.5 [drift] SCRAM nonce generation and its failure class

`generate_scram_nonce` exists twice, byte-for-byte the same shape (18 random
bytes, URL-safe-no-pad; SMTP's doc says "verbatim shape of IMAP's"). On
`getrandom` failure IMAP returns `Error::LocalRuntime` (internal, nothing on
the wire) and SMTP returns `invalid_input(..).with_phase(Auth)`, which maps to
`Authorization(PolicyBlocked)` - telling the operator to change policy because
the OS RNG failed. Owner: bifrost-sasl, inside the SCRAM client of 3.5, with a
single `SaslError::LocalRuntime`-style variant.

### 2.6 [dup] Channel binding from a peer certificate

`bifrost_smtp::authentication::resolve_scram_binding` and IMAP's
`scram_binding_from_der` are the same five lines (absent DER is the only skip;
present but unusable is an error, never a downgrade). IMAP's doc says it
"reads identically to `bifrost_smtp`'s `resolve_scram_binding`" and the
`resolve_scram_binding` doc in IMAP says "SMTP resolves this the same way; the
two crates must not differ here". That is the comment-held pair the brief
describes. Owner: bifrost-sasl
(`binding_from_peer_certificate(Option<&[u8]>)`), which is also the only
legitimate minter of the binding newtype in 1.4.

### 2.7 [drift] Mechanism vocabulary, preference ladder and cleartext gate

Question: which mechanisms exist, in what order are they tried, and which are
allowed on an unencrypted connection?

- Vocabulary: SMTP `Mechanism` (public, serde) has no CRAM-MD5; IMAP
  `AuthMechanism` (crate-private) has CRAM-MD5 and treats LOGIN as a command.
  bifrost-sasl implements CRAM-MD5 but only IMAP can reach it. SMTP parses EHLO
  names in `extension.rs::insert_auth_mechanisms`; IMAP matches
  `Capability::Auth(String)` case-insensitively in several helpers. The
  `-PLUS` spelling is already single-sourced in bifrost-sasl; the other names
  are not.
- Order: SMTP has two lists in one file, `PASSWORD_MECHANISMS` (the default
  allow-set, without LOGIN) and `MECHANISM_PREFERENCE` (the walk order, with
  LOGIN), plus `DEFAULT_MECHANISMS` aliasing the first. IMAP has a third
  literal array in `password_mechanism_ladder`. OAuth order: SMTP
  `OAUTH2_MECHANISMS` in caller order; IMAP hard-codes OAUTHBEARER then
  XOAUTH2 with a comment "mirroring SMTP's `OAUTH2_MECHANISMS` order".
- Cleartext gate: IMAP gates PLAIN, CRAM-MD5, LOGIN and OAuth per mechanism
  and allows SCRAM over plaintext ("SCRAM never sends reusable credentials").
  SMTP's `SmtpInfo::ensure_can_authenticate` refuses all AUTH on an unencrypted
  connection, SCRAM included, unless `dangerous_allow_insecure_auth`.
  ManageSieve gates everything. Three answers.
- Policy model: IMAP `AuthPolicy { allow_login, allow_cleartext_without_tls,
  allow_cram_md5 }`; SMTP an allow-list `Vec<Mechanism>` plus one bool.

Owner: bifrost-sasl. A shared `Mechanism` enum with `name()`,
`from_advertised(&str)`, `sends_reusable_secret()`, `is_bearer()`, and one
`negotiate(advertised, policy, encrypted, binding) -> Ladder` that both
protocol crates (and ManageSieve) walk. **[ask owner]** for what SMTP's
public `Mechanism` becomes (a re-export of the shared enum keeps the name; a
different variant set is a shape change).

### 2.8 [dup] SCRAM exchange state machine

SMTP `ScramExchange` ("Mirrors IMAP's `AuthenticateScramConsumer`") and IMAP
`AuthenticateScramConsumer` each hold `client_nonce`, `client_first_bare`,
`binding`, the prepared password, `expected_server_signature` and a
three-or-four-state enum; each formats `n={user},r={nonce}`, prefixes the GS2
header, calls `scram_client_final`, stores the signature, then calls
`verify_server_final`. bifrost-sasl's own doc warns that "a PLUS consumer MUST
build client-first from this, not from a literal, or the desync this type
prevents re-enters at the consumer boundary" - a rule that exists only because
client-first construction lives in the consumers. The SMTP side also owns the
"server-final folded onto 235" field quirk and the enhanced-code skip in
`decode_scram_payload`, in both adapters (3.1). Owner: bifrost-sasl
(`ScramClient`), see 3.5.

### 2.9 [drift] Deriving the submission envelope from headers

Question: given RFC 5322 headers, which address is the reverse path and which
are recipients?

- bifrost-smtp `Envelope::try_from(&Headers)`: `Sender` wins over `From`
  (pinned by `envelope_from_headers_sender` in `lib.rs`).
- IMAP `pim.rs::parse_draft_for_submission` (draft_send, send_raw_message):
  first `From`, else first `Sender`, else the configured default.
- bifrost-types `mime::render` (send_message): the resolved `from` (request or
  default); `Sender` is not modelled.

The same raw message sent through bifrost-smtp's `Transport::send` and through
the IMAP account's `send_raw_message` gets a different MAIL FROM. RFC 5322
makes `Sender` the agent responsible for transmission, which is the
conventional bounce target; IMAP chose the opposite without saying why. One
owner: a function on bifrost-types' `HeaderMap` (`submission_envelope()`),
used by IMAP and by bifrost-smtp's `Envelope::try_from`.

Adjacent: when a draft has no Message-ID, `parse_draft_for_submission` falls
back to the constant `bifrost.draft@{domain}` - every such draft from one
domain gets the same id, which is then the "stable fallback id" on the
receipt (see `append_to_sent_or_fallback`). Not unique by construction.

### 2.10 [drift] Message-ID and the two RFC 5322 composers

bifrost-smtp's `MessageBuilder::message_id(None)` generates
`<36 alphanumerics@HOSTNAME>` from `hostname::get()`. bifrost-types'
`mime::render` documents the opposite rule: "a deterministic, controlled-domain
Message-ID (the sender's domain, never a leaked machine hostname)". These are
two policies for the same header in two composers. More broadly the workspace
has two RFC 5322 / MIME writers (bifrost-smtp `message/` with its own header
encoding, folding, mailbox parser `message/mailbox/parsers/rfc2822.rs` and
Date formatting; bifrost-types `mime/render.rs` with its own) and two address
list parsers (bifrost-smtp `Mailbox`/`Mailboxes` FromStr vs
`bifrost_types::Address::parse_list`). account_error.rs notes that nothing in
the workspace builds a bifrost-smtp `Message`. The published builder is not a
deletion candidate; the structural question is which composer is the engine
and which is a facade (see 3.8).

### 2.11 [dup] TLS-mode dial sequence

The decision "given `Tls::{None, Opportunistic, Required, Wrapper}` and an
optional Unix socket, how is the connection established and upgraded" lives in
`transport.rs::SmtpClient::connection` (blocking) and in
`executor.rs::TokioExecutor::connect` (async). Both reject TLS over a Unix
socket with the same string, both pick `Wrapper` for implicit TLS and both run
the same STARTTLS match. They agree today. Owner: a pure
`DialPlan::from(&SmtpInfo) -> Result<DialPlan>` in the transport module; the
executor should not be the home of protocol dial policy at all (3.4).

### 2.12 [drift] Default ports per TLS mode

- `SmtpTransport::relay` sets 465, `starttls_relay` 587, `builder_dangerous`
  25 (SMTP) / 24 (LMTP).
- `connection_url::from_connection_url`: `smtp` with no `tls` uses
  `SMTP_PORT` (25). The doc table on `SmtpTransport::from_url` (and its async
  twin) says the default for that row is **587**. Doc and code disagree.
- IMAP `SubmissionTls::default_port`: Implicit 465, StartTls 587,
  **Plaintext 587** - and then always calls `.port(port)` over what `relay` /
  `starttls_relay` / `builder_dangerous` already chose.

Owner: one `default_port(dialect, tls_mode)` in bifrost-smtp, consumed by the
builders, the URL parser and IMAP.

### 2.13 [drift] Configuration errors classified as transport failures

- `from_connection_url` reports a malformed URL, a missing host, an unknown
  scheme and a bad percent-encoding as `error::connection(..)`, which
  `into_account_error` maps to `Transport(Network)` - retryable. A typo in a
  config URL is retried forever as a network outage.
- `TlsParametersBuilder::build`, `Certificate::from_pem/from_der`,
  `Identity::from_pem` map native-tls parse failures to `error::tls`, i.e.
  `Transport(Tls)`. bifrost-net classifies the identical failure (corrupt root
  cert DER at client construction) as `InvalidRequest` -> `Request(Malformed)`
  "not as a retryable transport setup failure" (`reference/net.md`, TLS
  section). Two answers to one question.
- IMAP `SubmissionTransport::build_with_connector` maps any bifrost-smtp
  builder error to `crate::Error::InvalidInput(e.to_string())`, discarding the
  kind entirely (and with it the distinction above).

### 2.14 [drift] "Is a rejected recipient a failed send?"

- `DirectSmtp` (`send_raw*`): the first negative RCPT reply ends the
  transaction with RSET and returns `Err` (after a 421, abort).
- `DirectLmtp`: a rejected RCPT is recorded and the send continues; all
  rejected returns `Ok(Vec<Response>)` of negatives ("Not a failure of the
  send").
- `BatchSmtp` / `BatchLmtp`: per-recipient lanes, DATA issued for the
  accepted subset.
- IMAP `send_rfc5322`: `Ok` iff any lane succeeded; otherwise the first
  uncertain error wins over the first failed one.

The SMTP and LMTP "direct" APIs answer the same question oppositely in sibling
methods of the same crate, and the IMAP policy for projecting a
`BatchOutcome` onto send success/failure lives in the consumer. The projection
is a bifrost-smtp (or bifrost-types `BatchOutcome`) decision: a
`BatchOutcome::<()>::as_send_result()` (or a smtp-side
`SubmissionOutcome`) would let every consumer get the same "committed to
someone, do not resend" rule.

### 2.15 [dup] MAIL parameter keyword identity

The keyword of a MAIL parameter is decided three ways: `MailParameter`'s
Display; `EnvelopeBuilder::mail_options` rendering each parameter and taking
`split('=').next()` to detect duplicates; and the hand-maintained
`TYPED_MAIL_KEYWORDS` array in `validate_custom_mail_keyword`, which must list
every typed variant's keyword. Adding a typed variant without touching the
array silently lets `Other { keyword: "NEW" }` bypass its capability check.
`RcptParameter::has_same_keyword` is a fourth. Owner: an exhaustive
`MailParameter::keyword() -> Keyword` (and the `RcptParameter` twin), from
which Display, the duplicate check and the "typed keyword" refusal all derive.

Adjacent: "does the envelope need SMTPUTF8" is computed by
`Envelope::has_non_ascii_addresses` on the direct path and by an inline
`!AsRef::<str>::as_ref(address).is_ascii()` fold on the batch path in
`EnvelopeBuilder::build_batch`.

### 2.16 [dup] Pool checkout and recycle policy

`pool/sync_impl.rs` and `pool/async_impl.rs` each decide checkout eligibility
in the same order (idle-expired, then `has_broken`, then the optional NOOP
probe) and recycle eligibility (`has_broken() || should_retire()`). They
already differ in what they do with a discarded connection: sync calls
`abort()` (QUIT and close), async's `recycle` just drops it. The judgement can
be a pure function over `(idle_duration, broken, retire, config)` shared by
both, leaving only the waiting primitive (Condvar vs Semaphore) to each.

### 2.17 [dup] Credentials and secret storage, three times

- `bifrost_sasl::Secret` and IMAP's `types::secret::SecretString` are the same
  type (same doc comment, same `Zeroizing<String>`, same conversions); IMAP's
  has a hand-rolled constant-time `PartialEq`, sasl's uses `subtle`. IMAP
  already converts `From<bifrost_sasl::Secret>`. `SecretString` is
  crate-private, so this needs no ruling: delete it and use `Secret`.
- `bifrost_smtp::Credentials` (public enum, `password: Zeroizing<String>`,
  re-exports `zeroize::Zeroizing`), `bifrost_imap::Credentials` (struct over a
  private `CredentialsKind`, `SecretString`), and
  `bifrost_imap::SubmissionCredentials` (public, `password: String` - plain,
  not zeroizing). `resolve_credentials` copies between them with
  `password.as_str().to_owned()`. All three model the same two shapes
  (password, OAuth identity plus token source). A shared `SaslCredentials` in
  bifrost-sasl (it would need the `TokenSource` trait, so either sasl depends
  on bifrost-net or the shared type lives in bifrost-net's auth module) is the
  single owner. **[ask owner]** for the public SMTP and IMAP types; at minimum
  `SubmissionCredentials::Password.password` should be a secret type.

---

## 3. Structure

### 3.1 [structure] The sans-IO core stops one layer too low

`client/core.rs` is a good, deliberate sans-IO design for the transaction
(MAIL/RCPT/DATA/BDAT/RSET/LMTP drain). Its own header lists what it leaves to
the adapters: "connecting, the greeting and EHLO/LHLO exchange, the AUTH
ladder, the STARTTLS upgrade, reply-line reading and parsing ...". Every item
on that list except connecting and deadlines is protocol, not I/O, and each is
written twice:

- AUTH: `SmtpConnection::auth` / `auth_scram` / `auth_legacy` /
  `verify_scram_server_final` / `write_auth_continuation` and the async
  versions, including the `334`-vs-`235` server-final branch and the
  ten-challenge loop.
- Reply framing: line cap, total cap, UTF-8 check, unsolicited-reply check,
  `nom` incomplete handling, "incomplete response" on EOF - in
  `read_response_until` and its async twin.
- STARTTLS: the "unsolicited reply before the STARTTLS upgrade" refusal.
- The `try_smtp!` macro itself ("mirrors the sync sibling").

The tests mirror the accident: the blocking and async connection files carry
parallel suites with the same test names
(`starttls_downgrade_is_refused_without_a_wire_command`,
`starttls_refuses_plaintext_bytes_buffered_after_the_reply`, ...), so a rule
is pinned twice and can drift once.

Shape: extend the `Op`/`OpOutcome`/`ProtocolMachine` vocabulary to a session
machine: `Greeting -> Hello -> (StartTls -> Hello)? -> Auth(ladder) -> Ready`,
with `Op::UpgradeTls` and `Op::ResolveToken` (the one place blocking and async
genuinely differ: async awaits `current()`, blocking takes `snapshot()`), and a
sans-IO `ReplyAssembler` fed lines. Both adapters shrink to
read-a-line/write-bytes/upgrade/deadline, and one transcript-driven
conformance suite runs every scenario through both adapters. This is the move
the crate already made for the transaction; it just was not finished.

EOF mid-reply is a side note here: both readers report
`error::parse("incomplete response")`, i.e. `Protocol(ParseFailed)` /
provider contract violation, for what is a connection closing under us. A
transport drop with InFlight evidence is the truthful class.

### 3.2 [structure] Four protocol machines for one transaction

`DirectSmtp`, `DirectLmtp`, `BatchSmtp`, `BatchLmtp` share a skeleton
(`Writing`, `MailReply`, `RcptReply`, `WindowWritten`, `WindowOpened`,
`MailWindowReply`, `RcptWindowReply`, `WindowClosing`, `DataReply`,
`BodyWritten`, `FinalReply` / `FinalStatus`) and each re-decides what a
negative MAIL FROM, a negative RCPT, a 421 and a write failure mean. Evidence
that this already drifted: `DirectLmtp`'s MAIL FROM arm carries the comment
"Aborting here cost one reconnect per rejected envelope ... and made the
direct LMTP path differ from both `DirectSmtp` and `BatchLmtp`", and
`closing_channel` exists because "the end-of-data arms had it and the envelope
arms did not". Capability asymmetries fall out of the split rather than from
design: `DirectLmtp` and `BatchLmtp` never pipeline; the batch API has no BDAT
(`build_batch` passes `allow_binary_mime = false`, `BatchSmtp` has no
`BodyKind`); `PhasedError`'s "cannot ship undecorated" guarantee applies to
`DirectSmtp` only, while `DirectLmtp` uses `or_phase`.

Shape: one machine, generic over a dialect (SMTP: one final reply for all
accepted; LMTP: one per accepted) and a recipient policy (`FailFast` - RSET on
the first rejection - vs `PerRecipient`), always recording into
`SendProgress`. The four public result shapes become projections of that one
record. 2.14's disagreement then becomes an explicit policy parameter instead
of an accident of which machine a method calls.

### 3.3 [structure] The 2x2 product at the public surface

`SmtpTransport`, `LmtpTransport`, `AsyncSmtpTransport<E>`,
`AsyncLmtpTransport<E>`, and four builders, each repeating `hello_name`,
`credentials`, `password`, `oauth2`, `oauth2_source`, `authentication`,
`dangerous_allow_insecure_auth`, `timeout`, `bandwidth_metering`, `port`,
`tls`, `pool_config`, `build`; each transport repeating `test_connection`,
`verify`, `expand`, `send_raw_with_options`, `send_raw_bdat*`,
`send_raw_batch_with_options` with the same body (validate, map, checkout,
send, resolve). `connection_url.rs` already had to introduce a private
`TransportBuilder` trait to paper over two of the four builders. The setters
already drift in small ways (the async LMTP builder's `tls` clears
`unix_socket` and is redundantly `#[cfg(feature = "tokio")]` inside an
always-tokio file).

Shape: one `Builder<D: Dialect, R: Runtime>` over `SmtpInfo`, and one
`Transport<D, R>` whose methods are written once over a runtime-abstract
connection (feasible once 3.1 lands, because the adapters become thin). The
current names can survive as type aliases, which keeps the published names;
**[ask owner]** because the types change from structs to aliases.

### 3.4 [structure] `Executor` is a Lettre vestige that also owns dialing

`Executor` is sealed with exactly one implementor (`TokioExecutor`); every
async transport is generic over `E` with `#[allow(private_bounds)]` on most
methods, and `SmtpExecutor::connect` holds the TLS-mode dial sequence (2.11).
Dialing policy is not a property of the runtime. Moving `connect` out of the
executor is internal; dropping the `E` parameter is **[ask owner]**. The
`ConnectionOptions` struct exists "keeping the dial signature stable across
features" - a symptom of the parameter list living in the wrong place.

### 3.5 [structure] bifrost-sasl should own the SASL client, not only the arithmetic

The crate's stated scope is "computation-only: no I/O, no async, no protocol
command flow". That boundary is right, but the crate draws it too close to the
math: a SASL client exchange (initial response, step on a challenge, verify,
done) is itself pure computation and protocol-neutral. Because sasl stops at
`scram_client_final`, every consumer reimplements the client around it (2.1,
2.4, 2.5, 2.6, 2.7, 2.8) and the crate's docs have to carry rules for its
consumers ("a PLUS consumer MUST build client-first from this").

Target surface for bifrost-sasl:

- `Mechanism` (shared vocabulary, names, `from_advertised`, credential
  exposure).
- `negotiate(advertised, policy, encrypted, binding) -> Ladder` with typed
  skips (absorbs `select_scram_rung`, both ladders, the cleartext gate).
- `SaslClient` per mechanism: `Plain`, `Login`, `CramMd5`, `XOAuth2`,
  `OAuthBearer`, `Scram { hash, binding }`, each with `initial_response()` and
  `step(challenge) -> Step { Reply(Secret), Done }` and owning nonce, GS2
  header, client-first-bare, expected signature, OAuth error-continuation
  answer and the LOGIN position.
- `ChannelBindingData` + `binding_from_peer_certificate`.
- Typed `SaslError` with `ScramServerError` and a `LocalRuntime` case, plus
  one `refusal_class()` the protocol crates map once.

What stays in IMAP and SMTP: framing (`AUTHENTICATE` + `+` vs `AUTH` + `334`,
SASL-IR, base64 line framing), the `235 v=...` field quirk, capability
snapshots, deadlines. IMAP's ManageSieve client becomes a third thin framing
layer and gets OAUTHBEARER and SCRAM for free.

### 3.6 [structure] One translation boundary, two error vocabularies on the public API

bifrost-smtp's public methods return `transport::smtp::Error` from `send_raw*`
and `AccountError` from `send_raw_batch_with_options`. The SMTP `Error` then
needs `preclassified_account_error: Option<Arc<AccountError>>` to tunnel a
dependency's typed classification back out through itself, and
`diagnostic_text()` reads the boxed `source`'s Display to produce support
text, so classification-relevant detail (the SCRAM token, "no compatible
mechanism") travels as the source's prose. With a typed auth failure (1.12)
and a typed SASL error (1.3) the tunnel and most prose reads go away; the
remaining question - whether the non-batch API should also speak
`AccountError` - is a product decision, **[ask owner]**.

`message_error_to_account_error` is `#[allow(dead_code)]` with no caller
"so that an account-oriented caller that DOES construct a `Message` has one
translation point". Each arm repeats its message string twice (once in the
cause, once in `text`). Kept deliberately per its comment; noted only because
it is a second copy of `crate::error::Error`'s Display strings.

### 3.7 [defect][structure] The `account-error` feature is fictional

`Cargo.toml` makes `bifrost-types` optional behind `account-error` (a default
feature). Nothing in `src/` is gated on `feature = "account-error"`, and
`account_error.rs`, `batch.rs`, `error.rs`, `authentication.rs`,
`client/core.rs`, `client/envelope.rs`, `client/connection.rs`,
`client/async_connection.rs`, `transport.rs` and `async_transport.rs` all name
`bifrost_types::` unconditionally, so `--no-default-features` cannot compile.
No `[[check]]` sweep builds bifrost-smtp without default features
(`smtp-dkim` adds a feature; `install-shape` uses defaults), so nothing would
notice. `SmtpTransmissionState` (+ `to_types_state`) duplicates
`bifrost_types::TransmissionState` and only makes sense if the dependency were
truly optional. Either make the dependency unconditional (drop the feature,
**[ask owner]** since it is a published feature name, and replace
`SmtpTransmissionState` with the types enum) or actually gate the account
surface and add a no-default sweep. The current state is the one combination
that is wrong: a promise to consumers the build does not keep.

Smaller feature-shape notes: `Credentials::oauth2_token` carries
`#[cfg_attr(not(feature = "tokio"), allow(dead_code))]` to keep an
async-only helper compiled in blocking builds; `test-support` exposes
`ScriptedConnector`/`Transcript`, which only the async executor path can use,
so a downstream crate cannot script the published blocking transport.

### 3.8 [structure] Which RFC 5322 composer is the engine

See 2.10. bifrost-types' renderer is what every account crate's send path uses
(IMAP, Google drafts); bifrost-smtp's `message/` is a richer published builder
(DKIM, attachments, typed headers) used by nobody in the workspace. Long term
one should be implemented on top of the other so header encoding, folding,
Message-ID, Date and Bcc stripping are decided once. A plausible direction is
bifrost-smtp's builder becoming a typed facade over bifrost-types' header and
body writers (bifrost-smtp already depends on bifrost-types), with its DKIM
signer staying in bifrost-smtp. **[ask owner]** on direction; no deletion is
implied.

### 3.9 [structure] Submission composition lives in IMAP but is not IMAP's

`imap/src/account/submission.rs` holds: a TLS-mode enum (`SubmissionTls`)
that mirrors both IMAP's TLS modes and bifrost-smtp's `Tls`; default ports
(2.12); a credential type (2.17); the envelope-address conversion (1.6); the
HOLDUNTIL formatting (1.7); and the BatchOutcome-to-send-result projection
(2.14). None of these are about IMAP. bifrost-smtp could expose a
`Submission` facade (`from(SubmissionConfig) -> Submission`,
`submit(&SubmissionEnvelope, raw, hold: Option<SystemTime>) ->
Result<SubmissionReceipt, AccountError>`) in `bifrost_types` vocabulary, and
any future account crate that composes SMTP (a CalDAV-only or JMAP-without-
submission deployment) would reuse it rather than copy IMAP's file. IMAP would
keep only the decision to own one.

### 3.10 [structure] Smaller placement notes

- `authentication.rs` mixes the public credential types, the mechanism enum,
  the stateless encoder, the SCRAM state machine, mechanism selection,
  channel-binding resolution, `334` payload decoding and the `SaslError`
  mapping. After 3.5 most of it moves to bifrost-sasl; what remains splits
  into `credentials.rs` and the framing helpers.
- `client/metering.rs`'s blocking admission loop is documented in
  `reference/net.md` as orchestration "a separate blocking budget API could
  own". Agreed; it belongs next to `BandwidthBudget` in bifrost-net.
- `transport/smtp/util.rs` and `base64.rs` / `time.rs` are fine as they are.

---

## 4. Lateral findings

- **[doc drift]** `SmtpTransport::from_url` / `AsyncSmtpTransport::from_url`
  doc tables give port 587 for `smtp://` with no `tls`; the code uses 25 (2.12).
- **[doc drift]** `reference/sync.md` (throttle section) says the clock move is
  "the same move `bifrost-smtp` made for `AsyncDeadline` and `ByteBucket`";
  there is no `ByteBucket` in bifrost-smtp any more (the name survives in a
  comment in `crates/sync/src/recovery.rs`).
- **[smell]** `bifrost-sasl` is described as private and "not public", but it
  is in the `install-shape` published set and its `Secret` appears in IMAP's
  and SMTP's internals; if it is ever a public type of a published crate, the
  "private" framing in its crate doc and Cargo description will mislead.
- **[smell]** `strip_frame_delim` in `oauth.rs` silently rewrites a
  credential containing `\x01` rather than refusing it (the comment says the
  builders keep an infallible signature for consumer convenience). Once the
  builders sit behind a fallible `SaslClient` (3.5), refusing is free and is
  the honest answer.
- **[smell]** `cram_md5_response` maps an HMAC key-construction failure to
  `SaslError::Protocol` ("invalid CRAM-MD5 key"), blaming the peer for the
  caller's password; HMAC accepts any key length, so the arm is unreachable,
  but the classification is wrong if it ever fires. Same for
  `hmac_digest` in `scram.rs`.
- **[smell]** Async pool `recycle` drops broken/retired connections without
  `abort()`, while the blocking pool aborts them (2.16).
- **[smell]** Several SMTP and IMAP tests assert on error prose
  (`to_string().contains("unsolicited reply")`, exact
  `"SCRAM server error: ..."` strings). Typed errors (1.3, 1.12) would let
  them assert on variants.
- **[smell]** `classify_status` / `classify_enhanced` sniff reply text for
  "rate", "limit", "throttle", "mailbox", "storage", "quota", "policy",
  "security" on 552, 554 and the 4.7.x fallback. Reasonable heuristics, but
  they are the only classification in the crate driven by free-form server
  prose and deserve a named table rather than inline `contains` chains.

---

## 5. Suggested order

1. Fix the live classification drift without restructuring: 2.2 (IMAP and
   ManageSieve token failures through net's projection), 2.1 (route
   ManageSieve and IMAP PLAIN through shared builders with the NUL refusal),
   2.5, 1.6 (validated `TryFrom` for envelope addresses), 2.13.
2. Grow bifrost-sasl into the client layer (3.5): typed `SaslError`,
   `ChannelBindingData`, shared `Mechanism` and negotiation, `SaslClient`
   state machines. 2.3, 2.4, 2.6, 2.7, 2.8 close as a consequence.
3. Finish the sans-IO core in bifrost-smtp (3.1), then fold the four machines
   (3.2) with `SendProgress` phase typing (1.9) and the dialect type (1.1).
4. Collapse the 2x2 public surface (3.3, 3.4) and settle the
   `account-error` feature (3.7) - both need owner rulings.
5. Decide the composer direction (3.8) and the submission facade (3.9).
