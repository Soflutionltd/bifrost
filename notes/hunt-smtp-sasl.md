# Hunt: bifrost-smtp and bifrost-sasl

Scope: `crates/smtp/` and `crates/sasl/`, read against `reference/smtp.md`,
`reference/sasl.md` and `reference/error-model.md`. No code was edited and
nothing was run. Findings are ordered by severity. Each one names the claim it
breaks.

## P1 - live defects

### 1. SMTP SCRAM base64-encodes the client-final twice, so every SCRAM rung fails against a real server

- `bifrost_sasl::scram_client_final` returns the client-final already
  base64-encoded (`scram.rs`: `STANDARD.encode(client_final.as_bytes()).into()`).
  IMAP writes that value to the wire as it is (`imap/src/connection/dispatch/auth.rs`,
  `bytes.extend_from_slice(client_final.as_bytes())`).
- SMTP's `ScramExchange::step` (`smtp/src/transport/smtp/authentication.rs`)
  encodes it again: `ScramStep::Reply(crate::base64::encode(client_final.as_bytes()))`.
  The blocking and async `auth_scram` both write that value through
  `write_auth_continuation` with no decode, so the server receives
  `base64(base64(c=...,r=...,p=...))`.
- Every RFC-conforming server rejects that line, so SCRAM-SHA-1, SCRAM-SHA-256
  and both `-PLUS` rungs, which are the top four entries of the default
  `PASSWORD_MECHANISMS`, cannot authenticate. On a server that offers SCRAM,
  password auth fails outright and never falls back to PLAIN (by design, a
  rejection is final). An in-flight SCRAM exchange is the common case for
  Dovecot and Cyrus submission.
- Claims broken: `reference/smtp.md` "Auth" (SCRAM is consumed from
  `bifrost-sasl`, SMTP owns only the wire sequencing) and `reference/sasl.md`
  "Surface" (`scram_client_final` returns "the base64 client-final").
- Why the tests missed it: no SMTP test drives a SCRAM exchange past
  server-first on the wire. `scram_exchange_none_binding_drives_to_complete`
  only checks that the reply is non-empty, then feeds a server-final computed
  separately. The transcript tests for `AUTH SCRAM-SHA-256-PLUS` all stop at a
  scripted 535. A transcript test that decodes the client-final line once and
  checks for `c=biws,r=` would have bitten.
- Fix: pass `client_final.as_str()` through, ideally as a `Secret`; see 7.

### 2. An EHLO line made only of whitespace panics the client

- `ServerInfo::from_response` (`extension.rs`) skips empty lines, then calls
  `line.split_whitespace().next().unwrap()`. `parse_response` accepts
  `250- \r\n` or `250-\t\r\n` (the text after `-` is `take_until("\r\n")`), so
  the line is `" "`: not empty, no tokens, and the `unwrap` panics.
- The input is peer-controlled, and the panic happens during connect, inside
  the pool, or under `tokio::spawn`. This breaks the crate-wide rule that
  malformed peer input is a typed `Parse` error ("An unparseable limit is a
  hard `Parse` error on the EHLO reply"). It is a remote crash.

### 3. The blocking transport panics on an `OAuthRefresher` that needs a refresh, and spuriously fails one that does not

- `Credentials::oauth2_token_blocking` polls `token_source.current()` once with
  a noop waker. The reference says a source "needing a network refresh is
  rejected".
- `OAuthRefresher::token_inner` (`net/src/auth.rs`) runs
  `self.state.lock().await` (a `tokio::sync::Mutex`), and on the Driver path
  calls `tokio::spawn(...)` before its first await. On a plain blocking thread
  with no runtime, a stale or empty refresher therefore panics in
  `tokio::spawn` instead of returning the documented rejection.
- If the tokio mutex is held for a moment by a concurrent async reader of the
  same shared source (which is exactly the ratatoskr shape: one
  `Arc<dyn TokenSource>` shared everywhere), the single poll returns `Pending`
  even when the token is fresh. The send then fails with "requires a network
  refresh". That contradicts "an already-fresh `OAuthRefresher` resolves
  immediately" (`reference/smtp.md` "Auth").
- That failure also lands as `InvalidInput` plus the Auth phase, which is
  `Authorization(PolicyBlocked)`, a terminal `NeedsPolicyChange`, for a lock
  race (see 5).

### 4. SCRAM never uses the GS2 `y` flag, so stripping `-PLUS` from the advertisement goes undetected

- RFC 5802 Section 6 makes downgrade protection depend on the GS2 header. A
  client that supports channel binding but does not see `-PLUS` advertised
  must send `y,,`, so a server that does support binding detects that the
  advertisement was stripped. `ScramChannelBinding` has only `None` (`n,,`)
  and `TlsServerEndPoint` (`p=...`). SMTP sends `n,,` whenever the chosen rung
  is unbound, even over TLS with a computable binding.
- The defence both references describe ("drop the unbound `SCRAM-SHA-N` rung
  whenever `SCRAM-SHA-N-PLUS` is advertised") only protects against a peer
  that ADVERTISES PLUS. It does nothing against one that REMOVES PLUS from
  EHLO: the client then picks unbound SCRAM with `n,,`, and a TLS-terminating
  relay can forward the exchange. `reference/sasl.md` and `reference/smtp.md`
  both claim RFC 5802 Section 6 downgrade protection. Only half of it is
  implemented.
- The fix belongs in `bifrost-sasl`: a third variant (`y,,`, `c=eSws`). It
  applies to IMAP as well.

## P2 - contract violations and misclassifications

### 5. OAuth token-source failures are classified as a terminal policy block

`oauth_token_error` maps every `bifrost_net::Error` from `current()` to
`invalid_input(...)` plus `Auth`, which `account_error.rs` routes to
`Authorization(PolicyBlocked)`, that is `NeedsPolicyChange`. A transient
refresh failure (network blip, IdP 5xx, backoff) therefore tells the consumer
to change policy instead of retrying, and a revoked refresh token is not
reported as `AuthLost`. The code comment admits that the projection "is not
refined here". Nothing downstream refines it either, because the
`AccountError` is built here. Claims broken: the error model's Authentication
rows (`RefreshTransient` gives Retry, `Revoked` / `ReauthorizationRequired`
give `AuthLost`) and "the TokenSource failure projection is the error model's
concern", which the error model cannot reach once the kind is already
`PolicyBlocked`. The blocking `Pending` rejection takes the same lane.

### 6. A SCRAM `e=` server error is classified differently from a 535 for the same condition, and differently from IMAP

`e=invalid-proof` (wrong password) becomes `SaslError::AuthFailed`, then
`InvalidInput` plus `Auth`, then `Authorization(PolicyBlocked)`
(`NeedsPolicyChange`). The same wrong password answered by a plain
`535 5.7.8` becomes `Authentication(ReauthorizationRequired)` (`AuthLost`).
IMAP maps `AuthFailed` to `Error::auth_with_code`, the authentication lane.
`reference/sasl.md` says "SMTP's split is the same", and it is not: one
credential failure gets two remediations depending on which framing the
server chose. `reference/smtp.md` records the `PolicyBlocked` routing as
intent, so this is also a document-versus-document disagreement that needs a
ruling.

### 7. The SCRAM client lines escape the zeroizing discipline

`reference/smtp.md` says stateless AUTH payloads stay `Secret` through the
command boundary and are written through zeroizing buffers. SCRAM does not:
`ScramStep::Reply(String)` and `client_first() -> String` are plain heap
strings, and the client-final (which carries `p=`) is copied into one. They
are written through `write_auth_continuation`, whose `format!` buffer is
zeroizing, but the `ScramStep` string is not. The exposure is small because
the proof is single-use, but the documented invariant does not cover the
mechanism the crate prefers by default. Fixing 1 is the natural moment to
carry `Secret` through.

### 8. `auth_legacy`: a success on the last challenge is reported as failure

After the loop, `if challenges == 0` errors with "Unexpected number of
challenges" and aborts, even when the tenth continuation's reply was `235`.
The server considers the session authenticated, while the client reports an
auth-lane `PolicyBlocked` and tears the connection down. The check should
test `response.has_code(334)`, not the budget. This is in both halves; only
servers with long LOGIN-style ladders reach it.

### 9. The SCRAM server-final folded onto `235` breaks when the server uses enhanced status codes

`decode_scram_payload` decodes `first_word()`. A server that folds the
server-final onto the success reply and advertises `ENHANCEDSTATUSCODES`
writes `235 2.7.0 <base64>`. The first word is then `2.7.0`, base64 decoding
fails, and a successful authentication becomes a `Parse` error plus an abort.
The branch exists to accept a "field-observed" shape, but it only accepts the
non-ESC variant of that shape.

## P3 - latent defects and smaller gaps

10. `build_recipient_commands` `zip`s addresses with `rcpt_options`. A shorter
    options vector silently drops recipients instead of failing. Every current
    caller builds one entry per recipient, so this is latent, but it sits on
    the path whose whole contract is "validate before MAIL FROM", and a
    dropped RCPT is a silent non-delivery.
11. `ServerInfo` stores features in a `HashSet<Extension>`, and
    `Size(Option<usize>)` / `FutureRelease { .. }` / `DeliverBy(..)` are
    distinct values. A server that repeats `SIZE` with different limits ends
    up with two entries, and `size_limit()` returns whichever `HashSet`
    iteration yields first, which is randomized per process. The "hard SIZE
    ceiling" then depends on hash seed. Reject duplicates, or keep the minimum.
12. A caller-supplied `MailParameter::Size` is emitted even when the server
    does not advertise SIZE (`validate_mail_parameter` only checks the limit).
    Every other extension parameter is gated on its advertisement ("Server-
    advertised `DSN` is verified before DSN parameters are emitted").
    User-supplied MAIL parameters are also never checked for duplicate
    keywords (two `RET=`, `BODY=` beside a user `BODY=`, and so on), although
    the RFC 3461 no-duplicate rule is applied on the RCPT side.
13. `validate_esmtp_raw_value` (used for `OtherRaw`, HOLDUNTIL, and
    `ClientId::domain`) accepts `=`. RFC 5321 `esmtp-value` excludes `%d61`.
14. In `build_batch`, a `Mail::new` failure is returned without the
    `Unsent` / `MailFrom` stamps that its sibling validation failures get two
    lines above.
15. The pool worker with `min_idle > 0` and `idle_timeout == 0` busy-loops:
    every connection is instantly expired, and it sleeps zero between dial
    rounds. Nothing rejects that configuration.
16. `bifrost_sasl::strip_frame_delim` returns `Cow::Owned(field.replace(..))`
    for the access token, a plain non-zeroizing copy of the bearer token,
    whenever the token contains `\x01`. The module promises zeroizing payloads.
17. `channel_binding::read_tlv` accepts non-minimal long-form lengths and
    high-tag-number forms. This is harmless today because the whole DER is
    hashed, but it is looser than the "anything it cannot read unambiguously
    is a hard error" policy the module states.

## Document-versus-code disagreements

- `reference/smtp.md` "DKIM": "Signing keys: `From<rsa::RsaPrivateKey>` and
  `From<ed25519_dalek::SigningKey>`" and "SHA-2 via `sha2` 0.10 to match
  `rsa` 0.9". The code uses `ed25519_compact::KeyPair`
  (`From<KeyPair> for DkimSigningKey`), and `Cargo.toml` pins
  `rsa = "0.10.0-rc.18"` and `ed25519-compact = "2"`. The text should be
  reworded around the trait shape rather than version numbers.
- `bifrost_sasl::SaslError::AuthFailed` doc: "or server-signature
  verification failed". The code returns `Protocol` for a signature mismatch,
  and `reference/sasl.md` and the tests say `Protocol` too. The doc comment is
  stale.
- `crates/sasl/src/lib.rs` module doc: "(`bifrost-imap`, and later
  `bifrost-smtp`)". SMTP has consumed it for a long time.
- `connection.rs` `resolve_scram_binding` doc: "`None` when the DER is absent
  or `tls_server_end_point` errors (`EdDSA` leaf." The sentence is unfinished,
  and the first half contradicts the second (errors propagate and do not
  become `None`).
- `secret.rs` test comment: "The length difference is folded into the
  accumulator". The impl comment directly above it says `subtle` short-circuits
  on a length mismatch. The test comment is wrong.

## Structural observations

- **The send-option rules exist four times.** `mail_options` and
  `mail_options_for_batch` in `connection.rs` are near-identical copies
  (SMTPUTF8, 8BITMIME, SIZE inference, parameter validation), as are
  `rcpt_options` and `rcpt_options_for_batch`. The async connection carries
  its own copies of both. `reference/smtp.md` presents the sans-I/O core as
  the end of half-to-half drift, but this validation is pure (it reads only
  `ServerInfo`, `is_encrypted` and the inputs) and still lives on the
  adapters. It is exactly the "local restatement" shape the standing lessons
  warn about. Moving `build_envelope` / `build_batch` into `core` (or a
  sibling sans-I/O module that takes `&ServerInfo`) would leave one copy.
  Finding 12 would then be fixed once rather than four times.
- **The SCRAM wire walk also exists twice** (`auth_scram` sync and async,
  including the 334-versus-235 server-final branch and the challenge-count
  loop). It is the same step-function shape the send core already uses. An
  `AuthMachine` yielding `Write` / `Read` ops would have given finding 1 one
  site and one transcript test, and findings 8 and 9 one fix each.
- **The SASL selection policy duplicates the safety logic in each protocol
  crate.** `reference/sasl.md` deliberately keeps selection per protocol. But
  RFC 5802 Section 6 handling (finding 4) is a SASL property, not a protocol
  one. Today each consumer has to remember to drop the unbound rung, and
  neither can express `y,,`. A `bifrost-sasl` helper that takes the advertised
  set and the binding availability, and returns the rung plus a
  `ScramChannelBinding` including the `y` case, would close it once for IMAP
  and SMTP while leaving the ladder and no-ladder difference to the callers.
