#![allow(clippy::wildcard_imports)]
use super::*;
use crate::types::SecretString;
use bifrost_types::TransmissionState;

impl ImapConnection {
    // -----------------------------------------------------------------------
    // Authentication
    // -----------------------------------------------------------------------

    /// Authenticate using a policy-selected mechanism.
    ///
    /// Password credentials prefer the channel-bound SCRAM variants
    /// (SCRAM-SHA-256-PLUS, SCRAM-SHA-1-PLUS) over their unbound counterparts
    /// (SCRAM-SHA-256, SCRAM-SHA-1), then PLAIN on encrypted connections, then
    /// CRAM-MD5 only when explicitly allowed by policy and TLS is active.
    /// LOGIN is never used unless explicitly allowed in [`AuthPolicy`].
    ///
    /// RFC 5802 Section 6 downgrade protection: when the server advertises a
    /// `SCRAM-SHA-N-PLUS` variant, the matching non-PLUS `SCRAM-SHA-N` rung is
    /// skipped with a `ChannelBindingUnavailable` rejection rather than
    /// attempted, so a man-in-the-middle cannot strip the channel binding by
    /// forcing the unbound fallback. If the PLUS binding cannot be resolved
    /// (plaintext, or an EdDSA leaf certificate), both SCRAM rungs for that
    /// hash are unavailable and auth falls through to PLAIN-over-TLS.
    ///
    /// OAuth credentials currently use XOAUTH2 and require TLS unless the
    /// policy explicitly permits cleartext credential mechanisms.
    pub async fn authenticate_best(
        &self,
        credentials: &crate::types::Credentials,
        policy: &crate::types::AuthPolicy,
        timeout: Duration,
    ) -> Result<crate::types::AuthOutcome, Error> {
        use crate::error::{
            AuthMechanismRejection, AuthMechanismRejectionReason, AuthPolicyFailure,
        };
        use crate::types::{AuthMechanism, AuthOutcome, CredentialsKind};

        let profile = self.server_profile();
        match credentials.kind() {
            CredentialsKind::OAuth2 {
                identity,
                token_source,
            } => {
                // OAUTHBEARER (RFC 7628) is the standard mechanism; XOAUTH2 is
                // the Google-defined predecessor kept as a fallback for
                // providers that advertise only it. Prefer the standard,
                // mirroring SMTP's `OAUTH2_MECHANISMS` order.
                let mechanism = if profile.supports_sasl_auth(AuthMechanism::OAuthBearer) {
                    AuthMechanism::OAuthBearer
                } else if profile.supports_sasl_auth(AuthMechanism::XOAuth2) {
                    AuthMechanism::XOAuth2
                } else {
                    return Err(Error::MissingCapability("AUTH=OAUTHBEARER".into()));
                };
                if !self.is_encrypted() && !policy.allow_cleartext_without_tls {
                    return Err(Error::AuthPolicy(AuthPolicyFailure::new(
                        offered_authentication(&profile, false),
                        vec![AuthMechanismRejection::new(
                            mechanism,
                            AuthMechanismRejectionReason::CleartextWithoutTls,
                        )],
                    )));
                }
                // Read the current token from the shared source at connect
                // time. On a reconnect this re-enters here and re-reads, so
                // a token rotated since the last connect is presented fresh.
                let access_token = tokio::time::timeout(timeout, token_source.current())
                    .await
                    .map_err(|_| Error::timeout())?
                    .map_err(|e| {
                        // The selected mechanism identifies the failed token lookup.
                        Error::auth_with_mechanism(
                            format!("failed to read OAuth access token: {e}"),
                            None,
                            mechanism.name(),
                        )
                    })?;
                match mechanism {
                    AuthMechanism::OAuthBearer => {
                        self.authenticate_oauthbearer(identity, access_token.as_str(), timeout)
                            .await?;
                    }
                    _ => {
                        self.authenticate_xoauth2(identity, access_token.as_str(), timeout)
                            .await?;
                    }
                }
                Ok(AuthOutcome { mechanism })
            }
            CredentialsKind::Password { username, password } => {
                let mut rejected = Vec::new();
                for candidate in password_mechanism_ladder(&profile, policy, self.is_encrypted()) {
                    let mechanism = match candidate {
                        PasswordCandidate::Reject(rejection) => {
                            rejected.push(rejection);
                            continue;
                        }
                        PasswordCandidate::Attempt(mechanism) => mechanism,
                    };
                    let attempt = match mechanism {
                        AuthMechanism::ScramSha256Plus | AuthMechanism::ScramSha1Plus => {
                            let hash = if mechanism == AuthMechanism::ScramSha256Plus {
                                bifrost_sasl::ScramHash::Sha256
                            } else {
                                bifrost_sasl::ScramHash::Sha1
                            };
                            // One DER fetch + hash per PLUS candidate. No
                            // certificate at all is a typed rejection recorded
                            // on the ladder; a certificate we cannot bind to
                            // aborts the ladder outright rather than falling
                            // through to unbound SCRAM (RFC 5802 Section 6).
                            let Some(resolved) = self.resolve_scram_binding().await? else {
                                rejected.push(AuthMechanismRejection::new(
                                    mechanism,
                                    AuthMechanismRejectionReason::ChannelBindingUnavailable,
                                ));
                                continue;
                            };
                            let data = match &resolved {
                                bifrost_sasl::ScramChannelBinding::TlsServerEndPoint(data) => {
                                    data.as_slice()
                                }
                                _ => unreachable!("certificate resolver returns bound data"),
                            };
                            let Some((_, resolved)) = bifrost_sasl::select_scram_rung(
                                hash,
                                true,
                                |name| {
                                    profile
                                        .auth_mechanisms
                                        .iter()
                                        .any(|m| m.eq_ignore_ascii_case(name))
                                },
                                Some(data),
                            ) else {
                                rejected.push(AuthMechanismRejection::new(
                                    mechanism,
                                    AuthMechanismRejectionReason::ChannelBindingUnavailable,
                                ));
                                continue;
                            };
                            self.authenticate_scram_with_binding(
                                username,
                                password.as_str(),
                                hash,
                                bifrost_sasl::ChannelBinding::TlsServerEndPoint,
                                Some(resolved),
                                timeout,
                            )
                            .await
                        }
                        AuthMechanism::ScramSha256 | AuthMechanism::ScramSha1 => {
                            let hash = if mechanism == AuthMechanism::ScramSha1 {
                                bifrost_sasl::ScramHash::Sha1
                            } else {
                                bifrost_sasl::ScramHash::Sha256
                            };
                            // Binding must be checked even without a PLUS advertisement:
                            // RFC 5802 section 6 then requires y, not n.
                            let available = self.resolve_scram_binding().await?;
                            let data = match &available {
                                Some(bifrost_sasl::ScramChannelBinding::TlsServerEndPoint(
                                    data,
                                )) => Some(data.as_slice()),
                                _ => None,
                            };
                            let Some((_, binding)) = bifrost_sasl::select_scram_rung(
                                hash,
                                false,
                                |name| {
                                    profile
                                        .auth_mechanisms
                                        .iter()
                                        .any(|m| m.eq_ignore_ascii_case(name))
                                },
                                data,
                            ) else {
                                rejected.push(AuthMechanismRejection::new(
                                    mechanism,
                                    AuthMechanismRejectionReason::ChannelBindingUnavailable,
                                ));
                                continue;
                            };
                            self.authenticate_scram_with_binding(
                                username,
                                password.as_str(),
                                hash,
                                bifrost_sasl::ChannelBinding::None,
                                Some(binding),
                                timeout,
                            )
                            .await
                        }
                        AuthMechanism::Plain => {
                            self.authenticate_plain(username, password.as_str(), timeout)
                                .await
                        }
                        AuthMechanism::CramMd5 => {
                            self.authenticate_cram_md5(username, password.as_str(), timeout)
                                .await
                        }
                        AuthMechanism::Login => {
                            self.login(username, password.as_str(), timeout).await
                        }
                        AuthMechanism::XOAuth2 | AuthMechanism::OAuthBearer => {
                            unreachable!("password path skips OAuth mechanisms")
                        }
                    };
                    // The ladder was built from `server_profile()` snapshotted
                    // above, but each `authenticate_*` re-reads the live
                    // capability snapshot (`require_auth_mechanism` /
                    // AUTH-presence checks). If the snapshot drifted between the
                    // build and this attempt (capability skew, e.g. a refetch
                    // after STARTTLS racing the ladder), a now-unadvertised
                    // mechanism surfaces as `MissingCapability`. Treat that as
                    // "this rung is no longer available" and fall through to the
                    // next rung rather than aborting the whole ladder, so a
                    // disappearing SCRAM rung still falls through to
                    // PLAIN-over-TLS instead of failing the connect.
                    //
                    // The rung must still be RECORDED. Falling through with
                    // nothing pushed makes the rung vanish from the failure
                    // entirely: it is not in `rejected` (nothing rejected it
                    // locally) and it left no other trace, so if it was the
                    // last rung the operator gets an `AuthPolicyFailure` whose
                    // diagnostic omits the one mechanism that actually went
                    // missing - and `offered`, snapshotted before the ladder
                    // ran, still lists it as available.
                    //
                    // This arm has no hermetic test, and cannot have one
                    // today. Reaching it needs the LIVE snapshot to lack a
                    // capability the ladder's profile snapshot had, but the
                    // profile and the per-site gates are governed by the same
                    // comparison, and the snapshot only moves when a command
                    // completes - while a rung that completes either succeeds
                    // and returns or fails non-`MissingCapability` and aborts.
                    // The one real path is a second handle on the same driver
                    // refetching CAPABILITY concurrently, which is not
                    // deterministic to script. Keep the arm; do not delete it
                    // for want of a test, and do not write a test that fakes
                    // the shape - an earlier attempt asserted a premise this
                    // code contradicts and had never been run.
                    match attempt {
                        Ok(()) => {}
                        Err(Error::MissingCapability(cap)) => {
                            debug!(
                                mechanism = mechanism.name(),
                                capability = %cap,
                                "auth mechanism unavailable on live snapshot \
                                 (capability skew); falling through to next rung"
                            );
                            rejected.push(AuthMechanismRejection::new(
                                mechanism,
                                AuthMechanismRejectionReason::UnavailableOnLiveSnapshot,
                            ));
                            continue;
                        }
                        // A server rejection is an authentication result, not
                        // evidence that this mechanism disappeared. Retrying
                        // with a weaker rung could turn an explicit failure
                        // into a downgrade and can consume another credential
                        // attempt. Only live capability skew is retryable.
                        Err(other) => return Err(other),
                    }
                    return Ok(AuthOutcome { mechanism });
                }
                Err(Error::AuthPolicy(AuthPolicyFailure::new(
                    offered_authentication(&profile, true),
                    rejected,
                )))
            }
        }
    }

    /// Authenticate with LOGIN command (RFC 3501 Section 6.2.3).
    ///
    /// **Note:** LOGIN is deprecated in `IMAP4rev2` (RFC 9051 Section 2.2).
    /// Prefer [`authenticate_plain`](Self::authenticate_plain) when the server
    /// advertises `AUTH=PLAIN`, which is the standard SASL mechanism and works
    /// with all servers including those that don't support LOGIN (e.g. Stalwart).
    ///
    /// **TLS gate:** this sends reusable credentials with no TLS/policy gate.
    /// The policy-driven gate (LOGIN is opt-in via `with_login` AND cleartext-
    /// gated) lives in [`authenticate_best`](Self::authenticate_best). Calling
    /// this method directly is the deliberate opt-out of that policy; a direct
    /// caller is responsible for ensuring the connection is encrypted (the
    /// `LOGINDISABLED` capability is still honored).
    pub async fn login(&self, user: &str, pass: &str, timeout: Duration) -> Result<(), Error> {
        use super::dispatch::LoginConsumer;

        // Reject before the command enters the driver. SASL credentials take
        // their separately framed AUTHENTICATE paths; this validation is only
        // for the line-oriented LOGIN command.
        crate::codec::encode::validate_login_credential_ascii(user, "user")?;
        crate::codec::encode::validate_login_credential_ascii(pass, "password")?;

        // Validate state and capabilities from the driver's snapshot.
        {
            let snap = self.state_rx.borrow();
            // RFC 3501 Section 6.2.3: LOGIN is only valid in
            // NotAuthenticated state.
            if let Some(refusal) =
                super::helpers::state_refusal(snap.session_state, &[SessionState::NotAuthenticated])
            {
                return Err(refusal);
            }
            // RFC 3501 Section 6.2.3: "If the server advertises the
            // LOGINDISABLED capability [...] the LOGIN command MUST NOT
            // be used."
            if snap
                .capabilities
                .iter()
                .any(|c| matches!(c, Capability::LoginDisabled))
            {
                return Err(Error::MissingCapability(
                    "LOGIN disabled by server (LOGINDISABLED capability \
                     advertised, RFC 3501 Section 6.2.3)"
                        .into(),
                ));
            }

            // RFC 9051 Section 2.2: warn when a better alternative
            // exists. Use the broader check (raw capability presence)
            // rather than is_rev2 so that dual-mode servers that
            // advertise rev2 but haven't had ENABLE issued yet still
            // trigger the warning.
            let has_rev2 = is_rev2_from_snapshot(&snap)
                || snap
                    .capabilities
                    .iter()
                    .any(|c| matches!(c, Capability::Imap4Rev2));
            let has_auth_plain = snap
                .capabilities
                .iter()
                .any(|c| matches!(c, Capability::Auth(m) if m.eq_ignore_ascii_case("PLAIN")));
            drop(snap);
            if has_rev2 && has_auth_plain {
                warn!(
                    "LOGIN is deprecated on IMAP4rev2 servers \
                     (RFC 9051 Section 2.2); prefer \
                     authenticate_plain()  -  the server advertises \
                     AUTH=PLAIN"
                );
            }
        }

        let deadline = tokio::time::Instant::now() + timeout;
        let cmd = Command::Login {
            user: user.to_owned(),
            pass: pass.to_owned().into(),
        };

        let caps_provided =
            tokio::time::timeout(timeout, self.submit_regular(cmd, LoginConsumer::default()))
                .await
                .map_err(|_| Error::timeout_inflight())??;

        self.complete_auth(caps_provided, deadline).await
    }

    /// Authenticate with SASL PLAIN mechanism (RFC 4616).
    ///
    /// Constructs the PLAIN payload (`\0user\0pass`) and sends it via
    /// AUTHENTICATE PLAIN, using SASL-IR (RFC 4959) when available.
    ///
    /// **TLS gate:** this sends reusable credentials with no TLS/policy gate.
    /// The policy-driven cleartext gate lives in
    /// [`authenticate_best`](Self::authenticate_best). Calling this method
    /// directly is the deliberate opt-out of that policy; a direct caller is
    /// responsible for ensuring the connection is encrypted.
    pub async fn authenticate_plain(
        &self,
        user: &str,
        pass: &str,
        timeout: Duration,
    ) -> Result<(), Error> {
        use super::dispatch::AuthenticatePlainConsumer;
        use base64::Engine;

        // Validate state and build SASL payload from the snapshot.
        let (encoded, has_sasl_ir) = {
            let snap = self.state_rx.borrow();
            if let Some(refusal) =
                super::helpers::state_refusal(snap.session_state, &[SessionState::NotAuthenticated])
            {
                return Err(refusal);
            }
            // RFC 3501 Section 6.2.2: verify the server advertises
            // AUTH=PLAIN before sending credentials.
            if !snap
                .capabilities
                .contains(&Capability::Auth("PLAIN".into()))
            {
                return Err(Error::MissingCapability("AUTH=PLAIN".into()));
            }

            // PLAIN payload per RFC 4616 Section 2:
            // [authzid] NUL authcid NUL passwd  -  authzid is empty.
            let mut payload =
                zeroize::Zeroizing::new(Vec::with_capacity(1 + user.len() + 1 + pass.len()));
            payload.push(b'\0');
            payload.extend_from_slice(user.as_bytes());
            payload.push(b'\0');
            payload.extend_from_slice(pass.as_bytes());
            let encoded: SecretString = base64::engine::general_purpose::STANDARD
                .encode(payload.as_slice())
                .into();

            let has_sasl_ir = snapshot_supports(&snap, &Capability::SaslIr);
            drop(snap);
            (encoded, has_sasl_ir)
        };

        let cmd = Command::Authenticate {
            mechanism: "PLAIN".to_owned(),
            initial_response: if has_sasl_ir {
                Some(encoded.clone())
            } else {
                None
            },
        };

        let consumer = AuthenticatePlainConsumer::new(encoded, has_sasl_ir);
        let deadline = tokio::time::Instant::now() + timeout;
        let caps_provided =
            tokio::time::timeout(timeout, self.submit_with_continuations(cmd, consumer))
                .await
                .map_err(|_| Error::timeout_inflight())??;

        self.complete_auth(caps_provided, deadline).await
    }

    /// Authenticate with XOAUTH2 SASL mechanism (Google-defined, not an
    /// IETF RFC).
    ///
    /// Uses SASL-IR (RFC 4959 Section 3) if the server advertises it.
    pub async fn authenticate_xoauth2(
        &self,
        user: &str,
        token: &str,
        timeout: Duration,
    ) -> Result<(), Error> {
        use super::dispatch::AuthenticateXoauth2Consumer;
        use base64::Engine;

        // Validate state and build XOAUTH2 payload from the snapshot.
        let (encoded, has_sasl_ir) = {
            let snap = self.state_rx.borrow();
            if let Some(refusal) =
                super::helpers::state_refusal(snap.session_state, &[SessionState::NotAuthenticated])
            {
                return Err(refusal);
            }
            // RFC 3501 Section 6.2.2: verify the server advertises
            // AUTH=XOAUTH2 before sending credentials.
            if !snap
                .capabilities
                .contains(&Capability::Auth("XOAUTH2".into()))
            {
                return Err(Error::MissingCapability("AUTH=XOAUTH2".into()));
            }

            // Build XOAUTH2 payload via the shared bifrost-sasl builder, then
            // base64-frame it for AUTHENTICATE. The raw token-bearing Secret is
            // a temporary that zeroizes on drop; only the base64 form persists.
            let encoded: SecretString = base64::engine::general_purpose::STANDARD
                .encode(bifrost_sasl::xoauth2_payload(user, token).as_bytes())
                .into();

            let has_sasl_ir = snapshot_supports(&snap, &Capability::SaslIr);
            drop(snap);
            (encoded, has_sasl_ir)
        };

        let cmd = Command::Authenticate {
            mechanism: "XOAUTH2".to_owned(),
            initial_response: if has_sasl_ir {
                Some(encoded.clone())
            } else {
                None
            },
        };

        let consumer = AuthenticateXoauth2Consumer::new(
            encoded,
            has_sasl_ir,
            crate::types::AuthMechanism::XOAuth2.name(),
        );
        let deadline = tokio::time::Instant::now() + timeout;
        let caps_provided =
            tokio::time::timeout(timeout, self.submit_with_continuations(cmd, consumer))
                .await
                .map_err(|_| Error::timeout_inflight())??;

        self.complete_auth(caps_provided, deadline).await
    }

    /// Authenticate with SASL OAUTHBEARER (RFC 7628).
    ///
    /// Uses SASL-IR (RFC 4959 Section 3) if the server advertises it. The
    /// payload framing matches XOAUTH2 on the wire (a single base64 blob
    /// followed by an empty-line reply to any error continuation, letting the
    /// server finish with a tagged NO/BAD), so the dispatch consumer is shared;
    /// only the payload bytes and the mechanism token differ. The `a=`
    /// authorization identity is GS2-escaped inside `bifrost_sasl`.
    ///
    /// **TLS gate:** like the other direct mechanism methods, this enforces no
    /// TLS/policy gate. Calling it directly is the opt-out; the policy-driven
    /// gate lives in [`authenticate_best`](Self::authenticate_best). A direct
    /// caller is responsible for ensuring the connection is encrypted before
    /// presenting a bearer token.
    pub async fn authenticate_oauthbearer(
        &self,
        identity: &str,
        token: &str,
        timeout: Duration,
    ) -> Result<(), Error> {
        use super::dispatch::AuthenticateXoauth2Consumer;
        use base64::Engine;

        // Validate state and build the OAUTHBEARER payload from the snapshot.
        let (encoded, has_sasl_ir) = {
            let snap = self.state_rx.borrow();
            if let Some(refusal) =
                super::helpers::state_refusal(snap.session_state, &[SessionState::NotAuthenticated])
            {
                return Err(refusal);
            }
            // RFC 3501 Section 6.2.2: verify the server advertises
            // AUTH=OAUTHBEARER before sending credentials.
            if !snap
                .capabilities
                .contains(&Capability::Auth("OAUTHBEARER".into()))
            {
                return Err(Error::MissingCapability("AUTH=OAUTHBEARER".into()));
            }

            // Build the OAUTHBEARER payload via the shared bifrost-sasl builder,
            // then base64-frame it for AUTHENTICATE. The raw token-bearing
            // Secret is a temporary that zeroizes on drop; only the base64 form
            // persists.
            let encoded: SecretString = base64::engine::general_purpose::STANDARD
                .encode(bifrost_sasl::oauthbearer_payload(identity, token).as_bytes())
                .into();

            let has_sasl_ir = snapshot_supports(&snap, &Capability::SaslIr);
            drop(snap);
            (encoded, has_sasl_ir)
        };

        let cmd = Command::Authenticate {
            mechanism: "OAUTHBEARER".to_owned(),
            initial_response: if has_sasl_ir {
                Some(encoded.clone())
            } else {
                None
            },
        };

        let consumer = AuthenticateXoauth2Consumer::new(
            encoded,
            has_sasl_ir,
            crate::types::AuthMechanism::OAuthBearer.name(),
        );
        let deadline = tokio::time::Instant::now() + timeout;
        let caps_provided =
            tokio::time::timeout(timeout, self.submit_with_continuations(cmd, consumer))
                .await
                .map_err(|_| Error::timeout_inflight())??;

        self.complete_auth(caps_provided, deadline).await
    }

    /// Authenticate with SASL CRAM-MD5.
    ///
    /// CRAM-MD5 is a challenge-response mechanism. The password is never sent
    /// directly on the wire, but the mechanism is still legacy and should only
    /// be used when stronger SASL mechanisms are unavailable.
    ///
    /// **TLS gate:** this enforces no TLS/policy gate. The policy-driven gate
    /// (CRAM-MD5 is opt-in via `with_cram_md5` AND TLS-gated, because a MITM can
    /// choose the challenge and brute-force `HMAC-MD5(password, challenge)`
    /// offline) lives in [`authenticate_best`](Self::authenticate_best). Calling
    /// this method directly is the deliberate opt-out of that policy; a direct
    /// caller is responsible for ensuring the connection is encrypted.
    pub async fn authenticate_cram_md5(
        &self,
        user: &str,
        pass: &str,
        timeout: Duration,
    ) -> Result<(), Error> {
        use super::dispatch::AuthenticateCramMd5Consumer;

        self.require_auth_mechanism("CRAM-MD5")?;

        let cmd = Command::Authenticate {
            mechanism: "CRAM-MD5".to_owned(),
            initial_response: None,
        };

        let consumer = AuthenticateCramMd5Consumer::new(user.to_owned(), pass.to_owned().into());
        let deadline = tokio::time::Instant::now() + timeout;
        let caps_provided =
            tokio::time::timeout(timeout, self.submit_with_continuations(cmd, consumer))
                .await
                .map_err(|_| Error::timeout_inflight())??;

        self.complete_auth(caps_provided, deadline).await
    }

    /// Authenticate with SASL SCRAM-SHA-1 (RFC 5802).
    pub async fn authenticate_scram_sha1(
        &self,
        user: &str,
        pass: &str,
        timeout: Duration,
    ) -> Result<(), Error> {
        self.authenticate_scram_with_binding(
            user,
            pass,
            bifrost_sasl::ScramHash::Sha1,
            bifrost_sasl::ChannelBinding::None,
            None,
            timeout,
        )
        .await
    }

    /// Authenticate with SASL SCRAM-SHA-256 (RFC 7677).
    pub async fn authenticate_scram_sha256(
        &self,
        user: &str,
        pass: &str,
        timeout: Duration,
    ) -> Result<(), Error> {
        self.authenticate_scram_with_binding(
            user,
            pass,
            bifrost_sasl::ScramHash::Sha256,
            bifrost_sasl::ChannelBinding::None,
            None,
            timeout,
        )
        .await
    }

    /// Run a SCRAM exchange under a chosen channel-binding dimension.
    ///
    /// `binding` selects the wire mechanism token and GS2 header. For
    /// `ChannelBinding::TlsServerEndPoint`, the data-carrying
    /// `ScramChannelBinding` is resolved here unless `resolved` already
    /// carries it (so `authenticate_best` can fetch the peer cert DER once and
    /// thread it in). A direct caller that omits `resolved` and whose binding
    /// cannot be resolved gets a typed `Error::AuthPolicy` carrying a single
    /// `ChannelBindingUnavailable` rejection rather than a silent fall-through.
    async fn authenticate_scram_with_binding(
        &self,
        user: &str,
        pass: &str,
        hash: bifrost_sasl::ScramHash,
        binding: bifrost_sasl::ChannelBinding,
        resolved: Option<bifrost_sasl::ScramChannelBinding>,
        timeout: Duration,
    ) -> Result<(), Error> {
        use bifrost_sasl::{ChannelBinding, ScramChannelBinding};

        use super::dispatch::AuthenticateScramConsumer;
        use crate::error::{
            AuthMechanismRejection, AuthMechanismRejectionReason, AuthPolicyFailure,
        };
        use crate::types::AuthMechanism;

        self.require_auth_mechanism(hash.mechanism_name(binding))?;

        let scram_binding = match binding {
            ChannelBinding::TlsServerEndPoint => match resolved {
                Some(resolved) => resolved,
                None => self.resolve_scram_binding().await?.ok_or_else(|| {
                    // `ScramHash` is `#[non_exhaustive]`; SHA-1 maps to its
                    // PLUS variant, SHA-256 (and any future hash) to the
                    // SHA-256 PLUS variant for the audit/display rejection.
                    let mechanism = match hash {
                        bifrost_sasl::ScramHash::Sha1 => AuthMechanism::ScramSha1Plus,
                        _ => AuthMechanism::ScramSha256Plus,
                    };
                    Error::AuthPolicy(AuthPolicyFailure::new(
                        offered_authentication(&self.server_profile(), false),
                        vec![AuthMechanismRejection::new(
                            mechanism,
                            AuthMechanismRejectionReason::ChannelBindingUnavailable,
                        )],
                    ))
                })?,
            },
            // Negotiated unbound SCRAM carries the helper's y flag in `resolved`.
            // Direct calls explicitly selecting unbound SCRAM do not negotiate
            // mechanisms and retain n (RFC 5802 section 6).
            _ => resolved.unwrap_or(ScramChannelBinding::None),
        };

        let has_sasl_ir = {
            let snap = self.state_rx.borrow();
            snapshot_supports(&snap, &Capability::SaslIr)
        };

        let nonce = generate_scram_nonce()?;
        let consumer = AuthenticateScramConsumer::new(
            hash,
            user.to_owned(),
            pass.to_owned().into(),
            nonce,
            has_sasl_ir,
            scram_binding,
        )?;
        let initial_response = has_sasl_ir.then(|| consumer.initial_response());
        let cmd = Command::Authenticate {
            // Single source of truth for the emitted token: the SASL crate.
            mechanism: hash.mechanism_name(binding).to_owned(),
            initial_response,
        };

        let deadline = tokio::time::Instant::now() + timeout;
        let caps_provided =
            tokio::time::timeout(timeout, self.submit_with_continuations(cmd, consumer))
                .await
                .map_err(|_| Error::timeout_inflight())??;

        self.complete_auth(caps_provided, deadline).await
    }

    /// Resolve the `tls-server-end-point` channel binding for the live
    /// connection: fetch the peer certificate DER once and hash it.
    ///
    /// Returns `Ok(None)` only when there is no peer certificate at all
    /// (plaintext, dead driver, or native-tls produced no DER); that absence is
    /// the sole binding-skip signal, and the caller turns it into a
    /// `ChannelBindingUnavailable` rejection.
    ///
    /// A certificate that is present but whose binding cannot be computed
    /// (unsupported signature algorithm, EdDSA leaf, malformed or truncated
    /// DER) is an error, not a skip. Swallowing it would let an attacker who
    /// can influence the presented leaf knock the client off the PLUS rung and
    /// onto unbound SCRAM or PLAIN, which is the downgrade the `PLUS`
    /// mechanisms exist to prevent (RFC 5802 Section 6). SMTP resolves this the
    /// same way; the two crates must not differ here.
    async fn resolve_scram_binding(
        &self,
    ) -> Result<Option<bifrost_sasl::ScramChannelBinding>, Error> {
        scram_binding_from_der(self.peer_certificate_der().await.as_deref())
    }

    /// Finalize authentication: refresh capabilities if the server did
    /// not provide them inline.
    ///
    /// State transition to `Authenticated` is handled by the driver task
    /// via the `in_auth` flag in `ProtocolState::apply_tagged`  -  the
    /// driver sets `in_auth` when it sees `Command::Login` or
    /// `Command::Authenticate`, and `apply_tagged` transitions on tagged
    /// OK (RFC 3501 Section6.2.2, Section6.2.3).
    ///
    /// If the server did not include updated capabilities in its response,
    /// issue an explicit CAPABILITY command (RFC 3501 Section6.2.2 / Section6.2.3).
    async fn complete_auth(
        &self,
        caps_provided: bool,
        deadline: tokio::time::Instant,
    ) -> Result<(), Error> {
        if !caps_provided {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(Error::timeout_inflight());
            }
            // Send CAPABILITY via the driver. apply_side_effects inside
            // the driver updates the cached capabilities automatically
            // when it processes the untagged CAPABILITY or tagged
            // [CAPABILITY] response code (RFC 3501 Section7.2.1).
            tokio::time::timeout(remaining, self.fetch_capabilities_via_driver())
                .await
                .map_err(|_| Error::timeout_inflight())??;
        }
        Ok(())
    }

    fn require_auth_mechanism(&self, mechanism: &str) -> Result<(), Error> {
        let snap = self.state_rx.borrow();
        if let Some(refusal) =
            super::helpers::state_refusal(snap.session_state, &[SessionState::NotAuthenticated])
        {
            return Err(refusal);
        }
        if !snap
            .capabilities
            .contains(&Capability::Auth(mechanism.to_owned()))
        {
            return Err(Error::MissingCapability(format!("AUTH={mechanism}")));
        }
        Ok(())
    }

    /// Send CAPABILITY via the driver task and wait for completion.
    ///
    /// The driver's `apply_side_effects` updates the cached capability set
    /// automatically. The consumer output is ignored  -  the canonical state
    /// lives in `ProtocolState` inside the driver.
    async fn fetch_capabilities_via_driver(&self) -> Result<(), Error> {
        use super::dispatch::CapabilityConsumer;

        let _caps: Vec<Capability> = self
            .submit_regular(Command::Capability, CapabilityConsumer::default())
            .await?;
        Ok(())
    }

    /// UNAUTHENTICATE  -  reset to unauthenticated state (RFC 8437 Section 2).
    ///
    /// Resets the IMAP session to Not Authenticated without closing the
    /// TLS connection. After success, the client can LOGIN or AUTHENTICATE
    /// as a different user  -  enabling connection pooling across users.
    ///
    /// Clears all per-user state: selected mailbox, NOTIFY registrations,
    /// and ENABLE'd extensions. The TLS layer remains intact.
    ///
    /// After unauthentication, issues an explicit CAPABILITY command to
    /// refresh the cached capabilities, since the server's advertised
    /// capabilities may differ between authenticated and unauthenticated
    /// states (RFC 8437 Section 2).
    ///
    /// # Stale events
    ///
    /// Events buffered before this call may contain stale data from the
    /// previous session (e.g., EXISTS, EXPUNGE for a previously selected
    /// mailbox). Callers should drain the event channel before
    /// re-authenticating.
    ///
    /// # Errors
    ///
    /// - [`Error::MissingCapability`] if `UNAUTHENTICATE` is not advertised.
    /// - [`Error::InvalidState`] if called in Not Authenticated state.
    pub async fn unauthenticate(&self, timeout: Duration) -> Result<(), Error> {
        // RFC 8437 Section2: valid in Authenticated or Selected state.
        self.require_state(crate::types::CommandKind::Unauthenticate)?;

        // Connection-level capability check.
        {
            let snap = self.state_rx.borrow();
            if !snapshot_supports(&snap, &Capability::Unauthenticate) {
                return Err(Error::MissingCapability("UNAUTHENTICATE".into()));
            }
        }

        let deadline = tokio::time::Instant::now() + timeout;

        tokio::time::timeout(
            timeout,
            self.submit_regular(
                Command::Unauthenticate,
                super::dispatch::TaggedOkConsumer::default(),
            ),
        )
        .await
        .map_err(|_| Error::timeout_inflight())??;

        // RFC 8437 Section2: capabilities may change after unauthentication.
        // Unconditional refresh  -  the server SHOULD send caps but is not
        // required to.
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(Error::timeout_inflight());
        }
        tokio::time::timeout(remaining, self.fetch_capabilities_via_driver())
            .await
            .map_err(|_| Error::timeout_inflight())??;

        Ok(())
    }

    /// Graceful logout (RFC 3501 Section 6.1.3 / RFC 9051 Section 6.1.3).
    ///
    /// Sends LOGOUT via the driver task and validates the required
    /// response sequence: the server MUST send `* BYE` followed by a
    /// tagged OK.
    ///
    /// State transitions are handled by `apply_side_effects` inside the
    /// driver task:
    /// - `* BYE` -> `Logout` (via `apply_untagged`)
    /// - Tagged OK with `in_logout` -> `Logout` (via `apply_tagged`)
    ///
    /// The driver sets `in_logout` automatically when it sees
    /// `Command::Logout`.
    pub async fn logout(&self) -> Result<(), Error> {
        use super::dispatch::LogoutConsumer;

        // RFC 3501 Section 3.4: already in Logout state  -  no-op.
        if self.state_rx.borrow().session_state == SessionState::Logout {
            return Ok(());
        }

        let _: () = self
            .submit_regular(Command::Logout, LogoutConsumer::default())
            .await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Submit helpers
    // -----------------------------------------------------------------------

    /// Submit a regular (non-continuation) command to the driver task
    /// and await the typed result.
    pub(super) async fn submit_regular<C: super::dispatch::Consumer + 'static>(
        &self,
        cmd: Command,
        consumer: C,
    ) -> Result<C::Output, Error>
    where
        C::Output: 'static,
    {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let dcmd = driver::DriverCommand::Run {
            command: cmd,
            consumer: driver::DriverConsumer::Regular(
                Box::new(consumer) as Box<dyn driver::ConsumerErased>
            ),
            result_tx,
        };
        if self.cmd_tx.send(dcmd).await.is_err() {
            return Err(self.observe_driver_panic(TransmissionState::Unsent).await);
        }
        let guard = self.in_flight();
        let received = result_rx.await;
        guard.completed();
        let result = match received {
            Ok(inner) => inner?,
            Err(_) => {
                return Err(self.observe_driver_panic(TransmissionState::InFlight).await);
            }
        };
        let output = *result
            .downcast::<C::Output>()
            .map_err(|_| Error::Internal("type mismatch in driver result".into()))?;
        Ok(output)
    }

    /// Submit a streaming regular command to the driver task and await the
    /// typed result.
    pub(super) async fn submit_streaming<C: super::dispatch::StreamingConsumer + 'static>(
        &self,
        cmd: Command,
        consumer: C,
    ) -> Result<C::Output, Error>
    where
        C::Output: 'static,
    {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let dcmd = driver::DriverCommand::Run {
            command: cmd,
            consumer: driver::DriverConsumer::StreamingRegular(
                Box::new(consumer) as Box<dyn driver::StreamingConsumerErased>
            ),
            result_tx,
        };
        if self.cmd_tx.send(dcmd).await.is_err() {
            return Err(self.observe_driver_panic(TransmissionState::Unsent).await);
        }
        let guard = self.in_flight();
        let received = result_rx.await;
        guard.completed();
        let result = match received {
            Ok(inner) => inner?,
            Err(_) => {
                return Err(self.observe_driver_panic(TransmissionState::InFlight).await);
            }
        };
        let output = *result
            .downcast::<C::Output>()
            .map_err(|_| Error::Internal("type mismatch in driver result".into()))?;
        Ok(output)
    }

    /// Submit a continuation-aware command to the driver task and await
    /// the typed result.
    ///
    /// Used by AUTHENTICATE (SASL PLAIN, XOAUTH2) which expect `+`
    /// continuations during the response loop (RFC 3501 Section7.5).
    pub(super) async fn submit_with_continuations<
        C: super::dispatch::ContinuationConsumer + 'static,
    >(
        &self,
        cmd: Command,
        consumer: C,
    ) -> Result<C::Output, Error>
    where
        C::Output: 'static,
    {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let dcmd = driver::DriverCommand::Run {
            command: cmd,
            consumer: driver::DriverConsumer::WithContinuations(
                Box::new(consumer) as Box<dyn driver::ContinuationConsumerErased>
            ),
            result_tx,
        };
        if self.cmd_tx.send(dcmd).await.is_err() {
            return Err(self.observe_driver_panic(TransmissionState::Unsent).await);
        }
        let guard = self.in_flight();
        let received = result_rx.await;
        guard.completed();
        let result = match received {
            Ok(inner) => inner?,
            Err(_) => {
                return Err(self.observe_driver_panic(TransmissionState::InFlight).await);
            }
        };
        let output = *result
            .downcast::<C::Output>()
            .map_err(|_| Error::Internal("type mismatch in driver result".into()))?;
        Ok(output)
    }

    /// Submit a stream upgrade (STARTTLS / COMPRESS) to the driver task.
    ///
    /// The driver handles the entire upgrade sequence atomically: sends
    /// the protocol command, awaits the tagged OK, then swaps the stream
    /// using the `Poisoned` sentinel (I9, I10). No consumer is needed.
    pub(super) async fn submit_upgrade(
        &self,
        payload: driver::UpgradePayload,
    ) -> Result<(), Error> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let dcmd = driver::DriverCommand::Upgrade { payload, result_tx };
        if self.cmd_tx.send(dcmd).await.is_err() {
            return Err(self.observe_driver_panic(TransmissionState::Unsent).await);
        }
        let guard = self.in_flight();
        let received = result_rx.await;
        guard.completed();
        match received {
            Ok(inner) => {
                inner?;
                Ok(())
            }
            Err(_) => Err(self.observe_driver_panic(TransmissionState::InFlight).await),
        }
    }
}

/// One rung of the password-credential mechanism ladder: either a mechanism
/// to attempt against the live driver, or a typed local rejection.
#[derive(Debug, Clone, PartialEq, Eq)]
enum PasswordCandidate {
    /// Mechanism is advertised and policy-allowed; attempt it.
    Attempt(crate::types::AuthMechanism),
    /// Mechanism is gated by local policy, TLS, or downgrade protection.
    Reject(crate::error::AuthMechanismRejection),
}

/// Pure, network-free selection of the password-credential mechanism ladder.
///
/// Walks the fixed preference order
/// `[ScramSha256Plus, ScramSha1Plus, ScramSha256, ScramSha1, Plain, CramMd5,
/// Login]`, emitting an `Attempt` for advertised + policy-allowed rungs and a
/// `Reject` for policy/TLS-gated ones. Per RFC 5802 Section 6, a non-PLUS
/// `SCRAM-SHA-N` rung whose matching `SCRAM-SHA-N-PLUS` is advertised is
/// emitted as `Reject(ChannelBindingUnavailable)` - the downgrade skip. PLUS
/// rungs are emitted as `Attempt`; whether the binding actually resolves needs
/// a live cert and is decided by `authenticate_best`, not here.
fn password_mechanism_ladder(
    profile: &crate::types::ServerProfile,
    policy: &crate::types::AuthPolicy,
    is_encrypted: bool,
) -> Vec<PasswordCandidate> {
    use crate::error::{AuthMechanismRejection, AuthMechanismRejectionReason};
    use crate::types::AuthMechanism;

    let order = [
        AuthMechanism::ScramSha256Plus,
        AuthMechanism::ScramSha1Plus,
        AuthMechanism::ScramSha256,
        AuthMechanism::ScramSha1,
        AuthMechanism::Plain,
        AuthMechanism::CramMd5,
        AuthMechanism::Login,
    ];

    let mut ladder = Vec::new();
    for mechanism in order {
        let advertised = if mechanism == AuthMechanism::Login {
            profile.supports_login_command()
        } else {
            profile.supports_sasl_auth(mechanism)
        };
        if !advertised {
            continue;
        }

        // Shared SASL policy owns PLUS suppression as well as the y header.
        // Binding availability for PLUS itself is resolved by the live caller.
        let unbound_hash = match mechanism {
            AuthMechanism::ScramSha256 => Some(bifrost_sasl::ScramHash::Sha256),
            AuthMechanism::ScramSha1 => Some(bifrost_sasl::ScramHash::Sha1),
            _ => None,
        };
        let downgrade_forbidden = unbound_hash.is_some_and(|hash| {
            bifrost_sasl::select_scram_rung(
                hash,
                false,
                |name| {
                    profile
                        .auth_mechanisms
                        .iter()
                        .any(|m| m.eq_ignore_ascii_case(name))
                },
                None,
            )
            .is_none()
        });
        if downgrade_forbidden {
            ladder.push(PasswordCandidate::Reject(AuthMechanismRejection::new(
                mechanism,
                AuthMechanismRejectionReason::ChannelBindingUnavailable,
            )));
            continue;
        }

        let mut reason = None;
        match mechanism {
            AuthMechanism::CramMd5 if !policy.allow_cram_md5 => {
                reason = Some(AuthMechanismRejectionReason::DisabledByPolicy);
            }
            AuthMechanism::Login if !policy.allow_login => {
                reason = Some(AuthMechanismRejectionReason::DisabledByPolicy);
            }
            _ => {}
        }
        // Cleartext gate applies to credential-bearing legacy mechanisms only;
        // SCRAM never sends reusable credentials. When a mechanism is both
        // disabled by policy and on a plaintext connection, the TLS reason is
        // reported (last-write-wins), preserving the pre-refactor behavior so
        // the rejection display does not silently change.
        let cleartext_gated = matches!(
            mechanism,
            AuthMechanism::Plain | AuthMechanism::CramMd5 | AuthMechanism::Login
        );
        if cleartext_gated && !is_encrypted && !policy.allow_cleartext_without_tls {
            reason = Some(AuthMechanismRejectionReason::CleartextWithoutTls);
        }

        match reason {
            Some(reason) => ladder.push(PasswordCandidate::Reject(AuthMechanismRejection::new(
                mechanism, reason,
            ))),
            None => ladder.push(PasswordCandidate::Attempt(mechanism)),
        }
    }
    ladder
}

fn offered_authentication(
    profile: &crate::types::ServerProfile,
    include_login: bool,
) -> Vec<String> {
    let mut offered = profile
        .auth_mechanisms
        .iter()
        .map(|mechanism| format!("AUTH={mechanism}"))
        .collect::<Vec<_>>();
    if include_login && profile.supports_login_command() {
        offered.push("LOGIN".to_owned());
    }
    offered
}

/// Check if `IMAP4rev2` behavior is active from a
/// [`ConnectionStateSnapshot`](driver::ConnectionStateSnapshot).
///
/// The snapshot-side view of the rule, delegating to the single authority in
/// `crate::types::profile` (RFC 9051 Section6.3.1). Taking the snapshot as an
/// argument is what lets a caller that already holds the borrow ask the
/// question; `ImapConnection::is_rev2` borrows and delegates here.
///
/// Answers about a SNAPSHOT, which the driver republishes only when a command
/// completes, so under concurrent handles it can be older than the live
/// protocol state. Safe for the gates on this side, which only ever refuse an
/// operation the driver would now accept or pick a legacy path that stays valid
/// under either revision. It is NOT safe for encoding wire bytes on this side
/// of the driver channel: every command's bytes (APPEND and MULTIAPPEND
/// included) are encoded by the driver from live state at send time, and a
/// handle-side encoding decision would go stale while the command is queued.
pub(super) fn is_rev2_from_snapshot(snap: &driver::ConnectionStateSnapshot) -> bool {
    crate::types::profile::imap4rev2_active(&snap.capabilities, &snap.enabled)
}

/// Whether `capability` is usable on the connection a
/// [`ConnectionStateSnapshot`](driver::ConnectionStateSnapshot) describes: the
/// snapshot-side view of the single capability authority
/// `crate::types::profile::supports` (advertised, QRESYNC implying CONDSTORE,
/// or folded into active IMAP4rev2).
///
/// EVERY handle gate of the shape "capability X, or rev2 folds it in" goes
/// through here rather than spelling `contains(&X) || is_rev2_from_snapshot`,
/// so a capability added to the rev2 baseline reaches every gate at once. The
/// same snapshot-staleness caveat as [`is_rev2_from_snapshot`] applies.
pub(super) fn snapshot_supports(
    snap: &driver::ConnectionStateSnapshot,
    capability: &Capability,
) -> bool {
    crate::types::profile::supports(&snap.capabilities, &snap.enabled, capability)
}

/// Channel-binding policy for a peer certificate, split out from the live
/// connection so the decision itself is testable and so it reads identically to
/// `bifrost_smtp`'s `resolve_scram_binding`.
///
/// `Ok(None)` means "no certificate at all", the only condition under which the
/// caller may record a `ChannelBindingUnavailable` rejection and move down the
/// mechanism ladder. A certificate that is present but unusable is an `Err`:
/// treating it as a skip would hand an attacker who can shape the presented
/// leaf a way to force the client off SCRAM-*-PLUS onto an unbound mechanism.
fn scram_binding_from_der(
    der: Option<&[u8]>,
) -> Result<Option<bifrost_sasl::ScramChannelBinding>, Error> {
    let Some(der) = der else {
        return Ok(None);
    };
    let bytes = bifrost_sasl::tls_server_end_point(der)?;
    Ok(Some(bifrost_sasl::ScramChannelBinding::TlsServerEndPoint(
        bytes,
    )))
}

#[cfg(test)]
mod binding_policy_tests {
    #![allow(clippy::unwrap_used)]
    use super::scram_binding_from_der;
    use crate::error::Error;

    #[test]
    fn absent_certificate_is_the_only_binding_skip() {
        assert!(scram_binding_from_der(None).unwrap().is_none());
    }

    #[test]
    fn unusable_certificate_is_an_error_not_a_downgrade() {
        // A present but unparseable DER. If this came back as `Ok(None)` the
        // ladder would silently continue to unbound SCRAM and then PLAIN.
        let err = scram_binding_from_der(Some(&[0x30, 0x01, 0x00])).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)), "got {err:?}");
    }
}

fn generate_scram_nonce() -> Result<String, Error> {
    use base64::Engine;

    let mut bytes = [0u8; 18];
    getrandom::fill(&mut bytes)
        // A local failure before AUTHENTICATE is submitted: nothing is on the
        // wire, and the server has done nothing wrong.
        .map_err(|e| Error::LocalRuntime(format!("failed to generate SCRAM nonce: {e}")))?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

#[cfg(test)]
mod ladder_tests {
    #![allow(clippy::unwrap_used)]
    use super::{PasswordCandidate, password_mechanism_ladder};
    use crate::error::{AuthMechanismRejection, AuthMechanismRejectionReason};
    use crate::types::{AuthMechanism, AuthPolicy, Capability, ServerProfile};

    fn profile(mechanisms: &[&str]) -> ServerProfile {
        let caps = mechanisms
            .iter()
            .map(|m| Capability::Auth((*m).to_owned()))
            .collect();
        ServerProfile::new(caps, vec![])
    }

    fn pos(ladder: &[PasswordCandidate], mechanism: AuthMechanism) -> Option<usize> {
        ladder.iter().position(|c| match c {
            PasswordCandidate::Attempt(m) => *m == mechanism,
            PasswordCandidate::Reject(r) => r.mechanism == mechanism,
        })
    }

    #[test]
    fn password_mechanism_ladder_downgrade_skip_when_plus_advertised() {
        // Server advertises SHA-256-PLUS, SHA-256, and SHA-1 (no SHA-1-PLUS).
        let profile = profile(&["SCRAM-SHA-256-PLUS", "SCRAM-SHA-256", "SCRAM-SHA-1"]);
        let ladder = password_mechanism_ladder(&profile, &AuthPolicy::default(), true);

        let plus = pos(&ladder, AuthMechanism::ScramSha256Plus).unwrap();
        assert_eq!(
            ladder[plus],
            PasswordCandidate::Attempt(AuthMechanism::ScramSha256Plus)
        );

        // The matching non-PLUS rung is the downgrade skip.
        let non_plus = pos(&ladder, AuthMechanism::ScramSha256).unwrap();
        assert_eq!(
            ladder[non_plus],
            PasswordCandidate::Reject(AuthMechanismRejection::new(
                AuthMechanism::ScramSha256,
                AuthMechanismRejectionReason::ChannelBindingUnavailable,
            ))
        );

        // SHA-1 has no PLUS offer, so it stays attemptable.
        let sha1 = pos(&ladder, AuthMechanism::ScramSha1).unwrap();
        assert_eq!(
            ladder[sha1],
            PasswordCandidate::Attempt(AuthMechanism::ScramSha1)
        );
    }

    #[test]
    fn password_mechanism_ladder_no_spurious_skip_without_plus() {
        let profile = profile(&["SCRAM-SHA-256"]);
        let ladder = password_mechanism_ladder(&profile, &AuthPolicy::default(), true);
        let idx = pos(&ladder, AuthMechanism::ScramSha256).unwrap();
        assert_eq!(
            ladder[idx],
            PasswordCandidate::Attempt(AuthMechanism::ScramSha256)
        );
    }

    #[test]
    fn password_mechanism_ladder_tls_reason_wins_over_policy_for_legacy() {
        // LOGIN advertised, disabled by policy (default), on a plaintext
        // connection: the original loop reported CleartextWithoutTls because
        // the TLS gate ran last (last-write-wins). The refactored helper must
        // preserve that, so the rejection display does not silently flip to
        // DisabledByPolicy.
        let profile = profile(&["PLAIN"]);
        let ladder = password_mechanism_ladder(&profile, &AuthPolicy::default(), false);

        // PLAIN is not policy-disabled but is cleartext-gated on plaintext.
        let plain = pos(&ladder, AuthMechanism::Plain).unwrap();
        assert_eq!(
            ladder[plain],
            PasswordCandidate::Reject(AuthMechanismRejection::new(
                AuthMechanism::Plain,
                AuthMechanismRejectionReason::CleartextWithoutTls,
            ))
        );

        // LOGIN is both disabled-by-policy and cleartext-gated; TLS wins.
        let login = pos(&ladder, AuthMechanism::Login).unwrap();
        assert_eq!(
            ladder[login],
            PasswordCandidate::Reject(AuthMechanismRejection::new(
                AuthMechanism::Login,
                AuthMechanismRejectionReason::CleartextWithoutTls,
            ))
        );
    }

    #[test]
    fn password_mechanism_ladder_orders_plus_before_non_plus_before_plain() {
        let profile = profile(&[
            "SCRAM-SHA-256-PLUS",
            "SCRAM-SHA-1-PLUS",
            "SCRAM-SHA-256",
            "SCRAM-SHA-1",
            "PLAIN",
        ]);
        let ladder = password_mechanism_ladder(&profile, &AuthPolicy::default(), true);

        let p256 = pos(&ladder, AuthMechanism::ScramSha256Plus).unwrap();
        let p1 = pos(&ladder, AuthMechanism::ScramSha1Plus).unwrap();
        let n256 = pos(&ladder, AuthMechanism::ScramSha256).unwrap();
        let plain = pos(&ladder, AuthMechanism::Plain).unwrap();

        assert!(p256 < p1, "SHA-256-PLUS must precede SHA-1-PLUS");
        assert!(p1 < n256, "SHA-1-PLUS must precede non-PLUS SCRAM");
        assert!(n256 < plain, "non-PLUS SCRAM must precede PLAIN");
    }

    /// RFC 3501 Section 7.2.1: capability names are case-insensitive, so a
    /// server advertising `AUTH=plain` advertises PLAIN. The rung gate
    /// (`ServerProfile::supports_sasl_auth`, which lowercases nothing but
    /// compares case-insensitively) and the live gate inside
    /// `authenticate_plain` (`Capability::Auth("PLAIN")` membership) must agree
    /// about that. If the live gate compared byte-exactly, such a server would
    /// have PLAIN admitted onto the ladder and then refused `MissingCapability`
    /// on every attempt: a permanent skew, not a race.
    #[tokio::test]
    async fn a_lowercase_auth_advertisement_satisfies_the_live_plain_gate() {
        use crate::connection::SessionState;
        use crate::connection::test_support::detached;
        use crate::error::Error;
        use std::time::Duration;

        let connection = detached(
            SessionState::NotAuthenticated,
            vec![Capability::Auth("plain".to_owned())],
            &[],
        );
        assert!(
            connection
                .server_profile()
                .supports_sasl_auth(AuthMechanism::Plain),
            "the rung gate admits a lowercase advertisement"
        );

        // The detached driver cannot answer, so this necessarily fails; what
        // matters is WHERE. Anything past the capability gate means the gate
        // accepted the advertisement.
        let err = connection
            .authenticate_plain("user", "pw", Duration::from_secs(5))
            .await
            .expect_err("a detached driver can never complete the exchange");
        assert!(
            matches!(err, Error::DriverGone { .. }),
            "AUTH=plain must satisfy the live AUTH=PLAIN gate and reach the \
             driver, got {err:?}"
        );
    }

    #[test]
    fn xoauth2_base64_payload_is_byte_stable() {
        use base64::Engine;

        // The base64 of the shared builder output for user/token must equal the
        // exact literal SMTP pins for the same inputs (commands.rs), proving the
        // moved bytes are unchanged on the wire.
        let encoded = base64::engine::general_purpose::STANDARD
            .encode(bifrost_sasl::xoauth2_payload("user", "token").as_bytes());
        assert_eq!(encoded, "dXNlcj11c2VyAWF1dGg9QmVhcmVyIHRva2VuAQE=");
    }

    #[test]
    fn oauthbearer_mechanism_name_is_wire_token() {
        // The OAUTHBEARER variant must map to the RFC 7628 wire token so the
        // `AUTH=OAUTHBEARER` capability parse (`supports_sasl_auth`, which
        // compares against `name()`) and the AUTHENTICATE command agree.
        assert_eq!(AuthMechanism::OAuthBearer.name(), "OAUTHBEARER");
    }

    #[test]
    fn oauthbearer_advertisement_is_selectable() {
        // A server advertising only AUTH=OAUTHBEARER must be recognized as
        // supporting it (the gap the OAUTHBEARER finding closed: previously no
        // AuthMechanism variant matched, so it fell to MissingCapability).
        let profile = profile(&["OAUTHBEARER"]);
        assert!(profile.supports_sasl_auth(AuthMechanism::OAuthBearer));
        assert!(!profile.supports_sasl_auth(AuthMechanism::XOAuth2));
    }

    #[test]
    fn oauthbearer_base64_payload_is_byte_stable() {
        use base64::Engine;

        // The base64 of the shared bifrost-sasl builder for identity/token must
        // be byte-identical to the RFC 7628 frame the consumer sends on the
        // wire: `n,a=user,\x01auth=Bearer token\x01\x01`.
        let encoded = base64::engine::general_purpose::STANDARD
            .encode(bifrost_sasl::oauthbearer_payload("user", "token").as_bytes());
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&encoded)
            .unwrap();
        assert_eq!(decoded, b"n,a=user,\x01auth=Bearer token\x01\x01");
    }
}
