//! Pure envelope validation and construction shared by every send adapter.
//! Commands are built before MAIL FROM opens a transaction, so local errors
//! cannot return a connection to the pool with a half-open transaction.

use super::smtp_data_size;
use crate::{
    address::{Address, Envelope},
    transport::smtp::{
        Protocol,
        batch::{SendProgress, SmtpBatchRecipient},
        commands::{Mail, Rcpt, build_recipient_commands, build_transaction_commands},
        error,
        error::{Error, SmtpCommandPhase, SmtpTransmissionState},
        extension::{
            DeliverByMode, Extension, FutureReleaseParameter, MailBodyParameter, MailParameter,
            RcptParameter, SendOptions, ServerInfo, addresses_match_for_recipient_options,
        },
    },
};

pub(super) struct EnvelopeBuilder<'a> {
    pub(super) server_info: &'a ServerInfo,
    pub(super) encrypted: bool,
}

impl EnvelopeBuilder<'_> {
    pub(super) fn build_envelope(
        &self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
        allow_binary_mime: bool,
    ) -> Result<(Mail, Vec<Rcpt>), Error> {
        let recipients: Vec<_> = envelope.to().iter().collect();
        let mail_options = self.mail_options(
            envelope.has_non_ascii_addresses(),
            email,
            options,
            allow_binary_mime,
        )?;
        let rcpt_options = self.rcpt_options(&recipients, options)?;
        build_transaction_commands(envelope, mail_options, &rcpt_options)
    }

    #[allow(clippy::type_complexity)]
    pub(super) fn build_batch(
        &self,
        protocol: Protocol,
        from: Option<Address>,
        recipients: Vec<SmtpBatchRecipient>,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<(Mail, Vec<Rcpt>, SendProgress), (Error, SendProgress)> {
        let progress = SendProgress::new(protocol, recipients);
        let addresses: Vec<_> = progress.recipients.iter().map(|r| &r.address).collect();
        let rcpt_options_all = self.rcpt_options(&addresses, options).map_err(|error| {
            (
                error
                    .with_attempt(SmtpTransmissionState::Unsent)
                    .with_phase(SmtpCommandPhase::RcptTo),
                progress.clone(),
            )
        })?;

        let has_non_ascii = from
            .iter()
            .chain(addresses.iter().copied())
            .any(|address| !AsRef::<str>::as_ref(address).is_ascii());
        let mail_options = self
            .mail_options(has_non_ascii, email, options, false)
            .map_err(|error| {
                (
                    error
                        .with_attempt(SmtpTransmissionState::Unsent)
                        .with_phase(SmtpCommandPhase::MailFrom),
                    progress.clone(),
                )
            })?;

        let mail_cmd = Mail::new(from, mail_options).map_err(|error| {
            (
                error
                    .with_attempt(SmtpTransmissionState::Unsent)
                    .with_phase(SmtpCommandPhase::MailFrom),
                progress.clone(),
            )
        })?;
        let rcpt_cmds = build_recipient_commands(
            progress.recipients.iter().map(|r| r.address.clone()),
            &rcpt_options_all,
        )
        .map_err(|e| (e, progress.clone()))?;

        Ok((mail_cmd, rcpt_cmds, progress))
    }

    fn rcpt_options_single(
        &self,
        addr: &Address,
        options: &SendOptions,
    ) -> Result<Vec<RcptParameter>, Error> {
        let parameters = options.rcpt_parameters_for(addr);
        for parameter in &parameters {
            self.validate_rcpt_parameter(parameter)?;
        }
        Ok(parameters)
    }

    fn rcpt_options(
        &self,
        recipients: &[&Address],
        options: &SendOptions,
    ) -> Result<Vec<Vec<RcptParameter>>, Error> {
        for (recipient, _) in options.recipient_parameters() {
            if !recipients.iter().any(|batch_recipient| {
                addresses_match_for_recipient_options(recipient, batch_recipient)
            }) {
                return Err(error::invalid_input(
                    "recipient-specific RCPT parameters do not match an envelope recipient",
                ));
            }
        }

        recipients
            .iter()
            .map(|recipient| self.rcpt_options_single(recipient, options))
            .collect()
    }

    fn mail_options(
        &self,
        has_non_ascii: bool,
        email: &[u8],
        options: &SendOptions,
        allow_binary_mime: bool,
    ) -> Result<Vec<MailParameter>, Error> {
        let mut mail_options = vec![];
        let message_size = if allow_binary_mime {
            email.len()
        } else {
            smtp_data_size(email)
        };

        let has_smtputf8 = options
            .mail_parameters()
            .iter()
            .any(|parameter| matches!(parameter, MailParameter::SmtpUtfEight));
        let has_body_parameter = options
            .mail_parameters()
            .iter()
            .any(|parameter| matches!(parameter, MailParameter::Body(_)));

        if has_non_ascii && !has_smtputf8 {
            if !self.server_info.supports_feature(Extension::SmtpUtfEight) {
                return Err(error::feature_unsupported(
                    "Envelope contains non-ascii chars but server does not support SMTPUTF8",
                ));
            }
            mail_options.push(MailParameter::SmtpUtfEight);
        }

        if !email.is_ascii() && !has_body_parameter {
            if !self.server_info.supports_feature(Extension::EightBitMime) {
                return Err(error::feature_unsupported(
                    "Message contains non-ascii chars but server does not support 8BITMIME",
                ));
            }
            mail_options.push(MailParameter::Body(MailBodyParameter::EightBitMime));
        }

        if self.server_info.supports_size()
            && !options
                .mail_parameters()
                .iter()
                .any(|parameter| matches!(parameter, MailParameter::Size(_)))
        {
            if self
                .server_info
                .size_limit()
                .is_some_and(|limit| message_size > limit)
            {
                return Err(error::invalid_input(
                    "Message is larger than the server-advertised SIZE limit",
                ));
            }
            mail_options.push(MailParameter::Size(message_size));
        }

        for parameter in options.mail_parameters() {
            self.validate_mail_parameter(
                parameter,
                message_size,
                email.is_ascii(),
                allow_binary_mime,
            )?;
            mail_options.push(parameter.clone());
        }

        // Compare emitted keywords, including custom aliases and inferred options.
        // No MAIL keyword may occur twice, regardless of ASCII case or value.
        let mut keywords = std::collections::HashSet::new();
        for parameter in &mail_options {
            let rendered = parameter.to_string();
            let keyword = rendered.split('=').next().expect("parameter has a keyword");
            if !keywords.insert(keyword.to_ascii_uppercase()) {
                return Err(error::invalid_input("duplicate MAIL parameter keyword"));
            }
        }
        Ok(mail_options)
    }

    fn validate_mail_parameter(
        &self,
        parameter: &MailParameter,
        message_size: usize,
        message_is_ascii: bool,
        allow_binary_mime: bool,
    ) -> Result<(), Error> {
        parameter.validate_syntax()?;

        match parameter {
            MailParameter::Body(MailBodyParameter::SevenBit) => {
                if message_is_ascii {
                    Ok(())
                } else {
                    Err(error::invalid_input(
                        "BODY=7BIT cannot be used with non-ASCII message content",
                    ))
                }
            }
            MailParameter::Body(MailBodyParameter::EightBitMime) => {
                if self.server_info.supports_feature(Extension::EightBitMime) {
                    Ok(())
                } else {
                    Err(error::feature_unsupported(
                        "BODY=8BITMIME requires server 8BITMIME support",
                    ))
                }
            }
            MailParameter::Body(MailBodyParameter::BinaryMime) => {
                if !allow_binary_mime {
                    return Err(error::invalid_input(
                        "BODY=BINARYMIME requires a BDAT send path",
                    ));
                }
                if self.server_info.supports_binary_mime() {
                    Ok(())
                } else {
                    Err(error::feature_unsupported(
                        "BODY=BINARYMIME requires server BINARYMIME support",
                    ))
                }
            }
            MailParameter::Size(size) => {
                // Explicit SIZE needs advertisement just as inferred SIZE does.
                if !self.server_info.supports_size() {
                    return Err(error::feature_unsupported(
                        "SIZE requires server SIZE support",
                    ));
                }
                if self
                    .server_info
                    .size_limit()
                    .is_some_and(|limit| *size > limit || message_size > limit)
                {
                    Err(error::invalid_input(
                        "Message is larger than the server-advertised SIZE limit",
                    ))
                } else {
                    Ok(())
                }
            }
            MailParameter::SmtpUtfEight => {
                if self.server_info.supports_feature(Extension::SmtpUtfEight) {
                    Ok(())
                } else {
                    Err(error::feature_unsupported(
                        "SMTPUTF8 requires server SMTPUTF8 support",
                    ))
                }
            }
            MailParameter::RequireTls => {
                if !self.server_info.supports_require_tls() {
                    return Err(error::feature_unsupported(
                        "REQUIRETLS requires server REQUIRETLS support",
                    ));
                }
                if !self.encrypted {
                    return Err(error::policy(
                        "REQUIRETLS requires an encrypted SMTP connection",
                    ));
                }
                Ok(())
            }
            MailParameter::FutureRelease(value) => {
                if !self.server_info.supports_future_release() {
                    return Err(error::feature_unsupported(
                        "FUTURERELEASE requires server FUTURERELEASE support",
                    ));
                }
                match value {
                    FutureReleaseParameter::HoldFor(seconds)
                        if self
                            .server_info
                            .future_release_max_interval()
                            .is_some_and(|limit| *seconds > limit) =>
                    {
                        return Err(error::parameter_over_limit(
                            "HOLDFOR exceeds the server-advertised FUTURERELEASE limit",
                        ));
                    }
                    _ => {}
                }
                Ok(())
            }
            MailParameter::DeliverBy(value) => {
                if !self.server_info.supports_deliver_by() {
                    return Err(error::feature_unsupported(
                        "BY requires server DELIVERBY support",
                    ));
                }
                if value.mode() == DeliverByMode::Return
                    && value.seconds() > 0
                    && self
                        .server_info
                        .deliver_by_minimum()
                        .is_some_and(|minimum| value.seconds() < minimum)
                {
                    return Err(error::invalid_input(
                        "BY return deadline is below the server-advertised DELIVERBY minimum",
                    ));
                }
                Ok(())
            }
            MailParameter::MtPriority(_) => {
                if self.server_info.supports_mt_priority() {
                    Ok(())
                } else {
                    Err(error::feature_unsupported(
                        "MT-PRIORITY requires server MT-PRIORITY support",
                    ))
                }
            }
            MailParameter::DsnReturn(_) | MailParameter::EnvelopeId(_) => {
                if self.server_info.supports_dsn() {
                    Ok(())
                } else {
                    Err(error::feature_unsupported(
                        "DSN MAIL parameters require server DSN support",
                    ))
                }
            }
            MailParameter::Other { keyword, .. } | MailParameter::OtherRaw { keyword, .. } => {
                Self::validate_custom_mail_keyword(keyword)
            }
        }
    }

    fn validate_custom_mail_keyword(keyword: &str) -> Result<(), Error> {
        // The generic variants are for extensions without a typed representation.
        // Letting them spell a standard MAIL keyword skips the capability and value
        // checks above, such as the SIZE advertisement and limit checks.
        const TYPED_MAIL_KEYWORDS: [&str; 10] = [
            "BODY",
            "SIZE",
            "SMTPUTF8",
            "REQUIRETLS",
            "HOLDFOR",
            "HOLDUNTIL",
            "BY",
            "MT-PRIORITY",
            "RET",
            "ENVID",
        ];

        if TYPED_MAIL_KEYWORDS
            .iter()
            .any(|typed_keyword| keyword.eq_ignore_ascii_case(typed_keyword))
        {
            return Err(error::invalid_input(format!(
                "standard MAIL parameter {keyword} must use its typed variant"
            )));
        }

        Ok(())
    }

    fn validate_rcpt_parameter(&self, parameter: &RcptParameter) -> Result<(), Error> {
        parameter.validate_syntax()?;

        match parameter {
            RcptParameter::Notify(_) | RcptParameter::OriginalRecipient { .. } => {
                if self.server_info.supports_dsn() {
                    Ok(())
                } else {
                    Err(error::feature_unsupported(
                        "DSN RCPT parameters require server DSN support",
                    ))
                }
            }
            RcptParameter::Other { .. } => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::smtp::{
        extension::{DsnNotify, DsnReturn},
        response::parse_response,
    };
    use bifrost_types::error::BatchItemId;

    fn server(reply: &str) -> ServerInfo {
        let (_, response) = parse_response(reply).unwrap();
        ServerInfo::from_response(&response).unwrap()
    }

    fn envelope() -> Envelope {
        Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap()
    }

    // Exercise the public construction paths, not only the validation helper:
    // direct DATA/BDAT and SMTP/LMTP batch sends must share each refusal.
    fn rejects(info: &ServerInfo, options: &SendOptions, body: &[u8], message: &str) {
        let builder = EnvelopeBuilder {
            server_info: info,
            encrypted: false,
        };
        let envelope = envelope();
        for binary in [false, true] {
            let error = builder
                .build_envelope(&envelope, body, options, binary)
                .unwrap_err();
            assert!(error.to_string().contains(message), "{error}");
        }
        for protocol in [Protocol::Smtp, Protocol::Lmtp] {
            let recipients = vec![SmtpBatchRecipient {
                id: BatchItemId("recipient".to_owned()),
                address: envelope.to()[0].clone(),
            }];
            let Err((error, progress)) = builder.build_batch(
                protocol,
                envelope.from().cloned(),
                recipients,
                body,
                options,
            ) else {
                panic!("invalid options must fail before MAIL FROM");
            };
            assert!(error.to_string().contains(message), "{error}");
            assert_eq!(error.phase(), Some(SmtpCommandPhase::MailFrom));
            assert_eq!(error.attempt(), Some(SmtpTransmissionState::Unsent));
            assert_eq!(progress.recipients.len(), 1);
        }
    }

    #[test]
    fn explicit_size_requires_advertisement_on_every_send_path() {
        rejects(
            &server("250 smtp.example\r\n"),
            &SendOptions::new().mail_parameter(MailParameter::Size(4)),
            b"body",
            "SIZE requires server SIZE support",
        );
    }

    #[test]
    fn generic_mail_parameters_cannot_spell_typed_keywords() {
        let info = server("250 smtp.example\r\n");
        for keyword in [
            "BODY",
            "SIZE",
            "SMTPUTF8",
            "REQUIRETLS",
            "HOLDFOR",
            "HOLDUNTIL",
            "BY",
            "MT-PRIORITY",
            "RET",
            "ENVID",
        ] {
            rejects(
                &info,
                &SendOptions::new().mail_parameter(MailParameter::Other {
                    keyword: keyword.to_ascii_lowercase(),
                    value: Some("1".to_owned()),
                }),
                b"body",
                "must use its typed variant",
            );
            rejects(
                &info,
                &SendOptions::new().mail_parameter(MailParameter::OtherRaw {
                    keyword: keyword.to_owned(),
                    value: Some("1".to_owned()),
                }),
                b"body",
                "must use its typed variant",
            );
        }
    }

    #[test]
    fn unknown_custom_mail_parameters_remain_available() {
        let info = server("250 smtp.example\r\n");
        let builder = EnvelopeBuilder {
            server_info: &info,
            encrypted: false,
        };
        let options = SendOptions::new()
            .mail_parameter(MailParameter::Other {
                keyword: "X-VENDOR".to_owned(),
                value: Some("a b".to_owned()),
            })
            .mail_parameter(MailParameter::OtherRaw {
                keyword: "X-RAW".to_owned(),
                value: Some("value!".to_owned()),
            });

        let (mail, _) = builder
            .build_envelope(&envelope(), b"body", &options, false)
            .unwrap();
        assert_eq!(
            mail.to_string(),
            "MAIL FROM:<sender@example.com> X-VENDOR=a+20b X-RAW=value!\r\n"
        );
    }

    #[test]
    fn duplicate_mail_keywords_are_rejected_for_typed_and_custom_parameters() {
        let info = server("250-smtp.example\r\n250-DSN\r\n250-8BITMIME\r\n250 SIZE 1000\r\n");
        let cases = [
            SendOptions::new()
                .dsn_return(DsnReturn::Full)
                .dsn_return(DsnReturn::Headers),
            SendOptions::new()
                .mail_parameter(MailParameter::Body(MailBodyParameter::SevenBit))
                .mail_parameter(MailParameter::Body(MailBodyParameter::EightBitMime)),
            SendOptions::new()
                .mail_parameter(MailParameter::Size(4))
                .mail_parameter(MailParameter::Size(5)),
            SendOptions::new()
                .mail_parameter(MailParameter::Other {
                    keyword: "X-TEST".to_owned(),
                    value: None,
                })
                .mail_parameter(MailParameter::OtherRaw {
                    keyword: "x-test".to_owned(),
                    value: None,
                }),
        ];
        for options in cases {
            rejects(&info, &options, b"body", "duplicate MAIL parameter keyword");
        }
    }

    #[test]
    fn advertised_size_is_emitted_once_and_still_enforces_limits() {
        for reply in [
            "250-smtp.example\r\n250 SIZE\r\n",
            "250-smtp.example\r\n250 SIZE 10\r\n",
        ] {
            let info = server(reply);
            let builder = EnvelopeBuilder {
                server_info: &info,
                encrypted: false,
            };
            for (options, size) in [
                (SendOptions::new(), 6),
                (SendOptions::new().mail_parameter(MailParameter::Size(4)), 4),
            ] {
                let (mail, _) = builder
                    .build_envelope(&envelope(), b"body", &options, false)
                    .unwrap();
                assert_eq!(
                    mail.to_string(),
                    format!("MAIL FROM:<sender@example.com> SIZE={size}\r\n")
                );
                let (mail, _) = builder
                    .build_envelope(&envelope(), b"body", &options, true)
                    .unwrap();
                assert_eq!(
                    mail.to_string(),
                    "MAIL FROM:<sender@example.com> SIZE=4\r\n"
                );
            }
        }
        let info = server("250-smtp.example\r\n250 SIZE 3\r\n");
        for options in [
            SendOptions::new(),
            SendOptions::new().mail_parameter(MailParameter::Size(1)),
        ] {
            rejects(&info, &options, b"body", "server-advertised SIZE limit");
        }
        rejects(
            &server("250-smtp.example\r\n250 SIZE 10\r\n"),
            &SendOptions::new().mail_parameter(MailParameter::Size(11)),
            b"body",
            "server-advertised SIZE limit",
        );
    }

    #[test]
    fn direct_and_batch_commands_share_inference_and_recipient_overrides() {
        let info =
            server("250-smtp.example\r\n250-DSN\r\n250-8BITMIME\r\n250-SMTPUTF8\r\n250 SIZE\r\n");
        let builder = EnvelopeBuilder {
            server_info: &info,
            encrypted: false,
        };
        let envelope = Envelope::new(
            Some(Address::new_dangerous("s\u{e9}nder", "example.com")),
            envelope().to().to_vec(),
        )
        .unwrap();
        let options = SendOptions::new()
            .notify([DsnNotify::Success])
            .unwrap()
            .recipient_never_notify(envelope.to()[0].clone());
        let body = b"\xc3\xa9";
        let (mail, rcpt) = builder
            .build_envelope(&envelope, body, &options, false)
            .unwrap();
        assert_eq!(
            mail.to_string(),
            "MAIL FROM:<s\u{e9}nder@example.com> SMTPUTF8 BODY=8BITMIME SIZE=4\r\n"
        );
        assert_eq!(
            rcpt[0].to_string(),
            "RCPT TO:<recipient@example.com> NOTIFY=NEVER\r\n"
        );
        for protocol in [Protocol::Smtp, Protocol::Lmtp] {
            let recipients = vec![SmtpBatchRecipient {
                id: BatchItemId("recipient".to_owned()),
                address: envelope.to()[0].clone(),
            }];
            let Ok((batch_mail, batch_rcpt, _)) = builder.build_batch(
                protocol,
                envelope.from().cloned(),
                recipients,
                body,
                &options,
            ) else {
                panic!("valid options must build");
            };
            assert_eq!(batch_mail, mail);
            assert_eq!(batch_rcpt, rcpt);
        }
    }
}
