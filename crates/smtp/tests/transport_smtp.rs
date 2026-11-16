#[cfg(test)]
mod sync {
    use bifrost_smtp::{Message, SmtpTransport, Transport};

    #[test]
    #[ignore = "requires a local SMTP server on 127.0.0.1:2525"]
    fn smtp_transport_simple() {
        let email = Message::builder()
            .from("NoBody <nobody@domain.tld>".parse().unwrap())
            .reply_to("Yuin <yuin@domain.tld>".parse().unwrap())
            .to("Hei <hei@domain.tld>".parse().unwrap())
            .subject("Happy new year")
            .body(String::from("Be happy!"))
            .unwrap();

        let sender = SmtpTransport::builder_dangerous("127.0.0.1")
            .port(2525)
            .build();
        sender.send(&email).unwrap();
    }
}

#[cfg(test)]
#[cfg(feature = "tokio")]
mod tokio {
    use bifrost_smtp::{AsyncSmtpTransport, AsyncTransport, Message};

    #[tokio::test]
    #[ignore = "requires a local SMTP server on 127.0.0.1:2525"]
    async fn smtp_transport_simple_tokio() {
        let email = Message::builder()
            .from("NoBody <nobody@domain.tld>".parse().unwrap())
            .reply_to("Yuin <yuin@domain.tld>".parse().unwrap())
            .to("Hei <hei@domain.tld>".parse().unwrap())
            .subject("Happy new year")
            .body(String::from("Be happy!"))
            .unwrap();

        let sender: AsyncSmtpTransport = AsyncSmtpTransport::builder_dangerous("127.0.0.1")
            .port(2525)
            .build();
        sender.send(&email).await.unwrap();
    }
}

// The oversized-banner cap that used to be pinned here against a loopback
// listener now lives in-crate as
// `an_oversized_greeting_line_is_a_parse_error_not_a_hang`, in both the
// blocking and the async connection transcript suites. It needs no socket:
// the transcript hands the driver the oversized greeting directly.
