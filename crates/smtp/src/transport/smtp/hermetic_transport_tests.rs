//! Public send paths over parked in-memory connections, with no dialing.
use std::marker::PhantomData;

use super::{ClientId, Dialect, PoolConfig, SmtpConnection, SmtpInfo, SmtpTransport};
use super::{pool::sync_impl::Pool, test_support::Transcript, transport::SmtpClient};
use crate::{Message, Transport, address::Envelope};

fn email() -> Message {
    Message::builder()
        .from("NoBody <nobody@domain.tld>".parse().unwrap())
        .reply_to("Yuin <yuin@domain.tld>".parse().unwrap())
        .to("Hei <hei@domain.tld>".parse().unwrap())
        .subject("Happy new year")
        .body(String::from("Be happy!"))
        .unwrap()
}

fn envelope() -> Envelope {
    Envelope::new(
        Some("user@localhost".parse().unwrap()),
        vec!["root@localhost".parse().unwrap()],
    )
    .unwrap()
}

fn greeting() -> Transcript {
    Transcript::new("220 smtp.example\r\n")
        .expect("EHLO client.example\r\n", "250 smtp.example\r\n")
}

fn delivery(script: Transcript, from: &str, to: &str, body: &[u8]) -> Transcript {
    script
        .expect(format!("MAIL FROM:<{from}>\r\n"), "250 sender ok\r\n")
        .expect(format!("RCPT TO:<{to}>\r\n"), "250 recipient ok\r\n")
        .expect("DATA\r\n", "354 send body\r\n")
        .expect(body, "")
        .expect(
            if body.ends_with(b"\r\n") {
                ".\r\n"
            } else {
                "\r\n.\r\n"
            },
            "250 queued\r\n",
        )
}

fn transport(script: &Transcript) -> SmtpTransport {
    let conn = SmtpConnection::from_transcript(
        script.clone(),
        &ClientId::Domain("client.example".into()),
        Dialect::Smtp,
    )
    .unwrap();
    let pool = Pool::new(
        PoolConfig::new().max_size(1).test_on_checkout(false),
        SmtpClient {
            info: SmtpInfo::new("transcript.invalid", Dialect::Smtp),
        },
    );
    pool.park_for_test(conn);
    SmtpTransport {
        inner: pool,
        marker_: PhantomData,
    }
}

#[test]
fn smtp_transport_simple() {
    let email = email();
    let script = delivery(
        greeting(),
        "nobody@domain.tld",
        "hei@domain.tld",
        &email.formatted(),
    );
    transport(&script).send(&email).unwrap();
    script.assert_exhausted();
}

#[test]
fn send_one() {
    let script = delivery(greeting(), "user@localhost", "root@localhost", b"test");
    transport(&script).send_raw(&envelope(), b"test").unwrap();
    script.assert_exhausted();
}

#[test]
fn send_from_thread() {
    let mut script = greeting();
    // Identical thread payloads make the transcript independent of which
    // caller wins checkout. The main-thread payload then proves reuse.
    for body in [
        b"thread".as_slice(),
        b"thread".as_slice(),
        b"test".as_slice(),
    ] {
        script = delivery(script, "user@localhost", "root@localhost", body);
    }
    let mailer = transport(&script);
    let start = std::sync::Barrier::new(2);
    std::thread::scope(|scope| {
        let first = mailer.clone();
        let second = mailer.clone();
        let start = &start;
        let one = scope.spawn(move || {
            start.wait();
            first.send_raw(&envelope(), b"thread").unwrap();
        });
        let two = scope.spawn(move || {
            start.wait();
            second.send_raw(&envelope(), b"thread").unwrap();
        });
        one.join().unwrap();
        two.join().unwrap();
    });
    mailer.send_raw(&envelope(), b"test").unwrap();
    script.assert_exhausted();
}

#[cfg(feature = "tokio")]
#[tokio::test]
async fn smtp_transport_simple_tokio() {
    use super::{AsyncSmtpConnection, AsyncSmtpTransport, async_transport::AsyncSmtpClient};
    use crate::AsyncTransport;

    let email = email();
    let script = delivery(
        greeting(),
        "nobody@domain.tld",
        "hei@domain.tld",
        &email.formatted(),
    );
    let conn = AsyncSmtpConnection::from_transcript(
        script.clone(),
        &ClientId::Domain("client.example".into()),
        Dialect::Smtp,
    )
    .await
    .unwrap();
    let pool = super::pool::async_impl::Pool::new(
        PoolConfig::new().test_on_checkout(false),
        AsyncSmtpClient {
            info: SmtpInfo::new("transcript.invalid", Dialect::Smtp),
        },
    );
    pool.park_for_test(conn).await;
    let sender = AsyncSmtpTransport {
        inner: pool,
        marker_: PhantomData,
    };
    sender.send(&email).await.unwrap();
    script.assert_exhausted();
}
