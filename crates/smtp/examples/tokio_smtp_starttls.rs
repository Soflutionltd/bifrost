use bifrost_smtp::{AsyncSmtpTransport, AsyncTransport, Message, message::header::ContentType};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let email = Message::builder()
        .from("NoBody <nobody@domain.tld>".parse().unwrap())
        .reply_to("Yuin <yuin@domain.tld>".parse().unwrap())
        .to("Hei <hei@domain.tld>".parse().unwrap())
        .subject("Happy new async year")
        .header(ContentType::TEXT_PLAIN)
        .body(String::from("Be happy with async!"))
        .unwrap();

    // Open a plaintext connection and require STARTTLS before authentication.
    let mailer: AsyncSmtpTransport = AsyncSmtpTransport::starttls_relay("smtp.example.com")
        .unwrap()
        .password("user@example.com", "smtp_password")
        .build();

    // Send the email
    match mailer.send(&email).await {
        Ok(_) => println!("Email sent successfully!"),
        Err(e) => panic!("Could not send email: {e:?}"),
    }
}
