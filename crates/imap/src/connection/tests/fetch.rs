use super::*;

#[tokio::test]
async fn uid_fetch_round_trip_with_a_literal_body() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;

    // Get into Selected state first.
    let select_script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        let tag = tag_of(&line).to_owned();
        respond(
            &mut server,
            &format!(
                "* 1 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * FLAGS (\\Seen)\r\n\
                 * OK [UIDVALIDITY 1] UIDs valid\r\n\
                 {tag} OK [READ-WRITE] SELECT completed\r\n"
            ),
        )
        .await;
        server
    });
    conn.select("INBOX", Duration::from_secs(5)).await.unwrap();
    let mut server = select_script.await.unwrap();

    let fetch_script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        assert!(line.contains("UID FETCH"), "unexpected command: {line}");
        let tag = tag_of(&line).to_owned();
        respond(
            &mut server,
            &format!(
                "* 1 FETCH (UID 7 FLAGS (\\Seen) BODY[TEXT] {{11}}\r\nHELLO WORLD)\r\n\
                 {tag} OK UID FETCH completed\r\n"
            ),
        )
        .await;
        server
    });

    let set = SequenceSet::new("7").unwrap();
    let items = [
        FetchAttr::Uid,
        FetchAttr::Flags,
        FetchAttr::BodySection {
            peek: true,
            section: Some("TEXT".to_owned()),
            partial: None,
        },
    ];
    let fetched = conn
        .uid_fetch(&set, &items, Duration::from_secs(5))
        .await
        .unwrap();
    let _server = fetch_script.await.unwrap();

    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].uid, Some(7));
    assert_eq!(fetched[0].flags.as_deref(), Some(&[Flag::Seen][..]));
    assert_eq!(fetched[0].body_sections.len(), 1);
    assert_eq!(
        fetched[0].body_sections[0].data.as_deref(),
        Some(&b"HELLO WORLD"[..])
    );
}
