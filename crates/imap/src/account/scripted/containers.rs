use super::test_support::*;

/// A container mutation must complete with a single-permit pool.
///
/// `container_create` holds its pooled checkout across the post-mutation
/// `refresh_folders` re-LIST. If that refresh acquired a second permit,
/// this deadlocks forever at `pool_cap = 1` even though CREATE already
/// committed server-side. The timeout turns that regression into a fast
/// failure instead of a hung suite.
#[tokio::test]
async fn container_create_completes_under_a_single_permit_pool() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);

    let script = tokio::spawn(async move {
        let create = read_line(&mut server).await;
        assert!(create.contains("CREATE"), "expected CREATE, got {create}");
        respond(
            &mut server,
            &format!("{} OK CREATE done\r\n", tag_of(&create)),
        )
        .await;

        // The re-LIST must arrive on the same connection the CREATE used.
        let list = read_line(&mut server).await;
        assert!(list.contains("LIST"), "expected LIST, got {list}");
        respond(
            &mut server,
            &format!(
                "* LIST (\\HasNoChildren) \".\" INBOX\r\n\
                 * LIST (\\HasNoChildren) \".\" Archive\r\n\
                 {} OK LIST done\r\n",
                tag_of(&list)
            ),
        )
        .await;
        server
    });

    let created = tokio::time::timeout(
        Duration::from_secs(5),
        crate::account::containers::container_create(
            account.clone(),
            ContainerKind::Folder,
            "Archive".to_owned(),
            None,
            None,
        ),
    )
    .await
    .expect("container_create must not deadlock on the pool permit")
    .expect("scripted CREATE + LIST succeed");

    assert_eq!(created.0, "Archive");
    // The refresh landed: the re-LIST replaced the personal snapshot.
    assert!(account.folders.get(&created_name()).is_some());
    let _server = script.await.unwrap();
}

/// DELETE must not be sent on a pooled connection that still has its target
/// selected. IMAP4rev2 provides UNSELECT, so this transcript proves the
/// affinity-free checkout explicitly deselects before STATUS/DELETE rather
/// than relying on a server accepting the selected-mailbox operation.
#[tokio::test]
async fn container_delete_deselects_a_target_left_selected_in_the_pool() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev2")).await;
    let account = scripted_account(conn, 1);
    let folder = crate::types::MailboxName::new("Archive").unwrap();

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(select.contains("SELECT"), "expected SELECT, got {select}");
        respond(
            &mut server,
            &format!(
                "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n\
                 * 0 EXISTS\r\n\
                 * LIST () \"/\" Archive\r\n\
                 * OK [UIDVALIDITY 1] selected\r\n\
                 {} OK [READ-WRITE] SELECT done\r\n",
                tag_of(&select)
            ),
        )
        .await;

        let unselect = read_line(&mut server).await;
        assert!(
            unselect.contains("UNSELECT"),
            "expected UNSELECT before DELETE, got {unselect}"
        );
        respond(
            &mut server,
            &format!("{} OK UNSELECT done\r\n", tag_of(&unselect)),
        )
        .await;

        let status = read_line(&mut server).await;
        assert!(status.contains("STATUS"), "expected STATUS, got {status}");
        respond(
            &mut server,
            &format!(
                "* STATUS Archive (MESSAGES 0)\r\n{} OK STATUS done\r\n",
                tag_of(&status)
            ),
        )
        .await;

        let delete = read_line(&mut server).await;
        assert!(delete.contains("DELETE"), "expected DELETE, got {delete}");
        respond(
            &mut server,
            &format!("{} OK DELETE done\r\n", tag_of(&delete)),
        )
        .await;

        let list = read_line(&mut server).await;
        assert!(list.contains("LIST"), "expected LIST, got {list}");
        respond(&mut server, &format!("{} OK LIST done\r\n", tag_of(&list))).await;
        server
    });

    let mut pooled = account.pool.checkout_any().await.unwrap();
    account
        .select_folder(&mut pooled, &folder, crate::account::SelectPurpose::Mutate)
        .await
        .unwrap();
    drop(pooled);

    crate::account::containers::container_delete(
        account.clone(),
        ContainerId("Archive".to_owned()),
    )
    .await
    .expect("selected target must be deselected before delete");
    let _server = script.await.unwrap();
}

/// A mailbox that still holds messages refuses deletion as a precondition the
/// user can resolve by emptying it, not as a malformed request, and no DELETE
/// is written.
#[tokio::test]
async fn container_delete_of_a_non_empty_mailbox_is_a_precondition_failure() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev2")).await;
    let account = scripted_account(conn, 1);

    let script = tokio::spawn(async move {
        let status = read_line(&mut server).await;
        assert!(status.contains("STATUS"), "expected STATUS, got {status}");
        respond(
            &mut server,
            &format!(
                "* STATUS Archive (MESSAGES 3)\r\n{} OK STATUS done\r\n",
                tag_of(&status)
            ),
        )
        .await;
        server
    });

    let error = crate::account::containers::container_delete(
        account.clone(),
        ContainerId("Archive".to_owned()),
    )
    .await
    .expect_err("a non-empty mailbox must not be deleted");
    assert_eq!(
        error.kind(),
        &AccountErrorKind::Request(bifrost_types::RequestErrorKind::PreconditionFailed)
    );
    assert_eq!(error.operation(), Some(AccountOperation::ContainerDelete));
    let _server = script.await.unwrap();
}
