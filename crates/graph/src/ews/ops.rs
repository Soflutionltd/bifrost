//! EWS read operations: `FindFolder`, `GetFolder`, `FindItem`,
//! `GetItem`. SOAP bodies are the ratatoskr request bodies verbatim;
//! the only reshape is the error boundary (`EwsError` not `String`) and
//! the transport (`AccountNet`, with the Bearer supplied by the net
//! layer, never a hand-built header). Routing headers thread through
//! every call via `EwsHeaders`.
//!
//! The pure body builders are factored out so their shape (the
//! `DateTimeReceived` restriction, distinguished-vs-opaque folder id) is
//! unit-pinnable without a live request.

use super::xml_helpers::{is_distinguished_folder_id, xml_escape};
use super::{
    EwsAttachmentContent, EwsClient, EwsError, EwsFolder, EwsHeaders, EwsItem, FindItemsResult,
};
use super::{
    parse_find_folder_response, parse_find_items_response, parse_get_attachment_response,
    parse_get_folder_response, parse_get_item_response,
};

impl EwsClient {
    /// Browse child folders under a parent. Pass `"publicfoldersroot"`
    /// for the top of the public-folder hierarchy.
    pub(crate) async fn find_folder(
        &self,
        parent_folder_id: &str,
        headers: &EwsHeaders,
    ) -> Result<Vec<EwsFolder>, EwsError> {
        let body = find_folder_body(parent_folder_id);
        let xml = self.execute(&body, headers).await?;
        parse_find_folder_response(&xml)
    }

    /// Fetch detailed info for one folder, including `PR_REPLICA_LIST`
    /// (used to resolve the content-mailbox routing for a public
    /// folder).
    pub(crate) async fn get_folder(
        &self,
        folder_id: &str,
        headers: &EwsHeaders,
    ) -> Result<EwsFolder, EwsError> {
        let body = get_folder_body(folder_id);
        let xml = self.execute(&body, headers).await?;
        parse_get_folder_response(&xml)
    }

    /// Find items in a folder, paged, optionally restricted to those
    /// with `DateTimeReceived >= since`, sorted descending by received
    /// time.
    pub(crate) async fn find_items(
        &self,
        folder_id: &str,
        since: Option<&str>,
        offset: u32,
        max_entries: u32,
        headers: &EwsHeaders,
    ) -> Result<FindItemsResult, EwsError> {
        let body = find_items_body(folder_id, since, offset, max_entries);
        let xml = self.execute(&body, headers).await?;
        parse_find_items_response(&xml)
    }

    /// Fetch the full body, headers, recipients, and attachment metadata of
    /// a single item. This is the public-folder hydration read: a public
    /// folder's items are raw EWS `ItemId`s that Graph REST
    /// (`/me/messages/{id}`) cannot address at all, so the hydration path
    /// dispatches here for a `Folder` scope present in the routing map.
    /// `headers` carries the folder's `X-AnchorMailbox` /
    /// `X-PublicFolderMailbox` routing pair.
    ///
    /// Cached classes select additional properties only. Unknown classes use
    /// the neutral shape; callers must verify the returned domain before
    /// projecting it. Default supplies properties appropriate to the actual
    /// wire type without imposing message field URIs on non-mail items.
    pub(crate) async fn get_item(
        &self,
        item_id: &str,
        shape: super::EwsItemShape,
        headers: &EwsHeaders,
    ) -> Result<EwsItem, EwsError> {
        let body = get_item_body(item_id, shape);
        let xml = self.execute(&body, headers).await?;
        parse_get_item_response(&xml)
    }

    /// Fetch one attachment's bytes. EWS has no byte-stream attachment
    /// endpoint (unlike Graph REST's `/attachments/{id}/$value`), so the
    /// content arrives base64-inline in the SOAP body and the parser decodes
    /// it. `headers` carries the same public-folder routing pair as the
    /// enclosing `GetItem`.
    pub(crate) async fn get_attachment(
        &self,
        attachment_id: &str,
        headers: &EwsHeaders,
    ) -> Result<EwsAttachmentContent, EwsError> {
        let body = get_attachment_body(attachment_id);
        let xml = self.execute(&body, headers).await?;
        parse_get_attachment_response(&xml)
    }
}

// ── SOAP body builders (pure) ───────────────────────────────

fn find_folder_body(parent_folder_id: &str) -> String {
    let escaped_id = xml_escape(parent_folder_id);
    let parent_xml = if is_distinguished_folder_id(parent_folder_id) {
        format!(r#"<t:DistinguishedFolderId Id="{escaped_id}"/>"#)
    } else {
        format!(r#"<t:FolderId Id="{escaped_id}"/>"#)
    };
    format!(
        r#"<m:FindFolder Traversal="Shallow">
  <m:FolderShape>
    <t:BaseShape>Default</t:BaseShape>
    <t:AdditionalProperties>
      <t:FieldURI FieldURI="folder:EffectiveRights"/>
      <t:FieldURI FieldURI="folder:FolderClass"/>
    </t:AdditionalProperties>
  </m:FolderShape>
  <m:ParentFolderIds>
    {parent_xml}
  </m:ParentFolderIds>
</m:FindFolder>"#
    )
}

fn get_folder_body(folder_id: &str) -> String {
    let escaped_id = xml_escape(folder_id);
    format!(
        r#"<m:GetFolder>
  <m:FolderShape>
    <t:BaseShape>Default</t:BaseShape>
    <t:AdditionalProperties>
      <t:FieldURI FieldURI="folder:EffectiveRights"/>
      <t:FieldURI FieldURI="folder:FolderClass"/>
      <t:ExtendedFieldURI PropertyTag="0x6698" PropertyType="Binary"/>
    </t:AdditionalProperties>
  </m:FolderShape>
  <m:FolderIds>
    <t:FolderId Id="{escaped_id}"/>
  </m:FolderIds>
</m:GetFolder>"#
    )
}

fn find_items_body(folder_id: &str, since: Option<&str>, offset: u32, max_entries: u32) -> String {
    let restriction = match since {
        Some(dt) => {
            let escaped_dt = xml_escape(dt);
            format!(
                r#"<m:Restriction>
    <t:IsGreaterThanOrEqualTo>
      <t:FieldURI FieldURI="item:DateTimeReceived"/>
      <t:FieldURIOrConstant>
        <t:Constant Value="{escaped_dt}"/>
      </t:FieldURIOrConstant>
    </t:IsGreaterThanOrEqualTo>
  </m:Restriction>"#
            )
        }
        None => String::new(),
    };
    let escaped_folder_id = xml_escape(folder_id);
    format!(
        r#"<m:FindItem Traversal="Shallow">
  <m:ItemShape>
    <t:BaseShape>Default</t:BaseShape>
    <t:AdditionalProperties>
      <t:FieldURI FieldURI="item:Subject"/>
      <t:FieldURI FieldURI="item:DateTimeReceived"/>
      <t:FieldURI FieldURI="item:Preview"/>
      <t:FieldURI FieldURI="item:Flag"/>
      <t:FieldURI FieldURI="item:Categories"/>
      <t:FieldURI FieldURI="item:ItemClass"/>
    </t:AdditionalProperties>
  </m:ItemShape>
  <m:IndexedPageItemView MaxEntriesReturned="{max_entries}" Offset="{offset}" BasePoint="Beginning"/>
  {restriction}
  <m:SortOrder>
    <t:FieldOrder Order="Descending">
      <t:FieldURI FieldURI="item:DateTimeReceived"/>
    </t:FieldOrder>
  </m:SortOrder>
  <m:ParentFolderIds>
    <t:FolderId Id="{escaped_folder_id}"/>
  </m:ParentFolderIds>
</m:FindItem>"#
    )
}

// Class-conditional: the `message:` field URIs are only valid against a
// `<t:Message>`, so a contact or calendar item gets the class-agnostic
// subset. See the `get_item` doc comment. The `Message` arm is byte-identical
// to the historical unconditional body.
fn get_item_body(item_id: &str, shape: super::EwsItemShape) -> String {
    let escaped_id = xml_escape(item_id);
    let message_properties = match shape {
        super::EwsItemShape::Message => {
            "\n      <t:FieldURI FieldURI=\"message:ToRecipients\"/>\
             \n      <t:FieldURI FieldURI=\"message:CcRecipients\"/>"
        }
        super::EwsItemShape::NonMessage => "",
    };
    format!(
        r#"<m:GetItem>
  <m:ItemShape>
    <t:BaseShape>Default</t:BaseShape>
    <t:AdditionalProperties>
      <t:FieldURI FieldURI="item:ItemClass"/>
      <t:FieldURI FieldURI="item:Body"/>
      <t:FieldURI FieldURI="item:Attachments"/>{message_properties}
    </t:AdditionalProperties>
    <t:BodyType>HTML</t:BodyType>
  </m:ItemShape>
  <m:ItemIds>
    <t:ItemId Id="{escaped_id}"/>
  </m:ItemIds>
</m:GetItem>"#
    )
}

fn get_attachment_body(attachment_id: &str) -> String {
    let escaped_id = xml_escape(attachment_id);
    format!(
        r#"<m:GetAttachment>
  <m:AttachmentShape>
    <t:IncludeMimeContent>false</t:IncludeMimeContent>
  </m:AttachmentShape>
  <m:AttachmentIds>
    <t:AttachmentId Id="{escaped_id}"/>
  </m:AttachmentIds>
</m:GetAttachment>"#
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn nc2a_subclass_boundaries_and_domains() {
        use super::super::{EwsItemKind as Kind, EwsItemShape as Shape};
        for class in [
            "IPM.NoteBook",
            "IPM.Contactless",
            "IPM.AppointmentBook",
            "IPM.DistListing",
            "IPM.Schedule.Meetingish",
            "IPM.Unknown",
            "",
        ] {
            assert_eq!(Kind::from_item_class(class), Kind::Other, "{class}");
            assert_eq!(Shape::from_item_class(class), Shape::NonMessage, "{class}");
        }
        for class in [
            "IPM.Note",
            "IPM.Note.SMIME",
            "IPM.Schedule.Meeting.Request",
            "IPM.Schedule.Meeting.Resp.Pos",
            "IPM.Schedule.Meeting.Canceled",
        ] {
            assert_eq!(Kind::from_item_class(class), Kind::Mail, "{class}");
        }
        assert_eq!(Kind::from_item_class("IPM.Contact.Custom"), Kind::Contact);
        assert_eq!(Kind::from_item_class("IPM.DistList"), Kind::Contact);
        assert_eq!(
            Kind::from_item_class("IPM.Appointment.Custom"),
            Kind::Calendar
        );
        assert_eq!(Kind::from_folder_class("IPF.Note.Custom"), Kind::Mail);
        assert_eq!(Kind::from_folder_class("IPF.NoteBook"), Kind::Other);
    }

    #[test]
    fn nc2a_mixed_inventory_requests_no_message_properties() {
        let body = find_items_body("folder", None, 0, 50);
        assert!(!body.contains("message:"));
        assert!(body.contains("item:ItemClass"));
        assert!(body.contains("<t:BaseShape>Default</t:BaseShape>"));
    }

    #[test]
    fn find_items_body_includes_datetime_restriction() {
        let with = find_items_body("AAMk=", Some("2026-03-01T10:00:00Z"), 0, 50);
        assert!(with.contains("IsGreaterThanOrEqualTo"));
        assert!(with.contains(r#"FieldURI="item:DateTimeReceived""#));
        assert!(with.contains(r#"Value="2026-03-01T10:00:00Z""#));

        let without = find_items_body("AAMk=", None, 0, 50);
        assert!(!without.contains("IsGreaterThanOrEqualTo"));
        assert!(!without.contains("Restriction"));
        // The descending DateTimeReceived sort is present regardless.
        assert!(without.contains(r#"Order="Descending""#));
    }

    #[test]
    fn find_folder_distinguished_vs_id() {
        let distinguished = find_folder_body("publicfoldersroot");
        assert!(distinguished.contains(r#"<t:DistinguishedFolderId Id="publicfoldersroot"/>"#));
        assert!(!distinguished.contains("<t:FolderId"));

        let opaque = find_folder_body("AAMkAGFk=");
        assert!(opaque.contains(r#"<t:FolderId Id="AAMkAGFk="/>"#));
        assert!(!opaque.contains("DistinguishedFolderId"));
    }

    #[test]
    fn get_item_body_requests_body_recipients_and_attachments() {
        let body = get_item_body("AAMkItem=", super::super::EwsItemShape::Message);
        assert!(body.contains(r#"<t:ItemId Id="AAMkItem="/>"#));
        assert!(body.contains(r#"FieldURI="item:Body""#));
        assert!(body.contains(r#"FieldURI="item:Attachments""#));
        assert!(body.contains(r#"FieldURI="message:ToRecipients""#));
        assert!(body.contains(r#"FieldURI="message:CcRecipients""#));
        assert!(body.contains("<t:BodyType>HTML</t:BodyType>"));
    }

    /// The message arm must stay BYTE-identical to the historical
    /// unconditional body: the whole point of the class-conditional shape is
    /// that mail hydration is untouched by it.
    #[test]
    fn the_message_shape_is_the_historical_body_verbatim() {
        let expected = r#"<m:GetItem>
  <m:ItemShape>
    <t:BaseShape>Default</t:BaseShape>
    <t:AdditionalProperties>
      <t:FieldURI FieldURI="item:ItemClass"/>
      <t:FieldURI FieldURI="item:Body"/>
      <t:FieldURI FieldURI="item:Attachments"/>
      <t:FieldURI FieldURI="message:ToRecipients"/>
      <t:FieldURI FieldURI="message:CcRecipients"/>
    </t:AdditionalProperties>
    <t:BodyType>HTML</t:BodyType>
  </m:ItemShape>
  <m:ItemIds>
    <t:ItemId Id="AAMkItem="/>
  </m:ItemIds>
</m:GetItem>"#;
        assert_eq!(
            get_item_body("AAMkItem=", super::super::EwsItemShape::Message),
            expected
        );
        // And an unknown class resolves to exactly that shape, so an item
        // whose class hydration could not learn behaves as it always did.
        assert_eq!(
            super::super::EwsItemShape::default(),
            super::super::EwsItemShape::NonMessage
        );
    }

    /// A contact / calendar item must not be asked for `message:` fields -
    /// that request is what EWS answers with `ErrorInvalidPropertyRequest`.
    #[test]
    fn the_non_message_shape_asks_for_no_message_properties() {
        let body = get_item_body("AAMkItem=", super::super::EwsItemShape::NonMessage);
        assert!(
            !body.contains("message:"),
            "leaked a message property: {body}"
        );
        // Still the properties both projections actually read.
        assert!(body.contains(r#"FieldURI="item:Body""#));
        assert!(body.contains(r#"FieldURI="item:Attachments""#));
        assert!(body.contains("<t:BodyType>HTML</t:BodyType>"));
        assert!(body.contains(r#"<t:ItemId Id="AAMkItem="/>"#));
    }

    #[test]
    fn item_and_folder_classes_map_onto_shapes() {
        use super::super::{EwsItemKind as Kind, EwsItemShape as Shape};
        for mail in ["IPM.Note", "IPM.Note.SMIME", "IPM.Schedule.Meeting.Request"] {
            assert_eq!(Shape::from_item_class(mail), Shape::Message, "{mail}");
        }
        for other in [
            "IPM.Contact",
            "IPM.DistList",
            "IPM.Appointment",
            "IPM.Whatever",
            "",
            "IPM.Task",
            "IPM.StickyNote",
        ] {
            assert_eq!(Shape::from_item_class(other), Shape::NonMessage, "{other}");
        }
        assert_eq!(Kind::from_folder_class("IPF.Note"), Kind::Mail);
        assert_eq!(Kind::from_folder_class(""), Kind::Other);
        for (class, kind) in [
            ("IPF.Contact", Kind::Contact),
            ("IPF.Appointment", Kind::Calendar),
            ("IPF.Task", Kind::Other),
        ] {
            assert_eq!(Kind::from_folder_class(class), kind, "{class}");
        }
    }

    #[test]
    fn get_attachment_body_names_the_attachment() {
        let body = get_attachment_body("AAMkAtt=");
        assert!(body.contains("<m:GetAttachment>"));
        assert!(body.contains(r#"<t:AttachmentId Id="AAMkAtt="/>"#));
    }

    #[test]
    fn get_folder_body_requests_replica_list() {
        let body = get_folder_body("AAMkPF=");
        assert!(body.contains(r#"PropertyTag="0x6698""#));
        assert!(body.contains(r#"<t:FolderId Id="AAMkPF="/>"#));
    }

    /// Every read this crate issues names exactly one id on the surface EWS
    /// answers per id. The whole-response error scan
    /// (`check_response_error`) and the single-result operation parsers are
    /// exact only under that invariant, so it is pinned per builder rather
    /// than left to inspection.
    #[test]
    fn every_ews_read_body_names_exactly_one_per_answer_id() {
        for body in [
            find_folder_body("publicfoldersroot"),
            find_folder_body("AAMkAGFk="),
            get_folder_body("AAMkPF="),
            find_items_body("AAMk=", None, 0, 50),
            find_items_body("AAMk=", Some("2026-03-01T10:00:00Z"), 100, 50),
            get_item_body("AAMkItem=", super::super::EwsItemShape::Message),
            get_item_body("AAMkItem=", super::super::EwsItemShape::NonMessage),
            get_attachment_body("AAMkAtt="),
        ] {
            assert_eq!(
                super::super::per_answer_request_ids(&body),
                1,
                "not a single-answer request: {body}"
            );
        }
    }

    #[test]
    fn routing_headers_pairs() {
        let both = EwsHeaders {
            anchor_mailbox: Some("anchor@contoso.com".to_string()),
            public_folder_mailbox: Some("pf@contoso.com".to_string()),
        };
        let pairs = both.pairs();
        assert_eq!(pairs.len(), 2);
        assert!(
            pairs
                .iter()
                .any(|(k, v)| *k == "X-AnchorMailbox" && v == "anchor@contoso.com")
        );
        assert!(
            pairs
                .iter()
                .any(|(k, v)| *k == "X-PublicFolderMailbox" && v == "pf@contoso.com")
        );

        let anchor_only = EwsHeaders {
            anchor_mailbox: Some("a@contoso.com".to_string()),
            public_folder_mailbox: None,
        };
        let pairs = anchor_only.pairs();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, "X-AnchorMailbox");

        assert!(EwsHeaders::default().pairs().is_empty());
    }
}
