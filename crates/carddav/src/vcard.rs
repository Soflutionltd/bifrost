use base64::Engine;
use bifrost_types::{
    AddressBookId, ContactAddress, ContactCard, ContactCorpus, ContactCreate, ContactEmail,
    ContactId, ContactOrganization, ContactPatch, ContactPhone, ContactPhoto, ContactProvenance,
    ProtocolKind,
};
use caldata::LineReader;

/// Projection failed because the resource body could not be unfolded into
/// content lines (invalid UTF-8 inside a folded run). The caller degrades a
/// single bad `.vcf` to a per-resource skip routed through `failed_hrefs`
/// rather than failing the whole sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VCardParseError(pub(crate) String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedVCard {
    pub(crate) display_name: Option<String>,
    pub(crate) emails: Vec<ContactEmail>,
    pub(crate) phones: Vec<ContactPhone>,
    pub(crate) organizations: Vec<ContactOrganization>,
    /// Raw structured ORG components, before personal-contact flattening.
    organization_components: Vec<Vec<String>>,
    pub(crate) addresses: Vec<ContactAddress>,
    pub(crate) notes: Option<String>,
    pub(crate) photo_url: Option<String>,
    pub(crate) photo: Option<ContactPhoto>,
}

pub(crate) fn contact_from_vcard(
    uri: String,
    address_book_id: Option<AddressBookId>,
    etag: Option<String>,
    data: &str,
) -> Result<ContactCard, VCardParseError> {
    let parsed = parse_vcard(data)?;
    Ok(ContactCard {
        id: ContactId(uri.clone()),
        address_book_id: address_book_id.clone(),
        native_id: uri.clone(),
        etag,
        provenance: ContactProvenance {
            provider: ProtocolKind::CardDav,
            native: uri,
            address_book_native: address_book_id.map(|id| id.0),
        },
        // CardDAV has no auto-collected corpus; every vCard is a personal
        // contact.
        corpus: ContactCorpus::Main,
        display_name: parsed.display_name,
        emails: parsed.emails,
        phones: parsed.phones,
        organizations: parsed.organizations,
        addresses: parsed.addresses,
        notes: parsed.notes,
        photo_url: parsed.photo_url,
        photo: parsed.photo,
    })
}

/// Dedicated read-only directory projection: no personal-contact identity or
/// corpus, and ORG components are read before the personal model flattens them.
pub(crate) fn directory_from_vcard(
    data: &str,
) -> Result<Option<bifrost_types::DirectoryCard>, VCardParseError> {
    let parsed = parse_vcard_for(data, true)?;
    let primary = parsed
        .emails
        .iter()
        .position(|e| e.is_primary && !e.value.trim().is_empty())
        .or_else(|| {
            parsed
                .emails
                .iter()
                .position(|e| !e.value.trim().is_empty())
        });
    let Some(primary) = primary else {
        return Ok(None);
    };
    let components = parsed.organization_components.first();
    let company = components
        .and_then(|c| c.first())
        .filter(|s| !s.is_empty())
        .cloned();
    // ORG's remaining components are nested organizational units, in order.
    let department = components
        .map(|c| {
            c.iter()
                .skip(1)
                .filter(|s| !s.is_empty())
                .cloned()
                .collect::<Vec<_>>()
                .join("; ")
        })
        .filter(|s| !s.is_empty());
    Ok(Some(bifrost_types::DirectoryCard {
        email: parsed.emails[primary].value.trim().to_string(),
        additional_emails: parsed
            .emails
            .into_iter()
            .enumerate()
            .filter(|(index, e)| *index != primary && !e.value.trim().is_empty())
            .map(|(_, e)| e.value)
            .collect(),
        display_name: parsed.display_name,
        phones: parsed.phones.into_iter().map(|p| p.value).collect(),
        company,
        department,
        title: parsed.organizations.iter().find_map(|o| o.title.clone()),
        provider: ProtocolKind::CardDav,
    }))
}

/// A consumer value the writers refuse to put on the wire.
///
/// The vCard writers emit `NAME:value` lines by concatenation. TEXT values
/// (RFC 6350 s3.4) escape CR and LF as `\n`, and parameter values take the
/// RFC 6868 caret form for CR, LF and DQUOTE, but every other ASCII control
/// character (NUL included) has no escape form in either and is not valid vCard.
/// It is REFUSED rather than stripped: a stripped value writes something the
/// caller did not ask for. The account layer maps this to `Request(Malformed)`
/// naming `field`, before any request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VCardWriteError {
    pub(crate) field: &'static str,
    pub(crate) reason: &'static str,
}

const CONTROL_CHARACTER: &str = "contains a control character";

/// Any ASCII control character other than tab, CR and LF.
fn is_unwritable_control(ch: char) -> bool {
    ch.is_ascii_control() && !matches!(ch, '\t' | '\r' | '\n')
}

/// A TEXT value, escaped for the wire; refused when it holds a control
/// character CR and LF do not cover.
fn text_value(field: &'static str, value: &str) -> Result<String, VCardWriteError> {
    if value.chars().any(is_unwritable_control) {
        return Err(VCardWriteError {
            field,
            reason: CONTROL_CHARACTER,
        });
    }
    Ok(escape_text(value))
}

/// A parameter value, RFC 6868 encoded; refused when it holds a control
/// character the caret form cannot carry.
fn param_value(field: &'static str, value: &str) -> Result<String, VCardWriteError> {
    if value.chars().any(is_unwritable_control) {
        return Err(VCardWriteError {
            field,
            reason: CONTROL_CHARACTER,
        });
    }
    Ok(escape_param(value))
}

/// A URI value, written verbatim: RFC 6350 section 3.4 escapes TEXT only, so a
/// comma or semicolon in a URL goes on the wire bare. A URI has no escape form
/// at all, so every control character is refused, CR and LF included, since a
/// line break would start a new content line.
fn uri_value<'a>(field: &'static str, value: &'a str) -> Result<&'a str, VCardWriteError> {
    if value.chars().any(|ch| ch.is_ascii_control()) {
        return Err(VCardWriteError {
            field,
            reason: CONTROL_CHARACTER,
        });
    }
    Ok(value)
}

pub(crate) fn vcard_from_create(
    contact: &ContactCreate,
    uid: &str,
) -> Result<String, VCardWriteError> {
    let version = VCardVersion::V4;
    let mut lines = vec![
        "BEGIN:VCARD".to_string(),
        "VERSION:4.0".to_string(),
        format!("UID:{}", text_value("uid", uid)?),
    ];
    // N is mandatory in vCard 3.0 and expected by several servers even on a
    // 4.0 card; synthesize a minimal structured name from the display name so
    // a created card is not rejected for the missing property.
    append_n(&mut lines, contact.display_name.as_deref())?;
    append_contact_fields(&mut lines, contact, version)?;
    lines.push("END:VCARD".to_string());
    Ok(fold_vcard_lines(lines))
}

pub(crate) fn vcard_from_patch(
    current: &ContactCard,
    source: &str,
    patch: &ContactPatch,
) -> Result<String, VCardWriteError> {
    // Detect the card's declared version so freshly emitted lines (TYPE/PREF
    // form, PHOTO form) match the version the preserved VERSION line keeps.
    let version = detect_version(source);

    // Group prefixes (`item1.EMAIL` / `item1.X-ABLabel`, the Apple Contacts
    // shape) live on the lines a patch replaces. When EMAIL/TEL/ADR are
    // rewritten we carry the groups off the lines being removed so an
    // X-ABLabel does not detach from its now-rewritten property.
    let mut email_groups = Vec::new();
    let mut phone_groups = Vec::new();
    let mut address_groups = Vec::new();

    let mut preserved: Vec<LineGroup<'_>> = Vec::new();
    let mut insert_at = None;
    let mut saw_fn = false;
    for group in logical_line_groups(source) {
        let head = group.logical_head();
        let property = property_name(head);
        if property.as_deref() == Some("END") {
            insert_at = Some(preserved.len());
        }
        if property.as_deref() == Some("FN") {
            saw_fn = true;
        }
        if should_replace_property(property.as_deref(), patch) {
            if let Some(prefix) = line_group_prefix(head) {
                match property.as_deref() {
                    Some("EMAIL") => carry_group(&mut email_groups, prefix),
                    Some("TEL") => carry_group(&mut phone_groups, prefix),
                    Some("ADR") => carry_group(&mut address_groups, prefix),
                    _ => {}
                }
            }
            continue;
        }
        preserved.push(group);
    }

    let mut replacement = Vec::new();
    if let Some(display_name) = &patch.display_name {
        append_fn(&mut replacement, display_name.as_deref())?;
    } else if !saw_fn {
        append_fn(&mut replacement, current.display_name.as_deref())?;
    }
    if let Some(emails) = &patch.emails {
        append_emails(&mut replacement, emails, version, &email_groups)?;
    }
    if let Some(phones) = &patch.phones {
        append_phones(&mut replacement, phones, version, &phone_groups)?;
    }
    if let Some(organizations) = &patch.organizations {
        append_organizations(&mut replacement, organizations)?;
    }
    if let Some(addresses) = &patch.addresses {
        append_addresses(&mut replacement, addresses, version, &address_groups)?;
    }
    if let Some(notes) = &patch.notes {
        append_notes(&mut replacement, notes.as_deref())?;
    }
    if let Some(photo_url) = &patch.photo_url {
        append_photo(&mut replacement, photo_url.as_deref())?;
    }
    if let Some(photo) = &patch.photo {
        append_inline_photo(&mut replacement, photo.as_ref(), version)?;
    }

    // Re-emit preserved lines verbatim (including their original fold
    // continuations) and fold only the freshly emitted replacement lines, so a
    // long unmodeled folded line round-trips byte for byte.
    let mut out = String::new();
    let at = insert_at.unwrap_or(preserved.len());
    for (index, group) in preserved.iter().enumerate() {
        if index == at {
            out.push_str(&fold_vcard_lines(std::mem::take(&mut replacement)));
        }
        group.push_verbatim(&mut out);
    }
    if at >= preserved.len() && !replacement.is_empty() {
        out.push_str(&fold_vcard_lines(replacement));
    }
    Ok(out)
}

/// vCard version the card declares, controlling the TYPE/PREF and inline PHOTO
/// syntax of any line bifrost emits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VCardVersion {
    V3,
    V4,
}

fn detect_version(source: &str) -> VCardVersion {
    for group in logical_line_groups(source) {
        let head = group.logical_head();
        if property_name(head).as_deref() == Some("VERSION") {
            let value = head.split_once(':').map(|(_, value)| value).unwrap_or("");
            if value.trim().starts_with("3.") {
                return VCardVersion::V3;
            }
            if value.trim().starts_with("4.") {
                return VCardVersion::V4;
            }
        }
    }
    // No VERSION line: default to 4.0, matching the create path.
    VCardVersion::V4
}

fn carry_group(groups: &mut Vec<String>, prefix: &str) {
    if !groups.iter().any(|known| known == prefix) {
        groups.push(prefix.to_string());
    }
}

/// Pair a replacement entry with the group prefix at the same index so a
/// rewritten EMAIL keeps the `item1.` it had, holding the X-ABLabel binding.
fn group_for(groups: &[String], index: usize) -> Option<&str> {
    groups.get(index).map(String::as_str)
}

fn append_contact_fields(
    lines: &mut Vec<String>,
    contact: &ContactCreate,
    version: VCardVersion,
) -> Result<(), VCardWriteError> {
    append_fn(lines, contact.display_name.as_deref())?;
    append_emails(lines, &contact.emails, version, &[])?;
    append_phones(lines, &contact.phones, version, &[])?;
    append_organizations(lines, &contact.organizations)?;
    append_addresses(lines, &contact.addresses, version, &[])?;
    append_notes(lines, contact.notes.as_deref())?;
    append_photo(lines, contact.photo_url.as_deref())
}

fn append_fn(lines: &mut Vec<String>, display_name: Option<&str>) -> Result<(), VCardWriteError> {
    lines.push(format!(
        "FN:{}",
        text_value("display name", display_name.unwrap_or(""))?
    ));
    Ok(())
}

fn append_n(lines: &mut Vec<String>, display_name: Option<&str>) -> Result<(), VCardWriteError> {
    // Place the whole display name in the family-name slot; the remaining four
    // structured components stay empty. This is a deliberately minimal but
    // RFC-valid N so 3.0-strict servers accept the card.
    let family = text_value("display name", display_name.unwrap_or(""))?;
    lines.push(format!("N:{family};;;;"));
    Ok(())
}

fn append_emails(
    lines: &mut Vec<String>,
    emails: &[ContactEmail],
    version: VCardVersion,
    groups: &[String],
) -> Result<(), VCardWriteError> {
    for (index, email) in emails.iter().enumerate() {
        let prefix = group_prefix(group_for(groups, index));
        let params = type_param(
            "email type",
            email.kind.as_deref(),
            email.is_primary,
            version,
        )?;
        lines.push(format!(
            "{prefix}EMAIL{params}:{}",
            text_value("email", &email.value)?
        ));
    }
    Ok(())
}

fn append_phones(
    lines: &mut Vec<String>,
    phones: &[ContactPhone],
    version: VCardVersion,
    groups: &[String],
) -> Result<(), VCardWriteError> {
    for (index, phone) in phones.iter().enumerate() {
        let prefix = group_prefix(group_for(groups, index));
        let params = type_param(
            "phone type",
            phone.kind.as_deref(),
            phone.is_primary,
            version,
        )?;
        lines.push(format!(
            "{prefix}TEL{params}:{}",
            text_value("phone", &phone.value)?
        ));
    }
    Ok(())
}

fn append_organizations(
    lines: &mut Vec<String>,
    organizations: &[ContactOrganization],
) -> Result<(), VCardWriteError> {
    for organization in organizations {
        // ORG is a structured (`;`-delimited) value; the model carries only a
        // single name, but encoding it as components rather than escaping the
        // separators preserves any `;`-bearing org name a caller round-trips
        // from a parse (where components are rejoined with `;`).
        let value = organization
            .name
            .split(';')
            .map(|part| text_value("organization name", part))
            .collect::<Result<Vec<_>, _>>()?
            .join(";");
        // A title-only organization (the shape a standalone TITLE line parses
        // into) emits no ORG line: a bare `ORG:` would invent an empty
        // organization the card never had.
        if !value.is_empty() {
            lines.push(format!("ORG:{value}"));
        }
        if let Some(title) = organization.title.as_deref() {
            lines.push(format!(
                "TITLE:{}",
                text_value("organization title", title)?
            ));
        }
    }
    Ok(())
}

fn append_addresses(
    lines: &mut Vec<String>,
    addresses: &[ContactAddress],
    version: VCardVersion,
    groups: &[String],
) -> Result<(), VCardWriteError> {
    for (index, address) in addresses.iter().enumerate() {
        let prefix = group_prefix(group_for(groups, index));
        let params = type_param(
            "address type",
            address.kind.as_deref(),
            address.is_primary,
            version,
        )?;
        // The 7 ADR components are: po-box; extended; street; locality;
        // region; postal; country. The model has no dedicated po-box/extended
        // slots, so the ordered `street` vector carries them: any leading
        // entries beyond the last are emitted as po-box/extended, the final
        // street lines join with `\n` into the street component. This keeps the
        // first two components from being zeroed (the old hardcoded `;;` bug).
        let street = address
            .street
            .iter()
            .map(|value| text_value("address street", value))
            .collect::<Result<Vec<_>, _>>()?
            .join("\\n");
        lines.push(format!(
            "{prefix}ADR{params}:;;{};{};{};{};{}",
            street,
            text_value(
                "address locality",
                address.locality.as_deref().unwrap_or_default()
            )?,
            text_value(
                "address region",
                address.region.as_deref().unwrap_or_default()
            )?,
            text_value(
                "address postal code",
                address.postal_code.as_deref().unwrap_or_default()
            )?,
            text_value(
                "address country",
                address.country.as_deref().unwrap_or_default()
            )?
        ));
    }
    Ok(())
}

fn append_notes(lines: &mut Vec<String>, notes: Option<&str>) -> Result<(), VCardWriteError> {
    if let Some(notes) = notes {
        lines.push(format!("NOTE:{}", text_value("notes", notes)?));
    }
    Ok(())
}

fn append_photo(lines: &mut Vec<String>, photo_url: Option<&str>) -> Result<(), VCardWriteError> {
    if let Some(photo_url) = photo_url {
        lines.push(format!(
            "PHOTO;VALUE=URI:{}",
            uri_value("photo url", photo_url)?
        ));
    }
    Ok(())
}

fn append_inline_photo(
    lines: &mut Vec<String>,
    photo: Option<&ContactPhoto>,
    version: VCardVersion,
) -> Result<(), VCardWriteError> {
    let Some(photo) = photo else {
        return Ok(());
    };
    let data = base64::engine::general_purpose::STANDARD.encode(&photo.data);
    let media = photo
        .media_type
        .as_deref()
        .filter(|value| !value.is_empty());
    match version {
        VCardVersion::V4 => {
            // 4.0 inline form is a data: URI value; the media type is a full
            // `image/<subtype>` so map a bare subtype hint (PNG/JPEG) into one.
            let media_type = media.map_or_else(
                || "application/octet-stream".to_string(),
                media_type_for_data_uri,
            );
            // The data: URI is a URI value with no escape form, so a control
            // character (a line break included) cannot be carried at all.
            if media_type.chars().any(|ch| ch.is_ascii_control()) {
                return Err(VCardWriteError {
                    field: "photo media type",
                    reason: CONTROL_CHARACTER,
                });
            }
            lines.push(format!("PHOTO:data:{media_type};base64,{data}"));
        }
        VCardVersion::V3 => {
            // 3.0 inline form uses ENCODING=b and a bare image-type TYPE hint.
            let mut line = String::from("PHOTO;ENCODING=b");
            if let Some(media_type) = media {
                line.push_str(";TYPE=");
                line.push_str(&param_value(
                    "photo media type",
                    &image_subtype(media_type).to_uppercase(),
                )?);
            }
            line.push(':');
            line.push_str(&data);
            lines.push(line);
        }
    }
    Ok(())
}

/// Build a `image/<subtype>` media type for a 4.0 data: URI from either a bare
/// subtype hint (`PNG`) or an already-qualified type (`image/png`).
fn media_type_for_data_uri(hint: &str) -> String {
    if hint.contains('/') {
        hint.to_string()
    } else {
        format!("image/{}", hint.to_ascii_lowercase())
    }
}

/// Reduce a media type to its image subtype hint (`image/png` -> `png`),
/// leaving a bare hint untouched.
fn image_subtype(media_type: &str) -> &str {
    media_type
        .rsplit_once('/')
        .map(|(_, subtype)| subtype)
        .unwrap_or(media_type)
}

fn should_replace_property(property: Option<&str>, patch: &ContactPatch) -> bool {
    match property {
        Some("FN") => patch.display_name.is_some(),
        Some("EMAIL") => patch.emails.is_some(),
        Some("TEL") => patch.phones.is_some(),
        Some("ORG" | "TITLE") => patch.organizations.is_some(),
        Some("ADR") => patch.addresses.is_some(),
        Some("NOTE") => patch.notes.is_some(),
        Some("PHOTO") => patch.photo_url.is_some() || patch.photo.is_some(),
        _ => false,
    }
}

/// Property name of a content line, group prefix stripped and uppercased.
/// `item1.X-ABLabel;...:value` -> `X-ABLABEL`.
fn property_name(line: &str) -> Option<String> {
    let (name_and_params, _) = line.split_once(':').unwrap_or((line, ""));
    let name = name_and_params
        .split_once(';')
        .map(|(name, _)| name)
        .unwrap_or(name_and_params);
    let name = name.rsplit_once('.').map(|(_, name)| name).unwrap_or(name);
    if name.is_empty() {
        None
    } else {
        Some(name.to_ascii_uppercase())
    }
}

/// Group prefix of a content line, without the trailing dot, when present.
/// `item1.EMAIL;TYPE=work:a@b` -> `Some("item1")`.
fn line_group_prefix(line: &str) -> Option<&str> {
    let (name_and_params, _) = line.split_once(':').unwrap_or((line, ""));
    let name = name_and_params
        .split_once(';')
        .map(|(name, _)| name)
        .unwrap_or(name_and_params);
    name.rsplit_once('.').map(|(group, _)| group)
}

/// Render a carried group prefix back onto an emitted line (`item1` ->
/// `item1.`), or empty when the line had no group.
fn group_prefix(group: Option<&str>) -> String {
    group.map_or_else(String::new, |group| format!("{group}."))
}

fn parse_vcard(data: &str) -> Result<ParsedVCard, VCardParseError> {
    parse_vcard_for(data, false)
}

fn parse_vcard_for(data: &str, directory: bool) -> Result<ParsedVCard, VCardParseError> {
    let mut parsed = ParsedVCard {
        display_name: None,
        emails: Vec::new(),
        phones: Vec::new(),
        organizations: Vec::new(),
        organization_components: Vec::new(),
        addresses: Vec::new(),
        notes: None,
        photo_url: None,
        photo: None,
    };
    let mut pending_org: Option<ContactOrganization> = None;
    let mut structured_name: Option<String> = None;
    let mut email_preferences = Vec::new();
    let mut phone_preferences = Vec::new();
    let mut address_preferences = Vec::new();

    // caldata's LineReader unfolds physical lines into logical content lines,
    // deleting exactly one leading WSP per RFC 6350 sec 3.2 (the old
    // hand-rolled unfolder ate the whole leading run). Param/value splitting is
    // done by `split_content_line`, which honors quoted parameter values that
    // legally contain `:`/`;`/`,` - the mis-split the old `split_once(':')`
    // path had - and keeps every TYPE value rather than the first.
    for line in LineReader::from_slice(data.as_bytes()) {
        let line = line.map_err(|error| VCardParseError(error.to_string()))?;
        let content = split_content_line(line.as_str()).map_err(VCardParseError)?;
        let name = content.name.as_str();
        let raw_value = content.value.as_str();
        let value = unescape_text(raw_value);
        match name {
            "FN" if !value.is_empty() => parsed.display_name = Some(value),
            "N" if !raw_value.is_empty() => {
                structured_name = Some(display_name_from_n(raw_value));
            }
            "EMAIL" if !value.is_empty() && (!directory || !value.trim().is_empty()) => {
                parsed.emails.push(ContactEmail {
                    value,
                    kind: type_from_params(&content.params),
                    is_primary: has_type_primary(&content.params),
                });
                email_preferences.push(preference_ordinal(&content.params));
            }
            "TEL" if !value.is_empty() => {
                parsed.phones.push(ContactPhone {
                    value,
                    kind: type_from_params(&content.params),
                    is_primary: has_type_primary(&content.params),
                });
                phone_preferences.push(preference_ordinal(&content.params));
            }
            "ORG" if !value.is_empty() => {
                parsed
                    .organization_components
                    .push(split_components(raw_value));
                if let Some(org) = pending_org.take() {
                    parsed.organizations.push(org);
                }
                pending_org = Some(ContactOrganization {
                    // Preserve the ORG `;`-structure: join non-empty components
                    // with `;` so the original unit/department layering can be
                    // reconstructed on re-serialize, rather than flattening to a
                    // space-joined blob.
                    name: split_components(raw_value)
                        .into_iter()
                        .filter(|part| !part.is_empty())
                        .collect::<Vec<_>>()
                        .join(";"),
                    title: None,
                });
            }
            "TITLE" if !value.is_empty() => {
                // A TITLE with no ORG above it is a job title the card really
                // carries, and the shared model has no title-only slot - so it
                // becomes an organization with an empty name rather than being
                // dropped. Dropped, it was invisible to the consumer AND
                // deleted by the next `organizations` patch, which strips
                // every ORG/TITLE line and re-emits only what the model holds.
                match pending_org.as_mut() {
                    Some(org) => {
                        // Directory projection promises the first title. Personal
                        // contacts retain their existing last-title selection in
                        // the shared single-title organization slot.
                        if !directory || org.title.is_none() {
                            org.title = Some(value);
                        }
                    }
                    None => {
                        pending_org = Some(ContactOrganization {
                            name: String::new(),
                            title: Some(value),
                        });
                    }
                }
            }
            "ADR" if !raw_value.is_empty() => {
                parsed.addresses.push(address_from_adr(
                    raw_value,
                    &content.params,
                    has_type_primary(&content.params),
                ));
                address_preferences.push(preference_ordinal(&content.params));
            }
            "NOTE" if !value.is_empty() => parsed.notes = Some(value),
            "PHOTO" => {
                if let Some(url) = photo_url(raw_value, &content.params) {
                    parsed.photo_url = Some(url);
                } else {
                    parsed.photo = inline_photo(raw_value, &content.params);
                }
            }
            _ => {}
        }
    }

    if let Some(org) = pending_org {
        parsed.organizations.push(org);
    }
    if parsed.display_name.is_none() {
        parsed.display_name = structured_name.filter(|name| !name.is_empty());
    }
    mark_lowest_preference_primary(&mut parsed.emails, &email_preferences, |email| {
        &mut email.is_primary
    });
    mark_lowest_preference_primary(&mut parsed.phones, &phone_preferences, |phone| {
        &mut phone.is_primary
    });
    mark_lowest_preference_primary(&mut parsed.addresses, &address_preferences, |address| {
        &mut address.is_primary
    });
    Ok(parsed)
}

/// One parsed content line: uppercased name (group prefix retained on the wire
/// but stripped here), every parameter with all its values, and the raw value.
struct ContentLine {
    name: String,
    params: Vec<(String, Vec<String>)>,
    value: String,
}

/// Split an unfolded logical line into name, parameters, and value.
///
/// Mirrors caldata's `ContentLineParser` quoted-parameter handling (which the
/// public API exposes only as a first-value-only `get_param`, insufficient for
/// multi-TYPE): a quoted parameter value may contain `:`/`;`/`,`, and a single
/// parameter may carry a comma-separated list of values. The property name's
/// group prefix (`item1.EMAIL`) is stripped so callers match on the bare name.
fn split_content_line(line: &str) -> Result<ContentLine, String> {
    let mut rest = line;
    let name_end = rest
        .find([';', ':'])
        .ok_or_else(|| "vCard content line is missing a name/value delimiter".to_string())?;
    let (raw_name, remainder) = rest.split_at(name_end);
    if raw_name.is_empty() {
        return Err("vCard content line is missing a property name".to_string());
    }
    let name = raw_name
        .rsplit_once('.')
        .map(|(_, name)| name)
        .unwrap_or(raw_name)
        .to_ascii_uppercase();
    rest = remainder;

    let mut params: Vec<(String, Vec<String>)> = Vec::new();
    while let Some(stripped) = rest.strip_prefix(';') {
        rest = stripped;
        // A vCard 3.0 (RFC 2426) shorthand allows a bare type value with no
        // `key=` (`EMAIL;WORK:...`). caldata's strict parser rejects this; to
        // not regress that real-world shape, a `=`-less param token is read as
        // a bare TYPE value rather than failing the whole line.
        let key_end = rest
            .find(['=', ';', ':'])
            .ok_or_else(|| "vCard parameter is missing a delimiter".to_string())?;
        let (key, has_value) = if rest.as_bytes()[key_end] == b'=' {
            let key = &rest[..key_end];
            rest = &rest[key_end + 1..];
            (key.to_ascii_uppercase(), true)
        } else {
            let key = &rest[..key_end];
            rest = &rest[key_end..];
            (key.to_ascii_uppercase(), false)
        };
        if key.is_empty() {
            return Err("vCard parameter is missing a key".to_string());
        }
        if !has_value {
            // Bare param: the token itself is the (TYPE) value.
            params.push(("TYPE".to_string(), vec![key]));
            continue;
        }
        let mut values = Vec::new();
        loop {
            if let Some(stripped) = rest.strip_prefix('"') {
                let (content, remainder) = stripped
                    .split_once('"')
                    .ok_or_else(|| "vCard parameter is missing a closing quote".to_string())?;
                values.push(unescape_param(content));
                rest = remainder;
            } else {
                let delim = rest
                    .find([';', ':', ','])
                    .ok_or_else(|| "vCard parameter value is missing a delimiter".to_string())?;
                let (content, remainder) = rest.split_at(delim);
                values.push(unescape_param(content));
                rest = remainder;
            }
            if let Some(stripped) = rest.strip_prefix(',') {
                rest = stripped;
            } else {
                break;
            }
        }
        params.push((key, values));
    }

    let value = rest
        .strip_prefix(':')
        .ok_or_else(|| "vCard content line is missing its value".to_string())?;
    Ok(ContentLine {
        name,
        params,
        value: value.to_string(),
    })
}

fn address_from_adr(
    value: &str,
    params: &[(String, Vec<String>)],
    is_primary: bool,
) -> ContactAddress {
    let parts = split_components(value);
    // Components 0/1 are po-box and extended-address. The shared model has no
    // dedicated slots, so rather than silently merging them into the street (or
    // dropping them as the old code did), carry any non-empty po-box/extended
    // as leading entries of the ordered `street` vector. The street component
    // itself (index 2) may itself be `\n`-multilined.
    let mut street = Vec::new();
    for index in 0..=2 {
        if let Some(part) = parts.get(index).filter(|part| !part.is_empty()) {
            street.extend(part.split('\n').filter(|p| !p.is_empty()).map(String::from));
        }
    }
    ContactAddress {
        kind: type_from_params(params),
        formatted: None,
        street,
        locality: non_empty_part(parts.get(3)),
        region: non_empty_part(parts.get(4)),
        postal_code: non_empty_part(parts.get(5)),
        country: non_empty_part(parts.get(6)),
        is_primary,
    }
}

fn non_empty_part(value: Option<&String>) -> Option<String> {
    value.filter(|value| !value.is_empty()).cloned()
}

fn display_name_from_n(value: &str) -> String {
    let parts = split_components(value);
    let family = parts.first().map(String::as_str).unwrap_or_default();
    let given = parts.get(1).map(String::as_str).unwrap_or_default();
    let additional = parts.get(2).map(String::as_str).unwrap_or_default();
    let prefix = parts.get(3).map(String::as_str).unwrap_or_default();
    let suffix = parts.get(4).map(String::as_str).unwrap_or_default();
    [prefix, given, additional, family, suffix]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn split_components(value: &str) -> Vec<String> {
    let mut components = Vec::new();
    let mut current = String::new();
    let mut escaped = false;
    for ch in value.chars() {
        if escaped {
            current.push('\\');
            current.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == ';' {
            components.push(unescape_text(&current));
            current.clear();
        } else {
            current.push(ch);
        }
    }
    if escaped {
        current.push('\\');
    }
    components.push(unescape_text(&current));
    components
}

/// One logical content line as a run of physical lines: the head plus any
/// folded continuation lines (those starting with SPACE or TAB). Carries its
/// own physical shape so it can be re-emitted verbatim on the patch path.
struct LineGroup<'a> {
    physical: Vec<&'a str>,
}

impl<'a> LineGroup<'a> {
    fn logical_head(&self) -> &'a str {
        self.physical.first().copied().unwrap_or_default()
    }

    fn push_verbatim(&self, out: &mut String) {
        for line in &self.physical {
            out.push_str(line);
            out.push_str("\r\n");
        }
    }
}

/// Group `source` into logical lines without unfolding their content, so
/// preserved lines can be re-emitted byte for byte. CR is trimmed from each
/// physical line and re-added on emit; folding markers (leading WSP) stay
/// attached to the continuation lines they belong to.
fn logical_line_groups(source: &str) -> Vec<LineGroup<'_>> {
    let mut groups: Vec<LineGroup<'_>> = Vec::new();
    for raw in source.lines() {
        let line = raw.strip_suffix('\r').unwrap_or(raw);
        if (line.starts_with(' ') || line.starts_with('\t'))
            && let Some(last) = groups.last_mut()
        {
            last.physical.push(line);
        } else {
            groups.push(LineGroup {
                physical: vec![line],
            });
        }
    }
    groups
}

fn type_param(
    field: &'static str,
    kind: Option<&str>,
    primary: bool,
    version: VCardVersion,
) -> Result<String, VCardWriteError> {
    let mut params = Vec::new();
    if let Some(kind) = kind.filter(|kind| !kind.is_empty()) {
        // The model carries TYPE values as a comma-joined string (the parse
        // path joins multi-TYPE that way); split them back into per-value TYPE
        // parameters so every type round-trips.
        for value in kind.split(',').filter(|value| !value.is_empty()) {
            params.push(format!("TYPE={}", param_value(field, value)?));
        }
    }
    if primary {
        // 4.0 marks preference with PREF=1; 3.0 uses a bare TYPE=PREF. Emitting
        // the 4.0 form into a 3.0 card (the old unconditional `PREF=1`) is
        // version-incorrect, so pick the form the card's version expects.
        params.push(match version {
            VCardVersion::V4 => "PREF=1".to_string(),
            VCardVersion::V3 => "TYPE=PREF".to_string(),
        });
    }
    Ok(if params.is_empty() {
        String::new()
    } else {
        format!(";{}", params.join(";"))
    })
}

/// Collect every TYPE value across repeated `TYPE=` parameters and
/// comma-separated value lists, lowercased, joined with `,`. `PREF` is treated
/// as a preference marker (3.0 style) rather than a type and is dropped here.
fn type_from_params(params: &[(String, Vec<String>)]) -> Option<String> {
    let mut types = Vec::new();
    for (key, values) in params {
        if key == "TYPE" {
            for value in values {
                let lower = value.to_ascii_lowercase();
                if lower == "pref" {
                    continue;
                }
                if !lower.is_empty() && !types.contains(&lower) {
                    types.push(lower);
                }
            }
        }
    }
    if types.is_empty() {
        None
    } else {
        Some(types.join(","))
    }
}

fn has_type_primary(params: &[(String, Vec<String>)]) -> bool {
    params.iter().any(|(key, values)| {
        key == "TYPE"
            && values
                .iter()
                .any(|value| value.eq_ignore_ascii_case("PREF"))
    })
}

fn preference_ordinal(params: &[(String, Vec<String>)]) -> Option<u8> {
    params
        .iter()
        .filter(|(key, _)| key == "PREF")
        .flat_map(|(_, values)| values)
        .filter_map(|value| value.parse::<u8>().ok())
        .filter(|value| (1..=100).contains(value))
        .min()
}

fn mark_lowest_preference_primary<T>(
    items: &mut [T],
    preferences: &[Option<u8>],
    primary: impl Fn(&mut T) -> &mut bool,
) {
    let Some(lowest) = preferences.iter().flatten().min().copied() else {
        return;
    };
    for (item, preference) in items.iter_mut().zip(preferences) {
        if *preference == Some(lowest) {
            *primary(item) = true;
        }
    }
}

/// RFC 6868 parameter-value decoding, the inverse of [`escape_param`]:
/// `^n` is a newline, `^'` a `"`, `^^` a caret. A caret before anything
/// else is literal, so a value from a producer that predates RFC 6868
/// survives unchanged.
fn unescape_param(value: &str) -> String {
    if !value.contains('^') {
        return value.to_string();
    }
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '^' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('\'') => out.push('"'),
            Some('^') => out.push('^'),
            Some(other) => {
                out.push('^');
                out.push(other);
            }
            None => out.push('^'),
        }
    }
    out
}

fn escape_param(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '^' => escaped.push_str("^^"),
            '"' => escaped.push_str("^'"),
            '\r' => {
                if chars.peek() == Some(&'\n') {
                    chars.next();
                }
                escaped.push_str("^n");
            }
            '\n' => escaped.push_str("^n"),
            _ => escaped.push(ch),
        }
    }
    if escaped.contains([';', ',', ':']) {
        format!("\"{escaped}\"")
    } else {
        escaped
    }
}

fn fold_vcard_lines(lines: Vec<String>) -> String {
    let mut folded = String::new();
    for line in lines {
        let mut current = String::new();
        let mut prefix = 0;
        for ch in line.chars() {
            if prefix + current.len() + ch.len_utf8() > 75 {
                folded.push_str(&current);
                folded.push_str("\r\n ");
                current.clear();
                // The folding space occupies one octet of the 75-octet budget
                // on every continuation line (matches fold_ical_lines).
                prefix = 1;
            }
            current.push(ch);
        }
        folded.push_str(&current);
        folded.push_str("\r\n");
    }
    folded
}

fn escape_text(value: &str) -> String {
    value
        .replace("\r\n", "\n")
        .replace('\r', "\n")
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace(';', "\\;")
        .replace(',', "\\,")
}

fn unescape_text(value: &str) -> String {
    let mut output = String::new();
    let mut escaped = false;
    for ch in value.chars() {
        if escaped {
            match ch {
                'n' | 'N' => output.push('\n'),
                '\\' | ';' | ',' => output.push(ch),
                _ => {
                    output.push('\\');
                    output.push(ch);
                }
            }
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else {
            output.push(ch);
        }
    }
    if escaped {
        output.push('\\');
    }
    output
}

/// Extract a photo URL from a PHOTO value. A 4.0 `data:` URI is inline binary,
/// not a remote URL, so it is excluded here and handled by `inline_photo`.
fn photo_url(value: &str, params: &[(String, Vec<String>)]) -> Option<String> {
    let is_uri_value = params.iter().any(|(key, values)| {
        key == "VALUE" && values.iter().any(|v| v.eq_ignore_ascii_case("uri"))
    });
    // A URI value is not TEXT and carries no escapes, but cards written with
    // TEXT escaping (this crate's own writer once did it) are still in the
    // wild. Unescaping is lossless for a valid URI: a literal backslash is not
    // a URI character and must appear percent-encoded.
    let unescaped = unescape_text(value);
    if unescaped.starts_with("data:") {
        return None;
    }
    if unescaped.starts_with("http://") || unescaped.starts_with("https://") {
        return Some(unescaped);
    }
    if is_uri_value && !unescaped.is_empty() {
        return Some(unescaped);
    }
    None
}

fn inline_photo(value: &str, params: &[(String, Vec<String>)]) -> Option<ContactPhoto> {
    // 4.0 inline form: PHOTO:data:image/png;base64,<...>
    if let Some(rest) = value.strip_prefix("data:") {
        let (meta, payload) = rest.split_once(',')?;
        let compact = payload.split_whitespace().collect::<String>();
        let data = base64::engine::general_purpose::STANDARD
            .decode(compact.as_bytes())
            .ok()?;
        let media_type = meta
            .split(';')
            .next()
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        return Some(ContactPhoto { data, media_type });
    }
    // 3.0 inline form: PHOTO;ENCODING=b;TYPE=PNG:<base64>
    let has_inline_encoding = params.iter().any(|(key, values)| {
        let first = values.first().map(String::as_str).unwrap_or_default();
        (key == "ENCODING"
            && (first.eq_ignore_ascii_case("b") || first.eq_ignore_ascii_case("BASE64")))
            || (key == "VALUE" && first.eq_ignore_ascii_case("BINARY"))
    });
    if !has_inline_encoding {
        return None;
    }
    let compact = value.split_whitespace().collect::<String>();
    let data = base64::engine::general_purpose::STANDARD
        .decode(compact.as_bytes())
        .ok()?;
    Some(ContactPhoto {
        data,
        media_type: photo_media_type(params),
    })
}

fn photo_media_type(params: &[(String, Vec<String>)]) -> Option<String> {
    params.iter().find_map(|(key, values)| {
        (key == "MEDIATYPE" || key == "TYPE")
            .then(|| values.first().cloned())
            .flatten()
            .filter(|value| !value.is_empty())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test shims: the production writers are fallible (a control character
    /// is refused); these shadow them for tests that feed writable input.
    fn vcard_from_create(contact: &ContactCreate, uid: &str) -> String {
        super::vcard_from_create(contact, uid).expect("writable contact")
    }

    fn vcard_from_patch(current: &ContactCard, source: &str, patch: &ContactPatch) -> String {
        super::vcard_from_patch(current, source, patch).expect("writable patch")
    }

    /// Test shim: the production projector is fallible (malformed bodies
    /// degrade to a per-resource skip); these tests feed well-formed input and
    /// expect a successful projection.
    fn parse_contact(
        uri: String,
        address_book_id: Option<AddressBookId>,
        etag: Option<String>,
        data: &str,
    ) -> ContactCard {
        contact_from_vcard(uri, address_book_id, etag, data).expect("valid vCard projects")
    }

    #[test]
    fn contact_from_vcard_reads_common_fields() {
        let contact = parse_contact(
            "/ab/1.vcf".to_string(),
            Some(AddressBookId("/ab/".to_string())),
            Some("etag".to_string()),
            "BEGIN:VCARD\r\nFN:Ada Lovelace\r\nEMAIL;TYPE=work;PREF=1:ADA@EXAMPLE.COM\r\nTEL;TYPE=mobile:+123\r\nORG:Analytical Engines\r\nTITLE:Programmer\r\nNOTE:hello\\nworld\r\nPHOTO;VALUE=URI:https://example.test/a.jpg\r\nEND:VCARD\r\n",
        );

        assert_eq!(contact.display_name.as_deref(), Some("Ada Lovelace"));
        assert_eq!(contact.emails[0].kind.as_deref(), Some("work"));
        assert!(contact.emails[0].is_primary);
        assert_eq!(contact.phones[0].kind.as_deref(), Some("mobile"));
        assert_eq!(
            contact.organizations[0].title.as_deref(),
            Some("Programmer")
        );
        assert_eq!(contact.notes.as_deref(), Some("hello\nworld"));
        assert_eq!(
            contact.photo_url.as_deref(),
            Some("https://example.test/a.jpg")
        );
        assert!(contact.photo.is_none());
    }

    /// A TITLE with no ORG above it used to be dropped on read, which also
    /// meant an `organizations` patch - which strips every ORG/TITLE line and
    /// re-emits only what the model holds - deleted a title the model had
    /// never seen. It round-trips now, and emits no invented empty ORG line.
    #[test]
    fn a_standalone_title_survives_read_and_write() {
        let contact = parse_contact(
            "/ab/1.vcf".to_string(),
            Some(AddressBookId("/ab/".to_string())),
            None,
            "BEGIN:VCARD\r\nVERSION:4.0\r\nFN:Ada\r\nTITLE:Programmer\r\nEND:VCARD\r\n",
        );

        assert_eq!(
            contact.organizations[0].title.as_deref(),
            Some("Programmer"),
            "a title with no ORG is still a title the card carries"
        );
        assert_eq!(contact.organizations[0].name, "");

        // The write half: an organizations patch strips every ORG/TITLE line
        // and re-emits the model, so a title the model never saw was deleted.
        let source = "BEGIN:VCARD\r\nVERSION:4.0\r\nFN:Ada\r\nTITLE:Programmer\r\nEND:VCARD\r\n";
        let written = vcard_from_patch(
            &contact,
            source,
            &ContactPatch {
                organizations: Some(contact.organizations.clone()),
                ..ContactPatch::default()
            },
        );
        assert!(written.contains("TITLE:Programmer"), "{written}");
        assert!(
            !written.lines().any(|line| line.trim_end() == "ORG:"),
            "a title-only organization must not invent an empty ORG line: {written}"
        );
    }

    #[test]
    fn contact_from_vcard_reads_inline_photo_data() {
        let contact = parse_contact(
            "/ab/1.vcf".to_string(),
            Some(AddressBookId("/ab/".to_string())),
            None,
            "BEGIN:VCARD\r\nFN:Ada Lovelace\r\nPHOTO;ENCODING=b;TYPE=JPEG:AQIDBA==\r\nEND:VCARD\r\n",
        );

        let photo = contact.photo.expect("inline photo");
        assert_eq!(photo.data, vec![1, 2, 3, 4]);
        assert_eq!(photo.media_type.as_deref(), Some("JPEG"));
        assert!(contact.photo_url.is_none());
    }

    #[test]
    fn contact_from_vcard_reads_v4_data_uri_photo() {
        // The 4.0 inline form is a data: URI value; the old http(s)-only
        // is_url check dropped it. base64 of [1,2,3,4] is AQIDBA==.
        let contact = parse_contact(
            "/ab/1.vcf".to_string(),
            None,
            None,
            "BEGIN:VCARD\r\nVERSION:4.0\r\nFN:Ada\r\nPHOTO:data:image/png;base64,AQIDBA==\r\nEND:VCARD\r\n",
        );

        let photo = contact.photo.expect("inline data: photo");
        assert_eq!(photo.data, vec![1, 2, 3, 4]);
        assert_eq!(photo.media_type.as_deref(), Some("image/png"));
        assert!(contact.photo_url.is_none());
    }

    #[test]
    fn vcard_from_create_escapes_text_fields() {
        let contact = ContactCreate {
            display_name: Some("Ada, Countess".to_string()),
            notes: Some("line 1\r\nline 2\rline 3".to_string()),
            ..ContactCreate::default()
        };
        let data = vcard_from_create(&contact, "id-1");
        assert!(data.contains("FN:Ada\\, Countess"));
        assert!(data.contains("NOTE:line 1\\nline 2\\nline 3"));
    }

    #[test]
    fn vcard_from_create_emits_structured_name() {
        // N is mandatory in 3.0 and expected even on a 4.0 card; the create
        // path must emit it (the old path wrote only FN).
        let contact = ContactCreate {
            display_name: Some("Ada Lovelace".to_string()),
            ..ContactCreate::default()
        };
        let data = vcard_from_create(&contact, "id-1");
        assert!(data.contains("N:Ada Lovelace;;;;"));
    }

    #[test]
    fn vcard_from_create_writes_postal_addresses() {
        let contact = ContactCreate {
            addresses: vec![ContactAddress {
                kind: Some("home".to_string()),
                formatted: None,
                street: vec!["1 Example St".to_string(), "Unit 2".to_string()],
                locality: Some("London".to_string()),
                region: Some("England".to_string()),
                postal_code: Some("N1".to_string()),
                country: Some("UK".to_string()),
                is_primary: true,
            }],
            ..ContactCreate::default()
        };
        let data = vcard_from_create(&contact, "id-1");

        assert!(data.contains("ADR;TYPE=home;PREF=1:;;1 Example St\\nUnit 2;London;England;N1;UK"));
    }

    #[test]
    fn vcard_from_create_always_emits_fn() {
        let data = vcard_from_create(&ContactCreate::default(), "id-1");
        assert!(data.contains("\r\nFN:\r\n"));
    }

    #[test]
    fn contact_from_vcard_uses_structured_name_when_fn_absent() {
        let contact = parse_contact(
            "/ab/1.vcf".to_string(),
            Some(AddressBookId("/ab/".to_string())),
            None,
            "BEGIN:VCARD\r\nN:Lovelace;Ada;Byron;Countess;\r\nEMAIL;WORK;PREF:ada@example.test\r\nADR;TYPE=home;PREF=1:;;1 Example St\\nUnit 2;London;England;N1;UK\r\nORG:Acme\\;R&D;Lab\r\nEND:VCARD\r\n",
        );

        assert_eq!(
            contact.display_name.as_deref(),
            Some("Countess Ada Byron Lovelace")
        );
        // EMAIL;WORK;PREF uses vCard 3.0 bare-param shorthand: WORK is a bare
        // TYPE value, PREF marks preference.
        assert_eq!(contact.emails[0].kind.as_deref(), Some("work"));
        assert!(contact.emails[0].is_primary);
        assert_eq!(contact.addresses[0].kind.as_deref(), Some("home"));
        assert_eq!(contact.addresses[0].street, vec!["1 Example St", "Unit 2"]);
        assert_eq!(contact.addresses[0].locality.as_deref(), Some("London"));
        assert!(contact.addresses[0].is_primary);
        assert_eq!(contact.organizations[0].name, "Acme;R&D;Lab");
    }

    #[test]
    fn org_structure_round_trips() {
        // ORG `;`-structure is preserved (not flattened to a space-joined
        // blob), and re-serializing reconstructs the component layout.
        let contact = parse_contact(
            "/ab/1.vcf".to_string(),
            None,
            None,
            "BEGIN:VCARD\r\nFN:Ada\r\nORG:Acme;R&D;Lab\r\nEND:VCARD\r\n",
        );
        assert_eq!(contact.organizations[0].name, "Acme;R&D;Lab");

        let mut lines = Vec::new();
        append_organizations(&mut lines, &contact.organizations).expect("writable");
        assert_eq!(lines[0], "ORG:Acme;R&D;Lab");
    }

    #[test]
    fn unknown_escapes_are_preserved() {
        let contact = parse_contact(
            "/ab/1.vcf".to_string(),
            None,
            None,
            "BEGIN:VCARD\r\nFN:Ada\\x\r\nEND:VCARD\r\n",
        );

        assert_eq!(contact.display_name.as_deref(), Some("Ada\\x"));
    }

    #[test]
    fn quoted_parameter_with_colon_and_semicolon_parses() {
        // A quoted TYPE containing `:`/`;`/`,` must not be mis-split on the
        // first `:` (the old parser bug).
        let contact = parse_contact(
            "/ab/1.vcf".to_string(),
            None,
            None,
            "BEGIN:VCARD\r\nFN:Ada\r\nEMAIL;TYPE=\"work:main;x,y\":ada@example.test\r\nEND:VCARD\r\n",
        );

        assert_eq!(contact.emails[0].value, "ada@example.test");
        // The quoted value is one TYPE token; comma-splitting does not apply
        // inside a quoted string, so it survives intact (lowercased).
        assert_eq!(contact.emails[0].kind.as_deref(), Some("work:main;x,y"));
    }

    #[test]
    fn multi_type_round_trips() {
        // Both the 4.0 repeated-parameter form and the 3.0 comma-list form
        // must keep every TYPE value, and re-serialize them all.
        let repeated = parse_contact(
            "/ab/1.vcf".to_string(),
            None,
            None,
            "BEGIN:VCARD\r\nVERSION:4.0\r\nFN:Ada\r\nTEL;TYPE=cell;TYPE=voice:+1\r\nEND:VCARD\r\n",
        );
        assert_eq!(repeated.phones[0].kind.as_deref(), Some("cell,voice"));

        let comma = parse_contact(
            "/ab/1.vcf".to_string(),
            None,
            None,
            "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Ada\r\nTEL;TYPE=HOME,WORK:+1\r\nEND:VCARD\r\n",
        );
        assert_eq!(comma.phones[0].kind.as_deref(), Some("home,work"));

        let mut lines = Vec::new();
        append_phones(&mut lines, &repeated.phones, VCardVersion::V4, &[]).expect("writable");
        assert_eq!(lines[0], "TEL;TYPE=cell;TYPE=voice:+1");
    }

    #[test]
    fn bare_params_collect_multiple_types() {
        // vCard 3.0 shorthand allows several bare TYPE tokens on one line;
        // each becomes a TYPE value and all of them survive.
        let contact = parse_contact(
            "/ab/1.vcf".to_string(),
            None,
            None,
            "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Ada\r\nEMAIL;INTERNET;HOME:ada@example.test\r\nEND:VCARD\r\n",
        );

        assert_eq!(contact.emails[0].kind.as_deref(), Some("internet,home"));
        assert!(!contact.emails[0].is_primary);
    }

    #[test]
    fn only_pref_one_maps_to_primary() {
        let contact = parse_contact(
            "/ab/1.vcf".to_string(),
            None,
            None,
            "BEGIN:VCARD\r\nVERSION:4.0\r\nFN:Ada\r\nEMAIL;PREF=2:second@example.test\r\nEMAIL;PREF=1:first@example.test\r\nEND:VCARD\r\n",
        );

        assert!(!contact.emails[0].is_primary);
        assert!(contact.emails[1].is_primary);
    }

    #[test]
    fn lowest_pref_ordinal_is_primary_when_pref_one_is_absent() {
        let contact = parse_contact(
            "/ab/1.vcf".to_string(),
            None,
            None,
            "BEGIN:VCARD\r\nVERSION:4.0\r\nFN:Ada\r\nEMAIL;PREF=3:third@example.test\r\nEMAIL;PREF=2:second@example.test\r\nTEL;PREF=3:+3\r\nTEL;PREF=2:+2\r\nADR;PREF=3:;;Third;;;;\r\nADR;PREF=2:;;Second;;;;\r\nEND:VCARD\r\n",
        );

        assert!(!contact.emails[0].is_primary);
        assert!(contact.emails[1].is_primary);
        assert!(!contact.phones[0].is_primary);
        assert!(contact.phones[1].is_primary);
        assert!(!contact.addresses[0].is_primary);
        assert!(contact.addresses[1].is_primary);
    }

    #[test]
    fn parameter_values_use_rfc6868_caret_encoding() {
        assert_eq!(escape_param("Ada \"Ace\" ^ Team"), "Ada ^'Ace^' ^^ Team");
        assert_eq!(escape_param("one\r\ntwo"), "one^ntwo");
    }

    #[test]
    fn parameter_values_round_trip_through_rfc6868_decoding() {
        // Writing carets a reader keeps verbatim would surface `^'` in a
        // TYPE label; the decode is what makes the encoding lossless.
        for value in ["Ada \"Ace\" ^ Team", "one\ntwo", "plain"] {
            assert_eq!(unescape_param(&escape_param(value)), value);
        }
        // A lone caret from a pre-RFC-6868 producer is literal.
        assert_eq!(unescape_param("50^ off"), "50^ off");

        let contact = parse_contact(
            "/ab/1.vcf".to_string(),
            None,
            None,
            "BEGIN:VCARD\r\nVERSION:4.0\r\nFN:Ada\r\nEMAIL;TYPE=^'work^':ada@example.test\r\nEND:VCARD\r\n",
        );
        assert_eq!(contact.emails[0].kind.as_deref(), Some("\"work\""));
    }

    #[test]
    fn detect_version_reads_version_line_and_defaults_to_v4() {
        assert_eq!(
            detect_version("BEGIN:VCARD\r\nVERSION:3.0\r\nFN:A\r\nEND:VCARD\r\n"),
            VCardVersion::V3
        );
        assert_eq!(
            detect_version("BEGIN:VCARD\r\nVERSION:4.0\r\nFN:A\r\nEND:VCARD\r\n"),
            VCardVersion::V4
        );
        // No VERSION line: default to 4.0, matching the create path.
        assert_eq!(
            detect_version("BEGIN:VCARD\r\nFN:A\r\nEND:VCARD\r\n"),
            VCardVersion::V4
        );
    }

    #[test]
    fn photo_value_uri_accepts_non_http_uri() {
        // A VALUE=URI PHOTO with a non-http scheme is still a URL
        // reference, not inline data.
        let contact = parse_contact(
            "/ab/1.vcf".to_string(),
            None,
            None,
            "BEGIN:VCARD\r\nFN:Ada\r\nPHOTO;VALUE=URI:content://photos/1\r\nEND:VCARD\r\n",
        );

        assert_eq!(contact.photo_url.as_deref(), Some("content://photos/1"));
        assert!(contact.photo.is_none());
    }

    /// A PHOTO URI is a URI value, not TEXT: commas and semicolons go on the
    /// wire bare, and a line break, which a URI cannot escape, is refused
    /// rather than starting a content line. Reverting `append_photo` to
    /// `text_value` writes `\,` and `\;` and accepts the line break.
    #[test]
    fn photo_uri_is_written_unescaped_and_refuses_line_breaks() {
        let url = "https://x.test/p;v=1,2?a=b,c";
        let mut lines = Vec::new();
        append_photo(&mut lines, Some(url)).expect("writable");
        assert_eq!(lines, vec![format!("PHOTO;VALUE=URI:{url}")]);

        let card = format!("BEGIN:VCARD\r\nFN:Ada\r\n{}\r\nEND:VCARD\r\n", lines[0]);
        let contact = parse_contact("/ab/1.vcf".to_string(), None, None, &card);
        assert_eq!(contact.photo_url.as_deref(), Some(url));

        let mut lines = Vec::new();
        let error = append_photo(&mut lines, Some("https://x.test/a\r\nX-EVIL:1"))
            .expect_err("a line break in a URI is refused");
        assert_eq!(error.field, "photo url");
        assert!(lines.is_empty());
    }

    #[test]
    fn unescape_text_handles_adjacent_backslashes_in_a_single_pass() {
        // Raw `\\n` is an escaped backslash followed by a literal n, not a
        // newline; an ordering-dependent replace-chain corrupts it.
        assert_eq!(unescape_text("a\\\\nb"), "a\\nb");
        assert_eq!(unescape_text("line\\nbreak"), "line\nbreak");
        assert_eq!(unescape_text("semi\\;comma\\,"), "semi;comma,");
        // Unknown escapes and a trailing backslash pass through verbatim.
        assert_eq!(unescape_text("odd\\x"), "odd\\x");
        assert_eq!(unescape_text("tail\\"), "tail\\");
    }

    #[test]
    fn escape_then_unescape_round_trips_text() {
        let original = "a,b;c\\d\nnewline";
        assert_eq!(unescape_text(&escape_text(original)), original);
    }

    #[test]
    fn adr_po_box_and_extended_round_trip() {
        // A real PO-box / extended-address must not be dropped on parse; the
        // model carries them as leading street entries.
        let contact = parse_contact(
            "/ab/1.vcf".to_string(),
            None,
            None,
            "BEGIN:VCARD\r\nFN:Ada\r\nADR;TYPE=work:PO Box 1;Suite 5;1 Main St;Town;Region;12345;US\r\nEND:VCARD\r\n",
        );
        assert_eq!(
            contact.addresses[0].street,
            vec!["PO Box 1", "Suite 5", "1 Main St"]
        );
        assert_eq!(contact.addresses[0].locality.as_deref(), Some("Town"));
        assert_eq!(contact.addresses[0].postal_code.as_deref(), Some("12345"));
        assert_eq!(contact.addresses[0].country.as_deref(), Some("US"));
    }

    #[test]
    fn vcard_from_create_v4_emits_data_uri_photo() {
        // A 4.0 card must use the data: URI inline PHOTO form, not the 3.0
        // ENCODING=b form.
        let mut lines = Vec::new();
        append_inline_photo(
            &mut lines,
            Some(&ContactPhoto {
                data: vec![1, 2, 3, 4],
                media_type: Some("PNG".to_string()),
            }),
            VCardVersion::V4,
        )
        .expect("writable");
        assert_eq!(lines[0], "PHOTO:data:image/png;base64,AQIDBA==");
    }

    fn refused_create(contact: &ContactCreate) -> VCardWriteError {
        super::vcard_from_create(contact, "id-1").expect_err("the writer must refuse this value")
    }

    /// TEXT and parameter values holding a control character other than tab,
    /// CR and LF are refused and named, on create and on patch, never
    /// stripped. CR and LF stay escaped (TEXT) or caret-encoded (parameters),
    /// and the inline PHOTO data URI, which has no escape form, refuses any
    /// control character. Reverting `text_value`, `param_value` or the data URI
    /// guard to a pass-through writes the value.
    #[test]
    fn writers_refuse_control_characters_and_still_encode_line_breaks() {
        let named = |edit: &dyn Fn(&mut ContactCreate)| {
            let mut contact = ContactCreate::default();
            edit(&mut contact);
            refused_create(&contact).field
        };
        assert_eq!(
            named(&|c| c.display_name = Some("Ada\u{0}Lovelace".to_string())),
            "display name"
        );
        assert_eq!(
            named(&|c| c.emails = vec![ContactEmail {
                value: "a@example.test\u{7}".to_string(),
                kind: None,
                is_primary: false,
            }]),
            "email"
        );
        assert_eq!(
            named(&|c| c.emails = vec![ContactEmail {
                value: "a@example.test".to_string(),
                kind: Some("ho\u{1}me".to_string()),
                is_primary: false,
            }]),
            "email type"
        );
        assert_eq!(
            named(&|c| c.phones = vec![ContactPhone {
                value: "+1\u{8}".to_string(),
                kind: None,
                is_primary: false,
            }]),
            "phone"
        );
        assert_eq!(
            named(&|c| c.organizations = vec![ContactOrganization {
                name: "Ac\u{b}me".to_string(),
                title: None,
            }]),
            "organization name"
        );
        assert_eq!(
            named(&|c| c.organizations = vec![ContactOrganization {
                name: "Acme".to_string(),
                title: Some("Boss\u{7f}".to_string()),
            }]),
            "organization title"
        );
        assert_eq!(
            named(&|c| c.addresses = vec![ContactAddress {
                kind: None,
                formatted: None,
                street: vec!["1 Main\u{c}St".to_string()],
                locality: None,
                region: None,
                postal_code: None,
                country: None,
                is_primary: false
            }]),
            "address street"
        );
        assert_eq!(
            named(&|c| c.addresses = vec![ContactAddress {
                kind: None,
                formatted: None,
                street: Vec::new(),
                locality: None,
                region: None,
                postal_code: None,
                country: Some("No\u{0}rway".to_string()),
                is_primary: false
            }]),
            "address country"
        );
        assert_eq!(
            named(&|c| c.notes = Some("note\u{1b}".to_string())),
            "notes"
        );
        assert_eq!(
            named(&|c| c.photo_url = Some("https://x.test/\u{0}".to_string())),
            "photo url"
        );
        assert_eq!(
            super::vcard_from_create(&ContactCreate::default(), "id\u{0}1")
                .expect_err("uid")
                .field,
            "uid"
        );

        // The 4.0 data URI carries the media type unescaped: even a line
        // break is refused rather than injecting a content line.
        let mut lines = Vec::new();
        let error = append_inline_photo(
            &mut lines,
            Some(&ContactPhoto {
                data: vec![1],
                media_type: Some("image/png\r\nX-EVIL:1".to_string()),
            }),
            VCardVersion::V4,
        )
        .expect_err("a line break in the media type is refused");
        assert_eq!(error.field, "photo media type");
        assert!(lines.is_empty());

        // Patch path: same emitters, same refusal.
        let source = "BEGIN:VCARD\r\nVERSION:4.0\r\nFN:Ada\r\nEND:VCARD\r\n";
        let current = parse_contact("/ab/1.vcf".to_string(), None, None, source);
        let error = super::vcard_from_patch(
            &current,
            source,
            &ContactPatch {
                notes: Some(Some("a\u{0}b".to_string())),
                ..ContactPatch::default()
            },
        )
        .expect_err("a control character in a patched note is refused");
        assert_eq!(error.field, "notes");

        // Line breaks and tab remain writable and stay ONE content line.
        let contact = ContactCreate {
            notes: Some("one\r\ntwo\tthree\rfour\nfive".to_string()),
            display_name: Some("Ada".to_string()),
            emails: vec![ContactEmail {
                value: "a@example.test".to_string(),
                kind: Some("ho\r\nme".to_string()),
                is_primary: false,
            }],
            ..ContactCreate::default()
        };
        let data = super::vcard_from_create(&contact, "id-1").expect("line breaks are writable");
        assert!(
            data.contains("NOTE:one\\ntwo\tthree\\nfour\\nfive\r\n"),
            "{data}"
        );
        assert!(
            data.contains("EMAIL;TYPE=ho^nme:a@example.test\r\n"),
            "{data}"
        );
    }

    #[test]
    fn vcard_patch_into_v3_card_emits_v3_photo() {
        // Patching a photo into a card declared VERSION:3.0 must use the 3.0
        // ENCODING=b form, not 4.0 data:.
        let current = parse_contact(
            "/ab/1.vcf".to_string(),
            None,
            None,
            "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Ada\r\nEND:VCARD\r\n",
        );
        let updated = vcard_from_patch(
            &current,
            "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Ada\r\nEND:VCARD\r\n",
            &ContactPatch {
                photo: Some(Some(ContactPhoto {
                    data: vec![1, 2, 3, 4],
                    media_type: Some("PNG".to_string()),
                })),
                ..ContactPatch::default()
            },
        );
        assert!(updated.contains("PHOTO;ENCODING=b;TYPE=PNG:AQIDBA=="));
        assert!(!updated.contains("data:image"));
        assert!(updated.contains("VERSION:3.0"));
    }

    #[test]
    fn vcard_from_patch_preserves_unmodeled_properties() {
        let source = "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Ada\r\nN:Lovelace;Ada;;;\r\nBDAY:18151210\r\nX-CUSTOM:value\r\nEMAIL;TYPE=work:old@example.test\r\nEND:VCARD\r\n";
        let current = parse_contact(
            "/ab/1.vcf".to_string(),
            Some(AddressBookId("/ab/".to_string())),
            Some("etag".to_string()),
            source,
        );
        let patch = ContactPatch {
            emails: Some(vec![ContactEmail {
                value: "new@example.test".to_string(),
                kind: Some("home".to_string()),
                is_primary: true,
            }]),
            ..ContactPatch::default()
        };

        let data = vcard_from_patch(&current, source, &patch);

        assert!(data.contains("N:Lovelace;Ada;;;"));
        assert!(data.contains("BDAY:18151210"));
        assert!(data.contains("X-CUSTOM:value"));
        assert!(!data.contains("old@example.test"));
        // The card is 3.0, so the preference marker is the 3.0 TYPE=PREF form.
        assert!(data.contains("EMAIL;TYPE=home;TYPE=PREF:new@example.test"));
    }

    #[test]
    fn vcard_from_patch_keeps_apple_group_prefix() {
        // The Apple Contacts shape: a grouped EMAIL paired with an X-ABLabel.
        // Patching the emails must keep the group on the rewritten EMAIL so the
        // X-ABLabel (preserved verbatim) stays bound to it.
        let source = "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Ada\r\nitem1.EMAIL;TYPE=INTERNET:old@example.test\r\nitem1.X-ABLabel:Work\r\nEND:VCARD\r\n";
        let current = parse_contact("/ab/1.vcf".to_string(), None, None, source);
        let updated = vcard_from_patch(
            &current,
            source,
            &ContactPatch {
                emails: Some(vec![ContactEmail {
                    value: "new@example.test".to_string(),
                    kind: Some("internet".to_string()),
                    is_primary: false,
                }]),
                ..ContactPatch::default()
            },
        );

        assert!(updated.contains("item1.EMAIL;TYPE=internet:new@example.test"));
        assert!(updated.contains("item1.X-ABLabel:Work"));
        assert!(!updated.contains("old@example.test"));
    }

    #[test]
    fn long_preserved_line_round_trips_losslessly_through_patch() {
        // A long folded preserved value must come back byte-identical after a
        // patch that does not touch it: no unfold/refold of unmodeled lines.
        let long_value = "x:    y ".repeat(40);
        let escaped = long_value.replace(',', "\\,");
        let mut source = String::from(
            "BEGIN:VCARD\r\nVERSION:4.0\r\nFN:Ada\r\nEMAIL;TYPE=work:old@example.test\r\nNOTE:",
        );
        for (index, chunk) in escaped.as_bytes().chunks(40).enumerate() {
            if index > 0 {
                source.push_str("\r\n ");
            }
            source.push_str(std::str::from_utf8(chunk).unwrap());
        }
        source.push_str("\r\nEND:VCARD\r\n");

        let current = parse_contact("/ab/1.vcf".to_string(), None, None, &source);
        assert_eq!(current.notes.as_deref(), Some(long_value.as_str()));

        let patched = vcard_from_patch(
            &current,
            &source,
            &ContactPatch {
                emails: Some(vec![ContactEmail {
                    value: "new@example.test".to_string(),
                    kind: None,
                    is_primary: false,
                }]),
                ..ContactPatch::default()
            },
        );

        let reparsed = parse_contact("/ab/1.vcf".to_string(), None, None, &patched);
        assert!(patched.contains("new@example.test"));
        assert_eq!(reparsed.notes.as_deref(), Some(long_value.as_str()));
    }

    #[test]
    fn malformed_resource_is_a_per_resource_projection_failure() {
        // An unterminated quoted parameter is a tokenizer error; the projector
        // returns Err so the caller can route the resource to failed_ids
        // instead of failing the whole sync.
        let data =
            "BEGIN:VCARD\r\nFN:Ada\r\nEMAIL;TYPE=\"unterminated:ada@example.test\r\nEND:VCARD\r\n";
        assert!(contact_from_vcard("/ab/1.vcf".to_string(), None, None, data).is_err());
    }

    #[test]
    fn vcard_from_patch_clears_photo_only_when_requested() {
        let source = "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Ada\r\nPHOTO;ENCODING=b;TYPE=JPEG:abcd\r\nEND:VCARD\r\n";
        let current = parse_contact(
            "/ab/1.vcf".to_string(),
            Some(AddressBookId("/ab/".to_string())),
            None,
            source,
        );

        let unchanged = vcard_from_patch(
            &current,
            source,
            &ContactPatch {
                notes: Some(Some("note".to_string())),
                ..ContactPatch::default()
            },
        );
        assert!(unchanged.contains("PHOTO;ENCODING=b;TYPE=JPEG:abcd"));

        let cleared = vcard_from_patch(
            &current,
            source,
            &ContactPatch {
                photo_url: Some(None),
                ..ContactPatch::default()
            },
        );
        assert!(!cleared.contains("PHOTO;ENCODING=b;TYPE=JPEG"));
    }

    #[test]
    fn vcard_from_patch_writes_inline_photo() {
        let source = "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Ada\r\nPHOTO;ENCODING=b;TYPE=JPEG:abcd\r\nEND:VCARD\r\n";
        let current = parse_contact(
            "/ab/1.vcf".to_string(),
            Some(AddressBookId("/ab/".to_string())),
            None,
            source,
        );

        let updated = vcard_from_patch(
            &current,
            source,
            &ContactPatch {
                photo: Some(Some(ContactPhoto {
                    data: vec![1, 2, 3, 4],
                    media_type: Some("PNG".to_string()),
                })),
                ..ContactPatch::default()
            },
        );

        assert!(updated.contains("PHOTO;ENCODING=b;TYPE=PNG:AQIDBA=="));
        assert!(!updated.contains("PHOTO;ENCODING=b;TYPE=JPEG:abcd"));
    }
}
