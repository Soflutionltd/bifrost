# DAV crates defect findings (caldav, carddav, dav-core)

1. An entry is removed entirely when completely resolved. No historical record
   stays here.
2. Stable IDs never change and are never reused; removal leaves a gap.
3. An entry adjudicated against, verified incorrect, or whose outcome is that no
   action is taken owes comments at the code sites it names - and, where the
   claim touches a documented contract, the relevant `reference/` or `docs/`
   page - before the entry is removed, so the finding is not hunted again.
4. Once all findings are resolved, the file gets deleted.
5. Finding IDs are never written into the code or other documents. They are
   stable only until this document is drained; the next hunt writes new ones,
   and they are never deduplicated through git history. Carry the context
   inline instead.

Filed from the October 2026 defect hunt and the resolution waves that followed.

## DAV-017 - CardDAV establishment and inventory list the whole home for one ctag

Lateral from the CalDAV depth-0 token work. CalDAV `event_snapshot` now reads a
collection's sync-token with a depth-0 request; CardDAV establishment and
inventory still issue a depth-1 PROPFIND of the whole address-book home to read
one collection's ctag, which is O(collections) per scope. That listing also
refreshes the observed directory URLs, so moving it to depth 0 needs those
URLs refreshed some other way.

## DAV-018 - Namespace-aware parsing hard-fails on sloppy server XML

Lateral from the namespace-aware parser work. `dav-core` now parses with
`NsReader`, and `resolved_xml_name` treats an undeclared prefix
(`ResolveResult::Unknown`) as a hard parse error where it used to be tolerated;
an element with no namespace at all can no longer match any DAV element. Every
in-repo fixture declares its prefixes, but a real server that omits `xmlns:D`
now fails every parse instead of degrading. Separately, `property_namespace` is
a closed name table defaulting to `DAV:`, so a future non-DAV property name not
in the table silently never matches. Decide whether to tolerate unbound
prefixes and default-namespace-less DAV elements.
