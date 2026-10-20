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

Filed from the October 2026 defect hunt: the DAV scope, plus DAV findings
from the net, types and sync adoption scopes. Several were reported
independently by more than one scope; each entry says which.

## DAV-016 - The URL identity key decodes path segments with lossy UTF-8

Lateral from the href-key work. The identity rule shared by `same_dav_url`, the
sync-report lane, snapshot diffs and failed-href preservation decodes
percent-encoded path segments lossily, so `%FF` and `%FE` produce the same key.
Left unchanged because changing it also changes the established mutation
identity matching; the evidence is documented at the key implementation.

