# bifrost

Rust clients for email/calendar/contact protocols, the protocol engine of [Courly](https://github.com/Soflutionltd/courly).

> **Bifrost** - the rainbow bridge that connects Asgard to the other realms in Norse cosmology. These crates connect a single client to many email-server realms.

## Crates

| Crate | Status | Description |
|---|---|---|
| [`bifrost-jmap`](crates/jmap) | Pre-1.0 | JMAP client (RFC 8620 / 8621 / 8887 / 9404 / 9425 / 9610 / 9670, calendars draft-26, sieve draft-14) |
| [`bifrost-imap`](crates/imap) | Pre-1.0 | IMAP client (daaki-derived, tokio + native-tls) |
| [`bifrost-smtp`](crates/smtp) | Pre-1.0 | SMTP submission + LMTP (lettre-derived, native-tls) |
| [`bifrost-graph`](crates/graph) | Pre-1.0 | Microsoft Graph + Exchange Web Services |
| [`bifrost-gmail`](crates/gmail) | Pre-1.0 | Gmail API |

Per-crate architecture references live in [`reference/`](reference/). Working examples live under each crate's `examples/` directory.

## License

This branch is source-available under the [PolyForm Shield License 1.0.0](LICENSE): you may use, modify and contribute, but not build a competing product from it. Contributions need the [Courly Contributor License Agreement](https://github.com/Soflutionltd/courly/blob/main/CLA.md).

The code published upstream at [folknor/bifrost](https://github.com/folknor/bifrost) remains available under the Apache License 2.0 ([text](LICENSES/Apache-2.0.txt)) or the MIT license ([text](LICENSES/MIT.txt)), at your option; see [NOTICE](NOTICE).

## Acknowledgements

bifrost. Copyright 2026 the bifrost authors.

This product includes software originally developed by Stalwart Labs LLC <hello@stalw.art> as the [`jmap-client`](https://github.com/stalwartlabs/jmap-client) crate, licensed under Apache-2.0 OR MIT. The `bifrost-jmap` crate descends from that work; substantial portions have been rewritten since the fork point.

This product includes software originally developed by the lettre authors as the [`lettre`](https://github.com/lettre/lettre) crate, licensed under MIT:

- Copyright (c) 2014-2024 Alexis Mousset <contact@amousset.me>
- Copyright (c) 2019-2025 Paolo Barbolini <paolo@paolo565.org>
- Copyright (c) 2018 K. <kayo@illumium.org>

The `bifrost-smtp` crate descends from that work.

This product includes software originally developed by Anees Iqbal as the [`daaki-imap`](https://github.com/steelbrain/daaki) crate, licensed under MIT. The `bifrost-imap` crate descends from daaki commit `e81a169c52afee65eb8a2fdfd252f8ebaacdb997`; local changes include bifrost workspace metadata, a tokio plus native-tls transport policy, and parser migration work for nom 8.
