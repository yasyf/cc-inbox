# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Add `retro`, `posted`, `design`, `serving`, `ready`, `duplicate`,
  `stand-down`, `not-ours`, `stopped`, `refuse`, `fail`, `recovered`,
  `delete-list`, `skew`, `not-live`, and `urgent` kinds. They have no
  default expiry or kind pairing. Accept `refused` as `refuse` and
  `failed` as `fail`.

### Changed

- Match grep patterns literally and case-insensitively by default. Use
  `--regex` for regular expressions, including `|` alternation. Keep
  newest-first ordering and the existing byte budgets.
- Reparse existing imports with parser version 5 so watched lead tokens
  gain their corresponding kinds without changing stored timestamps.

### Fixed

- Limit imported stamps to five minutes after the file's modification
  time. Move later stamps to the previous day and allow a one-day forward
  correction only within that limit. Keep out-of-order stamps on their day.
- Search the whole rendered record line in `cci grep`, including kind,
  lane, recipients, topic, and refs, so queries such as `STATE census`
  match records whose kind and lane are stored outside their text.
- Extract explicit `stack:<project>/<env>` and `target:<name>` refs
  from inbox lines and reparse existing imports with parser version 5.
  Track imported openers with either ref in digest open sections and
  preserve them through compaction. Imported openers without either
  ref remain untracked. Shared deployment refs close tracked items
  in the same drive using the opener's normal closers; imported
  openers also accept `fix-live`, `lift`, or `done`. Ref closure requires
  a later recorded time, with sequence number breaking a tie. If both records
  name stacks, at least one stack must match; otherwise, a shared
  target is enough. A native hold still needs `lift`. Imports from
  older archives cannot close newer regressions by ref.
- Include `state` records in `cci state` alongside `head` and `contract`,
  keeping the latest non-withdrawn record per kind, lane, and topic.
  Empty text reads say `no head, contract, or state records on <drive>`.
- Set the session's tail cursor to the drive's latest record in the
  same transaction as `cci drive use`, including on rebind. Named
  cursors and explicit `--since` reads keep their existing behavior.

## [0.4.0] - 2026-10-05

### Fixed

- Attribute stamped inbox lines to their writing lane, including lanes
  after parenthesized timestamps. Assign `OWNER` lines without a lane to
  `root` and parse `:5x` minutes as `:50`.
- Date imported stamps within 12 hours of the dating cursor and only
  move it backward, keeping out-of-order stamps on their day.
- Parse time-first inbox lines such as `1:43 AM PT <lane>: MECHANISM ...`
  without treating `AM` or `PM` as runner kinds. Preserve the full clock,
  lane, and kind, and reparse existing imports with parser version 2.
- List digest sections and `/v1/lanes` by sequence number descending while
  still selecting each lane's latest record by time.
- Attribute keyed runner events to `runner`, with the subject lane as
  `topic`. Preserve worker attribution for `msg_<id>` relays and unkeyed
  worker lines. For a `runner:` subject, use the first `<lane>=ctx_`
  token as the topic. Reparse existing imports with parser version 3.

### Changed

- Track the import parser version in schema migration 5. On a parser
  change, the next import or refresh reparses consumed lines and updates
  matching records without changing stored timestamps or restoring
  compacted records. `cci import` reports the number reparsed.
- Add `--width` to tail, watch, and digest, defaulting to 400 characters
  per rendered record line. `0` prints whole records. Include an ellipsis
  when clipping, then append reply marks. Grep, state, and JSON records
  stay whole. `SessionStart` uses width 400 for both reads and keeps its
  2,500-byte digest budget and 1,500-byte tail budget. Wider lines can
  leave room for fewer records within the unchanged byte budgets.

## [0.3.0] - 2026-10-05

### Changed

- Refresh registered markdown inboxes and discover archives every second in
  the daemon. `cci serve` or any plugin `SessionStart` starts or reuses it.
  Session start injects bound drive context before ensuring the daemon;
  ensure failures surface as non-blocking hook errors. Remove the `PostToolUse`
  hook and `cci hook post-tool` command. CLI reads stay direct to SQLite.
- Raise the default read budget from 4,000 to 6,144 bytes, matching the
  long-running skill's `inbox-digest.py`. The cap remains 16,000 bytes.
- Clip text record lines in tail, grep, watch, state, and digest sections to
  200 characters, including an ellipsis. Append reply marks after clipping so
  they stay visible. JSON record text is never clipped. `SessionStart` keeps
  its 2,500-byte digest budget and 1,500-byte tail budget.

## [0.2.0] - 2026-10-05

### Added

- Deploy stack, target, lane, board, PR, and build references; repeatable
  environments; release mode, outcome, commit, and census fields.
- `blocked`, `unblock`, and `review` kinds, plus `--resolves` to close an open
  item from any later record. Defects now remain open until closed.
- Stack, target, and PR filters for CLI reads and HTTP records and streams;
  computed opener status on JSON tail, grep, watch, and HTTP records reads.
- `/v1/lanes?drive=` for each lane's latest record by time, including expired
  records, and import extraction of stack tokens, PRs, and Buildkite build URLs.
- `cci serve` now ensures a daemonkit daemon (`com.yasyf.cc-inbox`) that serves
  the HTTP API at `127.0.0.1:7377` from a long-lived store, and returns. Writes
  and CLI reads stay direct to SQLite and need no daemon.

### Changed

- Schema 3 migrates `refs.pr` to `refs.prs`, `refs.build` to `refs.builds`, and
  `fields.env` to `fields.envs`, and merges the old `fields.stack` PR list into
  `refs.prs`. `--stack` now takes `<project>/<env>`; use repeated `--pr` for PRs.
- README and plugin guidance cover stack queries, explicit resolution, and
  `--to owner` for work that needs the owner.

## [0.1.0] - 2026-10-05

### Added

- `cci` CLI for typed drive coordination records in a local SQLite store.
- Kind validation, a 400-character text limit, typed references and fields,
  expiry, duplicate detection, and open-item pairing.
- Session drive bindings, root prompt capture, and named read cursors that
  resume after the last printed record. New tail cursors read the past hour.
- Bounded tail and regex search, Monitor watches with a 29-minute default
  lifetime, and digests that list open items in their window and count older ones.
- Incremental markdown import with source-line deduplication, file offsets, and
  blob references for long bodies.
- Daily compaction that preserves still-open items.
- Store isolation through the `CCI_HOME` environment variable.
- Read-only HTTP endpoints for drives, records, digests, and server-sent events.
- Claude Code marketplace plugin with `SessionStart`, `UserPromptSubmit`, and
  `PostToolUse` hooks, plus the `using-cci` skill.

[Unreleased]: https://github.com/yasyf/cc-inbox/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/yasyf/cc-inbox/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/yasyf/cc-inbox/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/yasyf/cc-inbox/releases/tag/v0.1.0
