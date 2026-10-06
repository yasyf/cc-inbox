# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Tell the session in the channel's instructions that every tag already
  passed the subscription's filter, that a record addressed to the reader
  arrives whatever its kind, and that `main` is the root. A verification
  session read an `ask` addressed to `main` as misrouted and refused to act on
  it.

## [0.8.0] - 2026-10-06

### Added

- Add a `cci` channel to the plugin. `cci subscribe --reader <lane>
  [--kind <k>]... [--cursor <name>]` delivers each new record addressed to the
  reader, plus other lanes' broadcasts of each kind, into the session as a
  `<channel source="plugin:cc-inbox:cci">` tag, with no Monitor to re-arm.
  The cursor defaults to `channel-<session>` and starts at the head when you
  subscribe; the channel advances it past each delivered record, and starts
  only once the client sends `notifications/initialized`. The
  subscription belongs to the Claude Code window and survives compaction and
  `/clear`, and the `SessionStart` hook carries it into a resumed session.
  `cci unsubscribe` stops it. Sessions must be launched with
  `--channels plugin:cc-inbox@cc-inbox`.
- `cci watch --for 0` streams until interrupted.

### Changed

- `cci watch` advances its cursor past each record it writes, so a write
  failure partway through a page no longer replays the records already
  written.

### Fixed

- Deliver records addressed to `main` to `--reader root` and `--to root`, and
  records addressed to `root` to `main`. Lanes and the dashboard address the
  drive's root by either name, and the root's watch read only `root`, so 58
  records in three hours on release-v3 never reached it.

## [0.7.2] - 2026-10-06

### Fixed

- Include `--re`, `--resolves`, `--topic` and `--to` in the duplicate check
  for `post`, so posts with the same text that close different records are
  stored separately instead of collapsing into one `(duplicate)`.

## [0.7.1] - 2026-10-05

### Fixed

- Strip `com.apple.quarantine` from the Homebrew cask's `cci` binary after
  install. The 0.7.0 cask left it set, so every new exec stalled on a
  Gatekeeper check.

## [0.7.0] - 2026-10-05

### Added

- Add the `evidence` kind for an incident's evidence lane. It has no default
  expiry or kind pairing. Imported `EVIDENCE` lines use it; existing imports
  reparse with parser version 6.
- Accept `-n` and `--limit` on `tail` and `grep` to print only the newest N
  matching records, as `tail -n` does. `tail` prints them oldest first and
  advances its cursor to the newest one.

### Changed

- Read `grep` patterns as regular expressions by default, so `Q0|Syncing`
  matches either word instead of nothing. `-F` (`--fixed-strings`) matches a
  pattern literally. `--regex` is removed.

## [0.6.3] - 2026-10-05

### Fixed

- Accept `-i` and `--ignore-case` on `grep`. Matching stays case-insensitive
  by default, and `--ignore-case=false` matches case exactly.
- Accept a clock time for `--since`, such as `09:00`, `9:00 AM`, or `12:0x PM`,
  read in local (Pacific) time, the zone every record is displayed in. It
  resolves to the latest such time at or before now; a `Z` or `UTC` suffix
  reads it in UTC.
- Take the text of `post` as its one positional argument, the same as
  `--text`. Two arguments, or an argument plus `--text`, fail with an error
  that names `--text`.

## [0.6.2] - 2026-10-05

### Fixed

- Drop an untracked hold from digest once a post with `--resolves <seq>`, or
  a `lift` with `--re <seq>`, closes it. The `untracked holds` line used to
  list every untracked hold in the window, closed or not.

## [0.6.1] - 2026-10-05

### Fixed

- Default text `digest` to a 32,000-byte budget, so every section prints
  whole at its record limit. Tail, grep, and state keep 6,144 bytes, and
  `--budget` still overrides either default.

## [0.6.0] - 2026-10-05

### Added

- List imported holds that name no `stack:<project>/<env>` or
  `target:<name>` ref on one `untracked holds` line in text digest, and
  as `untracked_holds` in JSON digest. No later line can close them, so
  writers can add the ref.

### Changed

- Honor `--budget` as given instead of capping it at 16,000 bytes, and
  print whole records in tail, watch, and digest unless `--width N` is
  set. `SessionStart` keeps its 400-character width, 2,500-byte digest
  budget, and 1,500-byte tail budget.

### Fixed

- Print `no records on <drive> match; store head #<seq>` when a text
  grep matches nothing, so an empty result reads differently from a
  stale store. JSON output stays empty.
- Name the running version and the command that upgrades it when the
  store schema is newer than the binary: `brew upgrade --cask
  yasyf/tap/cci` for the cask, `claude plugin update cc-inbox@cc-inbox`
  for the plugin build, and a `gh release download` into the binary's
  directory for any other copy.

## [0.5.0] - 2026-10-05

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

[Unreleased]: https://github.com/yasyf/cc-inbox/compare/v0.8.0...HEAD
[0.8.0]: https://github.com/yasyf/cc-inbox/compare/v0.7.2...v0.8.0
[0.7.2]: https://github.com/yasyf/cc-inbox/compare/v0.7.1...v0.7.2
[0.7.1]: https://github.com/yasyf/cc-inbox/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/yasyf/cc-inbox/compare/v0.6.3...v0.7.0
[0.6.3]: https://github.com/yasyf/cc-inbox/compare/v0.6.2...v0.6.3
[0.6.2]: https://github.com/yasyf/cc-inbox/compare/v0.6.1...v0.6.2
[0.6.1]: https://github.com/yasyf/cc-inbox/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/yasyf/cc-inbox/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/yasyf/cc-inbox/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/yasyf/cc-inbox/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/yasyf/cc-inbox/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/yasyf/cc-inbox/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/yasyf/cc-inbox/releases/tag/v0.1.0
