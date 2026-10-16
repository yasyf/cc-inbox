# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- `GET /v1/records` and the `--json` reads (`cci tail`, `watch` and `grep`)
  work out each record's `status` from the openers on the page. Before, every
  call loaded every opener each of the page's drives had ever held and matched
  each one against every closer, so the work grew with the drive's history,
  not the page. Now each call rereads only the page's tracked openers by seq
  and matches them against their drive's closers and resolvers, all read
  fresh, and a drive whose page records open nothing tracked is skipped. An
  opener reparsed or compacted after the page was read gets the same `status`
  as before. A malformed opener elsewhere in the drive's history no longer
  fails the page.

## [0.20.4] - 2026-10-15

### Fixed

- cci redacts GitHub and Buildkite tokens before it stores a record. Every
  write (`cci post`, imports, hook posts, and rows replicated from a peer)
  replaces `ghp_`, `gho_`, `ghs_`, `ghu_`, `ghr_` and `github_pat_` tokens with
  `[redacted:github-token]`, and `bkua_` tokens with
  `[redacted:buildkite-token]`. That covers the text, topic, refs, fields and
  body, and the blob `post` writes for text over 400 characters. Before, a
  token pasted into a post sat in the store and reached every node the mesh or
  relay replicated it to.

## [0.20.3] - 2026-10-15

### Fixed

- A record addressed to a bare lane that reads another drive now reaches it.
  `cci post --to <lane>` checks the live threads on this node. When none
  writes as that lane in the posting drive and one does in exactly one other
  drive, the recipient becomes `<lane>@<that drive>` and the post says so on
  stderr. That lane's `tail --reader`, `watch`, `subscribe` and hook reads
  already deliver `<lane>@<drive>`. When live threads read it in several other
  drives, the post fails and names them. Before, a GO posted on one drive to a
  lane bound to another sat unread until someone grepped for it.
- `cci post --drive` no longer moves the session's drive binding. A lane that
  posted once with `--drive` on another drive had every later bare post land
  there. The binding moves only through `cci drive use`; a session with no
  binding still binds on its first post.

## [0.20.2] - 2026-10-14

### Fixed

- `cci drive use` works from a Codex session. With `CLAUDE_CODE_SESSION_ID`
  unset, the session is `CODEX_THREAD_ID`, the key `cci announce` already
  used for Codex lanes, so the binding holds and later `post`, `tail`,
  `watch` and `digest` calls resolve the drive and default cursor without
  `--drive`. Before, it refused with `CLAUDE_CODE_SESSION_ID is not set`.

## [0.20.1] - 2026-10-13

### Fixed

- The `PostToolUse` hook no longer redelivers a `peer:<session>` record after
  the lane's session posts on another drive. Each drive keeps its own lane
  cursor, and a fresh one starts at the session's start, so every peer record
  since then came back in pages of three, once per newly used drive. A read
  now skips peer records at or before the furthest position the reader's
  cursor reached in any drive.

## [0.20.0] - 2026-10-13

### Changed

- `post` writes the record's own time into its text. A clock typed in a
  leading `R<n> (<clock>)` stamp, such as `R84 (10:59 AM)`, is replaced with
  that time in the form readers print, and `{at}` anywhere in the text
  becomes it. The stored text, a spilled file, and the duplicate checks read
  the stamped text. Only text given as `--text` or the one argument is
  stamped. A post that takes its text from a `--path` file stores the file's
  first line as written, so the record's text still equals that line. A
  keyless repost that carries a stamp or `{at}` is a duplicate only within
  the same clock minute.

## [0.19.0] - 2026-10-13

### Added

- `claim --rel opened --pr N` from a drive-bound session posts an `opened`
  record for each PR on that drive, with the caller's bound lane, `root`, or
  the subagent's id as its lane. `ccx vcs ship` already makes that claim, so
  every PR it opens reaches the drive's landing desk without a hand-posted
  `opened` record. A repeated claim posts nothing new.

## [0.18.0] - 2026-10-12

### Changed

- `post` text over 400 characters no longer refuses: the full text spills to
  a file under `$CCI_HOME/blobs/`, the record carries its first non-blank
  line with `refs.path` naming the file, and `post` prints the path on
  stderr. Two spilled posts that share a first line stay two records.

## [0.17.0] - 2026-10-11

### Added

- `--pr` on `post`, `claim`, `who`, and the read commands takes `owner/repo#N`
  or a GitHub pull request URL as well as a bare number, so a record can name
  a PR in another repo. A bare number is still stored as a JSON number and a
  qualified PR as an `owner/repo#N` string; lines render it as
  `pr:owner/repo#N`.

## [0.16.2] - 2026-10-11

### Fixed

- `cci watch` runs until it is interrupted. It used to exit with status `0`
  after 29 minutes by default, which ended a persistent Monitor silently and
  dropped records posted before the next arm. `--for` still sets a limit.

## [0.16.1] - 2026-10-11

### Fixed

- `cci relay enroll` signs an owner grant for the member it admits, as
  `cci pair accept` does. The relay forwards it on `peer.joined`, so every
  other owner pins the new service or owner on its next pull instead of
  running `cci relay join --service` by hand. A relay without relay-api v3.5
  ignores the grant.

## [0.16.0] - 2026-10-11

### Added

- `cci relay found` founds a relay mesh as its first owner. It sends the STS
  caller proof that `ci cci-relay prove` prints, with this node's Secure
  Enclave endorsement, then joins as owner. Before it, no client could create
  a mesh, so `cci relay join` had no member to join as.
- `cci relay enroll` admits a pinned owner node or a scoped service key to a
  relay mesh. The owner approves with Touch ID through `synckitd consent
  request`, and the command shares the current group key with the new member.

## [0.15.0] - 2026-10-11

### Added

- `cci upgrade` moves every cci install on the machine in one command: the
  plugin through `claude plugin update`, the Homebrew cask through
  `brew upgrade --cask`, and each standalone `cci` on `PATH` from the release
  asset. It then runs `serve` from the newest install so the daemon moves too.
  Run it once after a release workflow finishes.
- The daemon follows upgrades by itself. At start and every 30 seconds it
  reads the plugin cache and Homebrew cask version directories, and hands off
  to a newer release's `cci serve`, which drains it and starts the newer build.
  The daemon copy no longer stays behind until someone runs `cci serve`.

## [0.14.0] - 2026-10-11

### Added

- `cci pair` lets a machine outside the owner's synckit mesh join a mesh as a
  read-only node over the relay. It prints a one-time code and its node
  fingerprint. `cci pair accept CODE` on an owner shows that fingerprint and
  asks for Touch ID through `synckitd consent request`, here or on an attended
  owner host. `--tier service --allow --label` admits a scoped writer instead.
  The accepting owner signs a member grant and its owner roster, so no node
  pins a key on the relay's word alone (#64).
- `cci unpair NODE` revokes a node through the relay and rotates the group
  key. The daemon acts on relay-signed `peer.unpaired` controls on every
  node, so a write from a paired node unpairs it everywhere with one key
  rotation. `cci peers` lists trusted nodes with their tier, and `--mesh`
  reconciles that list against the relay's directory (#64).

## [0.13.0] - 2026-10-11

### Added


- `cci relay join|push|pull|sync|rotate|status` replicates records through the
  wide-area relay, end-to-end encrypted so the relay reads no record body.
  Each mesh has a group key that an owner node wraps to every pinned member
  with HPKE (P-256) and signs; a node accepts a wrap only from an owner it
  pinned itself. Record bodies are AES-256-GCM under that key, and the routing
  header (mesh, drive, origin, `origin_seq`, tier, kind, time, epoch) is
  signed by the writer and bound as additional data. Paired members never
  push, rotation is an epoch compare-and-set that locks out an unpaired node,
  and `cci daemon` runs a push and a pull loop per joined mesh (#62).
- Delegated service instances: `cci relay join --instance --drive D` mints a
  per-job key that `$CCI_SERVICE_KEY` signs into a scoped, expiring
  delegation, so concurrent CI jobs sharing one service write under their own
  origins (#62).
- The public `e2e` package holds the wrap, envelope, delegation and control
  formats for other Go implementations of the relay (#62).

## [0.12.0] - 2026-10-11

### Added

- `cci mesh enable` replicates records across the owner's Macs through
  synckit: the daemon runs the resident consumer `cci`, `synckitd` delivers
  node-signed deltas and bounded snapshots to every mesh host, and a receiver
  applies only changes signed by the owner node it pinned for the delivering
  host, fenced by its per-origin receipt. `synckitd host add` and `host enroll`
  pin each host's node key. `cci mesh status` shows what each host acked (#61).

## [0.11.0] - 2026-10-11

### Added

- Multi-origin records for the cross-machine mesh. Every record carries
  `origin`, the id of the node that wrote it, and `origin_seq`, its seq
  there; `(origin, origin_seq)` is its identity on every node. `Store.Export`
  and `Store.Apply` move records between nodes. Apply rewrites `re` and
  `resolves` to local seqs and holds a reply until its target arrives.
  Transport receipts, per-peer acked watermarks, pinned members, and
  replicated sessions and claims live beside them. Compaction never folds a
  record a peer has not acked, and a folded remote record stays refused if a
  transport replays it. Schema 11; older binaries refuse the store (#58).
- Records for services: a dotted kind such as `oncall.fix.status` is a
  namespaced service kind with a 14-day TTL, `cci post --attr k=v` fills
  `fields.attrs`, and `--body-file` attaches a JSON `body` of up to 64 KiB.
  A post whose `--id` the drive already holds returns the stored record.
  `cci cursor set` moves a cursor backwards for a reader that restarts. The
  channel tag and text `cci watch` print a record's body under its line, and
  the tag's meta carries its `id`.
- `--to peer:<node>/<session>` addresses a session on another node, `cci who`
  lists replicated sessions, and `--origin` narrows reads and `who` to one
  node.

### Fixed

- Compacting a drive deleted a resolver whose target lives in another drive
  unless the resolver was itself addressed across drives, so the other
  drive's digest reported the item open again. Compaction now keeps every
  resolver whose target sits in another drive.
- `cci state` kept the record that arrived last for a kind, lane and topic;
  it now keeps the newest by time, so a late replicated record cannot mask a
  newer local one.

## [0.10.4] - 2026-10-11

### Added

- Subagents and teammates are peers of their own. Every session thread is
  keyed by its session and the harness's `agent_id`, so `cci who` lists each
  lane of a session apart as `session=<session>/<agent name>`, and `cci who
  --pr` names the lane that holds a claim. The plugin's hooks announce a
  subagent on its first tool call, name it from the harness's agent metadata,
  deliver its own lane's records, and retire it on `SubagentStop`. `cci me`,
  `cci claim`, `cci unclaim`, `cci drive use`, a lane's lane binding and the
  default cursor act as the thread `CCI_AGENT_ID` names beside
  `CLAUDE_CODE_SESSION_ID`; without it, as the root thread.
  `peer:<session>/<agent id or name>` addresses one subagent, and a bare
  `peer:<session>` still means the root thread. `cci who --agent` and
  `GET /v1/peers?agent=` select one. Store schema 10 adds the agent to the
  `sessions` and `claims` keys; existing rows become root threads (#60).
- `cci node` prints this machine's node id, key fingerprint and endorsement;
  `cci node endorse` endorses the node's P-256 file key once with the macOS
  Secure Enclave key. The key lives at `<home>/node/key.pem`, or
  `$CCI_NODE_KEY` on CI and service hosts (#57).

## [0.10.3] - 2026-10-10

### Fixed

- A session bound before 0.10.0 has no stored role; `cci who` now derives
  `root`, `lane` or `session` from its binding, so `--role root` finds the
  roots of drives that predate the registry (#55).

## [0.10.2] - 2026-10-10

### Fixed

- The schema-skew error's upgrade command for a binary under `~/.local/bin`
  extracted the release over the live `cci` in place, so a `cci` call during
  the extract found it missing or half-written. It now extracts into a temp
  directory beside the binary and renames it over (#53).

## [0.10.1] - 2026-10-10

### Fixed

- `cci who` read the session registry's `procStart` as local time; it is
  UTC, so every window looked seven hours off and `who` reported them all
  `gone`. Liveness now parses it as UTC. A session bound before 0.10.0 has no
  stored role and now prints as `session` instead of a blank (#51).

## [0.10.0] - 2026-10-10

### Added

- A machine-wide peer registry so sessions in different worktrees and drives
  can find each other. `cci who` lists live Claude Code sessions, joined with
  the registry under `~/.claude/sessions`, one line each ending in the `uds:`
  address `SendMessage` accepts, and narrows by `--session` prefix, `--name`,
  `--role`, `--drive`, `--lane`, `--tag`, `--repo`, `--worktree`, `--branch`
  or `-q`. `cci announce` records a role, description and tags, with the
  worktree, branch and `origin` slug read from git; the `SessionStart` hook
  and `drive use` announce with an inferred role, and every hook call
  refreshes `seen_at`. `cci me` prints this session's line for handoffs.
  The `sessions` table gains the peer columns, so the store schema moves to 7
  and older `cci` builds refuse to open it until upgraded (#46).
- Claims tie a session to an artifact as `<rel> <kind>:<key>`. `cci claim`
  and `cci unclaim` record explicit ones (`--pr`, `--build`, `--branch`,
  `--worktree`, `--issue`, `--thread`, `--stack`, `--target`, `--subsystem`,
  `--ref <kind>:<key>`, `--rel`), and every posted record derives claims on
  its `--pr`, `--build`, `--stack` and `--target` refs for the posting lane's
  sessions. `cci who --pr`, `--build`, `--ref` and `--rel` find the sessions
  behind an artifact and list the claims that matched; `GET /v1/peers` serves
  the same query. Schema 8 adds the `claims` table (#47).
- `cci post --to peer:<session prefix or name>` reaches one session in any
  drive, resolved at post time, and `--to <lane>@<drive>` reaches a lane in
  another drive, `root@<drive>` for its root. Reads, the channel subscription
  and lane hook delivery include such records from any drive under the same
  cursor, `--resolves` may close one across drives, and lines render the
  recipient as `peer:<8-char prefix>`. `cci announce` broadcasts a new
  24-hour `peer` record in its drive so a root subscribed with `--kind peer`
  hears a sibling join (#48).
- Registry hardening from review: an automatic announce keeps an explicit
  role, description and tags; a session replaced in the same process by
  `/clear` is marked superseded (schema 9) and drops out of `cci who`;
  `drive use` and a lane's first post refresh an inferred role; liveness
  reads the registry's `procStart` instead of a registration delay; codex
  lanes announce with the `codex` process as their window; worktree paths
  are compared after resolving symlinks; a registry file removed mid-read
  is skipped; and the announce runs as its own `SessionStart` hook so a
  missing `git` never costs a session its digest. `cci claim` announces a
  session that has no registry row first, a build key's `%` and `_` match
  literally, and `GET /v1/peers` refuses two artifact selectors and
  normalizes `pr`. Across drives: compaction keeps a resolver addressed to
  another drive, a lane's first cursor starts before records addressed to it
  from other drives, `--to` restricts the cross-drive branch too, JSON
  annotation and `cci state` keep drives apart, and the `peer` broadcast is
  written by the `registry` lane so a root hears its own siblings (#49).

## [0.9.3] - 2026-10-09

### Fixed

- `cci post --path <file>` without a text argument or `--text` posts the file's
  first line, clipped to 400 characters, as the record's text, and the file
  stays the body. It used to fail with `post needs its text`, the dead end the
  over-400 error pointed to. That error now names the working command:
  `cci post --lane <lane> --kind <kind> --path <file>` (#44).

### Changed

- Builds use Go 1.26.9, which fixes the `net/http` advisories GO-2026-6611
  through GO-2026-6617 (#44).

## [0.9.2] - 2026-10-06

### Fixed

- Import each inbox line at most once per drive. A rotation archive, a reread
  after the inbox file was replaced, or a copy of an inbox no longer
  re-appends old lines as new records. The store now keeps a ledger of every
  line hash it has consumed, including lines a parser reparse skipped and
  records `cci compact` folded away. On Oct 6 the first `inbox-rotate` cut made
  the daemon import nine archives, and 313 records dated Sep 27 to Oct 4
  reached `cci watch` again under new sequence numbers (#42).

### Changed

- Store schema 6 adds the line ledger and backfills it from existing records.
  Binaries older than 0.9.2 refuse a store this release has opened, so
  upgrade the plugin, the Homebrew cask, and any copied `cci` binary together.

## [0.9.1] - 2026-10-06

### Fixed

- Stop delivering the root's own owner prompts back to the root. The prompt
  hook now records them with source `hook:root`, and `--reader root` and
  `--reader main` skip broadcasts with that source, so the root's
  `cci watch` and `cci subscribe` channel no longer echo each prompt the owner
  just typed. Other readers still receive them (#40).

## [0.9.0] - 2026-10-06

### Added

- Deliver every record addressed to a lane into that lane's Claude Code
  session after each tool call, whatever its kind. The first
  `cci post --lane <lane>` in a session binds it to the lane, and the new
  `PostToolUse` and `PostToolUseFailure` hook `cci hook post-tool-use` injects
  each unseen addressed record. Records posted to the lane after the window
  started arrive even when the lane has not posted yet. Before this, a running
  lane received no cci record at all: a `stopped` record to
  release-repin-head went unseen for ten minutes. `SessionStart` in a lane
  session delivers the same records instead of the drive digest.

### Fixed

- Print a budgeted read's first record whole even when it alone exceeds the
  budget. A 400-character record of multi-byte characters used to stall a
  cursored read forever.
- `cci drive use --root` clears a lane binding on the session.

## [0.8.1] - 2026-10-06

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

[Unreleased]: https://github.com/yasyf/cc-inbox/compare/v0.20.4...HEAD
[0.20.4]: https://github.com/yasyf/cc-inbox/compare/v0.20.3...v0.20.4
[0.20.3]: https://github.com/yasyf/cc-inbox/compare/v0.20.2...v0.20.3
[0.20.2]: https://github.com/yasyf/cc-inbox/compare/v0.20.1...v0.20.2
[0.20.1]: https://github.com/yasyf/cc-inbox/compare/v0.20.0...v0.20.1
[0.20.0]: https://github.com/yasyf/cc-inbox/compare/v0.19.0...v0.20.0
[0.19.0]: https://github.com/yasyf/cc-inbox/compare/v0.18.0...v0.19.0
[0.18.0]: https://github.com/yasyf/cc-inbox/compare/v0.17.0...v0.18.0
[0.17.0]: https://github.com/yasyf/cc-inbox/compare/v0.16.2...v0.17.0
[0.16.2]: https://github.com/yasyf/cc-inbox/compare/v0.16.1...v0.16.2
[0.16.1]: https://github.com/yasyf/cc-inbox/compare/v0.16.0...v0.16.1
[0.16.0]: https://github.com/yasyf/cc-inbox/compare/v0.15.0...v0.16.0
[0.15.0]: https://github.com/yasyf/cc-inbox/compare/v0.14.0...v0.15.0
[0.14.0]: https://github.com/yasyf/cc-inbox/compare/v0.13.0...v0.14.0
[0.13.0]: https://github.com/yasyf/cc-inbox/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/yasyf/cc-inbox/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/yasyf/cc-inbox/compare/v0.10.4...v0.11.0
[0.10.4]: https://github.com/yasyf/cc-inbox/compare/v0.10.3...v0.10.4
[0.10.3]: https://github.com/yasyf/cc-inbox/compare/v0.10.2...v0.10.3
[0.10.2]: https://github.com/yasyf/cc-inbox/compare/v0.10.1...v0.10.2
[0.10.1]: https://github.com/yasyf/cc-inbox/compare/v0.10.0...v0.10.1
[0.10.0]: https://github.com/yasyf/cc-inbox/compare/v0.9.3...v0.10.0
[0.9.3]: https://github.com/yasyf/cc-inbox/compare/v0.9.2...v0.9.3
[0.9.2]: https://github.com/yasyf/cc-inbox/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/yasyf/cc-inbox/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/yasyf/cc-inbox/compare/v0.8.1...v0.9.0
[0.8.1]: https://github.com/yasyf/cc-inbox/compare/v0.8.0...v0.8.1
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
