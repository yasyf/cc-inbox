# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- `cci post --kind head` without `--pr` takes the pull request from a single
  `#N` in the text and says so on stderr; with none or several it refuses and
  asks for `--pr`. A head with no pull request is invisible to the desks that
  key heads by pull request.

## [0.36.0] - 2026-10-28

### Fixed

- A lane's `PostToolUse` hook delivers `head`, `review`, `state`, `report`,
  `claim` and `matrix` records only when the lane is their first addressee;
  a lane merely copied on one reads it with `cci tail --reader`. A lane copied
  on hundreds of reviews fell hours behind, three or four records per tool
  call.
- `cci tail --reader <lane>` from the thread bound to that lane moves its hook
  delivery past what the tail read, so the hook never repeats it.
- `cci post` rewrites a time label ahead of the post time to the post time
  when the label follows up to four words and ends in a colon, or follows the
  lane's own name, as in `board 1:09 AM:`; before, only a label that opened
  the text was rewritten.
- The daemon cuts HTTP streams still open after half its drain budget, so a
  stop or an upgrade handoff finishes instead of holding its lock and leaving
  port 7377 refusing connections for minutes.
- A `SessionStart` hook running an older cci hands the daemon check to the
  newest install, so an old plugin no longer replaces a newer daemon and sets
  off repeated handoffs.

## [0.35.0] - 2026-10-28

### Fixed

- The Homebrew cask strips quarantine with a `postflight_steps` stanza instead of the Ruby `postflight` block Homebrew 7 deprecated, so `brew upgrade` no longer warns, and only on macOS.
- `cci post` without `--lane` no longer files the record as `root` or as the
  window subscription's reader. A subagent whose `CCI_AGENT_ID` was lost reads
  as the root thread, so its posts carried root's authority; a thread bound to
  no lane is now refused with an error naming `--lane`, and the root passes
  `--lane root`.

## [0.34.0] - 2026-10-27

### Added

- `cci subscribe --mute-kinds <kinds>` keeps those kinds off the channel and
  out of the subscribed root's `PostToolUse` delivery, even when addressed to
  the reader. `cci tail`, `watch` and `grep` take the same flag; without it
  they still read every muted record.
- An `urgent` record whose lane posted the same `--topic` as `urgent` within
  the previous 30 minutes is delivered by neither the channel nor the hook.
  `cci tail` still reads it.
- `cci me` prints the window's channel subscription under its registry line.

## [0.33.0] - 2026-10-27

### Changed

- `cci tail --reader <lane>` with a cursor prints the records addressed to the
  reader first when its backlog does not fit in one read. Such a record names
  the reader in `--to`, or replies with `--re` to one of the reader's own
  records. Peer mail keeps its place in seq order. Broadcasts
  follow in seq order, and a trailer says how many came first. A second
  cursor remembers what was printed early, so a later read never prints it
  again and the resume trailer never skips it. A backlog that fits still
  prints in seq order.
- `--reader` now also delivers a record that replies to one of the reader's own
  records, whoever it is addressed to.

## [0.32.2] - 2026-10-27

### Changed

- `cci post` warns on stderr about every time in its text that is more than a
  minute later than the post's own clock, such as `read 3:44 PM` in a post made
  at 3:42 PM, and stores the text as typed. Only a leading label is rewritten.

## [0.32.1] - 2026-10-27

### Fixed

- `cci post -` and `cci post --text -` read the text from stdin, so a heredoc
  body becomes the record's text. Before, the record stored a literal `-`.

## [0.32.0] - 2026-10-27

### Changed

- `cci post` rewrites a leading `H:MM AM/PM` label in its text that is more
  than a minute ahead of the post's own clock to the post time, and prints
  `cci: time label 3:20 PM rewritten to 3:17 PM` on stderr. A label at or
  before the clock stays as typed, and a label anywhere but the start of the
  text is never touched.

## [0.31.0] - 2026-10-27

### Changed

- The `PostToolUse` hook prints a lane's open asks only when the set changed
  since the last delivery. It no longer adds a "unchanged since the last list"
  line to every delivery, and a set that emptied and then refilled prints the
  one-line seq list instead of the full list again. Only a session start or a
  compaction prints the full list.
- The `PostToolUse` hook delivers only the newest `state` or `report` record on
  a topic. When a lane posts several to the same recipients under one topic
  before the reader's next tool call, such as a runner's heartbeat or its
  report about one PR, the earlier ones are skipped. A record with no topic is
  always delivered, and `cci tail`, `cci watch` and the channel still print
  every record.
- Default views print a record addressed to more than eight lanes as
  `-> <n> lanes` instead of its first eight recipients and `+<n> more`. The
  first eight names of a broadcast to every lane told the reader nothing, and
  they were half of each such line in every lane's hook. `cci show` and
  `cci grep` still print every recipient.

## [0.30.3] - 2026-10-27

### Fixed

- `cci post` no longer answers `(duplicate)` for a record whose text matches an
  earlier one but whose refs, fields or body differ. The dedup key now also
  covers the refs (`--pr`, `--url`, `--board` and the rest, but not `--path`),
  the fields (`--commit`, `--outcome` and the rest) and the body, so a post with
  a corrected `--commit` creates a new record instead of keeping the old sha.

## [0.30.2] - 2026-10-27

### Fixed

- A post or read with no `--drive` from a subagent whose own thread never ran
  `cci drive use` takes the drive of the nearest thread that spawned it, up to
  the session's root thread, so a fresh lane's first `cci post` works. A
  session bound to no drive takes `CLAUDE_LONG_RUNNING_DRIVE` when it is set.

## [0.30.1] - 2026-10-27

### Fixed

- `--reader` on `cci tail`, `cci watch`, `cci grep`, `cci state` and the
  channel delivers a record posted `--to all` as a broadcast, subject to
  `--kind`, `--lane` and `--topic`. It used to deliver only records with an
  empty `to` list, so a lane reading with `--reader` missed the root's
  drive-wide rulings. `--to` is unchanged.

## [0.30.0] - 2026-10-27

### Changed

- `cci tail`, `cci watch`, the channel, the hooks, `cci digest` and `cci state`
  name a record's first eight recipients and count the rest, as
  `-> a,b,c,d,e,f,g,h,+45 more`. A record sent to 50 lanes used to fill the
  hook's 400-character line with names and show none of its text. `cci show`
  and `cci grep` still print every recipient.

## [0.29.0] - 2026-10-27

### Changed

- `cci tail`, `cci watch`, the channel, the hooks, `cci digest` and `cci state`
  leave out a record's `[topic]` when its text already carries that topic as a
  word, and a `pr#N` or `pr:owner/repo#N` ref when its text already names that
  pull request. A line the view clips keeps every field, since the clip may
  cut the word that named it. `cci show` and `cci grep` still print every field.
- After a session's first full list of open asks, the `PostToolUse` hook names
  a changed list by seq, lane and age instead of printing each ask's text
  again; each ask's text was already delivered as its own record. A session
  start or compaction lists them in full.

## [0.28.0] - 2026-10-27

### Changed

- A root thread whose window runs the cci channel gets each record addressed
  to it once. The channel moves the `PostToolUse` hook's lane cursor past
  what it delivered, and skips a record the hook delivered first. A window
  launched without `--channels plugin:cc-inbox@...` keeps both paths as before.
- The hook lists the open asks a lane owes only when that set changed since
  its last list, and otherwise prints one line counting them. The first
  delivery after a session start or compaction lists them in full.
- A channel tag's body drops the `#seq time KIND lane` header the tag's
  attributes already carry, and keeps the time and the rest of the line.
- Text `cci tail` cuts a record a later one resolved to 160 characters and
  counts the cut lines in a trailer; `--all` prints them whole.

### Fixed

- `cci post` without `--lane` refused with `required flag(s) "lane" not set`.
  It now posts as the thread's bound lane, else its channel subscription's
  reader, else `root` for a drive's root session.

## [0.27.2] - 2026-10-26

### Fixed

- A teammate that took over a lane got the lane's whole history replayed by
  the `PostToolUse` hook: a teammate shares its root session's window, so its
  lane cursor started when the root session started. The cursor now starts at
  the later of the window's start and the teammate's spawn, read from the
  harness's agent metadata, so a record posted between spawn and the first
  tool call still arrives.
- The hook no longer delivers records a later record has closed, under the
  store's own closure rule (`--resolves`, `answer --re`, a matching topic),
  and still moves the cursor past them. A page whose records were all closed
  keeps its resume trailer, which says "up to N more" while closed records
  are skipped. `cci tail` itself still prints closed records.

## [0.27.1] - 2026-10-26

### Fixed

- `cci claim --for <lane>@<drive>` refused a teammate that had not posted yet
  with "no live session on this machine is bound as lane": a teammate is
  announced at spawn under its agent name, but its thread takes a lane only on
  its first `cci post`. When no session is bound as the lane, `--for` now
  takes the one live teammate named that lane with no lane yet, whose session
  is unbound or in that drive, binds it to the lane the way `cci post` does,
  and claims for it. A root thread or a teammate of a session in another
  drive is never adopted.

## [0.27.0] - 2026-10-26

### Added

- `cci show SEQ...` prints records whole, in the order named: each record's
  line with no width cut, then the full text a record over 400 characters
  spilled to its blob, or the body file it was posted with. A lane that had
  only a record id used to page `cci tail --since` with a wide `--width`,
  grep for the line, and `cat` the blob by hand.

## [0.26.0] - 2026-10-26

### Fixed

- `cci who --pr` ran for minutes at full CPU on a large store and never
  answered: the claims it derives from records paired every session with
  every record before filtering by the pull request. Each derived branch now
  filters the record's refs first, and a new `sessions(origin, drive, lane)`
  index answers the session lookup. On a store of 117k records it answers in
  about 0.2 s. The store schema moves to 14, so mesh nodes on different
  versions refuse each other's changes until both upgrade.
- A pull request opened by a lane's subagent was recorded under the
  subagent's agent id. `cci claim --rel opened` now posts the `opened` record
  as the lane that owns the thread: the thread itself when it is bound as a
  lane, else the nearest bound agent that spawned it, up to its session's
  root thread. The `PostToolUse` hook records each subagent's parent from the
  harness metadata. `cci who --pr` lists a lane subagent's claim under the
  lane's thread.
- `cci claim --for <lane>@<drive> --pr N` also posts a `claim` record as that
  lane, reading `claim: <lane> owns #N`, so a reader of the drive's records
  learns the owner by its lane name.
- A bare `--pr N` matched only refs stored as that bare number, missing every
  `owner/repo#N` ref. It now matches N in any repository, in `tail`, `grep`,
  `digest`, `state` and `who`.
- A post with another `--lane` rebound the posting thread to that lane, so it
  started receiving that lane's mail. A thread's lane is now set once, by its
  first post as itself; `cci drive use` still resets it.

## [0.25.0] - 2026-10-26

### Fixed

- `cci post --resolves` with several seqs wrote one full copy of the record
  per seq, so a three-seq closure woke every addressee three times. The post
  now writes one record that resolves every seq it names: `tail` marks each
  closed record `[RESOLVED #<seq>]` with that one seq, the open sections of
  `digest` and the stop hook drop all of them, and the record's line reads
  `resolves #<a>,#<b>,#<c>`. The extra seqs live in the record's
  `also_resolves` list and replicate across the mesh as pointers, so the mesh wire format moves to
  3 and nodes on different versions refuse each other's changes until both
  upgrade. A repeated `--re` still writes one record per seq.
- Text over 400 characters spilled to a blob and the record kept only its
  first line, so a one-paragraph post arrived as its first clause. The record
  now carries the first 400 characters, whitespace collapsed and cut at a
  word with `…`, followed by the blob path.

## [0.24.2] - 2026-10-25

### Fixed

- `cci post` with a repeated `--resolves` or `--re` kept only the last seq
  and dropped the rest without a word, so a sweep that passed 40
  `--resolves` closed one record. Both flags now accumulate, and a comma list
  works too: the post writes one record per seq, with the same text, and
  prints each record's seq. An `--id` becomes `<id>#<seq>` on each record so
  a repost stays idempotent. A post that repeats both flags is refused.

## [0.24.1] - 2026-10-25

### Fixed

- The open-asks section and the `Stop` block count only records the lane
  owes: an ask, a blocker, or a decide ending in a question mark, whose first
  addressee is the lane and whose author is another lane. A status decide
  that copies the root, or an ask that names the root after the lane that
  owes it, no longer counts. One drive's root had 415 such records listed.
- The `Stop` hook's first run in a thread records the drive's head and blocks
  nothing, so records posted before the hook was installed never block a
  stop.

## [0.24.0] - 2026-10-25

### Added

- A lane's `PostToolUse` delivery leads with the asks, decides and blockers
  addressed to it that nothing has answered or resolved, newest first, five
  at most, each with its age in minutes. An ask posted now no longer waits
  behind hours of older records replayed three at a time from a lagging
  cursor.
- A `Stop` hook blocks the stop once when an ask, decide or blocker
  addressed to the lane has waited ten minutes unanswered. Its reason names
  the newest such record, its lane and age, and `cci post --resolves <seq>`.
  Each record blocks one stop; the next stop passes.

### Fixed

- `cci subscribe` and `cci unsubscribe` refuse to run in a subagent or
  teammate. The subscription belongs to the Claude Code window, which its
  in-process agents share, so a teammate subscribing as its own lane replaced
  the root's subscription. The root then received only that lane's records
  and broadcasts, and asks addressed to it stopped arriving for hours.

## [0.23.0] - 2026-10-22

### Added

- `cci claim --for <lane>@<drive>` and `cci unclaim --for` act on the claims
  of the one live local session bound as that lane in that drive, instead of
  the caller's. A desk or runner process that reads a lane's `release --build`
  record can now claim the build for the lane, so `cci who --build` names the
  lane that started it rather than the runner. The flag refuses a lane with
  no live session or with more than one, and needs no session of the caller's.

## [0.22.2] - 2026-10-19

### Fixed

- A record filter's `--pr owner/repo#N` now also matches records that name
  the pull request as the bare number `N`, which says nothing about its repo.
  A reader can ask for one repo's pull request with one query and still find
  the records written before their posters qualified them, as `ccx vcs ship`'s
  `cci claim` now does. A bare `--pr N` still matches only the bare number.

## [0.22.1] - 2026-10-19

### Fixed

- `cci post` from a Codex session now binds the session's lane, as it
  already did for Claude Code. It found the session only through a Claude
  ancestor, so a Codex worker's posts left it unbound. `cci claim --rel opened`,
  which `ccx vcs ship` runs, then found no binding and posted no `opened`
  record. The long-running incident clock never saw a Codex fix lane's PR and
  launched an Opus fallback for a PR that already existed.

## [0.22.0] - 2026-10-18

### Added

- A root can message a running Workflow tool agent. The session hooks now name
  each workflow agent by its label and announce it with role `workflow`, so
  `cci who --role workflow` lists them. A subagent bound to no lane gets the
  records addressed to its peer address in its next tool result, with no
  `cci subscribe`. `cci post --to peer:<agent id or label>` reaches one of the
  posting session's own agents. Before, a workflow agent had no name in the
  registry and never received records, and `SendMessage` by its id resumed a
  second copy of it.

### Changed

- `--to peer:` resolves among live sessions and agents first, then among
  ended ones, so a label reused by a finished workflow run still names the
  running agent.

## [0.21.2] - 2026-10-18

### Fixed

- `cci post --path /dev/stdin` now keeps the body. Before, the record stored
  `/dev/stdin` as its path, so a reader opening it got its own stdin and the
  body was gone; with `--text`, cci never read stdin at all. `--path -` failed
  with no such file. cci now reads `-` as stdin, copies `-`, `/dev/stdin`,
  process substitutions, and any other path that is not a regular file to a
  file under `$CCI_HOME/blobs`, and stores that file's path. A regular file's
  path is stored as an absolute path, a missing file is refused even with
  `--text`, and an empty body is refused.

## [0.21.1] - 2026-10-17

### Fixed

- Deciding which asks, decisions, holds, incidents, blockers, and defects are
  still open no longer checks every opener against every closer. Before, each
  read compared each opener in the drive with each closer, so one drive with
  5,277 openers and 7,010 closers ran up to 36,991,770 pair checks per read. A
  digest from the daemon or the session hook ran two of those reads, one for
  open items and one for untracked holds, and marking the open records among
  printed lines ran a third. cci now indexes the drive's closers once per read
  by the record they reply to, their topic, their stacks, and their targets,
  and checks each opener only against the closers it shares one of those with.
  The same records count as open as before.

## [0.21.0] - 2026-10-17

### Changed

- The mesh `records` item fires only for `inbox.db` and `inbox.db-wal`.
  Before, synckitd watched the whole cci home recursively, so every blob,
  subscription, relay, or `mesh/revisions.json` write woke the item and cost
  a `List` call, and its 30-second sweep listed every file in `blobs`, 9,913
  on one host. cci now declares the home with synckit's `watch_names`, which
  synckitd 0.42.0 and newer honor by watching the home without its
  subdirectories. An older synckitd ignores the field and keeps the recursive
  watch, so cci and synckitd can upgrade in either order.
- A first `cci mesh enable` registers a 5s watch debounce instead of 1s.

### Fixed

- `cci mesh enable` keeps the watch debounce `synckitd` already has registered
  for `cci`. Before, every run wrote a fresh manifest with a 1s debounce and
  `synckitd register` replaced the registered copy, so a debounce tuned in
  `~/.config/synckit/manifests/cci.json` reset to 1s. A registered manifest
  that fails to load now stops the command instead of being overwritten.
  Every Mac still runs `cci mesh enable` after upgrading, as the fingerprint
  entry that follows requires, and that run no longer resets a tuned debounce.
- The mesh schema fingerprint covers the wire format of a change as well as
  the store migrations. v0.17.0 started sending repo-qualified pull requests
  in `refs.prs` as `owner/repo#N` strings and kept the fingerprint, so a peer
  running an older binary accepted the change and then failed to decode it
  with `json: cannot unmarshal string into Go struct field Refs.rows.refs.prs
  of type int`. A peer on another wire format is refused at the fingerprint
  check instead. Every Mac must run `cci mesh enable` after upgrading, so synckitd
  registers the new fingerprint. Until both ends of a host pair upgrade, that
  pair's lane sits in synckit's backoff with the schema-mismatch error; the
  sender keeps the change staged, so nothing is lost.

## [0.20.7] - 2026-10-16

### Changed

- The `--pr` refusal from 0.20.6 applies to `hold` records only. `blocker`
  and `blocked` records cite record sequence numbers as `#<seq>` all the time,
  and those look like pull request numbers, so refusing them stalled lanes.
  A hold that names `#<number>` without a matching `--pr` is still refused.

## [0.20.6] - 2026-10-16

### Changed

- `cci post` refuses a `hold`, `blocker`, or `blocked` record whose text or
  topic names `#<number>` without a matching `--pr <number>`. Merge-queue
  guards bind a hold through the record's pull request refs, so a hold that
  named its PR only in prose went unseen. The error names each missing
  `--pr`. A record that names no PR still posts as before.

## [0.20.5] - 2026-10-16

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

[Unreleased]: https://github.com/yasyf/cc-inbox/compare/v0.36.0...HEAD
[0.36.0]: https://github.com/yasyf/cc-inbox/compare/v0.35.0...v0.36.0
[0.35.0]: https://github.com/yasyf/cc-inbox/compare/v0.34.0...v0.35.0
[0.34.0]: https://github.com/yasyf/cc-inbox/compare/v0.33.0...v0.34.0
[0.33.0]: https://github.com/yasyf/cc-inbox/compare/v0.32.2...v0.33.0
[0.32.2]: https://github.com/yasyf/cc-inbox/compare/v0.32.1...v0.32.2
[0.32.1]: https://github.com/yasyf/cc-inbox/compare/v0.32.0...v0.32.1
[0.32.0]: https://github.com/yasyf/cc-inbox/compare/v0.31.0...v0.32.0
[0.31.0]: https://github.com/yasyf/cc-inbox/compare/v0.30.3...v0.31.0
[0.30.3]: https://github.com/yasyf/cc-inbox/compare/v0.30.2...v0.30.3
[0.30.2]: https://github.com/yasyf/cc-inbox/compare/v0.30.1...v0.30.2
[0.30.1]: https://github.com/yasyf/cc-inbox/compare/v0.30.0...v0.30.1
[0.30.0]: https://github.com/yasyf/cc-inbox/compare/v0.29.0...v0.30.0
[0.29.0]: https://github.com/yasyf/cc-inbox/compare/v0.28.0...v0.29.0
[0.28.0]: https://github.com/yasyf/cc-inbox/compare/v0.27.2...v0.28.0
[0.27.2]: https://github.com/yasyf/cc-inbox/compare/v0.27.1...v0.27.2
[0.27.1]: https://github.com/yasyf/cc-inbox/compare/v0.27.0...v0.27.1
[0.27.0]: https://github.com/yasyf/cc-inbox/compare/v0.26.0...v0.27.0
[0.26.0]: https://github.com/yasyf/cc-inbox/compare/v0.25.0...v0.26.0
[0.25.0]: https://github.com/yasyf/cc-inbox/compare/v0.24.2...v0.25.0
[0.24.2]: https://github.com/yasyf/cc-inbox/compare/v0.24.1...v0.24.2
[0.24.1]: https://github.com/yasyf/cc-inbox/compare/v0.24.0...v0.24.1
[0.24.0]: https://github.com/yasyf/cc-inbox/compare/v0.23.0...v0.24.0
[0.23.0]: https://github.com/yasyf/cc-inbox/compare/v0.22.2...v0.23.0
[0.22.2]: https://github.com/yasyf/cc-inbox/compare/v0.22.1...v0.22.2
[0.22.1]: https://github.com/yasyf/cc-inbox/compare/v0.22.0...v0.22.1
[0.22.0]: https://github.com/yasyf/cc-inbox/compare/v0.21.2...v0.22.0
[0.21.2]: https://github.com/yasyf/cc-inbox/compare/v0.21.1...v0.21.2
[0.21.1]: https://github.com/yasyf/cc-inbox/compare/v0.21.0...v0.21.1
[0.21.0]: https://github.com/yasyf/cc-inbox/compare/v0.20.7...v0.21.0
[0.20.7]: https://github.com/yasyf/cc-inbox/compare/v0.20.6...v0.20.7
[0.20.6]: https://github.com/yasyf/cc-inbox/compare/v0.20.5...v0.20.6
[0.20.5]: https://github.com/yasyf/cc-inbox/compare/v0.20.4...v0.20.5
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
