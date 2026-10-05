# cc-inbox

**Local, typed, append-only inbox CLI (cci) and Claude Code plugin for multi-agent drive coordination with bounded reads**

## Get started

Install the CLI with Homebrew:

```bash
brew install yasyf/tap/cci
```

Post an ask, answer it with a GO, and read the drive:

```bash
cci post --drive demo --lane planner --kind ask --topic rollout --text 'Ready to deploy?'
cci post --drive demo --lane root --kind go --topic rollout --text 'Deploy after the checks pass.'
cci tail --drive demo --since 0
```

```text
#1
#2
#1 <time> ASK planner [rollout] Ready to deploy?
#2 <time> GO root [rollout] Deploy after the checks pass.
```

Output was captured from separate empty stores for each example. Console times
are shown as `<time>`, session IDs as `<session>`, and the working directory as
`<cwd>`. Sequence numbers depend on the records already in your store.

<details>
<summary>Install with Go</summary>

```bash
go install github.com/yasyf/cc-inbox/cmd/cci@latest
```

</details>

Driving with an agent? Paste this:

```bash
claude plugin marketplace add yasyf/cc-inbox
claude plugin install cc-inbox@cc-inbox
```

The plugin includes the [using-cci skill](plugin/skills/using-cci/SKILL.md) and
[hooks](#plugin-hooks). Drive coordination goes to `cci`. Durable rulings,
runbooks, and design docs stay in `cc-notes`. Use `decision` for a call a lane
made that other lanes build on; use `decide` to request a decision.

---

## Use cases

### Resume after compaction without rereading the inbox

The root reads drive coordination through `cci digest`, `cci tail`, and
`cci watch`. `cci tail` saves what it printed under the current session's cursor,
so later reads skip those records after conversation compaction.

Inside Claude Code, bind the root to a drive. `cci drive use` requires
`CLAUDE_CODE_SESSION_ID`; keep the value supplied by the session. Each bind
sets that session's cursor to the drive's latest record, including when you
bind the same drive again. An empty drive starts at sequence 0. Subagents
share the parent's session ID, so give each lane a named cursor for its reads.

```console
$ cci drive use release-demo --root
session <session> -> release-demo
$ cci post --lane api --kind state --text 'Checks passed.'
#1
$ cci tail
#1 <time> STATE api Checks passed.
$ cci post --lane api --kind done --text 'Fix ready for review.'
#2
$ cci tail
#2 <time> DONE api Fix ready for review.
$ cci tail
```

After binding, tail starts with records posted after the bind. Only the new
record appears after the next post; another read prints nothing. The cursor
survives conversation compaction. Reads advance it only through printed records;
rebinding resets it to the drive's latest record. Named lane cursors are
unchanged, and one with no saved position reads the past hour.

An explicit `--since` accepts a sequence number, duration, local clock time
such as `09:00` or `9:00 AM` (the latest one at or before now), or RFC 3339
time, and leaves the cursor untouched, even when `--cursor` is also set. Use
`--since 0` to replay older records. `--kind` accepts repeated flags or
comma-separated kinds, such as `--kind hold,defect`.

Lanes read their deliveries with `cci tail --cursor api --reader api`, so their
reads do not advance the root's position. This delivers records addressed to
`api` and broadcasts from other lanes, excluding `api`'s own broadcasts.

### Watch only GO lines in a Monitor

A Monitor tailing a markdown file receives status chatter alongside permissions
to proceed. Filter by kind instead. Post two events, then run a two-second sample
that reads the last hour:

```bash
cci post --drive monitor-demo --lane api --kind state --text 'Checks passed.'
cci post --drive monitor-demo --lane root --kind go --topic rollout --text 'Deploy after the checks pass.'
cci watch --drive monitor-demo --kind go --cursor monitor --since 1h --for 2s
```

```text
#1
#2
#2 <time> GO root [rollout] Deploy after the checks pass.
```

The watch prints only the GO record. For a Monitor, omit `--for` to use the
29-minute lifetime. Re-arm with the same cursor and omit `--since` so the next
watch resumes after the last printed record. A watch without a saved cursor or
an explicit time window starts at the current head.

### Find why a stack cannot deploy

Attach the stack and target to both the problem and the work addressing it.
This keeps the dashboard's stack query useful as different lanes post updates:

```bash
cci post --drive stack-demo --lane walker --kind defect --stack api/plat-usw2-prod --target api --text 'Wave cycle blocks this stack.'
cci post --drive stack-demo --lane walker --kind blocked --stack api/plat-usw2-prod --target api --re 1 --text 'Platy cannot deploy this stack.'
cci post --drive stack-demo --lane wave-fix --kind opened --pr 30440 --stack api/plat-usw2-prod --target api --resolves 1 --text 'Fix ready for review.'
cci tail --drive stack-demo --stack api/plat-usw2-prod --since 0
```

```text
#1
#2
#3
#1 <time> DEFECT walker Wave cycle blocks this stack. stack:api/plat-usw2-prod target:api [RE #2] [RESOLVED #3]
#2 <time> BLOCKED walker re #1 Platy cannot deploy this stack. stack:api/plat-usw2-prod target:api
#3 <time> OPENED wave-fix Fix ready for review. pr#30440 stack:api/plat-usw2-prod target:api resolves #1
```

The `opened` record explicitly resolves the defect. The deployment block stays
open until a later record closes it. Once the stack can deploy, post:

```bash
cci post --drive stack-demo --lane walker --kind unblock --stack api/plat-usw2-prod --target api --re 2 --text 'Platy can deploy this stack.'
```

Add `--json` to the tail command to read each opener's computed `status`.

### Move a drive off markdown inbox files

Existing desks may still append to inbox files while the root moves to `cci`.
Import inbox files that lanes append to; the long-running runner's
`runner-state.md` is a rendered view rewritten in place, not an inbox.
Import a file once to register it, then read its records through a cursor:

```console
$ printf '%s\n' 'GO root Deploy after the checks pass.' > legacy.md
$ cci import legacy.md --drive migration
<cwd>/legacy.md: 1 entries, 1 new, 0 reparsed
$ cci tail --drive migration --cursor root
#1 <time> GO root Deploy after the checks pass.
```

The daemon refreshes registered imports every second. Run `cci serve` to start
or reuse it, or start any Claude Code session with the plugin installed.
Append another record:

```bash
cci serve
printf '%s\n' 'STATE api Checks passed.' >> legacy.md
```

After the next refresh, read the new record with the same cursor:

```bash
cci tail --drive migration --cursor root
```

`cci import` tracks offsets, inodes, and the parser version. It rereads files
from the start when they shrink or rotation replaces them, and deduplicates
source lines within a drive. After a parser version change, the next import
or daemon refresh reparses already-consumed lines, even if the file's size
and inode are unchanged. Matching records get updated parsed fields and
expiry based on their stored time. Their timestamps stay unchanged, and
records folded away by `cci compact` stay gone. The import output reports
the number reparsed.

Import dates stamps relative to the file's modification time. A stamp
more than five minutes after that time moves to the previous day. A one-day
forward correction must stay within the same limit. Out-of-order stamps
stay on their day.

The daemon also imports new `<inbox>.md.archive/*.md` files with the inbox's
drive and lane; direct archive imports default to the inbox name.

Import extracts all matching PR references (`#30440` or `/pull/30440`), Buildkite
build URLs, bare stack names such as `api/plat-usw2-prod`, and explicit
`stack:<project>/<env>` and `target:<name>` tokens. Repeated references appear
once per record. PR numbers must have four to six digits. Bare stack names
require region-shaped environments; ordinary paths such as `infra/lib` and
short environments such as `api/staging` are skipped. An explicit
`stack:api/staging` token supplies a stack ref without that region restriction.
Use lowercase letters, digits, and hyphens in explicit names; projects and
targets start with a letter, and environments start with a letter or digit.
Labels such as `tunnel/T`, `k8s/T`, and box names do not supply a stack ref.
Name the full stack, for example `stack:tunnel/tnt-usw2-1frg9c7`.

In keyed runner lines (`HH:MM KIND <key> <lane>: ...`), a `msg_<id>` key
keeps the worker as the lane. Other keys put the event under `runner` with
the subject lane as `topic`. When the subject is `runner:`, the first
`<lane>=ctx_` token supplies the topic. These runner event kinds do not
close native open items.

At cutover, change lane instructions to post records directly and stop appending
to the markdown files.

---

## Commands

Use `cci --help` for the command list and `cci post --help` for posting options.
Commands that select a drive accept `--drive` or use the current session binding.

| Command | Behavior |
| --- | --- |
| `cci post` | Append one typed record. Requires lane, kind, and text, given as `--text` or the one argument; prints its sequence number. |
| `cci tail` | Read after a saved cursor, bounded by bytes. The default cursor is `CLAUDE_CODE_SESSION_ID`; `drive use` sets it to the selected drive's latest record. `--since` reads an explicit window without changing the cursor. |
| `cci watch` | Stream matching records. Polls once a second and exits after 29 minutes by default. |
| `cci digest` | Summarize the last 24 hours by default with counts, open items, and the latest record per lane. |
| `cci grep` | Search whole rendered record lines literally and case-insensitively, newest first, including expired records. `--regex` enables regular expressions. |
| `cci state` | Show the latest `head`, `contract`, and `state` per lane and topic, skipping withdrawn records, within a byte budget. Text output names the drive when no records match. |
| `cci import` | Import markdown files incrementally. Unrecognized lines become `note` records. |
| `cci compact` | Fold records older than 48 hours by default into daily digests, then delete the folded rows. Still-open items remain. |
| `cci drive use` | Bind the current Claude Code session and reset its cursor to the drive's latest record in one transaction. `--root` enables owner-prompt capture. |
| `cci drive ls` | List drives by their most recent activity. |
| `cci serve` | Start or reuse the daemon, print its HTTP URL, and return. |
| `cci hook` | Run a plugin hook entry point. |

| Post option | Stored value or behavior |
| --- | --- |
| `--pr <number>`, `--build <URL-or-id>` | Repeat to populate `refs.prs` and `refs.builds`. |
| `--stack <project>/<env>`, `--target <name>` | Repeat to populate `refs.stacks` and `refs.targets`. Each stack requires two nonempty parts separated by one slash. |
| `--lane-ref <lane>` | Repeat to populate `refs.lanes`, the lanes the record is about. `--lane` identifies the writer. |
| `--path <file>`, `--ccn <id>`, `--url <URL>`, `--board <URL>` | Set `refs.path`, `refs.ccn`, `refs.url`, and `refs.board`. |
| `--env <environment>` | Repeat to populate `fields.envs`. |
| `--mode <mode>` | Set `fields.mode`: `platy`, `cli`, `manual`, or `walker`. |
| `--outcome <outcome>` | Set `fields.outcome`: `passed`, `failed`, `pending`, or `cancelled`. |
| `--commit <sha>` | Set `fields.commit` to the full commit SHA. |
| `--census '<JSON>'` | Set `fields.census`, for example `{"n":232,"denominator":293,"head":"<sha>","drift":0,"stacks_clean":["api/plat-usw2-prod"]}`. |
| `--count <name>=<integer>` | Set entries in `fields.counts`, for example `--count deletes=0`. |
| `--topic <key>`, `--re <seq>` | Group related events or link a reply to an earlier record. |
| `--resolves <seq>` | Must name an existing record in the same drive. Only a later record closes the open item, regardless of the new record's kind. |
| `--to <lane>` | Address one or more lanes. Use `--to owner` when the owner needs to act. |

| Read option | Commands | Behavior |
| --- | --- | --- |
| `--regex` | `grep` | Interpret the pattern as a regular expression; `a\|b` matches either alternative. The default matches the pattern literally. |
| `-i`, `--ignore-case` | `grep` | Match case-insensitively, the default; `--ignore-case=false` matches case exactly. |
| `--kind <kind>` | `tail`, `watch`, `grep` | Select kinds; repeat the flag or separate kinds with commas. |
| `--lane <lane>` | `tail`, `watch`, `grep`, `state` | Select posting lanes; repeat for multiple lanes. |
| `--topic <topic>` | `tail`, `watch`, `grep`, `state` | Select topics; repeat for multiple topics. |
| `--stack <project>/<env>` | `tail`, `watch`, `grep`, `state` | Match a member of `refs.stacks`. |
| `--target <name>` | `tail`, `watch`, `grep`, `state` | Match a member of `refs.targets`. |
| `--pr <number>` | `tail`, `watch`, `grep`, `state` | Match a member of `refs.prs`. |
| `--to <lane>` | `tail`, `watch`, `grep`, `state` | Keep only records whose `to` list contains the lane. |
| `--reader <lane>` | `tail`, `watch`, `grep`, `state` | Deliver records addressed to the reader regardless of kind, lane, or topic filters, plus other lanes' broadcasts that match those filters. Exclude the reader's own broadcasts. |
| `--since <point>` | `tail` | Read from a sequence number, duration, local clock time such as `9:00 AM`, or RFC 3339 time without reading or advancing the cursor. A sequence number selects records after that number. |
| `--budget <bytes>` | `tail`, `grep`, `state`, text `digest` | Bound output to whole lines; defaults to 6,144 bytes, or 32,000 for `digest`, and honors any larger value. |
| `--width <characters>` | `tail`, `watch`, text `digest` | Clip each rendered record line to this width. Records print whole by default. JSON ignores it. |

Broadcasts have an empty `to` list. With `--reader`, kind, lane, and topic filters
apply only to broadcasts; records addressed only to other lanes are excluded.
Stack, target, and PR filters apply to addressed records too. Each takes one
value per read; when combined, all must match.

Tail and watch text lines append reply marks after clipping, so the marks stay
visible. Grep matches the whole rendered record line: sequence number, time,
kind, lane, recipients, topic, text, and rendered refs. It searches all matching
records newest first, including expired records, and prints whole lines with
reply marks within the byte budget. Patterns are literal and case-insensitive
by default; `--regex` enables regular expressions such as `HOLD|DEFECT`.
When nothing matches, text output prints `no records on <drive> match; store
head #<seq>`, so an empty result reads differently from a stale store.

A reply names the record's sequence number with `--re`.
The marks are `[ANSWERED #12]`, `[WITHDRAWN #14]`,
`[LIFTED #n]`, `[DONE #n]`, `[GO #n]`, `[FIX-LIVE #n]`, or `[RE #n]` for other
reply kinds. A resolver adds `[RESOLVED #n]` to its target's line and
`resolves #n` to its own line. The first number identifies the resolver; the
second identifies the target. JSON records have no text marks.

Read `cci state` before asking a lane for its head, contract, or status. Publish
`head` on every push, `contract` for interfaces other lanes consume, and `state`
for lane status. The command selects the latest non-withdrawn record for each
kind, lane, and topic among `head`, `contract`, and `state`. Withdrawing one
with `--re` reveals its previous non-withdrawn record, if any. When no records
match, text output says `no head, contract, or state records on <drive>`.
An empty JSON read emits no records.

The digest groups open items from its window: asks and decision requests
(`decide`) in `open asks` (JSON `open_asks`), `blocker` and `blocked` records in
`open blockers` (`open_blockers`), and defects in `open defects` (`open_defects`).
Holds and incidents have their own sections. The digest counts older open
items on one line and selects each lane's latest record by time. Text
sections and the latest-per-lane list use sequence number order, newest
first. Use a longer window or a targeted read for older records.

Digest open-item tracking includes imported openers with a stack or target ref.
Imported `hold`, `defect`, `blocker`, and `blocked` records with either ref appear
in the digest's open sections and the dashboard's blocked view. Compaction
preserves tracked openers while open. Imported openers without either ref
remain untracked, with status `imported`. Text digest lists untracked imported
holds from its window on one `untracked holds` line after `open holds`, newest
first, so writers can add a `stack:<project>/<env>` or `target:<name>` ref.
JSON digest carries them as `untracked_holds`. A post with `--resolves <seq>`,
or a `lift` with `--re <seq>`, closes an untracked hold and drops it from that
line.

A record in the same drive closes a tracked open item by shared deployment
ref when it is later in recorded time, with sequence number breaking a tie.
It must be one of the opener's normal closers. A `hold` accepts `lift`;
`defect` accepts `fix-live` or `done`; `blocker` accepts `withdraw`, `answer`,
or `done`; `blocked` accepts `unblock`. Imported openers also accept
`fix-live`, `lift`, or `done`. A native hold still needs `lift`.

When both records name stacks, at least one stack must match. When either
names no stack, a shared target is enough. No matching topic or `--re` is
needed. A fix on `api/tnt-usw2-bbbb` does not close a defect on
`api/tnt-usw2-aaaa`, even when both name target `api`. Imports from older
archives cannot close newer regressions because closure compares recorded
time.

## Kinds and pairing

Pair records within a drive using a shared `--topic` or a closing record's
`--re` pointing to the opener's sequence number. Those links require a later
sequence number and use the table's kind pairs.

A shared deployment ref also closes a tracked opener with one of its normal
closers. Imported openers also accept `fix-live`, `lift`, or `done`. Ref
closure requires a later recorded time; sequence number breaks a tie.
When both records name stacks, at least one stack must match. When either
names no stack, a shared target is enough. TTL means time to live; `--ttl`
overrides the default.

Use `--resolves <seq>` on a later record of any kind to close a specific `ask`,
`decide`, `blocker`, `blocked`, `defect`, `hold`, or `incident`. The target must
already exist in the same drive; missing or cross-drive targets are rejected.
An `opened`, `landed`, `go`, or `fix-live` record can carry this link without a
matching topic or `--re`. Only a resolver with a later sequence closes the opener.
`matrix` is a status snapshot with a default expiry, not an open item.

| Kind | Default TTL | Pairing or requirement |
| --- | --- | --- |
| `ask`, `decide` | None | Open until `answer`, `go`, or `withdraw`. |
| `decision` | None | A call a lane made that others build on. Durable, with no default expiry; not an open item. |
| `blocker` | None | Open until `withdraw`, `answer`, or `done` with `--re` or the same `--topic`. |
| `blocked` | None | A stack or target cannot deploy. Open until `unblock` with `--re` or the same `--topic`, or an explicit resolver. |
| `unblock` | None | Closes a matching `blocked`; requires `--re`, `--topic`, or `--resolves`. |
| `answer`, `withdraw` | None | Close `ask`, `decide`, or `blocker`; require `--re`, `--topic`, or `--resolves`. A withdrawal can also retract another kind of record using `--re`. |
| `go` | None | Closes a matching `ask` or `decide`. |
| `hold` | None | Open until `lift`. |
| `lift` | None | Closes `hold`; requires `--re`, `--topic`, or `--resolves`. |
| `incident`, `defect` | None | Open until `fix-live`, `done`, or an explicit resolver. |
| `fix-live` | None | Closes a matching `incident` or `defect`. |
| `done` | None | Closes a matching `incident`, `defect`, or `blocker`. |
| `opened`, `landed` | None | Require `--pr`. |
| `review` | None | A rules-review verdict; use `--outcome` for the result. |
| `owner`, `mechanism`, `correction`, `release`, `applied`, `handoff`, `head`, `contract` | None | Unpaired. |
| `retro`, `posted`, `design`, `serving`, `ready`, `duplicate` | None | Unpaired. |
| `stand-down`, `not-ours`, `stopped`, `refuse`, `fail`, `recovered` | None | Unpaired. |
| `delete-list`, `skew`, `not-live`, `urgent` | None | Unpaired. |
| `digest` | None | Written only by compaction. |
| `claim`, `note`, `state`, `matrix`, `report` | 24 hours | No automatic pairing. |

`refused` is an alias for `refuse`; `failed` is an alias for `fail`.
Imported uppercase lead tokens map to their corresponding kinds. The 16 kinds
from `retro` through `urgent` above have no default expiry. They do not open
or close items through kind pairing.

`decide` requests a decision and stays open until closed. `decision` records a
call already made, and imported `DECISION` lines use this kind.

Posts require a known kind. A duplicate with the same drive, lane, kind, and
whitespace-normalized text within ten minutes returns the existing sequence
number. Refs and pairing fields do not distinguish duplicates.

## Record JSON

`post`, `tail`, `watch`, `grep`, and `state` can emit records with `--json`.
The following record was captured from a post and indented for readability:

```json
{
  "seq": 2,
  "drive": "demo",
  "lane": "root",
  "kind": "go",
  "at": "2026-10-05T08:04:02.913706Z",
  "text": "Proceed after review.",
  "topic": "rollout",
  "to": ["api"],
  "re": 1,
  "resolves": 1,
  "refs": {
    "path": "rollout.txt",
    "ccn": "ruling-42",
    "url": "https://example.com/review/42",
    "board": "https://example.com/board/release",
    "prs": [42, 43],
    "builds": ["https://example.com/build/42", "https://example.com/build/43"],
    "stacks": ["api/plat-usw2-prod"],
    "targets": ["api"],
    "lanes": ["wave-fix", "walker"]
  },
  "fields": {
    "envs": ["plat-usw2-prod", "plat-usw2-staging"],
    "mode": "platy",
    "outcome": "passed",
    "commit": "0123456789abcdef0123456789abcdef01234567",
    "census": {
      "n": 232,
      "denominator": 293,
      "head": "0123456789abcdef0123456789abcdef01234567",
      "drift": 0,
      "stacks_clean": ["api/plat-usw2-prod"]
    },
    "counts": {"checks": 3}
  },
  "expires_at": "2026-10-05T14:04:02.913706Z",
  "source": "post"
}
```

`seq` is monotonic across the machine's store, while `at` and `expires_at` use
RFC 3339 timestamps. Unset optional values are omitted; `refs` and `fields` remain
objects. When `census` is present, `n`, `denominator`, and `drift` remain integers
even when zero; `head` and `stacks_clean` are optional.

JSON reads from `tail`, `grep`, `watch`, and `/v1/records` add a computed `status`
to opener kinds: `open`, `closed`, or `imported`. Other kinds omit it. Imported
openers with a stack or target ref use `open` or `closed`; those without either
ref use `imported` and remain outside open-item tracking.
`post --json`, `/v1/lanes`, and `/v1/stream` return records without computed status.

Schema 3 upgrades existing stores on open: `refs.pr` becomes `refs.prs`,
`refs.build` becomes `refs.builds`, and the PR numbers in `fields.stack` merge
into `refs.prs` without duplicates. `fields.env` becomes `fields.envs`.
The old keys are removed. Deploy stack names live in `refs.stacks`; use repeated
`--pr` flags for a stack of pull requests.

`source` is `post`, `hook`, `import:<file>`, or `compact`. Record reads emit one
JSON object per line; `digest --json` emits one summary object with counts,
`open_asks` (asks and `decide`), `open_blockers` (`blocker` and `blocked`),
`open_defects`, `open_holds`, `open_incidents`, `latest`, `older_open`, and `max_seq`.

---

## HTTP endpoints

`cci serve` starts or reuses the daemon, prints `http://127.0.0.1:7377/v1`, and
returns. The daemon serves these read-only endpoints from its hot SQLite store.
The listener is fixed at `127.0.0.1:7377`.

| Endpoint | Response |
| --- | --- |
| `GET /v1/drives` | Array of drives with record counts and latest activity. |
| `GET /v1/records?drive=D` | Array of records with computed opener `status`. Accepts repeatable `kind`, `lane`, and `topic`, plus `stack=<project>/<env>`, `target=<name>`, and `pr=<number>`. `to=<lane>` selects addressed records; `reader=<lane>` selects deliveries. `since` is a sequence number, `since_time` is an RFC 3339 timestamp, and `expired=1` includes expired records. `limit` defaults to 100 and caps at 500. |
| `GET /v1/digest?drive=D` | Digest for the last 24 hours. Set `since_time` to an RFC 3339 timestamp to change the window. |
| `GET /v1/lanes?drive=D` | Select each lane's latest record by time and list the results by sequence number, newest first, for liveness. Includes expired records, excludes compaction digests, and returns at most 500 lanes. Use each record's `at` timestamp to measure age. |
| `GET /v1/stream?drive=D` | Server-sent events with a record JSON payload and sequence number as the event ID. Starts at the current head; pass `since` to resume after a sequence number. Accepts repeatable `kind`, `lane`, and `topic`, plus `to`, `reader`, `stack`, `target`, and `pr`. |

On both records and stream endpoints, `reader=<lane>` delivers records addressed
to that lane regardless of `kind`, `lane`, or `topic` filters, plus other lanes'
broadcasts that match those filters. The reader's own broadcasts are excluded.
Stack, target, and PR filters apply to every returned record and combine with
AND. Query `/v1/records?drive=D&stack=api/plat-usw2-prod` to read a stack's
problems and the records addressing them, including whether each opener is closed.

### Daemon

`daemonkit` manages `com.yasyf.cc-inbox` through launchd on macOS. Linux requires
the daemonkit supervisor for `Ensure`. It runs a copy of `cci` at
`~/.daemonkit/bin/com.yasyf.cc-inbox`, with state under
`~/.daemonkit/a/com.yasyf.cc-inbox`. It restarts on failure. When a different
`cci` build calls `Ensure` through `cci serve` or the `SessionStart` hook,
daemonkit drains and replaces the running daemon.

The daemon keeps one SQLite store open with a 256 MiB memory map and a 64 MiB
page cache. HTTP readers and the `digest` business operation reuse that store;
the `addr` operation returns the HTTP address. Each CLI process still writes
directly to SQLite's write-ahead log. CLI reads, including `tail`, `digest`,
`grep`, and `watch`, also stay direct and use the same reader code as the daemon.
Posting and CLI reads work without the daemon.

## Plugin hooks

Each entry invokes the plugin's `bin/cci`;
[hooks.json](plugin/hooks/hooks.json) sets the event matchers and timeouts.

| Claude Code event | Entry point | Behavior |
| --- | --- | --- |
| `SessionStart` | `cci hook session-start` | Inject the bound drive's digest and unseen session tail into model context, including after conversation compaction, then start or reuse the daemon. |
| `UserPromptSubmit` | `cci hook prompt` | In a session bound with `--root`, record ordinary user prompts as `owner` records. Long prompts get a blob ref; slash commands and prompts starting with `<` are skipped. |

`SessionStart` and `UserPromptSubmit` read the session ID from the hook payload
and emit no context or owner records for unbound sessions. `SessionStart` still
starts or reuses the daemon for unbound sessions. If that fails, the hook reports
a non-blocking error after any bound drive context has been written. The daemon
refreshes registered imports every second, independent of session bindings and
tool calls.

## Store, budgets, and caps

The local store is `~/.cc-inbox/inbox.db`. Set `CCI_HOME` to move the whole store
directory, including cursors, bindings, import offsets, and `blobs/`. Concurrent
sessions write to the same SQLite store without a daemon.

| Surface | Limit |
| --- | --- |
| Posted text | 400 characters. Put longer bodies in a file and attach it with `--path`. |
| Imported text and owner prompts | Truncated to 400 characters, with the original body saved under `blobs/` and referenced by path. |
| `tail`, `grep`, and `state` | Default 6,144-byte budget; `--budget` sets any other size. Output stops at a whole line. |
| Text `digest` | Default 32,000-byte budget, enough for every section at its record limit with whole records; `--budget` sets any other size. Output stops at a whole line. |
| Text record lines in `tail`, `watch`, and digest sections | Print whole records by default. `--width N` clips to N characters, including a final `…`. The limit excludes digest indentation and appended reply marks. |
| Text record lines in `grep` and `state` | Print whole records within the byte budget. |
| `tail` | At most 500 records per call. Repeat the same cursor read to continue. |
| Text `watch` | Output continues for the watch lifetime; `--budget` does not limit it. |
| JSON output | Record text is never clipped; `--width` has no effect. `tail`, `grep`, and `state` still apply their byte budgets. |
| JSON `watch` and `digest` | No byte-budget cap. |
| Text digest sections | Up to 8 asks and decision requests combined; 6 blockers and deployment blocks combined; 6 defects; 6 holds; 8 untracked holds on one line; 6 incidents; and 15 latest lane records, subject to the byte budget. |
| `SessionStart` context | Width 400 for both reads, with a digest budget of 2,500 bytes and a tail budget of 1,500 bytes. |

Wider record lines leave the byte budgets unchanged, so fewer records may fit.
Capped text tail and grep reads report omitted records; capped text digest
and state reads report truncation. Their JSON forms omit that trailer. Narrow
state output with `--lane` or `--topic`.

Tail and watch reads hide expired records; `grep` and `state` include them.
Compaction also folds old records that have no expiry into daily counts,
preserving still-open items.

Early, built for one long-running drive.
