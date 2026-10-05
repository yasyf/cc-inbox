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
decisions, runbooks, and design docs stay in `cc-notes`.

---

## Use cases

### Resume after compaction without rereading the inbox

A root reading a markdown tail after every conversation compaction sees the same
lines again. `cci tail` saves what it printed under the current session's cursor.

Inside Claude Code, bind the root to a drive. `cci drive use` requires
`CLAUDE_CODE_SESSION_ID`; keep the value supplied by the session.

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

Only the new record appears after the next post; another read prints nothing.
The cursor survives conversation compaction and advances only through printed
records. A named cursor with no saved position reads the past hour.
An explicit `--since` reads from that point and leaves the cursor untouched,
even when `--cursor` is also set.

Lanes use their own cursor, such as `cci tail --cursor api`, so their reads do not
advance the root's position.

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

### Move a drive off markdown inbox files

Existing desks may still append to inbox files while the root moves to `cci`.
Import a file once to register it, then read its records through a cursor:

```console
$ printf '%s\n' 'GO root Deploy after the checks pass.' > legacy.md
$ cci import legacy.md --drive migration
<cwd>/legacy.md: 1 entries, 1 new
$ cci tail --drive migration --cursor root
#1 <time> GO root Deploy after the checks pass.
```

The plugin's `PostToolUse` hook imports new content after a Bash, Write, Edit, or
`MultiEdit` call changes a registered file. This shell run invokes that same hook
entry point after an append:

```console
$ printf '%s\n' 'STATE api Checks passed.' >> legacy.md
$ cci hook post-tool
$ cci tail --drive migration --cursor root
#2 <time> STATE api Checks passed.
$ cci import legacy.md --drive migration
<cwd>/legacy.md: 0 entries, 0 new
```

Import remembers file offsets and deduplicates source lines across files. A file
that shrinks is reread from the start. At cutover, change lane instructions to
post records directly and stop appending to the markdown files.

---

## Commands

Use `cci --help` for the command list and `cci post --help` for posting options.
Commands that select a drive accept `--drive` or use the current session binding.

| Command | Behavior |
| --- | --- |
| `cci post` | Append one typed record. Requires lane, kind, and text; prints its sequence number. |
| `cci tail` | Read after a saved cursor, bounded by bytes. The default cursor is `CLAUDE_CODE_SESSION_ID`. |
| `cci watch` | Stream matching records. Polls once a second and exits after 29 minutes by default. |
| `cci digest` | Summarize the last 24 hours by default with counts, open items, and the latest record per lane. |
| `cci grep` | Search text with a case-insensitive regular expression, newest first, including expired records. |
| `cci import` | Import markdown files incrementally. Unrecognized lines become `note` records. |
| `cci compact` | Fold records older than 48 hours by default into daily digests, then delete the folded rows. Still-open items remain. |
| `cci drive use` | Bind the current Claude Code session to a drive. `--root` enables owner-prompt capture. |
| `cci drive ls` | List drives by their most recent activity. |
| `cci serve` | Serve the read-only HTTP API. |
| `cci hook` | Run a plugin hook entry point. |

| Read option | Commands | Behavior |
| --- | --- | --- |
| `--to <lane>` | `tail`, `watch`, `grep` | Keep only records whose `to` list contains the lane. `--lane` filters the posting lane instead. |
| `--since <point>` | `tail` | Read from a sequence number, duration, or RFC 3339 time without reading or advancing the cursor. A sequence number selects records after that number. |

The digest lists open asks and decisions, holds, and incidents from its window.
It counts older open items on one line. Its latest-per-lane section includes any
kind. Use a longer time window or a targeted read to inspect older records.

Digest open-item tracking and compaction's keep-open rule skip records whose
source is `import:<file>`. Imported markdown inbox lines lack the
`--re`/`--topic` pairing needed to close them.

## Kinds and pairing

Pair records within a drive using a shared `--topic` or a closing record's
`--re` pointing to the opener's sequence number. The closing record must come
after the opener. TTL means time to live; `--ttl` overrides the default.

| Kind | Default TTL | Pairing or requirement |
| --- | --- | --- |
| `ask`, `decide` | None | Open until `answer`, `go`, or `withdraw`. |
| `answer`, `withdraw` | None | Close `ask` or `decide`; require `--re` or `--topic`. |
| `go` | None | Closes a matching `ask` or `decide`. |
| `hold` | None | Open until `lift`. |
| `lift` | None | Closes `hold`; requires `--re` or `--topic`. |
| `incident` | None | Open until `fix-live` or `done`. |
| `fix-live`, `done` | None | Close a matching `incident`. |
| `opened`, `landed` | None | Require `--pr`. |
| `owner`, `mechanism`, `defect`, `correction`, `release`, `applied`, `handoff`, `head`, `contract`, `blocker` | None | Unpaired. |
| `digest` | None | Written only by compaction. |
| `claim`, `note`, `state`, `matrix`, `report` | 24 hours | No automatic pairing. |

Posts require a known kind. A duplicate with the same drive, lane, kind, and
whitespace-normalized text within ten minutes returns the existing sequence
number. Refs and pairing fields do not distinguish duplicates.

## Record JSON

`post`, `tail`, `watch`, and `grep` can emit records with `--json`. The following
record was captured from a post and indented for readability:

```json
{
  "seq": 2,
  "drive": "demo",
  "lane": "root",
  "kind": "answer",
  "at": "2026-10-05T06:02:59.589312Z",
  "text": "Proceed after review.",
  "topic": "rollout",
  "to": ["api"],
  "re": 1,
  "refs": {
    "path": "rollout.txt",
    "ccn": "ruling-42",
    "pr": 42,
    "build": "https://example.com/build/42",
    "url": "https://example.com/review/42"
  },
  "fields": {
    "stack": [41, 42],
    "env": "staging",
    "counts": {"checks": 3}
  },
  "expires_at": "2026-10-05T12:02:59.589312Z",
  "source": "post"
}
```

`seq` is monotonic across the machine's store, while `at` and `expires_at` use
RFC 3339 timestamps. Unset optional values are omitted; `refs` and `fields` remain
objects, with `pr` and `build` only under `refs`.

`source` is `post`, `hook`, `import:<file>`, or `compact`. Record reads emit one
JSON object per line; `digest --json` emits one summary object with counts,
open-item arrays, `latest`, `older_open`, and `max_seq`.

---

## HTTP endpoints

`cci serve` exposes these read-only endpoints on `127.0.0.1:7377` by default;
`--addr` changes the listener.

| Endpoint | Response |
| --- | --- |
| `GET /v1/drives` | Array of drives with record counts and latest activity. |
| `GET /v1/records?drive=D` | Array of records. Optional `kind` and `lane` filters repeat; `to=<lane>` keeps only records whose `to` list contains the lane. `since` is a sequence number, `since_time` is an RFC 3339 timestamp, and `expired=1` includes expired records. `limit` defaults to 100 and caps at 500. |
| `GET /v1/digest?drive=D` | Digest for the last 24 hours. Set `since_time` to an RFC 3339 timestamp to change the window. |
| `GET /v1/stream?drive=D` | Server-sent events with a record JSON payload and sequence number as the event ID. Starts at the current head; pass `since` to resume after a sequence number. `to=<lane>` keeps only records whose `to` list contains the lane. |

## Plugin hooks

Each entry invokes the plugin's `bin/cci`;
[hooks.json](plugin/hooks/hooks.json) sets the event matchers and timeouts.

| Claude Code event | Entry point | Behavior |
| --- | --- | --- |
| `SessionStart` | `cci hook session-start` | Inject the bound drive's digest and unseen session tail into model context, including after conversation compaction. |
| `UserPromptSubmit` | `cci hook prompt` | In a session bound with `--root`, record ordinary user prompts as `owner` records. Long prompts get a blob ref; slash commands and prompts starting with `<` are skipped. |
| `PostToolUse` | `cci hook post-tool` | After Bash, Write, Edit, or `MultiEdit`, refresh registered imports whose file size changed. |

`SessionStart` and `UserPromptSubmit` read the session ID from the hook payload
and stay silent for unbound sessions. `PostToolUse` refreshes registered imports
in the store, independent of the current session's binding.

## Store, budgets, and caps

The local store is `~/.cc-inbox/inbox.db`. Set `CCI_HOME` to move the whole store
directory, including cursors, bindings, import offsets, and `blobs/`. Concurrent
sessions write to the same SQLite store without a daemon.

| Surface | Limit |
| --- | --- |
| Posted text | 400 characters. Put longer bodies in a file and attach it with `--path`. |
| Imported text and owner prompts | Truncated to 400 characters, with the original body saved under `blobs/` and referenced by path. |
| `tail`, `grep`, and text `digest` | Default 4,000-byte budget, capped at 16,000 bytes. Output stops at a whole line. |
| `tail` | At most 500 records per call. Repeat the same cursor read to continue. |
| Text `watch` | Each line is capped at 600 characters. Output continues for the watch lifetime; `--budget` does not limit it. |
| JSON `watch` and `digest` | No byte-budget cap. JSON watch records are not clipped. |
| Text digest sections | Up to 8 open asks, 6 holds, 6 incidents, and 15 latest lane records, subject to the byte budget. |
| `SessionStart` context | Digest budget of 2,500 bytes and tail budget of 1,500 bytes. |

Capped text tail and grep reads report omitted records; a capped text digest
reports truncation. Their JSON forms omit that trailer.

Tail and watch reads hide expired records; `grep` includes them. Compaction also
folds old records that have no expiry into daily counts, preserving still-open
items.

Early, built for one long-running drive.
