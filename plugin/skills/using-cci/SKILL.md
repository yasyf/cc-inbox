---
name: using-cci
description: Load when posting or reading drive coordination records, inbox lines, lane status, GO lines, holds, or asks; when resuming a root or lane after compaction; when watching a drive with a Monitor; or when replacing appends to inbox markdown files or ephemeral cc-notes status with cci. Covers choosing a kind, pairing replies and closures, attaching references, reading with cursors, and keeping durable knowledge in cc-notes.
---

# Using cci

Put drive coordination in `cci`. Keep durable rulings, decisions, runbooks, and
design docs in `cc-notes`. A `decide` record requests a drive decision; the durable
decision belongs in `cc-notes` and can be linked with `--ccn`.

Use the drive and lane names from the task. Pass `--drive` explicitly or bind the
current Claude Code session. Keep its existing `CLAUDE_CODE_SESSION_ID`. A root
binding also enables owner-prompt capture through the plugin:

```bash
cci drive use release-demo --root
```

## Post one event

Write one record per event, with text under 400 characters. Use `state` for
current status, `ask` for a question, `hold` for
a stop, `go` for permission, and `opened` or `landed` for a pull request event.

Give related events a stable `--topic`. An answer or closure uses that topic or
`--re` with the opener's returned sequence number. `answer`, `lift`, and
`withdraw` require one of those links. This ask returns its sequence number:

```bash
cci post --drive demo --lane api --kind ask --topic rollout --text 'Approve the rollout?'
```

Use `--pr` for pull request references. `opened` and `landed` require it:

```bash
cci post --drive demo --lane api --kind opened --pr 42 --text 'Fix ready for review.'
```

Put a long body in a file and pass `--path` with a short summary:

```bash
printf '%s\n' 'Deployment plan and review evidence.' > rollout.txt
cci post --drive demo --lane api --kind report --text 'Review evidence attached.' --path rollout.txt
```

Use `--to` to address lanes. Keep `--pr`, `--build`, `--ccn`, `--url`, and `--path`
in references; structured fields are stack, environment, and named counts.
Repeating the same drive, lane, kind, and normalized text within ten minutes
returns the original record, even if its references differ.

## Read without repeating context

Give each desk its own cursor. A desk reads what is addressed to it with
`cci tail --cursor <desk> --to <desk>`. For the bound drive, the `api` desk reads:

```bash
cci tail --cursor api --to api
```

On `tail`, `watch`, and `grep`, `--to <lane>` keeps only records whose `to` list
contains that lane. `--lane` selects the posting lane.

The root uses the session default:

```bash
cci tail
```

A tail cursor with no saved position reads the past hour. It advances only
through printed records. If the read is capped, repeat the same command with the
same drive, cursor, and filters. The plugin's `SessionStart` hook shares the root's
session cursor and injects the digest plus unseen records after compaction.
An explicit `--since` reads from that point and leaves the cursor untouched,
even when `--cursor` is also set. `cci tail --cursor api --to api --since 0`
replays records addressed to `api` without changing its position.

Read the drive summary or search a specific issue:

```bash
cci digest
cci grep 'checks|review'
```

The digest defaults to 24 hours. It lists open asks and decisions, holds, and
incidents from that window, then counts older open items on one line. `grep`
searches newest first and includes expired records. Tail, grep, and text digest
default to 4,000 bytes, capped at 16,000. Long bodies stay behind their file refs.

## Watch GO lines with a Monitor

Run the watch as the Monitor's command:

```bash
cci watch --drive monitor-demo --kind go --cursor monitor
```

The watch polls once a second and exits after 29 minutes. Re-arm the same command
with the same cursor. Without a saved cursor or explicit time window, its first
run starts at the current head. Use a dedicated cursor for each filtered watch.
Text lines cap at 600 characters; the watch has no total byte budget.

## Choose a kind

Pairing requires a later closing record in the same drive, using the same topic
or `re` pointing to the opener. TTL means time to live; `--ttl` overrides defaults.

| Kinds | Default TTL | Pairing or requirement |
| --- | --- | --- |
| `ask`, `decide` | None | Open until `answer`, `go`, or `withdraw`. |
| `answer`, `withdraw` | None | Close asks or decisions; require `--re` or `--topic`. |
| `go` | None | Closes a matching ask or decision. |
| `hold` / `lift` | None | Lift closes hold; lift requires `--re` or `--topic`. |
| `incident` / `fix-live`, `done` | None | Fix-live or done closes the incident. |
| `opened`, `landed` | None | Require `--pr`. |
| `owner`, `mechanism`, `defect`, `correction`, `release`, `applied`, `handoff`, `head`, `contract`, `blocker` | None | Unpaired. |
| `digest` | None | Written only by compaction. |
| `claim`, `note`, `state`, `matrix`, `report` | 24 hours | No automatic pairing. |

Use `cci post` for new coordination. During a markdown cutover, `cci import`
registers old inbox files and the `PostToolUse` hook imports later appends. Once
lanes post directly, stop writing the markdown inboxes. Compaction folds old
records into daily counts while preserving still-open items.
