---
name: using-cci
description: Load when posting or reading drive coordination records, inbox lines, lane status, GO lines, holds, asks, or blocked deploys; when resuming after compaction; when watching a drive with a Monitor; or when replacing markdown inbox appends or ephemeral cc-notes status with cci. Covers kinds, closures, stack and target references, cursors, and durable knowledge in cc-notes.
---

# Using cci

Put drive coordination in `cci`. Keep durable rulings, runbooks, and design docs
in `cc-notes`, linked with `--ccn`. Use `decision` for a call already made and
`decide` to request one. A decision has no default expiry and is not an open item.

Use the task's drive and lane names. Pass `--drive` or bind the current Claude
Code session. Keep its existing `CLAUDE_CODE_SESSION_ID`. A root binding also
enables owner-prompt capture through the plugin:

```bash
cci drive use release-demo --root
```

## Post one event

Write one record per event, with text under 400 characters. Use `state` for
status, `ask` for a question, `hold` for a stop, `go` for permission, and `opened`
or `landed` for a PR event. Use `review` with `--outcome` for a rules-review verdict.
Address lanes with `--to`; when an action needs the owner, use `--to owner`.

Give related events a stable `--topic`. Replies use that topic or `--re <seq>`.
`--resolves <seq>` must name an existing record in the same drive. Only a later
record closes an `ask`, `decide`, `blocker`, `blocked`, `defect`, `hold`, or `incident`.
`go`, `opened`, `landed`, and `fix-live` can all carry `--resolves`.
`answer`, `lift`, `withdraw`, and `unblock` require `--re`, `--topic`, or `--resolves`.

Every `defect`, `matrix`, `hold`, `go`, or `opened` record about a stack carries
`--stack <project>/<env>`, `--target <name>`, or both. Attach them to related records
too, so a stack or target query finds both the problem and the work addressing it.
`--stack` names a deploy stack, such as `api/plat-usw2-prod`. Use repeated `--pr`
flags for pull requests; `opened` and `landed` require at least one.

Use `blocked` when a stack or target cannot deploy. It remains open until
`unblock` names it with `--re` or the same `--topic`, or a resolver closes it.
Resolving a defect does not close a separate `blocked` record. For a new drive:

```bash
cci post --drive demo --lane walker --kind blocked --stack api/plat-usw2-prod --target api --text 'Platy cannot deploy: wave cycle.'
cci post --drive demo --lane root --kind ask --to owner --topic rollout --stack api/plat-usw2-prod --text 'Approve the repair?'
```

Use the returned sequence number when the stack can deploy again. If the block
above returned `#1`, close it with:

```bash
cci post --drive demo --lane walker --kind unblock --re 1 --stack api/plat-usw2-prod --target api --text 'Platy can deploy this stack.'
```

Repeat `--pr`, `--build`, `--stack`, `--target`, and `--lane-ref` to attach refs.
`--lane-ref` names lanes the record is about; `--lane` names its writer.
`--path`, `--ccn`, `--url`, and `--board` also set refs. Put long bodies in a file
and attach it with `--path` and a short summary.

Structured fields use repeatable `--env`, `--mode` (`platy`, `cli`, `manual`,
`walker`), `--outcome` (`passed`, `failed`, `pending`, `cancelled`), `--commit`,
`--census` JSON, and `--count name=integer`. Census carries `n`, `denominator`,
`head`, `drift`, and `stacks_clean`. PRs, builds, and deploy stacks live in refs.
Repeating the same drive, lane, kind, and normalized text within ten minutes
returns the original record, even if its references differ.

Publish `head` on every push, with the full commit SHA as text and the PR or
branch as topic. Publish `contract` for interfaces other lanes consume; withdraw
it with `--re` before changing the interface. Broadcast both by omitting `--to`.
Read `cci state` before asking for a head or contract. It returns the latest of
each per lane and topic, skipping withdrawn records.

## Read without repeating context

Give each lane its own cursor. For the bound drive, the `api` lane reads:

```bash
cci tail --cursor api --reader api
```

On `tail`, `watch`, `grep`, and `state`, `--reader` delivers records addressed
to the lane regardless of kind, lane, or topic filters, plus other lanes'
broadcasts matching those filters. Broadcasts have empty `to`; your own never
come back. Repeat `--topic` or `--lane` to select topics or posting lanes.
Use `--kind` on tail, watch, or grep to select broadcast kinds; `--to` selects
addressed records only. Stack, target, and PR filters apply to all deliveries.

The root reads drive coordination through `cci digest`, `cci tail`, and
`cci watch`. Use `cci tail` with the session's default cursor. A new cursor reads
the past hour and advances only through printed records. If capped, repeat
with the same drive, cursor, and filters. Explicit `--since` leaves the cursor
untouched, even with `--cursor`; use `--since 0` to replay.

`SessionStart` shares the root's cursor and injects the digest plus unseen
records after compaction. Both reads use a 400-character width, with a
2,500-byte digest budget and 1,500-byte tail budget.

Read a stack's problems and fixes, or search records about a target or PR:

```bash
cci tail --drive demo --stack api/plat-usw2-prod --since 0 --json
cci grep 'Platy' --drive demo --target api
cci tail --drive demo --pr 30440 --since 0
```

Read filters `--stack`, `--target`, and `--pr` each take one value and combine
with AND. JSON tail, grep, and watch reads add `status` to opener kinds: `open`,
`closed`, or `imported`. Text tail and watch reads append reply marks after
clipping; grep prints whole record lines with reply marks. These include
`[ANSWERED #12]`,
`[WITHDRAWN #14]`, `[LIFTED #n]`, `[DONE #n]`, `[GO #n]`, `[FIX-LIVE #n]`, or
`[RE #n]`. A resolver adds `[RESOLVED #n]` to the target, naming the resolver,
and `resolves #n` to itself, naming the target.

`cci digest` defaults to 24 hours. `open asks` (JSON `open_asks`) contains asks and
decision requests. `open blockers` (`open_blockers`) contains `blocker` and
`blocked`; `open defects` (`open_defects`) contains defects. Holds and incidents
have their own sections, followed by a count of older open items. `grep` searches
newest first and includes expired records. Tail, grep, state, and text digest
default to 6,144 bytes, capped at 16,000 with `--budget`.

Use `--width N` on tail, watch, and digest to set the width of each rendered
record line. The default is 400 characters, including a final `…` when
clipped; `--width 0` prints whole records. The width excludes digest
indentation and appended reply marks. Grep and state print whole records.

JSON ignores `--width` and keeps record text whole. Tail, grep, and state
still apply their byte budgets in both formats. Wider lines can leave room
for fewer records within a budget.

## Watch GO lines with a Monitor

Run the watch as the Monitor's command:

```bash
cci watch --drive monitor-demo --kind go --cursor monitor
```

It polls once a second and exits after 29 minutes. Re-arm with the same cursor.
Without a saved cursor or explicit window, it starts at the current head.
Use one cursor per filtered watch.

Text record lines clip at `--width`
characters, default 400, before reply marks. Use `--width 0` for whole records.
Watch has no total byte budget in text or JSON form.

## Choose a kind

Pair kinds with a later record using `--re` or the same `--topic` in the drive.
TTL means time to live; `--ttl` overrides defaults.

| Kinds | Default TTL | Pairing or requirement |
| --- | --- | --- |
| `ask`, `decide` | None | Open until `answer`, `go`, or `withdraw`. |
| `decision` | None | A call already made; not open. Imported `DECISION` uses it. |
| `blocker` | None | Open until `withdraw`, `answer`, or `done`. |
| `blocked` / `unblock` | None | A deploy block stays open until unblock or a resolver. |
| `answer`, `withdraw` | None | Close asks, decision requests, or blockers; require a link. |
| `go` | None | Closes a matching ask or decision request. |
| `hold` / `lift` | None | Lift closes hold and requires a link. |
| `incident`, `defect` / `fix-live`, `done` | None | Fix-live or done closes the opener; done also closes blockers. |
| `opened`, `landed` | None | Require `--pr`. |
| `review` | None | A rules-review verdict. |
| `owner`, `mechanism`, `correction`, `release`, `applied`, `handoff`, `head`, `contract` | None | Unpaired. |
| `digest` | None | Written only by compaction. |
| `claim`, `note`, `state`, `matrix`, `report` | 24 hours | No automatic pairing. |

Import appended inbox files, not the runner's rewritten `runner-state.md` view.
`cci import` registers files, restarts after inode changes or shrinkage, and
deduplicates lines within the drive. After a parser version change, the next
import or daemon refresh reparses consumed lines and updates matching records
without changing their stored timestamps or restoring compacted records. Import
output includes the number reparsed. It extracts PRs, Buildkite build URLs, and
stack tokens such as `api/plat-usw2-prod`. The daemon refreshes registered imports
every second, importing later appends and new `<inbox>.md.archive/*.md` files
with the inbox's drive and lane; direct archive imports default to the inbox
name. Run `cci serve` or start any Claude Code session with the plugin installed
to start or reuse the daemon. Allow the next refresh to finish before reading
appended records.

Once lanes post directly, stop writing markdown inboxes. Compaction folds old
records into daily counts and keeps open items. Open-item tracking skips
`import:<file>` records because legacy lines lack closure links.
