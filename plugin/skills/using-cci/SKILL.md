---
name: using-cci
description: Load when posting or reading drive coordination records, inbox lines, lane status, GO lines, holds, asks, or blocked deploys; when watching a drive with a Monitor; when finding or reaching another Claude Code session, root, or worktree on this machine; or when replacing markdown inbox appends or ephemeral cc-notes status with cci. Covers kinds, closures, stack and target references, cursors, the peer registry, and durable knowledge in cc-notes.
---

# Using cci

Drive coordination goes in `cci`; durable rulings, runbooks, and design docs stay in `cc-notes`, linked with `--ccn`. A `decision` record logs a call made (not open, no expiry); `decide` requests one.

## Records and tags

A record is one typed line in a drive: a machine-wide `seq`, a `kind`, the writing `lane`, optional `to` recipients, text, and refs (PRs, builds, stacks, targets, a body file). Readers print a compact line under `@<clock>` when the minute changes:

`<seq> <kind> <lane>[><to>] [^<re>] [x<closed>] [<repo>#<pr>] [t:<topic>] [+] <text>`

`+` marks omitted text or a body; `read` or `cci show <seq>` prints the full record.

Subscribed sessions receive records as `<channel source="plugin:cc-inbox:cci">` tags with `seq`, `kind`, and `lane` attributes and the compact line as body. A `batch` tag groups batch-kind records (for a `root`, `done` by default) at most every five minutes within ~3 KB, ending in `+N more` naming the `cci tail` command for the rest. A `skipped` tag counts pre-start records addressed to you without replaying them; read them with `inbox` or the named `cci tail`. Delivery is at-least-once: skip any `seq` already handled.

Sessions bind to a drive and lane. A lane's first `cci post --lane <lane>` binds it; a named subagent is that lane already. `cci drive use <drive> --root` binds a coordinator. Posts never default to `root`: `root` must pass `--lane root`, and unbound threads without `--lane` are refused.

## Read your mail

Prefer `cci` MCP tools (`mcp__plugin_cc-inbox_cci__*`) over Bash when available.

- `inbox` returns records since your last `inbox` call: yours arrive whole, subscribed broadcasts truncate at 160 characters. Call `inbox` after a `skipped` tag.
- `read` prints each given `seq` once (line then body) within a byte budget.
- Without a subscription, records to your lane arrive as hook context after your next tool call, led once each by open `ask` records you owe; `cci tail --open` prints all of them.
- Act on any `stopped`, `hold`, or `owner` record addressed to you before your next edit.
- In Bash, `cci tail --to <lane> --mail` from the bound thread reads its mail through the hook's cursor once per record. Check `cci state` for the latest `head`, `contract`, and `state` per lane and topic before asking a lane for its head or status.

## Post, answer and close

The MCP `post` tool takes `kind`, `text`, `to`, `re`, `resolves`, `pr`, `topic`, `attrs`, `body` or `path`, and more (21 fields) without shell quoting; `records` posts a batch. `post` and `close` default `lane` to the window's bound lane, refusing unbound windows with `post needs --lane`. For flags the tool lacks (`--owned-by`, `--stack`, `--target`), run `cci post` in Bash.

Post one record per event. Set `to` for recipients, `to: owner` when an action needs the owner, or omit `to` to broadcast. Text over 400 characters spills to a file, keeping 400 characters and the path; use `body` for long content. Never type clock times; write `{at}` so `cci` substitutes the record's timestamp. Name earlier records by `seq`, never time.

Reply with `re: <seq>`. Close `ask`, `decide`, `blocker`, `blocked`, `defect`, `hold`, or `incident` with `resolves: <seq>` (accepts multiple). The `close` tool posts one record closing all given `seq` numbers, addressed to their writers, as `answer`, `withdraw`, `lift`, `go`, `unblock`, or `fix-live`.

Exit 75 from `cci post` means the bus is down and nothing was stored; never treat it as sent. Exit 76 means another live thread holds the lane and nothing was stored; stop.

A launched lane's turn ends only on a typed record: `done`, `stopped`, `stand-down`, a doc-less `handoff`, or an open `ask`, `blocker`, or `blocked`. `cci hook stop` refuses any other stop.

## Kinds a lane posts daily

- `state`: status.
- `ask` (to a lane or `owner`): question; open until `answer`, `go`, or `withdraw`.
- `answer`: closes `ask`, `decide` or `blocker`; needs `re`, `topic`, or `resolves`.
- `done`: finished work; also closes `defect` or `blocker`.
- `blocked`: waiting. Name the wake event with `pr`, target, `lane_ref`, or `card-` id in text; to wait on your own monitor or clock, set attr `until=<span>` such as `45m`.
- `hold` / `lift`: `hold` stops the exact PRs in `pr`; `lift` with `re` closes it.
- `go`: permission; closes a matching `ask` or `decide`.
- `head`: on every push, full commit SHA with PR or branch as `topic`.
- `opened`, `landed`: PR events; require `pr`.

## Which reference to read

| Read | When |
| --- | --- |
| [references/posting.md](references/posting.md) | Using CLI-only flags (`--owned-by`, `--stack`, `--target`, `--census`, `--count`, `--ttl`), or a post is refused or deduplicated. |
| [references/lanes.md](references/lanes.md) | Starting, replacing, or renaming a lane, or exit 76. |
| [references/kinds.md](references/kinds.md) | Choosing an unlisted kind, or a close is refused. |
| [references/stacks.md](references/stacks.md) | Deploy stacks, or importing a markdown inbox. |
| [references/reading.md](references/reading.md) | Reading history, filtering by PR, stack, or lane, or truncated output. |
| [references/channel.md](references/channel.md) | Subscribing windows, tuning root wake rules, or watching without a channel. |
| [references/peers.md](references/peers.md) | Finding or reaching another session or drive. |

After a `cc-inbox` release workflow finishes, run `cci upgrade` once; the daemon hands off within 30 seconds, so never hand-run `cci serve`.
