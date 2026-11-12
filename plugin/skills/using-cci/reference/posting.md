# Post records: the MCP tools and every `cci post` flag

## Use the MCP tools or the CLI

When the cci channel's tools (`mcp__plugin_cc-inbox_cci__*`) are in your tools,
use them rather than Bash. Each defaults to the session's drive, `post` and
`close` default `lane` to this window's, and each returns compact lines:
`<seq> <kind> <lane>><to> [^re] [x<closed>] [#pr] [+] <text>`, under an
`@<clock>` line when the minute changes; `+` means a body waits for `read`. The
`post` tool takes the `cci post` flags lanes pass most as fields, a dash
becoming an underscore (`lane_ref`), with `attrs` for `--attr`; a post that
needs another flag, such as `--owned-by` or `--or-heir`, runs in Bash. In a shell,
`cci post --batch file.json` posts a JSON list of those same objects, so no text
meets shell quoting. Every record is checked before any is stored, and the list
stores in one step, so a refusal stores none of it; cci names each refused
record's place and exits 1.

A session launched with the server whose server stops gets a blocker from its
lane to `meta-desk` on its next tool call, and inside Orca
`/mcp reconnect plugin:cc-inbox:cci` is typed at its next idle prompt; outside
Orca, ask the human to type it. Keep working through the CLI meanwhile, never
silently: the blocker closes itself when the server is back. Orca lanes launch
with `--strict-mcp-config` and have no server by design: the CLI is their route.

- `post`: one record from the fields in the following table, with `body` or an
  absolute `path` for a long body; `records` posts several in one call, each
  item taking every field except `lane`, `drive` and `records`, which come
  from the call. A field beside `records` fills in each item that lacks it,
  `attrs` key by key and `id` as `<id>#<place>`, and the list stores whole or
  not at all, so one refused item stores none and is named. No shell quoting.
  Returns the new seqs.
- `read`: `seqs` each once, line then the rest of the body, within `budget`.
- `inbox`: records new since your last inbox call; addressed ones whole,
  subscribed broadcasts cut to 160 characters.
- `find`: newest matches for `pattern`, `kinds`, `lanes`, `to`, `since`; `prs`
  answers several pull requests in one call. Each query returns about 4 KB;
  `budget` raises it.
- `close`: one record closing every seq in `seqs`, addressed to their writers.
- `who`: the live sessions on the session's drive, or on `drive`.
- `digest`: as the command.

The `post` tool refuses a field it does not advertise. Every flag stays on
`cci post` and in `cci post --batch` objects.

| Route | Fields or flags |
| --- | --- |
| MCP `post` tool, 21 fields | `lane`, `drive`, `kind`, `text`, `body`, `path`, `to`, `re`, `resolves`, `pr`, `topic`, `attrs`, `id`, `ccn`, `build`, `lane_ref`, `url`, `outcome`, `commit`, `head`, `records` |
| `cci post` in Bash only | `--owned-by`, `--successor`, `--or-heir`, `--quoted`, `--pairwise`, `--force`, `--ttl`, `--count`, `--body-file`, `--mode`, `--env`, `--stack`, `--target`, `--board`, `--census`, `--batch`, `--json` |

## One record per event

Write one record per event. Text over 400 characters spills to a file: the record
carries its first 400 characters, cut at a word and ended with `…`, then the
file's path, and `cci post` prints that path. Read the file for the rest. Use `state` for
status, `ask` for a question, `hold` for a stop, `go` for permission, and `opened`
or `landed` for a PR event. Use `review` with `--outcome` for a rules-review verdict.
Address lanes with `--to`; when an action needs the owner, use `--to owner`.
A post that exits 75 was not stored: the bus is down. With `--lane` and
`--drive` it is kept, and the next `cci post` that reaches the store stores it;
without them, repost it once the bus answers. Never treat exit 75 as sent.

A record whose every `--to` is the posting lane is refused, since that lane
reads its own records as ones it wrote. An agent shares its session's lane, so
an agent writing to its own session posts as its own lane, such as
`--lane handler` or `--lane builder`, and keeps `--to <session's lane>@<drive>`.

## Never type a clock time

Never type a clock time into a record. Every reader prints the record's own
stored time, and a time typed from memory can run minutes off it. Where prose
needs the time, write `{at}`, which `cci post` replaces with the record's
Pacific time in the form readers print, such as `10:52 AM`. A clock typed in a
leading `R<n> (<clock>)` stamp, such as `R84 (10:59 AM)`, is replaced the same
way, and so is a leading `H:MM AM/PM` label that labels the post: a time
followed by a colon, as in `10:59 AM: done`, or one beside the lane's own name.
cci prints a hint on stderr when it replaces one. Any other clock is stored as
typed, such as the deadline in `4:40 PM backup gate: queued`; no clock refuses
a post. Name an earlier record by its seq,
never its time, and put a deadline in an attribute, such as
`--attr due=9:30 AM`. A post that takes its
text from a `--path` file stores the file's first line exactly as written, so
the record's text equals that line. Leave clocks and `{at}` out of that line;
the record's own time already dates it.

## Attach refs and structured fields

The text can also be the one quoted argument after the flags, in place of
`--text`.
Repeat `--pr`, `--build`, `--stack`, `--target`, and `--lane-ref` to attach refs.
`--pr` takes a number, `owner/repo#N`, or a GitHub pull request URL; qualify a
PR in another repo, such as `--pr Forge-FDE/iris-chime#243`.
`--lane-ref` names lanes the record is about; `--lane` names its writer. An
`opened` record needs `--pr`, and so does a `head`, unless it names a note with
`--ccn`, such as an experiment's method note posted before any PR exists.
Without either, cci takes the one `#N` the text names or refuses.
A restack that moves several PRs posts one `head` record, not one per PR:
repeat `--head <pr>=<sha>`, or `--head <pr>=<was>..<sha>` with the head it
replaced, and an optional note after a space, such as
`--head "45002=4423264..c3b44a3 patch-identical"`. The record names every PR in
its refs, stores each head as `fields.attrs["head:<pr>"]` =
`[<was>..]<sha>[ <note>]`, and opens its text with
`HEAD 3 PRs: #45001 a1b2c3d, #45002 c3b44a3, ...`; the text argument, when
given, follows after a colon.
`--path`, `--ccn`, `--url`, and `--board` also set refs. Put long bodies in a file
and post it with `--path <file>`; without `--text`, the file's first line becomes
the text, so open the file with a one-line summary. `--path -` reads the body
from stdin, and cci copies stdin, `/dev/stdin`, and other pipes to a file under
`$CCI_HOME/blobs` so the record keeps the body.

Structured fields use repeatable `--env`, `--mode` (`platy`, `cli`, `manual`,
`walker`), `--outcome` (`passed`, `failed`, `pending`, `cancelled`), `--commit`,
`--census` JSON, and `--count name=integer`. Census carries `n`, `denominator`,
`head`, `drift`, and `stacks_clean`. PRs, builds, and deploy stacks live in refs.
Repeating the same drive, lane, kind, and normalized text within ten minutes
returns the original record, even if its references differ. The comparison
reads the stamped text, so a repost carrying `{at}` or a leading stamp matches
only within the same clock minute; give a record you may retry an `--id`.

## Publish head, contract and state

Publish `head` on every push, with the full commit SHA as text and the PR or
branch as topic. Publish `contract` for interfaces other lanes consume; withdraw
it with `--re` before changing the interface. Broadcast both by omitting `--to`.
Read `cci state` before asking for a head, contract, or lane status. It returns
the latest `head`, `contract`, and `state` per lane and topic, skipping withdrawn
records. With no matches, text output says
`no head, contract, or state records on <drive>`; JSON emits no records.
