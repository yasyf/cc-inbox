# Read records: cursors, filters and output

## Read without repeating context

Give each lane its own cursor. For the bound drive, the `api` lane reads:

```bash
cci tail --cursor api --reader api
```

On `tail`, `watch`, `grep`, and `state`, `--reader` delivers records addressed
to the lane regardless of kind, lane, or topic filters, plus other lanes'
broadcasts matching those filters. A broadcast omits `--to` or posts `--to all`;
your own never come back. Repeat `--topic` or `--lane` to select topics or posting lanes.
Use `--kind` on tail, watch, or grep to select broadcast kinds. Repeat the flag
or separate kinds with commas, such as `--kind hold,defect`. `--to` selects
addressed records only. Stack, target, and PR filters apply to all deliveries.

The root reads drive coordination through `cci digest`, `cci tail`, and
`cci watch`. Use `cci tail` with the session's default cursor. After `drive use`,
tail starts with records posted after the bind; rebinding resets that cursor
to the drive's latest record. Named lane cursors are unchanged, and one with
no saved position reads the past hour. Reads advance cursors only through
printed records. If capped, repeat with the same drive, cursor, and filters.
Explicit `--since` accepts a sequence number, duration, local clock time such
as `09:00` or `9:00 AM` (the latest one at or before now), or RFC 3339 time, and
leaves the cursor untouched, even with `--cursor`; use `--since 0` to replay.

`SessionStart` shares the root's cursor and injects the digest plus unseen
records after compaction. Both reads use a 400-character width, with a
2,500-byte digest budget and 1,500-byte tail budget.

## Filter by stack, target or PR, and read the output

Read a stack's problems and fixes, or search records about a target or PR:

```bash
cci tail --drive demo --stack api/plat-usw2-prod --since 0 --json
cci grep 'Platy' --drive demo --target api
cci tail --drive demo --pr 30440 --since 0
cci tail --drive demo --lane-prefix desk- --per-lane 2 --since 0
```

Read filters `--stack`, `--target`, and `--pr` each take one value and combine
with AND. JSON tail, grep, and watch reads add `status` to opener kinds: `open`,
`closed`, or `imported`. Imported openers with a stack or target ref use `open`
or `closed`; those without either ref use `imported` and remain untracked.
Text `tail`, `grep` and `show` print one compact line per record,
`<seq> <kind> <lane>[><to>] [^<re>] [x<closed>] [<repo>#<pr>] [t:<topic>] [+] <text>`,
under an `@<clock>` line each time the minute changes; `+` marks a body that
`cci show` prints. `watch` and `state` keep the long line. Text tail and watch
reads append reply marks after clipping; grep prints whole record lines with
reply marks, and its pattern matches each record's long line. Text tail cuts a
record a later one resolved to 160 characters and says so in a trailer;
`--all` prints those whole, and `cci show <seq>` prints one whole. These include
`[ANSWERED #12]`,
`[WITHDRAWN #14]`, `[LIFTED #n]`, `[DONE #n]`, `[GO #n]`, `[FIX-LIVE #n]`, or
`[RE #n]`. A resolver adds `[RESOLVED #n]` to the target, naming the resolver,
and `resolves #n` to itself, naming the target.

`cci digest` defaults to 24 hours. `open asks` (JSON `open_asks`) contains asks and
decision requests. `open blockers` (`open_blockers`) contains `blocker` and
`blocked`; `open defects` (`open_defects`) contains defects. Holds and incidents
have their own sections, followed by a count of older open items. An
`untracked holds` line (JSON `untracked_holds`) lists imported holds that name
no `stack:<project>/<env>` or `target:<name>` ref; no later imported line can close them, but a
post with `--resolves <seq>`, or a `lift` with `--re <seq>`, does. `grep` searches
whole rendered record lines, including sequence number, time, kind, lane,
recipients, topic, text, and rendered refs. Patterns are
case-insensitive regular expressions by default, so `HOLD|DEFECT` matches either
word (`-i` is accepted; `--ignore-case=false` matches case exactly); `-F`
matches the pattern literally. `-n N` (`--limit N`) on `tail` or `grep` prints
only the newest N matching records, as `tail -n` does. It searches all matching records newest first, including expired
records. A text grep with no match prints `no records on <drive> match; store
head #<seq>`. Tail, grep, and state default to 6,144 bytes, and text digest
to 4,000 bytes, one screen; `--budget` sets any other size.
When the budget cuts a text `tail --since` window, the newest records print and a
`... CUT <n> older records before #<seq>; --oldest reads them first` trailer
names what it dropped.

Tail, watch, and digest print whole records by default. Use `--width N` to
clip each rendered record line to N characters, including a final `…`. The
width excludes digest indentation and appended reply marks. Grep and state
always print whole records.

JSON ignores `--width` and keeps record text whole. Tail, grep, and state
still apply their byte budgets in both formats. Wider lines can leave room
for fewer records within a budget.
