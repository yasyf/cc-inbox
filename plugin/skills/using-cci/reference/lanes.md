# Lanes: binding, ending a turn, successors and stems

## Bind a session to a drive

Use the task's drive and lane names. Pass `--drive` or bind the current Claude
Code session. Keep its existing `CLAUDE_CODE_SESSION_ID`; subagents share their
parent's value and add `CCI_AGENT_ID` to it, so each subagent or teammate binds,
claims and reads as its own thread. A Codex lane has no `CLAUDE_CODE_SESSION_ID`;
its `CODEX_THREAD_ID` is the session, so `cci drive use` binds it and later
`post`, `tail` and `watch` calls resolve the drive without `--drive`. A
session another session starts to post for it, such as a drive handler's own
`claude --bg` session, carries `CCI_PARENT_SESSION` set to the starting
session's id: its posts bind no lane, so it posts as the lanes its parent holds
without being refused as their second binder. Each `cci drive use` sets the session's cursor to the selected
drive's latest record, including when rebinding the same drive. An empty drive
starts at sequence 0. A root binding also enables owner-prompt capture through
the plugin:

```bash
cci drive use release-demo --root
```

## Refused posts, turn ends and successors

A post or `cci announce --lane` that exits 76 was refused because another live
thread holds the lane, on this machine or on another node; nothing was stored.
Stop. The name passes to another thread only after the lane stands down.
A launched lane's turn ends only on a typed record: `cci hook stop` refuses
every other stop and prints the commands. The records that end a turn are
`done`, `stopped`, `stand-down`, a `handoff` with no doc, and an open `ask`,
`blocker` or `blocked`. To wait on an event, post `blocked` with `--pr`,
`--target`, `--lane-ref`, or a `card-` id in the text; that event wakes the lane.
To wait on a monitor or clock the lane armed itself, post `blocked` with
`--attr until=<span>`, such as `45m`; no event wakes it, and past the span the
supervisor pages it as overdue.
A second stop with nothing new posted writes the blocker for the lane.
A refused `cci post` stores a `blocker` to the posting lane holding the refusal
and the refused words, and the lane's next stored post closes it. Root, or the
lane's owner, hears of it only when the lane's next attempt is refused too,
in one blocker naming both, or when the lane ends its turn with no stored post. A new lane's first post needs
a `spawn` record naming it, or `CLAUDE_LONG_RUNNING_LANE` set to its name.
A lane that has not started yet is addressed like any other and reads the
record once it binds. `cci post` refuses only a never-seen bare `--to` lane
that looks like a slip for a live one, one or two edits from it or an older
number of it, and names that lane; `--force` addresses it as typed. When a
lane is replaced, record it once with
`cci post --lane root --kind stand-down --to <old> --successor <new> "<why>"`:
a later `--to <old>` is refused naming `<new>`, `cci who` marks `<old>`
stood down, and `cci who --successor-of <old> --json` names who stands in.
A stand-down stands down its first `--to` lane only; the lanes it copies stay live.
A thread cannot bind a lane another live thread holds until that lane stands
down: its `cci post` is refused and root hears a `defect`. A numbered successor
binding beside its live predecessor binds, and cci stands the predecessor down
for it at once, as lane `registry`, so mail to either name reaches the successor;
the predecessor posts its handoff and stops.

## Address a numbered lane by its stem

Readers name a writer by the stem it holds, so the name in a line is the one to
address; a writer that no longer holds its stem keeps its number.
Address a numbered lane by its stem, the name without its trailing `-<n>`:
`--to signal-valkey` reaches the stem's newest live lane, the one whose first
post, thread binding or spawn record came last, or the successor a stand-down
named. A running `signal-valkey-2` still reads what is addressed to it by name.
`cci post` stores `--to signal-valkey-2` as `--to signal-valkey` whenever the stem
reaches that same lane, so the record follows the stem to whichever lane holds it
next; a stand-down or an `--owned-by` decide keeps the lane it is about.
Once it stands down, that mail reaches the successor it named, else the stem's
newest live lane, and `cci post` prints a line naming the stem. The lane holding
a stem reads, in its tail, hook mail and open asks, what was addressed to the
stem and to its stood-down predecessors, so it never watches two names. When no
live lane reads a `--to` name, or several running lanes share its stem,
`cci post` says so on stderr and still stores the record; the first lane to
register under the stem reads it.

## A lane's binding and mail

A lane needs no reader setup: its first `cci post --lane <lane>` binds the
session, and later posts keep that drive and default `--lane` to that lane.
A subagent or teammate spawned with a name is that lane already: `cci me`
binds it, and its posts default `--lane` to the name. Records posted to a
lane before any thread bound it arrive on the first binding. A record stamped
before the lane started that `cci relay seed` applies after it bound stays out
of its mail and its open asks, while `cci relay sync` and the relay stream
always deliver; `cci tail --to <lane>` still reads it. `--drive` on a
post or cursor applies to that one call. A post never rebinds a bound thread;
only an explicit binding moves it: `cci drive use`, `cci announce --lane
<lane>`, or `cci subscribe --reader <lane>`, which binds the window's root
thread to its reader, and a session a `/clear` starts in that window takes the
same lane. The explicit lane wins over the one a first post or a name gave the
thread, so a bridge window that once posted `--lane root` and subscribes as
`--reader owner-relay` hears and owes only owner-relay's records. Claiming a
lane another live thread holds and never stood down is refused, as a second
binder's post is. A post never defaults to `root`: the root passes `--lane root`, and
an unnamed thread bound to no lane is refused. From
then every record addressed to the lane arrives as hook context after the
next tool call, including records posted since the window started. A
`head`, `review`, `state`, `claim` or `matrix` record arrives only
when the lane is its first addressee; read the ones it is copied on with
`cci tail --reader <lane>`, which also moves the hook past what it printed. The open asks the lane owes lead that
context once each: an ask the hook has not printed before brings one line
counting the new ones and naming `cci tail --open`, which prints every one,
newest first at any age; `--kind ask` narrows it and `--to <lane>` reads another lane's. The
first delivery after a session
start or compaction lists them in full. A `stopped`, `hold`, or `owner` record
addressed to you lands mid-task; act on it before your next edit.
