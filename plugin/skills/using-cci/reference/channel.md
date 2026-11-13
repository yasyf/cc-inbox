# Channel subscriptions and Monitor watches

## Receive records over the channel

A session launched with `--channels plugin:cc-inbox@cc-inbox` can take its
records as pushed `<channel source="plugin:cc-inbox:channel">` tags instead
of a Monitor. The plugin's `channel` server, `cci channel --no-tools`, has no
tools, so no tool call, a subagent's included, starts or replaces it; its `cci`
server, `cci mcp`, serves the tools. Both are the `cci` on `PATH`, started with
the same config in every plugin version, so a plugin update and
`/reload-plugins` leave them running. Subscribe once:

```bash
cci subscribe --drive release-demo --reader root --kind incident,decide,ask
```

Each tag's `seq`, `kind`, and `lane` attributes carry the record's header,
and its body is the record's compact line, the line `cci tail` prints, under
an `@<clock>` line. A `+` marks text or a body the line leaves out;
`cci show <seq>` or the `read` tool prints the whole record. A record addressed to a root reader
carries that rest under its line when it fits 2,000 bytes, so the root needs no read for it. In a window launched with the
channel, the `PostToolUse` hook skips what the channel delivered, and the
channel skips what the hook delivered first. The channel moves its cursor past
a record only once the session's transcript shows its tag on the main thread,
so a tag Claude Code never showed the session still reaches it through the
hook, and a tag arriving mid-turn can arrive from the hook as well. Delivery is
at-least-once, so skip a seq you have already handled. The subscription survives compaction and `/clear`, and
the `SessionStart` hook carries it into a resumed session. Re-running
`cci subscribe` replaces it, and `cci unsubscribe` ends it. Only the main
thread subscribes: both commands refuse to run in a subagent or teammate,
whose lane records reach it through the `PostToolUse` hook instead.
The cursor starts at the head when you subscribe and defaults to
`channel-<session>`. A channel Claude Code starts, as on a resume or a
reconnect, begins at the head instead of replaying what the cursor missed: one
`skipped` tag counts the records addressed to the reader it passed over, and
the `inbox` tool or `cci tail --since` reads them; a registered session's
channel replays them instead. Claude Code reads only a
window's newest channel server and leaves the older ones running, so the newest
to start owns the window: an older one delivers nothing, moves no cursor, and
exits. A subscribed channel that stops any other way, such as Claude Code
closing it on a reconnect, a reload or an exit, posts a `disconnect` record
from the reader's lane to `communicator`, and so does a channel whose session
shows no tag received 30 seconds after one went out; the next tag the session
receives posts `recovered`, closing it. Stop any Monitor that watched the same records after
subscribing; records in the overlap arrive twice.

A long-lived session, such as a drive's coordinator, communicator or meta
session, registers instead, so cci keeps it alive and its subscription belongs
to an alias rather than to one Claude Code process:

```bash
cci session register release-demo-coordinator --new --cwd ~/work/release-demo --command "claude --channels plugin:cc-inbox@cc-inbox" --resume-prompt "Resume as the coordinator" --drive release-demo --reader root --cursor coordinator
```

It takes the flags `cci subscribe` takes, plus `--command`, `--cwd`,
`--resume-prompt`, `--notify`, and `--session` (this session by default) or
`--new`. cci runs the session in the tmux session named by the alias, where
`cci session run` starts the command and starts it again with `--resume` and
the resume prompt each time Claude Code exits; `cci session attach <alias>`
shows it in a terminal such as Orca's, whose `ORCA_TERMINAL_HANDLE` the tmux
session keeps and each start passes to Claude Code. Each window the session runs in hears
the alias's subscription, and the session never runs `cci subscribe`, which
refuses it, as `cci unsubscribe` does. When its channel disconnects while
Claude Code runs on, or its session stops hearing it, cci types
`/mcp reconnect plugin:cc-inbox:channel` into its tmux session once per open
`disconnect`, and that record goes to each `--notify` lane, `communicator` by
default. Its next channel replays from the cursor: each `owner` record it
missed as its own tag, then the rest as one `batch` tag ending in the
`cci tail` that reads what the tag leaves out. Register the alias again to change what it
delivers; its cursor stays. `cci session list` names each alias, its session,
where it runs and its cursor, and `cci session rm <alias>` ends it.

A record addressed to the reader arrives only when its kind is a
`--to-kind`. For `--reader root` (or `main`) that defaults to
`incident,mechanism,fix-live,decide,ask,design,blocker,blocked,urgent,answer,release,correction,defect,owner`, the kinds a root
acts on, and `--batch-kinds` defaults to `done` in a window launched with
`--channels`; the state, head, red, report, review, note and evidence records lanes
copy to root stay in `cci tail` and `cci digest`, and the root's `PostToolUse`
hook holds them back too. Other readers get every addressed kind unless they
name `--to-kind`. `cci watch --reader root` applies the same default.

`--batch-kinds report,note` holds those addressed kinds off a window until one
tag carries them together, at most every five minutes and at once ahead of any
other record, so they never wake it alone. The tag keeps the newest record per
kind, lane and topic, and per pull request for `head`, within about 3 KB, and ends in
a `+N more` line naming the `cci tail` that reads the rest. `cci tail --to <lane> --mail`, from
the thread bound as that lane, reads its mail through the hook's cursor, so a
wait and the hook deliver a record once. `--to <stem> --mail` from a numbered
lane that holds the stem reads the same mail through the same cursor. A delivered line names no recipients;
`cci show <seq>` does.
`--mute-kinds state,review,note` keeps those kinds off the channel and out of
the root's hook delivery, even when addressed to the root; `cci tail` still
reads them. An `urgent` record repeating its lane's `urgent` topic within 30
minutes reaches no channel and no hook, for any reader; post a stall once and
repeat it no sooner than every 30 minutes. `cci me` prints the window's
subscription, to confirm a subscribe took.

## Watch GO lines with a Monitor

Run the watch as a persistent Monitor's command:

```bash
cci watch --drive monitor-demo --kind go --cursor monitor
```

It polls once a second until interrupted. Restart it with the same cursor.
A session that can take the channel subscribes once instead, as the section above says.
Without a saved cursor or explicit window, it starts at the current head.
Use one cursor per filtered watch.

Text record lines print whole unless `--width N` clips them before reply
marks.
Watch has no total byte budget in text or JSON form.
