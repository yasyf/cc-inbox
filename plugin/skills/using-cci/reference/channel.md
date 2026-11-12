# Channel subscriptions and Monitor watches

## Receive records over the channel

A session launched with `--channels plugin:cc-inbox@cc-inbox` can take its
records as pushed `<channel source="plugin:cc-inbox:cci">` tags instead of a
Monitor. The channel server is the `cci` on `PATH`, started with the same
config in every plugin version, so a plugin update and `/reload-plugins` leave
it running. Subscribe once:

```bash
cci subscribe --drive release-demo --reader root --kind incident,decide,ask
```

Each tag's `seq`, `kind`, and `lane` attributes carry the record's header,
and its body is the record's compact line, the line `cci tail` prints, under
an `@<clock>` line. A `+` marks text or a body the line leaves out;
`cci show <seq>` or the `read` tool prints the whole record. A record addressed to a root reader
carries that rest under its line when it fits 2,000 bytes, so the root needs no read for it. In a window launched with the
channel, a record addressed to the root's lane arrives once: the
`PostToolUse` hook skips what the channel delivered, and the channel skips
what the hook delivered first. Delivery is at-least-once, so skip a seq you
have already handled. The subscription survives compaction and `/clear`, and
the `SessionStart` hook carries it into a resumed session. Re-running
`cci subscribe` replaces it, and `cci unsubscribe` ends it. Only the main
thread subscribes: both commands refuse to run in a subagent or teammate,
whose lane records reach it through the `PostToolUse` hook instead.
The cursor starts at the head when you subscribe and defaults to
`channel-<session>`. A channel Claude Code starts, as on a resume or a
reconnect, begins at the head instead of replaying what the cursor missed: one
`skipped` tag counts the records addressed to the reader it passed over, and
the `inbox` tool or `cci tail --since` reads them. Claude Code reads only a
window's newest channel server and leaves the older ones running, so the newest
to start owns the window: an older one delivers nothing, moves no cursor, and
exits. Stop any Monitor that watched the same records after
subscribing; records in the overlap arrive twice.

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
