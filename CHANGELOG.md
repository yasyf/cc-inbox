# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- A channel whose session stops hearing it, a tag its transcript does not show
  received 30 seconds after it went out while the transcript showed no other,
  posts a `disconnect` and, for a registered session, types
  `/mcp reconnect plugin:cc-inbox:channel` into its tmux session once per open
  `disconnect`. A tag Claude Code queues while the session is busy counts as
  received, and the next tag the session receives posts `recovered`. A
  registered session's new channel replays what its cursor missed instead of
  starting at the head: each `owner` record as its own tag, then the rest as one
  `batch` tag, less what the hook already delivered.

- A registered session's tmux session keeps the `ORCA_TERMINAL_HANDLE` of the
  terminal that last attached to it, and `cci session run` starts Claude Code
  with it, so the session's Orca calls name the Orca terminal that shows it
  rather than none.

- A subscribed channel that stops for any reason but a newer channel claiming
  its window or its daemon handing off posts a `disconnect` record from the
  reader's lane to `communicator`, naming the window, the session and the
  reconnect command. The next channel to stream that reader posts `recovered`
  with `--resolves`, closing it; `withdraw` closes it too.

- `cci session register <alias> --command <command>` keeps a session alive
  under an alias in the tmux session named by the alias. There
  `cci session run <alias>` starts `--command` from `--cwd` in your login
  shell, with `--session-id` until Claude Code writes the session's
  transcript, then with `--resume` and `--resume-prompt`, and starts it again
  each time Claude Code exits, until `cci session rm`. `--new` starts a fresh
  session under the alias; a session already running in another window moves
  to tmux once that window exits. `cci session attach <alias>` attaches a
  terminal, such as an Orca terminal, to it, and the daemon launches the tmux
  session of every registered session running nowhere when it starts. The
  alias owns the session's channel subscription, set by the flags
  `cci subscribe` takes, so each window the session runs in hears it, and the
  session never runs `cci subscribe`, which now refuses a registered session,
  as `cci unsubscribe` does. When a registered session's channel disconnects
  while Claude Code runs on, cci types `/mcp reconnect plugin:cc-inbox:channel`
  into its tmux session once per open `disconnect`, which
  `cci session type <alias> <line>` also does by hand; the `disconnect` record
  goes to each `--notify` lane, `communicator` by default. `cci session list`
  names each alias, its session, the window it runs in, its tmux session and
  its cursor. Registrations are files under `~/.cc-inbox/registrations`,
  beside the subscription files, so every cci release keeps opening the same
  store.
- `cci relay export --lane <lane>` also writes, at any age, every record posted
  by or addressed to that lane, the stem it answers under, or a numbered lane of
  that stem. A successor seeded from `--since <its launch> --lane <it>` holds
  its predecessor's records, so `cci tail --lane <predecessor>` reads them there.
- An owner gives the workers it delegates the group keys through one ring: each
  epoch's key sealed under a ring key, and the ring key wrapped once for each
  worker. A worker reads every epoch it may hold in two relay reads, a delegation
  writes one wrap, and each rotation carries its new ring, which leaves revoked
  workers out, so a worker that hears the rotation already finds its new key.
- `cci relay grant` asks the owner's Touch ID once and lets this node issue
  `cci relay delegate` delegations for the next 24 hours; the relay refuses a
  delegation from an owner with no live grant.
- `cci relay delegate --pub <key> --drive <drive>` lets an owner sign a worker
  key a delegation of at most 24 hours, wrap it every group key, and register
  both with the relay, then prints the delegation. `cci relay join --delegation -`
  joins the worker with it, holding no key but its own, and drops a
  `service.pem` an earlier `--instance` join kept. The owner renews by running
  `cci relay delegate` again; the relay keeps taking the delegation just
  replaced, and the worker adopts the renewal from its next pull, refusing one
  that does not expire later or that no pinned owner signed for its key. A
  rotation or share wraps the new group key to every live worker this owner
  delegated, and `cci relay revoke <instance>` ends one. Needs a relay that
  serves `POST /v1/delegate`.

- `cci tail --open` prints every open ask, blocker and question-ending decide
  the lane this thread is bound as owes, newest first at any age, in one call;
  `--to <lane>` reads another lane's and `--kind ask` narrows it. A lane read
  them before by filtering `cci digest` for its name. The hook's "open asks
  changed" line now names it in place of `cci digest`, which lists the whole
  drive.
- `cci cursor show <name>` prints where a named reader cursor stands without
  moving it, so a lane can read a cursor before it decides to `cci cursor set`
  it. A cursor that was never set is refused rather than printed as `#0`.
- `client.WholeText(record)` returns a record's whole text the way `cci show`
  reads it: its text joined to the body file it names or spilled to, and a
  missing file named in place of its body. A file that exists and cannot be
  read is an error, never the clipped text. A caller in another module reads a
  record whole through it instead of keeping its own copy of the rule.

### Fixed

- A registered session no longer runs on the PATH of the process that
  registered it. `cci session run` starts the session's command with
  `$SHELL -l -i -c` in place of `$SHELL -l -c`, so the shell's interactive
  startup files, such as `mise activate`, set PATH for the session's `--cwd`:
  a session registered from another checkout's shell keeps no `tools/bin` of
  that checkout, and its bare commands run its own checkout's.
- The `close` tool with `how: withdraw` takes back a record that opens nothing,
  such as a lane's own `report`, instead of refusing it with `it is a report
  from root, which opens nothing to resolve`. `cci post --kind withdraw
  --resolves <seq>`, which the tool runs, posts the withdrawal `--re` each such
  record, as `--kind withdraw --re <seq>` does, and still resolves each `ask`,
  `decide`, `blocker` or other opener it names.
- `cci session register`, `cci session attach`, a supervised session's restart
  and the daemon's start no longer exit 1 after starting a registered session's
  tmux session. The `set-option -a update-environment ORCA_TERMINAL_HANDLE`
  chained after `new-session` targeted `=<alias>`, which tmux reads as a pane
  and looks up a session literally named `=<alias>`; it now targets
  `=<alias>:`, the form `cci session type` already used.
- A subagent's cci tool call no longer takes a session's channel. The plugin
  now runs two MCP servers: `channel`, `cci channel --no-tools`, delivers
  records and has no tools, so no tool call starts or replaces it, and `cci`,
  `cci mcp`, serves the `mcp__plugin_cc-inbox_cci__*` tools and never claims a
  window. Tags now arrive with `source="plugin:cc-inbox:channel"`, and a lost
  channel is reconnected with `/mcp reconnect plugin:cc-inbox:channel`.
- The channel moves a subscription's cursor past a record only once the
  session's transcript, `<config dir>/projects/*/<session>.jsonl`, shows that
  record's tag on the main thread, in the order the tags went out. A tag
  Claude Code dropped, as a reconnect does, leaves the record to the
  `PostToolUse` hook instead of losing it.

- A plugin update no longer restarts the `cci` channel server of every running
  session. Claude Code hashes a plugin MCP server's resolved config, `command`,
  `args` and `env` included, and on `/reload-plugins` stops and restarts each
  server whose hash changed. It sets `env.CLAUDE_PLUGIN_ROOT` to the versioned
  install directory, such as `~/.claude/plugins/cache/cc-inbox/cc-inbox/0.54.33`,
  and the command was `${CLAUDE_PLUGIN_ROOT}/bin/cci`, so every release changed
  both. The server now starts as `cci channel` from `PATH`, and the plugin's
  own `env` replaces Claude Code's `CLAUDE_PLUGIN_ROOT` with an empty string,
  so its config is the same in every version and Claude Code keeps the running
  server and its channel across an update. The channel needs a `cci` on
  `PATH`, such as the Homebrew cask or `~/.local/bin/cci`.
- Each `claude --bg` session under one Claude Code daemon keeps its own cci
  channel server. Claude Code titles a `--bg` session's process
  `claude bg-spare`, and the daemon's `cci channel` named a window by the
  nearest ancestor whose executable is exactly `claude`, which was the
  `claude daemon run` process every `--bg` session shares. Each newer session's
  channel server then claimed that one window and the older sessions' servers
  exited at their next beat. The daemon now names the window with cc-interact
  0.42.2's `procs.IsClaude`, which matches the first word of the process
  title, the same rule `procs.ClaudePID` uses outside the daemon, and cci's
  own copy of the matcher is gone.
- The MCP `post` tool posts every record of a `records` list. A `kind` beside
  the list made the call a record of its own with no text, and its refusal
  stopped the list before any listed record posted; a `to` or `attrs` beside
  the list reached no record. Each field beside the list now fills in every
  listed record that lacks it, `attrs` key by key and `id` as `<id>#<place>`,
  and the list stores in one commit, so one refused record stores none of it
  and is named. `cci post --batch`, which the tool now runs, stores its list the
  same way, all or none, where it used to post the records around a refusal.

- A Claude Code window that reconnected the cci channel gets every record
  addressed to it. Claude Code leaves a reconnected window's older `cci channel`
  servers running and reads only the newest, and every server advanced the
  same cursors: an older one delivered the second of four asks sent together
  to a lane, moved the lane cursor past it, and the newest skipped it as
  already delivered. Each server now claims its window by its process start
  time in `channels/<pid>-<started>.owner` under the cci home; the newest owns
  it, and an older one delivers nothing, moves no cursor, and exits at its next
  record or channel beat, within 15 seconds.

- The MCP `find` tool searches for a `pattern` that opens with a dash, such as
  `--from|orchestration check`, where it refused with
  `cci: unknown flag: --from|orchestration check`. It handed the pattern to
  `cci grep` ahead of its flags, so the flag parser read it as one; the `find`,
  `read` and `close` tools now pass the pattern and seqs after `--`.

- `cci grep` and `cci tail` print the line that says what they cut, such as
  `... more matches; narrow with --kind, --lane or --since`, once when stdout
  and stderr are one file, as `2>&1` and the Claude Code Bash tool make them.
  Since 0.49.17 a read whose stdout was not a terminal also echoed that line on
  stderr, so a filter such as `| grep ASK` could not drop it; with both streams
  on one file the line printed twice, the stderr copy above the records. The
  echo now goes out only when stderr is a different file from stdout.

- `cci post` from an agent whose session holds the posting lane, addressed only
  to that lane, as an agent of a `communicator` session posting
  `--lane communicator --to communicator@<drive>`, is refused telling the agent
  to post as its own lane, such as `--lane handler` or `--lane builder`, keeping
  its `--to`. The refusal used to say to name another lane to read it, which
  sent the agent's record away from the session it was for.

- A record's attrs now print on its line, as `[key=value ...]` sorted by key
  with the binder left out, in `cci show`, the MCP `read` and `inbox` tools,
  channel tags and every other line view. Before, they showed only in `--json`,
  so a session sent `--attr thread=<ts> --attr job=<j>` never saw either: on
  Nov 10 two drive handlers found no thread on a report that carried one and
  posted at the DM root, then again in the right thread.

- `--since` takes days and weeks, such as `5d` or `2w`, on every command and MCP
  tool that reads it: `tail`, `watch`, `digest`, `grep`, `state`, `who`,
  `relay export`, and the `find` and `digest` tools. Before, `--since 5d` was
  refused with an error that listed "a duration" among the forms it takes,
  because it parsed only Go's units, `ns` through `h`. The refusal and each
  `--since` help line now show the duration forms by example.
- The MCP `post` tool's `records` items take `attrs`, `path`, `url`, `id`,
  `ccn`, `build`, `lane_ref`, `outcome`, `commit` and `head` again, so a batch
  whose records each carry their own `attrs` posts in one call. Since 0.54.17 an
  item took only `kind`, `text`, `to`, `re`, `resolves`, `pr`, `topic` and
  `body`, and refused the rest with `no field attrs`.

- `cci post --or-heir` stores a record whose NEEDS-LANE lines would put the text
  over 400 characters. Before, it refused the record and pointed at `--path`,
  which could not help because cci writes those lines itself. Now the text reads
  `NEEDS-LANE <n> stood-down lanes, named in the body:` followed by the writer's
  own words, and the body carries one NEEDS-LANE line per stood-down lane above
  the `--path` file or the full text.
- The open-ask list the hook prints, `cci tail --open`, and the stop nudge count
  an ask a relay backfilled after the lane's thread bound. Before, a lane whose
  node received its asks through a relay seed listed none of them while every
  other node counted them as open: root's hook showed 1 open ask while the
  stall watch counted 130. Backfill still never arrives as mail.
- `cci post --to <lane>` refuses a lane that stood down, or a stem whose every
  lane stood down, before anything is written, and exits 1 with one line naming
  when it stood down and its live successor, or `root` when it has none. An
  alias whose newest lane is live still posts. When the refused post is an owner
  record or comes from `owner-relay`, cci also posts a blocker to `owner-relay`
  carrying the refused text, so an owner order sent to a retired lane is never
  left unread.
- A post to a lane that stood down and has posted since, such as `owner-relay`
  after `owner-relay-2` and `owner-relay-3` retired, no longer copies that
  stand-down's successors, and an ask, decide or defect to an owned lane no
  longer copies an owner that stood down naming no successor, even while an
  older generation of the owner's stem never stood down. Such a copy reached
  only retired lanes, so lr flagged it ASSIGNMENT-UNREAD.
- A second binder's `defect` to root keeps its first 400 characters and points
  at the full text, so a refusal whose release hint runs long still reaches
  root rather than failing the post.
- `cci post --resolves <seq>` prints one `cci: resolved <name> <kind> <lane>:
  <first 80 characters>` line on stderr for each record it closed, so a lane
  sees when it named the wrong number. It refuses, before anything is written,
  a seq whose record opens nothing to resolve, such as an answer or a note;
  asks, decides, blockers, blocked records, defects, holds and incidents still
  resolve.
- A thread's `cci post --lane <alias>` under a name with no registry row of its
  own posts as the registered live lane the alias reaches, such as
  `redo-epoch-reopen-1` for `redo-epoch-reopen`, instead of being refused. A
  name whose family has no registered live lane is still refused, and so is a
  post under an alias whose lane a thread in another window has bound.
- An ask, decide or defect sent to an owned lane no longer copies the owner when
  the owner stood down and no live lane carries its chain. The record still
  reaches every lane in its `--to`. A live owner, or a live successor of the
  owner, still gets its copy.
- `cci tail --seed <lane>` also starts no earlier than where the MCP `inbox`
  tool has read for that lane, so `lr wait` never wakes on records a lane
  already read through the tool. On Nov 6 a Sprite lane read its mail through
  `inbox`, which moves only the `mcp-<lane>` cursor; its next wait started at
  the `<lane>-wake` cursor its previous wait left, found no `<lane>` cursor to
  start past, and woke at once on 16 minutes of records the lane had already
  read instead of on the owner's pick it was waiting for.
- A cci upgrade no longer leaves the daemon down. The daemon's LaunchAgent now
  sets `KeepAlive` to true, so launchd starts the new build after every exit,
  and a Linux supervisor does the same. Before, it restarted only after a
  failed exit. An upgrade drains the old daemon, which exits 0, and then
  kickstarts the job. On Nov 6 at 3:01 AM the kickstart reached launchd before
  launchd had reaped the old daemon, so it did nothing, and nothing listened on
  127.0.0.1:7377 until the job was started again at 3:05 AM. The listener an
  upgrade holds through the swap now always reaches a new daemon, so a
  connection made during the swap waits for it instead of failing.
- `cci post --kind lift` is refused unless its `--re` or `--resolves` names an
  open hold, and the refusal lists the open holds on the lift's `--pr` and on
  the pull requests of the records it named, so the poster can pick one. On
  Nov 5 a lift meant for a hold named an ask with `--re`, closed nothing, and
  the hold kept a pull request from its grade for two hours. The MCP `close`
  tool posts through the same check. A lift naming an open hold posts as
  before.
- `cci relay seed` no longer locks every other writer out of the store while
  it runs. Since v0.52.2 it applied a whole drive in one write transaction and
  read its copy inside it, so on a Sprite every `cci post` went unconfirmed
  and `cci tail --mail` failed with `database is locked (SQLITE_BUSY)` until
  the seed finished; on Nov 5 a lane's posts stayed unconfirmed from 10:32 PM
  until it was relaunched. The seed now reads 100 rows, then stores them and
  moves its sync cursors in one transaction, so another writer waits for one
  page at most and never while the seed reads its copy. A seed that stops
  partway, a canceled one included, keeps the pages it stored, and
  `cci relay sync` resumes from them. A lane bound while the seed runs still
  reads none of its backfill as mail, and a live reply that waited in the store
  for a parent the seed brings still reaches the lane.
- `cci import`, `cci compact`, and the relay sync step that stores replies
  whose parent never arrived hold the store's write lock one page at a time,
  not for as long as their input is large. Each stores, folds or settles 64
  records per transaction, so a post or a hook with a 2 s budget waits for one
  page at most. A run that stops partway keeps the pages it committed, and
  running it again does the rest. A synced record's body file is written
  before its transaction starts, not while it holds the lock.
- The daemon's first board read after it starts, and `cci maintain-indexes`
  provisioning a drive's incident scope, build their index 64 records per
  transaction. Before, each built it in one transaction over the whole store,
  and the board build also read every body file while it held the lock, so
  posts and hooks failed with `SQLITE_BUSY` after each daemon start on a large
  store.
- The `--lane` help on `cci tail`, `cci grep` and the other readers says it
  keeps the records those lanes wrote, never the ones addressed to them, which
  `--to` reads. A Sprite lane read its parent's evidence with
  `cci tail --lane <parent>`, got nothing, because root and the evidence lane
  had written it to the parent, and fell back to `cci show` seq by seq.
- Moving the daemon onto a newer install no longer refuses connections. While
  the old daemon drained and the new one started, http://127.0.0.1:7377
  refused every connection, so each `lr wait` on a cci source and the drive
  dashboard lost the daemon for the whole move; on Nov 5 from about 5:37 to
  5:39 PM every lane's wait on this Mac exited 127. The process that makes the
  move, whether `cci upgrade`, a session start that finds a newer install, or
  the daemon's own upgrade check, now takes the daemon's listening socket
  before the old daemon drains and holds it until the new daemon serves it, so
  a connection made in between waits for the new daemon instead of being
  refused. That move also runs in its own session: the session-start hook's
  10 s wait and a cancelled `cci upgrade` used to kill it partway, which could
  stop the old daemon and never start the new one. The first move onto this
  release still refuses connections briefly, since the daemon it replaces does
  not offer its socket.
- `cci relay export --since <time>` writes only what the store took from the
  first record written at or after that time, so a node that joins the relay
  can be seeded with the minutes before its join instead of the whole drive.
  A relay queue holds only what was posted after the join, so a Sprite lane
  launched at 7:15 PM never held the incident record and evidence reports the
  Mac posted to it before its 7:19 PM join. A time past every record writes
  nothing, and a seq is refused, since export reads a time.
- `--to <stem>` reaches the stem's own live lane, never a successor from another
  stem. When red-sweep-5 stood down naming dev-red-d12370de07-fix-case2 as its
  successor, `--to red-sweep` reached that successor instead of the live
  red-sweep-7, in routing, `cci post` notices, hook delivery, `cci tail --mail`
  and the writer label. Mail addressed to red-sweep-5 itself still reaches its
  successor. A red-sweep lane that binds later also hears the `--to red-sweep`
  mail that waited for it, which the successor's registration used to hide.
- `cci post --to <lane>` on a node that is not the owner, such as a Sprite,
  no longer warns that no lane holds a name its own registry has not seen. A
  Sprite reads the drive's lanes only as far as its relay copy has reached, so
  it warned `no lane holds obs-desk` while obs-desk ran on the Mac and got the
  mail. Notices that name the lane a post reaches still print there.
- `cci post` keeps a time that opens the text and names some other event, such
  as `4:40 PM backup gate: #51584 is still queued`. It replaced that time with
  the post time before. A leading time is a label only when a colon follows
  it, as in `4:40 PM: merged`, or the lane's own name sits beside it.
- Every time cci prints is Pacific, whatever zone the host runs in. On a UTC
  Sprite, the label hint, `{at}` and every reader's stamp printed UTC, such as
  `12:40 AM` for a post made at 4:40 PM Pacific. A Pacific clock or date given
  to `--since` is read as Pacific there too.
- `client.Open` and every other open-record read now name each record the way
  every node does: bare when an owner node wrote it, else `#<n>@<node>`. Before,
  they carried no cross-node name, so `Name()` on a node other than the Mac gave
  that node's own seq. lr's landing pass on a landing host named root's holds on
  #48034 and #50750 as `#197289` and `#330048`, which on the Mac are a register
  note and a HEAD line, so root lifted the wrong records.
- `cci watch --since` no longer replays older mail when it resumes. A watch the
  daemon resumed after a restart dropped `--since` and started from the
  cursor it saved at arm time, seq 0 when nothing had printed yet, so it
  printed every record addressed to its reader since the drive began. A watch
  re-armed with `--cursor <name> --since <time>` ignored the cursor and printed
  everything since `--since` again. Now `--since` bounds every read of the
  watch, and a named cursor that exists resumes it.
- A lane no longer reads a seed's backfill as new mail. A record that `cci
  relay seed` applies after the lane bound, stamped before the lane started,
  stays out of the lane's hook delivery, its `cci tail --mail` wait, its
  open-ask list, and its stop nudge. Before, every such record took a seq past
  the lane's cursor, so a Sprite lane bound before its copy finished read days
  of the drive's old records as mail. A record that `cci relay sync` or the
  relay stream delivers always reaches the lane, whatever its date, so
  instructions posted while a lane spawns still arrive. A record posted to a
  lane before it bound still arrives on its first binding.
- `cci relay seed` applies a whole drive and moves its sync cursors in one
  transaction. It committed every 2,000 rows and then once per origin for the
  cursors, and stored each row's pointers to its parent and its target with a
  second write. A seed cut off partway still keeps every row it read; a seed
  you cancel keeps none.
- A post kept while the bus was down now replays each `--attr` whole. The kept
  copy joined every attr into one `--attr` argument, so on replay the first
  attr's value swallowed the rest after its first `=`. A post carrying two
  attrs, one of them a refusal reason with `=` and `,`, was stored with one
  attr.
- A replica now stores a reply to a record the relay never carries, such as an
  owner's `lr.escalation`, which stays on the owner's machine. The reply used to
  wait for its target forever, and every reply to it waited behind it, so a
  landing host never saw a grade answering such a thread. The reply is stored
  with that pointer unresolved, its own replies thread under it, and the next
  pull, even one that brings nothing new, stores every reply an earlier version
  held this way.
- One lane name has one live poster across nodes. A thread's post as the lane
  it holds now names that thread on the record, so another node reads the lane
  as held for 15 minutes after its last post. A second thread there that posts
  as the lane, or claims it with `cci announce --lane`, is refused with exit 76,
  naming the holder and its node and the stand-down that passes the name on,
  the same refusal a second thread on one machine already got. Before, a Mac
  session and a Sprite copy both posted as restate-redis-1 for 40 minutes,
  because each node checked only its own sessions.
- A record posted again after its twin was resolved is stored as a new record.
  The duplicate check now skips twins another record resolved, so a withdraw
  re-posted after a cancel answered it is no longer returned as the cancelled
  record.
- A watch stopped before it read its start cursor now ends cleanly instead of
  returning `read cursor <name>: context canceled`. A channel stopped in its
  first moments, as `TestABatchHeldAcrossARestartArrivesOnce` does after 50 ms,
  failed with that error.
- `cci tail --mail --to <stem>` from a thread bound as a numbered lane that
  holds that stem now reads the lane's mail through the cursor the
  `PostToolUse` hook advances, as `--to <lane>` does. Before, it read the stem
  through its own cursor, so a record the hook had already shown the lane woke
  the lane's `lr wait` on its stem a second time.
- `cci watch` now follows across a daemon restart. When the daemon stopped for a
  newer install, it told each watch it had been cut short and sent exit 1, so a
  Monitor on the watch ended and the lane stopped hearing its mail. Now the
  daemon drops a cancelled watch's connection without a word and the client
  resumes from the watch's cursor on the next daemon, as it already did when the
  connection broke by itself.
- A read whose caller cancelled it now fails with the cancellation, not `cci
  store is busy`. A busy store waits for the caller's deadline, or 30 seconds
  when it set none, and only then reports busy; `read schema version: context
  canceled` after "busy" was the caller's own cancel, so it read as a store
  fault that was not one.
- `cci post` now says what to use when a lane reaches for `--subject`,
  `--title`, `--message` or `--summary`: a record has no line of its own beside
  its text, which is the one quoted argument or `--text`, with a longer body in
  a `--path` file. Before, the refusal read only `unknown flag: --subject`.
- `cci me` and `cci who` now print `gone, never seen` for a peer with no sighting
  and no status. Before, they printed `gone, last seen Dec 31 4:00 PM`, the zero
  time in Pacific, so a lane that had never announced read as one that had left
  at year's end.
- A hold whose text names a pull request without `--pr` is still refused, but the
  refusal now says a hold holds only its `--pr` targets and asks for `--pr` on
  each pull request the hold stops. Before, it told the poster to add `--pr` for
  every number the text named, so a hold on a writer that named its readers as
  context held the readers too. A hold with `--pr` whose text names other pull
  requests now posts with a line listing them as not held.
- `cci watch` now exits nonzero, saying it stopped, when it is cut short before
  its `--for` ends, as a signal or a stopped daemon does. Before, it exited 0
  with no output, so a Monitor on it read the silence as nothing happening.
- An ask now reads closed once the lane that asked it has ended, by a stand-down
  or by its own newest record being `done`, `stopped` or `handoff`, and no lane
  hears its mail: no live successor down its stand-downs and no live lane under
  its alias. Before, such an ask stayed open in `cci digest`, in every open-ask
  read and in the overdue checks built on them, and re-fired with nobody left to
  hear the answer. An ask whose lane handed over to a live successor, or whose
  alias another live lane holds, stays open for that lane.
- On a Sprite, `cci show <n>` and `cci post --re <n>` now find the record the
  same Sprite printed as `#<n>@<node>`. Before, a bare number there named only
  an owner node's record, so a lane that dropped the `@<node>` got "no record
  has this number" for its own post. A bare number an owner node's record holds
  still names that record.
- An ask stays open, and reaches its addressee's wait and hook, when its asker
  answers another lane on the same topic. Before, root's answer to owner-relay
  on a topic closed root's own ask to a lane on that topic, so `lr wait` and the
  hook skipped the ask as answered and moved the lane's cursor past it unread.
  An answer or go now closes an opener by topic or ref only when another lane
  wrote it; `--re`, `--resolves` and a withdraw close as before.
- `cci watch --lane <stem>` through the daemon now prints the records of every
  numbered lane that held the stem. Before, the daemon's wake read matched only
  the bare name, so a watch on `fix-mac` never woke for `fix-mac-2`'s records,
  while `cci tail --lane fix-mac` listed them.
- A session now has cci's tools loaded from its first turn and after every
  compaction. Before, they sat behind tool search, and a call made without first
  fetching the schema failed with "input schema was not in your prompt".
- On a replica, a lane now holds its alias whatever order the records arrived in.
  Before, a predecessor's backlog arriving after its stand-down read as the
  predecessor still running, so a lane on a fresh Sprite heard nothing addressed
  to its alias.
- `cci tail --to <alias>` and `grep --to <alias>` now read what the lane holding
  the alias reads, including mail addressed to its number.
- `cci post --to <alias>` no longer refuses as a slip when a numbered lane on the
  drive answers to that alias.
- `cci post --to <alias>` now stays on the drive it names when a lane there holds
  the alias. Before, a live lane of that exact name on another drive took the
  mail, with one notice line and a clean exit.
- `--lane <alias>` on `tail`, `watch`, `grep`, `state` and `who` now reads every
  lane that has held the alias. Before, it matched only a lane of that exact
  name, so an alias printed nothing and exited clean. A numbered name still
  reads that lane alone.
- `--to <lane>` with `--lane`, `--kind` or `--topic` now applies them to mail
  from another drive, which used to print whatever its writer or kind.
- A report now reaches every lane it names, in hook delivery, `--mail` and `lr
  wait`. Before, a lane named after the first addressee never heard it. Head,
  claim, state, matrix and review stay first-addressee only.
- A record addressed to a lane now leads a capped page of its mail, whoever wrote
  it, after root's own orders. A stand-down from the supervisor no longer waits
  behind a replica's backlog of broadcasts.
- A lane's hook, `--mail` and `lr wait` feed no alias mail from before its
  predecessor's newest own record, except orders written since the predecessor
  started, which it may have missed. An order sent while no one held the alias,
  or to one nobody ever held, still arrives, and `cci tail --since` reads the
  rest.
- A lane on a fresh Sprite no longer replays every old broadcast. The relay
  fills a new store with records numbered in arrival order, so a floor made of
  record numbers read them all as new. A lane now also hears a broadcast only
  if it was written since the lane started. Lanes bound on an older cci keep
  their floor alone until they bind again.
- A lane that stood down and was then named another lane's successor no longer
  holds that lane's stem. The successor's new row carried no stand-down, so a
  finished fallback read as the stem's newest live lane and the running lane
  never received the stem's mail on `cci tail`, hook delivery or `lr wait`.
  Registry reads now take a lane's stand-down from any of its rows, and `Reach`
  names the same lane delivery does. No schema or trigger changes.
- `Reach` no longer panics on a successor that was named and never registered.
- `cci hook stop` lets a launched lane's root thread end its turn on a live harness
  wait. A Stop payload listing a background task or session cron wakes the thread
  itself, so the turn is not refused and no blocker is written.
- `client.Follow` returns nil again when its context ends, on the daemon as on
  the store, and `client.Open` reads the same empty list through either.
- A lane holding a stem receives records addressed to that stem again when an
  older cci's refused post had registered the stem as a lane. `cci
  maintain-indexes`, which the daemon runs at start, drops the row a refused
  post alone left, so `cci tail --to`, `cci watch`, hook delivery and `lr wait`
  read the stem as the numbered lane's alias.
- `Reach` and `Holding` name the same holder as delivery. A stem reaches its
  newest live registration, even when a lane named the stem itself is older and
  live, so `client.Resolve` and a reader's stem labels no longer name the old
  lane.

### Changed

- `cci grep` takes its pattern as several words, joined by single spaces, as
  `ccn`'s search verbs do. `cci grep --drive durable-agents Send them ahead`
  used to refuse with `accepts 1 arg(s), received 3`; it now searches for
  `Send them ahead`, and a quoted pattern runs as before.

- The `using-cci` skill loads a 6 KB core instead of 41 KB: records and tags,
  reading mail, posting, answering and closing, and the kinds a lane posts
  daily. Everything else moved to seven files under `reference/` that the core
  names with a trigger each. Its description no longer asks a session to load
  it after compaction.
- The cci channel's MCP tool list, which every Claude Code session with the
  plugin loads, drops from about 13.7 KB to 6.5 KB, and its instructions from
  2.4 KB to under 1 KB. The `post` tool advertises the 21 fields lanes pass and
  no hidden flag; a `records` item takes every one of them but `lane`, `drive`
  and `records`, and a field the tool does not advertise is refused.
  A post that needs another flag, such as `--owned-by`, runs `cci post` in Bash.
  `post` and `close` default `lane` to the window's bound lane, `who` lists the
  window's drive, and `find` returns about 4 KB per query unless given `budget`.

- The daemon's write queue takes the paged writes of `cci import`, `cci
  compact` and relay sync: a page of imported lines, a reparse page, a
  compaction fold, a page of relayed rows, a backfill page and a settle page,
  each committed on its one writer like a post. Its batch stops gathering once
  it holds 64 records, so one import page never shares a transaction with
  another. No cci sends these yet. The release that switches the client
  ships after this daemon runs on every Mac and Sprite.
- `cci tail`, `cci grep` and `cci show` print the compact line the hook and the
  MCP tools print, `<seq> <kind> <lane>[><to>] [^<re>] [x<closed>] … <text>`,
  under an `@<clock>` line each time the minute changes. `--json` is unchanged,
  and a grep pattern still matches each record's long line. The compact line
  names a record by its number, so a replica prints the origin's number, not its
  local seq. Its minute header is added as the line prints, so a record the
  budget or a skip leaves first still carries its minute.
- A numbered lane that registers while its live predecessor holds the stem
  stands that predecessor down for itself at once: cci posts the
  `stand-down --successor` as lane `registry`, so mail to either name reaches
  the successor with no `lr request stand-down` step. The predecessor posts its
  handoff and stops. A second thread binding the same lane is still refused, and
  root still hears that `defect`.
- `cci post` stores a `--to` lane ending `-<n>` as its stem whenever the stem
  reaches that same lane, so the record follows the stem to whichever lane holds
  it next. A name the stem does not reach, such as a running older lane beside a
  newer one, an unregistered name or a lane on another drive, is stored as typed,
  and a stand-down or an `--owned-by` decide keeps the lane it is about. A
  reply's default recipient is stored the same way.
- A reader names a record's writer by the stem it holds: `cci tail`, `grep`,
  `show`, `watch` and the compact lines print `meta-desk` for a record
  `meta-desk-3` wrote while `meta-desk-3` is the stem's newest live lane, and
  keep the number for an older lane, one beside a newer holder, or one whose
  stem a live lane is named for. `--json` keeps `lane` exact and adds `stem`
  when the writer holds it. One registry read per drive per page decides it.

### Removed

- No clock in a post's text refuses it any more. A leading `H:MM AM/PM` label
  off the post's own clock is replaced with that time and a one-line hint on
  stderr, and any other clock is stored as typed without a future check.
  The MCP `post` tool's `stamp` field and `client.Draft.Stamp` are gone with the
  refusal they answered. `cci post --stamp` stays as a hidden flag that does
  nothing, since live lanes' briefs still pass it.

### Added

- A `github` pull request record names its author as `author` and its labels,
  comma-separated, as `labels`, so a reader can keep only its own drive's pull
  requests without reading GitHub. A check run record names its repository as
  `repo`, as a status record does, since a check run need not name a pull
  request.

- `service.Writer.Post` takes any number of records and seals them under one
  group key read and one push, so a receiver relaying a batch makes two relay
  calls instead of two per record.

- `client.Generations` lists every lane ever registered under a stem on a
  drive, live or stood down, from the lane registry's stem index, so a launch
  can number past them and never reuse a name.
- `client.Resolve` reaches a lane name as `cci post --to` does, through the
  same lane registry: a stem gives its newest live lane, a stood-down lane its
  successor, and the result names the stem's live holders. `client.Reach` and
  `client.ErrNoLaneRegistry` come with it, so a program in another module
  resolves a stem without a second resolver.
- `cci post --re`, `--resolves` and `--pr` take an alias: `t:<topic>`,
  `ccn:<id or text prefix>`, `card:<id>`, `pr#N`, or `last:<kind>[:<lane>]`.
  An alias resolves when the record is posted, to its one open record or else
  its one record, and a miss or a tie is refused with the candidates named.
  The record keeps the seq and the alias, and `tail`, `show`, `watch` and the
  channel print it as `^<seq>(<alias>)`. `cci find <alias>` prints the record an
  alias names. Lookups go through the `record_ref_keys` index (topic, ccn id
  and text, pull request, card id) and the new `records_drive_kind_lane` index,
  so they cost the same at any log size; an existing store gets both from
  `cci maintain-indexes`.
- `cci post --quoted <words>` with no other text stores the words as the
  record's text, unchecked for times and `{at}`, so an `owner` record keeps the
  owner's words exactly as he wrote them.
- A record's text that names an owner record, `owner:m<N>` or `slack:<ts>`,
  reads with that record's text quoted after the first mention of each id, in
  `tail`, `show`, `watch`, the hooks and the channel, and in `--json`. One
  query per drive per page finds every id. The stored text keeps the bare id, so
  the bus carries the id and the reader gets the owner's words. A node expands
  only ids its store holds: on a Sprite, owner records that reached it after it
  joined.
- `cci relay sync` resumes from a cursor per origin, so a restart reads only
  what it has not read, and it stays behind a record it could not read yet.
- A running daemon renews a delegated instance's delegation within two hours of
  its expiry with the service key the join kept, so a long-lived instance does
  not lose the relay at its expiry.
- `cci subscribe --batch-kinds` holds those addressed kinds off a window until
  one channel tag carries them together, at most every five minutes and at once
  ahead of any other record.
- `cci tail --to <lane> --mail` reads the bound thread's mail through the hook's
  cursor, so a wait and the `PostToolUse` hook deliver a record once.
- `client.Follow` follows a reader's records in process, as `cci watch` does,
  but delivers every kind addressed to the reader: `--reader root` keeps only
  the kinds a root acts on, so a `hold` or `report` addressed to root never
  reached a program that followed root through it. `client.Kinds` lists the
  kinds a lane can post.
- A lane name ending `-<n>` answers under its stem, the name without that
  suffix. `--to <stem>` reaches the stem's newest live lane: the one whose
  first post, thread binding or spawn record came last, or the successor a
  stand-down named. A stood-down lane's mail reaches the successor it named,
  else the stem's newest live lane; a running numbered lane keeps the mail
  addressed to it by name. Tails, watches, hook mail, open asks and a first
  binding's cursor follow the stem, and stored recipients stay as posted.
  `cci post` prints one line when no live lane reads a `--to` name or several
  running lanes share its stem, and `client.Draft.Notices` receives it.
  The lane registry behind this is a projection: a new store has it, an
  existing store gets it from `cci maintain-indexes`, and until then posts
  and reads behave as before. `client.Stem` applies the same rule in Go.
- A post's stand-down successors come from an indexed lane registry instead of
  a read of every stand-down record on the drive.
- A `github` kind for a GitHub pull request, review or check change or a push
  to the default branch, kept 14 days, and `github.Handler`, which turns GitHub
  webhooks signed with `X-Hub-Signature-256` into one `github` record per
  delivery. A push record names the commit it replaced as `before`, as a pull
  request push does, and a check record carries GitHub's check run or status
  id.
- `lr.head` takes optional `title`, `change`, `plan`, `risk`, `blocks`, `group`
  and `summary` text, the words the owner's merge board shows for a PR.
- `github.com/yasyf/cc-inbox/client`, a public Go package other modules import
  to post and read records: `Post`, `Tail` and `Open` use the store and the rules
  `cci` uses, so a Go tool never shells out to the binary.
- `cci announce --lane <lane>` binds the thread to a lane explicitly, and
  `cci subscribe --reader <lane>` binds the window's root thread to its
  reader; a session a `/clear` starts in that window keeps it. The explicit
  lane wins over the lane a first post or a teammate name inferred, so the
  `PostToolUse` hook injects and the `Stop` hook blocks on that lane's records
  only: a bridge window that once posted `--lane root` and subscribes as
  owner-relay no longer hears root's mail. Claiming a lane another live thread
  holds is refused, as a second binder's post is.
- `cci post --kind decide --owned-by <lane> --to <script lane>` records who
  owns a lane a script posts as, such as `stall-watch` or `desk-runner`. Later
  asks, decides and defects addressed to that lane also reach its owner and
  the owner's successors, so a defect filed to a script reaches the lane that fixes it.
  The newest such decide wins.
- The cci channel MCP server has a `post` tool that runs `cci post` with the
  arguments it is given, inside the server, so an agent posts without a Bash
  call and its shell and hooks. It writes as the window's thread, as a Bash
  `cci post` does, and resolves relative `--path` and `--body-file` values
  against an optional `cwd`.
- A `cci post` the store does not take within 15 seconds, or refuses as
  locked, tells its sender the bus is down and that the post was not stored,
  and exits 75. A post naming `--lane` and `--drive` is kept under
  `$CCI_HOME/unsent`, and the next `cci post` that reaches the store stores
  every kept post, oldest first, and says how many it stored. A kept post the
  store refuses moves to a `.refused` file beside it, and the post that found
  it says so. A post that names no `--lane` or `--drive`, or reads stdin, is
  not kept, and the error says to repost it.
- A `spawn` kind records a lane's start: the lane as `--lane-ref`, its brief
  and model as `--attr brief=` and `--attr model=`, and its effort and task id
  when known. It never expires, unlike the `state` record that carried
  `SPAWNED <lane>` before, and `cci who` lists the lane as spawned, naming the
  record, its brief and model, until a thread binds it. `--json` stays peer
  rows only.
- A `queue` kind for a pull request's merge-queue state change. It requires
  `--pr` and carries the state in `--outcome`.
- `service.Writer` posts records to a relay mesh as a scoped service member
  with no local store, and `trunk.Handler` turns Svix-signed Trunk
  merge-queue webhooks into one `queue` record per state change.
- `e2e.Key` and `e2e.ParseKey` hold a P-256 node or service key in memory;
  the node key is one, so an importer of `service` links no keychain code.
- `cci post --kind head --head <pr>=[<was>..]<sha>[ <note>]`, repeated, posts
  one head record for a restack that moves several pull requests. Each head
  lands in `fields.attrs` as `head:<pr>` = `[<was>..]<sha>[ <note>]`, every PR
  joins `refs.prs`, and the text opens with `HEAD <n> PRs: #<pr> <short sha>, ...`.
  The wire format is unchanged, so nodes on older releases keep replicating.
- `cci who --fields lane,live,seen_at` prints only those keys of each peer
  with `--json`, and `cci who --since 24h` includes the ended sessions last
  seen since then, so a supervisor that reads a drive's roster every pass
  reads only what it needs.

### Changed

- A numbered successor that binds beside its live predecessor now sends its
  `defect` to itself, never root, naming the `lr request stand-down` it runs once
  the predecessor's handoff is posted. A second binder of the same lane name is
  still refused and still reaches root.
- `cci post` refuses a clock later than the post time only when a past-tense
  word before it claims it already happened today. A clock with an earlier date
  beside it (`Nov 2`, `yesterday`, an ISO date) passes, and a bare clock with no
  past-tense claim reads as yesterday's and is stored with a stderr warning.

- A refused `cci post` files its `refused-post` blocker to the posting lane, no
  longer to root or the lane's owner. They get one only when the lane's retry is
  refused too, naming both refusals, or when the lane's turn ends with no stored
  post after the refusal. The lane's next stored post closes either.

- The cci daemon is the store's preferred writer. `cci post`, the hooks'
  session touches and cursor moves, and every other client write go to the
  daemon over its socket, and one writer commits every write already waiting
  in one transaction, each under its own savepoint, so concurrent clients no
  longer queue on SQLite's lock one commit at a time. A client whose daemon
  is down, draining, older than this release, or serving another home or node
  writes the store itself, as before. A write that misses its turn before
  its caller's deadline applies nothing; a `cci post` whose reply the daemon
  never confirmed is kept under `$CCI_HOME/unsent` as the exact write it
  sent, so the replay dedups against the record if it already landed.
- A migration the daemon runs at start on a store that holds data leaves a
  `note` in drive `cci`, lane `daemon`, naming the schemas and how long every
  client open waited behind `init.lock`.

- `cci who --json` leaves out each peer's `env` unless `--fields` names it.
- `cci claim --for <lane>@<drive>` falls back to the newest ended session
  bound as that lane when none is live, so the runner can record the codex
  lane that started a release build; a codex lane's session never announces a
  process and so never reads as live.
- An `incident` record that replies to or resolves an incident record is an
  update to that incident, never a new one: an evidence lane's progress line
  and root's closing line no longer each open an incident, so the digest's
  open incidents and every hook reading them count real incidents only.
- Text `cci tail --since` and `cci tail -n` reads keep the newest records when the
  budget cuts the window, and end with `... CUT <n> older records before
  #<seq>; --oldest reads them first`. Before, a window wider than
  its budget printed only its oldest records, so a watch for a new record in
  a fixed window never matched. `--json` reads still keep the oldest records
  and name the `--since` to resume from, since machine readers page by it.
  `--oldest` keeps the oldest records, as
  before. Cursor reads, `cci watch`, and the hooks are unchanged.
- `cci tail` reads past its 500-record page within one call: it keeps reading
  pages until its byte budget fills or the range ends, so a `--json` read with
  a large `--budget` returns every record in the range instead of stopping at
  500 and naming the rest only on stderr.
- `cci post` refuses a clock time in its text that is more than 3 minutes and
  less than 3 hours after the post time, since it names something that has not
  happened yet. A time after a tense word in the four words before it (by,
  until, till, before, next, ETA, due, expected, since), a time a date follows,
  and a time 3 hours or more ahead, which a clock without a date reads as
  yesterday's, still post; the last still warns. `H:MM:SS AM` reads as one time.
- `cci post` refuses a go withdrawal written as prose, such as a correction
  reading `WITHDRAW gos #274105 #274106`, and names the
  `cci post --kind withdraw --re 274105 --re 274106` command that posts it as
  one withdraw record per go. Parenthesized pull request numbers are left out
  of the named seqs.
- `cci post --to <lane>` accepts a lane that has not started yet without
  `--force`, and that lane reads the record when it first binds. Only a
  never-seen name that looks like a slip for a live lane, one or two edits
  from it or an older number of it, is refused, naming that lane; `--force`
  addresses it as typed.
- A subagent or teammate spawned with a name is bound as that lane by
  `cci me`, and its posts default `--lane` to the name. `cci me` registers a
  subagent the hooks have not announced yet, so it works as the first command.
- A lane that registers with its first `cci post --lane` reads the records
  posted to it before it started, as one bound by `cci me` does.
- `cci post --kind stand-down --to <lane> --successor <new>` records who took
  a lane over: later records addressed to the lane reach every successor too,
  `cci who` marks it `stood-down successor=<new>`, and
  `cci who --successor-of <lane> [--json]` prints the lane standing in now.
- A cci binary that an older plugin copy or PATH entry reaches hands off to
  the newest cci installed on the machine, plugin or cask.

- `cci subscribe --to-kind <kinds>` limits the records addressed to `--reader`
  that the channel and the reader's `PostToolUse` hook deliver. For `root` it
  defaults to `incident,decide,ask,blocker,urgent,answer,correction,defect`,
  so state, head, red, report, review, note and evidence records copied to
  the root stay in `cci tail` and `cci digest`. `cci watch` takes the same
  flag and default; `cci tail` and `cci grep` take the flag with no default.
- The root's default `--to-kind` also carries `mechanism`, `fix-live` and
  `owner`: a root reports an incident at its mechanism and at fix-live, and
  relays the owner's words.

### Fixed

- `cci hook stop` lets a launched lane end its turn on `landed` once it has no
  open PR, ask or blocker of its own, so a helper whose last record is `landed`
  stops without a refusal. A stop that cannot read the store passes with a
  warning instead of failing the hook.

- `cci show` with no `--drive`, on a node joined to a relay and bound to no
  drive, reads through on the one drive its relay carries, and names the
  drives when it carries several.
- A reply that reaches a replica for a record posted before the replica
  joined no longer waits forever: the relay pull reads the record it answers
  from the relay's log and stores both.
- On a node joined to a relay, `cci show` of a record the store lacks, such as
  one a Sprite replica missed because it was posted before the Sprite joined,
  reads it from the relay's log, body included, and prints it without storing
  it. `cci tail --since` from such a record reads that origin's records from
  the relay up to the store's first record of it, then the store's own, as one
  list under the usual filters and budget. A number no relay holds keeps its
  error.
- A Claude Code window whose cci channel started on an older release now sees
  the MCP tools a newer one adds. The channel re-executes the newest installed
  `cci` in every window, subscribed or not, and the resumed server opens with
  `notifications/tools/list_changed`, which Claude Code honors because the
  server now advertises `tools.listChanged` (cc-interact 0.40.0). Before, a
  window that never ran `cci subscribe` kept its first build for life, and a
  re-executed one kept the first build's tool list, so a drive's sessions
  saw no `post` tool and posted through Bash. A window whose channel started
  on 0.49.36 or earlier needs one restart to load a server that advertises it.
- The MCP `post` tool takes `stamp`, as `cci post --stamp` does: cci writes
  the leading H:MM AM/PM label from the post's own clock.
- A typed kind whose audience includes `lane`, such as `lr.item` and `lr.note`,
  reaches any lane by name again, and never a role such as `root`. The store
  refused every `lr.item` addressed to a lane.
- `cci who` reads only the registry rows its flags select, checks each
  process once however many rows name it, and reads a drive's stand-downs in
  one query, so `cci who --drive <d> --all --json` on a registry of 19,000
  threads no longer takes over a second of CPU. Every store read also stops
  re-deriving this node's id from its key on each row.
- A text `cci tail` or `cci show` numbers each reply once and joins it only to
  the records it read. A record that resolves 2,084 others had cost 2,084
  squared name lookups on every page showing any of them, so a 4 MB tail of a
  lane's blockers took 50 seconds and `cci show` of one such record 9.
- The `UserPromptSubmit` hook (`cci hook prompt`) answers within two seconds even
  when another process holds the store's writer or init lock, or a read
  stalls on disk. Past that budget it keeps the prompt under
  `$CCI_HOME/prompts`, and the daemon, or the next prompt hook, records the
  root's owner ruling once the store frees, so a prompt no longer waits out
  the hook's 10-second timeout and no detached process outlives the hook.
- The upgrade command for a standalone `cci` runs the new binary once before
  moving it over the old one, so macOS's first-launch hold, which hung every
  drive `cci` call for up to 22 minutes on Oct 31, lands on the copy no
  caller runs yet.
- `cci post` refuses to bind a thread to a lane another live thread holds,
  unless that thread is the one it answers to or the lane stood down. It posts
  nothing, names the holder, and posts root one `defect`. A numbered successor
  that binds beside its live, never-stood-down predecessor still binds, and
  root hears the same `defect`. Two sessions ran p8a-step7's work on Oct 31
  and Nov 1.
- A `stand-down` record stands down only its first `--to` lane. The lanes it
  copies, a successor it hands work to included, stay live in `cci who` and
  `cci who --successor-of`, which now names the stand-down's seq as `record`.
  Root copies most stand-downs to the lanes that act on them, so every copied
  lane read as stood down (board-refresher-6 and mq-health-9 on Nov 1).
- `cci post --kind withdraw --re <seq>` refuses a seq that names no record of
  the withdrawal's drive, such as a pull request number passed as the seq.
- A new lane's hook mail delivers records addressed to `all` only from the
  lane's spawn on. Its direct records still start at the oldest one waiting
  for it, so a lane spawned onto an old order no longer replays every
  broadcast since that order. A teammate whose first `cci post` binds its
  lane before its hook announces it starts at that post, never at its parent
  session's process start.
- A text `cci tail` or `cci grep` whose stdout is a pipe or a file also prints
  its `... more` or `... CUT` trailer on stderr, so a cut read piped into
  `grep` no longer looks complete.
- A root reader hears `release`, `done` and `blocked` records addressed to it.
  Run `cci subscribe --reader root` again, since a subscription keeps the
  addressed kinds it was made with.
- `cci tail --to <lane>` and a lane's hook mail include records addressed to
  `all` from other lanes.
- A bare record number that two nodes share in a drive is refused with both
  tagged spellings, never resolved to the local record.
- Root's records lead a lane's hook mail when a backlog from other lanes
  would push them past the budget.
- `cci post` refuses a go withdrawal written `Withdrawn: go #N on #PR` as well,
  and the `cci post --kind withdraw` command it names carries only the go seqs,
  never a pull request number that follows `on`, `for` or `of`.

- **A cci hands off only to an install that has already started once.** An
  older cci that finds a newer plugin or cask install starts it once in the
  background with `--version` and keeps running the call itself; the newer
  install records `cci.started` beside itself when it runs, and from then on
  every call hands off to it. A release macOS holds at its first launch no
  longer hangs every caller: on Oct 31 about 50 posts sat up to 22 minutes in
  the first exec of the 0.47.0 cask binary, and seven never landed. A warm-up
  that has not started after 10 minutes is tried again by the next call.
- **`cci upgrade` can no longer leave the Mac without a cci.** Each upgrade
  step runs in its own session, so killing the caller mid-upgrade leaves
  `brew upgrade --cask` to finish relinking `/opt/homebrew/bin/cci`. Before
  the cask step, it downloads the newest release and runs it once; a release
  macOS holds at launch for a minute stops the upgrade with the cask and its
  link untouched.

- **A store that never built the reply index builds it when cci opens it.**
  `cci grep` names `records_re` for its reply lookups, and only a fresh store
  or `cci maintain-indexes` created it, so an upgraded store, a Sprite replica
  among them, failed every grep with `no such index: records_re`. Opening the
  store now creates that index when it is missing, once, without changing the
  schema version, so older cci builds keep reading the store.

- `cci post --re <seq>` with no `--to` addresses the lane that wrote the record it
  answers, as `<lane>@<drive>` when that record is on another drive. A reply used
  to go to no one. A record whose every `--to` is the posting lane itself is
  refused, and the refusal names who wrote the record it answers.
- A record whose body sits in a file (`--path`, or text over 400 characters)
  now carries that file to every node it replicates to, up to 64 KiB cut at a
  line, and the receiving node stores it in its own blobs and names that copy.
  A reader on another machine used to see only the 400-character excerpt.
  The file rides in a new `path_body` field, so the mesh wire format moves to
  4 and nodes on different versions refuse each other's changes until both
  upgrade.

## [0.41.0] - 2026-10-29

### Changed

- `cci post --kind head --ccn <note id>` records a head that names a note,
  such as an experiment's method note, with no pull request. A head with
  neither `--pr`, `--ccn`, nor one `#N` in the text still refuses.

## [0.40.0] - 2026-10-29

### Changed

- `cci post --to <lane>` refuses a bare lane name that no live session reads,
  no thread is bound as on the drive, and no record on the drive was posted
  as, and names the nearest live lane. `--force` addresses a lane that has not
  started yet; `owner` and `all` always pass.

## [0.39.2] - 2026-10-29

### Changed

- `cci post` refuses a text whose leading `H:MM AM/PM` label is more than a
  minute off the post's own clock, ahead or behind, with a one-line message.
  `cci post --stamp` writes the label from the clock instead: it replaces a
  typed leading label, or opens the text with one.

### Fixed

- `cci tail --json` prints its truncation trailer on stderr when its budget or
  page cut records, so stdout stays one JSON record per line and a cut read
  still says so.

## [0.38.0] - 2026-10-28

### Fixed

- `cci post --kind opened` without `--pr` takes the pull request from a single
  `#N` in the text and says so on stderr, as `head` does; with none, several,
  or `#0` it refuses and asks for `--pr`.

## [0.37.0] - 2026-10-28

### Fixed

- `cci post --kind head` without `--pr` takes the pull request from a single
  `#N` in the text and says so on stderr; with none or several it refuses and
  asks for `--pr`. A head with no pull request is invisible to the desks that
  key heads by pull request.

## [0.36.0] - 2026-10-28

### Fixed

- A lane's `PostToolUse` hook delivers `head`, `review`, `state`, `report`,
  `claim` and `matrix` records only when the lane is their first addressee;
  a lane merely copied on one reads it with `cci tail --reader`. A lane copied
  on hundreds of reviews fell hours behind, three or four records per tool
  call.
- `cci tail --reader <lane>` from the thread bound to that lane moves its hook
  delivery past what the tail read, so the hook never repeats it.
- `cci post` rewrites a time label ahead of the post time to the post time
  when the label follows up to four words and ends in a colon, or follows the
  lane's own name, as in `board 1:09 AM:`; before, only a label that opened
  the text was rewritten.
- The daemon cuts HTTP streams still open after half its drain budget, so a
  stop or an upgrade handoff finishes instead of holding its lock and leaving
  port 7377 refusing connections for minutes.
- A `SessionStart` hook running an older cci hands the daemon check to the
  newest install, so an old plugin no longer replaces a newer daemon and sets
  off repeated handoffs.

## [0.35.0] - 2026-10-28

### Fixed

- The Homebrew cask strips quarantine with a `postflight_steps` stanza instead of the Ruby `postflight` block Homebrew 7 deprecated, so `brew upgrade` no longer warns, and only on macOS.
- `cci post` without `--lane` no longer files the record as `root` or as the
  window subscription's reader. A subagent whose `CCI_AGENT_ID` was lost reads
  as the root thread, so its posts carried root's authority; a thread bound to
  no lane is now refused with an error naming `--lane`, and the root passes
  `--lane root`.

## [0.34.0] - 2026-10-27

### Added

- `cci subscribe --mute-kinds <kinds>` keeps those kinds off the channel and
  out of the subscribed root's `PostToolUse` delivery, even when addressed to
  the reader. `cci tail`, `watch` and `grep` take the same flag; without it
  they still read every muted record.
- An `urgent` record whose lane posted the same `--topic` as `urgent` within
  the previous 30 minutes is delivered by neither the channel nor the hook.
  `cci tail` still reads it.
- `cci me` prints the window's channel subscription under its registry line.

## [0.33.0] - 2026-10-27

### Changed

- `cci tail --reader <lane>` with a cursor prints the records addressed to the
  reader first when its backlog does not fit in one read. Such a record names
  the reader in `--to`, or replies with `--re` to one of the reader's own
  records. Peer mail keeps its place in seq order. Broadcasts
  follow in seq order, and a trailer says how many came first. A second
  cursor remembers what was printed early, so a later read never prints it
  again and the resume trailer never skips it. A backlog that fits still
  prints in seq order.
- `--reader` now also delivers a record that replies to one of the reader's own
  records, whoever it is addressed to.

## [0.32.2] - 2026-10-27

### Changed

- `cci post` warns on stderr about every time in its text that is more than a
  minute later than the post's own clock, such as `read 3:44 PM` in a post made
  at 3:42 PM, and stores the text as typed. Only a leading label is rewritten.

## [0.32.1] - 2026-10-27

### Fixed

- `cci post -` and `cci post --text -` read the text from stdin, so a heredoc
  body becomes the record's text. Before, the record stored a literal `-`.

## [0.32.0] - 2026-10-27

### Changed

- `cci post` rewrites a leading `H:MM AM/PM` label in its text that is more
  than a minute ahead of the post's own clock to the post time, and prints
  `cci: time label 3:20 PM rewritten to 3:17 PM` on stderr. A label at or
  before the clock stays as typed, and a label anywhere but the start of the
  text is never touched.

## [0.31.0] - 2026-10-27

### Changed

- The `PostToolUse` hook prints a lane's open asks only when the set changed
  since the last delivery. It no longer adds a "unchanged since the last list"
  line to every delivery, and a set that emptied and then refilled prints the
  one-line seq list instead of the full list again. Only a session start or a
  compaction prints the full list.
- The `PostToolUse` hook delivers only the newest `state` or `report` record on
  a topic. When a lane posts several to the same recipients under one topic
  before the reader's next tool call, such as a runner's heartbeat or its
  report about one PR, the earlier ones are skipped. A record with no topic is
  always delivered, and `cci tail`, `cci watch` and the channel still print
  every record.
- Default views print a record addressed to more than eight lanes as
  `-> <n> lanes` instead of its first eight recipients and `+<n> more`. The
  first eight names of a broadcast to every lane told the reader nothing, and
  they were half of each such line in every lane's hook. `cci show` and
  `cci grep` still print every recipient.

## [0.30.3] - 2026-10-27

### Fixed

- `cci post` no longer answers `(duplicate)` for a record whose text matches an
  earlier one but whose refs, fields or body differ. The dedup key now also
  covers the refs (`--pr`, `--url`, `--board` and the rest, but not `--path`),
  the fields (`--commit`, `--outcome` and the rest) and the body, so a post with
  a corrected `--commit` creates a new record instead of keeping the old sha.

## [0.30.2] - 2026-10-27

### Fixed

- A post or read with no `--drive` from a subagent whose own thread never ran
  `cci drive use` takes the drive of the nearest thread that spawned it, up to
  the session's root thread, so a fresh lane's first `cci post` works. A
  session bound to no drive takes `CLAUDE_LONG_RUNNING_DRIVE` when it is set.

## [0.30.1] - 2026-10-27

### Fixed

- `--reader` on `cci tail`, `cci watch`, `cci grep`, `cci state` and the
  channel delivers a record posted `--to all` as a broadcast, subject to
  `--kind`, `--lane` and `--topic`. It used to deliver only records with an
  empty `to` list, so a lane reading with `--reader` missed the root's
  drive-wide rulings. `--to` is unchanged.

## [0.30.0] - 2026-10-27

### Changed

- `cci tail`, `cci watch`, the channel, the hooks, `cci digest` and `cci state`
  name a record's first eight recipients and count the rest, as
  `-> a,b,c,d,e,f,g,h,+45 more`. A record sent to 50 lanes used to fill the
  hook's 400-character line with names and show none of its text. `cci show`
  and `cci grep` still print every recipient.

## [0.29.0] - 2026-10-27

### Changed

- `cci tail`, `cci watch`, the channel, the hooks, `cci digest` and `cci state`
  leave out a record's `[topic]` when its text already carries that topic as a
  word, and a `pr#N` or `pr:owner/repo#N` ref when its text already names that
  pull request. A line the view clips keeps every field, since the clip may
  cut the word that named it. `cci show` and `cci grep` still print every field.
- After a session's first full list of open asks, the `PostToolUse` hook names
  a changed list by seq, lane and age instead of printing each ask's text
  again; each ask's text was already delivered as its own record. A session
  start or compaction lists them in full.

## [0.28.0] - 2026-10-27

### Changed

- A root thread whose window runs the cci channel gets each record addressed
  to it once. The channel moves the `PostToolUse` hook's lane cursor past
  what it delivered, and skips a record the hook delivered first. A window
  launched without `--channels plugin:cc-inbox@...` keeps both paths as before.
- The hook lists the open asks a lane owes only when that set changed since
  its last list, and otherwise prints one line counting them. The first
  delivery after a session start or compaction lists them in full.
- A channel tag's body drops the `#seq time KIND lane` header the tag's
  attributes already carry, and keeps the time and the rest of the line.
- Text `cci tail` cuts a record a later one resolved to 160 characters and
  counts the cut lines in a trailer; `--all` prints them whole.

### Fixed

- `cci post` without `--lane` refused with `required flag(s) "lane" not set`.
  It now posts as the thread's bound lane, else its channel subscription's
  reader, else `root` for a drive's root session.

## [0.27.2] - 2026-10-26

### Fixed

- A teammate that took over a lane got the lane's whole history replayed by
  the `PostToolUse` hook: a teammate shares its root session's window, so its
  lane cursor started when the root session started. The cursor now starts at
  the later of the window's start and the teammate's spawn, read from the
  harness's agent metadata, so a record posted between spawn and the first
  tool call still arrives.
- The hook no longer delivers records a later record has closed, under the
  store's own closure rule (`--resolves`, `answer --re`, a matching topic),
  and still moves the cursor past them. A page whose records were all closed
  keeps its resume trailer, which says "up to N more" while closed records
  are skipped. `cci tail` itself still prints closed records.

## [0.27.1] - 2026-10-26

### Fixed

- `cci claim --for <lane>@<drive>` refused a teammate that had not posted yet
  with "no live session on this machine is bound as lane": a teammate is
  announced at spawn under its agent name, but its thread takes a lane only on
  its first `cci post`. When no session is bound as the lane, `--for` now
  takes the one live teammate named that lane with no lane yet, whose session
  is unbound or in that drive, binds it to the lane the way `cci post` does,
  and claims for it. A root thread or a teammate of a session in another
  drive is never adopted.

## [0.27.0] - 2026-10-26

### Added

- `cci show SEQ...` prints records whole, in the order named: each record's
  line with no width cut, then the full text a record over 400 characters
  spilled to its blob, or the body file it was posted with. A lane that had
  only a record id used to page `cci tail --since` with a wide `--width`,
  grep for the line, and `cat` the blob by hand.

## [0.26.0] - 2026-10-26

### Fixed

- `cci who --pr` ran for minutes at full CPU on a large store and never
  answered: the claims it derives from records paired every session with
  every record before filtering by the pull request. Each derived branch now
  filters the record's refs first, and a new `sessions(origin, drive, lane)`
  index answers the session lookup. On a store of 117k records it answers in
  about 0.2 s. The store schema moves to 14, so mesh nodes on different
  versions refuse each other's changes until both upgrade.
- A pull request opened by a lane's subagent was recorded under the
  subagent's agent id. `cci claim --rel opened` now posts the `opened` record
  as the lane that owns the thread: the thread itself when it is bound as a
  lane, else the nearest bound agent that spawned it, up to its session's
  root thread. The `PostToolUse` hook records each subagent's parent from the
  harness metadata. `cci who --pr` lists a lane subagent's claim under the
  lane's thread.
- `cci claim --for <lane>@<drive> --pr N` also posts a `claim` record as that
  lane, reading `claim: <lane> owns #N`, so a reader of the drive's records
  learns the owner by its lane name.
- A bare `--pr N` matched only refs stored as that bare number, missing every
  `owner/repo#N` ref. It now matches N in any repository, in `tail`, `grep`,
  `digest`, `state` and `who`.
- A post with another `--lane` rebound the posting thread to that lane, so it
  started receiving that lane's mail. A thread's lane is now set once, by its
  first post as itself; `cci drive use` still resets it.

## [0.25.0] - 2026-10-26

### Fixed

- `cci post --resolves` with several seqs wrote one full copy of the record
  per seq, so a three-seq closure woke every addressee three times. The post
  now writes one record that resolves every seq it names: `tail` marks each
  closed record `[RESOLVED #<seq>]` with that one seq, the open sections of
  `digest` and the stop hook drop all of them, and the record's line reads
  `resolves #<a>,#<b>,#<c>`. The extra seqs live in the record's
  `also_resolves` list and replicate across the mesh as pointers, so the mesh wire format moves to
  3 and nodes on different versions refuse each other's changes until both
  upgrade. A repeated `--re` still writes one record per seq.
- Text over 400 characters spilled to a blob and the record kept only its
  first line, so a one-paragraph post arrived as its first clause. The record
  now carries the first 400 characters, whitespace collapsed and cut at a
  word with `…`, followed by the blob path.

## [0.24.2] - 2026-10-25

### Fixed

- `cci post` with a repeated `--resolves` or `--re` kept only the last seq
  and dropped the rest without a word, so a sweep that passed 40
  `--resolves` closed one record. Both flags now accumulate, and a comma list
  works too: the post writes one record per seq, with the same text, and
  prints each record's seq. An `--id` becomes `<id>#<seq>` on each record so
  a repost stays idempotent. A post that repeats both flags is refused.

## [0.24.1] - 2026-10-25

### Fixed

- The open-asks section and the `Stop` block count only records the lane
  owes: an ask, a blocker, or a decide ending in a question mark, whose first
  addressee is the lane and whose author is another lane. A status decide
  that copies the root, or an ask that names the root after the lane that
  owes it, no longer counts. One drive's root had 415 such records listed.
- The `Stop` hook's first run in a thread records the drive's head and blocks
  nothing, so records posted before the hook was installed never block a
  stop.

## [0.24.0] - 2026-10-25

### Added

- A lane's `PostToolUse` delivery leads with the asks, decides and blockers
  addressed to it that nothing has answered or resolved, newest first, five
  at most, each with its age in minutes. An ask posted now no longer waits
  behind hours of older records replayed three at a time from a lagging
  cursor.
- A `Stop` hook blocks the stop once when an ask, decide or blocker
  addressed to the lane has waited ten minutes unanswered. Its reason names
  the newest such record, its lane and age, and `cci post --resolves <seq>`.
  Each record blocks one stop; the next stop passes.

### Fixed

- `cci subscribe` and `cci unsubscribe` refuse to run in a subagent or
  teammate. The subscription belongs to the Claude Code window, which its
  in-process agents share, so a teammate subscribing as its own lane replaced
  the root's subscription. The root then received only that lane's records
  and broadcasts, and asks addressed to it stopped arriving for hours.

## [0.23.0] - 2026-10-22

### Added

- `cci claim --for <lane>@<drive>` and `cci unclaim --for` act on the claims
  of the one live local session bound as that lane in that drive, instead of
  the caller's. A desk or runner process that reads a lane's `release --build`
  record can now claim the build for the lane, so `cci who --build` names the
  lane that started it rather than the runner. The flag refuses a lane with
  no live session or with more than one, and needs no session of the caller's.

## [0.22.2] - 2026-10-19

### Fixed

- A record filter's `--pr owner/repo#N` now also matches records that name
  the pull request as the bare number `N`, which says nothing about its repo.
  A reader can ask for one repo's pull request with one query and still find
  the records written before their posters qualified them, as `ccx vcs ship`'s
  `cci claim` now does. A bare `--pr N` still matches only the bare number.

## [0.22.1] - 2026-10-19

### Fixed

- `cci post` from a Codex session now binds the session's lane, as it
  already did for Claude Code. It found the session only through a Claude
  ancestor, so a Codex worker's posts left it unbound. `cci claim --rel opened`,
  which `ccx vcs ship` runs, then found no binding and posted no `opened`
  record. The long-running incident clock never saw a Codex fix lane's PR and
  launched an Opus fallback for a PR that already existed.

## [0.22.0] - 2026-10-18

### Added

- A root can message a running Workflow tool agent. The session hooks now name
  each workflow agent by its label and announce it with role `workflow`, so
  `cci who --role workflow` lists them. A subagent bound to no lane gets the
  records addressed to its peer address in its next tool result, with no
  `cci subscribe`. `cci post --to peer:<agent id or label>` reaches one of the
  posting session's own agents. Before, a workflow agent had no name in the
  registry and never received records, and `SendMessage` by its id resumed a
  second copy of it.

### Changed

- `--to peer:` resolves among live sessions and agents first, then among
  ended ones, so a label reused by a finished workflow run still names the
  running agent.

## [0.21.2] - 2026-10-18

### Fixed

- `cci post --path /dev/stdin` now keeps the body. Before, the record stored
  `/dev/stdin` as its path, so a reader opening it got its own stdin and the
  body was gone; with `--text`, cci never read stdin at all. `--path -` failed
  with no such file. cci now reads `-` as stdin, copies `-`, `/dev/stdin`,
  process substitutions, and any other path that is not a regular file to a
  file under `$CCI_HOME/blobs`, and stores that file's path. A regular file's
  path is stored as an absolute path, a missing file is refused even with
  `--text`, and an empty body is refused.

## [0.21.1] - 2026-10-17

### Fixed

- Deciding which asks, decisions, holds, incidents, blockers, and defects are
  still open no longer checks every opener against every closer. Before, each
  read compared each opener in the drive with each closer, so one drive with
  5,277 openers and 7,010 closers ran up to 36,991,770 pair checks per read. A
  digest from the daemon or the session hook ran two of those reads, one for
  open items and one for untracked holds, and marking the open records among
  printed lines ran a third. cci now indexes the drive's closers once per read
  by the record they reply to, their topic, their stacks, and their targets,
  and checks each opener only against the closers it shares one of those with.
  The same records count as open as before.

## [0.21.0] - 2026-10-17

### Changed

- The mesh `records` item fires only for `inbox.db` and `inbox.db-wal`.
  Before, synckitd watched the whole cci home recursively, so every blob,
  subscription, relay, or `mesh/revisions.json` write woke the item and cost
  a `List` call, and its 30-second sweep listed every file in `blobs`, 9,913
  on one host. cci now declares the home with synckit's `watch_names`, which
  synckitd 0.42.0 and newer honor by watching the home without its
  subdirectories. An older synckitd ignores the field and keeps the recursive
  watch, so cci and synckitd can upgrade in either order.
- A first `cci mesh enable` registers a 5s watch debounce instead of 1s.

### Fixed

- `cci mesh enable` keeps the watch debounce `synckitd` already has registered
  for `cci`. Before, every run wrote a fresh manifest with a 1s debounce and
  `synckitd register` replaced the registered copy, so a debounce tuned in
  `~/.config/synckit/manifests/cci.json` reset to 1s. A registered manifest
  that fails to load now stops the command instead of being overwritten.
  Every Mac still runs `cci mesh enable` after upgrading, as the fingerprint
  entry that follows requires, and that run no longer resets a tuned debounce.
- The mesh schema fingerprint covers the wire format of a change as well as
  the store migrations. v0.17.0 started sending repo-qualified pull requests
  in `refs.prs` as `owner/repo#N` strings and kept the fingerprint, so a peer
  running an older binary accepted the change and then failed to decode it
  with `json: cannot unmarshal string into Go struct field Refs.rows.refs.prs
  of type int`. A peer on another wire format is refused at the fingerprint
  check instead. Every Mac must run `cci mesh enable` after upgrading, so synckitd
  registers the new fingerprint. Until both ends of a host pair upgrade, that
  pair's lane sits in synckit's backoff with the schema-mismatch error; the
  sender keeps the change staged, so nothing is lost.

## [0.20.7] - 2026-10-16

### Changed

- The `--pr` refusal from 0.20.6 applies to `hold` records only. `blocker`
  and `blocked` records cite record sequence numbers as `#<seq>` all the time,
  and those look like pull request numbers, so refusing them stalled lanes.
  A hold that names `#<number>` without a matching `--pr` is still refused.

## [0.20.6] - 2026-10-16

### Changed

- `cci post` refuses a `hold`, `blocker`, or `blocked` record whose text or
  topic names `#<number>` without a matching `--pr <number>`. Merge-queue
  guards bind a hold through the record's pull request refs, so a hold that
  named its PR only in prose went unseen. The error names each missing
  `--pr`. A record that names no PR still posts as before.

## [0.20.5] - 2026-10-16

### Fixed

- `GET /v1/records` and the `--json` reads (`cci tail`, `watch` and `grep`)
  work out each record's `status` from the openers on the page. Before, every
  call loaded every opener each of the page's drives had ever held and matched
  each one against every closer, so the work grew with the drive's history,
  not the page. Now each call rereads only the page's tracked openers by seq
  and matches them against their drive's closers and resolvers, all read
  fresh, and a drive whose page records open nothing tracked is skipped. An
  opener reparsed or compacted after the page was read gets the same `status`
  as before. A malformed opener elsewhere in the drive's history no longer
  fails the page.

## [0.20.4] - 2026-10-15

### Fixed

- cci redacts GitHub and Buildkite tokens before it stores a record. Every
  write (`cci post`, imports, hook posts, and rows replicated from a peer)
  replaces `ghp_`, `gho_`, `ghs_`, `ghu_`, `ghr_` and `github_pat_` tokens with
  `[redacted:github-token]`, and `bkua_` tokens with
  `[redacted:buildkite-token]`. That covers the text, topic, refs, fields and
  body, and the blob `post` writes for text over 400 characters. Before, a
  token pasted into a post sat in the store and reached every node the mesh or
  relay replicated it to.

## [0.20.3] - 2026-10-15

### Fixed

- A record addressed to a bare lane that reads another drive now reaches it.
  `cci post --to <lane>` checks the live threads on this node. When none
  writes as that lane in the posting drive and one does in exactly one other
  drive, the recipient becomes `<lane>@<that drive>` and the post says so on
  stderr. That lane's `tail --reader`, `watch`, `subscribe` and hook reads
  already deliver `<lane>@<drive>`. When live threads read it in several other
  drives, the post fails and names them. Before, a GO posted on one drive to a
  lane bound to another sat unread until someone grepped for it.
- `cci post --drive` no longer moves the session's drive binding. A lane that
  posted once with `--drive` on another drive had every later bare post land
  there. The binding moves only through `cci drive use`; a session with no
  binding still binds on its first post.

## [0.20.2] - 2026-10-14

### Fixed

- `cci drive use` works from a Codex session. With `CLAUDE_CODE_SESSION_ID`
  unset, the session is `CODEX_THREAD_ID`, the key `cci announce` already
  used for Codex lanes, so the binding holds and later `post`, `tail`,
  `watch` and `digest` calls resolve the drive and default cursor without
  `--drive`. Before, it refused with `CLAUDE_CODE_SESSION_ID is not set`.

## [0.20.1] - 2026-10-13

### Fixed

- The `PostToolUse` hook no longer redelivers a `peer:<session>` record after
  the lane's session posts on another drive. Each drive keeps its own lane
  cursor, and a fresh one starts at the session's start, so every peer record
  since then came back in pages of three, once per newly used drive. A read
  now skips peer records at or before the furthest position the reader's
  cursor reached in any drive.

## [0.20.0] - 2026-10-13

### Changed

- `post` writes the record's own time into its text. A clock typed in a
  leading `R<n> (<clock>)` stamp, such as `R84 (10:59 AM)`, is replaced with
  that time in the form readers print, and `{at}` anywhere in the text
  becomes it. The stored text, a spilled file, and the duplicate checks read
  the stamped text. Only text given as `--text` or the one argument is
  stamped. A post that takes its text from a `--path` file stores the file's
  first line as written, so the record's text still equals that line. A
  keyless repost that carries a stamp or `{at}` is a duplicate only within
  the same clock minute.

## [0.19.0] - 2026-10-13

### Added

- `claim --rel opened --pr N` from a drive-bound session posts an `opened`
  record for each PR on that drive, with the caller's bound lane, `root`, or
  the subagent's id as its lane. `ccx vcs ship` already makes that claim, so
  every PR it opens reaches the drive's landing desk without a hand-posted
  `opened` record. A repeated claim posts nothing new.

## [0.18.0] - 2026-10-12

### Changed

- `post` text over 400 characters no longer refuses: the full text spills to
  a file under `$CCI_HOME/blobs/`, the record carries its first non-blank
  line with `refs.path` naming the file, and `post` prints the path on
  stderr. Two spilled posts that share a first line stay two records.

## [0.17.0] - 2026-10-11

### Added

- `--pr` on `post`, `claim`, `who`, and the read commands takes `owner/repo#N`
  or a GitHub pull request URL as well as a bare number, so a record can name
  a PR in another repo. A bare number is still stored as a JSON number and a
  qualified PR as an `owner/repo#N` string; lines render it as
  `pr:owner/repo#N`.

## [0.16.2] - 2026-10-11

### Fixed

- `cci watch` runs until it is interrupted. It used to exit with status `0`
  after 29 minutes by default, which ended a persistent Monitor silently and
  dropped records posted before the next arm. `--for` still sets a limit.

## [0.16.1] - 2026-10-11

### Fixed

- `cci relay enroll` signs an owner grant for the member it admits, as
  `cci pair accept` does. The relay forwards it on `peer.joined`, so every
  other owner pins the new service or owner on its next pull instead of
  running `cci relay join --service` by hand. A relay without relay-api v3.5
  ignores the grant.

## [0.16.0] - 2026-10-11

### Added

- `cci relay found` founds a relay mesh as its first owner. It sends the STS
  caller proof that `ci cci-relay prove` prints, with this node's Secure
  Enclave endorsement, then joins as owner. Before it, no client could create
  a mesh, so `cci relay join` had no member to join as.
- `cci relay enroll` admits a pinned owner node or a scoped service key to a
  relay mesh. The owner approves with Touch ID through `synckitd consent
  request`, and the command shares the current group key with the new member.

## [0.15.0] - 2026-10-11

### Added

- `cci upgrade` moves every cci install on the machine in one command: the
  plugin through `claude plugin update`, the Homebrew cask through
  `brew upgrade --cask`, and each standalone `cci` on `PATH` from the release
  asset. It then runs `serve` from the newest install so the daemon moves too.
  Run it once after a release workflow finishes.
- The daemon follows upgrades by itself. At start and every 30 seconds it
  reads the plugin cache and Homebrew cask version directories, and hands off
  to a newer release's `cci serve`, which drains it and starts the newer build.
  The daemon copy no longer stays behind until someone runs `cci serve`.

## [0.14.0] - 2026-10-11

### Added

- `cci pair` lets a machine outside the owner's synckit mesh join a mesh as a
  read-only node over the relay. It prints a one-time code and its node
  fingerprint. `cci pair accept CODE` on an owner shows that fingerprint and
  asks for Touch ID through `synckitd consent request`, here or on an attended
  owner host. `--tier service --allow --label` admits a scoped writer instead.
  The accepting owner signs a member grant and its owner roster, so no node
  pins a key on the relay's word alone (#64).
- `cci unpair NODE` revokes a node through the relay and rotates the group
  key. The daemon acts on relay-signed `peer.unpaired` controls on every
  node, so a write from a paired node unpairs it everywhere with one key
  rotation. `cci peers` lists trusted nodes with their tier, and `--mesh`
  reconciles that list against the relay's directory (#64).

## [0.13.0] - 2026-10-11

### Added


- `cci relay join|push|pull|sync|rotate|status` replicates records through the
  wide-area relay, end-to-end encrypted so the relay reads no record body.
  Each mesh has a group key that an owner node wraps to every pinned member
  with HPKE (P-256) and signs; a node accepts a wrap only from an owner it
  pinned itself. Record bodies are AES-256-GCM under that key, and the routing
  header (mesh, drive, origin, `origin_seq`, tier, kind, time, epoch) is
  signed by the writer and bound as additional data. Paired members never
  push, rotation is an epoch compare-and-set that locks out an unpaired node,
  and `cci daemon` runs a push and a pull loop per joined mesh (#62).
- Delegated service instances: `cci relay join --instance --drive D` mints a
  per-job key that `$CCI_SERVICE_KEY` signs into a scoped, expiring
  delegation, so concurrent CI jobs sharing one service write under their own
  origins (#62).
- The public `e2e` package holds the wrap, envelope, delegation and control
  formats for other Go implementations of the relay (#62).

## [0.12.0] - 2026-10-11

### Added

- `cci mesh enable` replicates records across the owner's Macs through
  synckit: the daemon runs the resident consumer `cci`, `synckitd` delivers
  node-signed deltas and bounded snapshots to every mesh host, and a receiver
  applies only changes signed by the owner node it pinned for the delivering
  host, fenced by its per-origin receipt. `synckitd host add` and `host enroll`
  pin each host's node key. `cci mesh status` shows what each host acked (#61).

## [0.11.0] - 2026-10-11

### Added

- Multi-origin records for the cross-machine mesh. Every record carries
  `origin`, the id of the node that wrote it, and `origin_seq`, its seq
  there; `(origin, origin_seq)` is its identity on every node. `Store.Export`
  and `Store.Apply` move records between nodes. Apply rewrites `re` and
  `resolves` to local seqs and holds a reply until its target arrives.
  Transport receipts, per-peer acked watermarks, pinned members, and
  replicated sessions and claims live beside them. Compaction never folds a
  record a peer has not acked, and a folded remote record stays refused if a
  transport replays it. Schema 11; older binaries refuse the store (#58).
- Records for services: a dotted kind such as `oncall.fix.status` is a
  namespaced service kind with a 14-day TTL, `cci post --attr k=v` fills
  `fields.attrs`, and `--body-file` attaches a JSON `body` of up to 64 KiB.
  A post whose `--id` the drive already holds returns the stored record.
  `cci cursor set` moves a cursor backwards for a reader that restarts. The
  channel tag and text `cci watch` print a record's body under its line, and
  the tag's meta carries its `id`.
- `--to peer:<node>/<session>` addresses a session on another node, `cci who`
  lists replicated sessions, and `--origin` narrows reads and `who` to one
  node.

### Fixed

- Compacting a drive deleted a resolver whose target lives in another drive
  unless the resolver was itself addressed across drives, so the other
  drive's digest reported the item open again. Compaction now keeps every
  resolver whose target sits in another drive.
- `cci state` kept the record that arrived last for a kind, lane and topic;
  it now keeps the newest by time, so a late replicated record cannot mask a
  newer local one.

## [0.10.4] - 2026-10-11

### Added

- Subagents and teammates are peers of their own. Every session thread is
  keyed by its session and the harness's `agent_id`, so `cci who` lists each
  lane of a session apart as `session=<session>/<agent name>`, and `cci who
  --pr` names the lane that holds a claim. The plugin's hooks announce a
  subagent on its first tool call, name it from the harness's agent metadata,
  deliver its own lane's records, and retire it on `SubagentStop`. `cci me`,
  `cci claim`, `cci unclaim`, `cci drive use`, a lane's lane binding and the
  default cursor act as the thread `CCI_AGENT_ID` names beside
  `CLAUDE_CODE_SESSION_ID`; without it, as the root thread.
  `peer:<session>/<agent id or name>` addresses one subagent, and a bare
  `peer:<session>` still means the root thread. `cci who --agent` and
  `GET /v1/peers?agent=` select one. Store schema 10 adds the agent to the
  `sessions` and `claims` keys; existing rows become root threads (#60).
- `cci node` prints this machine's node id, key fingerprint and endorsement;
  `cci node endorse` endorses the node's P-256 file key once with the macOS
  Secure Enclave key. The key lives at `<home>/node/key.pem`, or
  `$CCI_NODE_KEY` on CI and service hosts (#57).

## [0.10.3] - 2026-10-10

### Fixed

- A session bound before 0.10.0 has no stored role; `cci who` now derives
  `root`, `lane` or `session` from its binding, so `--role root` finds the
  roots of drives that predate the registry (#55).

## [0.10.2] - 2026-10-10

### Fixed

- The schema-skew error's upgrade command for a binary under `~/.local/bin`
  extracted the release over the live `cci` in place, so a `cci` call during
  the extract found it missing or half-written. It now extracts into a temp
  directory beside the binary and renames it over (#53).

## [0.10.1] - 2026-10-10

### Fixed

- `cci who` read the session registry's `procStart` as local time; it is
  UTC, so every window looked seven hours off and `who` reported them all
  `gone`. Liveness now parses it as UTC. A session bound before 0.10.0 has no
  stored role and now prints as `session` instead of a blank (#51).

## [0.10.0] - 2026-10-10

### Added

- A machine-wide peer registry so sessions in different worktrees and drives
  can find each other. `cci who` lists live Claude Code sessions, joined with
  the registry under `~/.claude/sessions`, one line each ending in the `uds:`
  address `SendMessage` accepts, and narrows by `--session` prefix, `--name`,
  `--role`, `--drive`, `--lane`, `--tag`, `--repo`, `--worktree`, `--branch`
  or `-q`. `cci announce` records a role, description and tags, with the
  worktree, branch and `origin` slug read from git; the `SessionStart` hook
  and `drive use` announce with an inferred role, and every hook call
  refreshes `seen_at`. `cci me` prints this session's line for handoffs.
  The `sessions` table gains the peer columns, so the store schema moves to 7
  and older `cci` builds refuse to open it until upgraded (#46).
- Claims tie a session to an artifact as `<rel> <kind>:<key>`. `cci claim`
  and `cci unclaim` record explicit ones (`--pr`, `--build`, `--branch`,
  `--worktree`, `--issue`, `--thread`, `--stack`, `--target`, `--subsystem`,
  `--ref <kind>:<key>`, `--rel`), and every posted record derives claims on
  its `--pr`, `--build`, `--stack` and `--target` refs for the posting lane's
  sessions. `cci who --pr`, `--build`, `--ref` and `--rel` find the sessions
  behind an artifact and list the claims that matched; `GET /v1/peers` serves
  the same query. Schema 8 adds the `claims` table (#47).
- `cci post --to peer:<session prefix or name>` reaches one session in any
  drive, resolved at post time, and `--to <lane>@<drive>` reaches a lane in
  another drive, `root@<drive>` for its root. Reads, the channel subscription
  and lane hook delivery include such records from any drive under the same
  cursor, `--resolves` may close one across drives, and lines render the
  recipient as `peer:<8-char prefix>`. `cci announce` broadcasts a new
  24-hour `peer` record in its drive so a root subscribed with `--kind peer`
  hears a sibling join (#48).
- Registry hardening from review: an automatic announce keeps an explicit
  role, description and tags; a session replaced in the same process by
  `/clear` is marked superseded (schema 9) and drops out of `cci who`;
  `drive use` and a lane's first post refresh an inferred role; liveness
  reads the registry's `procStart` instead of a registration delay; codex
  lanes announce with the `codex` process as their window; worktree paths
  are compared after resolving symlinks; a registry file removed mid-read
  is skipped; and the announce runs as its own `SessionStart` hook so a
  missing `git` never costs a session its digest. `cci claim` announces a
  session that has no registry row first, a build key's `%` and `_` match
  literally, and `GET /v1/peers` refuses two artifact selectors and
  normalizes `pr`. Across drives: compaction keeps a resolver addressed to
  another drive, a lane's first cursor starts before records addressed to it
  from other drives, `--to` restricts the cross-drive branch too, JSON
  annotation and `cci state` keep drives apart, and the `peer` broadcast is
  written by the `registry` lane so a root hears its own siblings (#49).

## [0.9.3] - 2026-10-09

### Fixed

- `cci post --path <file>` without a text argument or `--text` posts the file's
  first line, clipped to 400 characters, as the record's text, and the file
  stays the body. It used to fail with `post needs its text`, the dead end the
  over-400 error pointed to. That error now names the working command:
  `cci post --lane <lane> --kind <kind> --path <file>` (#44).

### Changed

- Builds use Go 1.26.9, which fixes the `net/http` advisories GO-2026-6611
  through GO-2026-6617 (#44).

## [0.9.2] - 2026-10-06

### Fixed

- Import each inbox line at most once per drive. A rotation archive, a reread
  after the inbox file was replaced, or a copy of an inbox no longer
  re-appends old lines as new records. The store now keeps a ledger of every
  line hash it has consumed, including lines a parser reparse skipped and
  records `cci compact` folded away. On Oct 6 the first `inbox-rotate` cut made
  the daemon import nine archives, and 313 records dated Sep 27 to Oct 4
  reached `cci watch` again under new sequence numbers (#42).

### Changed

- Store schema 6 adds the line ledger and backfills it from existing records.
  Binaries older than 0.9.2 refuse a store this release has opened, so
  upgrade the plugin, the Homebrew cask, and any copied `cci` binary together.

## [0.9.1] - 2026-10-06

### Fixed

- Stop delivering the root's own owner prompts back to the root. The prompt
  hook now records them with source `hook:root`, and `--reader root` and
  `--reader main` skip broadcasts with that source, so the root's
  `cci watch` and `cci subscribe` channel no longer echo each prompt the owner
  just typed. Other readers still receive them (#40).

## [0.9.0] - 2026-10-06

### Added

- Deliver every record addressed to a lane into that lane's Claude Code
  session after each tool call, whatever its kind. The first
  `cci post --lane <lane>` in a session binds it to the lane, and the new
  `PostToolUse` and `PostToolUseFailure` hook `cci hook post-tool-use` injects
  each unseen addressed record. Records posted to the lane after the window
  started arrive even when the lane has not posted yet. Before this, a running
  lane received no cci record at all: a `stopped` record to
  release-repin-head went unseen for ten minutes. `SessionStart` in a lane
  session delivers the same records instead of the drive digest.

### Fixed

- Print a budgeted read's first record whole even when it alone exceeds the
  budget. A 400-character record of multi-byte characters used to stall a
  cursored read forever.
- `cci drive use --root` clears a lane binding on the session.

## [0.8.1] - 2026-10-06

### Fixed

- Tell the session in the channel's instructions that every tag already
  passed the subscription's filter, that a record addressed to the reader
  arrives whatever its kind, and that `main` is the root. A verification
  session read an `ask` addressed to `main` as misrouted and refused to act on
  it.

## [0.8.0] - 2026-10-06

### Added

- Add a `cci` channel to the plugin. `cci subscribe --reader <lane>
  [--kind <k>]... [--cursor <name>]` delivers each new record addressed to the
  reader, plus other lanes' broadcasts of each kind, into the session as a
  `<channel source="plugin:cc-inbox:cci">` tag, with no Monitor to re-arm.
  The cursor defaults to `channel-<session>` and starts at the head when you
  subscribe; the channel advances it past each delivered record, and starts
  only once the client sends `notifications/initialized`. The
  subscription belongs to the Claude Code window and survives compaction and
  `/clear`, and the `SessionStart` hook carries it into a resumed session.
  `cci unsubscribe` stops it. Sessions must be launched with
  `--channels plugin:cc-inbox@cc-inbox`.
- `cci watch --for 0` streams until interrupted.

### Changed

- `cci watch` advances its cursor past each record it writes, so a write
  failure partway through a page no longer replays the records already
  written.

### Fixed

- Deliver records addressed to `main` to `--reader root` and `--to root`, and
  records addressed to `root` to `main`. Lanes and the dashboard address the
  drive's root by either name, and the root's watch read only `root`, so 58
  records in three hours on release-v3 never reached it.

## [0.7.2] - 2026-10-06

### Fixed

- Include `--re`, `--resolves`, `--topic` and `--to` in the duplicate check
  for `post`, so posts with the same text that close different records are
  stored separately instead of collapsing into one `(duplicate)`.

## [0.7.1] - 2026-10-05

### Fixed

- Strip `com.apple.quarantine` from the Homebrew cask's `cci` binary after
  install. The 0.7.0 cask left it set, so every new exec stalled on a
  Gatekeeper check.

## [0.7.0] - 2026-10-05

### Added

- Add the `evidence` kind for an incident's evidence lane. It has no default
  expiry or kind pairing. Imported `EVIDENCE` lines use it; existing imports
  reparse with parser version 6.
- Accept `-n` and `--limit` on `tail` and `grep` to print only the newest N
  matching records, as `tail -n` does. `tail` prints them oldest first and
  advances its cursor to the newest one.

### Changed

- Read `grep` patterns as regular expressions by default, so `Q0|Syncing`
  matches either word instead of nothing. `-F` (`--fixed-strings`) matches a
  pattern literally. `--regex` is removed.

## [0.6.3] - 2026-10-05

### Fixed

- Accept `-i` and `--ignore-case` on `grep`. Matching stays case-insensitive
  by default, and `--ignore-case=false` matches case exactly.
- Accept a clock time for `--since`, such as `09:00`, `9:00 AM`, or `12:0x PM`,
  read in local (Pacific) time, the zone every record is displayed in. It
  resolves to the latest such time at or before now; a `Z` or `UTC` suffix
  reads it in UTC.
- Take the text of `post` as its one positional argument, the same as
  `--text`. Two arguments, or an argument plus `--text`, fail with an error
  that names `--text`.

## [0.6.2] - 2026-10-05

### Fixed

- Drop an untracked hold from digest once a post with `--resolves <seq>`, or
  a `lift` with `--re <seq>`, closes it. The `untracked holds` line used to
  list every untracked hold in the window, closed or not.

## [0.6.1] - 2026-10-05

### Fixed

- Default text `digest` to a 32,000-byte budget, so every section prints
  whole at its record limit. Tail, grep, and state keep 6,144 bytes, and
  `--budget` still overrides either default.

## [0.6.0] - 2026-10-05

### Added

- List imported holds that name no `stack:<project>/<env>` or
  `target:<name>` ref on one `untracked holds` line in text digest, and
  as `untracked_holds` in JSON digest. No later line can close them, so
  writers can add the ref.

### Changed

- Honor `--budget` as given instead of capping it at 16,000 bytes, and
  print whole records in tail, watch, and digest unless `--width N` is
  set. `SessionStart` keeps its 400-character width, 2,500-byte digest
  budget, and 1,500-byte tail budget.

### Fixed

- Print `no records on <drive> match; store head #<seq>` when a text
  grep matches nothing, so an empty result reads differently from a
  stale store. JSON output stays empty.
- Name the running version and the command that upgrades it when the
  store schema is newer than the binary: `brew upgrade --cask
  yasyf/tap/cci` for the cask, `claude plugin update cc-inbox@cc-inbox`
  for the plugin build, and a `gh release download` into the binary's
  directory for any other copy.

## [0.5.0] - 2026-10-05

### Added

- Add `retro`, `posted`, `design`, `serving`, `ready`, `duplicate`,
  `stand-down`, `not-ours`, `stopped`, `refuse`, `fail`, `recovered`,
  `delete-list`, `skew`, `not-live`, and `urgent` kinds. They have no
  default expiry or kind pairing. Accept `refused` as `refuse` and
  `failed` as `fail`.

### Changed

- Match grep patterns literally and case-insensitively by default. Use
  `--regex` for regular expressions, including `|` alternation. Keep
  newest-first ordering and the existing byte budgets.
- Reparse existing imports with parser version 5 so watched lead tokens
  gain their corresponding kinds without changing stored timestamps.

### Fixed

- Limit imported stamps to five minutes after the file's modification
  time. Move later stamps to the previous day and allow a one-day forward
  correction only within that limit. Keep out-of-order stamps on their day.
- Search the whole rendered record line in `cci grep`, including kind,
  lane, recipients, topic, and refs, so queries such as `STATE census`
  match records whose kind and lane are stored outside their text.
- Extract explicit `stack:<project>/<env>` and `target:<name>` refs
  from inbox lines and reparse existing imports with parser version 5.
  Track imported openers with either ref in digest open sections and
  preserve them through compaction. Imported openers without either
  ref remain untracked. Shared deployment refs close tracked items
  in the same drive using the opener's normal closers; imported
  openers also accept `fix-live`, `lift`, or `done`. Ref closure requires
  a later recorded time, with sequence number breaking a tie. If both records
  name stacks, at least one stack must match; otherwise, a shared
  target is enough. A native hold still needs `lift`. Imports from
  older archives cannot close newer regressions by ref.
- Include `state` records in `cci state` alongside `head` and `contract`,
  keeping the latest non-withdrawn record per kind, lane, and topic.
  Empty text reads say `no head, contract, or state records on <drive>`.
- Set the session's tail cursor to the drive's latest record in the
  same transaction as `cci drive use`, including on rebind. Named
  cursors and explicit `--since` reads keep their existing behavior.

## [0.4.0] - 2026-10-05

### Fixed

- Attribute stamped inbox lines to their writing lane, including lanes
  after parenthesized timestamps. Assign `OWNER` lines without a lane to
  `root` and parse `:5x` minutes as `:50`.
- Date imported stamps within 12 hours of the dating cursor and only
  move it backward, keeping out-of-order stamps on their day.
- Parse time-first inbox lines such as `1:43 AM PT <lane>: MECHANISM ...`
  without treating `AM` or `PM` as runner kinds. Preserve the full clock,
  lane, and kind, and reparse existing imports with parser version 2.
- List digest sections and `/v1/lanes` by sequence number descending while
  still selecting each lane's latest record by time.
- Attribute keyed runner events to `runner`, with the subject lane as
  `topic`. Preserve worker attribution for `msg_<id>` relays and unkeyed
  worker lines. For a `runner:` subject, use the first `<lane>=ctx_`
  token as the topic. Reparse existing imports with parser version 3.

### Changed

- Track the import parser version in schema migration 5. On a parser
  change, the next import or refresh reparses consumed lines and updates
  matching records without changing stored timestamps or restoring
  compacted records. `cci import` reports the number reparsed.
- Add `--width` to tail, watch, and digest, defaulting to 400 characters
  per rendered record line. `0` prints whole records. Include an ellipsis
  when clipping, then append reply marks. Grep, state, and JSON records
  stay whole. `SessionStart` uses width 400 for both reads and keeps its
  2,500-byte digest budget and 1,500-byte tail budget. Wider lines can
  leave room for fewer records within the unchanged byte budgets.

## [0.3.0] - 2026-10-05

### Changed

- Refresh registered markdown inboxes and discover archives every second in
  the daemon. `cci serve` or any plugin `SessionStart` starts or reuses it.
  Session start injects bound drive context before ensuring the daemon;
  ensure failures surface as non-blocking hook errors. Remove the `PostToolUse`
  hook and `cci hook post-tool` command. CLI reads stay direct to SQLite.
- Raise the default read budget from 4,000 to 6,144 bytes, matching the
  long-running skill's `inbox-digest.py`. The cap remains 16,000 bytes.
- Clip text record lines in tail, grep, watch, state, and digest sections to
  200 characters, including an ellipsis. Append reply marks after clipping so
  they stay visible. JSON record text is never clipped. `SessionStart` keeps
  its 2,500-byte digest budget and 1,500-byte tail budget.

## [0.2.0] - 2026-10-05

### Added

- Deploy stack, target, lane, board, PR, and build references; repeatable
  environments; release mode, outcome, commit, and census fields.
- `blocked`, `unblock`, and `review` kinds, plus `--resolves` to close an open
  item from any later record. Defects now remain open until closed.
- Stack, target, and PR filters for CLI reads and HTTP records and streams;
  computed opener status on JSON tail, grep, watch, and HTTP records reads.
- `/v1/lanes?drive=` for each lane's latest record by time, including expired
  records, and import extraction of stack tokens, PRs, and Buildkite build URLs.
- `cci serve` now ensures a daemonkit daemon (`com.yasyf.cc-inbox`) that serves
  the HTTP API at `127.0.0.1:7377` from a long-lived store, and returns. Writes
  and CLI reads stay direct to SQLite and need no daemon.

### Changed

- Schema 3 migrates `refs.pr` to `refs.prs`, `refs.build` to `refs.builds`, and
  `fields.env` to `fields.envs`, and merges the old `fields.stack` PR list into
  `refs.prs`. `--stack` now takes `<project>/<env>`; use repeated `--pr` for PRs.
- README and plugin guidance cover stack queries, explicit resolution, and
  `--to owner` for work that needs the owner.

## [0.1.0] - 2026-10-05

### Added

- `cci` CLI for typed drive coordination records in a local SQLite store.
- Kind validation, a 400-character text limit, typed references and fields,
  expiry, duplicate detection, and open-item pairing.
- Session drive bindings, root prompt capture, and named read cursors that
  resume after the last printed record. New tail cursors read the past hour.
- Bounded tail and regex search, Monitor watches with a 29-minute default
  lifetime, and digests that list open items in their window and count older ones.
- Incremental markdown import with source-line deduplication, file offsets, and
  blob references for long bodies.
- Daily compaction that preserves still-open items.
- Store isolation through the `CCI_HOME` environment variable.
- Read-only HTTP endpoints for drives, records, digests, and server-sent events.
- Claude Code marketplace plugin with `SessionStart`, `UserPromptSubmit`, and
  `PostToolUse` hooks, plus the `using-cci` skill.

[Unreleased]: https://github.com/yasyf/cc-inbox/compare/v0.38.0...HEAD
[0.38.0]: https://github.com/yasyf/cc-inbox/compare/v0.37.0...v0.38.0
[0.37.0]: https://github.com/yasyf/cc-inbox/compare/v0.36.0...v0.37.0
[0.36.0]: https://github.com/yasyf/cc-inbox/compare/v0.35.0...v0.36.0
[0.35.0]: https://github.com/yasyf/cc-inbox/compare/v0.34.0...v0.35.0
[0.34.0]: https://github.com/yasyf/cc-inbox/compare/v0.33.0...v0.34.0
[0.33.0]: https://github.com/yasyf/cc-inbox/compare/v0.32.2...v0.33.0
[0.32.2]: https://github.com/yasyf/cc-inbox/compare/v0.32.1...v0.32.2
[0.32.1]: https://github.com/yasyf/cc-inbox/compare/v0.32.0...v0.32.1
[0.32.0]: https://github.com/yasyf/cc-inbox/compare/v0.31.0...v0.32.0
[0.31.0]: https://github.com/yasyf/cc-inbox/compare/v0.30.3...v0.31.0
[0.30.3]: https://github.com/yasyf/cc-inbox/compare/v0.30.2...v0.30.3
[0.30.2]: https://github.com/yasyf/cc-inbox/compare/v0.30.1...v0.30.2
[0.30.1]: https://github.com/yasyf/cc-inbox/compare/v0.30.0...v0.30.1
[0.30.0]: https://github.com/yasyf/cc-inbox/compare/v0.29.0...v0.30.0
[0.29.0]: https://github.com/yasyf/cc-inbox/compare/v0.28.0...v0.29.0
[0.28.0]: https://github.com/yasyf/cc-inbox/compare/v0.27.2...v0.28.0
[0.27.2]: https://github.com/yasyf/cc-inbox/compare/v0.27.1...v0.27.2
[0.27.1]: https://github.com/yasyf/cc-inbox/compare/v0.27.0...v0.27.1
[0.27.0]: https://github.com/yasyf/cc-inbox/compare/v0.26.0...v0.27.0
[0.26.0]: https://github.com/yasyf/cc-inbox/compare/v0.25.0...v0.26.0
[0.25.0]: https://github.com/yasyf/cc-inbox/compare/v0.24.2...v0.25.0
[0.24.2]: https://github.com/yasyf/cc-inbox/compare/v0.24.1...v0.24.2
[0.24.1]: https://github.com/yasyf/cc-inbox/compare/v0.24.0...v0.24.1
[0.24.0]: https://github.com/yasyf/cc-inbox/compare/v0.23.0...v0.24.0
[0.23.0]: https://github.com/yasyf/cc-inbox/compare/v0.22.2...v0.23.0
[0.22.2]: https://github.com/yasyf/cc-inbox/compare/v0.22.1...v0.22.2
[0.22.1]: https://github.com/yasyf/cc-inbox/compare/v0.22.0...v0.22.1
[0.22.0]: https://github.com/yasyf/cc-inbox/compare/v0.21.2...v0.22.0
[0.21.2]: https://github.com/yasyf/cc-inbox/compare/v0.21.1...v0.21.2
[0.21.1]: https://github.com/yasyf/cc-inbox/compare/v0.21.0...v0.21.1
[0.21.0]: https://github.com/yasyf/cc-inbox/compare/v0.20.7...v0.21.0
[0.20.7]: https://github.com/yasyf/cc-inbox/compare/v0.20.6...v0.20.7
[0.20.6]: https://github.com/yasyf/cc-inbox/compare/v0.20.5...v0.20.6
[0.20.5]: https://github.com/yasyf/cc-inbox/compare/v0.20.4...v0.20.5
[0.20.4]: https://github.com/yasyf/cc-inbox/compare/v0.20.3...v0.20.4
[0.20.3]: https://github.com/yasyf/cc-inbox/compare/v0.20.2...v0.20.3
[0.20.2]: https://github.com/yasyf/cc-inbox/compare/v0.20.1...v0.20.2
[0.20.1]: https://github.com/yasyf/cc-inbox/compare/v0.20.0...v0.20.1
[0.20.0]: https://github.com/yasyf/cc-inbox/compare/v0.19.0...v0.20.0
[0.19.0]: https://github.com/yasyf/cc-inbox/compare/v0.18.0...v0.19.0
[0.18.0]: https://github.com/yasyf/cc-inbox/compare/v0.17.0...v0.18.0
[0.17.0]: https://github.com/yasyf/cc-inbox/compare/v0.16.2...v0.17.0
[0.16.2]: https://github.com/yasyf/cc-inbox/compare/v0.16.1...v0.16.2
[0.16.1]: https://github.com/yasyf/cc-inbox/compare/v0.16.0...v0.16.1
[0.16.0]: https://github.com/yasyf/cc-inbox/compare/v0.15.0...v0.16.0
[0.15.0]: https://github.com/yasyf/cc-inbox/compare/v0.14.0...v0.15.0
[0.14.0]: https://github.com/yasyf/cc-inbox/compare/v0.13.0...v0.14.0
[0.13.0]: https://github.com/yasyf/cc-inbox/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/yasyf/cc-inbox/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/yasyf/cc-inbox/compare/v0.10.4...v0.11.0
[0.10.4]: https://github.com/yasyf/cc-inbox/compare/v0.10.3...v0.10.4
[0.10.3]: https://github.com/yasyf/cc-inbox/compare/v0.10.2...v0.10.3
[0.10.2]: https://github.com/yasyf/cc-inbox/compare/v0.10.1...v0.10.2
[0.10.1]: https://github.com/yasyf/cc-inbox/compare/v0.10.0...v0.10.1
[0.10.0]: https://github.com/yasyf/cc-inbox/compare/v0.9.3...v0.10.0
[0.9.3]: https://github.com/yasyf/cc-inbox/compare/v0.9.2...v0.9.3
[0.9.2]: https://github.com/yasyf/cc-inbox/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/yasyf/cc-inbox/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/yasyf/cc-inbox/compare/v0.8.1...v0.9.0
[0.8.1]: https://github.com/yasyf/cc-inbox/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/yasyf/cc-inbox/compare/v0.7.2...v0.8.0
[0.7.2]: https://github.com/yasyf/cc-inbox/compare/v0.7.1...v0.7.2
[0.7.1]: https://github.com/yasyf/cc-inbox/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/yasyf/cc-inbox/compare/v0.6.3...v0.7.0
[0.6.3]: https://github.com/yasyf/cc-inbox/compare/v0.6.2...v0.6.3
[0.6.2]: https://github.com/yasyf/cc-inbox/compare/v0.6.1...v0.6.2
[0.6.1]: https://github.com/yasyf/cc-inbox/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/yasyf/cc-inbox/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/yasyf/cc-inbox/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/yasyf/cc-inbox/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/yasyf/cc-inbox/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/yasyf/cc-inbox/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/yasyf/cc-inbox/releases/tag/v0.1.0
