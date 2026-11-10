# Peers: find and reach other sessions

## Find and reach a sibling session

Every Claude Code session on this machine is a peer in one machine-wide
registry, whatever its drive or worktree. `cci who` lists the live ones, one
line each, ending with the address `SendMessage` accepts:

```bash
cci who
cci who --session 06923255
cci who --worktree /Users/me/.orca/workspaces/monorepo/monorepo/sole
cci who --branch yasyf/eng-1062-ssql-bucket --role root
cci who --repo Forge-AI/monorepo --tag release
cci who -q sandsql --json
```

```text
sole-51 root drive=release-v3 branch=main wt=/Users/me/.orca/workspaces/monorepo/monorepo/sole busy 6m "api release" #release session=900424b6 -> uds:/tmp/cc-socks/29348.sock
```

`--session` takes the id or its prefix, the form agents cite. `--name` takes
the name `ListAgents` prints, which changes when a session resumes.
`--worktree` matches the worktree or cwd. `--repo` matches the `owner/name`
slug, or the git common dir. Pass the trailing `uds:` address to `SendMessage`; subagents
can run `cci who` even though they lack `ListAgents`. Without `--all`, a
session whose process has ended is hidden; with it, the line reads
`gone, last seen <time>`. Unnarrowed, or with `--drive` alone, `cci who` folds
subagents and workflow agents that hold no lane, and sessions idle over an
hour, into one `+N more` line; any other flag turns the fold off.

Announce what this session is doing so the lines above carry it:

```bash
cci announce --desc "sandsql replica bake" --tag release --tag sandsql
cci announce --lane root --drive demo   # this thread is the drive's coordinator
```

The `SessionStart` hook, `cci drive use` and a lane's first post announce
automatically. The role comes from the binding: `root`, `lane`, or
`session`; a role, description or tag set by hand stays until the next
explicit `cci announce` names a new one. The worktree, branch and `origin`
slug come from git, and the Orca and long-running identity variables come
from the environment. A codex lane announces under `CODEX_THREAD_ID` with
the `codex` process as its window. `--role` overrides the inference; `desk`, `watcher` and
`codex` are the other values in use. Each hook call refreshes the peer's
`seen_at`. A subagent or teammate is its own peer: the plugin's hooks
announce it on its first tool call under its session and the harness's
`agent_id`, name it from the harness's agent metadata, and retire it on
`SubagentStop`. Its line cites it as `session=<session>/<agent name>`, and
`peer:<session>/<agent id or name>` addresses it; a bare `peer:<session>`
is the root thread. Its own `cci` calls carry `CCI_AGENT_ID`, the payload's
`agent_id` exported into each of its Bash commands, so its claims, lane
binding and default cursor are its own. Without `CCI_AGENT_ID` a command
acts as the root thread.

## Message a running Workflow agent

A Workflow tool agent is named by its label and announced with role
`workflow`. It never runs `cci subscribe`: a subagent bound to no lane gets
every record addressed to its peer address in its next tool result. From the
root, `peer:<label>` reaches one of the root's own agents, a live one before
an ended run's agent with the same label:

```bash
cci who --role workflow --session <your session prefix>
cci post --lane root --kind go --to peer:apply:box-and-capture "stop after this file"
```

When two live agents share a label, the post fails and names both; address
one by its agent id. Never `SendMessage` a running workflow agent by its id:
that resumes a second copy instead of reaching the live one.

Paste `cci me` into a handoff instead of a session name or a PR list:

```bash
cci me
```

## Find the session behind an artifact

A claim ties a session to an artifact as `<rel> <kind>:<key>`. Records
already make claims: a posted `--pr`, `--build`, `--stack` or `--target`
ties the posting lane's sessions to that artifact, with `opened`, `landed`,
`reviewed`, `released`, `handling`, `handed-off` or `mentioned` as the rel,
from the record's kind. Add the rest by hand:

```bash
cci claim --pr 34363 --rel opened
cci claim --pr 34370 --rel watches
cci claim --build 4860 --rel started
cci claim --thread C0822AHFY3G/1759012345.678 --rel posted
cci claim --issue ENG-1062 --branch yasyf/eng-1062-ssql-bucket
cci claim --ref dispatch:ctx_6e8064160648 --rel runs
```

`--rel` defaults to `owns`. The kinds with flags are `pr`, `build`,
`branch`, `worktree`, `issue`, `thread`, `stack`, `target` and `subsystem`;
`--ref <kind>:<key>` takes any other pair. `cci unclaim` with the same
flags drops an explicit claim; a derived one stays with its record.

`--for <lane>@<drive>` makes the claim the lane's instead of the caller's,
so a desk or runner that learns of a lane's artifact can tie it to that lane:
`cci claim --for walker@release-v3 --build 8748 --rel started`. It names the
one live session on this machine bound as that lane in that drive, and
refuses when none is live or more than one is, and needs no session of its own.

`cci claim --rel opened --pr N` from a session bound to a drive also posts
one `opened` record for each PR on that drive, so a landing desk sees the PR
without a lane posting it. `ccx vcs ship` makes this claim for every PR it
opens. The record's lane is the caller's bound lane, else `root` for the root
thread, else the subagent's id. A repeat claim posts nothing new.

Ask `who` by artifact, and each match lists the claims that selected it:

```bash
cci who --pr 34363
cci who --build 4860 --rel started
cci who --ref branch:yasyf/eng-1062-ssql-bucket
```

```text
alewife-1a root drive=b577ef71 branch=yasyf/eng-1062-ssql-bucket wt=… busy 2m session=b577ef71 -> uds:/tmp/cc-socks/19022.sock
  opened pr:34363 2:14 PM (#36201)
  watches pr:34363 2:20 PM
```

A build key matches a stored build URL ending in `/builds/<key>`. A
suffix `(#seq)` names the record a claim came from. The daemon serves the
same query at `GET /v1/peers`.

## Address a session in another drive

`--to` reaches across drives in two forms. `peer:<session prefix or name>`
resolves to one session at post time and fails when the prefix matches
none or several. `<lane>@<drive>` reaches a lane in a named drive, with
`root@<drive>` and `main@<drive>` both reaching that drive's root. A bare
lane that no live thread reads in the posting drive, but one reads in exactly
one other drive, is addressed `<lane>@<that drive>` and the post says so on
stderr; when live threads read it in several other drives, the post fails and
names them.

```bash
cci post --lane root --kind ask --to peer:900424b6 "can you take #34363?"
cci post --lane root --kind go --to root@release-v3 "GO on the bake"
cci post --lane root --kind answer --resolves 36201 --to peer:b577ef71 "yes"
```

`peer:<node>/<session>` reaches a session on another node in the mesh;
`cci who` prints that address for every replicated session, and
`cci node` prints this machine's node id. A bare `peer:` names a session on
this machine.

The record stays in the sender's drive. The recipient's reads (`tail`,
`watch`, the channel subscription, and a lane's hook delivery) include
records addressed to `peer:<its session>` or `<its reader>@<its drive>`
whatever drive holds them, under the same cursor, since sequence numbers
are machine-wide. `--resolves` may name such a record from the other
drive, and the opener shows `[RESOLVED #n]` on both sides. Lines render
the peer as `peer:<8-char prefix>`. `tail --to <lane>` reads one lane's
addressed records only and never adds the session's own peer records.

`cci announce` broadcasts a `peer` record in the session's drive, so a
root subscribed with `--kind peer` hears a sibling join.

## Pair a machine outside the owner mesh

Owner hosts trust each other through synckit. Any other machine joins a mesh
as a read-only node by pairing over the relay:

```bash
cci pair --url https://<relay URL>     # on the joining machine: prints CODE + fingerprint
cci pair accept <CODE>                 # on an owner machine: Touch ID, here or routed
cci peers --mesh                       # nodes with tier: self, mesh, pair, relay
cci unpair <node id prefix>            # revoke and rotate the group key
```

Compare the fingerprint `cci pair` prints with the one `pair accept` shows
and the Touch ID sheet repeats; that comparison is the only check that the
relay did not swap keys. A paired node never writes. A record it posts is
rejected and unpairs it on every node, and the owners rotate the group key.
`--tier service --allow '<drives>|<kinds>' --label <name>` on `pair accept`
admits a scoped writer instead.
