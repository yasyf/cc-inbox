# Kinds and closures

## Link and close records

Give related events a stable `--topic`. Replies use that topic or `--re <seq>`.
`--resolves <seq>` must name an existing record in the same drive. Only a later
record closes an `ask`, `decide`, `blocker`, `blocked`, `defect`, `disconnect`, `hold`, or `incident`.
`go`, `opened`, `landed`, and `fix-live` can all carry `--resolves`.
`answer`, `withdraw`, and `unblock` require `--re`, `--topic`, or `--resolves`.
A `lift` needs `--re` or `--resolves` naming an open hold; one that names none
is refused with the open holds on its pull requests, so pass one of those.
Withdraw a go with `cci post --kind withdraw --re <go seq>`, one `--re` per go;
`cci post` refuses a go withdrawal written as a sentence in any other kind.
Repeat `--resolves`, or give a comma list, to close several at once: one record
resolves every seq, and its line reads `x<a> x<b> x<c>`.

Name a record by what it carries instead of typing its seq: `--re`, `--resolves`
and `--pr` take an alias, and `cci find <alias>` prints the record it names.

| Alias | Names |
| --- | --- |
| `t:<topic>` | records with that `--topic` |
| `ccn:<id or prefix>` | records whose `--ccn` id, or whose text, starts with it |
| `card:<id>` | records whose text names `card-<id>` |
| `pr#N` | records carrying `--pr N` |
| `last:<kind>[:<lane>]` | the newest record of that kind, from that lane |

An alias resolves once, when the record is posted, and the record stores the
seq it found. When exactly one of its records is open, an ask, blocker, hold or
other opener nothing has closed, the alias names that one; otherwise `--re`
and `--pr` take its only record. `--resolves` takes only an open record. An
alias that names nothing, or several where one is needed, is refused with the
candidates listed; post again with the seq you meant. `--pr <alias>` copies the
named record's pull requests. Readers print the alias beside the seq it found,
as `^374168(t:rollout)`. `ccn:` reads only cci's own records, so it works on a
box without `ccn`; it matches the id a record linked with `--ccn` or the start
of that record's text, never the doc's title in cc-notes. A store from an
older cci gets the alias index from `cci maintain-indexes`; until then an alias
is refused and names that command.

## Choose a kind

Pair kinds through `--re` or a shared `--topic` in the drive using the table's
kind pairs and a later sequence number.

A shared deployment ref also closes a tracked opener with one of its normal
closers. A `hold` accepts `lift`; `defect` accepts `fix-live` or `done`;
`blocker` accepts `withdraw`, `answer`, or `done`; `blocked` accepts `unblock`.
Imported openers also accept `fix-live`, `lift`, or `done`.

Ref closure requires a later recorded time; sequence number breaks a tie.
When both records name stacks, at least one stack must match. When either
names no stack, a shared target is enough. A native hold still needs `lift`.
TTL means time to live; `--ttl` overrides defaults.

| Kinds | Default TTL | Pairing or requirement |
| --- | --- | --- |
| `ask`, `decide` | None | Open until `answer`, `go`, or `withdraw`. |
| `decision` | None | A call already made; not open. Imported `DECISION` uses it. |
| `blocker` | None | Open until `withdraw`, `answer`, or `done`. |
| `blocked` / `unblock` | None | A deploy block stays open until unblock or a resolver. |
| `answer`, `withdraw` | None | Close asks, decision requests, or blockers, and withdraw also a disconnect; require a link. A withdraw takes back any other record it names. |
| `go` | None | Closes a matching ask or decision request. |
| `hold` / `lift` | None | Lift closes the open hold its `--re` or `--resolves` names. A hold holds exactly its `--pr` PRs; a hold naming `#<number>` with no `--pr` is refused. |
| `incident`, `defect` / `fix-live`, `done` | None | Fix-live or done closes the opener; done also closes blockers. |
| `opened`, `landed` | None | Require `--pr`. |
| `red` | None | Classifies a pull request's red CI at its head, such as `class=flake owner=<lane>`. Requires `--pr`. |
| `queue` | None | A merge-queue state change for a pull request, with the state in `--outcome`. Requires `--pr`. |
| `github` | 14 days | A GitHub pull request, review or check change, or a push to the default branch, with the action or conclusion in `--outcome` and the commit in `--commit`. A commit status or a push names no pull request. |
| `spawn` | None | A lane's start: `--lane-ref <lane> --attr brief=<absolute path> --attr model=<model>`, with `--attr effort=` and `--attr task=` when known. Kept, never expiring; `cci who` lists the lane as spawned until a thread binds it. |
| `review` | None | A rules-review verdict. |
| `disconnect` | None | A subscribed session's channel server posts it to `communicator` when its channel stops. Open until `withdraw` or a resolver, such as the `recovered` record the next channel to stream that reader posts with `--resolves`. |
| `owner`, `mechanism`, `evidence`, `correction`, `release`, `applied`, `handoff`, `head`, `contract` | None | Unpaired. |
| `retro`, `posted`, `design`, `serving`, `ready`, `duplicate` | None | Unpaired. |
| `stand-down`, `not-ours`, `stopped`, `refuse`, `fail`, `recovered` | None | Unpaired. |
| `delete-list`, `skew`, `not-live`, `urgent` | None | Unpaired. |
| `digest` | None | Written only by compaction. |
| `claim`, `note`, `state`, `matrix`, `report`, `peer` | 24 hours | No automatic pairing. `peer` is the broadcast `cci announce` makes in its drive. |

`refused` is an alias for `refuse`; `failed` is an alias for `fail`.
Imported uppercase lead tokens map to their corresponding kinds. The 16 kinds
from `retro` through `urgent` above have no default expiry. They do not open
or close items through kind pairing.
