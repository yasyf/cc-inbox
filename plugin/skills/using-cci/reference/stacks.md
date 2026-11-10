# Stacks, targets, deploy blocks, markdown imports and upgrades

## Stack and target refs

Every `defect`, `matrix`, `hold`, `go`, or `opened` record about a stack carries
`--stack <project>/<env>`, `--target <name>`, or both. Attach them to related records
too, so a stack or target query finds both the problem and the work addressing it.
`--stack` names a deploy stack, such as `api/plat-usw2-prod`. Use repeated `--pr`
flags for pull requests; `opened` and `landed` require at least one. A `hold`
holds exactly the pull requests its `--pr` flags name, because merge-queue
guards read the hold's pull requests, never its prose. Give `--pr` only for each
pull request the hold stops; one the text names as context stays unheld, and
post prints a line listing it. Post refuses a hold whose text or topic names
`#<number>` with no `--pr` at all. Write a record sequence number in a hold
without its `#`.

Use `blocked` when a stack or target cannot deploy. Close it with `unblock`
using `--re` or the same `--topic`, or with a resolver.

A shared deployment ref also lets `unblock` close it in the same drive.
Imported openers also accept `fix-live`, `lift`, or `done` by ref. Ref closure
requires a later recorded time, with sequence number breaking a tie. When
both records name stacks, at least one stack must match; when either names
no stack, a shared target is enough.

An explicit `--resolves` link closes only the named item.
A `fix-live` record can close a defect and a separate imported deployment
block by ref; a native `blocked` record still needs `unblock`.

## A deploy block, end to end

For a new drive:

```bash
cci post --drive demo --lane walker --kind blocked --stack api/plat-usw2-prod --target api --text 'Platy cannot deploy: wave cycle.'
cci post --drive demo --lane root --kind ask --to owner --topic rollout --stack api/plat-usw2-prod --text 'Approve the repair?'
```

Use the returned sequence number when the stack can deploy again. If the block
above returned `#1`, close it with:

```bash
cci post --drive demo --lane walker --kind unblock --re 1 --stack api/plat-usw2-prod --target api --text 'Platy can deploy this stack.'
```

## Import markdown inboxes

Import appended inbox files, not the runner's rewritten `runner-state.md` view.
`cci import` registers files, restarts after inode changes or shrinkage, and
deduplicates lines within the drive. After a parser version change, the next
import or daemon refresh reparses consumed lines and updates matching records
without changing their stored timestamps or restoring compacted records. Import
output includes the number reparsed. It extracts PRs, Buildkite build URLs,
bare stack names with region-shaped environments such as `api/plat-usw2-prod`,
and explicit `stack:<project>/<env>` and `target:<name>` tokens. Explicit names
use lowercase letters, digits, and hyphens; projects and targets start with a
letter, and environments start with a letter or digit. `stack:api/staging`
works without a region-shaped environment. Labels such as `tunnel/T`, `k8s/T`,
and box names supply no stack ref; write `stack:tunnel/tnt-usw2-1frg9c7`.
The daemon refreshes registered imports every second, importing later appends
and new `<inbox>.md.archive/*.md` files
with the inbox's drive and lane; direct archive imports default to the inbox
name. Run `cci serve` or start any Claude Code session with the plugin installed
to start or reuse the daemon. Allow the next refresh to finish before reading
appended records.

Once lanes post directly, stop writing markdown inboxes. Open-item tracking
includes imported openers with a stack or target ref. Imported `hold`, `defect`,
`blocker`, and `blocked` records with either ref appear in the digest's open
sections and the dashboard's blocked view. Imported openers without either
ref remain untracked.

A fix on `api/tnt-usw2-bbbb` leaves a defect on `api/tnt-usw2-aaaa` open even
when both name target `api`. Imports from older archives cannot close newer
regressions because closure compares recorded time. Compaction folds old
records into daily counts and preserves tracked open items.

## Upgrade after a release

After a cc-inbox release workflow finishes, run `cci upgrade` once. It moves the
plugin, the Homebrew cask, and each standalone `cci` on `PATH`, then runs `serve`
from the newest install. Skip hand-running `cci serve` after a release: the
daemon hands off to a newer plugin or cask install within 30 seconds.
