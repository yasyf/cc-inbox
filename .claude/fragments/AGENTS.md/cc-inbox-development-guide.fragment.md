# cc-inbox Development Guide

Local, typed, append-only inbox CLI (cci) and Claude Code plugin for multi-agent drive coordination with bounded reads. Distributed via Homebrew: `brew install yasyf/tap/cci`.

## Repository Structure

```
cc-inbox/
├── cmd/cci/               # main package — the CLI entry point
├── internal/
│   ├── cli/               # cobra commands: post, tail, watch, digest, grep, import, compact, drive, serve, hook
│   ├── store/             # SQLite store: records, cursors, session bindings, import offsets, compaction
│   ├── kinds/             # the closed kind set, TTLs, aliases, open/close pairing
│   ├── inbox/             # bounded reads: tail, grep, digest, watch
│   ├── importer/          # markdown inbox parser and incremental import
│   ├── hook/              # SessionStart, UserPromptSubmit, PostToolUse handlers
│   ├── server/            # read-only HTTP JSON API for dashboards
│   ├── render/            # record line format and byte budgets
│   ├── testutil/          # test store and clock
│   ├── version/           # build version, stamped via -ldflags
│   └── log/               # slog setup
├── plugin/                # Claude Code plugin: hooks, skill, binrun-provisioned bin/cci
├── .github/               # GitHub Actions workflows
├── AGENTS.md              # This file — shared conventions
└── README.md              # Project overview
```
