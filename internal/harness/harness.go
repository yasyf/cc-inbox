// Package harness reads the Claude Code session registry that the CLI keeps
// for its own peer messaging: one JSON file per window under
// <config dir>/sessions, naming the session, its process, cwd and socket.
package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v4/process"
)

// Session is one window's registry entry.
type Session struct {
	PID             int    `json:"pid"`
	SessionID       string `json:"sessionId"`
	Cwd             string `json:"cwd"`
	ProcStart       string `json:"procStart"`
	Kind            string `json:"kind"`
	Socket          string `json:"messagingSocketPath"`
	Name            string `json:"name"`
	Status          string `json:"status"`
	StatusUpdatedAt int64  `json:"statusUpdatedAt"`
}

// Address is the string SendMessage accepts for this window.
func (s Session) Address() string {
	if s.Socket == "" {
		return ""
	}
	return "uds:" + s.Socket
}

// Alive reports whether the window's process still runs: the pid exists and
// was created when the registry says it was, to the second. The registry
// writes procStart as ANSIC in UTC, or on Linux as the process's start time
// in kernel ticks.
func (s Session) Alive(procs Procs) bool {
	if procs.startTicks(s.PID) == s.ProcStart && s.ProcStart != "" {
		return true
	}
	started, err := time.ParseInLocation(time.ANSIC, s.ProcStart, time.UTC)
	if err != nil {
		return false
	}
	created, ok := procs.createTime(s.PID)
	if !ok {
		return false
	}
	delta := created - started.UnixMilli()
	return delta > -time.Second.Milliseconds() && delta < 2*time.Second.Milliseconds()
}

type configDirKey struct{}

// WithConfigDir has Dir read dir, the caller's CLAUDE_CONFIG_DIR, in place of
// this process's: a daemon answering a client reads the client's registry.
func WithConfigDir(ctx context.Context, dir string) context.Context {
	return context.WithValue(ctx, configDirKey{}, dir)
}

// Dir is the registry directory: <config dir>/sessions or ~/.claude/sessions.
func Dir(ctx context.Context) (string, error) {
	configDir, given := ctx.Value(configDirKey{}).(string)
	if !given {
		configDir = os.Getenv("CLAUDE_CONFIG_DIR")
	}
	if configDir != "" {
		return filepath.Join(configDir, "sessions"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home: %w", err)
	}
	return filepath.Join(home, ".claude", "sessions"), nil
}

// Sessions reads every registry file in dir, keyed by session id. A missing
// dir is an empty registry, and a file that does not decode, such as one its
// writer is still writing, is skipped with a warning.
func Sessions(dir string) (map[string]Session, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]Session{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read session registry: %w", err)
	}
	out := make(map[string]Session, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name())) //nolint:gosec // G304: dir is the registry; names come from ReadDir.
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", e.Name(), err)
		}
		var s Session
		if err := json.Unmarshal(raw, &s); err != nil {
			slog.Warn("skipping a session registry file that does not decode", "file", e.Name(), "err", err)
			continue
		}
		out[s.SessionID] = s
	}
	return out, nil
}

// Procs reads each process's create time once, however many registry rows
// and windows name it.
type Procs map[int]created

type created struct {
	at    int64
	ok    bool
	ticks string
}

// Alive reports whether pid exists and was created at the given millisecond
// timestamp, as a Window records it.
func (p Procs) Alive(pid int, started int64) bool {
	at, ok := p.createTime(pid)
	return ok && at == started
}

func (p Procs) createTime(pid int) (int64, bool) {
	c := p.read(pid)
	return c.at, c.ok
}

func (p Procs) startTicks(pid int) string {
	return p.read(pid).ticks
}

func (p Procs) read(pid int) created {
	c, read := p[pid]
	if !read {
		c.at, c.ok = createTime(pid)
		c.ticks = startTicks(pid)
		p[pid] = c
	}
	return c
}

func startTicks(pid int) string {
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)) //nolint:gosec // G304: the path holds only a numeric pid.
	if err != nil {
		return ""
	}
	return strings.Fields(string(stat[bytes.LastIndexByte(stat, ')')+1:]))[19]
}

func createTime(pid int) (int64, bool) {
	if pid <= 0 {
		return 0, false
	}
	p, err := process.NewProcess(int32(pid)) //nolint:gosec // G115: OS pids fit in int32.
	if err != nil {
		return 0, false
	}
	created, err := p.CreateTime()
	if err != nil {
		return 0, false
	}
	return created, true
}
