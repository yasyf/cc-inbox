// Package harness reads the Claude Code session registry that the CLI keeps
// for its own peer messaging: one JSON file per window under
// <config dir>/sessions, naming the session, its process, cwd and socket.
package harness

import (
	"encoding/json"
	"errors"
	"fmt"
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
// was created when the registry says it was, to the second.
func (s Session) Alive() bool {
	started, err := time.ParseInLocation(time.ANSIC, s.ProcStart, time.Local)
	if err != nil {
		return false
	}
	created, ok := createTime(s.PID)
	if !ok {
		return false
	}
	delta := created - started.UnixMilli()
	return delta > -time.Second.Milliseconds() && delta < 2*time.Second.Milliseconds()
}

// Dir is the registry directory: $CLAUDE_CONFIG_DIR/sessions or ~/.claude/sessions.
func Dir() (string, error) {
	if d := os.Getenv("CLAUDE_CONFIG_DIR"); d != "" {
		return filepath.Join(d, "sessions"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home: %w", err)
	}
	return filepath.Join(home, ".claude", "sessions"), nil
}

// Sessions reads every registry file in dir, keyed by session id. A missing
// dir is an empty registry.
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
			return nil, fmt.Errorf("decode %s: %w", e.Name(), err)
		}
		out[s.SessionID] = s
	}
	return out, nil
}

// ProcessAlive reports whether pid exists and was created at the given
// millisecond timestamp, as a Window records it.
func ProcessAlive(pid int, started int64) bool {
	created, ok := createTime(pid)
	return ok && created == started
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
