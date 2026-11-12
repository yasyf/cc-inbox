package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const (
	channelBeatDir    = "channels"
	channelBeatWindow = 7 * 24 * time.Hour
)

// ChannelOutage is a window's open report that its channel server stopped: the
// blocker posted about it and the last time a reconnect was started.
type ChannelOutage struct {
	Blocker int64     `json:"blocker"`
	TypedAt time.Time `json:"typed_at"`
}

func (s *Store) channelBeatPath(w Window) string {
	return filepath.Join(s.home, channelBeatDir, fmt.Sprintf("%d-%d", w.PID, w.Started))
}

// BeatChannel stamps w's channel server as running now.
func (s *Store) BeatChannel(w Window) error {
	path, now := s.channelBeatPath(w), s.Now()
	err := os.Chtimes(path, now, now)
	if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("make channel beat dir: %w", err)
	}
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		return fmt.Errorf("write channel beat: %w", err)
	}
	return os.Chtimes(path, now, now)
}

type channelOwner struct {
	Window
	Toolless bool `json:"toolless"`
}

// ClaimChannel makes proc, a cci channel server's process, the owner of w's
// channel unless one that outranks it owns it, and returns the owner. A
// toolless server outranks one serving tools, which is the server a plugin
// config from before the toolless channel starts for a subagent's tool call;
// among servers of one kind the newest owns, since Claude Code reads only a
// window's newest channel server and leaves the older ones running.
func (s *Store) ClaimChannel(w, proc Window, toolless bool) (Window, error) {
	path := s.channelBeatPath(w) + ".owner"
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return Window{}, fmt.Errorf("make channel beat dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600) //nolint:gosec // G304: the path is under the cci home.
	if err != nil {
		return Window{}, fmt.Errorf("open channel owner: %w", err)
	}
	defer func() { _ = f.Close() }()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return Window{}, fmt.Errorf("lock channel owner: %w", err)
	}
	raw, err := io.ReadAll(f)
	if err != nil {
		return Window{}, fmt.Errorf("read channel owner: %w", err)
	}
	var owner channelOwner
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &owner); err != nil {
			return Window{}, fmt.Errorf("decode channel owner: %w", err)
		}
	}
	if owner.Toolless && !toolless || owner.Toolless == toolless && owner.Started >= proc.Started {
		return owner.Window, nil
	}
	if raw, err = json.Marshal(channelOwner{Window: proc, Toolless: toolless}); err != nil {
		return Window{}, err
	}
	if err := f.Truncate(0); err != nil {
		return Window{}, fmt.Errorf("write channel owner: %w", err)
	}
	if _, err := f.WriteAt(raw, 0); err != nil {
		return Window{}, fmt.Errorf("write channel owner: %w", err)
	}
	return proc, nil
}

// ChannelBeat returns when w's channel server last ran; ok is false when none
// ever started in this window.
func (s *Store) ChannelBeat(w Window) (at time.Time, ok bool, err error) {
	info, err := os.Stat(s.channelBeatPath(w))
	if errors.Is(err, fs.ErrNotExist) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("stat channel beat: %w", err)
	}
	return info.ModTime(), true, nil
}

func (s *Store) ChannelOutage(w Window) (ChannelOutage, bool, error) {
	raw, err := os.ReadFile(s.channelBeatPath(w) + ".outage") //nolint:gosec // G304: the path is under the cci home.
	if errors.Is(err, fs.ErrNotExist) {
		return ChannelOutage{}, false, nil
	}
	if err != nil {
		return ChannelOutage{}, false, fmt.Errorf("read channel outage: %w", err)
	}
	var o ChannelOutage
	if err := json.Unmarshal(raw, &o); err != nil {
		return ChannelOutage{}, false, fmt.Errorf("decode channel outage: %w", err)
	}
	return o, true, nil
}

func (s *Store) SetChannelOutage(w Window, o ChannelOutage) error {
	raw, err := json.Marshal(o)
	if err != nil {
		return err
	}
	path := s.channelBeatPath(w) + ".outage"
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("make channel beat dir: %w", err)
	}
	return os.WriteFile(path, raw, 0o600)
}

func (s *Store) ClearChannelOutage(w Window) error {
	return os.Remove(s.channelBeatPath(w) + ".outage")
}
