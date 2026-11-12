package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

type Window struct {
	PID     int   `json:"pid"`
	Started int64 `json:"started"`
}

type Subscription struct {
	Alias      string       `json:"alias,omitempty"`
	Session    string       `json:"session"`
	Window     Window       `json:"window"`
	Drive      string       `json:"drive"`
	Follow     []Follow     `json:"follow,omitempty"`
	Reader     string       `json:"reader"`
	Kinds      []kinds.Kind `json:"kinds"`
	Mute       []kinds.Kind `json:"mute,omitempty"`
	ToKinds    []kinds.Kind `json:"to_kinds,omitempty"`
	TopicKinds []kinds.Kind `json:"topic_kinds,omitempty"`
	Batch      []kinds.Kind `json:"batch,omitempty"`
	Cursor     string       `json:"cursor"`
	Head       int64        `json:"head"`
	At         time.Time    `json:"at"`
}

// UrgentRepeat is how long an urgent record's lane and topic stay delivered:
// a repeat inside it reaches no channel or hook, only cci tail.
const UrgentRepeat = 30 * time.Minute

func DefaultToKinds(reader string) []kinds.Kind {
	if RootName(reader) {
		return kinds.RootAddressed()
	}
	return nil
}

// DefaultBatch is the kinds a channel subscription for reader carries together
// when it names none: done, for a root.
func DefaultBatch(reader string) []kinds.Kind {
	if RootName(reader) {
		return []kinds.Kind{kinds.Done}
	}
	return nil
}

func (s Subscription) Filter() Filter {
	return Filter{Drive: s.Drive, Follow: s.Follow, Peer: s.Session, For: s.Reader, Kinds: s.Kinds, Mute: s.held(), ToKinds: s.ToKinds, TopicKinds: s.TopicKinds, RepeatWindow: UrgentRepeat}
}

func (s Subscription) held() []kinds.Kind {
	return slices.Concat(s.Mute, s.Batch)
}

// BatchFilter selects what the channel delivers together, never on arrival:
// the records addressed to the reader of a batch kind, whether addressed as
// the lane or as <reader>@<drive>.
func (s Subscription) BatchFilter() Filter {
	return Filter{Drive: s.Drive, Addressed: s.Reader, Kinds: s.Batch, ToKinds: s.Batch, Mute: s.Mute}
}

func (s Subscription) BatchCursor() string {
	return s.Cursor + "-batch"
}

// FloorCursor names where the channel Claude Code started for s's window began
// following s.Cursor: that channel delivered every record s selects after it.
func (s Subscription) FloorCursor() string {
	return fmt.Sprintf("%s-floor-%d-%d", s.Cursor, s.Window.PID, s.Window.Started)
}

// WindowMail is what the hook delivers to the thread k bound as b: its lane's
// mail, less what its window's subscription keeps off that thread.
func (s *Store) WindowMail(ctx context.Context, k PeerKey, b Binding) (Filter, error) {
	f := LaneMail(k, b)
	sub, ok, err := s.windowSubscription(ctx, k, b)
	if err != nil || !ok {
		return f, err
	}
	f.Mute, f.ToKinds = sub.held(), sub.ToKinds
	return f, nil
}

// ChannelCarried selects what the channel of the window k's thread runs in
// already delivered: the records its subscription selects after where it
// started following and through its cursor. ok is false when no channel
// started following for that window.
func (s *Store) ChannelCarried(ctx context.Context, k PeerKey, b Binding) (Filter, bool, error) {
	sub, ok, err := s.windowSubscription(ctx, k, b)
	if err != nil || !ok {
		return Filter{}, false, err
	}
	floor, ok, err := s.Cursor(ctx, sub.FloorCursor(), b.Drive)
	if err != nil || !ok {
		return Filter{}, false, err
	}
	through, _, err := s.Cursor(ctx, sub.Cursor, b.Drive)
	if err != nil {
		return Filter{}, false, err
	}
	f := sub.Filter()
	f.After, f.Before = floor, through+1
	return f, true, nil
}

func (s *Store) windowSubscription(ctx context.Context, k PeerKey, b Binding) (Subscription, bool, error) {
	if k.Agent != "" {
		return Subscription{}, false, nil
	}
	sub, ok, err := s.SessionSubscription(ctx, k.Session)
	if err != nil || !ok || sub.Drive != b.Drive || !SameLane(sub.Reader, b.Lane) {
		return Subscription{}, false, err
	}
	return sub, true, nil
}

func (s *Store) Subscribe(ctx context.Context, sub Subscription) (Subscription, error) {
	sub, err := s.seed(ctx, sub)
	if err != nil {
		return Subscription{}, err
	}
	return sub, s.lockSubscriptions(func() error {
		all, err := s.subscriptions()
		if err != nil {
			return err
		}
		for _, other := range all {
			if other.Session == sub.Session && other.Window.PID != sub.Window.PID {
				if err := s.dropSubscription(other.Window.PID); err != nil {
					return err
				}
			}
		}
		return s.writeSubscription(sub)
	})
}

func (s *Store) seed(ctx context.Context, sub Subscription) (Subscription, error) {
	sub.At = s.Now().UTC()
	head, err := s.MaxSeq(ctx)
	if err != nil {
		return Subscription{}, err
	}
	sub.Head = head
	_, ok, err := s.Cursor(ctx, sub.Cursor, sub.Drive)
	if err != nil {
		return Subscription{}, err
	}
	if !ok {
		if err := s.SetCursor(ctx, sub.Cursor, sub.Drive, head); err != nil {
			return Subscription{}, err
		}
	}
	return sub, s.seedBatchCursor(ctx, sub)
}

func (s *Store) seedBatchCursor(ctx context.Context, sub Subscription) error {
	if len(sub.Batch) == 0 {
		return nil
	}
	_, ok, err := s.Cursor(ctx, sub.BatchCursor(), sub.Drive)
	if err != nil || ok {
		return err
	}
	from, _, err := s.Cursor(ctx, sub.Cursor, sub.Drive)
	if err != nil {
		return err
	}
	return s.SetCursor(ctx, sub.BatchCursor(), sub.Drive, from)
}

func (s *Store) Subscription(_ context.Context, w Window) (Subscription, bool, error) {
	sub, ok, err := readJSON[Subscription](s.subscriptionPath(w.PID))
	if err != nil || !ok || sub.Window != w {
		return Subscription{}, false, err
	}
	return sub, true, nil
}

// SubscriptionStamp names the state of w's subscription file, for a reader
// that rereads it only when the stamp moves.
type SubscriptionStamp struct {
	ModTime int64
	Size    int64
}

// SubscriptionStamp stats w's subscription file; a window with none stamps zero.
func (s *Store) SubscriptionStamp(w Window) (SubscriptionStamp, error) {
	info, err := os.Stat(s.subscriptionPath(w.PID))
	if errors.Is(err, fs.ErrNotExist) {
		return SubscriptionStamp{}, nil
	}
	if err != nil {
		return SubscriptionStamp{}, fmt.Errorf("stat subscription: %w", err)
	}
	return SubscriptionStamp{ModTime: info.ModTime().UnixNano(), Size: info.Size()}, nil
}

// SessionSubscription finds the subscription session holds in any window.
func (s *Store) SessionSubscription(_ context.Context, session string) (Subscription, bool, error) {
	all, err := s.subscriptions()
	if err != nil {
		return Subscription{}, false, err
	}
	for _, sub := range all {
		if sub.Session == session {
			return sub, true, nil
		}
	}
	return Subscription{}, false, nil
}

func (s *Store) Repoint(_ context.Context, session string, w Window) error {
	return s.lockSubscriptions(func() error { return s.repoint(session, w) })
}

func (s *Store) repoint(session string, w Window) error {
	all, err := s.subscriptions()
	if err != nil {
		return err
	}
	var moved *Subscription
	for i := range all {
		switch {
		case all[i].Session == session:
			moved = &all[i]
		case all[i].Window == w:
			if moved == nil {
				moved = &all[i]
			}
		case all[i].Window.PID == w.PID:
			if err := s.dropSubscription(w.PID); err != nil {
				return err
			}
		}
	}
	if moved == nil {
		return s.placeRegistered(session, w)
	}
	if moved.Session == session && moved.Window == w {
		return nil
	}
	if moved.Alias != "" {
		if err := s.followRegisteredSession(moved.Alias, session); err != nil {
			return err
		}
	}
	from := moved.Window.PID
	moved.Session, moved.Window = session, w
	if err := s.writeSubscription(*moved); err != nil {
		return err
	}
	if from == w.PID {
		return nil
	}
	return s.dropSubscription(from)
}

func (s *Store) Unsubscribe(ctx context.Context, w Window) (bool, error) {
	var removed bool
	err := s.lockSubscriptions(func() error {
		_, ok, err := s.Subscription(ctx, w)
		if err != nil || !ok {
			return err
		}
		removed = true
		return s.dropSubscription(w.PID)
	})
	return removed, err
}

func (s *Store) lockSubscriptions(fn func() error) error {
	if err := os.MkdirAll(s.subscriptionDir(), 0o700); err != nil {
		return fmt.Errorf("create subscriptions dir: %w", err)
	}
	lock, err := os.OpenFile(filepath.Join(s.subscriptionDir(), ".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open subscriptions lock: %w", err)
	}
	defer func() { _ = lock.Close() }()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("take subscriptions lock: %w", err)
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()
	return fn()
}

func (s *Store) subscriptionDir() string {
	return filepath.Join(s.home, "subscriptions")
}

func (s *Store) subscriptionPath(pid int) string {
	return filepath.Join(s.subscriptionDir(), strconv.Itoa(pid)+".json")
}

func (s *Store) subscriptions() ([]Subscription, error) {
	entries, err := os.ReadDir(s.subscriptionDir())
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list subscriptions: %w", err)
	}
	var out []Subscription
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		sub, ok, err := readJSON[Subscription](filepath.Join(s.subscriptionDir(), e.Name()))
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, sub)
		}
	}
	return out, nil
}

func (s *Store) writeSubscription(sub Subscription) error {
	return writeJSON(s.subscriptionDir(), s.subscriptionPath(sub.Window.PID), sub)
}

func readJSON[T any](path string) (T, bool, error) {
	var v T
	b, err := os.ReadFile(path) //nolint:gosec // G304: path is a file under the cci home.
	if errors.Is(err, fs.ErrNotExist) {
		return v, false, nil
	}
	if err != nil {
		return v, false, fmt.Errorf("read %s: %w", path, err)
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return v, false, fmt.Errorf("decode %s: %w", path, err)
	}
	return v, true, nil
}

func writeJSON(dir, path string, v any) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create %s: %w", dir, err)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("encode %s: %w", path, err)
	}
	tmp, err := os.CreateTemp(dir, ".write-*")
	if err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	if _, err := tmp.Write(b); err != nil {
		return errors.Join(fmt.Errorf("write %s: %w", path, err), tmp.Close(), os.Remove(tmp.Name()))
	}
	if err := tmp.Close(); err != nil {
		return errors.Join(fmt.Errorf("write %s: %w", path, err), os.Remove(tmp.Name()))
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return errors.Join(fmt.Errorf("write %s: %w", path, err), os.Remove(tmp.Name()))
	}
	return nil
}

func (s *Store) dropSubscription(pid int) error {
	if err := os.Remove(s.subscriptionPath(pid)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("drop subscription: %w", err)
	}
	return nil
}
