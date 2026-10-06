package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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
	Session string       `json:"session"`
	Window  Window       `json:"window"`
	Drive   string       `json:"drive"`
	Reader  string       `json:"reader"`
	Kinds   []kinds.Kind `json:"kinds"`
	Cursor  string       `json:"cursor"`
	At      time.Time    `json:"at"`
}

func (s Subscription) Filter() Filter {
	return Filter{Drive: s.Drive, For: s.Reader, Kinds: s.Kinds}
}

func (s *Store) Subscribe(ctx context.Context, sub Subscription) (Subscription, error) {
	sub.At = s.Now().UTC()
	_, ok, err := s.Cursor(ctx, sub.Cursor, sub.Drive)
	if err != nil {
		return Subscription{}, err
	}
	if !ok {
		head, err := s.MaxSeq(ctx)
		if err != nil {
			return Subscription{}, err
		}
		if err := s.SetCursor(ctx, sub.Cursor, sub.Drive, head); err != nil {
			return Subscription{}, err
		}
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

func (s *Store) Subscription(_ context.Context, w Window) (Subscription, bool, error) {
	sub, ok, err := s.readSubscription(s.subscriptionPath(w.PID))
	if err != nil || !ok || sub.Window != w {
		return Subscription{}, false, err
	}
	return sub, true, nil
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
	if moved == nil || moved.Session == session && moved.Window == w {
		return nil
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

func (s *Store) readSubscription(path string) (Subscription, bool, error) {
	b, err := os.ReadFile(path) //nolint:gosec // G304: path is a subscription file under the cci home.
	if errors.Is(err, fs.ErrNotExist) {
		return Subscription{}, false, nil
	}
	if err != nil {
		return Subscription{}, false, fmt.Errorf("read subscription: %w", err)
	}
	var sub Subscription
	if err := json.Unmarshal(b, &sub); err != nil {
		return Subscription{}, false, fmt.Errorf("decode subscription %s: %w", path, err)
	}
	return sub, true, nil
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
		sub, ok, err := s.readSubscription(filepath.Join(s.subscriptionDir(), e.Name()))
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
	if err := os.MkdirAll(s.subscriptionDir(), 0o700); err != nil {
		return fmt.Errorf("create subscriptions dir: %w", err)
	}
	b, err := json.Marshal(sub)
	if err != nil {
		return fmt.Errorf("encode subscription: %w", err)
	}
	tmp, err := os.CreateTemp(s.subscriptionDir(), ".subscription-*")
	if err != nil {
		return fmt.Errorf("write subscription: %w", err)
	}
	if _, err := tmp.Write(b); err != nil {
		return errors.Join(fmt.Errorf("write subscription: %w", err), tmp.Close(), os.Remove(tmp.Name()))
	}
	if err := tmp.Close(); err != nil {
		return errors.Join(fmt.Errorf("write subscription: %w", err), os.Remove(tmp.Name()))
	}
	if err := os.Rename(tmp.Name(), s.subscriptionPath(sub.Window.PID)); err != nil {
		return errors.Join(fmt.Errorf("write subscription: %w", err), os.Remove(tmp.Name()))
	}
	return nil
}

func (s *Store) dropSubscription(pid int) error {
	if err := os.Remove(s.subscriptionPath(pid)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("drop subscription: %w", err)
	}
	return nil
}
