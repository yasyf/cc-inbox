package store

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Registration keeps a session under its alias. The alias owns the session's
// channel subscription: each window the session runs in gets a copy, which
// follows a resume into a new window and a /clear into a new session, so the
// session hears with no cci subscribe. Cwd, Command and ResumePrompt run the
// session again, and Notify names the lanes told when it stops hearing.
type Registration struct {
	Subscription
	Cwd          string   `json:"cwd,omitempty"`
	Command      string   `json:"command,omitempty"`
	ResumePrompt string   `json:"resume_prompt,omitempty"`
	Notify       []string `json:"notify,omitempty"`
}

var aliasName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// Register keeps r.Session under r.Alias, replacing what the alias held. It
// seeds the cursors as Subscribe does, and when r.Window names the window the
// session runs in, that window's subscription becomes the alias's.
func (s *Store) Register(ctx context.Context, r Registration) (Registration, error) {
	if !aliasName.MatchString(r.Alias) {
		return Registration{}, fmt.Errorf("alias %q is not a name of letters, digits, '.', '_' and '-'", r.Alias)
	}
	sub, err := s.seed(ctx, r.Subscription)
	if err != nil {
		return Registration{}, err
	}
	r.Subscription = sub
	return r, s.lockSubscriptions(func() error {
		others, err := s.registrations()
		if err != nil {
			return err
		}
		for _, other := range others {
			if other.Session == r.Session && other.Alias != r.Alias {
				return fmt.Errorf("session %s is registered as %s: cci session rm %s first", r.Session, other.Alias, other.Alias)
			}
		}
		kept := r
		kept.Window = Window{}
		if err := s.writeRegistration(kept); err != nil {
			return err
		}
		subs, err := s.subscriptions()
		if err != nil {
			return err
		}
		for _, held := range subs {
			if held.Session == r.Session || held.Alias == r.Alias {
				if err := s.dropSubscription(held.Window.PID); err != nil {
					return err
				}
			}
		}
		if r.Window.PID == 0 {
			return nil
		}
		return s.writeSubscription(r.Subscription)
	})
}

// Registrations lists every registered session, by alias.
func (s *Store) Registrations(_ context.Context) ([]Registration, error) {
	return s.registrations()
}

// SessionRegistration finds the registration that keeps session.
func (s *Store) SessionRegistration(_ context.Context, session string) (Registration, bool, error) {
	all, err := s.registrations()
	if err != nil {
		return Registration{}, false, err
	}
	for _, r := range all {
		if r.Session == session {
			return r, true, nil
		}
	}
	return Registration{}, false, nil
}

// Unregister removes alias and the window subscription it owned, returning
// what it held.
func (s *Store) Unregister(_ context.Context, alias string) (Registration, bool, error) {
	var (
		r     Registration
		found bool
	)
	err := s.lockSubscriptions(func() error {
		var err error
		if r, found, err = readJSON[Registration](s.registrationPath(alias)); err != nil || !found {
			return err
		}
		subs, err := s.subscriptions()
		if err != nil {
			return err
		}
		for _, held := range subs {
			if held.Alias == alias {
				if err := s.dropSubscription(held.Window.PID); err != nil {
					return err
				}
			}
		}
		if err := os.Remove(s.registrationPath(alias)); err != nil {
			return fmt.Errorf("drop registration %s: %w", alias, err)
		}
		return nil
	})
	return r, found, err
}

func (s *Store) followRegisteredSession(alias, session string) error {
	r, ok, err := readJSON[Registration](s.registrationPath(alias))
	if err != nil || !ok || r.Session == session {
		return err
	}
	r.Session = session
	return s.writeRegistration(r)
}

func (s *Store) placeRegistered(session string, w Window) error {
	all, err := s.registrations()
	if err != nil {
		return err
	}
	for _, r := range all {
		if r.Session == session {
			sub := r.Subscription
			sub.Window = w
			return s.writeSubscription(sub)
		}
	}
	return nil
}

func (s *Store) registrationDir() string {
	return filepath.Join(s.home, "registrations")
}

func (s *Store) registrationPath(alias string) string {
	return filepath.Join(s.registrationDir(), alias+".json")
}

func (s *Store) registrations() ([]Registration, error) {
	entries, err := os.ReadDir(s.registrationDir())
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list registrations: %w", err)
	}
	var out []Registration
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		r, ok, err := readJSON[Registration](filepath.Join(s.registrationDir(), e.Name()))
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, r)
		}
	}
	return out, nil
}

func (s *Store) writeRegistration(r Registration) error {
	return writeJSON(s.registrationDir(), s.registrationPath(r.Alias), r)
}
