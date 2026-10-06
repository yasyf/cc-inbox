package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

type Subscription struct {
	Session string       `json:"session"`
	Window  int          `json:"window"`
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
	encoded, err := json.Marshal(sub.Kinds)
	if err != nil {
		return Subscription{}, fmt.Errorf("encode subscription kinds: %w", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Subscription{}, fmt.Errorf("subscribe: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "DELETE FROM subscriptions WHERE window = ? AND session != ?", sub.Window, sub.Session); err != nil {
		return Subscription{}, fmt.Errorf("subscribe: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO subscriptions (session, window, drive, reader, kinds, cursor, at) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (session) DO UPDATE SET window = excluded.window, drive = excluded.drive, reader = excluded.reader, kinds = excluded.kinds, cursor = excluded.cursor, at = excluded.at`,
		sub.Session, sub.Window, sub.Drive, sub.Reader, string(encoded), sub.Cursor, sub.At.UnixMilli()); err != nil {
		return Subscription{}, fmt.Errorf("subscribe: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Subscription{}, fmt.Errorf("subscribe: %w", err)
	}
	return sub, nil
}

func (s *Store) Subscription(ctx context.Context, window int) (Subscription, bool, error) {
	var (
		sub     Subscription
		encoded string
		at      int64
	)
	err := s.db.QueryRowContext(ctx, "SELECT session, window, drive, reader, kinds, cursor, at FROM subscriptions WHERE window = ?", window).
		Scan(&sub.Session, &sub.Window, &sub.Drive, &sub.Reader, &encoded, &sub.Cursor, &at)
	if errors.Is(err, sql.ErrNoRows) {
		return Subscription{}, false, nil
	}
	if err != nil {
		return Subscription{}, false, fmt.Errorf("read subscription: %w", err)
	}
	if err := json.Unmarshal([]byte(encoded), &sub.Kinds); err != nil {
		return Subscription{}, false, fmt.Errorf("decode subscription kinds: %w", err)
	}
	sub.At = time.UnixMilli(at).UTC()
	return sub, true, nil
}

func (s *Store) Repoint(ctx context.Context, session string, window int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("repoint subscription: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "DELETE FROM subscriptions WHERE window = ? AND session != ? AND EXISTS (SELECT 1 FROM subscriptions WHERE session = ?)", window, session, session); err != nil {
		return fmt.Errorf("repoint subscription: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE subscriptions SET window = ?, session = ? WHERE session = ? OR window = ?", window, session, session, window); err != nil {
		return fmt.Errorf("repoint subscription: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("repoint subscription: %w", err)
	}
	return nil
}

func (s *Store) Unsubscribe(ctx context.Context, window int) (bool, error) {
	res, err := s.db.ExecContext(ctx, "DELETE FROM subscriptions WHERE window = ?", window)
	if err != nil {
		return false, fmt.Errorf("unsubscribe: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("unsubscribe: %w", err)
	}
	return n > 0, nil
}
