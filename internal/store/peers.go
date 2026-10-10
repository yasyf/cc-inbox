package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Peer is one session's row in the machine-wide registry: its drive binding
// plus the attributes it announced, so other sessions can find and address it.
type Peer struct {
	Session     string            `json:"session"`
	Drive       string            `json:"drive,omitempty"`
	Root        bool              `json:"root,omitempty"`
	Lane        string            `json:"lane,omitempty"`
	Role        string            `json:"role,omitempty"`
	Description string            `json:"description,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	Cwd         string            `json:"cwd,omitempty"`
	Repo        string            `json:"repo,omitempty"`
	Worktree    string            `json:"worktree,omitempty"`
	Branch      string            `json:"branch,omitempty"`
	Remote      string            `json:"remote,omitempty"`
	PID         int               `json:"pid,omitempty"`
	PIDStarted  int64             `json:"pid_started,omitempty"`
	Env         map[string]string `json:"env,omitempty"`
	AnnouncedAt time.Time         `json:"announced_at"`
	SeenAt      time.Time         `json:"seen_at"`
}

const peerColumns = "session, drive, root, lane, role, description, tags, cwd, repo, worktree, branch, remote, pid, pid_started, env, announced_at, seen_at"

// Announce upserts the session's announced attributes and leaves its drive
// binding as Bind and BindLane set it.
func (s *Store) Announce(ctx context.Context, p Peer) (Peer, error) {
	if p.Session == "" {
		return Peer{}, errors.New("announce: session is required")
	}
	now := s.Now().UTC()
	p.AnnouncedAt, p.SeenAt = now, now
	tags, _ := json.Marshal(nonNil(p.Tags))
	env, _ := json.Marshal(p.Env)
	_, err := s.db.ExecContext(ctx, `INSERT INTO sessions (session, drive, root, lane, role, description, tags, cwd, repo, worktree, branch, remote, pid, pid_started, env, announced_at, seen_at)
VALUES (?, '', 0, '', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (session) DO UPDATE SET role = excluded.role, description = excluded.description, tags = excluded.tags, cwd = excluded.cwd, repo = excluded.repo, worktree = excluded.worktree, branch = excluded.branch, remote = excluded.remote, pid = excluded.pid, pid_started = excluded.pid_started, env = excluded.env, announced_at = excluded.announced_at, seen_at = excluded.seen_at`,
		p.Session, p.Role, p.Description, string(tags), p.Cwd, p.Repo, p.Worktree, p.Branch, p.Remote, p.PID, p.PIDStarted, string(env), now.UnixMilli(), now.UnixMilli())
	if err != nil {
		return Peer{}, fmt.Errorf("announce %s: %w", p.Session, err)
	}
	return s.Peer(ctx, p.Session)
}

// Touch records that the session is still active.
func (s *Store) Touch(ctx context.Context, session string) error {
	if _, err := s.db.ExecContext(ctx, "UPDATE sessions SET seen_at = ? WHERE session = ?", s.Now().UTC().UnixMilli(), session); err != nil {
		return fmt.Errorf("touch %s: %w", session, err)
	}
	return nil
}

// Peer reads one session's row; a session that never bound or announced is sql.ErrNoRows.
func (s *Store) Peer(ctx context.Context, session string) (Peer, error) {
	p, err := scanPeer(s.db.QueryRowContext(ctx, "SELECT "+peerColumns+" FROM sessions WHERE session = ?", session))
	if errors.Is(err, sql.ErrNoRows) {
		return Peer{}, err
	}
	if err != nil {
		return Peer{}, fmt.Errorf("read peer %s: %w", session, err)
	}
	return p, nil
}

// Peers lists every known session, most recently seen first.
func (s *Store) Peers(ctx context.Context) ([]Peer, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT "+peerColumns+" FROM sessions ORDER BY seen_at DESC, session")
	if err != nil {
		return nil, fmt.Errorf("list peers: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Peer
	for rows.Next() {
		p, err := scanPeer(rows)
		if err != nil {
			return nil, fmt.Errorf("scan peer: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func scanPeer(row scanner) (Peer, error) {
	var (
		p                   Peer
		tags, env           string
		announcedAt, seenAt int64
	)
	if err := row.Scan(&p.Session, &p.Drive, &p.Root, &p.Lane, &p.Role, &p.Description, &tags, &p.Cwd, &p.Repo, &p.Worktree, &p.Branch, &p.Remote, &p.PID, &p.PIDStarted, &env, &announcedAt, &seenAt); err != nil {
		return Peer{}, err
	}
	if err := json.Unmarshal([]byte(tags), &p.Tags); err != nil {
		return Peer{}, fmt.Errorf("decode tags of %s: %w", p.Session, err)
	}
	if err := json.Unmarshal([]byte(env), &p.Env); err != nil {
		return Peer{}, fmt.Errorf("decode env of %s: %w", p.Session, err)
	}
	if len(p.Tags) == 0 {
		p.Tags = nil
	}
	if len(p.Env) == 0 {
		p.Env = nil
	}
	p.AnnouncedAt = time.UnixMilli(announcedAt).UTC()
	p.SeenAt = time.UnixMilli(seenAt).UTC()
	return p, nil
}

func nonNil(v []string) []string {
	if v == nil {
		return []string{}
	}
	return v
}
