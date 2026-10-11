package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// PeerKey names one thread of a session: its root thread when Agent is
// empty, or the subagent or teammate the harness calls Agent.
type PeerKey struct {
	Session string
	Agent   string
}

// String is the key as addresses and cursors spell it: <session> or <session>/<agent>.
func (k PeerKey) String() string {
	if k.Agent == "" {
		return k.Session
	}
	return k.Session + "/" + k.Agent
}

// Peer is one session thread's row in the machine-wide registry: its drive
// binding plus the attributes it announced, so other sessions can find and
// address it.
type Peer struct {
	Session     string            `json:"session"`
	Agent       string            `json:"agent,omitempty"`
	AgentName   string            `json:"agent_name,omitempty"`
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
	Superseded  bool              `json:"superseded,omitempty"`
}

func (p Peer) Key() PeerKey {
	return PeerKey{Session: p.Session, Agent: p.Agent}
}

const peerColumns = "session, agent, agent_name, drive, root, lane, role, description, tags, cwd, repo, worktree, branch, remote, pid, pid_started, env, announced_at, seen_at, superseded"

// Announce upserts the thread's announced attributes and leaves its drive
// binding as Bind and BindLane set it. An empty role, description, tag list
// or agent name keeps the stored one, so an automatic announce never erases
// an explicit one. Another session announced from the same process, the one
// a /clear replaced, is marked superseded.
func (s *Store) Announce(ctx context.Context, p Peer) (Peer, error) {
	if p.Session == "" {
		return Peer{}, errors.New("announce: session is required")
	}
	now := s.Now().UTC()
	p.AnnouncedAt, p.SeenAt = now, now
	tags, _ := json.Marshal(nonNil(p.Tags))
	env, _ := json.Marshal(p.Env)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Peer{}, fmt.Errorf("announce %s: %w", p.Session, err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `INSERT INTO sessions (session, agent, agent_name, drive, root, lane, role, description, tags, cwd, repo, worktree, branch, remote, pid, pid_started, env, announced_at, seen_at, superseded)
VALUES (?, ?, ?, '', 0, '', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
ON CONFLICT (session, agent) DO UPDATE SET
	agent_name = CASE WHEN excluded.agent_name = '' THEN sessions.agent_name ELSE excluded.agent_name END,
	role = CASE WHEN excluded.role = '' THEN sessions.role ELSE excluded.role END,
	description = CASE WHEN excluded.description = '' THEN sessions.description ELSE excluded.description END,
	tags = CASE WHEN excluded.tags = '[]' THEN sessions.tags ELSE excluded.tags END,
	cwd = excluded.cwd, repo = excluded.repo, worktree = excluded.worktree, branch = excluded.branch, remote = excluded.remote,
	pid = excluded.pid, pid_started = excluded.pid_started, env = excluded.env, announced_at = excluded.announced_at, seen_at = excluded.seen_at, superseded = 0`,
		p.Session, p.Agent, p.AgentName, p.Role, p.Description, string(tags), p.Cwd, p.Repo, p.Worktree, p.Branch, p.Remote, p.PID, p.PIDStarted, string(env), now.UnixMilli(), now.UnixMilli()); err != nil {
		return Peer{}, fmt.Errorf("announce %s: %w", p.Key(), err)
	}
	if p.PID != 0 && p.Agent == "" {
		if _, err := tx.ExecContext(ctx, "UPDATE sessions SET superseded = 1 WHERE pid = ? AND pid_started = ? AND session != ?", p.PID, p.PIDStarted, p.Session); err != nil {
			return Peer{}, fmt.Errorf("supersede sessions of pid %d: %w", p.PID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return Peer{}, fmt.Errorf("announce %s: %w", p.Key(), err)
	}
	return s.Peer(ctx, p.Key())
}

// Touch records that the thread is still active.
func (s *Store) Touch(ctx context.Context, k PeerKey) error {
	if _, err := s.db.ExecContext(ctx, "UPDATE sessions SET seen_at = ? WHERE session = ? AND agent = ?", s.Now().UTC().UnixMilli(), k.Session, k.Agent); err != nil {
		return fmt.Errorf("touch %s: %w", k, err)
	}
	return nil
}

// Retire marks a subagent or teammate whose harness reported it stopped as no longer live.
func (s *Store) Retire(ctx context.Context, k PeerKey) error {
	if _, err := s.db.ExecContext(ctx, "UPDATE sessions SET superseded = 1, seen_at = ? WHERE session = ? AND agent = ?", s.Now().UTC().UnixMilli(), k.Session, k.Agent); err != nil {
		return fmt.Errorf("retire %s: %w", k, err)
	}
	return nil
}

// Peer reads one thread's row; a thread that never bound or announced is sql.ErrNoRows.
func (s *Store) Peer(ctx context.Context, k PeerKey) (Peer, error) {
	p, err := scanPeer(s.db.QueryRowContext(ctx, "SELECT "+peerColumns+" FROM sessions WHERE session = ? AND agent = ?", k.Session, k.Agent))
	if errors.Is(err, sql.ErrNoRows) {
		return Peer{}, err
	}
	if err != nil {
		return Peer{}, fmt.Errorf("read peer %s: %w", k, err)
	}
	return p, nil
}

// Peers lists every known session thread, most recently seen first.
func (s *Store) Peers(ctx context.Context) ([]Peer, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT "+peerColumns+" FROM sessions ORDER BY seen_at DESC, session, agent")
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
	if err := row.Scan(&p.Session, &p.Agent, &p.AgentName, &p.Drive, &p.Root, &p.Lane, &p.Role, &p.Description, &tags, &p.Cwd, &p.Repo, &p.Worktree, &p.Branch, &p.Remote, &p.PID, &p.PIDStarted, &env, &announcedAt, &seenAt, &p.Superseded); err != nil {
		return Peer{}, err
	}
	if err := json.Unmarshal([]byte(tags), &p.Tags); err != nil {
		return Peer{}, fmt.Errorf("decode tags of %s: %w", p.Key(), err)
	}
	if err := json.Unmarshal([]byte(env), &p.Env); err != nil {
		return Peer{}, fmt.Errorf("decode env of %s: %w", p.Key(), err)
	}
	if len(p.Tags) == 0 {
		p.Tags = nil
	}
	if len(p.Env) == 0 {
		p.Env = nil
	}
	p.AnnouncedAt = time.UnixMilli(announcedAt).UTC()
	p.SeenAt = time.UnixMilli(seenAt).UTC()
	if p.Role == "" {
		p.Role = inferredRole(p)
	}
	return p, nil
}

func inferredRole(p Peer) string {
	switch {
	case p.Root:
		return "root"
	case p.Lane != "":
		return "lane"
	case p.Agent != "":
		return "agent"
	}
	return "session"
}

func nonNil(v []string) []string {
	if v == nil {
		return []string{}
	}
	return v
}
