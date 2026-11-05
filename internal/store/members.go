package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/yasyf/cc-inbox/internal/node"
)

// Member is a node this one has pinned: its key, the tier it holds, and who
// admitted it.
type Member struct {
	Node      string    `json:"node"`
	PublicKey []byte    `json:"public_key"`
	Tier      node.Tier `json:"tier"`
	Via       string    `json:"via"`
	Host      string    `json:"host,omitempty"`
	By        string    `json:"by"`
	At        time.Time `json:"at"`
}

var (
	tiers = []node.Tier{node.TierOwner, node.TierService, node.TierPaired}
	vias  = []string{"mesh", "relay", "pair"}
)

// PutMember pins m, replacing the key and tier of a node already pinned.
func (s *Store) PutMember(ctx context.Context, m Member) error {
	if m.Node != node.IDOf(m.PublicKey) {
		return fmt.Errorf("member %s does not match its public key (%s)", m.Node, node.IDOf(m.PublicKey))
	}
	if !slices.Contains(tiers, m.Tier) {
		return fmt.Errorf("member %s: tier %q is not owner, service or paired", m.Node, m.Tier)
	}
	if !slices.Contains(vias, m.Via) {
		return fmt.Errorf("member %s: via %q is not mesh, relay or pair", m.Node, m.Via)
	}
	if m.At.IsZero() {
		m.At = s.Now().UTC()
	}
	if _, err := s.rw.ExecContext(ctx, `INSERT INTO members (node, public_key, tier, via, host, by, at) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (node) DO UPDATE SET public_key = excluded.public_key, tier = excluded.tier, via = excluded.via, host = excluded.host, by = excluded.by, at = excluded.at`,
		m.Node, m.PublicKey, string(m.Tier), m.Via, m.Host, m.By, m.At.UnixMilli()); err != nil {
		return fmt.Errorf("pin member %s: %w", m.Node, err)
	}
	return nil
}

// Member reads one pinned node; an unknown node is sql.ErrNoRows.
func (s *Store) Member(ctx context.Context, id string) (Member, error) {
	m, err := scanMember(s.db.QueryRowContext(ctx, "SELECT node, public_key, tier, via, host, by, at FROM members WHERE node = ?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return Member{}, err
	}
	if err != nil {
		return Member{}, fmt.Errorf("read member %s: %w", id, err)
	}
	return m, nil
}

func (s *Store) Members(ctx context.Context) ([]Member, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT node, public_key, tier, via, host, by, at FROM members ORDER BY node")
	if err != nil {
		return nil, fmt.Errorf("list members: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Member
	for rows.Next() {
		m, err := scanMember(rows)
		if err != nil {
			return nil, fmt.Errorf("scan member: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// DropMember unpins a node and forgets its watermark, sessions and claims.
func (s *Store) DropMember(ctx context.Context, id string) (bool, error) {
	tx, err := s.rw.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("drop member %s: %w", id, err)
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx, "DELETE FROM members WHERE node = ?", id)
	if err != nil {
		return false, fmt.Errorf("drop member %s: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("drop member %s: %w", id, err)
	}
	if err := forget(ctx, tx, id); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("drop member %s: %w", id, err)
	}
	return n > 0, nil
}

func scanMember(row scanner) (Member, error) {
	var (
		m    Member
		tier string
		at   int64
	)
	if err := row.Scan(&m.Node, &m.PublicKey, &tier, &m.Via, &m.Host, &m.By, &at); err != nil {
		return Member{}, err
	}
	m.Tier, m.At = node.Tier(tier), time.UnixMilli(at).UTC()
	return m, nil
}
