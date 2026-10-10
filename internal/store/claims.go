package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Claim ties a session to an artifact: (rel, kind, key) such as
// (opened, pr, 34363). Kind and rel are open vocabularies. Source is "claim"
// for an explicit one and "#<seq>" for one derived from a record's refs.
type Claim struct {
	Session string    `json:"session"`
	Rel     string    `json:"rel"`
	Kind    string    `json:"kind"`
	Key     string    `json:"key"`
	At      time.Time `json:"at"`
	Source  string    `json:"source"`
}

// Ref renders the claim's artifact as kind:key.
func (c Claim) Ref() string {
	return c.Kind + ":" + c.Key
}

// ParseRef splits kind:key, refusing an empty side.
func ParseRef(ref string) (kind, key string, err error) {
	kind, key, ok := strings.Cut(ref, ":")
	if !ok || kind == "" || key == "" {
		return "", "", fmt.Errorf("ref %q is not <kind>:<key>", ref)
	}
	return kind, key, nil
}

// Claim records an explicit association, replacing the time of an identical one.
func (s *Store) Claim(ctx context.Context, c Claim) (Claim, error) {
	if c.Session == "" || c.Rel == "" || c.Kind == "" || c.Key == "" {
		return Claim{}, errors.New("claim needs a session, rel, kind and key")
	}
	c.At, c.Source = s.Now().UTC(), "claim"
	if _, err := s.db.ExecContext(ctx, `INSERT INTO claims (session, rel, kind, key, at) VALUES (?, ?, ?, ?, ?)
ON CONFLICT (session, rel, kind, key) DO UPDATE SET at = excluded.at`, c.Session, c.Rel, c.Kind, c.Key, c.At.UnixMilli()); err != nil {
		return Claim{}, fmt.Errorf("claim %s %s: %w", c.Rel, c.Ref(), err)
	}
	return c, nil
}

// Unclaim removes an explicit association and reports whether one existed.
func (s *Store) Unclaim(ctx context.Context, c Claim) (bool, error) {
	res, err := s.db.ExecContext(ctx, "DELETE FROM claims WHERE session = ? AND rel = ? AND kind = ? AND key = ?", c.Session, c.Rel, c.Kind, c.Key)
	if err != nil {
		return false, fmt.Errorf("unclaim %s %s: %w", c.Rel, c.Ref(), err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("unclaim %s %s: %w", c.Rel, c.Ref(), err)
	}
	return n > 0, nil
}

// ClaimFilter selects claims; every set field must match. A build key
// matches a stored build URL ending in /builds/<key> as well as the key itself.
type ClaimFilter struct {
	Sessions []string
	Rel      string
	Kind     string
	Key      string
}

// Claims lists explicit claims and the ones derived from posted records, newest
// first. A record's refs tie its lane's bound sessions to each pr, build, stack
// and target it names, with the rel the record's kind implies.
func (s *Store) Claims(ctx context.Context, f ClaimFilter) ([]Claim, error) {
	var (
		clauses []string
		params  []any
	)
	if len(f.Sessions) > 0 {
		clauses = append(clauses, "session IN ("+placeholders(len(f.Sessions))+")")
		for _, v := range f.Sessions {
			params = append(params, v)
		}
	}
	if f.Rel != "" {
		clauses = append(clauses, "rel = ?")
		params = append(params, f.Rel)
	}
	if f.Kind != "" {
		clauses = append(clauses, "kind = ?")
		params = append(params, f.Kind)
	}
	if f.Key != "" {
		clauses = append(clauses, "(key = ? OR (kind = 'build' AND key LIKE '%/builds/' || ?))")
		params = append(params, f.Key, f.Key)
	}
	where := ""
	if len(clauses) > 0 {
		where = " WHERE " + strings.Join(clauses, " AND ")
	}
	rows, err := s.db.QueryContext(ctx, "SELECT session, rel, kind, key, at, source FROM ("+claimsUnion+")"+where+" ORDER BY at DESC, source", params...)
	if err != nil {
		return nil, fmt.Errorf("list claims: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Claim
	for rows.Next() {
		var (
			c  Claim
			at int64
		)
		if err := rows.Scan(&c.Session, &c.Rel, &c.Kind, &c.Key, &at, &c.Source); err != nil {
			return nil, fmt.Errorf("scan claim: %w", err)
		}
		c.At = time.UnixMilli(at).UTC()
		out = append(out, c)
	}
	return out, rows.Err()
}

const derivedRel = `CASE r.kind
	WHEN 'opened' THEN 'opened'
	WHEN 'landed' THEN 'landed'
	WHEN 'review' THEN 'reviewed'
	WHEN 'release' THEN 'released'
	WHEN 'applied' THEN 'released'
	WHEN 'incident' THEN 'handling'
	WHEN 'handoff' THEN 'handed-off'
	ELSE 'mentioned' END`

const laneSessions = `JOIN sessions s ON s.drive = r.drive AND (s.lane = r.lane OR (s.root AND r.lane IN ('root', 'main')))`

var claimsUnion = strings.Join([]string{
	"SELECT session, rel, kind, key, at, 'claim' AS source FROM claims",
	derived("pr", "$.prs"),
	derived("build", "$.builds"),
	derived("stack", "$.stacks"),
	derived("target", "$.targets"),
}, " UNION ALL ")

func derived(kind, path string) string {
	return "SELECT s.session, " + derivedRel + ", '" + kind + "', CAST(j.value AS TEXT), r.at, '#' || r.seq FROM records r, json_each(r.refs, '" + path + "') j " + laneSessions
}
