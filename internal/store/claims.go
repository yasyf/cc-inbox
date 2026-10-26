package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Claim ties a session thread to an artifact: (rel, kind, key) such as
// (opened, pr, 34363). Kind and rel are open vocabularies. Source is "claim"
// for an explicit one and "#<seq>" for one derived from a record's refs.
type Claim struct {
	Session string    `json:"session"`
	Agent   string    `json:"agent,omitempty"`
	Rel     string    `json:"rel"`
	Kind    string    `json:"kind"`
	Key     string    `json:"key"`
	At      time.Time `json:"at"`
	Source  string    `json:"source"`
}

func (c Claim) Peer() PeerKey {
	return PeerKey{Session: c.Session, Agent: c.Agent}
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
	if _, err := s.db.ExecContext(ctx, `INSERT INTO claims (session, agent, rel, kind, key, at, origin) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (session, agent, rel, kind, key) DO UPDATE SET at = excluded.at, origin = excluded.origin`, c.Session, c.Agent, c.Rel, c.Kind, c.Key, c.At.UnixMilli(), s.Self()); err != nil {
		return Claim{}, fmt.Errorf("claim %s %s: %w", c.Rel, c.Ref(), err)
	}
	return c, nil
}

// Unclaim removes an explicit association and reports whether one existed.
func (s *Store) Unclaim(ctx context.Context, c Claim) (bool, error) {
	res, err := s.db.ExecContext(ctx, "DELETE FROM claims WHERE session = ? AND agent = ? AND rel = ? AND kind = ? AND key = ? AND origin = ?", c.Session, c.Agent, c.Rel, c.Kind, c.Key, s.Self())
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
	query, params := f.query()
	rows, err := s.db.QueryContext(ctx, query, params...)
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
		if err := rows.Scan(&c.Session, &c.Agent, &c.Rel, &c.Kind, &c.Key, &at, &c.Source); err != nil {
			return nil, fmt.Errorf("scan claim: %w", err)
		}
		c.At = time.UnixMilli(at).UTC()
		out = append(out, c)
	}
	return out, rows.Err()
}

// query filters each record's refs by kind and key before joining the lane's
// sessions, so a lookup by ref never pairs every record with every session.
func (f ClaimFilter) query() (string, []any) {
	var (
		branches []string
		params   []any
	)
	var explicit []string
	if f.Kind != "" {
		explicit = append(explicit, "kind = ?")
		params = append(params, f.Kind)
	}
	if f.Key != "" {
		match, p := keyMatch("kind", "key", f.Key)
		explicit = append(explicit, match)
		params = append(params, p...)
	}
	branches = append(branches, "SELECT session, agent, rel, kind, key, at, 'claim' AS source FROM claims"+where(explicit))
	for _, ref := range derivedRefs {
		if f.Kind != "" && f.Kind != ref.kind {
			continue
		}
		for _, join := range laneJoins {
			clauses := []string{"s.origin = r.origin", "s.drive = r.drive", join}
			if f.Key != "" {
				match, p := keyMatch("'"+ref.kind+"'", refKey, f.Key)
				clauses = append(clauses, match)
				params = append(params, p...)
			}
			branches = append(branches, "SELECT s.session, s.agent, "+derivedRel+", '"+ref.kind+"', "+refKey+", r.at, '#' || r.seq FROM records r CROSS JOIN json_each(r.refs, '"+ref.path+"') j CROSS JOIN sessions s"+where(clauses))
		}
	}
	var outer []string
	if len(f.Sessions) > 0 {
		outer = append(outer, "session IN ("+placeholders(len(f.Sessions))+")")
		for _, v := range f.Sessions {
			params = append(params, v)
		}
	}
	if f.Rel != "" {
		outer = append(outer, "rel = ?")
		params = append(params, f.Rel)
	}
	return "SELECT session, agent, rel, kind, key, at, source FROM (" + strings.Join(branches, " UNION ALL ") + ")" + where(outer) + " ORDER BY at DESC, source", params
}

func keyMatch(kind, key, value string) (string, []any) {
	alternatives := []string{key + " = ?", "(" + kind + " = 'build' AND " + key + ` LIKE '%/builds/' || ? ESCAPE '\')`}
	params := []any{value, likeLiteral(value)}
	if pr, err := ParsePR(value); err == nil {
		text, param := pr.textMatch(key)
		alternatives = append(alternatives, "("+kind+" = 'pr' AND ("+key+" = ? OR "+text+"))")
		params = append(params, strconv.Itoa(pr.Number), param)
	}
	return "(" + strings.Join(alternatives, " OR ") + ")", params
}

func where(clauses []string) string {
	if len(clauses) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(clauses, " AND ")
}

func likeLiteral(v string) string {
	return strings.NewReplacer(`\`, `\\`, "%", `\%`, "_", `\_`).Replace(v)
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

const refKey = "CAST(j.value AS TEXT)"

var (
	derivedRefs = []struct{ kind, path string }{{"pr", "$.prs"}, {"build", "$.builds"}, {"stack", "$.stacks"}, {"target", "$.targets"}}
	laneJoins   = []string{"s.lane = r.lane", "s.lane = '' AND s.root AND r.lane IN ('root', 'main')"}
)
