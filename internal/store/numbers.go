package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/yasyf/cc-inbox/internal/node"
)

const tagLen = 8

// SetOwner records whether this node holds the owner tier, which decides
// whether the records it writes carry bare numbers.
func (s *Store) SetOwner(owner bool) {
	s.owner = owner
}

func (s *Store) owners(ctx context.Context) (map[string]bool, error) {
	return readOwners(ctx, s.db, s)
}

func readOwners(ctx context.Context, q rowsQuerier, s *Store) (map[string]bool, error) {
	rows, err := q.QueryContext(ctx, "SELECT node FROM members WHERE tier = ?", string(node.TierOwner))
	if err != nil {
		return nil, fmt.Errorf("read owner members: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]bool{s.Self(): s.owner}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, fmt.Errorf("scan owner member: %w", err)
		}
		out[n] = true
	}
	return out, rows.Err()
}

func number(owners map[string]bool, origin string, seq int64) string {
	if owners[origin] {
		return fmt.Sprintf("#%d", seq)
	}
	return fmt.Sprintf("#%d@%s", seq, origin[:min(tagLen, len(origin))])
}

// numbered names each record, and the records it answers and resolves, the way
// every node names them: the origin node's number, bare when an owner node
// wrote it, else followed by @ and the origin node's first eight characters.
func (s *Store) numbered(ctx context.Context, records []Record) error {
	if len(records) == 0 {
		return nil
	}
	var targets []int64
	for _, r := range records {
		targets = append(targets, r.Re, r.Resolves)
		targets = append(targets, r.AlsoResolves...)
	}
	owners, names, err := s.ownersAndNames(ctx, targets)
	if err != nil {
		return err
	}
	for i := range records {
		r := &records[i]
		r.Num, r.ReNum, r.CloseNums = s.name(owners, *r), names[r.Re], nil
		for _, seq := range r.Closes() {
			r.CloseNums = append(r.CloseNums, names[seq])
		}
	}
	return nil
}

func (s *Store) ownersAndNames(ctx context.Context, targets []int64) (map[string]bool, map[int64]string, error) {
	if m := s.window.model.Load(); m != nil {
		if owners, names, ok := m.namesOf(ctx, s, targets); ok {
			return owners, names, nil
		}
	}
	owners, err := s.owners(ctx)
	if err != nil {
		return nil, nil, err
	}
	names, err := s.names(ctx, owners, targets)
	return owners, names, err
}

func (s *Store) name(owners map[string]bool, r Record) string {
	if r.Origin == "" {
		return number(owners, s.Self(), r.Seq)
	}
	return number(owners, r.Origin, r.OriginSeq)
}

func (s *Store) names(ctx context.Context, owners map[string]bool, seqs []int64) (map[int64]string, error) {
	var wanted []int64
	for _, seq := range seqs {
		if seq != 0 {
			wanted = append(wanted, seq)
		}
	}
	out := map[int64]string{}
	if len(wanted) == 0 {
		return out, nil
	}
	ids, _ := json.Marshal(wanted)
	rows, err := s.db.QueryContext(ctx, "SELECT seq, origin, origin_seq FROM records WHERE seq IN (SELECT value FROM json_each(?))", string(ids))
	if err != nil {
		return nil, fmt.Errorf("name targets: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			r      Record
			origin string
		)
		if err := rows.Scan(&r.Seq, &origin, &r.OriginSeq); err != nil {
			return nil, fmt.Errorf("scan target: %w", err)
		}
		r.Origin = s.local(origin)
		out[r.Seq] = s.name(owners, r)
	}
	return out, rows.Err()
}

func (s *Store) origins(ctx context.Context, tag string, tagged bool) (string, []any, error) {
	if tagged {
		return "origin LIKE ?", []any{tag + "%"}, nil
	}
	owners, err := s.owners(ctx)
	if err != nil {
		return "", nil, err
	}
	args := make([]any, 0, len(owners))
	for origin, owner := range owners {
		if owner && origin != s.Self() {
			args = append(args, origin)
		}
	}
	return "origin IN (" + placeholders(len(args)) + ")", args, nil
}

// Number names r the way every node names it.
func (s *Store) Number(ctx context.Context, r Record) (string, error) {
	owners, err := s.owners(ctx)
	if err != nil {
		return "", err
	}
	return s.name(owners, r), nil
}

// ErrNoNumber is returned for a record number this store holds no record for.
var ErrNoNumber = errors.New("no record has this number")

// Number is a record number as any node prints it: #N names record N of an
// owner node, and #N@tag record N of the node whose id starts with Tag.
type Number struct {
	Seq int64
	Tag string
}

// ParseNumber reads a record number, #N or #N@node.
func ParseNumber(ref string) (Number, error) {
	digits, tag, tagged := strings.Cut(strings.TrimPrefix(ref, "#"), "@")
	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || n < 0 || tagged && (tag == "" || n == 0) {
		return Number{}, fmt.Errorf("%q is not a record number (#N or #N@node)", ref)
	}
	return Number{Seq: n, Tag: tag}, nil
}

// Resolve turns a record number as any node prints it, #N or #N@node, into
// this store's seq for that record. On a node that is not an owner, a bare #N
// held from no owner node names this node's own record N, the number it printed
// as #N@node.
func (s *Store) Resolve(ctx context.Context, ref string) (int64, error) {
	num, err := ParseNumber(ref)
	if err != nil {
		return 0, err
	}
	n, tag, tagged := num.Seq, num.Tag, num.Tag != ""
	if tagged && strings.HasPrefix(s.Self(), tag) || !tagged && (s.owner || n == 0) {
		if !tagged && n != 0 {
			if err := s.unambiguous(ctx, ref, n); err != nil {
				return 0, err
			}
		}
		return n, nil
	}
	match, args, err := s.origins(ctx, tag, tagged)
	if err != nil {
		return 0, err
	}
	var seq int64
	err = sql.ErrNoRows
	if len(args) > 0 {
		err = s.db.QueryRowContext(ctx, "SELECT seq FROM records WHERE "+match+" AND origin_seq = ? UNION ALL SELECT seq FROM aliases WHERE "+match+" AND origin_seq = ? LIMIT 1",
			append(append(append([]any{}, args...), n), append(args, n)...)...).Scan(&seq)
	}
	if errors.Is(err, sql.ErrNoRows) && !tagged {
		err = s.db.QueryRowContext(ctx, "SELECT seq FROM records WHERE origin = ? AND origin_seq = ?", s.Self(), n).Scan(&seq)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("%s: %w", ref, ErrNoNumber)
	}
	if err != nil {
		return 0, fmt.Errorf("resolve %s: %w", ref, err)
	}
	return seq, nil
}

func (s *Store) unambiguous(ctx context.Context, ref string, n int64) error {
	var lane, kind, origin string
	if m := s.window.model.Load(); m != nil {
		if other, ok := m.otherNumbered(ctx, n); ok {
			if other == nil {
				return nil
			}
			return s.ambiguous(ref, n, other.lane, other.kind, other.from)
		}
	}
	err := s.db.QueryRowContext(ctx, `SELECT other.lane, other.kind, other.origin FROM records own, (SELECT DISTINCT origin FROM records) known JOIN records other INDEXED BY records_origin ON other.origin = known.origin AND other.origin_seq = ? WHERE own.seq = ? AND other.drive = own.drive AND other.origin != own.origin LIMIT 1`, n, n).Scan(&lane, &kind, &origin)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("resolve %s: %w", ref, err)
	}
	return s.ambiguous(ref, n, lane, kind, origin)
}

func (s *Store) ambiguous(ref string, n int64, lane, kind, origin string) error {
	other := origin[:min(tagLen, len(origin))]
	return fmt.Errorf("%s names two records in its drive, this node's #%d and %s's %s #%d@%s; pass #%d@%s for this node's or #%d@%s for %s's", ref, n, lane, kind, n, other, n, s.Self()[:min(tagLen, len(s.Self()))], n, other, lane)
}
