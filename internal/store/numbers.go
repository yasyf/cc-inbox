package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
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

// OwnerNode reports whether this node holds the owner tier. A node that does
// not reads a drive's lane registry as far as its relay copy has reached.
func (s *Store) OwnerNode() bool {
	return s.owner
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

func number(owners map[string]bool, shared map[int64]bool, origin string, seq int64) string {
	if owners[origin] && !shared[seq] {
		return fmt.Sprintf("#%d", seq)
	}
	return fmt.Sprintf("#%d@%s", seq, origin[:min(tagLen, len(origin))])
}

// numbered names each record, and the records it answers and resolves, the way
// every node names them: the origin node's number, bare when an owner node
// wrote it and no other node's record carries that number, else followed by @
// and the origin node's first eight characters.
func (s *Store) numbered(ctx context.Context, records []Record) error {
	if len(records) == 0 {
		return nil
	}
	var targets []int64
	for _, r := range records {
		targets = append(targets, r.Re, r.Resolves)
		targets = append(targets, r.AlsoResolves...)
	}
	owners, shared, names, err := s.naming(ctx, records, targets)
	if err != nil {
		return err
	}
	for i := range records {
		r := &records[i]
		r.Num, r.ReNum, r.CloseNums = s.name(owners, shared, *r), names[r.Re], nil
		for _, seq := range r.Closes() {
			r.CloseNums = append(r.CloseNums, names[seq])
		}
	}
	return nil
}

func (s *Store) naming(ctx context.Context, records []Record, targets []int64) (map[string]bool, map[int64]bool, map[int64]string, error) {
	if m := s.reads.model.Load(); m != nil {
		if owners, shared, names, ok := m.namesOf(ctx, s, records, targets); ok {
			return owners, shared, names, nil
		}
	}
	owners, err := s.owners(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	shared, names, err := s.names(ctx, s.db, owners, records, targets)
	return owners, shared, names, err
}

func (s *Store) numberOf(r Record) (string, int64) {
	if r.Origin == "" {
		return s.Self(), r.Seq
	}
	return r.Origin, r.OriginSeq
}

func (s *Store) name(owners map[string]bool, shared map[int64]bool, r Record) string {
	origin, seq := s.numberOf(r)
	return number(owners, shared, origin, seq)
}

func (s *Store) bare(owners map[string]bool, records []Record) []int64 {
	var out []int64
	for _, r := range records {
		if origin, seq := s.numberOf(r); owners[origin] {
			out = append(out, seq)
		}
	}
	return out
}

func (s *Store) names(ctx context.Context, q rowsQuerier, owners map[string]bool, records []Record, seqs []int64) (map[int64]bool, map[int64]string, error) {
	var wanted []int64
	for _, seq := range seqs {
		if seq != 0 {
			wanted = append(wanted, seq)
		}
	}
	var targets []Record
	if len(wanted) > 0 {
		ids, _ := json.Marshal(wanted)
		rows, err := q.QueryContext(ctx, "SELECT seq, origin, origin_seq FROM records WHERE seq IN (SELECT value FROM json_each(?))", string(ids))
		if err != nil {
			return nil, nil, fmt.Errorf("name targets: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var (
				r      Record
				origin string
			)
			if err := rows.Scan(&r.Seq, &origin, &r.OriginSeq); err != nil {
				return nil, nil, fmt.Errorf("scan target: %w", err)
			}
			r.Origin = s.local(origin)
			targets = append(targets, r)
		}
		if err := rows.Err(); err != nil {
			return nil, nil, err
		}
	}
	shared, err := sharedNumbers(ctx, q, append(s.bare(owners, records), s.bare(owners, targets)...))
	if err != nil {
		return nil, nil, err
	}
	out := make(map[int64]string, len(targets))
	for _, r := range targets {
		out[r.Seq] = s.name(owners, shared, r)
	}
	return shared, out, nil
}

func sharedNumbers(ctx context.Context, q rowsQuerier, seqs []int64) (map[int64]bool, error) {
	out := map[int64]bool{}
	if len(seqs) == 0 {
		return out, nil
	}
	ids, _ := json.Marshal(seqs)
	rows, err := q.QueryContext(ctx, `SELECT r.origin_seq FROM (SELECT DISTINCT value FROM json_each(?)) n, (SELECT DISTINCT origin FROM records) known JOIN records r INDEXED BY records_origin ON r.origin = known.origin AND r.origin_seq = n.value GROUP BY r.origin_seq HAVING COUNT(*) > 1`, string(ids))
	if err != nil {
		return nil, fmt.Errorf("find shared numbers: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var n int64
		if err := rows.Scan(&n); err != nil {
			return nil, fmt.Errorf("scan shared number: %w", err)
		}
		out[n] = true
	}
	return out, rows.Err()
}

// Number names r the way every node names it.
func (s *Store) Number(ctx context.Context, r Record) (string, error) {
	owners, err := s.owners(ctx)
	if err != nil {
		return "", err
	}
	shared, _, err := s.names(ctx, s.db, owners, []Record{r}, nil)
	if err != nil {
		return "", err
	}
	return s.name(owners, shared, r), nil
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
// this store's seq for that record. A bare #N names the record any node
// numbered N. When several nodes' records carry N, it picks the one record
// among them addressed to reader and says so in picked, and refuses otherwise,
// naming each as #N@node.
func (s *Store) Resolve(ctx context.Context, ref, reader string) (seq int64, picked string, err error) {
	num, err := ParseNumber(ref)
	if err != nil {
		return 0, "", err
	}
	if num.Tag == "" {
		return s.resolveBare(ctx, ref, num.Seq, reader)
	}
	if strings.HasPrefix(s.Self(), num.Tag) {
		return num.Seq, "", nil
	}
	err = s.db.QueryRowContext(ctx, "SELECT seq FROM records WHERE origin LIKE ? AND origin_seq = ? UNION ALL SELECT seq FROM aliases WHERE origin LIKE ? AND origin_seq = ? LIMIT 1", num.Tag+"%", num.Seq, num.Tag+"%", num.Seq).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, "", fmt.Errorf("%s: %w", ref, ErrNoNumber)
	}
	if err != nil {
		return 0, "", fmt.Errorf("resolve %s: %w", ref, err)
	}
	return seq, "", nil
}

func (s *Store) resolveBare(ctx context.Context, ref string, n int64, reader string) (int64, string, error) {
	if n == 0 {
		return 0, "", nil
	}
	rows, err := s.db.QueryContext(ctx, "SELECT "+columns+" FROM records WHERE seq IN (SELECT r.seq FROM (SELECT DISTINCT origin FROM records) known JOIN records r INDEXED BY records_origin ON r.origin = known.origin AND r.origin_seq = ? UNION SELECT a.seq FROM (SELECT DISTINCT origin FROM aliases) aliased JOIN aliases a ON a.origin = aliased.origin AND a.origin_seq = ?) ORDER BY seq", n, n)
	if err != nil {
		return 0, "", fmt.Errorf("resolve %s: %w", ref, err)
	}
	defer func() { _ = rows.Close() }()
	var candidates []Record
	for rows.Next() {
		r, err := s.scan(rows)
		if err != nil {
			return 0, "", fmt.Errorf("resolve %s: %w", ref, err)
		}
		candidates = append(candidates, r)
	}
	if err := rows.Err(); err != nil {
		return 0, "", fmt.Errorf("resolve %s: %w", ref, err)
	}
	switch len(candidates) {
	case 0:
		return 0, "", fmt.Errorf("%s: %w", ref, ErrNoNumber)
	case 1:
		return candidates[0].Seq, "", nil
	}
	if err := s.numbered(ctx, candidates); err != nil {
		return 0, "", err
	}
	listed := make([]string, len(candidates))
	for i, r := range candidates {
		listed[i] = fmt.Sprintf("%s %s from %s: %s", r.Name(), strings.ToUpper(string(r.Kind)), r.Lane, firstWords(r.Text))
	}
	addressed := slices.DeleteFunc(slices.Clone(candidates), func(r Record) bool {
		return reader == "" || !slices.ContainsFunc(r.To, func(to string) bool {
			lane, _, _ := strings.Cut(to, "@")
			return SameLane(lane, reader)
		})
	})
	if len(addressed) == 1 {
		return addressed[0].Seq, fmt.Sprintf("%s names %d records; picked %s, the one addressed to %s. All of them:\n  %s", ref, len(candidates), addressed[0].Name(), reader, strings.Join(listed, "\n  ")), nil
	}
	return 0, "", fmt.Errorf("%s names %d records, one per node; pass one of these numbers:\n  %s", ref, len(candidates), strings.Join(listed, "\n  "))
}

func firstWords(text string) string {
	words := strings.Fields(text)
	if len(words) > 8 {
		return strings.Join(words[:8], " ") + " ..."
	}
	return strings.Join(words, " ")
}
