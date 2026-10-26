package store

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func (s *Store) OpenItems(ctx context.Context, drive string) ([]Record, error) {
	return s.open(ctx, drive, Record.Tracked)
}

// OpenTo returns, in seq order, the tracked openers of ks addressed to reader
// that no closer or resolver has closed.
func (s *Store) OpenTo(ctx context.Context, drive, reader string, ks []kinds.Kind) ([]Record, error) {
	openers, err := s.Query(ctx, Filter{Drive: drive, To: reader, Kinds: ks, IncludeExpired: true})
	if err != nil {
		return nil, err
	}
	return s.unclosed(ctx, drive, slices.DeleteFunc(openers, func(o Record) bool { return !o.Tracked() || !slices.Contains(ks, o.Kind) }))
}

func (s *Store) UntrackedHolds(ctx context.Context, drive string) ([]Record, error) {
	return s.open(ctx, drive, func(r Record) bool { return r.Kind == kinds.Hold && !r.Tracked() })
}

// OpenAmong returns, in seq order, the tracked openers among seqs that sit in
// drive and that no closer or resolver has closed. It rereads those openers
// and the drive's closers and resolvers, so a seq that a reparse took out of
// the opener kinds or out of tracking, or that compaction folded, is not
// returned. seqs travels as one JSON array parameter, so its length is not
// bounded by SQLite's limit on bound variables.
func (s *Store) OpenAmong(ctx context.Context, drive string, seqs []int64) ([]Record, error) {
	openers, err := s.openersAmong(ctx, drive, seqs)
	if err != nil {
		return nil, err
	}
	return s.unclosed(ctx, drive, slices.DeleteFunc(openers, func(o Record) bool { return !o.Tracked() }))
}

func (s *Store) openersAmong(ctx context.Context, drive string, seqs []int64) ([]Record, error) {
	openers := kinds.Openers()
	ids, _ := json.Marshal(seqs)
	params := make([]any, 0, 2+len(openers))
	params = append(params, drive, string(ids))
	for _, k := range openers {
		params = append(params, string(k))
	}
	rows, err := s.db.QueryContext(ctx, "SELECT "+columns+" FROM records WHERE drive = ? AND seq IN (SELECT value FROM json_each(?)) AND kind IN ("+placeholders(len(openers))+") ORDER BY seq", params...)
	if err != nil {
		return nil, fmt.Errorf("read openers: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Record
	for rows.Next() {
		r, err := s.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) unclosed(ctx context.Context, drive string, openers []Record) ([]Record, error) {
	closers, err := s.Query(ctx, Filter{Drive: drive, Kinds: kinds.Closers(), IncludeExpired: true})
	if err != nil {
		return nil, err
	}
	resolved, err := s.Resolvers(ctx, drive)
	if err != nil {
		return nil, err
	}
	ix := indexClosers(closers)
	var open []Record
	for _, o := range openers {
		if by, ok := resolved[o.Seq]; ok && by > o.Seq {
			continue
		}
		if !ix.closed(o) {
			open = append(open, o)
		}
	}
	return open, nil
}

func (s *Store) open(ctx context.Context, drive string, keep func(Record) bool) ([]Record, error) {
	openers, err := s.Query(ctx, Filter{Drive: drive, Kinds: kinds.Openers(), IncludeExpired: true})
	if err != nil {
		return nil, err
	}
	return s.unclosed(ctx, drive, slices.DeleteFunc(openers, func(o Record) bool { return !keep(o) }))
}

type closerIndex struct {
	byRe              seqBuckets[int64]
	byTopic           seqBuckets[string]
	byStack           stampBuckets
	byTarget          stampBuckets
	byUnstackedTarget stampBuckets
}

func indexClosers(closers []Record) closerIndex {
	ix := closerIndex{
		byRe:              seqBuckets[int64]{},
		byTopic:           seqBuckets[string]{},
		byStack:           stampBuckets{},
		byTarget:          stampBuckets{},
		byUnstackedTarget: stampBuckets{},
	}
	for _, c := range closers {
		ix.byRe.add(c.Re, c)
		if c.Topic != "" {
			ix.byTopic.add(c.Topic, c)
		}
		for _, stack := range c.Refs.Stacks {
			ix.byStack.add(stack, c)
		}
		for _, target := range c.Refs.Targets {
			ix.byTarget.add(target, c)
			if len(c.Refs.Stacks) == 0 {
				ix.byUnstackedTarget.add(target, c)
			}
		}
	}
	return ix
}

func (ix closerIndex) closed(opener Record) bool {
	if ix.byRe.closes(opener.Seq, opener) || ix.byTopic.closes(opener.Topic, opener) {
		return true
	}
	byTarget := ix.byTarget
	if len(opener.Refs.Stacks) > 0 {
		if ix.byStack.closes(opener.Refs.Stacks, opener) {
			return true
		}
		byTarget = ix.byUnstackedTarget
	}
	return byTarget.closes(opener.Refs.Targets, opener)
}

type seqBuckets[K comparable] map[K]map[kinds.Kind]int64

func (b seqBuckets[K]) add(key K, c Record) {
	latest, ok := b[key]
	if !ok {
		latest = map[kinds.Kind]int64{}
		b[key] = latest
	}
	if seq, ok := latest[c.Kind]; !ok || c.Seq > seq {
		latest[c.Kind] = c.Seq
	}
}

func (b seqBuckets[K]) closes(key K, opener Record) bool {
	for kind, seq := range b[key] {
		if seq > opener.Seq && kind.Closes(opener.Kind) {
			return true
		}
	}
	return false
}

type stamp struct {
	at  time.Time
	seq int64
}

func (a stamp) after(b stamp) bool {
	return a.at.After(b.at) || a.at.Equal(b.at) && a.seq > b.seq
}

type stampBuckets map[string]map[kinds.Kind]stamp

func (b stampBuckets) add(key string, c Record) {
	latest, ok := b[key]
	if !ok {
		latest = map[kinds.Kind]stamp{}
		b[key] = latest
	}
	at := stamp{c.At, c.Seq}
	if cur, ok := latest[c.Kind]; !ok || at.after(cur) {
		latest[c.Kind] = at
	}
}

func (b stampBuckets) closes(keys []string, opener Record) bool {
	since := stamp{opener.At, opener.Seq}
	for _, key := range keys {
		for kind, at := range b[key] {
			if at.after(since) && pairsByRef(kind, opener) {
				return true
			}
		}
	}
	return false
}

func pairsByRef(kind kinds.Kind, opener Record) bool {
	return kind.Closes(opener.Kind) || kind.ClosesByRef() && strings.HasPrefix(opener.Source, "import:")
}

type CompactResult struct {
	Folded  int `json:"folded"`
	Digests int `json:"digests"`
}

func (s *Store) Compact(ctx context.Context, drive string, keep time.Duration) (CompactResult, error) {
	cutoff := s.Now().Add(-keep)
	open, err := s.OpenItems(ctx, drive)
	if err != nil {
		return CompactResult{}, err
	}
	keepSeq := map[int64]bool{}
	for _, o := range open {
		keepSeq[o.Seq] = true
	}
	old, err := s.Query(ctx, Filter{Drive: drive, Until: cutoff, IncludeExpired: true})
	if err != nil {
		return CompactResult{}, err
	}
	floor, held, err := s.compactionFloor(ctx)
	if err != nil {
		return CompactResult{}, err
	}
	elsewhere, err := s.foreignResolvers(ctx, drive)
	if err != nil {
		return CompactResult{}, err
	}
	for _, r := range old {
		if held && r.Origin == "" && r.Seq > floor {
			keepSeq[r.Seq] = true
		}
	}
	days := map[string][]Record{}
	for _, r := range old {
		if r.Kind == kinds.Digest || keepSeq[r.Seq] || elsewhere[r.Seq] || keepSeq[r.Re] || slices.ContainsFunc(r.Closes(), func(seq int64) bool { return keepSeq[seq] }) {
			continue
		}
		day := r.At.In(time.Local).Format(time.DateOnly)
		days[day] = append(days[day], r)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CompactResult{}, fmt.Errorf("begin compact: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var res CompactResult
	for _, day := range slices.Sorted(maps.Keys(days)) {
		if err := s.fold(ctx, tx, drive, day, days[day]); err != nil {
			return CompactResult{}, err
		}
		res.Folded += len(days[day])
		res.Digests++
	}
	if err := tx.Commit(); err != nil {
		return CompactResult{}, fmt.Errorf("commit compact: %w", err)
	}
	return res, nil
}

func (s *Store) foreignResolvers(ctx context.Context, drive string) (map[int64]bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT r.seq FROM records r JOIN records t ON t.seq = r.resolves WHERE r.drive = ? AND t.drive != ?
UNION SELECT r.seq FROM records r, json_each(r.also_resolves) j JOIN records t ON t.seq = j.value WHERE r.drive = ? AND t.drive != ?`, drive, drive, drive, drive)
	if err != nil {
		return nil, fmt.Errorf("read cross-drive resolvers: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[int64]bool{}
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return nil, fmt.Errorf("scan cross-drive resolver: %w", err)
		}
		out[seq] = true
	}
	return out, rows.Err()
}

func (s *Store) fold(ctx context.Context, tx *sql.Tx, drive, day string, records []Record) error {
	topic := "digest:" + day
	counts := map[string]int{}
	var existing int64
	var raw string
	err := tx.QueryRowContext(ctx, "SELECT seq, fields FROM records WHERE drive = ? AND kind = ? AND topic = ?", drive, string(kinds.Digest), topic).Scan(&existing, &raw)
	switch {
	case err == nil:
		var f Fields
		if err := json.Unmarshal([]byte(raw), &f); err != nil {
			return fmt.Errorf("decode digest %s: %w", topic, err)
		}
		maps.Copy(counts, f.Counts)
	case !errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("read digest %s: %w", topic, err)
	}
	last := records[len(records)-1].At
	for _, r := range records {
		counts["kind:"+string(r.Kind)]++
		counts["lane:"+r.Lane]++
		counts["total"]++
	}
	d := Record{
		Drive:  drive,
		Lane:   "cci",
		Kind:   kinds.Digest,
		At:     last,
		Text:   digestText(day, counts),
		Topic:  topic,
		Fields: Fields{Counts: counts},
		Source: "compact",
	}
	if existing != 0 {
		fields, _ := json.Marshal(d.Fields)
		if _, err := tx.ExecContext(ctx, "UPDATE records SET text = ?, fields = ?, at = MAX(at, ?) WHERE seq = ?", d.Text, string(fields), last.UnixMilli(), existing); err != nil {
			return fmt.Errorf("update digest %s: %w", topic, err)
		}
	} else if _, err := s.insert(ctx, tx, d, Hash(d), nil); err != nil {
		return err
	}
	seqs := make([]any, len(records))
	var through int64
	for i, r := range records {
		seqs[i] = r.Seq
		if r.Origin == "" {
			through = max(through, r.Seq)
			continue
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO folded (origin, origin_seq) VALUES (?, ?) ON CONFLICT DO NOTHING", r.Origin, r.OriginSeq); err != nil {
			return fmt.Errorf("record folded %s/%d: %w", r.Origin, r.OriginSeq, err)
		}
	}
	if through > 0 {
		if _, err := tx.ExecContext(ctx, `INSERT INTO meta (key, value) VALUES ('compacted_through', ?)
ON CONFLICT (key) DO UPDATE SET value = MAX(value, excluded.value)`, through); err != nil {
			return fmt.Errorf("record compacted-through: %w", err)
		}
	}
	for chunk := range slices.Chunk(seqs, 500) {
		if _, err := tx.ExecContext(ctx, "DELETE FROM records WHERE seq IN ("+placeholders(len(chunk))+")", chunk...); err != nil {
			return fmt.Errorf("delete folded records: %w", err)
		}
	}
	return nil
}

func digestText(day string, counts map[string]int) string {
	return Clip(fmt.Sprintf("%s: %d records. kinds: %s. lanes: %s", day, counts["total"], top(counts, "kind:", 8), top(counts, "lane:", 8)))
}

func top(counts map[string]int, prefix string, n int) string {
	type kv struct {
		k string
		v int
	}
	var all []kv
	for k, v := range counts {
		if name, ok := strings.CutPrefix(k, prefix); ok {
			all = append(all, kv{name, v})
		}
	}
	slices.SortFunc(all, func(a, b kv) int {
		return cmp.Or(cmp.Compare(b.v, a.v), cmp.Compare(a.k, b.k))
	})
	parts := make([]string, 0, n)
	for i, e := range all {
		if i == n {
			parts = append(parts, fmt.Sprintf("+%d more", len(all)-n))
			break
		}
		parts = append(parts, fmt.Sprintf("%s %d", e.k, e.v))
	}
	return strings.Join(parts, ", ")
}
