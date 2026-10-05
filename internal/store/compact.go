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
	openers, err := s.Query(ctx, Filter{Drive: drive, Kinds: kinds.Openers(), IncludeExpired: true})
	if err != nil {
		return nil, err
	}
	closers, err := s.Query(ctx, Filter{Drive: drive, Kinds: kinds.Closers(), IncludeExpired: true})
	if err != nil {
		return nil, err
	}
	resolved, err := s.Resolvers(ctx, drive)
	if err != nil {
		return nil, err
	}
	var open []Record
	for _, o := range openers {
		if by, ok := resolved[o.Seq]; ok && by > o.Seq {
			continue
		}
		if strings.HasPrefix(o.Source, "import:") {
			continue
		}
		if !closed(o, closers) {
			open = append(open, o)
		}
	}
	return open, nil
}

func closed(opener Record, closers []Record) bool {
	for _, c := range closers {
		if !c.Kind.Closes(opener.Kind) || c.Seq <= opener.Seq {
			continue
		}
		if c.Re == opener.Seq || (c.Topic != "" && c.Topic == opener.Topic) {
			return true
		}
	}
	return false
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
	days := map[string][]Record{}
	for _, r := range old {
		if r.Kind == kinds.Digest || keepSeq[r.Seq] {
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
	} else if _, err := insert(ctx, tx, d, Hash(d), nil); err != nil {
		return err
	}
	seqs := make([]any, len(records))
	for i, r := range records {
		seqs[i] = r.Seq
	}
	for chunk := range slices.Chunk(seqs, 500) {
		if _, err := tx.ExecContext(ctx, "DELETE FROM records WHERE seq IN ("+placeholders(len(chunk))+")", chunk...); err != nil {
			return fmt.Errorf("delete folded records: %w", err)
		}
	}
	return nil
}

func digestText(day string, counts map[string]int) string {
	text := fmt.Sprintf("%s: %d records. kinds: %s. lanes: %s", day, counts["total"], top(counts, "kind:", 8), top(counts, "lane:", 8))
	if r := []rune(text); len(r) > MaxText {
		text = string(r[:MaxText-1]) + "…"
	}
	return text
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
