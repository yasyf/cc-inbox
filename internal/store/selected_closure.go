package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
)

const selectedClosersSQL = `SELECT ` + columns + ` FROM records WHERE seq IN (
SELECT k.source_seq FROM json_each(?) j CROSS JOIN record_closer_keys k INDEXED BY record_closer_keys_lookup
WHERE k.drive = ? AND k.axis = json_extract(j.value, '$[0]') AND k.key = json_extract(j.value, '$[1]')
) ORDER BY seq`

const selectedResolversSQL = `WITH wanted(seq) AS MATERIALIZED (SELECT value FROM json_each(?)),
candidates(seq) AS MATERIALIZED (
SELECT seq FROM records WHERE resolves != 0 AND resolves IN (SELECT seq FROM wanted)
UNION SELECT source_seq FROM record_also_resolves INDEXED BY record_also_resolves_target WHERE target IN (SELECT seq FROM wanted)
)
SELECT resolves, seq FROM (
SELECT r.resolves, r.seq FROM candidates c JOIN records r ON r.seq = c.seq
WHERE r.resolves != 0 AND r.drive = ? ORDER BY r.resolves, r.seq
)
UNION ALL SELECT resolves, seq FROM (
SELECT r.resolves, r.seq FROM candidates c JOIN records r ON r.seq = c.seq
WHERE r.resolves != 0 AND r.drive != ?
AND EXISTS (SELECT 1 FROM records target WHERE target.seq = r.resolves AND target.drive = ?)
ORDER BY r.resolves, r.drive, r.seq
)
UNION ALL SELECT j.value, r.seq FROM candidates c JOIN records r ON r.seq = c.seq, json_each(r.also_resolves) j
WHERE r.drive = ? OR EXISTS (SELECT 1 FROM records target WHERE target.seq = j.value AND target.drive = ?)`

func (s *Store) unclosedAmong(ctx context.Context, drive string, openers []Record) ([]Record, error) {
	if len(openers) == 0 {
		return nil, nil
	}
	closers, err := s.closersAmong(ctx, drive, openers)
	if err != nil {
		return nil, err
	}
	seqs := make([]int64, len(openers))
	for i, o := range openers {
		seqs[i] = o.Seq
	}
	resolved, err := s.resolversAmong(ctx, drive, seqs)
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

func (s *Store) closersAmong(ctx context.Context, drive string, openers []Record) ([]Record, error) {
	var keys [][2]string
	seen := map[[2]string]bool{}
	add := func(axis, key string) {
		pair := [2]string{axis, key}
		if !seen[pair] {
			seen[pair] = true
			keys = append(keys, pair)
		}
	}
	for _, o := range openers {
		add("re", strconv.FormatInt(o.Seq, 10))
		if o.Topic != "" {
			add("topic", fmt.Sprintf("%X", []byte(o.Topic)))
		}
		for _, key := range o.Refs.Stacks {
			add("ref", key)
		}
		for _, key := range o.Refs.Targets {
			add("ref", key)
		}
		if len(o.Refs.Stacks) != 0 || len(o.Refs.Targets) != 0 {
			add("ref-ambiguous", "")
		}
	}
	ids, err := json.Marshal(keys)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, selectedClosersSQL, string(ids), drive)
	if err != nil {
		return nil, fmt.Errorf("read selected closers: %w", err)
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, s.numbered(ctx, out)
}

func (s *Store) resolversAmong(ctx context.Context, drive string, seqs []int64) (map[int64]int64, error) {
	ids, err := json.Marshal(seqs)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, selectedResolversSQL, string(ids), drive, drive, drive, drive, drive)
	if err != nil {
		return nil, fmt.Errorf("read resolvers: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[int64]int64{}
	for rows.Next() {
		var target, seq int64
		if err := rows.Scan(&target, &seq); err != nil {
			return nil, fmt.Errorf("scan resolver: %w", err)
		}
		out[target] = max(out[target], seq)
	}
	return out, rows.Err()
}
