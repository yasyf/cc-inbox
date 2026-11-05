package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/node"
)

type BoardSnapshot struct {
	store *Store
	tx    *sql.Tx
	now   time.Time
}

func (s *BoardSnapshot) owners(ctx context.Context) (map[string]bool, error) {
	rows, err := s.tx.QueryContext(ctx, "SELECT node FROM members WHERE tier = ?", string(node.TierOwner))
	if err != nil {
		return nil, fmt.Errorf("read owner members: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]bool{s.store.Self(): s.store.owner}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, fmt.Errorf("scan owner member: %w", err)
		}
		out[n] = true
	}
	return out, rows.Err()
}

func (s *BoardSnapshot) numbered(ctx context.Context, records []Record) error {
	if len(records) == 0 {
		return nil
	}
	owners, err := s.owners(ctx)
	if err != nil {
		return err
	}
	var targets []int64
	for _, r := range records {
		targets = append(targets, r.Re, r.Resolves)
		targets = append(targets, r.AlsoResolves...)
	}
	names, err := s.names(ctx, owners, targets)
	if err != nil {
		return err
	}
	for i := range records {
		r := &records[i]
		r.Num, r.ReNum, r.CloseNums = s.store.name(owners, *r), names[r.Re], nil
		for _, seq := range r.Closes() {
			r.CloseNums = append(r.CloseNums, names[seq])
		}
	}
	return nil
}

func (s *BoardSnapshot) names(ctx context.Context, owners map[string]bool, seqs []int64) (map[int64]string, error) {
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
	rows, err := s.tx.QueryContext(ctx, "SELECT seq, origin, origin_seq FROM records WHERE seq IN (SELECT value FROM json_each(?))", string(ids))
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
		r.Origin = s.store.local(origin)
		out[r.Seq] = s.store.name(owners, r)
	}
	return out, rows.Err()
}

func (s *BoardSnapshot) OpenAmong(ctx context.Context, drive string, seqs []int64) ([]Record, error) {
	openers, err := s.openersAmong(ctx, drive, seqs)
	if err != nil {
		return nil, err
	}
	return s.unclosedAmong(ctx, drive, slices.DeleteFunc(openers, func(o Record) bool { return !o.Tracked() }))
}

func (s *BoardSnapshot) openersAmong(ctx context.Context, drive string, seqs []int64) ([]Record, error) {
	openers := kinds.Openers()
	ids, _ := json.Marshal(seqs)
	params := make([]any, 0, 2+len(openers))
	params = append(params, drive, string(ids))
	for _, k := range openers {
		params = append(params, string(k))
	}
	rows, err := s.tx.QueryContext(ctx, "SELECT "+columns+" FROM records WHERE drive = ? AND seq IN (SELECT value FROM json_each(?)) AND kind IN ("+placeholders(len(openers))+") ORDER BY seq", params...)
	if err != nil {
		return nil, fmt.Errorf("read openers: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Record
	for rows.Next() {
		r, err := s.store.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *BoardSnapshot) withoutIncidentUpdates(ctx context.Context, drive string, openers []Record) ([]Record, error) {
	var targets []int64
	for _, o := range openers {
		if o.Kind == kinds.Incident {
			targets = append(targets, incidentTargets(o)...)
		}
	}
	if len(targets) == 0 {
		return openers, nil
	}
	named, err := s.openersAmong(ctx, drive, targets)
	if err != nil {
		return nil, err
	}
	incidents := map[int64]bool{}
	for _, r := range named {
		incidents[r.Seq] = r.Kind == kinds.Incident
	}
	return slices.DeleteFunc(openers, func(o Record) bool {
		return o.Kind == kinds.Incident && slices.ContainsFunc(incidentTargets(o), func(seq int64) bool { return incidents[seq] })
	}), nil
}

func (s *BoardSnapshot) unclosedAmong(ctx context.Context, drive string, openers []Record) ([]Record, error) {
	openers, err := s.withoutIncidentUpdates(ctx, drive, openers)
	if err != nil || len(openers) == 0 {
		return nil, err
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
	return s.store.heardAsks(ctx, s.tx, drive, open)
}

func (s *BoardSnapshot) closersAmong(ctx context.Context, drive string, openers []Record) ([]Record, error) {
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
	rows, err := s.tx.QueryContext(ctx, selectedClosersSQL, string(ids), drive)
	if err != nil {
		return nil, fmt.Errorf("read selected closers: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Record
	for rows.Next() {
		r, err := s.store.scan(rows)
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

func (s *BoardSnapshot) resolversAmong(ctx context.Context, drive string, seqs []int64) (map[int64]int64, error) {
	ids, err := json.Marshal(seqs)
	if err != nil {
		return nil, err
	}
	rows, err := s.tx.QueryContext(ctx, selectedResolversSQL, string(ids), drive, drive, drive, drive, drive)
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

func (s *BoardSnapshot) Records(ctx context.Context, seqs []int64) ([]Record, error) {
	ids, err := json.Marshal(seqs)
	if err != nil {
		return nil, err
	}
	rows, err := s.tx.QueryContext(ctx, "SELECT "+columns+" FROM records WHERE seq IN (SELECT value FROM json_each(?)) AND (expires_at IS NULL OR expires_at > ?) ORDER BY seq", string(ids), s.now.UnixMilli())
	if err != nil {
		return nil, err
	}
	var out []Record
	for rows.Next() {
		record, scanErr := s.store.scan(rows)
		if scanErr != nil {
			_ = rows.Close()
			return nil, scanErr
		}
		out = append(out, record)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, s.numbered(ctx, out)
}
