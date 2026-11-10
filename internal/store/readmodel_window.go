package store

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"unsafe"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

const (
	readModelWindowShare = 60
	readModelRefillShare = 85
	readModelRowEstimate = 300
)

var errReadModelBelow = errors.New("the read reaches records older than the read model's window")

type modelWindow struct {
	floor, floorAt int64
	pinned         map[int64]Record
	pinnedDrive    map[int64]string
	pinnedBytes    int64
	wrong          bool
	incidents      map[int64]string
	foreignNumbers map[int64]bool
	replyLanes     map[int64]string
}

func readModelWindowRows(limit int64) int64 {
	return limit * readModelWindowShare / 100 / readModelRowEstimate
}

func (s *Store) readModelWindow(ctx context.Context, rows int64) (modelWindow, error) {
	var newestOut int64
	switch err := s.db.QueryRowContext(ctx, "SELECT seq FROM records ORDER BY seq DESC LIMIT 1 OFFSET ?", rows).Scan(&newestOut); {
	case errors.Is(err, sql.ErrNoRows):
		return modelWindow{}, nil
	case err != nil:
		return modelWindow{}, fmt.Errorf("find the read model's window: %w", err)
	}
	w := modelWindow{floor: newestOut + 1, pinned: map[int64]Record{}, pinnedDrive: map[int64]string{}, incidents: map[int64]string{}, foreignNumbers: map[int64]bool{}, replyLanes: map[int64]string{}}
	if err := s.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(at), 0) FROM records WHERE seq < ?", w.floor).Scan(&w.floorAt); err != nil {
		return modelWindow{}, fmt.Errorf("read the read model's window: %w", err)
	}
	if err := s.scanPairs(ctx, "SELECT seq, drive FROM records WHERE seq < ? AND kind = 'incident'", []any{w.floor}, func(seq int64, drive string) { w.incidents[seq] = drive }); err != nil {
		return modelWindow{}, err
	}
	if err := s.scanPairs(ctx, "SELECT origin_seq, origin FROM records WHERE seq < ? AND origin != ?", []any{w.floor, s.Self()}, func(seq int64, _ string) { w.foreignNumbers[seq] = true }); err != nil {
		return modelWindow{}, err
	}
	if err := s.scanPairs(ctx, "SELECT DISTINCT target.seq, target.lane FROM records reply JOIN records target ON target.seq = reply.re WHERE reply.seq >= ?1 AND reply.re != 0 AND target.seq < ?1", []any{w.floor}, func(seq int64, lane string) { w.replyLanes[seq] = lane }); err != nil {
		return modelWindow{}, err
	}
	var drives []string
	if err := s.scanPairs(ctx, "SELECT 0, drive FROM records WHERE seq < ? AND kind IN ("+placeholders(len(kinds.Openers()))+") GROUP BY drive", append([]any{w.floor}, kindArgs(kinds.Openers())...), func(_ int64, drive string) { drives = append(drives, drive) }); err != nil {
		return modelWindow{}, err
	}
	for _, drive := range drives {
		openers, err := s.closureRowsBefore(ctx, drive, kinds.Openers(), w.floor)
		if err != nil {
			return modelWindow{}, err
		}
		open, err := s.unclosedInStore(ctx, drive, openers)
		if err != nil {
			return modelWindow{}, err
		}
		for _, o := range open {
			o.Refs = Refs{Stacks: o.Refs.Stacks, Targets: o.Refs.Targets}
			w.pinned[o.Seq], w.pinnedDrive[o.Seq] = o, drive
			w.pinnedBytes += pinnedBytes(o)
		}
	}
	if err := s.scanPairs(ctx, "SELECT seq, refs FROM records WHERE seq < ? AND refs != '{}' AND kind IN ("+placeholders(len(kinds.Openers()))+")", append([]any{w.floor}, kindArgs(kinds.Openers())...), func(_ int64, refs string) {
		if parsed := parsedModelRefs(refs); parsed != nil && (parsed.wrong || !fitsRefPool(parsed)) {
			w.wrong = true
		}
	}); err != nil {
		return modelWindow{}, err
	}
	return w, nil
}

func (s *Store) scanPairs(ctx context.Context, query string, args []any, each func(int64, string)) error {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("read the read model's window: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			seq  int64
			name string
		)
		if err := rows.Scan(&seq, &name); err != nil {
			return fmt.Errorf("read the read model's window: %w", err)
		}
		each(seq, name)
	}
	return rows.Err()
}

func (w modelWindow) bytes() int64 {
	return w.pinnedBytes + int64(len(w.incidents)+len(w.foreignNumbers)+len(w.replyLanes))*readModelKeyBytes
}

func pinnedBytes(o Record) int64 {
	n := int64(unsafe.Sizeof(o)) + 2*readModelKeyBytes + int64(len(o.Lane)+len(o.Topic)+len(o.Source)+8*len(o.AlsoResolves))
	for _, ref := range slices.Concat(o.Refs.Stacks, o.Refs.Targets) {
		n += int64(len(ref)) + 16
	}
	return n
}

func (w modelWindow) holdsFrom(f Filter, since int64) bool {
	return w.floor == 0 || f.After+1 >= w.floor || !f.Since.IsZero() && since > w.floorAt
}

func (w modelWindow) pinnedIn(drive string) []Record {
	var out []Record
	for seq, held := range w.pinnedDrive {
		if held == drive {
			out = append(out, w.pinned[seq])
		}
	}
	slices.SortFunc(out, func(a, b Record) int { return cmp.Compare(a.Seq, b.Seq) })
	return out
}

func (w modelWindow) sameOpeners(ks []kinds.Kind) bool {
	return len(ks) == len(modelOpeners) && !slices.ContainsFunc(ks, func(k kinds.Kind) bool { return !modelOpeners[string(k)] })
}

func (w modelWindow) heldOpeners(drive string, openers []Record) ([]Record, error) {
	if w.floor == 0 {
		return openers, nil
	}
	kept := make([]Record, 0, len(openers))
	for _, o := range openers {
		switch {
		case o.Seq >= w.floor:
			if o.At.UnixMilli() <= w.floorAt && len(o.Refs.Stacks)+len(o.Refs.Targets) > 0 {
				return nil, errReadModelBelow
			}
			kept = append(kept, o)
		case w.pinnedDrive[o.Seq] == drive:
			kept = append(kept, o)
		case o.Drive != drive || !modelOpeners[string(o.Kind)]:
			return nil, errReadModelBelow
		}
	}
	return kept, nil
}

func (m *ReadModel) nameReplyTargets(ctx context.Context, snapshot *sql.Tx) error {
	if len(m.unnamed) == 0 {
		return nil
	}
	wanted, err := json.Marshal(m.unnamed)
	if err != nil {
		return err
	}
	rows, err := snapshot.QueryContext(ctx, "SELECT seq, lane FROM records WHERE seq IN (SELECT value FROM json_each(?))", string(wanted))
	if err != nil {
		return fmt.Errorf("read the lanes replies name: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			seq  int64
			lane string
		)
		if err := rows.Scan(&seq, &lane); err != nil {
			return fmt.Errorf("read the lanes replies name: %w", err)
		}
		if _, held := m.window.replyLanes[seq]; !held {
			m.window.replyLanes[seq] = lane
			m.bytes += readModelKeyBytes
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read the lanes replies name: %w", err)
	}
	m.unnamed = m.unnamed[:0]
	return nil
}
