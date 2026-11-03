package store

import (
	"context"
	"fmt"
	"slices"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

// RefusedPostTopic is the topic of the blocker a refused post leaves behind.
const RefusedPostTopic = "refused-post"

const refusalsRead = 8

// LaneSpawn reads the newest record that launched lane on drive: a spawn
// record naming it, or the state record a launcher posts on topic spawn:<lane>.
func (s *Store) LaneSpawn(ctx context.Context, drive, lane string) (Record, bool, error) {
	return s.newest(ctx, "drive = ? AND ((kind = ? AND topic = ?) OR (kind = ? AND json_extract(refs, '$.lanes[0]') = ?))", drive, string(kinds.State), "spawn:"+lane, string(kinds.Spawn), lane)
}

// NewestOfLane reads the record that says where lane's turn stands: its newest,
// past claims and past the handoff --ccn a rotation or a compaction writes, after
// which the lane keeps working.
func (s *Store) NewestOfLane(ctx context.Context, drive, lane string) (Record, bool, error) {
	return s.newest(ctx, "drive = ? AND lane = ? AND kind NOT IN (?, ?, ?) AND NOT (kind = ? AND json_extract(refs, '$.ccn') IS NOT NULL)", drive, lane, string(kinds.Claim), string(kinds.Peer), string(kinds.Digest), string(kinds.Handoff))
}

func (s *Store) newest(ctx context.Context, where string, args ...any) (Record, bool, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT "+columns+" FROM records WHERE "+where+" ORDER BY seq DESC LIMIT 1", args...)
	if err != nil {
		return Record{}, false, fmt.Errorf("read the newest record: %w", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		return Record{}, false, rows.Err()
	}
	r, err := s.scan(rows)
	return r, err == nil, err
}

// LaneRegistered reports whether lane may post on drive: it has posted before,
// a record launched it, or the drive launches nothing through spawn records. A
// blocker recording this lane's refused post does not count as having posted.
func (s *Store) LaneRegistered(ctx context.Context, drive, lane string) (bool, error) {
	var posted bool
	if err := s.db.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM records WHERE drive = ? AND lane = ? AND topic != ?)", drive, lane, RefusedPostTopic).Scan(&posted); err != nil {
		return false, fmt.Errorf("read the records of %s on %s: %w", lane, drive, err)
	}
	if posted {
		return true, nil
	}
	if _, found, err := s.LaneSpawn(ctx, drive, lane); err != nil || found {
		return found, err
	}
	var launches bool
	if err := s.db.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM records WHERE drive = ? AND (kind = ? OR (kind = ? AND topic LIKE 'spawn:%')))", drive, string(kinds.Spawn), string(kinds.State)).Scan(&launches); err != nil {
		return false, fmt.Errorf("read the spawn records of %s: %w", drive, err)
	}
	return !launches, nil
}

// Escalation names who hears a lane's blocker when no one chose: the lane's
// owner, or root when it has none.
func (s *Store) Escalation(ctx context.Context, drive, lane string) ([]string, error) {
	owner, err := s.LaneOwner(ctx, drive, lane)
	if err != nil {
		return nil, err
	}
	if owner != "" {
		return []string{owner}, nil
	}
	return []string{"root"}, nil
}

// OpenRefusals returns the blockers lane's refused posts left open on drive,
// oldest first.
func (s *Store) OpenRefusals(ctx context.Context, drive, lane string) ([]Record, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT seq FROM records WHERE drive = ? AND lane = ? AND kind = ? AND topic = ? ORDER BY seq DESC LIMIT ?", drive, lane, string(kinds.Blocker), RefusedPostTopic, refusalsRead)
	if err != nil {
		return nil, fmt.Errorf("read the refusals of %s on %s: %w", lane, drive, err)
	}
	defer func() { _ = rows.Close() }()
	var seqs []int64
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return nil, err
		}
		seqs = append(seqs, seq)
	}
	if err := rows.Err(); err != nil || len(seqs) == 0 {
		return nil, err
	}
	return s.OpenAmong(ctx, drive, seqs)
}

// Unescalated returns the first of lane's open refusals when every one of them
// went only to lane, so nobody else has heard of them yet.
func Unescalated(open []Record, lane string) (Record, bool) {
	if len(open) == 0 || slices.ContainsFunc(open, func(r Record) bool { return !slices.Equal(r.To, []string{lane}) }) {
		return Record{}, false
	}
	return open[0], true
}

// Resolving returns r closing open as well as whatever it already closes.
func (r Record) Resolving(open []Record) Record {
	for _, o := range open {
		if r.Resolves == 0 {
			r.Resolves = o.Seq
			continue
		}
		r.AlsoResolves = append(r.AlsoResolves, o.Seq)
	}
	return r
}
