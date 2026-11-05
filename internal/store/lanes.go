package store

import (
	"context"
	"fmt"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

// RefusedPostTopic is the topic of the blocker a refused post leaves behind.
const RefusedPostTopic = "refused-post"

// LaneSpawn reads the newest record that launched lane on drive: a spawn
// record naming it, or the state record a launcher posts on topic spawn:<lane>.
func (s *Store) LaneSpawn(ctx context.Context, drive, lane string) (Record, bool, error) {
	return s.newest(ctx, "drive = ? AND ((kind = ? AND topic = ?) OR (kind = ? AND json_extract(refs, '$.lanes[0]') = ?))", drive, string(kinds.State), "spawn:"+lane, string(kinds.Spawn), lane)
}

// BinderAttr names, by its peer key, the thread that posted a record as the
// lane it holds, so another node reads that lane as held.
const BinderAttr = "binder"

// RemoteBinder reads the newest record that a thread on another node posted on
// drive as the lane it holds, at or after since.
func (s *Store) RemoteBinder(ctx context.Context, drive, lane string, since time.Time) (Record, bool, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT "+columns+` FROM records WHERE drive = ? AND lane = ? AND kind != 'digest' AND at * 1000000 + seq >= ?
AND origin NOT IN ('', ?) AND json_extract(fields, '$.attrs.`+BinderAttr+`') IS NOT NULL ORDER BY at * 1000000 + seq DESC LIMIT 1`, drive, lane, since.UnixMilli()*1000000, s.Self())
	if err != nil {
		return Record{}, false, fmt.Errorf("read the binder of %s on %s: %w", lane, drive, err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		return Record{}, false, rows.Err()
	}
	r, err := s.scan(rows)
	return r, err == nil, err
}

// NewestOfLane reads the record that says where lane's turn stands: its newest,
// past claims and past the handoff --ccn a rotation or a compaction writes, after
// which the lane keeps working.
func (s *Store) NewestOfLane(ctx context.Context, drive, lane string) (Record, bool, error) {
	return s.newest(ctx, "drive = ? AND lane = ? AND kind NOT IN (?, ?, ?) AND NOT (kind = ? AND json_extract(refs, '$.ccn') IS NOT NULL)", drive, lane, string(kinds.Claim), string(kinds.Peer), string(kinds.Digest), string(kinds.Handoff))
}

// OpenPR reports whether lane opened a PR on drive that it has not posted
// landed for since.
func (s *Store) OpenPR(ctx context.Context, drive, lane string) (bool, error) {
	var open bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM records o, json_each(o.refs, '$.prs') p WHERE o.drive = ? AND o.lane = ? AND o.kind = ?
AND NOT EXISTS (SELECT 1 FROM records l, json_each(l.refs, '$.prs') q WHERE l.drive = o.drive AND l.lane = o.lane AND l.kind = ? AND l.seq > o.seq AND q.value = p.value))`,
		drive, lane, string(kinds.Opened), string(kinds.Landed)).Scan(&open)
	if err != nil {
		return false, fmt.Errorf("read the open PRs of %s on %s: %w", lane, drive, err)
	}
	return open, nil
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
