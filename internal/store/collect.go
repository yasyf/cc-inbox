package store

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

const (
	// HotWindow is how long a record that nothing open depends on stays in the store.
	HotWindow = 14 * 24 * time.Hour

	expiredGrace     = 72 * time.Hour
	sessionWindow    = 30 * 24 * time.Hour
	pinnedWindow     = 30 * 24 * time.Hour
	archiveWindow    = 180 * 24 * time.Hour
	retiredWindow    = 90 * 24 * time.Hour
	blobGrace        = time.Hour
	collectBatch     = 1000
	vacuumFreeRatio  = 10
	vacuumPages      = 2048
	archiveNameBytes = 120
	archiveDir       = "archive"
	blobDir          = "blobs"
	// UnsentDir holds the writes a post could not confirm until they replay.
	UnsentDir = "unsent"
)

// Collected reports one Collect pass.
type Collected struct {
	Archived int
	Sessions int
	Claims   int
	Skipped  []string
	Vacuumed bool
	Freed    int
	// Checkpointed is false when a reader held the log open; the next pass retries.
	Checkpointed bool
}

// Collect deletes the records past their retention, newest first so a reply goes
// before its parent, after publishing each to a complete archive segment, then
// returns the freed pages to the file system. Records that name each other go or
// stay together, so a kept record never loses its parent, its closer or its
// replies. A record stays while it is open, while an opener sharing its topic,
// stack or target stays, while a peer or a buffered row needs it, and while it is
// the answer to a lane's newest record, newest spawn or unlanded PR. The newest
// state or report of its identity stays until HotWindow, and the newest head,
// contract or stand-down until 30 days. A drive silent for 90 days goes whole.
// pause runs between batches, so a writer never waits on more than one, and a
// record that arrives during the pass spares whatever it names.
func (s *Store) Collect(ctx context.Context, pause func(context.Context) error) (Collected, error) {
	var res Collected
	now := s.Now()
	window := collectWindow{hot: now.Add(-HotWindow).UnixMilli(), expired: now.Add(-expiredGrace).UnixMilli(), pinned: now.Add(-pinnedWindow).UnixMilli(), retired: now.Add(-retiredWindow).UnixMilli()}
	var head sql.NullInt64
	if err := s.db.QueryRowContext(ctx, "SELECT MAX(seq) FROM records").Scan(&head); err != nil {
		return res, fmt.Errorf("read the newest record: %w", err)
	}
	doomed, skipped, keep, err := s.collectable(ctx, window, head.Int64)
	if err != nil {
		return res, err
	}
	res.Skipped = skipped
	if err := s.archive(ctx, doomed); err != nil {
		return res, err
	}
	for batch := range slices.Chunk(doomed, collectBatch) {
		forgotten, err := s.forget(ctx, batch, head.Int64, keep)
		if err != nil {
			return res, err
		}
		res.Archived += forgotten
		if err := pause(ctx); err != nil {
			return res, err
		}
	}
	if res.Sessions, res.Claims, err = s.forgetSessions(ctx, now.Add(-sessionWindow).UnixMilli()); err != nil {
		return res, err
	}
	if res.Vacuumed, res.Freed, err = s.returnSpace(ctx, pause); err != nil {
		return res, err
	}
	res.Checkpointed, err = s.checkpoint(ctx)
	return res, err
}

const spawnTopic = "spawn:"

var newestStays = []kinds.Kind{kinds.Head, kinds.Contract, kinds.StandDown}

type recordQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type collectWindow struct {
	hot, expired, pinned, retired int64
}

type collectRow struct {
	seq, re, resolves, at, originSeq int64
	drive, kind, lane, origin, hash  string
	spawnOf                          string
	handoffToCCN                     bool
	also                             []int64
	opens, closes                    []string
	aged                             bool
}

func (r collectRow) targets() []int64 {
	return append([]int64{r.re, r.resolves}, r.also...)
}

func (r collectRow) pinned(w collectWindow) bool {
	if r.at >= w.hot {
		return kinds.Kind(r.kind).Spec().Latest
	}
	return r.at >= w.pinned && slices.Contains(newestStays, kinds.Kind(r.kind))
}

func (w collectWindow) eachRecord(ctx context.Context, q recordQuerier, where string, arg int64, each func(collectRow)) error {
	rows, err := q.QueryContext(ctx, `SELECT seq, re, resolves, COALESCE(also_resolves, ''), at, CASE WHEN expires_at IS NULL THEN at < ? ELSE expires_at < ? END, drive, kind, lane, origin, origin_seq, hash, topic, refs
FROM records WHERE `+where, w.hot, w.expired, arg)
	if err != nil {
		return fmt.Errorf("read records: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			r                collectRow
			also, topic, raw string
		)
		if err := rows.Scan(&r.seq, &r.re, &r.resolves, &also, &r.at, &r.aged, &r.drive, &r.kind, &r.lane, &r.origin, &r.originSeq, &r.hash, &topic, &raw); err != nil {
			return fmt.Errorf("scan record: %w", err)
		}
		if also != "" && also != "[]" && json.Unmarshal([]byte(also), &r.also) != nil {
			r.also = nil
		}
		kind := kinds.Kind(r.kind)
		opens, closes := slices.Contains(kinds.Openers(), kind) || kind == kinds.Opened, slices.Contains(kinds.Closers(), kind) || kind == kinds.Landed
		if kind == kinds.State && strings.HasPrefix(topic, spawnTopic) {
			r.spawnOf = strings.TrimPrefix(topic, spawnTopic)
		}
		if opens || closes || kind == kinds.Spawn || kind == kinds.Handoff {
			var refs Refs
			_ = json.Unmarshal([]byte(raw), &refs)
			if kind == kinds.Spawn && len(refs.Lanes) > 0 {
				r.spawnOf = refs.Lanes[0]
			}
			r.handoffToCCN = kind == kinds.Handoff && refs.CCN != ""
			keys := closureKeys(r.lane, topic, kind, refs)
			if opens {
				r.opens = keys
			}
			if closes {
				r.closes = keys
			}
		}
		each(r)
	}
	return rows.Err()
}

func closureKeys(lane, topic string, kind kinds.Kind, refs Refs) []string {
	if kind == kinds.Opened || kind == kinds.Landed {
		keys := make([]string, 0, len(refs.PRs))
		for _, pr := range refs.PRs {
			keys = append(keys, fmt.Sprintf("pr:%s:%s:%d", lane, pr.Repo, pr.Number))
		}
		return keys
	}
	var keys []string
	if topic != "" {
		keys = append(keys, "topic:"+topic)
	}
	for _, stack := range refs.Stacks {
		keys = append(keys, "stack:"+stack)
	}
	for _, target := range refs.Targets {
		keys = append(keys, "target:"+target)
	}
	return keys
}

func (r collectRow) showsLaneTurn() bool {
	switch kinds.Kind(r.kind) {
	case kinds.Claim, kinds.Peer, kinds.Digest:
		return false
	}
	return !r.handoffToCCN
}

func driveKey(drive, key string) string {
	return drive + "\x00" + key
}

type retention struct {
	aged      map[int64]collectRow
	referrers map[int64][]int64
	closers   map[string][]int64
	kept      map[int64]bool
	pending   []int64
}

func (k *retention) keep(seq int64) {
	if _, aged := k.aged[seq]; aged && !k.kept[seq] {
		k.kept[seq] = true
		k.pending = append(k.pending, seq)
	}
}

func (k *retention) keepClosers(drive string, opens []string) {
	for _, key := range opens {
		for _, seq := range k.closers[driveKey(drive, key)] {
			k.keep(seq)
		}
	}
}

func (k *retention) spread() {
	for len(k.pending) > 0 {
		r := k.aged[k.pending[len(k.pending)-1]]
		k.pending = k.pending[:len(k.pending)-1]
		for _, target := range r.targets() {
			k.keep(target)
		}
		for _, referrer := range k.referrers[r.seq] {
			k.keep(referrer)
		}
		k.keepClosers(r.drive, r.opens)
	}
}

func (s *Store) collectable(ctx context.Context, w collectWindow, head int64) ([]collectRow, []string, *retention, error) {
	k := &retention{aged: map[int64]collectRow{}, referrers: map[int64][]int64{}, closers: map[string][]int64{}, kept: map[int64]bool{}}
	var fresh []collectRow
	newest := map[string]int64{}
	spoke := map[string]int64{}
	landed := map[string]int64{}
	err := w.eachRecord(ctx, s.db, "seq <= ?", head, func(r collectRow) {
		spoke[r.drive] = max(spoke[r.drive], r.at)
		if r.showsLaneTurn() {
			newest[driveKey(r.drive, "lane:"+r.lane)] = max(newest[driveKey(r.drive, "lane:"+r.lane)], r.seq)
		}
		if r.spawnOf != "" {
			newest[driveKey(r.drive, "spawn:"+r.spawnOf)] = max(newest[driveKey(r.drive, "spawn:"+r.spawnOf)], r.seq)
		}
		if r.kind == string(kinds.Landed) {
			for _, key := range r.closes {
				landed[driveKey(r.drive, key)] = max(landed[driveKey(r.drive, key)], r.seq)
			}
		}
		if !r.aged {
			if r.re != 0 || r.resolves != 0 || len(r.also) > 0 || len(r.opens) > 0 {
				fresh = append(fresh, collectRow{re: r.re, resolves: r.resolves, also: r.also, drive: r.drive, opens: r.opens})
			}
			return
		}
		k.aged[r.seq] = r
		for _, target := range r.targets() {
			if target != 0 {
				k.referrers[target] = append(k.referrers[target], r.seq)
			}
		}
		for _, key := range r.closes {
			k.closers[driveKey(r.drive, key)] = append(k.closers[driveKey(r.drive, key)], r.seq)
		}
	})
	if err != nil {
		return nil, nil, nil, err
	}
	floor, held, err := s.compactionFloor(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	open := map[string]map[int64]bool{}
	skipped := map[string]bool{}
	var pinned, outside []int64
	for _, r := range k.aged {
		if _, read := open[r.drive]; !read && !skipped[r.drive] && spoke[r.drive] >= w.retired {
			seqs, err := s.openAmong(ctx, r.drive)
			if ctx.Err() != nil {
				return nil, nil, nil, ctx.Err()
			}
			if err != nil {
				skipped[r.drive] = true
			} else {
				open[r.drive] = seqs
			}
		}
		if r.pinned(w) {
			pinned = append(pinned, r.seq)
		}
		for _, target := range r.targets() {
			if _, aged := k.aged[target]; !aged && target != 0 {
				outside = append(outside, target)
			}
		}
	}
	replaced, err := s.supersededAmong(ctx, pinned)
	if err != nil {
		return nil, nil, nil, err
	}
	live, err := s.liveAmong(ctx, outside)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, r := range k.aged {
		switch {
		case held && r.seq > floor,
			slices.ContainsFunc(r.targets(), func(target int64) bool { return live[target] }):
			k.keep(r.seq)
		case spoke[r.drive] < w.retired:
		case r.kind == string(kinds.Digest), r.kind == string(kinds.Incident), skipped[r.drive], open[r.drive][r.seq],
			r.pinned(w) && !replaced[r.seq],
			r.showsLaneTurn() && newest[driveKey(r.drive, "lane:"+r.lane)] == r.seq,
			r.spawnOf != "" && newest[driveKey(r.drive, "spawn:"+r.spawnOf)] == r.seq,
			r.kind == string(kinds.Opened) && slices.ContainsFunc(r.opens, func(key string) bool { return landed[driveKey(r.drive, key)] < r.seq }):
			k.keep(r.seq)
		}
	}
	for _, r := range fresh {
		for _, target := range r.targets() {
			k.keep(target)
		}
		k.keepClosers(r.drive, r.opens)
	}
	buffered, err := pendingTargets(ctx, s.db)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, seq := range buffered {
		k.keep(seq)
	}
	k.spread()
	doomed := make([]collectRow, 0, len(k.aged)-len(k.kept))
	for _, seq := range slices.Backward(slices.Sorted(maps.Keys(k.aged))) {
		if !k.kept[seq] {
			doomed = append(doomed, k.aged[seq])
		}
	}
	return doomed, slices.Sorted(maps.Keys(skipped)), k, nil
}

func pendingTargets(ctx context.Context, q recordQuerier) ([]int64, error) {
	rows, err := q.QueryContext(ctx, "SELECT row FROM pending")
	if err != nil {
		return nil, fmt.Errorf("read buffered rows: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var pointers []Pointer
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan buffered row: %w", err)
		}
		var row Row
		if err := json.Unmarshal([]byte(raw), &row); err != nil {
			return nil, fmt.Errorf("decode buffered row: %w", err)
		}
		pointers = append(pointers, row.AlsoResolves...)
		for _, p := range []*Pointer{row.Re, row.Resolves} {
			if p != nil {
				pointers = append(pointers, *p)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var seqs []int64
	for _, p := range pointers {
		found, err := pointerSeqs(ctx, q, p)
		if err != nil {
			return nil, err
		}
		seqs = append(seqs, found...)
	}
	return seqs, nil
}

func pointerSeqs(ctx context.Context, q recordQuerier, p Pointer) ([]int64, error) {
	rows, err := q.QueryContext(ctx, "SELECT seq FROM records WHERE origin = ?1 AND origin_seq = ?2 UNION SELECT seq FROM aliases WHERE origin = ?1 AND origin_seq = ?2", p.Origin, p.Seq)
	if err != nil {
		return nil, fmt.Errorf("resolve buffered reference: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var seqs []int64
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return nil, fmt.Errorf("scan buffered reference: %w", err)
		}
		seqs = append(seqs, seq)
	}
	return seqs, rows.Err()
}

func (s *Store) supersededAmong(ctx context.Context, seqs []int64) (map[int64]bool, error) {
	ids, err := json.Marshal(seqs)
	if err != nil {
		return nil, fmt.Errorf("encode pinned records: %w", err)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT r.seq FROM records r WHERE r.seq IN (SELECT value FROM json_each(?))
AND EXISTS (SELECT 1 FROM records l WHERE l.drive = r.drive AND l.lane = r.lane AND l.kind = r.kind AND l.topic = r.topic AND l.recipients = r.recipients AND l.seq > r.seq
AND json_extract(l.refs, '$.lanes') IS json_extract(r.refs, '$.lanes') AND json_extract(l.refs, '$.stacks') IS json_extract(r.refs, '$.stacks')
AND json_extract(l.refs, '$.targets') IS json_extract(r.refs, '$.targets') AND json_extract(l.refs, '$.prs') IS json_extract(r.refs, '$.prs'))`, string(ids))
	if err != nil {
		return nil, fmt.Errorf("read superseded records: %w", err)
	}
	defer func() { _ = rows.Close() }()
	superseded := map[int64]bool{}
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return nil, fmt.Errorf("scan superseded record: %w", err)
		}
		superseded[seq] = true
	}
	return superseded, rows.Err()
}

func (s *Store) liveAmong(ctx context.Context, seqs []int64) (map[int64]bool, error) {
	ids, err := json.Marshal(seqs)
	if err != nil {
		return nil, fmt.Errorf("encode retained targets: %w", err)
	}
	rows, err := s.db.QueryContext(ctx, "SELECT seq FROM records WHERE seq IN (SELECT value FROM json_each(?))", string(ids))
	if err != nil {
		return nil, fmt.Errorf("read retained targets: %w", err)
	}
	defer func() { _ = rows.Close() }()
	live := map[int64]bool{}
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return nil, fmt.Errorf("scan retained target: %w", err)
		}
		live[seq] = true
	}
	return live, rows.Err()
}

func (s *Store) openAmong(ctx context.Context, drive string) (map[int64]bool, error) {
	openers, err := s.closureRows(ctx, drive, kinds.Openers())
	if err != nil {
		return nil, err
	}
	open, err := s.unclosed(ctx, drive, openers)
	if err != nil {
		return nil, err
	}
	seqs := make(map[int64]bool, len(open))
	for _, o := range open {
		seqs[o.Seq] = true
	}
	return seqs, nil
}

func (s *Store) archive(ctx context.Context, rows []collectRow) error {
	segments := map[string][]int64{}
	for _, r := range rows {
		name := archiveName(r.drive) + "-" + time.UnixMilli(r.at).UTC().Format("2006-01")
		segments[name] = append(segments[name], r.seq)
	}
	if len(segments) == 0 {
		return nil
	}
	path := filepath.Join(s.home, archiveDir)
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("create archive dir: %w", err)
	}
	stamp := time.Now().UnixNano()
	for name, seqs := range segments {
		if err := s.writeSegment(ctx, path, fmt.Sprintf("%s-%d.jsonl.gz", name, stamp), seqs); err != nil {
			return err
		}
	}
	dir, err := os.Open(path) //nolint:gosec // G304: path is the store's own archive dir.
	if err != nil {
		return fmt.Errorf("open archive dir: %w", err)
	}
	defer func() { _ = dir.Close() }()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync archive dir: %w", err)
	}
	return nil
}

func archiveName(drive string) string {
	name := url.PathEscape(drive)
	if len(name) <= archiveNameBytes {
		return name
	}
	sum := sha256.Sum256([]byte(drive))
	return name[:archiveNameBytes] + "-" + hex.EncodeToString(sum[:8])
}

func (s *Store) writeSegment(ctx context.Context, dir, name string, seqs []int64) error {
	f, err := os.CreateTemp(dir, ".segment-*")
	if err != nil {
		return fmt.Errorf("create archive segment: %w", err)
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}()
	gz := gzip.NewWriter(f)
	lines := json.NewEncoder(gz)
	for batch := range slices.Chunk(seqs, collectBatch) {
		if err := s.encodeRecords(ctx, lines, batch); err != nil {
			return err
		}
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("finish archive segment: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync archive segment: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close archive segment: %w", err)
	}
	if err := os.Rename(f.Name(), filepath.Join(dir, name)); err != nil {
		return fmt.Errorf("publish archive segment: %w", err)
	}
	return nil
}

func (s *Store) encodeRecords(ctx context.Context, lines *json.Encoder, seqs []int64) error {
	ids, err := json.Marshal(seqs)
	if err != nil {
		return fmt.Errorf("encode archived records: %w", err)
	}
	rows, err := s.db.QueryContext(ctx, "SELECT * FROM records WHERE seq IN (SELECT value FROM json_each(?))", string(ids))
	if err != nil {
		return fmt.Errorf("read archived records: %w", err)
	}
	defer func() { _ = rows.Close() }()
	names, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("read record columns: %w", err)
	}
	values := make([]any, len(names))
	pointers := make([]any, len(names))
	for i := range values {
		pointers[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			return fmt.Errorf("scan archived record: %w", err)
		}
		line := make(map[string]any, len(names)+1)
		for i, name := range names {
			line[name] = values[i]
		}
		var refs Refs
		if raw, ok := line["refs"].(string); ok && json.Unmarshal([]byte(raw), &refs) == nil {
			if blob, ok := s.blobBody(refs.Path); ok {
				line["blob"] = blob
			}
		}
		if err := lines.Encode(line); err != nil {
			return fmt.Errorf("write archive segment: %w", err)
		}
	}
	return rows.Err()
}

func (s *Store) blobBody(path string) (string, bool) {
	if path == "" || filepath.Dir(path) != filepath.Join(s.home, blobDir) {
		return "", false
	}
	body, err := os.ReadFile(path) //nolint:gosec // G304: path is under the store's own blob dir.
	if err != nil {
		return "", false
	}
	return string(body), true
}

func (s *Store) spared(ctx context.Context, tx *sql.Tx, rows []collectRow, head int64, k *retention) ([]collectRow, error) {
	opened := map[string]bool{}
	if err := (collectWindow{}).eachRecord(ctx, tx, "seq > ?", head, func(r collectRow) {
		for _, target := range r.targets() {
			k.keep(target)
		}
		for _, key := range r.opens {
			opened[driveKey(r.drive, key)] = true
		}
	}); err != nil {
		return nil, err
	}
	buffered, err := pendingTargets(ctx, tx)
	if err != nil {
		return nil, err
	}
	for _, seq := range buffered {
		k.keep(seq)
	}
	ids, err := json.Marshal(collectedSeqs(rows))
	if err != nil {
		return nil, fmt.Errorf("encode forgotten records: %w", err)
	}
	current, err := tx.QueryContext(ctx, "SELECT seq, hash FROM records WHERE seq IN (SELECT value FROM json_each(?))", string(ids))
	if err != nil {
		return nil, fmt.Errorf("read forgotten versions: %w", err)
	}
	defer func() { _ = current.Close() }()
	stored := make(map[int64]string, len(rows))
	for current.Next() {
		var (
			seq  int64
			hash string
		)
		if err := current.Scan(&seq, &hash); err != nil {
			return nil, fmt.Errorf("scan forgotten version: %w", err)
		}
		stored[seq] = hash
	}
	if err := current.Err(); err != nil {
		return nil, err
	}
	for _, r := range rows {
		if stored[r.seq] != r.hash || slices.ContainsFunc(r.closes, func(key string) bool { return opened[driveKey(r.drive, key)] }) {
			k.keep(r.seq)
		}
	}
	k.spread()
	return slices.DeleteFunc(rows, func(r collectRow) bool { return k.kept[r.seq] }), nil
}

func collectedSeqs(rows []collectRow) []int64 {
	seqs := make([]int64, len(rows))
	for i, r := range rows {
		seqs[i] = r.seq
	}
	return seqs
}

func (s *Store) forget(ctx context.Context, rows []collectRow, head int64, k *retention) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin forget: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	rows, err = s.spared(ctx, tx, rows, head, k)
	if err != nil {
		return 0, err
	}
	self := s.Self()
	var through int64
	for _, r := range rows {
		if r.origin == self {
			through = max(through, r.seq)
			continue
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO folded (origin, origin_seq) VALUES (?, ?) ON CONFLICT DO NOTHING", r.origin, r.originSeq); err != nil {
			return 0, fmt.Errorf("record forgotten %s/%d: %w", r.origin, r.originSeq, err)
		}
	}
	if through > 0 {
		if _, err := tx.ExecContext(ctx, `INSERT INTO meta (key, value) VALUES ('compacted_through', ?)
ON CONFLICT (key) DO UPDATE SET value = MAX(value, excluded.value)`, through); err != nil {
			return 0, fmt.Errorf("record compacted-through: %w", err)
		}
	}
	ids, err := json.Marshal(collectedSeqs(rows))
	if err != nil {
		return 0, fmt.Errorf("encode forgotten records: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO folded (origin, origin_seq) SELECT origin, origin_seq FROM aliases WHERE seq IN (SELECT value FROM json_each(?)) ON CONFLICT DO NOTHING", string(ids)); err != nil {
		return 0, fmt.Errorf("record forgotten aliases: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM aliases WHERE seq IN (SELECT value FROM json_each(?))", string(ids)); err != nil {
		return 0, fmt.Errorf("delete forgotten aliases: %w", err)
	}
	var newest sql.NullInt64
	if err := tx.QueryRowContext(ctx, "SELECT MAX(at) FROM records WHERE seq IN (SELECT value FROM json_each(?))", string(ids)).Scan(&newest); err != nil {
		return 0, fmt.Errorf("read the newest forgotten record: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM records WHERE seq IN (SELECT value FROM json_each(?))", string(ids)); err != nil {
		return 0, fmt.Errorf("delete forgotten records: %w", err)
	}
	if newest.Int64 >= s.Now().Add(-2*hotWindowSpan).UnixMilli() {
		if err := countRecordsDeleted(ctx, tx); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit forget: %w", err)
	}
	return len(rows), nil
}

func (s *Store) forgetSessions(ctx context.Context, cutoff int64) (sessions, claims int, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("begin session retention: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	gone, err := tx.ExecContext(ctx, "DELETE FROM sessions WHERE MAX(seen_at, announced_at) < ?", cutoff)
	if err != nil {
		return 0, 0, fmt.Errorf("delete idle sessions: %w", err)
	}
	if err := forgetDeadWatches(ctx, tx); err != nil {
		return 0, 0, err
	}
	dropped, err := tx.ExecContext(ctx, "DELETE FROM claims WHERE at < ? AND NOT EXISTS (SELECT 1 FROM sessions s WHERE s.session = claims.session)", cutoff)
	if err != nil {
		return 0, 0, fmt.Errorf("delete orphaned claims: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, fmt.Errorf("commit session retention: %w", err)
	}
	s64, err := gone.RowsAffected()
	if err != nil {
		return 0, 0, fmt.Errorf("count idle sessions: %w", err)
	}
	c64, err := dropped.RowsAffected()
	if err != nil {
		return 0, 0, fmt.Errorf("count orphaned claims: %w", err)
	}
	return int(s64), int(c64), nil
}

// forgetDeadWatches deletes the cursors of watch clients that have exited. A
// client names its cursor watch-<pid>-<nanos>.
func forgetDeadWatches(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, "SELECT DISTINCT name FROM cursors WHERE name LIKE 'watch-%'")
	if err != nil {
		return fmt.Errorf("read watch cursors: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var dead []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("scan watch cursor: %w", err)
		}
		pid, _, _ := strings.Cut(strings.TrimPrefix(name, "watch-"), "-")
		if n, err := strconv.Atoi(pid); err == nil && errors.Is(syscall.Kill(n, 0), syscall.ESRCH) {
			dead = append(dead, name)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, name := range dead {
		if _, err := tx.ExecContext(ctx, "DELETE FROM cursors WHERE name = ?", name); err != nil {
			return fmt.Errorf("delete watch cursor: %w", err)
		}
	}
	return nil
}

func (s *Store) returnSpace(ctx context.Context, pause func(context.Context) error) (vacuumed bool, freed int, err error) {
	var mode, free, total int
	if err := s.db.QueryRowContext(ctx, "SELECT (SELECT auto_vacuum FROM pragma_auto_vacuum), (SELECT freelist_count FROM pragma_freelist_count), (SELECT page_count FROM pragma_page_count)").Scan(&mode, &free, &total); err != nil {
		return false, 0, fmt.Errorf("read free pages: %w", err)
	}
	if mode == 0 {
		if free*vacuumFreeRatio < total {
			return false, 0, nil
		}
		return true, free, s.vacuumIncremental(ctx)
	}
	for free > 0 {
		if _, err := s.db.ExecContext(ctx, fmt.Sprintf("PRAGMA incremental_vacuum(%d)", min(free, vacuumPages))); err != nil {
			return false, freed, fmt.Errorf("return free pages: %w", err)
		}
		freed += min(free, vacuumPages)
		free -= min(free, vacuumPages)
		if err := pause(ctx); err != nil {
			return false, freed, err
		}
	}
	return false, freed, nil
}

func (s *Store) vacuumIncremental(ctx context.Context) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open vacuum connection: %w", err)
	}
	defer func() { _ = conn.Close() }()
	for _, statement := range []string{"PRAGMA auto_vacuum = INCREMENTAL", "VACUUM"} {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("%s: %w", strings.ToLower(statement), err)
		}
	}
	return nil
}

func (s *Store) checkpoint(ctx context.Context) (bool, error) {
	var busy, log, moved int
	if err := s.db.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &log, &moved); err != nil {
		return false, fmt.Errorf("checkpoint: %w", err)
	}
	return busy == 0, nil
}

// SweepFiles deletes the blobs neither a record nor a queued write names, and the
// archives past their window. A blob stays for an hour, since a post writes it
// before its record.
func (s *Store) SweepFiles(ctx context.Context) (blobs, archives int, err error) {
	queued, err := s.queuedWrites()
	if err != nil {
		return 0, 0, err
	}
	named, err := s.blobsNamed(ctx)
	if err != nil {
		return 0, 0, err
	}
	now := s.Now()
	if blobs, err = sweep(filepath.Join(s.home, blobDir), now.Add(-blobGrace), func(name string) bool {
		return named[name] || bytes.Contains(queued, []byte(name))
	}); err != nil {
		return blobs, 0, err
	}
	if archives, err = sweep(filepath.Join(s.home, archiveDir), now.Add(-archiveWindow), func(string) bool { return false }); err != nil {
		return blobs, archives, err
	}
	if _, err = sweep(filepath.Join(s.home, channelBeatDir), now.Add(-channelBeatWindow), func(string) bool { return false }); err != nil {
		return blobs, archives, err
	}
	return blobs, archives, nil
}

func (s *Store) queuedWrites() ([]byte, error) {
	dir := filepath.Join(s.home, UnsentDir)
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list queued writes: %w", err)
	}
	var queued []byte
	for _, e := range entries {
		body, err := os.ReadFile(filepath.Join(dir, e.Name())) //nolint:gosec // G304: path is under the store's own unsent dir.
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read queued write: %w", err)
		}
		queued = append(queued, body...)
	}
	return queued, nil
}

func (s *Store) blobsNamed(ctx context.Context) (map[string]bool, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT refs FROM records WHERE instr(refs, ?) > 0", string(filepath.Separator)+blobDir+string(filepath.Separator))
	if err != nil {
		return nil, fmt.Errorf("read blob names: %w", err)
	}
	defer func() { _ = rows.Close() }()
	named := map[string]bool{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan blob name: %w", err)
		}
		var refs struct {
			Path string `json:"path"`
		}
		if json.Unmarshal([]byte(raw), &refs) == nil && refs.Path != "" {
			named[filepath.Base(refs.Path)] = true
		}
	}
	return named, rows.Err()
}

func sweep(dir string, before time.Time, keep func(string) bool) (int, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("list %s: %w", dir, err)
	}
	removed := 0
	for _, e := range entries {
		if keep(e.Name()) {
			continue
		}
		info, err := e.Info()
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return removed, fmt.Errorf("stat %s: %w", e.Name(), err)
		}
		if info.ModTime().After(before) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return removed, fmt.Errorf("remove %s: %w", e.Name(), err)
		}
		removed++
	}
	return removed, nil
}
