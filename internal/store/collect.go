package store

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

const (
	// HotWindow is how long a record that nothing open depends on stays in the store.
	HotWindow = 14 * 24 * time.Hour

	expiredGrace    = 72 * time.Hour
	sessionWindow   = 30 * 24 * time.Hour
	pinnedWindow    = 30 * 24 * time.Hour
	archiveWindow   = 180 * 24 * time.Hour
	blobGrace       = time.Hour
	collectBatch    = 1000
	vacuumFreeRatio = 4
	vacuumPages     = 2048
	archiveDir      = "archive"
	blobDir         = "blobs"
)

// Collected reports one Collect pass.
type Collected struct {
	Archived int
	Sessions int
	Claims   int
	Skipped  []string
	Vacuumed bool
	Freed    int
}

// Collect deletes the records past their retention, oldest first, after writing
// each to the drive's archive, then returns the freed pages to the file system.
// A record stays while it is open, while another record it closes or answers
// stays, and while a peer has not acknowledged it. The newest state or report of
// a lane and topic stays until HotWindow, and the newest head, contract, opened,
// spawn or stand-down until 30 days. pause runs between batches, so a writer
// never waits on more than one.
func (s *Store) Collect(ctx context.Context, pause func(context.Context) error) (Collected, error) {
	var res Collected
	now := s.Now()
	hot, expired := now.Add(-HotWindow).UnixMilli(), now.Add(-expiredGrace).UnixMilli()
	floor, held, err := s.compactionFloor(ctx)
	if err != nil {
		return res, err
	}
	pinned := now.Add(-pinnedWindow).UnixMilli()
	pass := &collectPass{open: map[string]map[int64]bool{}, skipped: map[string]bool{}}
	var after int64
	for {
		rows, err := s.collectCandidates(ctx, after, hot, expired)
		if err != nil {
			return res, err
		}
		if len(rows) == 0 {
			break
		}
		after = rows[len(rows)-1].seq
		doomed, err := s.collectable(ctx, pass, rows, pinned, floor, held)
		if err != nil {
			return res, err
		}
		if err := s.archive(doomed); err != nil {
			return res, err
		}
		if err := s.forget(ctx, doomed); err != nil {
			return res, err
		}
		res.Archived += len(doomed)
		if err := pause(ctx); err != nil {
			return res, err
		}
	}
	res.Skipped = slices.Sorted(maps.Keys(pass.skipped))
	if res.Sessions, res.Claims, err = s.forgetSessions(ctx, now.Add(-sessionWindow).UnixMilli()); err != nil {
		return res, err
	}
	if res.Vacuumed, res.Freed, err = s.returnSpace(ctx, pause); err != nil {
		return res, err
	}
	return res, nil
}

var newestStays = []kinds.Kind{kinds.Head, kinds.Contract, kinds.Opened, kinds.Spawn, kinds.StandDown}

type collectPass struct {
	open    map[string]map[int64]bool
	skipped map[string]bool
}

type collectRow struct {
	seq, re, resolves, at int64
	drive, kind, origin   string
	also                  []int64
	path                  string
	expired               bool
	columns               map[string]any
}

func (s *Store) collectCandidates(ctx context.Context, after, hot, expired int64) ([]collectRow, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT * FROM records WHERE seq > ? AND (at < ? OR expires_at < ?) ORDER BY seq LIMIT ?", after, hot, expired, collectBatch)
	if err != nil {
		return nil, fmt.Errorf("read records past retention: %w", err)
	}
	defer func() { _ = rows.Close() }()
	names, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("read record columns: %w", err)
	}
	var out []collectRow
	for rows.Next() {
		values := make([]any, len(names))
		pointers := make([]any, len(names))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, fmt.Errorf("scan record past retention: %w", err)
		}
		columns := make(map[string]any, len(names))
		for i, name := range names {
			columns[name] = values[i]
		}
		out = append(out, newCollectRow(columns, hot))
	}
	return out, rows.Err()
}

func newCollectRow(columns map[string]any, hot int64) collectRow {
	number := func(name string) int64 {
		n, _ := columns[name].(int64)
		return n
	}
	text := func(name string) string {
		t, _ := columns[name].(string)
		return t
	}
	row := collectRow{
		seq: number("seq"), re: number("re"), resolves: number("resolves"), at: number("at"),
		drive: text("drive"), kind: text("kind"), origin: text("origin"), columns: columns,
	}
	row.expired = row.at >= hot
	if json.Unmarshal([]byte(text("also_resolves")), &row.also) != nil {
		row.also = nil
	}
	var refs struct {
		Path string `json:"path"`
	}
	if json.Unmarshal([]byte(text("refs")), &refs) == nil {
		row.path = refs.Path
	}
	return row
}

func (r collectRow) pinned(window int64) bool {
	if r.expired {
		return kinds.Kind(r.kind).Spec().Latest
	}
	return r.at >= window && slices.Contains(newestStays, kinds.Kind(r.kind))
}

func (s *Store) collectable(ctx context.Context, pass *collectPass, rows []collectRow, window, floor int64, held bool) ([]collectRow, error) {
	var targets, pinned []int64
	for _, r := range rows {
		targets = append(targets, r.re, r.resolves)
		targets = append(targets, r.also...)
		if r.pinned(window) {
			pinned = append(pinned, r.seq)
		}
		if _, read := pass.open[r.drive]; !read && !pass.skipped[r.drive] {
			if err := s.readOpen(ctx, pass, r.drive); err != nil {
				return nil, err
			}
		}
	}
	live, err := s.liveAmong(ctx, targets)
	if err != nil {
		return nil, err
	}
	replaced, err := s.supersededAmong(ctx, pinned)
	if err != nil {
		return nil, err
	}
	var doomed []collectRow
	for _, r := range rows {
		switch {
		case r.kind == string(kinds.Digest), pass.skipped[r.drive], pass.open[r.drive][r.seq]:
		case held && r.seq > floor:
		case r.pinned(window) && !replaced[r.seq]:
		case live[r.re], live[r.resolves], slices.ContainsFunc(r.also, func(seq int64) bool { return live[seq] }):
		default:
			doomed = append(doomed, r)
			delete(live, r.seq)
		}
	}
	return doomed, nil
}

func (s *Store) supersededAmong(ctx context.Context, seqs []int64) (map[int64]bool, error) {
	ids, err := json.Marshal(seqs)
	if err != nil {
		return nil, fmt.Errorf("encode pinned records: %w", err)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT r.seq FROM records r WHERE r.seq IN (SELECT value FROM json_each(?))
AND EXISTS (SELECT 1 FROM records l WHERE l.drive = r.drive AND l.lane = r.lane AND l.kind = r.kind AND l.topic = r.topic AND l.seq > r.seq)`, string(ids))
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

func (s *Store) readOpen(ctx context.Context, pass *collectPass, drive string) error {
	openers, err := s.closureRows(ctx, drive, kinds.Openers())
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		pass.skipped[drive] = true
		return nil
	}
	open, err := s.unclosed(ctx, drive, openers)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		pass.skipped[drive] = true
		return nil
	}
	seqs := make(map[int64]bool, len(open))
	for _, o := range open {
		seqs[o.Seq] = true
	}
	pass.open[drive] = seqs
	return nil
}

func (s *Store) liveAmong(ctx context.Context, seqs []int64) (map[int64]bool, error) {
	ids, err := json.Marshal(slices.DeleteFunc(slices.Compact(slices.Sorted(slices.Values(seqs))), func(seq int64) bool { return seq == 0 }))
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

func (s *Store) archive(rows []collectRow) error {
	lines := map[string][]collectRow{}
	for _, r := range rows {
		month := time.UnixMilli(r.at).UTC().Format("2006-01")
		name := url.PathEscape(r.drive) + "-" + month + ".jsonl.gz"
		lines[name] = append(lines[name], r)
	}
	if len(lines) == 0 {
		return nil
	}
	dir := filepath.Join(s.home, archiveDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create archive dir: %w", err)
	}
	for name, group := range lines {
		if err := s.appendArchive(filepath.Join(dir, name), group); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) appendArchive(path string, rows []collectRow) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600) //nolint:gosec // G304: path is under the store's own archive dir.
	if err != nil {
		return fmt.Errorf("open archive: %w", err)
	}
	defer func() { _ = f.Close() }()
	gz := gzip.NewWriter(f)
	lines := json.NewEncoder(gz)
	for _, r := range rows {
		line := r.columns
		if blob, ok := s.blobBody(r.path); ok {
			line = maps.Clone(line)
			line["blob"] = blob
		}
		if err := lines.Encode(line); err != nil {
			return fmt.Errorf("write archive: %w", err)
		}
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("finish archive: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync archive: %w", err)
	}
	return nil
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

func (s *Store) forget(ctx context.Context, rows []collectRow) error {
	if len(rows) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin forget: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	seqs := make([]int64, len(rows))
	var through int64
	for i, r := range rows {
		seqs[i] = r.seq
		if r.origin == "" {
			through = max(through, r.seq)
			continue
		}
		originSeq, _ := r.columns["origin_seq"].(int64)
		if _, err := tx.ExecContext(ctx, "INSERT INTO folded (origin, origin_seq) VALUES (?, ?) ON CONFLICT DO NOTHING", r.origin, originSeq); err != nil {
			return fmt.Errorf("record forgotten %s/%d: %w", r.origin, originSeq, err)
		}
	}
	if through > 0 {
		if _, err := tx.ExecContext(ctx, `INSERT INTO meta (key, value) VALUES ('compacted_through', ?)
ON CONFLICT (key) DO UPDATE SET value = MAX(value, excluded.value)`, through); err != nil {
			return fmt.Errorf("record compacted-through: %w", err)
		}
	}
	ids, err := json.Marshal(seqs)
	if err != nil {
		return fmt.Errorf("encode forgotten records: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO folded (origin, origin_seq) SELECT origin, origin_seq FROM aliases WHERE seq IN (SELECT value FROM json_each(?)) ON CONFLICT DO NOTHING", string(ids)); err != nil {
		return fmt.Errorf("record forgotten aliases: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM aliases WHERE seq IN (SELECT value FROM json_each(?))", string(ids)); err != nil {
		return fmt.Errorf("delete forgotten aliases: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM records WHERE seq IN (SELECT value FROM json_each(?))", string(ids)); err != nil {
		return fmt.Errorf("delete forgotten records: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit forget: %w", err)
	}
	return nil
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
	for _, statement := range []string{"PRAGMA auto_vacuum = INCREMENTAL", "VACUUM", "PRAGMA wal_checkpoint(TRUNCATE)"} {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("%s: %w", strings.ToLower(statement), err)
		}
	}
	return nil
}

// SweepFiles deletes the blobs no record names and the archives past their
// window. A blob stays for an hour, since a post writes it before its record.
func (s *Store) SweepFiles(ctx context.Context) (blobs, archives int, err error) {
	named, err := s.blobsNamed(ctx)
	if err != nil {
		return 0, 0, err
	}
	now := s.Now()
	if blobs, err = sweep(filepath.Join(s.home, blobDir), now.Add(-blobGrace), func(name string) bool { return named[name] }); err != nil {
		return blobs, 0, err
	}
	if archives, err = sweep(filepath.Join(s.home, archiveDir), now.Add(-archiveWindow), func(string) bool { return false }); err != nil {
		return blobs, archives, err
	}
	return blobs, archives, nil
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
