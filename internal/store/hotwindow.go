package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// HotWindowBytes caps the memory a hot window's database may hold.
	HotWindowBytes = 128 << 20
	hotWindowSpan  = 36 * time.Hour
	hotWindowBatch = 1000
	hotWindowTrim  = 2000
	hotWindowAge   = 10 * time.Minute
	hotWindowFile  = "window.on"
	hotWindowConns = 8
)

var (
	hotWindowObjects = regexp.MustCompile(`^(records|record_recipients|record_also_resolves|record_json_errors|record_closer_keys|record_root_audience|record_ref_keys)$`)
	hotWindowSkipped = regexp.MustCompile(`^(records_hash|records_line_hash|records_id|records_drive_tally|records_drive_lane_score|records_identity|record_mutations_.*|record_trigrams_.*|incident_.*|lane_holds_.*)$`)
	hotWindowSeq     atomic.Int64

	errHotWindowStale = errors.New("hot window: a stored record changed under it")
)

// HotWindowOn reports whether home's switch asks the daemon for a hot window.
func HotWindowOn(home string) bool {
	_, err := os.Stat(filepath.Join(home, hotWindowFile))
	return err == nil
}

// SetHotWindow flips home's switch; a running daemon follows within a second.
func SetHotWindow(home string, on bool) error {
	path := filepath.Join(home, hotWindowFile)
	if !on {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("turn the hot window off: %w", err)
		}
		return nil
	}
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		return fmt.Errorf("turn the hot window on: %w", err)
	}
	return nil
}

type hotWindowSlot struct {
	current atomic.Pointer[MemWindow]
}

// MemWindow holds every record from a floor seq up in an in-memory database with
// the store's own schema, so the store's own queries run on it unchanged and a
// commit costs it only the new rows. A read it cannot answer exactly goes to the
// store: whatever it returns is what the store would have.
type MemWindow struct {
	source   *Store
	mem      *Store
	observer *ChangeVersion
	pin      *sql.Conn
	columns  []string
	insert   string

	mu        sync.RWMutex
	version   int64
	head      int64
	mutations int64
	inserted  int
	trimmed   time.Time
	stale     atomic.Bool
	floor     atomic.Int64
	belowAt   atomic.Int64
	bytes     atomic.Int64
	limit     int64
	hits      atomic.Int64
	misses    atomic.Int64
	failures  atomic.Int64
}

// HotWindowStats is what a hot window holds and how often it has answered.
type HotWindowStats struct {
	Floor    int64
	Head     int64
	Bytes    int64
	Hits     int64
	Misses   int64
	Failures int64
}

// BuildHotWindow fills a window from the newest hotWindowSpan of s, in batches
// that pace calls between, so readers keep the store while it fills.
func (s *Store) BuildHotWindow(ctx context.Context, pace func(context.Context) error) (*MemWindow, error) {
	return s.BuildHotWindowUnder(ctx, pace, HotWindowBytes)
}

// BuildHotWindowUnder is BuildHotWindow with its own cap on the bytes the window holds.
func (s *Store) BuildHotWindowUnder(ctx context.Context, pace func(context.Context) error, limit int64) (*MemWindow, error) {
	w, err := s.openHotWindow(ctx)
	if err != nil {
		return nil, err
	}
	w.limit = limit
	if err := w.fill(ctx, pace); err != nil {
		return nil, errors.Join(err, w.Close())
	}
	return w, nil
}

// ServeHotWindow has s's reads try w first; nil returns them to the store alone.
func (s *Store) ServeHotWindow(w *MemWindow) {
	s.window.current.Store(w)
}

// ServedHotWindow is the window s's reads try first, nil when there is none.
func (s *Store) ServedHotWindow() *MemWindow {
	return s.window.current.Load()
}

func (s *Store) openHotWindow(ctx context.Context) (*MemWindow, error) {
	db, err := sql.Open("sqlite", fmt.Sprintf("file:/cci-hot-window-%d?vfs=memdb&_pragma=busy_timeout(%d)", hotWindowSeq.Add(1), busyTimeout.Milliseconds()))
	if err != nil {
		return nil, fmt.Errorf("open hot window: %w", err)
	}
	w := &MemWindow{source: s, mem: &Store{db: db, rw: db, home: s.home, node: s.node, owner: s.owner, Now: s.Now, floors: &sinceFloors{}, window: &hotWindowSlot{}}}
	db.SetMaxOpenConns(hotWindowConns)
	if w.pin, err = db.Conn(ctx); err != nil {
		return nil, errors.Join(fmt.Errorf("pin the hot window: %w", err), db.Close())
	}
	if err := w.copySchema(ctx); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	tip, err := s.RecordTip(ctx)
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	if !tip.Exact {
		return nil, errors.Join(errors.New("hot window: the store does not count changes to its records yet"), db.Close())
	}
	if w.observer, err = s.OpenChangeVersion(ctx); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	if err := w.observer.connection.QueryRowContext(ctx, "SELECT COALESCE((SELECT value FROM meta WHERE key = ?), 0)", recordMutationsKey).Scan(&w.mutations); err != nil {
		return nil, errors.Join(fmt.Errorf("read record mutations: %w", err), w.Close())
	}
	return w, nil
}

// Close frees the window; a read still on it fails and goes to the store.
func (w *MemWindow) Close() error {
	var closing error
	if w.observer != nil {
		closing = w.observer.Close()
	}
	return errors.Join(closing, w.pin.Close(), w.mem.db.Close())
}

func (w *MemWindow) copySchema(ctx context.Context) error {
	rows, err := w.source.db.QueryContext(ctx, "SELECT type, name, tbl_name, sql FROM sqlite_master WHERE sql IS NOT NULL")
	if err != nil {
		return fmt.Errorf("read the store's schema: %w", err)
	}
	defer func() { _ = rows.Close() }()
	byType := map[string][]string{}
	for rows.Next() {
		var kind, name, table, statement string
		if err := rows.Scan(&kind, &name, &table, &statement); err != nil {
			return fmt.Errorf("read the store's schema: %w", err)
		}
		if hotWindowObjects.MatchString(table) && !hotWindowSkipped.MatchString(name) {
			byType[kind] = append(byType[kind], statement)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read the store's schema: %w", err)
	}
	for _, kind := range []string{"table", "index", "trigger"} {
		for _, statement := range byType[kind] {
			if _, err := w.mem.rw.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("copy %q to the hot window: %w", statement, err)
			}
		}
	}
	columns, err := w.mem.db.QueryContext(ctx, "SELECT name FROM pragma_table_info('records')")
	if err != nil {
		return fmt.Errorf("read the records columns: %w", err)
	}
	defer func() { _ = columns.Close() }()
	for columns.Next() {
		var name string
		if err := columns.Scan(&name); err != nil {
			return fmt.Errorf("read the records columns: %w", err)
		}
		w.columns = append(w.columns, name)
	}
	w.insert = "INSERT OR IGNORE INTO records (" + strings.Join(w.columns, ", ") + ") VALUES (" + strings.Join(slices.Repeat([]string{"?"}, len(w.columns)), ", ") + ")"
	return columns.Err()
}

func (w *MemWindow) fill(ctx context.Context, pace func(context.Context) error) error {
	var since sql.NullInt64
	if err := w.source.db.QueryRowContext(ctx, "SELECT MIN(seq) FROM records WHERE at >= ?", w.source.Now().Add(-hotWindowSpan).UnixMilli()).Scan(&since); err != nil {
		return fmt.Errorf("find the hot window's floor: %w", err)
	}
	if !since.Valid {
		var head sql.NullInt64
		if err := w.source.db.QueryRowContext(ctx, "SELECT MAX(seq) FROM records").Scan(&head); err != nil {
			return fmt.Errorf("find the hot window's floor: %w", err)
		}
		since = sql.NullInt64{Int64: head.Int64 + 1, Valid: true}
	}
	if err := w.moveFloor(ctx, since.Int64); err != nil {
		return err
	}
	w.mu.Lock()
	w.head = since.Int64 - 1
	w.mu.Unlock()
	for {
		w.mu.Lock()
		more, err := w.catchUpBatch(ctx)
		w.mu.Unlock()
		if err != nil {
			return err
		}
		if !more {
			return w.catchUp(ctx)
		}
		if err := pace(ctx); err != nil {
			return err
		}
	}
}

func (w *MemWindow) moveFloor(ctx context.Context, floor int64) error {
	var below sql.NullInt64
	if err := w.source.db.QueryRowContext(ctx, "SELECT at FROM records INDEXED BY records_at WHERE seq < ? ORDER BY at DESC LIMIT 1", floor).Scan(&below); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read the newest record under the hot window: %w", err)
	}
	w.belowAt.Store(below.Int64)
	w.floor.Store(floor)
	return nil
}

func (w *MemWindow) catchUp(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	version, err := w.observer.Read(ctx)
	if err != nil {
		return err
	}
	if version == w.version && w.version != 0 {
		return nil
	}
	for {
		more, err := w.catchUpBatch(ctx)
		if err != nil {
			return err
		}
		if !more {
			break
		}
	}
	if err := w.trim(ctx); err != nil {
		w.stale.Store(true)
		return err
	}
	w.version = version
	return nil
}

func (w *MemWindow) catchUpBatch(ctx context.Context) (bool, error) {
	snapshot, err := w.observer.connection.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return false, fmt.Errorf("begin hot window read: %w", err)
	}
	defer func() { _ = snapshot.Rollback() }()
	var mutations int64
	if err := snapshot.QueryRowContext(ctx, "SELECT COALESCE((SELECT value FROM meta WHERE key = ?), 0)", recordMutationsKey).Scan(&mutations); err != nil {
		return false, fmt.Errorf("read record mutations: %w", err)
	}
	if mutations != w.mutations {
		w.stale.Store(true)
		return false, errHotWindowStale
	}
	rows, err := w.readRows(ctx, snapshot, "seq > ? ORDER BY seq LIMIT ?", w.head, hotWindowBatch)
	if err != nil || len(rows) == 0 {
		return false, err
	}
	tx, err := w.mem.rw.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin hot window batch: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	seqAt, reAt := slices.Index(w.columns, "seq"), slices.Index(w.columns, "re")
	floor := w.floor.Load()
	for _, row := range rows {
		if re, _ := row[reAt].(int64); re > 0 && re < floor {
			if err := w.keepParent(ctx, snapshot, tx, re); err != nil {
				return false, err
			}
		}
		if _, err := tx.ExecContext(ctx, w.insert, row...); err != nil {
			return false, fmt.Errorf("insert into the hot window: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit hot window batch: %w", err)
	}
	w.head = rows[len(rows)-1][seqAt].(int64)
	w.inserted += len(rows)
	return len(rows) == hotWindowBatch, nil
}

func (w *MemWindow) keepParent(ctx context.Context, snapshot, tx *sql.Tx, seq int64) error {
	rows, err := w.readRows(ctx, snapshot, "seq = ?", seq)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := tx.ExecContext(ctx, w.insert, row...); err != nil {
			return fmt.Errorf("keep a parent in the hot window: %w", err)
		}
	}
	return nil
}

func (w *MemWindow) readRows(ctx context.Context, snapshot *sql.Tx, where string, params ...any) ([][]any, error) {
	rows, err := snapshot.QueryContext(ctx, "SELECT "+strings.Join(w.columns, ", ")+" FROM records WHERE "+where, params...)
	if err != nil {
		return nil, fmt.Errorf("read records for the hot window: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out [][]any
	for rows.Next() {
		row := make([]any, len(w.columns))
		targets := make([]any, len(w.columns))
		for i := range row {
			targets[i] = &row[i]
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, fmt.Errorf("read records for the hot window: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (w *MemWindow) trim(ctx context.Context) error {
	now := w.source.Now()
	if w.inserted < hotWindowTrim && w.bytes.Load() != 0 && now.Sub(w.trimmed) < hotWindowAge {
		return nil
	}
	w.inserted, w.trimmed = 0, now
	var aged sql.NullInt64
	if err := w.mem.db.QueryRowContext(ctx, "SELECT MIN(seq) FROM records WHERE seq >= ? AND at >= ?", w.floor.Load(), now.Add(-hotWindowSpan).UnixMilli()).Scan(&aged); err != nil {
		return fmt.Errorf("find the hot window's oldest record: %w", err)
	}
	if aged.Valid && aged.Int64 > w.floor.Load() {
		if err := w.dropBelow(ctx, aged.Int64); err != nil {
			return err
		}
	}
	for {
		used, err := w.usedBytes(ctx)
		if err != nil {
			return err
		}
		w.bytes.Store(used)
		floor := w.floor.Load()
		if used <= w.limit || floor > w.head {
			return nil
		}
		if err := w.dropBelow(ctx, min(floor+hotWindowTrim, w.head+1)); err != nil {
			return err
		}
	}
}

func (w *MemWindow) dropBelow(ctx context.Context, floor int64) error {
	if _, err := w.mem.rw.ExecContext(ctx, "DELETE FROM records WHERE seq < ? AND seq NOT IN (SELECT re FROM records WHERE seq >= ?)", floor, floor); err != nil {
		return fmt.Errorf("trim the hot window: %w", err)
	}
	return w.moveFloor(ctx, floor)
}

func (w *MemWindow) usedBytes(ctx context.Context) (int64, error) {
	var used int64
	err := w.mem.db.QueryRowContext(ctx, "SELECT (SELECT page_count FROM pragma_page_count()) * (SELECT page_size FROM pragma_page_size()) - (SELECT freelist_count FROM pragma_freelist_count()) * (SELECT page_size FROM pragma_page_size())").Scan(&used)
	if err != nil {
		return 0, fmt.Errorf("measure the hot window: %w", err)
	}
	return used, nil
}

// Stats reports what the window holds and how often it answered.
func (w *MemWindow) Stats() HotWindowStats {
	w.mu.RLock()
	head := w.head
	w.mu.RUnlock()
	return HotWindowStats{Floor: w.floor.Load(), Head: head, Bytes: w.bytes.Load(), Hits: w.hits.Load(), Misses: w.misses.Load(), Failures: w.failures.Load()}
}

// Stale reports that a stored record changed under the window, which must be rebuilt.
func (w *MemWindow) Stale() bool {
	return w.stale.Load()
}

// CatchUp brings the window to every commit finished before it was called.
func (w *MemWindow) CatchUp(ctx context.Context) error {
	return w.catchUp(ctx)
}

func (w *MemWindow) answer(ctx context.Context, s *Store, f Filter, projection string, now time.Time) ([]Record, bool) {
	if f.BroadcastFloor != "" {
		after, _, err := s.Cursor(ctx, f.BroadcastFloor, f.Drive)
		if err != nil {
			w.failures.Add(1)
			return nil, false
		}
		f.broadcastAfter = &after
		if f.broadcastSince, _, err = s.Cursor(ctx, broadcastStart(f.BroadcastFloor), f.Drive); err != nil {
			w.failures.Add(1)
			return nil, false
		}
	}
	if err := w.catchUp(ctx); err != nil {
		w.failures.Add(1)
		return nil, false
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.stale.Load() || f.Trigrams != "" {
		w.misses.Add(1)
		return nil, false
	}
	floor := w.floor.Load()
	whole := f.After >= floor-1 || !f.Since.IsZero() && f.Since.UnixMilli() > w.belowAt.Load()
	paged := f.Descending && f.Limit > 0
	if !whole && !paged || f.Before > 0 && f.Before <= floor {
		w.misses.Add(1)
		return nil, false
	}
	f.After = max(f.After, floor-1)
	out, err := w.mem.selected(ctx, f, projection, now)
	if err != nil {
		w.failures.Add(1)
		return nil, false
	}
	if !whole && len(out) < f.Limit || f.RepeatWindow > 0 && slices.ContainsFunc(out, func(r Record) bool {
		return r.At.Add(-f.RepeatWindow).UnixMilli() <= w.belowAt.Load()
	}) {
		w.misses.Add(1)
		return nil, false
	}
	w.hits.Add(1)
	return out, true
}

// Covers reports whether the window holds every record f can select, so f needs
// no narrowing that exists to spare the store a scan.
func (s *Store) Covers(f Filter) bool {
	w := s.window.current.Load()
	return w != nil && !w.stale.Load() && (f.After >= w.floor.Load()-1 || !f.Since.IsZero() && f.Since.UnixMilli() > w.belowAt.Load())
}

// firstSince is the first seq at or after at when the window holds it. Rows copy
// in seq order, so a row a lagging window already holds is still the first.
func (w *MemWindow) firstSince(ctx context.Context, at int64) sql.NullInt64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	var found sql.NullInt64
	if w.stale.Load() || at <= w.belowAt.Load() {
		return found
	}
	if err := w.mem.db.QueryRowContext(ctx, "SELECT MIN(seq) FROM records WHERE at >= ?", at).Scan(&found); err != nil {
		w.failures.Add(1)
		return sql.NullInt64{}
	}
	return found
}

// Answer is what the window returns for f, with whether it could answer: the
// parity harness compares it to the store's own read.
func (w *MemWindow) Answer(ctx context.Context, s *Store, f Filter) ([]Record, bool, error) {
	f, err := s.aliased(ctx, f)
	if err != nil {
		return nil, false, err
	}
	out, ok := w.answer(ctx, s, f, columns, s.Now())
	if !ok {
		return nil, false, nil
	}
	return out, true, s.numbered(ctx, out)
}

// ParityDigest sums what a replay may have changed: every cursor and every record.
func (s *Store) ParityDigest(ctx context.Context) (string, string, error) {
	var cursors, records string
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) || ':' || COALESCE(SUM(seq % 1000000007), 0) FROM cursors").Scan(&cursors)
	if err != nil {
		return "", "", fmt.Errorf("sum the cursors: %w", err)
	}
	err = s.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(seq), 0) FROM records").Scan(&records)
	if err != nil {
		return "", "", fmt.Errorf("read the newest record: %w", err)
	}
	return cursors, records, nil
}

// Reader names a lane that recently posted on a drive.
type Reader struct {
	Drive string
	Lane  string
}

// RecentReaders returns the limit lanes that posted most recently.
func (s *Store) RecentReaders(ctx context.Context, limit int) ([]Reader, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT drive, lane FROM (SELECT drive, lane, MAX(seq) AS newest FROM (SELECT drive, lane, seq FROM records ORDER BY seq DESC LIMIT 20000) GROUP BY drive, lane) ORDER BY newest DESC LIMIT ?", limit)
	if err != nil {
		return nil, fmt.Errorf("read recent lanes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Reader
	for rows.Next() {
		var r Reader
		if err := rows.Scan(&r.Drive, &r.Lane); err != nil {
			return nil, fmt.Errorf("read recent lanes: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
