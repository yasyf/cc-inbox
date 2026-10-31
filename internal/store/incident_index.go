package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

type IncidentIndex struct {
	mu             sync.Mutex
	store          *Store
	watch          *incidentFileWatch
	owner          *os.File
	installed      bool
	authorityError error
	attempted      map[string]bool
	pending        map[string]bool
	stats          IncidentIndexStats
}

type IncidentIndexStats struct {
	BodyStats         uint64        `json:"body_stats"`
	BodyOpens         uint64        `json:"body_opens"`
	BodyBytes         uint64        `json:"body_bytes"`
	RouteStats        uint64        `json:"route_stats"`
	Watches           int           `json:"watches"`
	MaxWritePhase     time.Duration `json:"max_write_phase_ns"`
	MissingNameChecks uint64        `json:"missing_name_checks"`
}

func (index *IncidentIndex) Stats() IncidentIndexStats {
	index.mu.Lock()
	defer index.mu.Unlock()
	stats := index.stats
	if index.watch != nil {
		stats.RouteStats, stats.Watches, stats.MissingNameChecks = index.watch.stats, len(index.watch.nodes), index.watch.missingNames
	}
	return stats
}

type IncidentError struct {
	Code  string
	Cause error
}

func (e *IncidentError) Error() string { return e.Cause.Error() }
func (e *IncidentError) Unwrap() error { return e.Cause }

func OpenIncidentIndex(ctx context.Context, st *Store) (*IncidentIndex, error) {
	installed, err := incidentProjectionInstalled(ctx, st.db)
	if err != nil {
		return nil, err
	}
	index := &IncidentIndex{store: st, installed: installed, attempted: map[string]bool{}, pending: map[string]bool{}}
	if !installed {
		return index, nil
	}
	owner, err := os.OpenFile(filepath.Join(st.home, "incident-owner.lock"), os.O_RDWR, 0) //nolint:gosec // G304: this is the fixed preprovisioned lock in the caller-selected CCI home.
	if err != nil {
		return nil, &IncidentError{Code: "projection_unavailable", Cause: fmt.Errorf("open incident projection owner: %w", err)}
	}
	if err := syscall.Flock(int(owner.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = owner.Close()
		return nil, &IncidentError{Code: "projection_unavailable", Cause: fmt.Errorf("take incident projection owner: %w", err)}
	}
	index.owner = owner
	opened := false
	defer func() {
		if !opened {
			_ = index.Close()
		}
	}()
	watch, err := newIncidentFileWatch()
	if err != nil {
		return nil, err
	}
	index.watch = watch
	if err := index.reconcile(ctx); err != nil {
		return nil, err
	}
	rows, err := st.db.QueryContext(ctx, "SELECT path FROM incident_body_paths f WHERE EXISTS(SELECT 1 FROM incident_records r WHERE r.body_path=f.path) UNION SELECT path FROM incident_body_changes c WHERE EXISTS(SELECT 1 FROM incident_records r WHERE r.body_path=c.path) ORDER BY path")
	if err != nil {
		return nil, err
	}
	var paths []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			_ = rows.Close()
			return nil, err
		}
		paths = append(paths, path)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	for _, path := range paths {
		if err := index.publishBody(ctx, path); err != nil {
			return nil, err
		}
	}
	opened = true
	return index, nil
}

func (index *IncidentIndex) Close() error {
	index.mu.Lock()
	defer index.mu.Unlock()
	var result error
	if index.watch != nil {
		result = index.watch.Close()
		index.watch = nil
	}
	if index.owner != nil {
		result = errors.Join(result, index.owner.Close())
		index.owner = nil
	}
	return result
}

func incidentBodyID(identity string) string {
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:])
}

func (index *IncidentIndex) publishBody(ctx context.Context, path string) error {
	index.attempted[path] = true
	var used bool
	if err := index.store.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM incident_records WHERE body_path=?)", path).Scan(&used); err != nil {
		return err
	}
	if !used {
		if err := index.watch.Forget(path); err != nil {
			return err
		}
		return index.linkBody(ctx, path, "")
	}
	if err := index.watch.Register(path); err != nil {
		index.authorityError = err
		return &IncidentError{Code: "watch_authority_lost", Cause: err}
	}
	index.stats.BodyStats++
	info, err := os.Stat(path)
	identity := "missing"
	if err == nil {
		identity = incidentFileIdentity(info)
	} else if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, syscall.ENOTDIR) {
		identity = "error:" + path
	}
	id := incidentBodyID(identity)
	var existing bool
	if err := index.store.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM incident_body_versions WHERE id=? AND error='')", id).Scan(&existing); err != nil {
		return err
	}
	if existing {
		return index.linkBody(ctx, path, id)
	}
	body, bodyErr := readIncidentBody(ctx, path, &index.stats)
	if bodyErr == nil {
		identity, id = body.Identity, incidentBodyID(body.Identity)
	}
	tx, err := index.store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	started := time.Now()
	defer func() { index.stats.MaxWritePhase = max(index.stats.MaxWritePhase, time.Since(started)) }()
	defer func() { _ = tx.Rollback() }()
	errorText := ""
	if bodyErr != nil {
		errorText = bodyErr.Error()
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO incident_body_versions(id,identity,valid,error) VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET valid=excluded.valid,error=excluded.error", id, identity, body.Valid, errorText); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM incident_body_prs WHERE body_version=?", id); err != nil {
		return err
	}
	for pr := range body.Words {
		if _, err := tx.ExecContext(ctx, "INSERT INTO incident_body_prs(body_version,pr) VALUES(?,?)", id, pr); err != nil {
			return err
		}
	}
	if err := linkIncidentBody(ctx, tx, path, id); err != nil {
		return err
	}
	return tx.Commit()
}

func (index *IncidentIndex) linkBody(ctx context.Context, path, id string) error {
	var previous string
	err := index.store.db.QueryRowContext(ctx, "SELECT body_version FROM incident_body_paths WHERE path=?", path).Scan(&previous)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if previous == id {
		return nil
	}
	tx, err := index.store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	started := time.Now()
	defer func() { index.stats.MaxWritePhase = max(index.stats.MaxWritePhase, time.Since(started)) }()
	defer func() { _ = tx.Rollback() }()
	if err := linkIncidentBody(ctx, tx, path, id); err != nil {
		return err
	}
	return tx.Commit()
}

func linkIncidentBody(ctx context.Context, tx *sql.Tx, path, id string) error {
	var previous string
	err := tx.QueryRowContext(ctx, "SELECT body_version FROM incident_body_paths WHERE path=?", path).Scan(&previous)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if id == "" {
		if _, err := tx.ExecContext(ctx, "DELETE FROM incident_body_paths WHERE path=?", path); err != nil {
			return err
		}
	} else {
		if _, err := tx.ExecContext(ctx, "INSERT INTO incident_body_paths(path,body_version) VALUES(?,?) ON CONFLICT(path) DO UPDATE SET body_version=excluded.body_version WHERE body_version!=excluded.body_version", path, id); err != nil {
			return err
		}
	}
	if previous != "" && previous != id {
		if _, err := tx.ExecContext(ctx, "DELETE FROM incident_body_prs WHERE body_version=? AND NOT EXISTS(SELECT 1 FROM incident_body_paths WHERE body_version=?)", previous, previous); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM incident_body_versions WHERE id=? AND NOT EXISTS(SELECT 1 FROM incident_body_paths WHERE body_version=?)", previous, previous); err != nil {
			return err
		}
	}
	return nil
}

func (index *IncidentIndex) reconcile(ctx context.Context) error {
	index.attempted = map[string]bool{}
	dirty, err := index.watch.Drain(ctx)
	for path := range dirty {
		index.pending[path] = true
	}
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, syscall.EINTR) {
			return err
		}
		index.authorityError = err
		return &IncidentError{Code: "watch_authority_lost", Cause: err}
	}
	changes, err := index.store.captureIncidentChanges(ctx)
	if err != nil {
		return err
	}
	started := time.Now()
	paths, err := index.store.publishIncidentChanges(ctx, changes)
	if len(changes) != 0 {
		index.stats.MaxWritePhase = max(index.stats.MaxWritePhase, time.Since(started))
	}
	if err != nil {
		return err
	}
	rows, err := index.store.db.QueryContext(ctx, "SELECT path FROM incident_body_changes")
	if err != nil {
		return err
	}
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			_ = rows.Close()
			return err
		}
		paths[path] = true
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	for path := range paths {
		index.pending[path] = true
	}
	for path := range index.pending {
		if err := index.publishBody(ctx, path); err != nil {
			return err
		}
		if _, err := index.store.db.ExecContext(ctx, "DELETE FROM incident_body_changes WHERE path=?", path); err != nil {
			return err
		}
		delete(index.pending, path)
	}
	return nil
}

func (index *IncidentIndex) Holds(ctx context.Context, view *Store, drive string, prs []string) (map[string]Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !index.installed {
		return nil, &IncidentError{Code: "projection_unavailable", Cause: errors.New("incident projection is not provisioned")}
	}
	if index.authorityError != nil || index.watch == nil {
		return nil, &IncidentError{Code: "watch_authority_lost", Cause: errors.New("incident watch authority is unavailable")}
	}
	var ready bool
	err := index.store.db.QueryRowContext(ctx, "SELECT ready FROM incident_scopes WHERE drive=?", drive).Scan(&ready)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, &IncidentError{Code: "scope_not_admitted", Cause: errors.New("incident scope is not admitted")}
	}
	if err != nil {
		return nil, err
	}
	if !ready {
		return nil, &IncidentError{Code: "projection_unavailable", Cause: errors.New("incident scope provisioning is incomplete")}
	}
	if err := index.reconcile(ctx); err != nil {
		return nil, err
	}
	var unsupported bool
	if err := index.store.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM incident_records WHERE drive=? AND unsupported_path!=0)", drive).Scan(&unsupported); err != nil {
		return nil, err
	}
	if unsupported {
		return nil, &IncidentError{Code: "scope_not_admitted", Cause: errors.New("incident scope contains relative body paths")}
	}
	now := view.Now().UnixMilli()
	var decodeError string
	err = index.store.db.QueryRowContext(ctx, "SELECT decode_error FROM incident_records WHERE drive=? AND decode_error!='' AND (expires_at IS NULL OR expires_at>?) ORDER BY seq DESC LIMIT 1", drive, now).Scan(&decodeError)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if decodeError != "" {
		return nil, errors.New(decodeError)
	}
	if err := index.checkBodyErrors(ctx, view, drive, now); err != nil {
		return nil, err
	}
	out := map[string]Record{}
	if len(prs) == 0 {
		return out, nil
	}
	wanted, _ := json.Marshal(prs)
	rows, err := index.store.db.QueryContext(ctx, incidentMatchesSQL, string(wanted), drive, now)
	if err != nil {
		return nil, err
	}
	bySeq := map[int64][]string{}
	snapshot := map[int64]Record{}
	var seqs []int64
	for rows.Next() {
		var pr string
		record, err := view.scan(&incidentMatchScanner{row: rows, pr: &pr})
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		if len(bySeq[record.Seq]) == 0 {
			seqs = append(seqs, record.Seq)
			snapshot[record.Seq] = record
		}
		bySeq[record.Seq] = append(bySeq[record.Seq], pr)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	if len(seqs) == 0 {
		return out, nil
	}
	open, err := view.OpenAmong(ctx, drive, seqs)
	if err != nil {
		return nil, err
	}
	best := map[string]int64{}
	for _, record := range open {
		for _, pr := range bySeq[record.Seq] {
			best[pr] = max(best[pr], record.Seq)
		}
	}
	selected := map[int64]bool{}
	var result []Record
	for _, pr := range prs {
		seq := best[pr]
		if seq != 0 && !selected[seq] {
			selected[seq] = true
			result = append(result, snapshot[seq])
		}
	}
	if err := view.numbered(ctx, result); err != nil {
		return nil, err
	}
	for _, record := range result {
		record.Status = "open"
		for _, pr := range bySeq[record.Seq] {
			if best[pr] == record.Seq {
				out[pr] = record
			}
		}
	}
	return out, nil
}

type incidentMatchScanner struct {
	row scanner
	pr  *string
}

func (row *incidentMatchScanner) Scan(dest ...any) error {
	return row.row.Scan(append([]any{row.pr}, dest...)...)
}

func (index *IncidentIndex) checkBodyErrors(ctx context.Context, view *Store, drive string, now int64) error {
	rows, err := index.store.db.QueryContext(ctx, `SELECT r.seq,f.path,v.error FROM incident_body_versions v INDEXED BY incident_body_versions_errors
JOIN incident_body_paths f ON f.body_version=v.id JOIN incident_records r ON r.body_path=f.path
WHERE v.error!='' AND r.drive=? AND (r.expires_at IS NULL OR r.expires_at>?) ORDER BY r.seq DESC`, drive, now)
	if err != nil {
		return err
	}
	var seqs []int64
	errorsBySeq := map[int64]string{}
	pathsBySeq := map[int64]string{}
	for rows.Next() {
		var seq int64
		var path, message string
		if err := rows.Scan(&seq, &path, &message); err != nil {
			_ = rows.Close()
			return err
		}
		seqs = append(seqs, seq)
		errorsBySeq[seq] = message
		pathsBySeq[seq] = path
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	if len(seqs) == 0 {
		return nil
	}
	open, err := view.OpenAmong(ctx, drive, seqs)
	if err != nil {
		return err
	}
	for _, record := range open {
		path := pathsBySeq[record.Seq]
		message := errorsBySeq[record.Seq]
		if !index.attempted[path] {
			if err := index.publishBody(ctx, path); err != nil {
				return err
			}
			if err := index.store.db.QueryRowContext(ctx, "SELECT v.error FROM incident_body_paths f JOIN incident_body_versions v ON v.id=f.body_version WHERE f.path=?", path).Scan(&message); err != nil {
				return err
			}
		}
		if message != "" {
			return fmt.Errorf("incident body: %s", message)
		}
	}
	return nil
}

var incidentMatchesSQL = `WITH wanted AS MATERIALIZED(SELECT value AS pr FROM json_each(?)),
matches AS (
SELECT p.pr,p.source_seq AS seq FROM wanted w CROSS JOIN incident_record_prs p ON p.pr=w.pr AND p.axis='refs'
UNION
SELECT p.pr,p.source_seq FROM wanted w CROSS JOIN incident_record_prs p ON p.pr=w.pr AND p.axis='summary'
JOIN incident_records r ON r.seq=p.source_seq LEFT JOIN incident_body_paths f ON f.path=r.body_path LEFT JOIN incident_body_versions v ON v.id=f.body_version
WHERE r.body_path='' OR v.valid=0
UNION
SELECT p.pr,r.seq FROM wanted w CROSS JOIN incident_body_prs p ON p.pr=w.pr
JOIN incident_body_paths f ON f.body_version=p.body_version JOIN incident_records r ON r.body_path=f.path
JOIN incident_body_versions v ON v.id=p.body_version WHERE v.valid=1
)
SELECT m.pr,r.` + strings.ReplaceAll(incidentRecordColumns, ",", ",r.") + ` FROM matches m JOIN incident_records r ON r.seq=m.seq WHERE r.drive=? AND (r.expires_at IS NULL OR r.expires_at>?) ORDER BY r.seq DESC`
