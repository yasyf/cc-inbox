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
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

type BoardSpec struct {
	Drive   string    `json:"drive"`
	Repo    string    `json:"repo"`
	CWD     string    `json:"cwd"`
	GoLanes []string  `json:"go_lanes"`
	Filters [3]Filter `json:"filters"`
}

type BoardSelection struct {
	PRs    []string
	RowIDs []string
}

type BoardCategory struct {
	Error      error
	Records    []Record
	Bodies     map[int64][]byte
	BodyFiles  map[int64]bool
	BodyErrors map[int64]string
	Lanes      []string
}

type BoardResult struct {
	ScopeID     string
	Revision    int64
	Clock       time.Time
	ErrorReplay bool
	Rulings     BoardCategory
	Withdrawals BoardCategory
	Reviews     BoardCategory
}

type boardScope struct {
	id         string
	spec       BoardSpec
	watch      *incidentFileWatch
	incomplete error
	pending    map[string]bool
}

type BoardIndex struct {
	mu     sync.Mutex
	store  *Store
	owner  *os.File
	scopes map[string]*boardScope
}

func NewBoardIndex(st *Store) *BoardIndex {
	return &BoardIndex{store: st, scopes: map[string]*boardScope{}}
}

func boardScopeID(spec BoardSpec) (string, string, error) {
	data, err := json.Marshal(spec)
	if err != nil {
		return "", "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), string(data), nil
}

func (index *BoardIndex) Close() error {
	index.mu.Lock()
	defer index.mu.Unlock()
	var err error
	for _, scope := range index.scopes {
		err = errors.Join(err, scope.watch.Close())
	}
	index.scopes = map[string]*boardScope{}
	if index.owner != nil {
		err = errors.Join(err, index.owner.Close())
		index.owner = nil
	}
	return err
}

func (index *BoardIndex) authority(ctx context.Context) error {
	installed, err := boardProjectionInstalled(ctx, index.store.db)
	if err != nil {
		return err
	}
	if !installed {
		return errors.New("board query namespace is not provisioned")
	}
	if index.owner != nil {
		return nil
	}
	owner, err := os.OpenFile(filepath.Join(index.store.home, "board-query-owner.lock"), os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if err := syscall.Flock(int(owner.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = owner.Close()
		return err
	}
	index.owner = owner
	return nil
}

func (index *BoardIndex) Provision(ctx context.Context, spec BoardSpec) (string, error) {
	index.mu.Lock()
	defer index.mu.Unlock()
	if err := index.authority(ctx); err != nil {
		return "", err
	}
	id, encoded, err := boardScopeID(spec)
	if err != nil {
		return "", err
	}
	if old := index.scopes[id]; old != nil {
		_ = old.watch.Close()
		delete(index.scopes, id)
	}
	watch, err := newIncidentFileWatch()
	if err != nil {
		return "", err
	}
	scope := &boardScope{id: id, spec: spec, watch: watch, pending: map[string]bool{}}
	if err := index.reconcile(ctx, scope, encoded, true, index.store.Now()); err != nil {
		_ = watch.Close()
		return "", err
	}
	dirty, err := watch.DrainBoard(ctx)
	if err != nil || len(dirty) != 0 {
		_ = watch.Close()
		return "", errors.Join(err, errors.New("board provision did not reach a common cut"))
	}
	index.scopes[id] = scope
	return id, nil
}

func (index *BoardIndex) Read(ctx context.Context, view *Store, spec BoardSpec, selection BoardSelection, consume func(*BoardSnapshot, *BoardResult) error) error {
	index.mu.Lock()
	defer index.mu.Unlock()
	id, _, err := boardScopeID(spec)
	if err != nil {
		return err
	}
	scope := index.scopes[id]
	if scope == nil {
		return errors.New("board query scope requires explicit live provisioning")
	}
	if scope.incomplete != nil {
		return scope.incomplete
	}
	if err := index.authority(ctx); err != nil {
		return err
	}
	now := index.store.Now()
	if err := index.reconcile(ctx, scope, "", false, now); err != nil {
		scope.incomplete = err
		return err
	}
	tx, err := index.store.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var revision, applied int64
	if err := tx.QueryRowContext(ctx, "SELECT m.revision,s.applied FROM board_query_meta m JOIN board_query_scopes s ON s.id=? WHERE m.id=1", id).Scan(&revision, &applied); err != nil {
		return err
	}
	if revision != applied {
		return errors.New("board canonical records changed before the read snapshot")
	}
	snapshot := &BoardSnapshot{store: view, tx: tx, now: now}
	result := &BoardResult{ScopeID: id, Revision: revision, Clock: now}
	if err := index.selectResult(ctx, scope, snapshot, selection, result); err != nil {
		return err
	}
	if err := consume(snapshot, result); err != nil {
		return err
	}
	if result.ErrorReplay {
		var current int64
		if err := index.store.db.QueryRowContext(ctx, "SELECT revision FROM board_query_meta WHERE id=1").Scan(&current); err != nil {
			return err
		}
		if current != revision {
			return errors.New("board canonical records changed during known-error replay")
		}
	}
	dirty, err := scope.watch.DrainBoard(ctx)
	if err != nil {
		scope.incomplete = err
		return err
	}
	if len(dirty) != 0 {
		for path := range dirty {
			scope.pending[path] = true
		}
		return errors.New("board bodies changed across the read snapshot")
	}
	return ctx.Err()
}

func (index *BoardIndex) reconcile(ctx context.Context, scope *boardScope, encoded string, cold bool, now time.Time) error {
	dirty, err := scope.watch.DrainBoard(ctx)
	if err != nil {
		return err
	}
	for path := range scope.pending {
		dirty[path] = true
	}
	tx, err := index.store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var revision int64
	if err := tx.QueryRowContext(ctx, "SELECT revision FROM board_query_meta WHERE id=1").Scan(&revision); err != nil {
		return err
	}
	var applied int64
	if cold {
		for _, table := range []string{"board_query_keys", "board_query_rows", "board_query_bodies", "board_query_openers", "board_query_effects", "board_query_lanes"} {
			if _, err := tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE scope=?", scope.id); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO board_query_scopes(id,spec,applied,clock_ms) VALUES(?,?,0,?) ON CONFLICT(id) DO UPDATE SET spec=excluded.spec,applied=0,clock_ms=excluded.clock_ms", scope.id, encoded, now.UnixMilli()); err != nil {
			return err
		}
	} else if err := tx.QueryRowContext(ctx, "SELECT applied FROM board_query_scopes WHERE id=?", scope.id).Scan(&applied); err != nil {
		return err
	}
	clockChanged := false
	if !cold {
		clockChanged, err = boardRefreshClock(ctx, tx, scope.id, now.UnixMilli())
		if err != nil {
			return err
		}
	}
	query := "SELECT seq FROM board_query_changes WHERE revision>? AND revision<=? ORDER BY seq"
	args := []any{applied, revision}
	if cold {
		query = "SELECT seq FROM records ORDER BY seq"
		args = nil
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	var changed []int64
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			_ = rows.Close()
			return err
		}
		changed = append(changed, seq)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	if !cold && len(changed) == 0 && len(dirty) == 0 && !clockChanged {
		return nil
	}
	affected := map[[2]int64]bool{}
	for _, seq := range changed {
		if err := boardChangedEffects(ctx, tx, scope, seq, affected); err != nil {
			return err
		}
		old, err := tx.QueryContext(ctx, "SELECT DISTINCT path FROM board_query_rows WHERE scope=? AND seq=? AND path!=''", scope.id, seq)
		if err != nil {
			return err
		}
		for old.Next() {
			var path string
			if err := old.Scan(&path); err != nil {
				_ = old.Close()
				return err
			}
			dirty[path] = true
		}
		if err := errors.Join(old.Err(), old.Close()); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM board_query_keys WHERE scope=? AND seq=?", scope.id, seq); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM board_query_rows WHERE scope=? AND seq=?", scope.id, seq); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM board_query_openers WHERE scope=? AND seq=?", scope.id, seq); err != nil {
			return err
		}
		for category, filter := range scope.spec.Filters {
			filter.IncludeExpired = true
			where, params := filter.where(index.store.Self(), index.store.Now())
			params = append([]any{seq}, params...)
			selected, queryErr := tx.QueryContext(ctx, "SELECT "+columns+" FROM records WHERE seq=? AND ("+where+")", params...)
			var record Record
			var present bool
			if queryErr == nil {
				if selected.Next() {
					present = true
					record, queryErr = index.store.scan(selected)
				}
				queryErr = errors.Join(queryErr, selected.Err(), selected.Close())
			}
			if queryErr != nil {
				var expiry sql.NullInt64
				if err := tx.QueryRowContext(ctx, "SELECT expires_at FROM records WHERE seq=?", seq).Scan(&expiry); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, "INSERT INTO board_query_rows(scope,category,seq,path,lane,expires_at,error,active) VALUES(?,?,?,'','',?,?,?)", scope.id, category, seq, expiry, queryErr.Error(), !expiry.Valid || expiry.Int64 > now.UnixMilli()); err != nil {
					return err
				}
				continue
			}
			if !present {
				continue
			}
			path := ""
			if (category == 1 || category == 0 && boardRootEligible(scope, record)) && record.Refs.Path != "" {
				path = record.Refs.Path
				if !filepath.IsAbs(path) {
					path = scope.spec.CWD + string(filepath.Separator) + path
				}
				dirty[path] = true
			}
			var expiry any
			if record.ExpiresAt != nil {
				expiry = record.ExpiresAt.UnixMilli()
			}
			if _, err := tx.ExecContext(ctx, "INSERT INTO board_query_rows(scope,category,seq,path,lane,expires_at,error,active) VALUES(?,?,?,?,?,?,'',?)", scope.id, category, seq, path, record.Lane, expiry, record.ExpiresAt == nil || record.ExpiresAt.After(now)); err != nil {
				return err
			}
			if err := boardOpenerKeys(ctx, tx, scope, category, record); err != nil {
				return err
			}
			affected[[2]int64{int64(category), seq}] = true
			if err := boardPutKeys(ctx, tx, scope, category, record, nil); err != nil {
				return err
			}
		}
	}
	if err := boardAnnotateErrors(ctx, tx, index.store, scope, affected); err != nil {
		return err
	}
	paths := make([]string, 0, len(dirty))
	for path := range dirty {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		var used bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM board_query_rows WHERE scope=? AND path=?)", scope.id, path).Scan(&used); err != nil {
			return err
		}
		if !used {
			if err := scope.watch.Forget(path); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, "DELETE FROM board_query_bodies WHERE scope=? AND path=?", scope.id, path); err != nil {
				return err
			}
			continue
		}
		if scope.watch.rebind[path] {
			if err := scope.watch.Forget(path); err != nil {
				return err
			}
		}
		if err := scope.watch.Register(path); err != nil {
			return err
		}
		info, statErr := os.Stat(path)
		regular := statErr == nil && info.Mode().IsRegular()
		body, readErr := os.ReadFile(path)
		message := ""
		if readErr != nil {
			body = nil
			message = readErr.Error()
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO board_query_bodies(scope,path,body,regular,error) VALUES(?,?,?,?,?) ON CONFLICT(scope,path) DO UPDATE SET body=excluded.body,regular=excluded.regular,error=excluded.error", scope.id, path, body, regular, message); err != nil {
			return err
		}
		linked, err := tx.QueryContext(ctx, "SELECT seq FROM board_query_rows WHERE scope=? AND path=? AND category=0 ORDER BY seq", scope.id, path)
		if err != nil {
			return err
		}
		var seqs []int64
		for linked.Next() {
			var seq int64
			if err := linked.Scan(&seq); err != nil {
				_ = linked.Close()
				return err
			}
			seqs = append(seqs, seq)
		}
		if err := errors.Join(linked.Err(), linked.Close()); err != nil {
			return err
		}
		for _, seq := range seqs {
			record, err := index.store.scan(tx.QueryRowContext(ctx, "SELECT "+columns+" FROM records WHERE seq=?", seq))
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, "DELETE FROM board_query_keys WHERE scope=? AND category=0 AND seq=?", scope.id, seq); err != nil {
				return err
			}
			if err := boardPutKeys(ctx, tx, scope, 0, record, body); err != nil {
				return err
			}
		}
	}
	if _, err := tx.ExecContext(ctx, "UPDATE board_query_scopes SET applied=?,clock_ms=? WHERE id=?", revision, now.UnixMilli(), scope.id); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	clear(scope.pending)
	return nil
}

func (index *BoardIndex) selectResult(ctx context.Context, scope *boardScope, snapshot *BoardSnapshot, wanted BoardSelection, result *BoardResult) error {
	categories := []*BoardCategory{&result.Rulings, &result.Reviews, &result.Withdrawals}
	for category, out := range categories {
		out.Bodies = map[int64][]byte{}
		out.BodyFiles = map[int64]bool{}
		out.BodyErrors = map[int64]string{}
		out.Records = []Record{}
		out.Lanes = []string{}
		var message string
		err := snapshot.tx.QueryRowContext(ctx, "SELECT CASE WHEN error!='' THEN error ELSE annotation_error END FROM board_query_rows WHERE scope=? AND category=? AND active=1 AND (error!='' OR annotation_error!='') ORDER BY seq LIMIT 1", scope.id, category).Scan(&message)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			out.Error = errors.New(message)
		}
	}
	for category, out := range categories {
		if out.Error != nil {
			continue
		}
		selected := map[int64]bool{}
		lookup := func(axis, key string, prefix bool) error {
			sqlText := "SELECT seq FROM board_query_keys WHERE scope=? AND category=? AND axis=? AND key=?"
			params := []any{scope.id, category, axis, key}
			if prefix {
				sqlText = "SELECT seq FROM board_query_keys WHERE scope=? AND category=? AND axis=? AND key>=? AND key<?"
				params = append(params, key+"\U0010ffff")
			}
			rows, err := snapshot.tx.QueryContext(ctx, sqlText, params...)
			if err != nil {
				return err
			}
			for rows.Next() {
				var seq int64
				if err := rows.Scan(&seq); err != nil {
					_ = rows.Close()
					return err
				}
				selected[seq] = true
			}
			return errors.Join(rows.Err(), rows.Close())
		}
		if category == 2 {
			for _, r := range result.Rulings.Records {
				if err := lookup("close", fmt.Sprint(r.Seq), false); err != nil {
					return err
				}
			}
		} else {
			for _, pr := range wanted.PRs {
				if err := lookup("pr", pr, false); err != nil {
					return err
				}
			}
			if category == 0 {
				for _, id := range wanted.RowIDs {
					if err := lookup("row", id, true); err != nil {
						return err
					}
				}
			}
		}
		seqs := make([]int64, 0, len(selected))
		for seq := range selected {
			seqs = append(seqs, seq)
		}
		slices.Sort(seqs)
		records, err := snapshot.Records(ctx, seqs)
		if err != nil {
			out.Error = err
			continue
		}
		out.Records = records
		if category != 2 {
			for _, r := range records {
				var body []byte
				var regular bool
				var message string
				err := snapshot.tx.QueryRowContext(ctx, "SELECT b.body,b.regular,b.error FROM board_query_rows r JOIN board_query_bodies b ON b.scope=r.scope AND b.path=r.path WHERE r.scope=? AND r.category=? AND r.seq=?", scope.id, category, r.Seq).Scan(&body, &regular, &message)
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				out.Bodies[r.Seq] = body
				out.BodyFiles[r.Seq] = regular
				out.BodyErrors[r.Seq] = message
			}
		}
		if category == 1 {
			rows, err := snapshot.tx.QueryContext(ctx, "SELECT lane FROM board_query_lanes WHERE scope=? AND n>0 ORDER BY lane", scope.id)
			if err != nil {
				return err
			}
			for rows.Next() {
				var lane string
				if err := rows.Scan(&lane); err != nil {
					_ = rows.Close()
					return err
				}
				out.Lanes = append(out.Lanes, lane)
			}
			if err := errors.Join(rows.Err(), rows.Close()); err != nil {
				return err
			}
		}
	}
	if result.Rulings.Error != nil || result.Withdrawals.Error != nil {
		result.Rulings.Records = []Record{}
		result.Withdrawals.Records = []Record{}
	}
	return nil
}

func boardText(body []byte) string {
	text := strings.ToValidUTF8(string(body), "\ufffd")
	return strings.ReplaceAll(strings.ReplaceAll(text, "\r\n", "\n"), "\r", "\n")
}
