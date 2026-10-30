package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

var physicalIndexes = [...]struct {
	name string
	sql  string
}{
	{"records_drive_lane", "CREATE INDEX records_drive_lane ON records(drive, lane)"},
	{"sessions_drive_lane", "CREATE INDEX sessions_drive_lane ON sessions(drive, lane)"},
	{"sessions_origin_lane", "CREATE INDEX sessions_origin_lane ON sessions(origin, lane)"},
}

const (
	deriveMemberships = `INSERT INTO record_recipients (address, source_seq) SELECT DISTINCT value, NEW.seq FROM json_each(CASE WHEN json_error_position(NEW.recipients) = 0 THEN NEW.recipients ELSE '[]' END) WHERE typeof(value) = 'text';
INSERT INTO record_also_resolves (target, source_seq) SELECT atom, NEW.seq FROM json_each(CASE WHEN json_error_position(NEW.also_resolves) = 0 THEN NEW.also_resolves ELSE '[]' END) WHERE atom IS NOT NULL;
INSERT INTO record_json_errors (field, source_seq) SELECT 'recipients', NEW.seq WHERE json_error_position(NEW.recipients) != 0 UNION ALL SELECT 'also_resolves', NEW.seq WHERE json_error_position(NEW.also_resolves) != 0;
`
	forgetMemberships = `DELETE FROM record_recipients WHERE source_seq = OLD.seq;
DELETE FROM record_also_resolves WHERE source_seq = OLD.seq;
DELETE FROM record_json_errors WHERE source_seq = OLD.seq;
`
)

var recordMemberships = [...]struct {
	kind, name, sql, fill string
}{
	{"table", "record_recipients", "CREATE TABLE record_recipients (address TEXT NOT NULL, source_seq INTEGER NOT NULL)",
		`INSERT INTO record_recipients (address, source_seq) SELECT DISTINCT j.value, r.seq FROM records r, json_each(CASE WHEN json_error_position(r.recipients) = 0 THEN r.recipients ELSE '[]' END) j WHERE typeof(j.value) = 'text'`},
	{"table", "record_also_resolves", "CREATE TABLE record_also_resolves (target NUMERIC NOT NULL, source_seq INTEGER NOT NULL)",
		`INSERT INTO record_also_resolves (target, source_seq) SELECT j.atom, r.seq FROM records r, json_each(CASE WHEN json_error_position(r.also_resolves) = 0 THEN r.also_resolves ELSE '[]' END) j WHERE j.atom IS NOT NULL`},
	{"table", "record_json_errors", "CREATE TABLE record_json_errors (field TEXT NOT NULL, source_seq INTEGER NOT NULL)",
		`INSERT INTO record_json_errors (field, source_seq) SELECT 'recipients', seq FROM records WHERE json_error_position(recipients) != 0 UNION ALL SELECT 'also_resolves', seq FROM records WHERE json_error_position(also_resolves) != 0`},
	{"index", "record_recipients_address", "CREATE INDEX record_recipients_address ON record_recipients(address, source_seq)", ""},
	{"index", "record_recipients_source", "CREATE INDEX record_recipients_source ON record_recipients(source_seq)", ""},
	{"index", "record_also_resolves_target", "CREATE INDEX record_also_resolves_target ON record_also_resolves(target, source_seq)", ""},
	{"index", "record_also_resolves_source", "CREATE INDEX record_also_resolves_source ON record_also_resolves(source_seq)", ""},
	{"index", "record_json_errors_field", "CREATE INDEX record_json_errors_field ON record_json_errors(field, source_seq)", ""},
	{"index", "record_json_errors_source", "CREATE INDEX record_json_errors_source ON record_json_errors(source_seq)", ""},
	{"index", "records_resolves_target", "CREATE INDEX records_resolves_target ON records(resolves, drive) WHERE resolves != 0", ""},
	{"trigger", "record_memberships_insert", "CREATE TRIGGER record_memberships_insert AFTER INSERT ON records\nBEGIN\n" + deriveMemberships + "END", ""},
	{"trigger", "record_memberships_update", "CREATE TRIGGER record_memberships_update AFTER UPDATE OF seq, recipients, also_resolves ON records\nWHEN OLD.seq IS NOT NEW.seq OR OLD.recipients IS NOT NEW.recipients OR OLD.also_resolves IS NOT NEW.also_resolves\nBEGIN\n" + forgetMemberships + deriveMemberships + "END", ""},
	{"trigger", "record_memberships_delete", "CREATE TRIGGER record_memberships_delete AFTER DELETE ON records\nBEGIN\n" + forgetMemberships + "END", ""},
}

// IndexMaintenanceReport describes the physical tuning and record memberships
// one maintenance committed, with storage snapshots, not peak I/O.
type IndexMaintenanceReport struct {
	Created     []string          `json:"created"`
	Memberships []string          `json:"memberships"`
	Elapsed     time.Duration     `json:"elapsed_ns"`
	Before      IndexStorageStats `json:"before"`
	After       IndexStorageStats `json:"after"`
}

// IndexStorageStats samples SQLite allocation and file sizes without measuring peak usage.
type IndexStorageStats struct {
	DatabaseBytes int64 `json:"database_bytes"`
	WALBytes      int64 `json:"wal_bytes"`
	PageCount     int64 `json:"page_count"`
	PageSize      int64 `json:"page_size"`
	FreelistCount int64 `json:"freelist_count"`
}

// MaintainIndexes explicitly tunes an initialized store and installs its
// record memberships in one transaction, failing immediately when its init
// lock or database writer is busy.
func MaintainIndexes(ctx context.Context, home string) (IndexMaintenanceReport, error) {
	started := time.Now()
	report := IndexMaintenanceReport{Created: []string{}, Memberships: []string{}}
	if err := ctx.Err(); err != nil {
		return report, err
	}
	lock, err := os.OpenFile(filepath.Join(home, "init.lock"), os.O_RDWR, 0) //nolint:gosec // G304: home is the caller-selected CCI store; init.lock is its fixed existing lock.
	if err != nil {
		return report, fmt.Errorf("open index maintenance lock: %w", err)
	}
	defer func() { _ = lock.Close() }()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return report, fmt.Errorf("take index maintenance lock: %w", err)
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()
	db, err := sql.Open("sqlite", storeDSN(home, 0)+"&mode=rw")
	if err != nil {
		return report, fmt.Errorf("open index maintenance store: %w", err)
	}
	defer func() { _ = db.Close() }()
	s := &Store{db: db, home: home}
	var schema int
	if err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&schema); err != nil {
		return report, fmt.Errorf("read index maintenance schema: %w", err)
	}
	if schema != len(migrations) {
		return report, fmt.Errorf("index maintenance requires schema %d, got %d", len(migrations), schema)
	}
	report.Before, err = s.indexStorageStats(ctx)
	if err != nil {
		return report, err
	}
	missing, err := missingPhysicalIndexes(ctx, s.db)
	if err != nil {
		return report, err
	}
	installed, err := membershipsInstalled(ctx, s.db)
	if err != nil {
		return report, err
	}
	if len(missing) > 0 || !installed {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return report, fmt.Errorf("begin index maintenance: %w", err)
		}
		defer func() { _ = tx.Rollback() }()
		if !installed {
			if err := installMemberships(ctx, tx); err != nil {
				return report, err
			}
		}
		if err := createPhysicalIndexes(ctx, tx, missing); err != nil {
			return report, err
		}
		if err := tx.Commit(); err != nil {
			return report, fmt.Errorf("commit index maintenance: %w", err)
		}
		for _, index := range missing {
			report.Created = append(report.Created, physicalIndexes[index].name)
		}
		if !installed {
			for _, object := range recordMemberships {
				report.Memberships = append(report.Memberships, object.name)
			}
		}
	}
	report.After, err = s.indexStorageStats(ctx)
	if err != nil {
		return report, err
	}
	report.Elapsed = time.Since(started)
	return report, nil
}

func missingPhysicalIndexes(ctx context.Context, db *sql.DB) ([]int, error) {
	missing := make([]int, 0, len(physicalIndexes))
	for i, index := range physicalIndexes {
		var exists bool
		if err := db.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'index' AND name = ?)", index.name).Scan(&exists); err != nil {
			return nil, fmt.Errorf("inspect physical index %s: %w", index.name, err)
		}
		if !exists {
			missing = append(missing, i)
		}
	}
	return missing, nil
}

func createPhysicalIndexes(ctx context.Context, tx *sql.Tx, indexes []int) error {
	for _, i := range indexes {
		index := physicalIndexes[i]
		if _, err := tx.ExecContext(ctx, index.sql); err != nil {
			return fmt.Errorf("create physical index %s: %w", index.name, err)
		}
	}
	return nil
}

func membershipsInstalled(ctx context.Context, q querier) (bool, error) {
	present := 0
	for _, object := range recordMemberships {
		var kind, definition string
		err := q.QueryRowContext(ctx, "SELECT type, COALESCE(sql, '') FROM sqlite_schema WHERE name = ?", object.name).Scan(&kind, &definition)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			continue
		case err != nil:
			return false, fmt.Errorf("inspect record membership %s: %w", object.name, err)
		case kind != object.kind || definition != object.sql:
			return false, fmt.Errorf("record membership %s is %s %q, not this cci's definition", object.name, kind, definition)
		}
		present++
	}
	if present != 0 && present != len(recordMemberships) {
		return false, fmt.Errorf("record memberships are partially installed: %d of %d objects", present, len(recordMemberships))
	}
	return present == len(recordMemberships), nil
}

func installMemberships(ctx context.Context, tx *sql.Tx) error {
	for _, object := range recordMemberships {
		if _, err := tx.ExecContext(ctx, object.sql); err != nil {
			return fmt.Errorf("create record membership %s: %w", object.name, err)
		}
		if object.fill == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, object.fill); err != nil {
			return fmt.Errorf("fill record membership %s: %w", object.name, err)
		}
	}
	return nil
}

func (s *Store) indexStorageStats(ctx context.Context) (IndexStorageStats, error) {
	var stats IndexStorageStats
	if err := s.db.QueryRowContext(ctx, "PRAGMA page_count").Scan(&stats.PageCount); err != nil {
		return stats, fmt.Errorf("read index storage page count: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, "PRAGMA page_size").Scan(&stats.PageSize); err != nil {
		return stats, fmt.Errorf("read index storage page size: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, "PRAGMA freelist_count").Scan(&stats.FreelistCount); err != nil {
		return stats, fmt.Errorf("read index storage freelist count: %w", err)
	}
	info, err := os.Stat(filepath.Join(s.home, DBFile))
	if err != nil {
		return stats, fmt.Errorf("stat index storage database: %w", err)
	}
	stats.DatabaseBytes = info.Size()
	info, err = os.Stat(filepath.Join(s.home, DBFile) + "-wal")
	if errors.Is(err, os.ErrNotExist) {
		return stats, nil
	}
	if err != nil {
		return stats, fmt.Errorf("stat index storage WAL: %w", err)
	}
	stats.WALBytes = info.Size()
	return stats, nil
}
