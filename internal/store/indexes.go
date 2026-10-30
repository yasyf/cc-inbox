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

// IndexMaintenanceReport describes committed physical tuning and storage snapshots, not peak I/O.
type IndexMaintenanceReport struct {
	Created []string          `json:"created"`
	Elapsed time.Duration     `json:"elapsed_ns"`
	Before  IndexStorageStats `json:"before"`
	After   IndexStorageStats `json:"after"`
}

// IndexStorageStats samples SQLite allocation and file sizes without measuring peak usage.
type IndexStorageStats struct {
	DatabaseBytes int64 `json:"database_bytes"`
	WALBytes      int64 `json:"wal_bytes"`
	PageCount     int64 `json:"page_count"`
	PageSize      int64 `json:"page_size"`
	FreelistCount int64 `json:"freelist_count"`
}

// MaintainIndexes explicitly tunes an initialized store, failing immediately when its init lock is busy.
func MaintainIndexes(ctx context.Context, home string) (IndexMaintenanceReport, error) {
	started := time.Now()
	report := IndexMaintenanceReport{Created: []string{}}
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
	if len(missing) > 0 {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return report, fmt.Errorf("begin index maintenance: %w", err)
		}
		defer func() { _ = tx.Rollback() }()
		if err := createPhysicalIndexes(ctx, tx, missing); err != nil {
			return report, err
		}
		if err := tx.Commit(); err != nil {
			return report, fmt.Errorf("commit index maintenance: %w", err)
		}
		for _, index := range missing {
			report.Created = append(report.Created, physicalIndexes[index].name)
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
