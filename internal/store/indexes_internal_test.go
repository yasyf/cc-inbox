package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"modernc.org/sqlite"
)

func TestExistingStoreOpenDoesNotBuildPhysicalIndexes(t *testing.T) {
	ctx := context.Background()
	home := t.TempDir()
	st, err := Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	dropLaneIndexes(t, st)
	seedLaneHistory(t, st, 64)
	fingerprint := st.SchemaFingerprint()
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	other, err := Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close() })
	if other.SchemaFingerprint() != fingerprint {
		t.Fatal("Open changed logical fingerprint")
	}
	assertLaneIndexCount(t, other, 0)
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	registered, err := other.Registered(ctx, "drive", "old-lane")
	if err != nil || !registered {
		t.Fatalf("unindexed Registered = %v, %v", registered, err)
	}
}

func TestMaintainIndexesPreservesLogicalStore(t *testing.T) {
	for _, history := range []int{64, 4096, 65536} {
		t.Run(fmt.Sprint(history), func(t *testing.T) {
			ctx := context.Background()
			st, err := Open(ctx, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			dropLaneIndexes(t, st)
			seedLaneHistory(t, st, history)
			fingerprint := st.SchemaFingerprint()
			report, err := MaintainIndexes(ctx, st.home)
			if err != nil {
				t.Fatal(err)
			}
			if len(report.Created) != 2 {
				t.Fatalf("created indexes = %v, want both", report.Created)
			}
			assertLaneIndexCount(t, st, 2)
			var schema, rows int
			if err := st.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&schema); err != nil {
				t.Fatal(err)
			}
			if err := st.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&rows); err != nil {
				t.Fatal(err)
			}
			if schema != 14 || rows != history || st.SchemaFingerprint() != fingerprint {
				t.Fatalf("maintenance changed logical state: schema=%d rows=%d fingerprint=%s", schema, rows, st.SchemaFingerprint())
			}
			for _, tc := range []struct {
				drive, lane string
				want        bool
			}{
				{"drive", "old-lane", true},
				{"drive", "missing", false},
				{"other", "old-lane", false},
			} {
				got, err := st.Registered(ctx, tc.drive, tc.lane)
				if err != nil || got != tc.want {
					t.Fatalf("Registered(%q,%q) = %v, %v; want %v", tc.drive, tc.lane, got, err, tc.want)
				}
			}
			assertRegisteredPlan(t, st)
			tx, err := st.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = tx.Rollback() })
			repeated, err := MaintainIndexes(ctx, st.home)
			if err != nil || len(repeated.Created) != 0 {
				t.Fatalf("indexed maintenance under ordinary writer = %+v, %v", repeated, err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(report)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("rows=%d actual_index_build_report=%s", history, encoded)
		})
	}
}

func TestMaintainIndexesRejectsNewerLogicalSchema(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	dropLaneIndexes(t, st)
	if _, err := st.db.ExecContext(ctx, "PRAGMA user_version = 15"); err != nil {
		t.Fatal(err)
	}
	if _, err := MaintainIndexes(ctx, st.home); err == nil {
		t.Fatal("maintenance accepted newer logical schema")
	}
	assertLaneIndexCount(t, st, 0)
}

func TestMaintainIndexesFailsBusyWithoutDDL(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	dropLaneIndexes(t, st)
	lock, err := os.OpenFile(filepath.Join(st.home, "init.lock"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lock.Close() })
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) })
	if _, err := MaintainIndexes(ctx, st.home); err == nil {
		t.Fatal("maintenance crossed active initialization barrier")
	}
	assertLaneIndexCount(t, st, 0)
}

func TestMaintainIndexesRollsBackCancelledBuild(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var building atomic.Bool
	var interrupted atomic.Bool
	collation := fmt.Sprintf("cci_cancel_lane_%d", laneCollationID.Add(1))
	if err := sqlite.RegisterCollationUtf8(collation, func(a, b string) int {
		if building.Load() && interrupted.CompareAndSwap(false, true) {
			cancel()
		}
		return strings.Compare(a, b)
	}); err != nil {
		t.Fatal(err)
	}
	st := countedLaneStore(t, collation)
	var dbPath string
	if err := st.db.QueryRowContext(context.Background(), "SELECT file FROM pragma_database_list WHERE name = 'main'").Scan(&dbPath); err != nil {
		t.Fatal(err)
	}
	st.home = filepath.Dir(dbPath)
	if err := os.WriteFile(filepath.Join(st.home, "init.lock"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"PRAGMA journal_mode = WAL", "PRAGMA user_version = 14"} {
		if _, err := st.db.ExecContext(context.Background(), query); err != nil {
			t.Fatal(err)
		}
	}
	dropLaneIndexes(t, st)
	seedLaneHistory(t, st, 4096)
	building.Store(true)
	if _, err := MaintainIndexes(ctx, st.home); err == nil {
		t.Fatal("cancelled index build succeeded")
	}
	if !interrupted.Load() {
		t.Fatal("test did not cancel an active index build")
	}
	assertLaneIndexCount(t, st, 0)
}

func dropLaneIndexes(t *testing.T, st *Store) {
	t.Helper()
	for _, name := range []string{"records_drive_lane", "sessions_drive_lane"} {
		if _, err := st.db.ExecContext(context.Background(), "DROP INDEX IF EXISTS "+name); err != nil {
			t.Fatal(err)
		}
	}
}

func assertLaneIndexCount(t *testing.T, st *Store, want int) {
	t.Helper()
	var got int
	if err := st.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM sqlite_schema
WHERE type = 'index' AND name IN ('records_drive_lane', 'sessions_drive_lane')`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("physical index count = %d, want %d", got, want)
	}
}
