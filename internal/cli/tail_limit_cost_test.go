package cli_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"

	"github.com/yasyf/cc-inbox/internal/store"
)

func TestTailNewestLimitDoesNotWalkSameDriveHistory(t *testing.T) {
	for _, population := range []int{32, 10000} {
		t.Run(fmt.Sprintf("rows=%d", population), func(t *testing.T) {
			var comparisons atomic.Int64
			collation := fmt.Sprintf("cci_tail_drive_%d", tailCounterID.Add(1))
			if err := sqlite.RegisterCollationUtf8(collation, func(a, b string) int {
				comparisons.Add(1)
				return strings.Compare(a, b)
			}); err != nil {
				t.Fatal(err)
			}
			db := tailLimitComparedStore(t, collation)
			if _, err := db.Exec(`WITH RECURSIVE history(n) AS (
VALUES(1) UNION ALL SELECT n + 1 FROM history WHERE n < ?
)
INSERT INTO records (drive,lane,kind,at,text,source,hash)
SELECT 'durable-agents-rebuild','lane','note',?,'selected','post','same-drive-' || n FROM history`, population, time.Now().UnixMilli()); err != nil {
				t.Fatal(err)
			}
			comparisons.Store(0)
			out, err := run(t, "tail", "--drive", "durable-agents-rebuild", "--since", "36h", "-n", "1", "--json", "--budget", "300000000")
			if err != nil {
				t.Fatal(err)
			}
			tailCostOutput(t, out, []int64{int64(population)}, "")
			got := comparisons.Load()
			t.Logf("actual_tail same_drive_rows=%d requested_rows=1 output_rows=1 native_drive_comparisons=%d", population, got)
			if got == 0 || got > 64 {
				t.Fatalf("tail compared drive %d times for %d same-drive rows and one requested row, want 1..64", got, population)
			}
		})
	}
}

func tailLimitComparedStore(t *testing.T, collation string) *sql.DB {
	t.Helper()
	ctx := context.Background()
	productionHome := t.TempDir()
	st, err := store.Open(ctx, productionHome)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	production, err := sql.Open("sqlite", filepath.Join(productionHome, store.DBFile))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = production.Close() })
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "tail-limit-session")
	t.Setenv("CODEX_THREAD_ID", "")
	t.Setenv("CCI_AGENT_ID", "")
	db, err := sql.Open("sqlite", filepath.Join(home, store.DBFile))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	definitions, err := production.QueryContext(ctx, `SELECT type, name, sql FROM sqlite_schema
WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'
ORDER BY CASE type WHEN 'table' THEN 0 WHEN 'index' THEN 1 WHEN 'trigger' THEN 2 ELSE 3 END, name`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = definitions.Close() }()
	for definitions.Next() {
		var kind, name, definition string
		if err := definitions.Scan(&kind, &name, &definition); err != nil {
			t.Fatal(err)
		}
		if kind == "table" && name == "records" {
			definition = strings.Replace(definition, "drive TEXT", "drive TEXT COLLATE "+collation, 1)
		}
		if _, err := db.ExecContext(ctx, definition); err != nil {
			t.Fatal(err)
		}
	}
	if err := definitions.Err(); err != nil {
		t.Fatal(err)
	}
	var schema int
	if err := production.QueryRowContext(ctx, "PRAGMA user_version").Scan(&schema); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode = WAL"); err != nil {
		t.Fatal(err)
	}
	return db
}
