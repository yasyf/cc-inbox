package cli_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"modernc.org/sqlite"

	"github.com/yasyf/cc-inbox/internal/store"
)

var grepCounterID atomic.Uint64

func TestGrepReadsHistoryOnceForAnnotations(t *testing.T) {
	for _, population := range []int{32, 10000} {
		for _, budget := range []int{6144, 4 << 20} {
			t.Run(fmt.Sprintf("rows=%d/budget=%d", population, budget), func(t *testing.T) {
				home := t.TempDir()
				t.Setenv("CCI_HOME", home)
				t.Setenv("CLAUDE_CODE_SESSION_ID", "grep-cost-session")
				t.Setenv("CCI_AGENT_ID", "")
				ctx := context.Background()
				st, err := store.Open(ctx, home)
				if err != nil {
					t.Fatal(err)
				}
				if err := st.Close(); err != nil {
					t.Fatal(err)
				}
				db, err := sql.Open("sqlite", filepath.Join(home, store.DBFile))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				if _, err := db.ExecContext(ctx, `WITH RECURSIVE history(n) AS (
VALUES(1) UNION ALL SELECT n + 1 FROM history WHERE n < ?
)
INSERT INTO records (drive, lane, kind, at, text, source, hash)
SELECT 'durable-agents-rebuild', 'reviewer', 'note', n, 'REVIEW history ' || n, 'post', 'history-' || n FROM history`, population); err != nil {
					t.Fatal(err)
				}
				var reads atomic.Int64
				function := fmt.Sprintf("cci_grep_read_%d", grepCounterID.Add(1))
				if err := sqlite.RegisterScalarFunction(function, 1, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
					reads.Add(1)
					return args[0], nil
				}); err != nil {
					t.Fatal(err)
				}
				columns, err := db.QueryContext(ctx, "PRAGMA table_info(records)")
				if err != nil {
					t.Fatal(err)
				}
				var selection []string
				for columns.Next() {
					var cid, notNull, primaryKey int
					var name, kind string
					var defaultValue any
					if err := columns.Scan(&cid, &name, &kind, &notNull, &defaultValue, &primaryKey); err != nil {
						t.Fatal(err)
					}
					column := name
					if name == "also_resolves" {
						column = function + "(also_resolves) AS also_resolves"
					}
					selection = append(selection, column)
				}
				if err := columns.Err(); err != nil {
					t.Fatal(err)
				}
				if err := columns.Close(); err != nil {
					t.Fatal(err)
				}
				for _, query := range []string{
					"ALTER TABLE records RENAME TO counted_history",
					"CREATE VIEW records AS SELECT " + strings.Join(selection, ", ") + " FROM counted_history",
				} {
					if _, err := db.ExecContext(ctx, query); err != nil {
						t.Fatal(err)
					}
				}
				out, err := run(t, "grep", "REVIEW", "--drive", "durable-agents-rebuild", "--budget", fmt.Sprint(budget))
				if err != nil {
					t.Fatal(err)
				}
				if !strings.HasPrefix(out, fmt.Sprintf("#%d ", population)) {
					t.Fatalf("grep newest row missing: %q", out)
				}
				if got, want := reads.Load(), 2*int64(population); got != want {
					t.Fatalf("actual CLI read also_resolves %d times, want %d (one candidate scan and one annotation scan)", got, want)
				}
				if budget > 6144 && strings.Count(out, "\n") != population {
					t.Fatalf("wide budget printed %d records, want %d", strings.Count(out, "\n"), population)
				}
				t.Logf("rows=%d budget=%d actual_cli_history_column_reads=%d candidate_rows=%d annotation_history_rows=%d", population, budget, reads.Load(), population, reads.Load()-int64(population))
			})
		}
	}
}
