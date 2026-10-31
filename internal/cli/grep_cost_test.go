package cli_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
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
				var reads atomic.Int64
				function := fmt.Sprintf("cci_grep_read_%d", grepCounterID.Add(1))
				if err := sqlite.RegisterDeterministicScalarFunction(function, 1, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
					reads.Add(1)
					return args[0], nil
				}); err != nil {
					t.Fatal(err)
				}
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
				for _, query := range []string{
					"ALTER TABLE records RENAME COLUMN also_resolves TO counted_also_resolves",
					"ALTER TABLE records ADD COLUMN also_resolves TEXT GENERATED ALWAYS AS (" + function + "(counted_also_resolves)) VIRTUAL",
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
				wantReads := int64(population)
				if os.Getenv("CCI_GREP_EXPECT_PAGED") == "1" {
					wantReads = int64(population * (1 + (population+499)/500))
				}
				if got := reads.Load(); got != wantReads {
					t.Fatalf("actual CLI read also_resolves %d times, want %d", got, wantReads)
				}
				if budget > 6144 && strings.Count(out, "\n") != population {
					t.Fatalf("wide budget printed %d records, want %d", strings.Count(out, "\n"), population)
				}
				t.Logf("rows=%d budget=%d actual_cli_history_column_reads=%d candidate_rows=%d annotation_history_rows=%d", population, budget, reads.Load(), population, reads.Load()-int64(population))
				reads.Store(0)
				out, err = run(t, "grep", "REVIEW", "--drive", "durable-agents-rebuild", "--json", "--limit", "-1")
				if err != nil || out != "" || reads.Load() != int64(population) {
					t.Fatalf("zero-output grep = %q, %v, reads=%d; want empty output and %d candidate reads", out, err, reads.Load(), population)
				}
				t.Logf("rows=%d zero_output_cli_history_column_reads=%d annotation_history_rows=0", population, reads.Load())
			})
		}
	}
}
