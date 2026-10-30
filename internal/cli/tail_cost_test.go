package cli_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"

	"github.com/yasyf/cc-inbox/internal/store"
)

var tailCounterID atomic.Uint64

func TestTailRecipientHistoryBaseline(t *testing.T) {
	for _, population := range []int{32, 10000} {
		for _, peer := range []bool{false, true} {
			t.Run(fmt.Sprintf("irrelevant=%d/peer=%t", population, peer), func(t *testing.T) {
				db := tailCostStore(t)
				if peer {
					t.Setenv("CLAUDE_CODE_SESSION_ID", "tail-cost-session")
				}
				at := time.Now().UnixMilli()
				for _, row := range []struct{ drive, recipients string }{
					{"durable-agents-rebuild", "[]"},
					{"foreign", `["peer:tail-cost-session"]`},
				} {
					if _, err := db.Exec("INSERT INTO records (drive,lane,kind,at,text,recipients,source,hash) VALUES (?, 'lane', 'note', ?, 'selected', ?, 'post', ?)", row.drive, at, row.recipients, row.drive); err != nil {
						t.Fatal(err)
					}
				}
				tailCostHistory(t, db, population, at)
				reads := tailCostColumn(t, db, "recipients")
				out, err := run(t, "tail", "--drive", "durable-agents-rebuild", "--since", "36h", "-n", "80000", "--json", "--budget", "300000000")
				if err != nil {
					t.Fatal(err)
				}
				want := []int64{1}
				if peer {
					want = append(want, 2)
				}
				tailCostOutput(t, out, want, "")
				got := reads.Load()
				if peer && got < int64(2*population) || !peer && got != 1 {
					t.Fatalf("recipient reads = %d for irrelevant=%d peer=%t", got, population, peer)
				}
				t.Logf("actual_tail irrelevant=%d peer=%t output_rows=%d recipient_column_reads=%d", population, peer, len(want), got)
			})
		}
	}
}

func TestTailResolverHistoryBaseline(t *testing.T) {
	for _, population := range []int{32, 10000} {
		t.Run(fmt.Sprintf("irrelevant=%d", population), func(t *testing.T) {
			db := tailCostStore(t)
			at := time.Now().UnixMilli()
			if _, err := db.Exec("INSERT INTO records (drive,lane,kind,at,text,source,hash) VALUES ('durable-agents-rebuild','lane','ask',?,'selected','post','selected')", at); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("INSERT INTO records (drive,lane,kind,at,text,also_resolves,source,hash) VALUES ('foreign','lane','note',?,'resolver','[1]','post','resolver')", at); err != nil {
				t.Fatal(err)
			}
			tailCostHistory(t, db, population, at)
			reads := tailCostColumn(t, db, "also_resolves")
			out, err := run(t, "tail", "--drive", "durable-agents-rebuild", "--since", "36h", "-n", "80000", "--json", "--budget", "300000000")
			if err != nil {
				t.Fatal(err)
			}
			tailCostOutput(t, out, []int64{1}, "closed")
			got := reads.Load()
			if got < int64(population) {
				t.Fatalf("resolver reads = %d for irrelevant=%d", got, population)
			}
			t.Logf("actual_tail irrelevant=%d output_rows=1 cross_drive_status=closed also_resolves_column_reads=%d", population, got)
		})
	}
}

func TestTailPreservesRequested80000JSONRecords(t *testing.T) {
	db := tailCostStore(t)
	if _, err := db.Exec(`WITH RECURSIVE history(n) AS (
VALUES(1) UNION ALL SELECT n + 1 FROM history WHERE n < 80000
)
INSERT INTO records (drive,lane,kind,at,text,source,hash)
SELECT 'durable-agents-rebuild','lane','note',?,'requested','post','requested-' || n FROM history`, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	out, err := run(t, "tail", "--drive", "durable-agents-rebuild", "--since", "36h", "-n", "80000", "--json", "--budget", "300000000")
	if err != nil {
		t.Fatal(err)
	}
	want := make([]int64, 80000)
	for i := range want {
		want[i] = int64(i + 1)
	}
	tailCostOutput(t, out, want, "")
	t.Logf("actual_tail requested_rows=80000 output_rows=80000 output_bytes=%d", len(out))
}

func tailCostStore(t *testing.T) *sql.DB {
	t.Helper()
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	t.Setenv("CODEX_THREAD_ID", "")
	t.Setenv("CCI_AGENT_ID", "")
	st, err := store.Open(context.Background(), home)
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
	return db
}

func tailCostHistory(t *testing.T, db *sql.DB, population int, at int64) {
	t.Helper()
	if _, err := db.Exec(`WITH RECURSIVE history(n) AS (
VALUES(1) UNION ALL SELECT n + 1 FROM history WHERE n < ?
)
INSERT INTO records (drive,lane,kind,at,text,recipients,also_resolves,source,hash)
SELECT 'irrelevant','lane','note',?,'irrelevant','["someone-else"]','[]','post','irrelevant-' || n FROM history`, population, at); err != nil {
		t.Fatal(err)
	}
}

func tailCostColumn(t *testing.T, db *sql.DB, counted string) *atomic.Int64 {
	t.Helper()
	reads := &atomic.Int64{}
	function := fmt.Sprintf("cci_tail_read_%d", tailCounterID.Add(1))
	if err := sqlite.RegisterScalarFunction(function, 1, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
		reads.Add(1)
		return args[0], nil
	}); err != nil {
		t.Fatal(err)
	}
	columns, err := db.Query("PRAGMA table_info(records)")
	if err != nil {
		t.Fatal(err)
	}
	selection := make([]string, 0, 24)
	for columns.Next() {
		var cid, notNull, primaryKey int
		var name, kind string
		var defaultValue any
		if err := columns.Scan(&cid, &name, &kind, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		column := name
		if name == counted {
			column = function + "(" + name + ") AS " + name
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
		"ALTER TABLE records RENAME TO counted_tail_history",
		"CREATE VIEW records AS SELECT " + strings.Join(selection, ", ") + " FROM counted_tail_history",
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	return reads
}

func tailCostOutput(t *testing.T, out string, seqs []int64, status string) {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != len(seqs) {
		t.Fatalf("tail output rows = %d, want %d", len(lines), len(seqs))
	}
	for i, line := range lines {
		var record store.Record
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		if record.Seq != seqs[i] || record.Status != status {
			t.Fatalf("tail row %d seq/status = %d/%q, want %d/%q", i, record.Seq, record.Status, seqs[i], status)
		}
	}
}
