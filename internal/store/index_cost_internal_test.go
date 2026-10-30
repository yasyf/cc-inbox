package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"modernc.org/sqlite"
)

var laneCollationID atomic.Uint64

func TestRegisteredLaneComparisonsStayBounded(t *testing.T) {
	for _, table := range []string{"records", "sessions"} {
		for _, rows := range []int{64, 4096, 65536} {
			t.Run(fmt.Sprintf("%s/%d", table, rows), func(t *testing.T) {
				var comparisons atomic.Int64
				collation := fmt.Sprintf("cci_lane_%d", laneCollationID.Add(1))
				if err := sqlite.RegisterCollationUtf8(collation, func(a, b string) int {
					comparisons.Add(1)
					return strings.Compare(a, b)
				}); err != nil {
					t.Fatal(err)
				}
				st := countedLaneStore(t, collation)
				ctx := context.Background()
				query := `WITH RECURSIVE history(n) AS (
	VALUES(1) UNION ALL SELECT n + 1 FROM history WHERE n < ?
)
INSERT INTO records (drive, lane, kind, at, text, source, hash)
SELECT 'drive', 'old-lane', 'note', n, 'history', 'post', 'history-' || n FROM history`
				if table == "sessions" {
					query = `WITH RECURSIVE history(n) AS (
	VALUES(1) UNION ALL SELECT n + 1 FROM history WHERE n < ?
)
INSERT INTO sessions (session, agent, origin, drive, root, lane)
SELECT 'session-' || n, '', '', 'drive', 0, 'old-lane' FROM history`
				}
				if _, err := st.db.ExecContext(ctx, query, rows); err != nil {
					t.Fatal(err)
				}
				comparisons.Store(0)
				registered, err := st.Registered(ctx, "drive", "missing-lane")
				if err != nil || registered {
					t.Fatalf("Registered missing lane = %v, %v", registered, err)
				}
				got := comparisons.Load()
				if got == 0 || got > 64 {
					t.Fatalf("Registered compared lane %d times for %d %s rows, want 1..64", got, rows, table)
				}
				t.Logf("table=%s rows=%d actual_lane_comparisons=%d", table, rows, got)
				registered, err = st.Registered(ctx, "drive", "old-lane")
				if err != nil || !registered {
					t.Fatalf("Registered present lane = %v, %v", registered, err)
				}
				assertRegisteredPlan(t, st)
			})
		}
	}
}

func countedLaneStore(t *testing.T, collation string) *Store {
	t.Helper()
	ctx := context.Background()
	production, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = production.Close() })
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), DBFile))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	definitions, err := production.db.QueryContext(ctx, `SELECT type, sql FROM sqlite_schema
WHERE tbl_name IN ('records', 'sessions', 'record_recipients', 'record_also_resolves', 'record_json_errors') AND type IN ('table', 'index', 'trigger') AND sql IS NOT NULL
ORDER BY CASE type WHEN 'table' THEN 0 WHEN 'index' THEN 1 ELSE 2 END, name`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = definitions.Close() }()
	for definitions.Next() {
		var kind, definition string
		if err := definitions.Scan(&kind, &definition); err != nil {
			t.Fatal(err)
		}
		if kind == "table" {
			definition = strings.Replace(definition, "lane TEXT", "lane TEXT COLLATE "+collation, 1)
		}
		if _, err := db.ExecContext(ctx, definition); err != nil {
			t.Fatal(err)
		}
	}
	if err := definitions.Err(); err != nil {
		t.Fatal(err)
	}
	return &Store{db: db, node: production.node}
}

func assertRegisteredPlan(t *testing.T, st *Store) {
	t.Helper()
	rows, err := st.db.QueryContext(context.Background(), `EXPLAIN QUERY PLAN
SELECT EXISTS (SELECT 1 FROM sessions WHERE drive = ? AND lane = ?)
OR EXISTS (SELECT 1 FROM records WHERE drive = ? AND lane = ?)`, "drive", "lane", "drive", "lane")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for _, index := range []string{"records_drive_lane", "sessions_drive_lane"} {
		if !strings.Contains(strings.Join(plan, "\n"), "USING COVERING INDEX "+index+" (drive=? AND lane=?)") {
			t.Fatalf("Registered query plan lacks covering lookup %s: %v", index, plan)
		}
	}
	t.Logf("Registered plan: %v", plan)
}
