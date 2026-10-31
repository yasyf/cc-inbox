package store

import (
	"fmt"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"
)

func TestPeerTimePlannerPublicReadsProbe(t *testing.T) {
	if os.Getenv("CCI_PEER_TIME_PLANNER_PROBE") != "1" {
		t.Skip("the isolated peer-history workflow supplies the planner probe")
	}
	for _, population := range []int{32, 10000} {
		for _, scenario := range []string{"same-drive-stale", "many-drive-stale", "unrelated-recent"} {
			for _, profile := range []struct {
				name     string
				globalAt bool
				analyze  bool
			}{
				{"drive-at", false, false},
				{"drive-at-analyze", false, true},
				{"drive-at-global-at", true, false},
				{"drive-at-global-at-analyze", true, true},
			} {
				t.Run(fmt.Sprintf("M=%d/%s/%s", population, scenario, profile.name), func(t *testing.T) {
					var comparisons atomic.Int64
					collation := fmt.Sprintf("cci_peer_planner_store_%d", laneCollationID.Add(1))
					if err := sqlite.RegisterCollationUtf8(collation, func(a, b string) int {
						comparisons.Add(1)
						return strings.Compare(a, b)
					}); err != nil {
						t.Fatal(err)
					}
					st := countedColumnStore(t, "address", collation)
					now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
					st.Now = func() time.Time { return now }
					if _, err := st.db.ExecContext(t.Context(), `INSERT INTO records (drive,lane,kind,at,text,recipients,source,hash) VALUES
('d','writer','note',?,'pure port selected local','[]','post','recent-local'),
('foreign','writer','note',?,'pure port selected peer','["peer:reader"]','post','recent-peer')`, now.Add(-time.Hour).UnixMilli(), now.Add(-time.Hour).UnixMilli()); err != nil {
						t.Fatal(err)
					}
					at, recipients, text := now.Add(-48*time.Hour), `["peer:reader"]`, "pure port stale peer"
					if scenario == "unrelated-recent" {
						at, recipients, text = now.Add(-time.Hour), `["peer:other"]`, "pure port unrelated recent"
					}
					if _, err := st.db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?)
INSERT INTO records (drive,lane,kind,at,text,recipients,source,hash)
SELECT CASE WHEN ?='many-drive-stale' THEN 'foreign-'||n ELSE 'foreign' END,'writer','note',?,?,?,'post','planner-peer-'||n FROM history`, population, scenario, at.UnixMilli(), text, recipients); err != nil {
						t.Fatal(err)
					}
					timestampRangeIndex(t, st, true)
					if _, err := st.db.ExecContext(t.Context(), "DROP INDEX IF EXISTS records_at_seq"); err != nil {
						t.Fatal(err)
					}
					if profile.globalAt {
						if _, err := st.db.ExecContext(t.Context(), "CREATE INDEX records_at_seq ON records(at, seq)"); err != nil {
							t.Fatal(err)
						}
					}
					if profile.analyze {
						if _, err := st.db.ExecContext(t.Context(), "ANALYZE"); err != nil {
							t.Fatal(err)
						}
					}
					var statsTables int
					if err := st.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM sqlite_schema WHERE name='sqlite_stat1'").Scan(&statsTables); err != nil {
						t.Fatal(err)
					}
					var stats []string
					if statsTables > 0 {
						rows, err := st.db.QueryContext(t.Context(), "SELECT tbl || ' ' || COALESCE(idx, '') || ' ' || stat FROM sqlite_stat1 ORDER BY tbl, idx")
						if err != nil {
							t.Fatal(err)
						}
						for rows.Next() {
							var stat string
							if err := rows.Scan(&stat); err != nil {
								t.Fatal(err)
							}
							stats = append(stats, stat)
						}
						if err := rows.Err(); err != nil {
							t.Fatal(err)
						}
						if err := rows.Close(); err != nil {
							t.Fatal(err)
						}
					}
					if (statsTables > 0) != profile.analyze {
						t.Fatalf("sqlite_stat1 present=%t, want analyzed=%t", statsTables > 0, profile.analyze)
					}
					t.Logf("PEER_PLANNER_PROFILE scenario=%s profile=%s M=%d drive_at=true global_at=%t analyzed=%t sqlite_stat1=%v", scenario, profile.name, population, profile.globalAt, profile.analyze, stats)
					st.db.SetMaxOpenConns(1)
					filter := Filter{Drive: "d", Peer: "reader", Since: now.Add(-24 * time.Hour), Descending: true, Limit: 500}
					query, args := st.selection(filter)
					where, params := filter.bounded(st.Self(), st.Now())
					countSQL := "SELECT COUNT(*) FROM records WHERE " + where
					t.Logf("PEER_PLANNER_PLAN operation=query scenario=%s profile=%s M=%d query=%q args=%v plan=%v", scenario, profile.name, population, query, args, queryPlan(t.Context(), t, st, query, args...))
					t.Logf("PEER_PLANNER_PLAN operation=count scenario=%s profile=%s M=%d query=%q args=%v plan=%v", scenario, profile.name, population, countSQL, params, queryPlan(t.Context(), t, st, countSQL, params...))
					for _, operation := range []string{"query", "count"} {
						comparisons.Store(0)
						timestampCacheWork(t, st, true)
						if operation == "query" {
							got, err := st.Query(t.Context(), filter)
							seqs := make([]int64, 0, len(got))
							for _, record := range got {
								seqs = append(seqs, record.Seq)
							}
							if err != nil || !slices.Equal(seqs, []int64{2, 1}) {
								t.Fatalf("Query seqs=%v, error=%v, want=[2 1]", seqs, err)
							}
						} else if got, err := st.Count(t.Context(), filter); err != nil || got != 2 {
							t.Fatalf("Count=%d, error=%v, want=2", got, err)
						}
						hits, misses := timestampCacheWork(t, st, false)
						if hits+misses == 0 {
							t.Fatal("native whole-operation cache counters recorded no page work")
						}
						t.Logf("PEER_PLANNER_WORK operation=%s scenario=%s profile=%s M=%d eligible_rows=2 actual_address_comparisons=%d actual_cache_hits=%d actual_cache_misses=%d actual_cache_accesses=%d authority=whole_public_operation_native_DBStatus_address_collation_is_partial_component", operation, scenario, profile.name, population, comparisons.Load(), hits, misses, hits+misses)
					}
				})
			}
		}
	}
}
