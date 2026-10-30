package store

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"modernc.org/sqlite"
)

func TestRecipientLaneQueryUsesTargetedIndex(t *testing.T) {
	for _, population := range []int{64, 4096, 65536} {
		for _, indexed := range []bool{false, true} {
			t.Run(fmt.Sprintf("rows=%d/indexed=%t", population, indexed), func(t *testing.T) {
				var comparisons atomic.Int64
				collation := fmt.Sprintf("cci_recipients_%d", laneCollationID.Add(1))
				if err := sqlite.RegisterCollationUtf8(collation, func(a, b string) int {
					comparisons.Add(1)
					return strings.Compare(a, b)
				}); err != nil {
					t.Fatal(err)
				}
				st := countedLaneStore(t, collation)
				ctx := context.Background()
				if !indexed {
					if _, err := st.db.ExecContext(ctx, "DROP INDEX sessions_origin_lane"); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := st.db.ExecContext(ctx, `WITH RECURSIVE history(n) AS (
	VALUES(1) UNION ALL SELECT n + 1 FROM history WHERE n < ?
)
INSERT INTO sessions (session, agent, origin, drive, root, lane)
SELECT 'unrelated-' || n, '', '', 'unrelated-drive-' || n, 0, 'unrelated-lane' FROM history`, population); err != nil {
					t.Fatal(err)
				}
				for _, query := range []string{
					"INSERT INTO sessions (session, agent, origin, drive, root, lane) VALUES ('target-a', '', '', 'drive-a', 0, 'api')",
					"INSERT INTO sessions (session, agent, origin, drive, root, lane) VALUES ('target-b', 'worker', '', 'drive-b', 0, 'api')",
					"INSERT INTO sessions (session, agent, origin, drive, root, lane) VALUES ('remote', '', 'remote', 'drive-c', 0, 'api')",
					"INSERT INTO sessions (session, agent, origin, drive, root, lane) VALUES ('unbound', '', '', '', 0, 'api')",
				} {
					if _, err := st.db.ExecContext(ctx, query); err != nil {
						t.Fatal(err)
					}
				}
				comparisons.Store(0)
				rows, err := st.LanePeers(ctx, []string{"api", "missing"})
				if err != nil {
					t.Fatal(err)
				}
				got := comparisons.Load()
				if len(rows) != 2 || rows[0].Session != "target-a" || rows[1].Session != "target-b" {
					t.Fatalf("LanePeers = %+v, want target-a and target-b", rows)
				}
				if indexed && (got == 0 || got > 128) {
					t.Fatalf("indexed lane comparisons = %d for %d unrelated rows, want 1..128", got, population)
				}
				if !indexed && got < int64(population) {
					t.Fatalf("unindexed lane comparisons = %d for %d unrelated rows, want at least %d", got, population, population)
				}
				all, err := st.Peers(ctx)
				if err != nil {
					t.Fatal(err)
				}
				want := slices.DeleteFunc(all, func(p Peer) bool {
					return p.Origin != "" || p.Drive == "" || !slices.Contains([]string{"api", "missing"}, p.Lane)
				})
				if !slices.EqualFunc(rows, want, func(a, b Peer) bool { return a.Key() == b.Key() }) {
					t.Fatalf("targeted rows = %+v, baseline rows = %+v", rows, want)
				}
				candidateAllocs := testing.AllocsPerRun(1, func() {
					if _, err := st.LanePeers(ctx, []string{"api", "missing"}); err != nil {
						t.Fatal(err)
					}
				})
				baselineAllocs := testing.AllocsPerRun(1, func() {
					if _, err := st.Peers(ctx); err != nil {
						t.Fatal(err)
					}
				})
				t.Logf("unrelated_sessions=%d fixed_lanes=2 indexed=%t actual_lane_comparisons=%d actual_materialized_rows=%d baseline_materialized_rows=%d actual_candidate_allocs=%.0f actual_baseline_allocs=%.0f", population, indexed, got, len(rows), len(all), candidateAllocs, baselineAllocs)
			})
		}
	}
}
