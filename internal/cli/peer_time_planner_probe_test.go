package cli_test

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"
)

func TestPeerTimePlannerActualGrepProbe(t *testing.T) {
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
					collation := fmt.Sprintf("cci_peer_planner_cli_%d", tailCounterID.Add(1))
					if err := sqlite.RegisterCollationUtf8(collation, func(a, b string) int {
						comparisons.Add(1)
						return strings.Compare(a, b)
					}); err != nil {
						t.Fatal(err)
					}
					db := tailComparedStore(t, "record_recipients", "address", collation)
					now := time.Now()
					if _, err := db.ExecContext(t.Context(), `INSERT INTO records (drive,lane,kind,at,text,recipients,source,hash) VALUES
('d','writer','note',?,'pure port selected local','[]','post','recent-local'),
('foreign','writer','note',?,'pure port selected peer','["peer:tail-limit-session"]','post','recent-peer')`, now.Add(-time.Hour).UnixMilli(), now.Add(-time.Hour).UnixMilli()); err != nil {
						t.Fatal(err)
					}
					at, recipients, text := now.Add(-48*time.Hour), `["peer:tail-limit-session"]`, "pure port stale peer"
					if scenario == "unrelated-recent" {
						at, recipients, text = now.Add(-time.Hour), `["peer:other"]`, "pure port unrelated recent"
					}
					if _, err := db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?)
INSERT INTO records (drive,lane,kind,at,text,recipients,source,hash)
SELECT CASE WHEN ?='many-drive-stale' THEN 'foreign-'||n ELSE 'foreign' END,'writer','note',?,?,?,'post','planner-peer-'||n FROM history`, population, scenario, at.UnixMilli(), text, recipients); err != nil {
						t.Fatal(err)
					}
					for _, statement := range []string{"DROP INDEX IF EXISTS records_at_seq", "DROP INDEX IF EXISTS records_drive_at_seq", "CREATE INDEX records_drive_at_seq ON records(drive, at, seq)"} {
						if _, err := db.ExecContext(t.Context(), statement); err != nil {
							t.Fatal(err)
						}
					}
					if profile.globalAt {
						if _, err := db.ExecContext(t.Context(), "CREATE INDEX records_at_seq ON records(at, seq)"); err != nil {
							t.Fatal(err)
						}
					}
					if profile.analyze {
						if _, err := db.ExecContext(t.Context(), "ANALYZE"); err != nil {
							t.Fatal(err)
						}
					}
					var statsTables int
					if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM sqlite_schema WHERE name='sqlite_stat1'").Scan(&statsTables); err != nil {
						t.Fatal(err)
					}
					var stats []string
					if statsTables > 0 {
						rows, err := db.QueryContext(t.Context(), "SELECT tbl || ' ' || COALESCE(idx, '') || ' ' || stat FROM sqlite_stat1 ORDER BY tbl, idx")
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
					t.Logf("PEER_PLANNER_CLI_PROFILE scenario=%s profile=%s M=%d drive_at=true global_at=%t analyzed=%t sqlite_stat1=%v", scenario, profile.name, population, profile.globalAt, profile.analyze, stats)
					comparisons.Store(0)
					out, err := run(t, "grep", "pure port", "--drive", "d", "--since", "24h", "-n", "800", "--budget", "600000")
					if err != nil {
						t.Fatal(err)
					}
					lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
					if len(lines) != 2 || !strings.HasPrefix(lines[0], "#2") || !strings.Contains(lines[0], "pure port selected peer") || !strings.HasPrefix(lines[1], "#1") || !strings.Contains(lines[1], "pure port selected local") || strings.Contains(out, "stale peer") || strings.Contains(out, "unrelated recent") {
						t.Fatalf("Grep canonical output=%q, want fresh peer#2 then local#1 only", out)
					}
					t.Logf("PEER_PLANNER_WORK operation=grep scenario=%s profile=%s M=%d eligible_rows=2 actual_address_comparisons=%d output_bytes=%d authority=whole_NewRootCmd_Grep_address_collation_partial_component_not_native_cache", scenario, profile.name, population, comparisons.Load(), len(out))
				})
			}
		}
	}
}
