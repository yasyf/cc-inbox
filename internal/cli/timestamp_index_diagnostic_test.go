package cli_test

import (
	"fmt"
	"math/bits"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"
)

func TestTimestampIndexCLIWork(t *testing.T) {
	for _, population := range []int{32, 10000} {
		for _, index := range []struct {
			name string
			ddl  string
		}{
			{"missing", ""},
			{"drive-at", "CREATE INDEX records_drive_at_seq ON records(drive, at, seq)"},
			{"at", "CREATE INDEX records_at_seq ON records(at, seq)"},
		} {
			for _, peer := range []bool{false, true} {
				t.Run(fmt.Sprintf("rows=%d/index=%s/peer=%t", population, index.name, peer), func(t *testing.T) {
					var comparisons atomic.Int64
					collation := fmt.Sprintf("cci_timestamp_cli_%d", tailCounterID.Add(1))
					if err := sqlite.RegisterCollationUtf8(collation, func(a, b string) int {
						comparisons.Add(1)
						return strings.Compare(a, b)
					}); err != nil {
						t.Fatal(err)
					}
					db := tailComparedStore(t, "records", "drive", collation)
					if !peer {
						t.Setenv("CLAUDE_CODE_SESSION_ID", "")
					}
					now := time.Now()
					if _, err := db.ExecContext(t.Context(), "INSERT INTO records (drive,lane,kind,at,text,source,hash) VALUES ('d','writer','note',?,'pure port selected early','post','early')", now.Add(-time.Hour).UnixMilli()); err != nil {
						t.Fatal(err)
					}
					if _, err := db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?)
INSERT INTO records (drive,lane,kind,at,text,source,hash)
SELECT 'd','writer','state',?,'stale','post','noise-'||n FROM history`, population, now.Add(-48*time.Hour).UnixMilli()); err != nil {
						t.Fatal(err)
					}
					if _, err := db.ExecContext(t.Context(), `INSERT INTO records (drive,lane,kind,at,text,recipients,source,hash) VALUES
('foreign','writer','state',?,'pure port selected global','["peer:tail-limit-session"]','post','global'),
('d','writer','note',?,'pure port stale high seq','[]','post','stale-high'),
('d','writer','state',?,'pure port selected latest','[]','post','latest')`, now.Add(-time.Hour).UnixMilli(), now.Add(-48*time.Hour).UnixMilli(), now.Add(-time.Hour).UnixMilli()); err != nil {
						t.Fatal(err)
					}
					for _, ddl := range []string{"DROP INDEX IF EXISTS records_at_seq", "DROP INDEX IF EXISTS records_drive_at_seq", index.ddl} {
						if ddl != "" {
							if _, err := db.ExecContext(t.Context(), ddl); err != nil {
								t.Fatal(err)
							}
						}
					}
					comparisons.Store(0)
					out, err := run(t, "grep", "pure port", "--drive", "d", "--since", "24h", "-n", "800", "--budget", "600000")
					if err != nil {
						t.Fatal(err)
					}
					want := 2
					if peer {
						want++
					}
					if strings.Count(out, "\n") != want || !strings.Contains(out, "selected early") || !strings.Contains(out, "selected latest") || strings.Contains(out, "stale high seq") || strings.Contains(out, "selected global") != peer {
						t.Fatalf("Grep output=%q, want=%d rows, peer=%t", out, want, peer)
					}
					measured := comparisons.Load()
					if measured == 0 {
						t.Fatal("actual CLI made no drive comparisons")
					}
					bound := int64(2*bits.Len(uint(population)) + 16)
					t.Logf("TIMESTAMP_INDEX_WORK operation=grep history=%d index=%s peer=%t actual_drive_comparisons=%d diagnostic_bound=%d bounded=%t", population, index.name, peer, measured, bound, measured <= bound)
					if os.Getenv("CCI_TIMESTAMP_REQUIRE_MISSING_BOUND") == "1" && index.name == "missing" && peer && measured > bound {
						t.Errorf("TIMESTAMP_INDEX_UNBOUNDED operation=grep history=%d index=missing peer=true comparisons=%d bound=%d", population, measured, bound)
					}
				})
			}
		}
	}
}
