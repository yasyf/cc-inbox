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

func TestPeerHistoryActualGrepDiagnostic(t *testing.T) {
	if os.Getenv("CCI_PEER_HISTORY_DIAGNOSTIC") != "1" {
		t.Skip("the isolated peer-history workflow supplies the diagnostic gate")
	}
	for _, population := range []int{32, 10000} {
		t.Run(fmt.Sprint(population), func(t *testing.T) {
			var comparisons atomic.Int64
			collation := fmt.Sprintf("cci_peer_history_cli_%d", tailCounterID.Add(1))
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
			if _, err := db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?)
INSERT INTO records (drive,lane,kind,at,text,recipients,source,hash)
SELECT 'foreign','writer','note',?,'pure port stale peer','["peer:tail-limit-session"]','post','stale-peer-'||n FROM history`, population, now.Add(-48*time.Hour).UnixMilli()); err != nil {
				t.Fatal(err)
			}
			var indexed bool
			if err := db.QueryRowContext(t.Context(), "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='index' AND name='records_drive_at_seq')").Scan(&indexed); err != nil || !indexed {
				t.Fatalf("current drive/time index present=%t, error=%v", indexed, err)
			}
			comparisons.Store(0)
			out, err := run(t, "grep", "pure port", "--drive", "d", "--since", "24h", "-n", "800", "--budget", "600000")
			if err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
			if len(lines) != 2 || !strings.HasPrefix(lines[0], "#2") || !strings.Contains(lines[0], "pure port selected peer") || !strings.HasPrefix(lines[1], "#1") || !strings.Contains(lines[1], "pure port selected local") || strings.Contains(out, "stale peer") {
				t.Fatalf("Grep canonical output=%q, want fresh peer#2 then local#1 only", out)
			}
			got := comparisons.Load()
			if got == 0 {
				t.Fatal("actual NewRootCmd Grep made no address comparisons")
			}
			reference := int64(8*bits.Len(uint(population)) + 8)
			t.Logf("PEER_HISTORY_WORK operation=grep M=%d eligible_rows=2 actual_address_comparisons=%d logarithmic_reference=%d within_reference=%t authority=whole_command_selected_address_collation output_bytes=%d", population, got, reference, got <= reference, len(out))
			if os.Getenv("CCI_PEER_HISTORY_REQUIRE_BOUND") == "1" && got > reference {
				t.Errorf("PEER_HISTORY_REFERENCE_EXCEEDED operation=grep M=%d actual_address_comparisons=%d reference=%d", population, got, reference)
			}
		})
	}
}
