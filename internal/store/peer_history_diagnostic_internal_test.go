package store

import (
	"fmt"
	"math/bits"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"
)

func TestPeerHistoryPublicReadsDiagnostic(t *testing.T) {
	if os.Getenv("CCI_PEER_HISTORY_DIAGNOSTIC") != "1" {
		t.Skip("the isolated peer-history workflow supplies the diagnostic gate")
	}
	for _, population := range []int{32, 10000} {
		t.Run(fmt.Sprint(population), func(t *testing.T) {
			var comparisons atomic.Int64
			collation := fmt.Sprintf("cci_peer_history_store_%d", laneCollationID.Add(1))
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
			if _, err := st.db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?)
INSERT INTO records (drive,lane,kind,at,text,recipients,source,hash)
SELECT 'foreign','writer','note',?,'pure port stale peer','["peer:reader"]','post','stale-peer-'||n FROM history`, population, now.Add(-48*time.Hour).UnixMilli()); err != nil {
				t.Fatal(err)
			}
			timestampRangeIndex(t, st, true)
			st.db.SetMaxOpenConns(1)
			filter := Filter{Drive: "d", Peer: "reader", Since: now.Add(-24 * time.Hour), Until: now, Descending: true, Limit: 500}
			query, args := st.selection(filter)
			where, params := filter.bounded(st.Self(), st.Now())
			countSQL := "SELECT COUNT(*) FROM records WHERE " + where
			t.Logf("PEER_HISTORY_PLAN operation=query M=%d query=%q args=%v plan=%v", population, query, args, queryPlan(t.Context(), t, st, query, args...))
			t.Logf("PEER_HISTORY_PLAN operation=count M=%d query=%q args=%v plan=%v", population, countSQL, params, queryPlan(t.Context(), t, st, countSQL, params...))
			t.Logf("PEER_HISTORY_VM_PROGRAM operation=query M=%d program=%v authority=compiled_program_not_executed_steps", population, timestampDiagnosticVM(t, st, query, args...))
			t.Logf("PEER_HISTORY_VM_PROGRAM operation=count M=%d program=%v authority=compiled_program_not_executed_steps", population, timestampDiagnosticVM(t, st, countSQL, params...))
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
				got := comparisons.Load()
				if got == 0 {
					t.Fatal("actual public read made no address comparisons")
				}
				reference := int64(8*bits.Len(uint(population)) + 8)
				t.Logf("PEER_HISTORY_WORK operation=%s M=%d eligible_rows=2 actual_address_comparisons=%d logarithmic_reference=%d within_reference=%t actual_cache_hits=%d actual_cache_misses=%d authority=whole_public_operation_address_collation_and_native_DBStatus", operation, population, got, reference, got <= reference, hits, misses)
				if os.Getenv("CCI_PEER_HISTORY_REQUIRE_BOUND") == "1" && got > reference {
					t.Errorf("PEER_HISTORY_REFERENCE_EXCEEDED operation=%s M=%d actual_address_comparisons=%d reference=%d", operation, population, got, reference)
				}
			}
		})
	}
}
