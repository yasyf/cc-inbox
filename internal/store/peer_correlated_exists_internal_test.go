package store

import (
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestPeerCorrelatedExistsMemberships(t *testing.T) {
	if os.Getenv("CCI_PEER_EXISTS_DIAGNOSTIC") != "1" {
		t.Skip("the isolated peer workflow supplies the exploratory EXISTS gate")
	}
	completed := 0
	for _, collation := range []string{"BINARY", "NOCASE"} {
		t.Run(collation, func(t *testing.T) {
			st := countedColumnStore(t, "address", collation)
			now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
			st.Now = func() time.Time { return now }
			for i, recipients := range []string{`[]`, `["peer:reader",null,"peer:reader"]`, `[null]`, `null`, `["peer:Reader"]`, `["root@d",null,"root@d"]`, `["peer:reader"]`, `["peer:reader"]`, `["peer:reader "]`} {
				drive, origin, at := "foreign", "feedfacefeedface", now.Add(-time.Hour)
				if i == 0 || i == 7 {
					drive = "d"
				}
				if i == 1 {
					origin = "deadc0dedeadc0de"
				}
				if i == 6 {
					at = now.Add(-48 * time.Hour)
				}
				if _, err := st.db.ExecContext(t.Context(), `INSERT INTO records (drive,lane,kind,at,text,recipients,source,hash,origin,origin_seq) VALUES (?,'writer','note',?,'membership',?,'post',?,?,?)`, drive, at.UnixMilli(), recipients, fmt.Sprintf("member-%d", i+1), origin, i+1); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := st.db.ExecContext(t.Context(), "INSERT INTO record_recipients (address,source_seq) VALUES ('peer:reader',2)"); err != nil {
				t.Fatal(err)
			}
			timestampRangeIndex(t, st, true)
			for _, test := range []struct {
				name string
				f    Filter
				want []int64
			}{
				{"window", Filter{Drive: "d", Peer: "reader", Since: now.Add(-24 * time.Hour)}, []int64{8, 2, 1}},
				{"no-window", Filter{Drive: "d", Peer: "reader"}, []int64{8, 7, 2, 1}},
				{"peer-only", Filter{Peer: "reader", Since: now.Add(-24 * time.Hour)}, []int64{8, 2}},
				{"peer-after", Filter{Drive: "d", Peer: "reader", Since: now.Add(-24 * time.Hour), PeerAfter: 2}, []int64{8, 1}},
				{"after-before", Filter{Drive: "d", Peer: "reader", Since: now.Add(-24 * time.Hour), After: 1, Before: 8}, []int64{2}},
				{"reader", Filter{Drive: "d", Peer: "reader", For: "root", Since: now.Add(-24 * time.Hour)}, []int64{8, 6, 2, 1}},
				{"origin", Filter{Drive: "d", Peer: "reader", Origin: "feedfacefeedface", Since: now.Add(-24 * time.Hour)}, []int64{8, 1}},
				{"until-only", Filter{Drive: "d", Peer: "reader", Until: now.Add(-90 * time.Minute)}, []int64{7}},
				{"empty-window", Filter{Drive: "d", Peer: "reader", Since: now, Until: now}, nil},
			} {
				t.Run(test.name, func(t *testing.T) {
					filter := test.f
					filter.Descending = true
					got, err := st.Query(t.Context(), filter)
					seqs := make([]int64, 0, len(got))
					for _, record := range got {
						seqs = append(seqs, record.Seq)
					}
					if err != nil || !slices.Equal(seqs, test.want) {
						t.Fatalf("Query seqs=%v, error=%v, want=%v", seqs, err, test.want)
					}
					count, countErr := st.Count(t.Context(), filter)
					if countErr != nil || count != len(test.want) {
						t.Fatalf("Count=%d, error=%v, want=%d", count, countErr, len(test.want))
					}
					native, nativeErr := unboundedQuery(t.Context(), st, filter)
					assertSameRead(t, "valid EXISTS rows", got, err, native, nativeErr)
					completed++
					t.Logf("PEER_EXISTS_SEMANTICS collation=%s case=%s seqs=%v count=%d", collation, test.name, seqs, count)
				})
			}
		})
	}
	if completed != 18 {
		t.Fatalf("completed cases=%d, want=18", completed)
	}
	t.Logf("PEER_EXISTS_SEMANTICS_COMPLETE cases=%d", completed)
}

func TestPeerCorrelatedExistsForeignWork(t *testing.T) {
	if os.Getenv("CCI_PEER_EXISTS_DIAGNOSTIC") != "1" {
		t.Skip("the isolated peer workflow supplies the exploratory EXISTS gate")
	}
	for _, population := range []int{32, 10000} {
		for _, scenario := range []string{"many-foreign-stale", "unrelated-recent"} {
			t.Run(fmt.Sprintf("M=%d/%s", population, scenario), func(t *testing.T) {
				st := openMembershipStore(t.Context(), t, t.TempDir())
				now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
				st.Now = func() time.Time { return now }
				if _, err := st.db.ExecContext(t.Context(), `INSERT INTO records (drive,lane,kind,at,text,recipients,source,hash) VALUES
('d','writer','note',?,'pure port selected local','[]','post','recent-local'),
('foreign','writer','note',?,'pure port selected peer','["peer:reader"]','post','recent-peer')`, now.Add(-time.Hour).UnixMilli(), now.Add(-time.Hour).UnixMilli()); err != nil {
					t.Fatal(err)
				}
				at, recipients := now.Add(-48*time.Hour), `["peer:reader"]`
				if scenario == "unrelated-recent" {
					at, recipients = now.Add(-time.Hour), `["peer:other"]`
				}
				if _, err := st.db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?)
INSERT INTO records (drive,lane,kind,at,text,recipients,source,hash)
SELECT CASE WHEN ?='many-foreign-stale' THEN 'foreign-'||n ELSE 'foreign' END,'writer','note',?,'foreign work',?,'post','exists-peer-'||n FROM history`, population, scenario, at.UnixMilli(), recipients); err != nil {
					t.Fatal(err)
				}
				timestampRangeIndex(t, st, true)
				st.db.SetMaxOpenConns(1)
				filter := Filter{Drive: "d", Peer: "reader", Since: now.Add(-24 * time.Hour), Until: now, Descending: true, Limit: 500}
				query, args := st.selection(filter)
				where, params := filter.bounded(st.Self(), st.Now())
				queryDetails := queryPlan(t.Context(), t, st, query, args...)
				countDetails := queryPlan(t.Context(), t, st, "SELECT COUNT(*) FROM records WHERE "+where, params...)
				t.Logf("PEER_EXISTS_PLAN operation=query scenario=%s M=%d sql=%q args=%v plan=%v", scenario, population, query, args, queryDetails)
				t.Logf("PEER_EXISTS_PLAN operation=count scenario=%s M=%d sql=%q args=%v plan=%v", scenario, population, "SELECT COUNT(*) FROM records WHERE "+where, params, countDetails)
				for _, operation := range []string{"query", "count"} {
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
						t.Fatal("native whole-operation cache counters recorded no work")
					}
					t.Logf("PEER_EXISTS_WORK operation=%s scenario=%s M=%d eligible_rows=2 actual_cache_hits=%d actual_cache_misses=%d actual_cache_accesses=%d authority=whole_public_operation_native_DBStatus", operation, scenario, population, hits, misses, hits+misses)
				}
				for _, details := range [][]string{queryDetails, countDetails} {
					for _, line := range details {
						if strings.HasPrefix(line, "SCAN candidate") {
							t.Errorf("PEER_EXISTS_GLOBAL_RECORD_SCAN scenario=%s M=%d plan=%q", scenario, population, line)
						}
					}
				}
			})
		}
	}
}
