package store

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"modernc.org/sqlite"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func TestTimestampRangePlanAndNativeCacheWork(t *testing.T) {
	for _, population := range []int{32, 10000} {
		for _, indexed := range []bool{false, true} {
			t.Run(fmt.Sprintf("rows=%d/indexed=%t", population, indexed), func(t *testing.T) {
				st := openMembershipStore(t.Context(), t, t.TempDir())
				now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
				st.Now = func() time.Time { return now }
				seedTimestampDiagnostic(t, st, population, now)
				timestampRangeIndex(t, st, indexed)
				st.db.SetMaxOpenConns(1)
				filter := Filter{Drive: "d", Peer: "reader", Since: now.Add(-24 * time.Hour), Until: now, Descending: true, Limit: 500}
				query, args := st.selection(filter)
				plan := queryPlan(t.Context(), t, st, query, args...)
				if indexed {
					joined := strings.Join(plan, "\n")
					if !strings.Contains(joined, "USING COVERING INDEX records_drive_at_seq (drive=? AND at>? AND at<?)") {
						t.Fatalf("timestamp range index is not used:\n%s", joined)
					}
					for _, step := range plan {
						if strings.HasPrefix(step, "SCAN records") {
							t.Fatalf("indexed query still scans records: %s\n%s", step, joined)
						}
					}
				}
				for _, operation := range []string{"query", "count"} {
					timestampCacheWork(t, st, true)
					if operation == "query" {
						got, err := st.Query(t.Context(), filter)
						seqs := make([]int64, 0, len(got))
						for _, record := range got {
							seqs = append(seqs, record.Seq)
						}
						if err != nil || !slices.Equal(seqs, []int64{int64(population + 4), int64(population + 2), 1}) {
							t.Fatalf("Query=%v, error=%v", seqs, err)
						}
					} else if got, err := st.Count(t.Context(), filter); err != nil || got != 3 {
						t.Fatalf("Count=%d, error=%v", got, err)
					}
					hits, misses := timestampCacheWork(t, st, false)
					t.Logf("TIMESTAMP_RANGE_NATIVE_WORK history=%d indexed=%t operation=%s actual_cache_hits=%d actual_cache_misses=%d", population, indexed, operation, hits, misses)
				}
			})
		}
	}
}

func TestTimestampRangeWindowBoundaries(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprint(indexed), func(t *testing.T) {
			st := openMembershipStore(t.Context(), t, t.TempDir())
			now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
			st.Now = func() time.Time { return now }
			timestampRangeIndex(t, st, indexed)
			since := now.Add(-24 * time.Hour)
			s, u := since.UnixMilli(), now.UnixMilli()
			if _, err := st.db.ExecContext(t.Context(), `INSERT INTO records (drive,lane,kind,at,text,recipients,re,source,hash) VALUES
('d','writer','note',?,'before','[]',0,'post','boundary-1'),
('d','writer','note',?,'since local','[]',0,'post','boundary-2'),
('foreign','writer','state',?,'since peer','["peer:reader"]',0,'post','boundary-3'),
('foreign','writer','state',?,'before until peer','["peer:reader"]',0,'post','boundary-4'),
('d','writer','note',?,'until local','[]',0,'post','boundary-5'),
('d','writer','note',?,'after until','[]',0,'post','boundary-6'),
('foreign','writer','state',?,'until peer','["peer:reader"]',0,'post','boundary-7'),
('foreign','writer','state',?,'unaddressed','[]',0,'post','boundary-8'),
('foreign','writer','state',?,'since reader','["root@d"]',0,'post','boundary-9'),
('foreign','root','note',?,'parent outside window','[]',0,'post','boundary-10'),
('d','writer','state',?,'parent reply inside window','[]',10,'post','boundary-11'),
('d','writer','note',?,'direct api','["api"]',0,'post','boundary-12')`, s-1, s, s, u-1, u, u+1, u, s, s, u+1, s, s); err != nil {
				t.Fatal(err)
			}
			for _, test := range []struct {
				name string
				f    Filter
				want []int64
			}{
				{"window peer", Filter{Drive: "d", Peer: "reader", Since: since, Until: now}, []int64{12, 11, 4, 3, 2}},
				{"window no peer", Filter{Drive: "d", Since: since, Until: now}, []int64{12, 11, 2}},
				{"since inclusive", Filter{Drive: "d", Peer: "reader", Since: since}, []int64{12, 11, 7, 6, 5, 4, 3, 2}},
				{"until exclusive", Filter{Drive: "d", Peer: "reader", Until: now}, []int64{12, 11, 4, 3, 2, 1}},
				{"after", Filter{Drive: "d", Peer: "reader", Since: since, Until: now, After: 2}, []int64{12, 11, 4, 3}},
				{"before", Filter{Drive: "d", Peer: "reader", Since: since, Until: now, Before: 4}, []int64{3, 2}},
				{"peer after", Filter{Drive: "d", Peer: "reader", Since: since, Until: now, PeerAfter: 3}, []int64{12, 11, 4, 2}},
				{"reader parent", Filter{Drive: "d", Peer: "reader", For: "root", Kinds: []kinds.Kind{kinds.Note}, Since: since, Until: now}, []int64{11, 9, 4, 3, 2}},
				{"addressed kinds", Filter{Drive: "d", Peer: "reader", For: "root", Kinds: []kinds.Kind{kinds.Note}, ToKinds: []kinds.Kind{kinds.Ask}, Since: since, Until: now}, []int64{2}},
				{"to", Filter{Drive: "d", To: "api", Since: since, Until: now}, []int64{12}},
				{"empty range", Filter{Drive: "d", Peer: "reader", Since: now, Until: now}, nil},
			} {
				t.Run(test.name, func(t *testing.T) {
					filter := test.f
					filter.Descending = true
					filter.Limit = 500
					got, err := st.Query(t.Context(), filter)
					seqs := make([]int64, 0, len(got))
					for _, record := range got {
						seqs = append(seqs, record.Seq)
					}
					if err != nil || !slices.Equal(seqs, test.want) {
						t.Fatalf("Query=%v, error=%v, want=%v", seqs, err, test.want)
					}
					count, countErr := st.Count(t.Context(), filter)
					if countErr != nil || count != len(test.want) {
						t.Fatalf("Count=%d, error=%v, want=%d", count, countErr, len(test.want))
					}
					native, nativeErr := unboundedQuery(t.Context(), st, filter)
					assertSameRead(t, "boundary Query", got, err, native, nativeErr)
				})
			}
		})
	}
}

func TestTimestampRangeForeignRecentWork(t *testing.T) {
	for _, population := range []int{32, 10000} {
		for _, indexed := range []bool{false, true} {
			t.Run(fmt.Sprintf("rows=%d/indexed=%t", population, indexed), func(t *testing.T) {
				st := openMembershipStore(t.Context(), t, t.TempDir())
				now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
				st.Now = func() time.Time { return now }
				if _, err := st.db.ExecContext(t.Context(), "INSERT INTO records (drive,lane,kind,at,text,source,hash) VALUES ('d','writer','note',?,'selected','post','selected')", now.Add(-time.Hour).UnixMilli()); err != nil {
					t.Fatal(err)
				}
				if _, err := st.db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?)
INSERT INTO records (drive,lane,kind,at,text,recipients,source,hash)
SELECT 'foreign','writer','note',?,'unrelated recent','["peer:other"]','post','recent-'||n FROM history`, population, now.Add(-time.Hour).UnixMilli()); err != nil {
					t.Fatal(err)
				}
				timestampRangeIndex(t, st, indexed)
				st.db.SetMaxOpenConns(1)
				filter := Filter{Drive: "d", Peer: "reader", Since: now.Add(-24 * time.Hour), Until: now, Limit: 500, Descending: true}
				query, args := st.selection(filter)
				t.Logf("TIMESTAMP_RANGE_FOREIGN_RECENT_PLAN history=%d indexed=%t plan=%v", population, indexed, queryPlan(t.Context(), t, st, query, args...))
				for _, operation := range []string{"query", "count"} {
					timestampCacheWork(t, st, true)
					if operation == "query" {
						got, err := st.Query(t.Context(), filter)
						if err != nil || len(got) != 1 || got[0].Seq != 1 {
							t.Fatalf("Query=%+v, error=%v", got, err)
						}
					} else if got, err := st.Count(t.Context(), filter); err != nil || got != 1 {
						t.Fatalf("Count=%d, error=%v", got, err)
					}
					hits, misses := timestampCacheWork(t, st, false)
					t.Logf("TIMESTAMP_RANGE_FOREIGN_RECENT_WORK history=%d indexed=%t operation=%s selected_rows=1 actual_cache_hits=%d actual_cache_misses=%d", population, indexed, operation, hits, misses)
				}
			})
		}
	}
}

func timestampRangeIndex(t *testing.T, st *Store, indexed bool) {
	t.Helper()
	if _, err := st.db.ExecContext(t.Context(), "DROP INDEX IF EXISTS records_drive_at_seq"); err != nil {
		t.Fatal(err)
	}
	if indexed {
		if _, err := st.db.ExecContext(t.Context(), "CREATE INDEX records_drive_at_seq ON records(drive, at, seq)"); err != nil {
			t.Fatal(err)
		}
	}
}

func timestampCacheWork(t *testing.T, st *Store, reset bool) (int, int) {
	t.Helper()
	connection, err := st.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	var hits, misses int
	if err := connection.Raw(func(raw any) error {
		status := raw.(sqlite.DBStatus)
		hits, _, err = status.Status(sqlite.DBStatusCacheHit, reset)
		if err != nil {
			return err
		}
		misses, _, err = status.Status(sqlite.DBStatusCacheMiss, reset)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return hits, misses
}
