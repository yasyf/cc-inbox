package store

import (
	"crypto/sha256"
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

var timestampDiagnosticIndexes = []struct {
	name string
	ddl  string
}{
	{"missing", ""},
	{"drive-at", "CREATE INDEX records_drive_at_seq ON records(drive, at, seq)"},
	{"at", "CREATE INDEX records_at_seq ON records(at, seq)"},
}

func TestTimestampIndexDiagnosticWork(t *testing.T) {
	for _, population := range []int{32, 10000} {
		for _, index := range timestampDiagnosticIndexes {
			for _, peer := range []bool{false, true} {
				t.Run(fmt.Sprintf("rows=%d/index=%s/peer=%t", population, index.name, peer), func(t *testing.T) {
					var comparisons atomic.Int64
					collation := fmt.Sprintf("cci_timestamp_drive_%d", laneCollationID.Add(1))
					if err := sqlite.RegisterCollationUtf8(collation, func(a, b string) int {
						comparisons.Add(1)
						return strings.Compare(a, b)
					}); err != nil {
						t.Fatal(err)
					}
					st := countedColumnStore(t, "drive", collation)
					now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
					st.Now = func() time.Time { return now }
					seedTimestampDiagnostic(t, st, population, now)
					for _, ddl := range []string{"DROP INDEX IF EXISTS records_at_seq", "DROP INDEX IF EXISTS records_drive_at_seq", index.ddl} {
						if ddl != "" {
							if _, err := st.db.ExecContext(t.Context(), ddl); err != nil {
								t.Fatal(err)
							}
						}
					}
					filter := Filter{Drive: "d", Since: now.Add(-24 * time.Hour), Until: now, Limit: 500, Descending: true}
					want := []int64{int64(population + 4), 1}
					if peer {
						filter.Peer = "reader"
						want = []int64{int64(population + 4), int64(population + 2), 1}
					}
					bound := int64(2*bits.Len(uint(population)) + 16)
					for _, operation := range []string{"query", "count"} {
						comparisons.Store(0)
						if operation == "query" {
							got, err := st.Query(t.Context(), filter)
							seqs := make([]int64, 0, len(got))
							for _, record := range got {
								seqs = append(seqs, record.Seq)
							}
							if err != nil || !slices.Equal(seqs, want) {
								t.Fatalf("Query seqs=%v, error=%v, want=%v", seqs, err, want)
							}
						} else if got, err := st.Count(t.Context(), filter); err != nil || got != len(want) {
							t.Fatalf("Count=%d, error=%v, want=%d", got, err, len(want))
						}
						measured := comparisons.Load()
						if measured == 0 {
							t.Fatal("actual operation made no drive comparisons")
						}
						t.Logf("TIMESTAMP_INDEX_WORK operation=%s history=%d index=%s peer=%t actual_drive_comparisons=%d diagnostic_bound=%d bounded=%t", operation, population, index.name, peer, measured, bound, measured <= bound)
						if os.Getenv("CCI_TIMESTAMP_REQUIRE_MISSING_BOUND") == "1" && index.name == "missing" && peer && measured > bound {
							t.Errorf("TIMESTAMP_INDEX_UNBOUNDED operation=%s history=%d index=missing peer=true comparisons=%d bound=%d", operation, population, measured, bound)
						}
					}
					query, args := st.selection(filter)
					t.Logf("TIMESTAMP_INDEX_PLAN history=%d index=%s peer=%t sql_sha256=%x plan=%v", population, index.name, peer, sha256.Sum256([]byte(query)), queryPlan(t.Context(), t, st, query, args...))
					where, params := filter.bounded(st.Self(), st.Now())
					t.Logf("TIMESTAMP_INDEX_COUNT_PLAN history=%d index=%s peer=%t plan=%v", population, index.name, peer, queryPlan(t.Context(), t, st, "SELECT COUNT(*) FROM records WHERE "+where, params...))
					t.Logf("TIMESTAMP_INDEX_VM_PROGRAM history=%d index=%s peer=%t program=%v", population, index.name, peer, timestampDiagnosticVM(t, st, query, args...))
				})
			}
		}
	}
}

func TestTimestampIndexNativeParity(t *testing.T) {
	variants := slices.Clone(membershipVariants)
	recent := time.Date(2026, 10, 31, 11, 0, 0, 0, time.UTC).UnixMilli()
	variants = append(variants, []struct {
		name string
		sets []membershipSet
	}{
		{"stale integer at", []membershipSet{{4, "at=0"}}},
		{"old real at", []membershipSet{{4, "at=1.5"}}},
		{"recent real at", []membershipSet{{4, fmt.Sprintf("at=%d.5", recent)}}},
		{"raw text at", []membershipSet{{4, "at='invalid'"}}},
		{"raw numeric suffix at", []membershipSet{{4, fmt.Sprintf("at='%dx'", recent)}}},
		{"raw blob at", []membershipSet{{4, "at=X'FF'"}}},
	}...)
	for _, index := range timestampDiagnosticIndexes[1:] {
		for _, variant := range variants {
			t.Run(index.name+"/"+variant.name, func(t *testing.T) {
				st := openMembershipStore(t.Context(), t, t.TempDir())
				now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
				st.Now = func() time.Time { return now }
				seedMembershipFixture(t.Context(), t, st, now)
				for _, ddl := range []string{"DROP INDEX IF EXISTS records_at_seq", "DROP INDEX IF EXISTS records_drive_at_seq"} {
					if _, err := st.db.ExecContext(t.Context(), ddl); err != nil {
						t.Fatal(err)
					}
				}
				for _, set := range variant.sets {
					if _, err := st.db.ExecContext(t.Context(), "UPDATE records SET "+set.set+" WHERE seq=?", set.seq); err != nil {
						t.Fatal(err)
					}
				}
				filters := []Filter{
					{Drive: "d", Since: now.Add(-24 * time.Hour), Limit: 2, Descending: true},
					{Drive: "d", Peer: "reader", Since: now.Add(-24 * time.Hour), Limit: 2, Descending: true},
					{Drive: "d", Peer: "reader", Since: now.Add(-24 * time.Hour)},
					{Drive: "d", Peer: "reader", Since: now.Add(-24 * time.Hour), Until: now},
					{Drive: "d", Peer: "reader", Until: now},
					{Drive: "d", Peer: "reader", Since: now.Add(-24 * time.Hour), Origin: "feedfacefeedface", Limit: 2, Descending: true},
					{Drive: "d", Peer: "reader", Since: now.Add(-24 * time.Hour), PeerAfter: 4, Limit: 2, Descending: true},
					{Drive: "d", Peer: "reader", For: "root", Since: now.Add(-24 * time.Hour), Limit: 2, Descending: true},
					{Drive: "d", To: "api", Since: now.Add(-24 * time.Hour)},
					{Drive: "d", Peer: "reader", Stack: "p/e", Since: now.Add(-24 * time.Hour)},
				}
				type result struct {
					rows     []Record
					queryErr error
					count    int
					countErr error
				}
				before := make([]result, len(filters))
				for i, filter := range filters {
					before[i].rows, before[i].queryErr = st.Query(t.Context(), filter)
					before[i].count, before[i].countErr = st.Count(t.Context(), filter)
				}
				if _, err := st.db.ExecContext(t.Context(), index.ddl); err != nil {
					t.Fatal(err)
				}
				for i, filter := range filters {
					rows, queryErr := st.Query(t.Context(), filter)
					count, countErr := st.Count(t.Context(), filter)
					assertSameRead(t, fmt.Sprintf("indexed Query filter=%+v", filter), rows, queryErr, before[i].rows, before[i].queryErr)
					assertSameRead(t, fmt.Sprintf("indexed Count filter=%+v", filter), count, countErr, before[i].count, before[i].countErr)
				}
			})
		}
	}
}

func seedTimestampDiagnostic(t *testing.T, st *Store, population int, now time.Time) {
	t.Helper()
	for _, insert := range []struct {
		query string
		args  []any
	}{
		{"INSERT INTO records (drive,lane,kind,at,text,source,hash) VALUES ('d','writer','note',?,'pure port selected early','post','early')", []any{now.Add(-time.Hour).UnixMilli()}},
		{`WITH RECURSIVE history(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?)
INSERT INTO records (drive,lane,kind,at,text,source,hash)
SELECT 'd','writer','state',?,'stale','post','noise-'||n FROM history`, []any{population, now.Add(-48 * time.Hour).UnixMilli()}},
		{`INSERT INTO records (drive,lane,kind,at,text,recipients,source,hash) VALUES
('foreign','writer','state',?,'pure port selected global','["peer:reader"]','post','global'),
('d','writer','note',?,'pure port stale high seq','[]','post','stale-high'),
('d','writer','state',?,'pure port selected latest','[]','post','latest')`, []any{now.Add(-time.Hour).UnixMilli(), now.Add(-48 * time.Hour).UnixMilli(), now.Add(-time.Hour).UnixMilli()}},
	} {
		if _, err := st.db.ExecContext(t.Context(), insert.query, insert.args...); err != nil {
			t.Fatal(err)
		}
	}
}

func timestampDiagnosticVM(t *testing.T, st *Store, query string, args ...any) []string {
	t.Helper()
	rows, err := st.db.QueryContext(t.Context(), "EXPLAIN "+query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var program []string
	for rows.Next() {
		var address, p1, p2, p3, p5 int
		var opcode string
		var p4, comment any
		if err := rows.Scan(&address, &opcode, &p1, &p2, &p3, &p4, &p5, &comment); err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(opcode, "Seek") || strings.HasPrefix(opcode, "Idx") || slices.Contains([]string{"OpenRead", "Rewind", "Next", "Prev", "SorterOpen", "SorterInsert", "SorterSort", "SorterNext", "Sort", "Yield"}, opcode) {
			program = append(program, fmt.Sprintf("%d:%s(%d,%d,%d,%v,%d)", address, opcode, p1, p2, p3, p4, p5))
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return program
}
