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

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func TestAudienceQueryPlanDiagnostic(t *testing.T) {
	for _, population := range []int{32, 10000} {
		t.Run(fmt.Sprint(population), func(t *testing.T) {
			ctx := t.Context()
			st := openMembershipStore(ctx, t, t.TempDir())
			if _, err := st.db.ExecContext(ctx, `WITH RECURSIVE history(n) AS (
VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?
)
INSERT INTO records (drive,lane,kind,at,text,topic,source,hash)
SELECT 'd','noise','state',n,'unrelated','noise','post','history-'||n FROM history`, population); err != nil {
				t.Fatal(err)
			}
			for _, filter := range []Filter{
				{Drive: "d", Peer: "reader", Kinds: []kinds.Kind{kinds.Note}, Limit: 2, Descending: true},
				{Drive: "d", Peer: "reader", Lanes: []string{"wanted"}, Limit: 2, Descending: true},
				{Drive: "d", Peer: "reader", Topics: []string{"wanted"}, Limit: 2, Descending: true},
				{Drive: "d", Peer: "reader", For: "root", Kinds: []kinds.Kind{kinds.Note}, Limit: 4, Descending: true},
			} {
				query, args := st.selection(filter)
				t.Logf("AUDIENCE_PLAN_DIAGNOSTIC history=%d filter=%+v plan=%v", population, filter, queryPlan(ctx, t, st, query, args...))
				where, args := filter.bounded(st.Self(), st.Now())
				t.Logf("AUDIENCE_COUNT_PLAN_DIAGNOSTIC history=%d filter=%+v plan=%v", population, filter, queryPlan(ctx, t, st, "SELECT COUNT(*) FROM records WHERE "+where, args...))
			}
		})
	}
}

func TestAudienceKindQueryAndCountWork(t *testing.T) {
	for _, population := range []int{32, 10000} {
		t.Run(fmt.Sprint(population), func(t *testing.T) {
			var comparisons atomic.Int64
			collation := fmt.Sprintf("cci_audience_kind_%d", laneCollationID.Add(1))
			if err := sqlite.RegisterCollationUtf8(collation, func(a, b string) int {
				comparisons.Add(1)
				return strings.Compare(a, b)
			}); err != nil {
				t.Fatal(err)
			}
			st := countedColumnStore(t, "kind", collation)
			now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
			st.Now = func() time.Time { return now }
			ctx := t.Context()
			for _, query := range []string{
				`INSERT INTO records (drive,lane,kind,at,text,source,hash) VALUES ('d','wanted','note',1,'selected','post','selected')`,
				`WITH RECURSIVE history(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<` + fmt.Sprint(population) + `)
INSERT INTO records (drive,lane,kind,at,text,source,hash) SELECT 'd','noise','state',n,'unrelated','post','history-'||n FROM history`,
				`INSERT INTO records (drive,lane,kind,at,text,recipients,source,hash) VALUES ('foreign','noise','state',1,'global','["peer:reader"]','post','global')`,
				`INSERT INTO records (drive,lane,kind,at,text,source,hash) VALUES ('d','wanted','ask',1,'second selected kind','post','ask')`,
			} {
				if _, err := st.db.ExecContext(ctx, query); err != nil {
					t.Fatal(err)
				}
			}
			for _, selected := range []struct {
				kinds  []kinds.Kind
				unique int
				want   []int64
			}{
				{[]kinds.Kind{kinds.Note}, 1, []int64{1, int64(population + 2)}},
				{[]kinds.Kind{kinds.Note, kinds.Ask, kinds.Note}, 2, []int64{1, int64(population + 2), int64(population + 3)}},
			} {
				filter := Filter{Drive: "d", Peer: "reader", Kinds: selected.kinds}
				bound := int64(2*selected.unique*bits.Len(uint(population)) + 8)
				for _, operation := range []string{"query", "count"} {
					comparisons.Store(0)
					if operation == "query" {
						got, err := st.Query(ctx, filter)
						var seqs []int64
						for _, record := range got {
							seqs = append(seqs, record.Seq)
						}
						if err != nil || !slices.Equal(seqs, selected.want) {
							t.Fatalf("query seqs=%v, error=%v; want %v", seqs, err, selected.want)
						}
					} else if got, err := st.Count(ctx, filter); err != nil || got != len(selected.want) {
						t.Fatalf("count=%d, error=%v; want %d", got, err, len(selected.want))
					}
					t.Logf("AUDIENCE_STORE_WORK operation=%s history=%d unique_kinds=%d comparisons=%d bound=%d", operation, population, selected.unique, comparisons.Load(), bound)
					if comparisons.Load() == 0 || os.Getenv("CCI_AUDIENCE_REQUIRE_BOUNDED_KIND") == "1" && comparisons.Load() > bound {
						t.Errorf("AUDIENCE_STORE_BOUND_EXCEEDED operation=%s history=%d unique_kinds=%d comparisons=%d bound=%d", operation, population, selected.unique, comparisons.Load(), bound)
					}
				}
			}
		})
	}
}

func TestAudienceKindsPreserveNativeSpecialPaths(t *testing.T) {
	for _, variant := range membershipVariants {
		t.Run(variant.name, func(t *testing.T) {
			ctx := t.Context()
			st := openMembershipStore(ctx, t, t.TempDir())
			now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
			st.Now = func() time.Time { return now }
			seedMembershipFixture(ctx, t, st, now)
			for _, set := range variant.sets {
				if _, err := st.db.ExecContext(ctx, "UPDATE records SET "+set.set+" WHERE seq=?", set.seq); err != nil {
					t.Fatal(err)
				}
			}
			for _, filter := range []Filter{
				{Drive: "d", Peer: "reader", Kinds: []kinds.Kind{kinds.Note}, Origin: "feedfacefeedface", Limit: 2, Descending: true},
				{Drive: "d", Peer: "reader", Kinds: []kinds.Kind{kinds.Note}, PeerAfter: 4, After: 1, Limit: 2, Descending: true},
				{Drive: "d", Peer: "reader", Kinds: []kinds.Kind{kinds.Note}, For: "root", ToKinds: []kinds.Kind{kinds.Ask}},
			} {
				got, gotErr := st.Query(ctx, filter)
				want, wantErr := unboundedQuery(ctx, st, filter)
				assertSameRead(t, "special-path query", got, gotErr, want, wantErr)
				count, countErr := st.Count(ctx, filter)
				legacy, legacyErr := unboundedCount(ctx, st, filter)
				assertSameRead(t, "special-path count", count, countErr, legacy, legacyErr)
			}
		})
	}
}
