package store

import (
	"fmt"
	"testing"

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
