package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"modernc.org/sqlite"
)

func TestSelectedClosureKeyComparisonsStayBounded(t *testing.T) {
	for _, history := range []int{32, 10000} {
		t.Run(strconv.Itoa(history), func(t *testing.T) {
			var comparisons atomic.Int64
			collation := fmt.Sprintf("cci_closer_key_%d", laneCollationID.Add(1))
			if err := sqlite.RegisterCollationUtf8(collation, func(a, b string) int {
				comparisons.Add(1)
				return strings.Compare(a, b)
			}); err != nil {
				t.Fatal(err)
			}
			st := countedColumnStore(t, "key", collation)
			ctx := context.Background()
			if _, err := st.db.ExecContext(ctx, `INSERT INTO records (drive,lane,kind,at,text,source,hash) VALUES ('d','selected','ask',1,'selected','post','selected')`); err != nil {
				t.Fatal(err)
			}
			if _, err := st.db.ExecContext(ctx, `WITH RECURSIVE history(n) AS (
VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?
)
INSERT INTO records (drive,lane,kind,at,text,re,topic,source,hash)
SELECT 'd','history','answer',n+1,'unrelated',1000000+n,'other-'||n,'post','history-'||n FROM history`, history); err != nil {
				t.Fatal(err)
			}
			for _, present := range []bool{false, true} {
				if present {
					if _, err := st.db.ExecContext(ctx, `INSERT INTO records (drive,lane,kind,at,text,re,source,hash) VALUES ('d','closer','answer',2,'actual candidate',1,'post','closer')`); err != nil {
						t.Fatal(err)
					}
				}
				comparisons.Store(0)
				got, err := st.OpenAmong(ctx, "d", []int64{1})
				if err != nil || (len(got) == 0) != present {
					t.Fatalf("selected opener presentCloser=%t: %+v, %v", present, got, err)
				}
				if n := comparisons.Load(); n == 0 || n > 64 {
					t.Fatalf("selected closer key comparisons=%d, history=%d; want1..64", n, history)
				}
				t.Logf("SELECTED_CLOSURE_KEY_WORK history=%d selected=1 presentCloser=%t actual_key_comparisons=%d", history, present, comparisons.Load())
			}
			comparisons.Store(0)
			if _, err := st.OpenAmong(ctx, "d", nil); err != nil || comparisons.Load() != 0 {
				t.Fatalf("empty selection key work=%d, %v", comparisons.Load(), err)
			}
			for _, query := range []struct {
				sql, index string
				args       []any
			}{
				{selectedClosersSQL, "record_closer_keys_lookup (drive=? AND axis=? AND key=?)", []any{`[["re","1"]]`, "d"}},
				{selectedResolversSQL, "records_resolves_target", []any{`[1]`}},
			} {
				rows, err := st.db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query.sql, query.args...)
				if err != nil {
					t.Fatal(err)
				}
				var details []string
				for rows.Next() {
					var id, parent, unused int
					var detail string
					if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
						t.Fatal(err)
					}
					details = append(details, detail)
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				if err := rows.Close(); err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(strings.Join(details, "\n"), query.index) {
					t.Fatalf("selected closure plan lacks %s: %v", query.index, details)
				}
				t.Logf("selected closure plan: %v", details)
			}
		})
	}
}
