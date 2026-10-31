package store

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"
)

func DenseGrepDiagnosticFixture(t *testing.T, population int) (*Store, time.Time, func(bool) (int64, int, int), func(Filter) (string, []any, []string)) {
	t.Helper()
	var comparisons atomic.Int64
	collation := fmt.Sprintf("cci_dense_grep_drive_%d", laneCollationID.Add(1))
	if err := sqlite.RegisterCollationUtf8(collation, func(a, b string) int {
		comparisons.Add(1)
		return strings.Compare(a, b)
	}); err != nil {
		t.Fatal(err)
	}
	st := countedColumnStore(t, "drive", collation)
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	st.Now = func() time.Time { return now }
	if _, err := st.db.ExecContext(t.Context(), `WITH RECURSIVE dense(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM dense WHERE n<?)
INSERT INTO records (drive,lane,kind,at,text,recipients,source,hash,origin,origin_seq)
SELECT 'd','writer','note',?-3600000-((n*7919)%10000)*1000,'pure port dense '||n,'[]','post','dense-'||n,'feedfacefeedface',n FROM dense`, population, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	timestampRangeIndex(t, st, true)
	st.db.SetMaxOpenConns(1)
	measure := func(reset bool) (int64, int, int) {
		if reset {
			comparisons.Store(0)
		}
		hits, misses := timestampCacheWork(t, st, reset)
		return comparisons.Load(), hits, misses
	}
	plan := func(filter Filter) (string, []any, []string) {
		query, args := st.selectionColumns(filter, textColumns)
		return query, args, queryPlan(t.Context(), t, st, query, args...)
	}
	return st, now, measure, plan
}
