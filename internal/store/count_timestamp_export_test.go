package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/node"

	"modernc.org/sqlite"
)

func CountTimestampDenseFixture(t *testing.T, population int, indexed bool) (*Store, time.Time, func(bool) (int64, int, int)) {
	t.Helper()
	var comparisons atomic.Int64
	collation := fmt.Sprintf("cci_count_timestamp_drive_%d", laneCollationID.Add(1))
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
	timestampRangeIndex(t, st, indexed)
	identityHome := filepath.Join("..", "..", ".ci-count-timestamp-identity")
	if os.Getenv("CCI_COUNT_TIMESTAMP_RECORD") == "1" {
		identityHome = filepath.Join("..", identityHome)
	}
	identity, err := node.Load(identityHome)
	if err != nil {
		t.Fatal(err)
	}
	st.node = identity
	st.db.SetMaxOpenConns(1)
	measure := func(reset bool) (int64, int, int) {
		if reset {
			comparisons.Store(0)
		}
		hits, misses := timestampCacheWork(t, st, reset)
		return comparisons.Load(), hits, misses
	}
	return st, now, measure
}

func DenseReadDiagnosticPlan(t *testing.T, st *Store, filter Filter, count bool) (string, []any, []string) {
	t.Helper()
	query, args := st.selection(filter)
	if count {
		where, params := timestampCountWhere(filter, st.Self(), st.Now())
		query, args = "SELECT COUNT(*) FROM records WHERE "+where, params
	}
	return query, args, queryPlan(t.Context(), t, st, query, args...)
}
