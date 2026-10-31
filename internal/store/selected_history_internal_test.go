package store

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"modernc.org/sqlite"
)

func selectedSequenceStore(t *testing.T, count int) (*Store, *atomic.Int64, []int64) {
	t.Helper()
	var comparisons atomic.Int64
	collation := fmt.Sprintf("cci_selected_%d", laneCollationID.Add(1))
	if err := sqlite.RegisterCollationUtf8(collation, func(a, b string) int {
		comparisons.Add(1)
		return strings.Compare(a, b)
	}); err != nil {
		t.Fatal(err)
	}
	st := countedColumnStore(t, "drive", collation)
	st.SetOwner(true)
	_, err := st.db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (
VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?
)
INSERT INTO records(seq,id,drive,lane,kind,at,text,source,hash,origin,origin_seq)
SELECT n,'selected-'||n,'drive','root','hold',n,'history','post','selected-'||n,?,n FROM history`, count, st.Self())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(t.Context(), `INSERT INTO records(seq,id,drive,lane,kind,at,text,source,hash,origin,origin_seq)
VALUES(?,'foreign','elsewhere','root','hold',1,'foreign','post','foreign',?,?)`, count+1, st.Self(), count+1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(t.Context(), "UPDATE records SET expires_at=1 WHERE seq=1"); err != nil {
		t.Fatal(err)
	}
	comparisons.Store(0)
	return st, &comparisons, []int64{int64(count), 1, 1, int64(count + 1), int64(count + 2)}
}

func TestSelectedRecordsHistoryScanNegative(t *testing.T) {
	if os.Getenv("CCI_SELECTED_HISTORY_NEGATIVE") != "1" {
		t.Skip("immutable unrestricted history-query negative control")
	}
	for _, count := range []int{32, 10000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			st, comparisons, _ := selectedSequenceStore(t, count)
			records, err := st.Query(t.Context(), Filter{Drive: "drive", IncludeExpired: true})
			if err != nil || len(records) != count {
				t.Fatalf("history query returned %d records, %v", len(records), err)
			}
			if got := comparisons.Load(); got > 3 {
				t.Fatalf("selected lookup compared drive %d times for %d history rows, want <=3", got, count)
			}
		})
	}
}
