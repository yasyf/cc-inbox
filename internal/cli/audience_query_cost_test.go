package cli_test

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

func TestAudienceActualCLIQueryWorkDiagnostic(t *testing.T) {
	for _, population := range []int{32, 10000} {
		for _, filter := range []struct {
			name, column string
			args         []string
			reader       bool
			toKinds      bool
		}{
			{"kind", "kind", []string{"--kind", "note"}, false, false},
			{"lane", "lane", []string{"--lane", "wanted"}, false, false},
			{"topic", "topic", []string{"--topic", "wanted"}, false, false},
			{"reader kind", "kind", []string{"--reader", "root", "--kind", "note"}, true, false},
			{"reader addressed kind", "kind", []string{"--reader", "root", "--kind", "note", "--to-kind", "note"}, true, true},
		} {
			t.Run(fmt.Sprintf("rows=%d/%s", population, filter.name), func(t *testing.T) {
				var comparisons atomic.Int64
				collation := fmt.Sprintf("cci_audience_%d", tailCounterID.Add(1))
				if err := sqlite.RegisterCollationUtf8(collation, func(a, b string) int {
					comparisons.Add(1)
					return strings.Compare(a, b)
				}); err != nil {
					t.Fatal(err)
				}
				db := tailComparedStore(t, "records", filter.column, collation)
				at := time.Now().UnixMilli()
				if _, err := db.Exec(`INSERT INTO records (drive,lane,kind,at,text,topic,source,hash)
VALUES ('d','wanted','note',?,'selected local','wanted','post','selected')`, at); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`WITH RECURSIVE history(n) AS (
VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?
)
INSERT INTO records (drive,lane,kind,at,text,topic,source,hash)
SELECT 'd','noise','state',?,'unrelated','noise','post','history-'||n FROM history`, population, at); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`INSERT INTO records (drive,lane,kind,at,text,topic,recipients,source,hash)
VALUES ('foreign','noise','state',?,'selected global peer','noise','["peer:tail-limit-session"]','post','global')`, at); err != nil {
					t.Fatal(err)
				}
				want := []int64{1, int64(population + 2)}
				if filter.reader {
					if _, err := db.Exec(`INSERT INTO records (drive,lane,kind,at,text,topic,recipients,re,source,hash) VALUES
('d','noise','state',?,'selected direct','noise','["root"]',0,'post','direct'),
('foreign','root','state',?,'parent','noise','[]',0,'post','parent'),
('d','noise','state',?,'selected parent reply','noise','[]',?,'post','reply')`, at, at, at, population+4); err != nil {
						t.Fatal(err)
					}
					want = append(want, int64(population+3), int64(population+5))
				}
				if filter.toKinds {
					want = []int64{1}
				}
				for _, verb := range []string{"tail", "grep"} {
					args := []string{verb}
					if verb == "grep" {
						args = append(args, "selected")
					}
					args = append(args, "--drive", "d", "--since-seq", "0", "--json", "--budget", "0")
					args = append(args, filter.args...)
					if verb == "tail" {
						args = append(args, "-n", fmt.Sprint(len(want)))
					}
					comparisons.Store(0)
					started := time.Now()
					out, err := run(t, args...)
					elapsed := time.Since(started)
					if err != nil {
						t.Fatal(err)
					}
					ordered := slices.Clone(want)
					if verb == "grep" {
						slices.Reverse(ordered)
					}
					tailCostOutput(t, out, ordered, "")
					if comparisons.Load() == 0 {
						t.Fatal("actual CLI did not compare the selected filter column")
					}
					t.Logf("AUDIENCE_QUERY_DIAGNOSTIC verb=%s history=%d filter=%q column=%s actual_comparisons=%d output_rows=%d elapsed=%s", verb, population, filter.name, filter.column, comparisons.Load(), len(want), elapsed)
					bound := int64(2*bits.Len(uint(population)) + 8)
					if os.Getenv("CCI_AUDIENCE_REQUIRE_BOUNDED_KIND") == "1" && filter.name == "kind" && comparisons.Load() > bound {
						t.Errorf("AUDIENCE_KIND_BOUND_EXCEEDED verb=%s history=%d comparisons=%d bound=%d", verb, population, comparisons.Load(), bound)
					}
				}
			})
		}
	}
}
