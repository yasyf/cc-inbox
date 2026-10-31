package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"modernc.org/sqlite"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

var incidentCostID atomic.Uint64

func TestIncidentMembershipSeeksIgnoreUnrelatedHistory(t *testing.T) {
	for _, population := range []int{32, 10000} {
		t.Run(fmt.Sprintf("rows=%d", population), func(t *testing.T) {
			ctx := context.Background()
			var comparisons atomic.Int64
			collation := fmt.Sprintf("cci_incident_comparisons_%d", incidentCostID.Add(1))
			if err := sqlite.RegisterCollationUtf8(collation, func(a, b string) int { comparisons.Add(1); return strings.Compare(a, b) }); err != nil {
				t.Fatal(err)
			}
			st, err := Open(ctx, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			folder := t.TempDir()
			paths := make([]string, 0, 3)
			for i, body := range []string{"unrelated #900001", "unrelated #900002", "selected #29020"} {
				path := filepath.Join(folder, fmt.Sprintf("body%d", i))
				if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
					t.Fatal(err)
				}
				paths = append(paths, path)
			}
			if _, err := st.db.ExecContext(ctx, `WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<?)
INSERT INTO records(drive,lane,kind,at,text,refs,source,hash)
SELECT 'd','other','incident',x,'unrelated #'||(100000+x),json_object('path',CASE x%2 WHEN 0 THEN ? ELSE ? END),'post','noise-'||x FROM n`, population, paths[0], paths[1]); err != nil {
				t.Fatal(err)
			}
			selected, _, err := st.Post(ctx, Record{Drive: "d", Lane: "root", Kind: kinds.Incident, Text: "selected", Source: "post", Refs: Refs{Path: paths[2]}}, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := st.ProvisionIncidentScope(ctx, "d"); err != nil {
				t.Fatal(err)
			}
			index, err := OpenIncidentIndex(ctx, st)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = index.Close() })
			for _, name := range []string{"incident_record_prs", "incident_body_prs"} {
				var definition, lookup string
				for _, object := range incidentObjects {
					if object.name == name {
						definition = object.sql
					}
					if object.name == name+"_lookup" {
						lookup = object.sql
					}
				}
				for _, statement := range []string{
					"ALTER TABLE " + name + " RENAME TO measured_" + name,
					"DROP INDEX " + name + "_lookup",
					strings.Replace(definition, "pr TEXT NOT NULL", "pr TEXT COLLATE "+collation+" NOT NULL", 1),
					"INSERT INTO " + name + " SELECT * FROM measured_" + name,
					lookup,
					"DROP TABLE measured_" + name,
				} {
					if _, err := st.db.ExecContext(ctx, statement); err != nil {
						t.Fatal(err)
					}
				}
			}
			if _, err := index.Holds(ctx, st, "d", []string{"29020"}); err != nil {
				t.Fatal(err)
			}
			before := index.Stats()
			comparisons.Store(0)
			got, err := index.Holds(ctx, st, "d", []string{"29020"})
			after := index.Stats()
			if err != nil || len(got) != 1 || got["29020"].Seq != selected.Seq {
				t.Fatalf("holds=%v, %v", got, err)
			}
			if before != after {
				t.Fatalf("unchanged query touched files: before=%+v after=%+v", before, after)
			}
			if count := comparisons.Load(); count <= 0 || count >= 128 {
				t.Fatalf("native key comparisons=%d", count)
			}
			t.Logf("rows=%d native_PR_key_comparisons=%d warm_body_stats=0 warm_body_opens=0 warm_body_bytes=0", population, comparisons.Load())
		})
	}
}
