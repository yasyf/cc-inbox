package store

import (
	"context"
	"strings"
	"testing"
)

func TestClaimsSearchLaneSessionsByIndex(t *testing.T) {
	st, err := Open(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	for _, f := range []ClaimFilter{{Kind: "pr", Key: "42874"}, {Kind: "build", Key: "4860"}, {Rel: "opened"}} {
		query, params := f.query()
		rows, err := st.db.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+query, params...)
		if err != nil {
			t.Fatal(err)
		}
		var plan []string
		for rows.Next() {
			var (
				id, parent, unused int
				detail             string
			)
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			plan = append(plan, detail)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		for _, step := range plan {
			if strings.HasPrefix(step, "SCAN s") {
				t.Fatalf("Claims(%+v) scans every session per record:\n%s", f, strings.Join(plan, "\n"))
			}
		}
	}
}
