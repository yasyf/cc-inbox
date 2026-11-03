package store

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

type namedFixture struct {
	ask, report, landed, card, doc Record
}

func seedNamed(t *testing.T, st *Store) namedFixture {
	t.Helper()
	post := func(r Record) Record {
		t.Helper()
		r.Source = "post"
		if r.Drive == "" {
			r.Drive = "d"
		}
		got, _, err := st.Post(t.Context(), r, 0)
		if err != nil {
			t.Fatalf("Post(%+v) error = %v", r, err)
		}
		return got
	}
	f := namedFixture{
		ask:    post(Record{Lane: "lane-a", Kind: kinds.Ask, Text: "ship it?", Topic: "rollout"}),
		report: post(Record{Lane: "lane-b", Kind: kinds.Report, Text: "rollout halfway", Topic: "rollout"}),
	}
	post(Record{Drive: "e", Lane: "lane-a", Kind: kinds.Ask, Text: "another drive's rollout", Topic: "rollout"})
	f.landed = post(Record{Lane: "lane-b", Kind: kinds.Landed, Text: "merged", Refs: Refs{PRs: []PR{{Repo: "yasyf/cc-inbox", Number: 7}}}})
	f.card = post(Record{Lane: "lane-c", Kind: kinds.Blocked, Text: "waiting on card-merge-triage."})
	f.doc = post(Record{Lane: "lane-c", Kind: kinds.Note, Text: "ruling on aliases", Refs: Refs{CCN: "doc-c3230a2"}})
	return f
}

func resolveNamed(t *testing.T, st *Store, ref string, resolving bool) (int64, error) {
	t.Helper()
	n, err := ParseNamedRef(ref)
	if err != nil {
		t.Fatalf("ParseNamedRef(%q) error = %v", ref, err)
	}
	return st.ResolveNamed(t.Context(), n, "d", resolving)
}

func (f namedFixture) cases() []struct {
	ref       string
	resolving bool
	want      int64
} {
	return []struct {
		ref       string
		resolving bool
		want      int64
	}{
		{"t:rollout", false, f.ask.Seq},
		{"t:rollout", true, f.ask.Seq},
		{"pr#7", false, f.landed.Seq},
		{"card:merge-triage", true, f.card.Seq},
		{"ccn:doc-c32", false, f.doc.Seq},
		{"ccn:ruling on", false, f.doc.Seq},
		{"last:report", false, f.report.Seq},
		{"last:ask:lane-a", false, f.ask.Seq},
	}
}

func TestNamedRefsNameOneRecordOrRefuseWithCandidates(t *testing.T) {
	st, err := Open(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	f := seedNamed(t, st)
	for _, c := range f.cases() {
		got, err := resolveNamed(t, st, c.ref, c.resolving)
		if err != nil || got != c.want {
			t.Errorf("ResolveNamed(%q, resolving=%v) = %d, %v; want %d", c.ref, c.resolving, got, err, c.want)
		}
	}

	if _, _, err := st.Post(t.Context(), Record{Drive: "d", Lane: "lane-b", Kind: kinds.Answer, Text: "yes", Resolves: f.ask.Seq, Source: "post"}, 0); err != nil {
		t.Fatal(err)
	}
	refusals := []struct {
		ref       string
		resolving bool
		want      []string
	}{
		{"t:rollout", false, []string{"none is open", fmt.Sprintf("#%d", f.ask.Seq), fmt.Sprintf("#%d", f.report.Seq)}},
		{"t:rollout", true, []string{"no open record"}},
		{"t:nope", false, []string{"names no record"}},
		{"pr#8", false, []string{"names no record"}},
	}
	for _, c := range refusals {
		_, err := resolveNamed(t, st, c.ref, c.resolving)
		for _, want := range c.want {
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("ResolveNamed(%q, resolving=%v) error = %v, want it to name %q", c.ref, c.resolving, err, want)
			}
		}
	}

	asks := make([]int64, 0, 2)
	for i := range 2 {
		r, _, err := st.Post(t.Context(), Record{Drive: "d", Lane: "lane-a", Kind: kinds.Ask, Text: fmt.Sprintf("deploy stack %d now?", i), Topic: "deploy", Source: "post"}, 0)
		if err != nil {
			t.Fatal(err)
		}
		asks = append(asks, r.Seq)
	}
	_, err = resolveNamed(t, st, "t:deploy", false)
	for _, want := range []string{"2 open records", fmt.Sprintf("#%d", asks[0]), fmt.Sprintf("#%d", asks[1])} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("ResolveNamed(t:deploy) error = %v, want it to name %q", err, want)
		}
	}
}

func TestParseNamedRefRefusesMalformedAliases(t *testing.T) {
	for _, ref := range []string{"t:", "pr#x", "pr#yasyf/cc-inbox#7", "last:bogus", "ccn:"} {
		if _, err := ParseNamedRef(ref); err == nil {
			t.Errorf("ParseNamedRef(%q) = nil error", ref)
		}
	}
	for _, ref := range []string{"#12", "12", "#12@abcd"} {
		if IsNamedRef(ref) {
			t.Errorf("IsNamedRef(%q) = true", ref)
		}
	}
}

func TestNamedRefPlansMatchAtOneAndOneThousandRecords(t *testing.T) {
	plans := map[int]map[string][]string{}
	for _, n := range []int{1, 1000} {
		st, err := Open(context.Background(), t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = st.Close() })
		if _, err := st.db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (
	VALUES(1) UNION ALL SELECT n + 1 FROM history WHERE n < ?
)
INSERT INTO records (drive, lane, kind, at, text, topic, refs, source, hash, origin_seq)
SELECT 'd', 'lane-a', CASE n % 2 WHEN 0 THEN 'report' ELSE 'ask' END, n, 'noise card-noise-' || n, 'noise-' || n,
'{"prs":[' || (1000 + n) || '],"ccn":"doc-noise-' || n || '"}', 'post', 'noise-' || n, n FROM history`, n); err != nil {
			t.Fatal(err)
		}
		f := seedNamed(t, st)
		plans[n] = map[string][]string{}
		for _, c := range f.cases() {
			got, err := resolveNamed(t, st, c.ref, c.resolving)
			if err != nil || got != c.want {
				t.Fatalf("N=%d ResolveNamed(%q) = %d, %v; want %d", n, c.ref, got, err, c.want)
			}
			ref, _ := ParseNamedRef(c.ref)
			for _, openers := range []bool{false, true} {
				where, args := ref.where("d", openers)
				plan := explainNamed(t, st, "SELECT "+columns+" FROM records WHERE "+where+" ORDER BY seq DESC LIMIT 5", args)
				for _, step := range plan {
					if strings.Contains(step, "SCAN records") || strings.Contains(step, "SCAN record_ref_keys") {
						t.Fatalf("N=%d %s plan scans: %v", n, c.ref, plan)
					}
				}
				plans[n][fmt.Sprintf("%s/%v", c.ref, openers)] = plan
			}
		}
	}
	for key, plan := range plans[1] {
		if !slices.Equal(plan, plans[1000][key]) {
			t.Errorf("%s plan at N=1 %v differs from N=1000 %v", key, plan, plans[1000][key])
		}
	}
}

func explainNamed(t *testing.T, st *Store, query string, args []any) []string {
	t.Helper()
	rows, err := st.db.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return plan
}
