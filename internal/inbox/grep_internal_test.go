package inbox

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestGrepMatchesPagedOutput(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	var history []store.Ingestion
	for i := range 1005 {
		history = append(history, store.Ingestion{
			Record:   store.Record{Drive: "d", Lane: "reviewer", Kind: kinds.Note, At: st.Now(), Text: fmt.Sprintf("REVIEW history %d", i), Source: "import:/history"},
			LineHash: fmt.Sprintf("history-%d", i),
		})
	}
	if _, err := st.IngestAll(ctx, history); err != nil {
		t.Fatal(err)
	}
	ask := testutil.Post(t, st, store.Record{Kind: kinds.Ask, Text: "REVIEW open question", To: []string{"peer:reader"}})
	testutil.Post(t, st, store.Record{Drive: "elsewhere", Kind: kinds.Answer, Text: "REVIEW answer", Re: ask.Seq})
	other := testutil.Post(t, st, store.Record{Kind: kinds.Ask, Text: "REVIEW another question"})
	testutil.Post(t, st, store.Record{Kind: kinds.Done, Text: "REVIEW closed", Resolves: other.Seq, AlsoResolves: []int64{ask.Seq}})
	for _, tt := range []struct {
		name    string
		pattern string
		filter  store.Filter
		limit   int
		budget  int
	}{
		{"default", "(?i)review", store.Filter{Drive: "d"}, 0, 6144},
		{"wide", "REVIEW", store.Filter{Drive: "d"}, 0, 1 << 20},
		{"oversized first", "REVIEW", store.Filter{Drive: "d"}, 0, 1},
		{"marks fill budget", "REVIEW", store.Filter{Drive: "d"}, 0, 230},
		{"negative limit", "REVIEW", store.Filter{Drive: "d"}, -1, 6144},
		{"one", "REVIEW", store.Filter{Drive: "d"}, 1, 6144},
		{"cross page limit", "REVIEW", store.Filter{Drive: "d"}, 501, 1 << 20},
		{"none", "absent", store.Filter{Drive: "d"}, 0, 6144},
		{"whole line regex", "ASK lane-a.*peer:reader", store.Filter{Drive: "d"}, 0, 6144},
		{"peer and filters", "REVIEW", store.Filter{Drive: "d", Peer: "reader", Kinds: []kinds.Kind{kinds.Ask}}, 0, 6144},
		{"keyset", "REVIEW", store.Filter{Drive: "d", After: 300, Before: 900}, 0, 6144},
	} {
		for _, asJSON := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/json=%t", tt.name, asJSON), func(t *testing.T) {
				pattern := regexp.MustCompile(tt.pattern)
				var got, want bytes.Buffer
				if err := Grep(ctx, st, tt.filter, pattern, tt.limit, tt.budget, asJSON, &got); err != nil {
					t.Fatal(err)
				}
				if err := pagedGrep(ctx, st, tt.filter, pattern, tt.limit, tt.budget, asJSON, &want); err != nil {
					t.Fatal(err)
				}
				if got.String() != want.String() {
					t.Fatalf("grep differs from paged output:\ngot %q\nwant %q", got.String(), want.String())
				}
			})
		}
	}
}

func pagedGrep(ctx context.Context, st *store.Store, f store.Filter, pattern *regexp.Regexp, limit, budget int, asJSON bool, out *bytes.Buffer) error {
	b := render.NewBudget(out, budget)
	matched, printed := 0, 0
	f.Descending, f.Limit = true, pageRows
	now := st.Now()
	for {
		records, err := st.Query(ctx, f)
		if err != nil {
			return err
		}
		var hits []store.Record
		for _, r := range records {
			if pattern.MatchString(render.Line(r, now)) {
				hits = append(hits, r)
			}
		}
		rendered, _, err := lines(ctx, st, hits, view{JSON: asJSON, Whole: true})
		if err != nil {
			return err
		}
		for _, line := range rendered {
			matched++
			if (limit == 0 || printed < limit) && b.Line(line) {
				printed++
			}
		}
		if len(records) < pageRows {
			break
		}
		f.Before = records[len(records)-1].Seq
	}
	if asJSON {
		return nil
	}
	if matched == 0 {
		head, err := st.MaxSeq(ctx)
		if err != nil {
			return err
		}
		b.Trailer(fmt.Sprintf("no records on %s match; store head #%d", f.Drive, head))
	}
	if matched > printed {
		b.Trailer(fmt.Sprintf("... %d more matches; narrow with --kind, --lane or --since", matched-printed))
	}
	return nil
}
