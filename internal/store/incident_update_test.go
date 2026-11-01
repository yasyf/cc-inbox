package store_test

import (
	"context"
	"slices"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestAnIncidentUpdateOrClosingLineNeverOpensAnIncident(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	alert := testutil.Post(t, st, store.Record{Lane: "desk-runner", Kind: kinds.Blocker, Text: "500s in prod runtime"})
	opened := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Incident, Re: alert.Seq, Text: "INCIDENT run-grant: 500s since 11:24 AM"})
	testutil.Post(t, st, store.Record{Lane: "run-grant-evidence", Kind: kinds.Incident, Re: opened.Seq, Text: "11:40 AM: 12 failures in the last 10 min"})
	open := func() []int64 {
		t.Helper()
		items, err := st.OpenItems(ctx, "d")
		if err != nil {
			t.Fatal(err)
		}
		among, err := st.OpenAmong(ctx, "d", []int64{opened.Seq, opened.Seq + 1, opened.Seq + 2})
		if err != nil {
			t.Fatal(err)
		}
		seqs := func(records []store.Record) []int64 {
			var out []int64
			for _, r := range records {
				if r.Kind == kinds.Incident {
					out = append(out, r.Seq)
				}
			}
			return out
		}
		if got, want := seqs(among), seqs(items); !slices.Equal(got, want) {
			t.Fatalf("OpenAmong incidents = %v, OpenItems incidents = %v", got, want)
		}
		return seqs(items)
	}
	if got := open(); !slices.Equal(got, []int64{opened.Seq}) {
		t.Fatalf("open incidents = %v, want only the opener #%d: an update replying to it opened another", got, opened.Seq)
	}
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Incident, Resolves: opened.Seq, Text: "INCIDENT run-grant CLOSED: the 500s stopped"})
	if got := open(); len(got) != 0 {
		t.Fatalf("open incidents = %v after root's closing line resolved the opener, want none", got)
	}
}
