package store_test

import (
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestTrigramsNarrowAGrepWithoutLosingARecord(t *testing.T) {
	st, _ := testutil.Store(t)
	inText := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "merge:49638 went out"})
	inTopic := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "plain", Topic: "merge:49638"})
	testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "nothing to see"})
	if st.TrigramsReady(t.Context()) {
		t.Fatal("trigrams ready before they were built")
	}
	if err := st.IndexTrigrams(t.Context(), noPause); err != nil {
		t.Fatal(err)
	}
	if !st.TrigramsReady(t.Context()) {
		t.Fatal("trigrams not ready after the fill")
	}
	after := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "MERGE:49638 again"})
	if st.TrigramsNarrow(t.Context(), []string{"12345"}, nil) != "" {
		t.Fatal("a word that may name a number column narrowed")
	}
	for _, tt := range []struct {
		name        string
		either, all []string
		want        []int64
	}{
		{"one word", []string{"merge:49638"}, nil, []int64{inText.Seq, inTopic.Seq, after.Seq}},
		{"either word", []string{"merge:49638", "nothing"}, nil, []int64{inText.Seq, inTopic.Seq, inTopic.Seq + 1, after.Seq}},
		{"every word", nil, []string{"merge:49638", "again"}, []int64{after.Seq}},
		{"no word", []string{"absent-word"}, nil, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			match := st.TrigramsNarrow(t.Context(), tt.either, tt.all)
			if match == "" {
				t.Fatal("no match expression")
			}
			records, err := st.Query(t.Context(), store.Filter{Drive: "d", Trigrams: match})
			if err != nil {
				t.Fatal(err)
			}
			got := make([]int64, 0, len(records))
			for _, r := range records {
				got = append(got, r.Seq)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("seqs = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("seqs = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestTrigramsForgetACollectedRecord(t *testing.T) {
	st, clock := testutil.Store(t)
	gone := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "collect-me-word"})
	if err := st.IndexTrigrams(t.Context(), noPause); err != nil {
		t.Fatal(err)
	}
	clock.Advance(store.HotWindow + 24*time.Hour)
	testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "newer"})
	if _, err := st.Collect(t.Context(), noPause); err != nil {
		t.Fatal(err)
	}
	if exists(t, st, gone.Seq) {
		t.Fatal("record not collected")
	}
	records, err := st.Query(t.Context(), store.Filter{Drive: "d", Trigrams: st.TrigramsNarrow(t.Context(), []string{"collect-me-word"}, nil)})
	if err != nil || len(records) != 0 {
		t.Fatalf("records = %+v, %v", records, err)
	}
}

func TestBoundSinceKeepsEveryRecordInTheWindow(t *testing.T) {
	st, clock := testutil.Store(t)
	testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "before"})
	clock.Advance(time.Hour)
	since := st.Now()
	first := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "first inside"})
	last := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "last inside"})
	bounded, err := st.BoundSince(t.Context(), store.Filter{Drive: "d", Since: since})
	if err != nil {
		t.Fatal(err)
	}
	if bounded.After != first.Seq-1 {
		t.Fatalf("After = %d, want %d", bounded.After, first.Seq-1)
	}
	records, err := st.Query(t.Context(), bounded)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].Seq != first.Seq || records[1].Seq != last.Seq {
		t.Fatalf("records = %v", records)
	}
	later, err := st.BoundSince(t.Context(), store.Filter{Drive: "d", Since: since, After: last.Seq})
	if err != nil {
		t.Fatal(err)
	}
	if later.After != last.Seq {
		t.Fatalf("a later After moved back to %d", later.After)
	}
	empty, err := st.BoundSince(t.Context(), store.Filter{Drive: "d", Since: since.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if empty.After != 0 {
		t.Fatalf("an empty window set After to %d", empty.After)
	}
}
