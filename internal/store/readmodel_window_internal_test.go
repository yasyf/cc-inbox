package store

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func TestAWindowedReadModelMatchesTheStore(t *testing.T) {
	st, _ := openStore(t)
	ctx := t.Context()
	asker := selectedPost(t, st, Record{Lane: "asker", Kind: kinds.Note, Text: "asks below"})
	open := selectedPost(t, st, Record{Lane: "asker", Kind: kinds.Ask, To: []string{"desk"}, Text: "still open"})
	closedOld := selectedPost(t, st, Record{Lane: "asker", Kind: kinds.Ask, To: []string{"desk"}, Text: "closed below the floor"})
	selectedPost(t, st, Record{Lane: "desk", Kind: kinds.Answer, Re: closedOld.Seq, To: []string{"asker"}, Text: "answered below"})
	closedNew := selectedPost(t, st, Record{Lane: "asker", Kind: kinds.Ask, To: []string{"desk"}, Text: "closed above the floor"})
	incident := selectedPost(t, st, Record{Lane: "desk", Kind: kinds.Incident, Topic: "fire", Text: "an old incident"})
	for i := range 40 {
		selectedPost(t, st, Record{Lane: "filler", Kind: kinds.Note, Text: fmt.Sprint("filler ", i)})
	}
	selectedPost(t, st, Record{Lane: "desk", Kind: kinds.Answer, Re: closedNew.Seq, To: []string{"asker"}, Text: "answered above"})
	update := selectedPost(t, st, Record{Lane: "desk", Kind: kinds.Incident, Re: incident.Seq, Topic: "fire", Text: "an update"})
	reply := selectedPost(t, st, Record{Lane: "desk", Kind: kinds.Note, Re: asker.Seq, To: []string{"other"}, Text: "a reply to the asker's old note"})
	for i := range 5 {
		selectedPost(t, st, Record{Lane: "filler", Kind: kinds.Note, To: []string{"other"}, Text: fmt.Sprint("late ", i)})
	}
	seqs := []int64{open.Seq, closedOld.Seq, closedNew.Seq, incident.Seq, update.Seq, reply.Seq}
	since := time.UnixMilli(0)
	type reads struct {
		Open                 []int64
		OpenSince, Untracked []Record
		Older, Count         int
		ForAsker, Ascending  []Record
		Newest               []Record
	}
	read := func() reads {
		t.Helper()
		var r reads
		var err error
		if r.Open, err = st.OpenSeqsAmong(ctx, "d", seqs); err != nil {
			t.Fatal(err)
		}
		slices.Sort(r.Open)
		if r.OpenSince, r.Untracked, r.Older, err = st.OpenSince(ctx, "d", since); err != nil {
			t.Fatal(err)
		}
		if r.Count, err = st.Count(ctx, Filter{Drive: "d"}); err != nil {
			t.Fatal(err)
		}
		if r.ForAsker, err = st.Query(ctx, Filter{Drive: "d", For: "asker", Descending: true, Limit: 1}); err != nil {
			t.Fatal(err)
		}
		if r.Ascending, err = st.Query(ctx, Filter{Drive: "d"}); err != nil {
			t.Fatal(err)
		}
		if r.Newest, err = st.Query(ctx, Filter{Drive: "d", Descending: true, Limit: 3}); err != nil {
			t.Fatal(err)
		}
		return r
	}
	want := read()
	model, err := st.buildReadModel(ctx, func(context.Context) error { return nil }, ReadModelBytes, 20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.ServeReadModel(nil); _ = model.Close() })
	if floor := model.Stats().Floor; floor <= incident.Seq || floor > update.Seq {
		t.Fatalf("floor = %d, want it between #%d and #%d", floor, incident.Seq, update.Seq)
	}
	st.ServeReadModel(model)
	got := read()
	a, _ := json.Marshal(want)
	b, _ := json.Marshal(got)
	if string(a) != string(b) {
		t.Errorf("a windowed model reads\n%s\nthe store reads\n%s", b, a)
	}
	if _, answered, err := model.Answer(ctx, st, Filter{Drive: "d"}); err != nil || answered {
		t.Errorf("a read from the first record answered = %t, %v; want it left to the store", answered, err)
	}
	if got, answered, err := model.Answer(ctx, st, Filter{Drive: "d", For: "asker", Descending: true, Limit: 1}); err != nil || !answered || len(got) != 1 || got[0].Seq != reply.Seq {
		t.Errorf("the asker's newest record = %v, %t, %v; want the model to name #%d through its reply target", seqsOf(got), answered, err, reply.Seq)
	}
	later := selectedPost(t, st, Record{Lane: "desk", Kind: kinds.Note, Re: closedOld.Seq, To: []string{"other"}, Text: "a reply the model takes after it was built"})
	if got, answered, err := model.Answer(ctx, st, Filter{Drive: "d", For: "asker", Descending: true, Limit: 1}); err != nil || !answered || len(got) != 1 || got[0].Seq != later.Seq {
		t.Errorf("the asker's newest record = %v, %t, %v; want #%d, named while the model caught up", seqsOf(got), answered, err, later.Seq)
	}
	if _, answered, err := model.Answer(ctx, st, Filter{Drive: "d", Descending: true, Limit: 3}); err != nil || !answered {
		t.Errorf("the newest three answered = %t, %v; want the model to answer them", answered, err)
	}
	if !slices.Contains(want.Open, open.Seq) || slices.Contains(want.Open, closedOld.Seq) || slices.Contains(want.Open, closedNew.Seq) {
		t.Errorf("open = %v; the fixture wants #%d open and #%d, #%d closed", want.Open, open.Seq, closedOld.Seq, closedNew.Seq)
	}
}
