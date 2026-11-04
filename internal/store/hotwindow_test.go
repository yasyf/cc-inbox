package store_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func noPace(context.Context) error { return nil }

func sameRecords(t *testing.T, got, want []store.Record) {
	t.Helper()
	a, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) {
		t.Fatalf("window = %s\nstore = %s", a, b)
	}
}

func TestHotWindowReadsWhatTheStoreReads(t *testing.T) {
	st, clock := testutil.Store(t)
	ask := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Ask, To: []string{"worker"}, Text: "old parent"})
	clock.Advance(72 * time.Hour)
	testutil.Post(t, st, store.Record{Lane: "worker", Kind: kinds.Answer, Re: ask.Seq, Text: "answers an ask under the floor"})
	testutil.Post(t, st, store.Record{Lane: "worker", Kind: kinds.Note, To: []string{"root"}, Text: "to root"})
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "broadcast"})
	window, err := st.BuildHotWindow(t.Context(), noPace)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = window.Close() })
	if stats := window.Stats(); stats.Floor <= ask.Seq {
		t.Fatalf("floor = %d, want it above the old parent %d", stats.Floor, ask.Seq)
	}
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Ask, To: []string{"worker"}, Text: "after the build"})
	floor := window.Stats().Floor
	for _, f := range []store.Filter{
		{Drive: "d", After: floor - 1},
		{Drive: "d", For: "root", After: floor - 1},
		{Drive: "d", For: "worker", After: floor - 1},
		{Drive: "d", Addressed: "root", After: floor - 1},
		{Drive: "d", To: "worker", After: floor - 1},
		{Drive: "d", Kinds: []kinds.Kind{kinds.Ask}, After: floor - 1},
		{Drive: "d", Descending: true, Limit: 2},
	} {
		got, answered, err := window.Answer(t.Context(), st, f)
		if err != nil || !answered {
			t.Fatalf("%+v: answered = %t, %v", f, answered, err)
		}
		want, err := st.Query(t.Context(), f)
		if err != nil {
			t.Fatal(err)
		}
		sameRecords(t, got, want)
	}
}

func TestHotWindowLeavesWhatItCannotProveToTheStore(t *testing.T) {
	st, clock := testutil.Store(t)
	old := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "under the floor"})
	clock.Advance(72 * time.Hour)
	inside := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "inside"})
	window, err := st.BuildHotWindow(t.Context(), noPace)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = window.Close() })
	for name, f := range map[string]store.Filter{
		"a walk from the start":        {Drive: "d"},
		"a page the window can't fill": {Drive: "d", Descending: true, Limit: 5},
		"a page under the floor":       {Drive: "d", Before: inside.Seq, Descending: true, Limit: 1},
	} {
		if _, answered, err := window.Answer(t.Context(), st, f); err != nil || answered {
			t.Errorf("%s: answered = %t, %v", name, answered, err)
		}
	}
	st.ServeHotWindow(window)
	records, err := st.Query(t.Context(), store.Filter{Drive: "d"})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].Seq != old.Seq {
		t.Fatalf("a read the window passed on = %+v", records)
	}
	if stats := window.Stats(); stats.Misses == 0 {
		t.Fatalf("stats = %+v, want the misses counted", stats)
	}
}

func TestHotWindowSwitch(t *testing.T) {
	home := t.TempDir()
	if store.HotWindowOn(home) {
		t.Fatal("on before anyone turned it on")
	}
	if err := store.SetHotWindow(home, true); err != nil || !store.HotWindowOn(home) {
		t.Fatalf("turn on: %v", err)
	}
	if err := store.SetHotWindow(home, false); err != nil || store.HotWindowOn(home) {
		t.Fatalf("turn off: %v", err)
	}
	if err := store.SetHotWindow(home, false); err != nil {
		t.Fatalf("turn off twice: %v", err)
	}
}
