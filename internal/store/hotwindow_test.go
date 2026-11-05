package store_test

import (
	"context"
	"encoding/json"
	"strings"
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
	records, err := st.Query(t.Context(), store.Filter{Drive: "d", IncludeExpired: true})
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

func TestHotWindowBoundsSinceAndKnowsWhatItCovers(t *testing.T) {
	st, clock := testutil.Store(t)
	below := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "below the floor"})
	clock.Advance(72 * time.Hour)
	first := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "first in the window"})
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "second in the window"})
	if st.Covers(store.Filter{Drive: "d", Since: first.At}) {
		t.Fatal("covers with no window served")
	}
	window, err := st.BuildHotWindow(t.Context(), noPace)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = window.Close() })
	st.ServeHotWindow(window)
	t.Cleanup(func() { st.ServeHotWindow(nil) })
	tests := []struct {
		name string
		f    store.Filter
		want bool
	}{
		{"a since inside the window", store.Filter{Drive: "d", Since: first.At}, true},
		{"a cursor at the floor", store.Filter{Drive: "d", After: first.Seq - 1}, true},
		{"a since past the newest record below the floor", store.Filter{Drive: "d", Since: below.At.Add(time.Hour)}, true},
		{"a since at the newest record below the floor", store.Filter{Drive: "d", Since: below.At}, false},
		{"the whole drive", store.Filter{Drive: "d"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := st.Covers(tt.f); got != tt.want {
				t.Errorf("Covers = %t, want %t", got, tt.want)
			}
		})
	}
	bounded, err := st.BoundSince(t.Context(), store.Filter{Drive: "d", Since: first.At})
	if err != nil {
		t.Fatal(err)
	}
	if bounded.After != first.Seq-1 {
		t.Errorf("After = %d, want %d", bounded.After, first.Seq-1)
	}
	if failures := window.Stats().Failures; failures != 0 {
		t.Errorf("failures = %d, want 0", failures)
	}
}

func TestHotWindowStaysUnderItsCap(t *testing.T) {
	st, _ := testutil.Store(t)
	first := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "first"})
	text := strings.Repeat("x", 390)
	for range 4200 {
		testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: text})
	}
	const limit = 1 << 18
	window, err := st.BuildHotWindowUnder(t.Context(), noPace, limit)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = window.Close() })
	stats := window.Stats()
	if stats.Bytes == 0 || stats.Bytes > limit {
		t.Errorf("bytes = %d, want some and at most %d", stats.Bytes, limit)
	}
	if stats.Floor <= first.Seq {
		t.Errorf("floor = %d, want it above the first record %d", stats.Floor, first.Seq)
	}
	f := store.Filter{Drive: "d", Descending: true, Limit: 10}
	got, answered, err := window.Answer(t.Context(), st, f)
	if err != nil {
		t.Fatal(err)
	}
	want, err := st.Query(t.Context(), f)
	if err != nil {
		t.Fatal(err)
	}
	if answered {
		sameRecords(t, got, want)
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
