package store_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

type timed struct {
	after time.Duration
	rec   store.Record
}

func stall(lane, topic, text string) store.Record {
	return store.Record{Lane: lane, Kind: kinds.Urgent, Topic: topic, To: []string{"root"}, Text: text}
}

func delivered(t *testing.T, f store.Filter, corpus []timed) []string {
	t.Helper()
	st, clock := testutil.Store(t)
	start := clock.Now()
	for _, c := range corpus {
		clock.Advance(start.Add(c.after).Sub(clock.Now()))
		testutil.Post(t, st, c.rec)
	}
	got, err := st.Query(context.Background(), f)
	if err != nil {
		t.Fatal(err)
	}
	texts := make([]string, 0, len(got))
	for _, r := range got {
		texts = append(texts, r.Text)
	}
	return texts
}

func TestDeliveryMutesKindsAndHoldsUrgentRepeats(t *testing.T) {
	corpus := []timed{
		{0, stall("stall-watch", "go:1", "go 1 stalled 10 min")},
		{0, store.Record{Lane: "desk-runner", Kind: kinds.State, To: []string{"root"}, Text: "QUEUED go 1"}},
		{0, store.Record{Lane: "desk-runner", Kind: kinds.Ask, To: []string{"root"}, Text: "merge go 1?"}},
		{10 * time.Minute, stall("stall-watch", "go:1", "go 1 stalled 20 min")},
		{10 * time.Minute, stall("desk-runner", "go:1", "go 1 unqueued")},
		{10 * time.Minute, stall("stall-watch", "go:2", "go 2 stalled 10 min")},
		{10 * time.Minute, stall("stall-watch", "", "untopiced stall")},
		{20 * time.Minute, stall("stall-watch", "", "untopiced stall again")},
		{41 * time.Minute, stall("stall-watch", "go:1", "go 1 stalled 51 min")},
	}
	for _, tc := range []struct {
		name   string
		filter store.Filter
		want   []string
	}{
		{
			name:   "delivery",
			filter: store.Filter{Drive: "d", For: "root", Mute: []kinds.Kind{kinds.State}, RepeatWindow: store.UrgentRepeat},
			want:   []string{"go 1 stalled 10 min", "merge go 1?", "go 1 unqueued", "go 2 stalled 10 min", "untopiced stall", "untopiced stall again", "go 1 stalled 51 min"},
		},
		{
			name:   "tail",
			filter: store.Filter{Drive: "d", For: "root"},
			want:   []string{"go 1 stalled 10 min", "QUEUED go 1", "merge go 1?", "go 1 stalled 20 min", "go 1 unqueued", "go 2 stalled 10 min", "untopiced stall", "untopiced stall again", "go 1 stalled 51 min"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := delivered(t, tc.filter, corpus); !slices.Equal(got, tc.want) {
				t.Fatalf("delivered %q, want %q", got, tc.want)
			}
		})
	}
}

func TestAddressedKindsNarrowOnlyRecordsAddressedToTheReader(t *testing.T) {
	corpus := []timed{
		{0, store.Record{Lane: "desk-runner", Kind: kinds.State, To: []string{"root"}, Text: "state to root"}},
		{0, store.Record{Lane: "ship-lane", Kind: kinds.Report, To: []string{"main"}, Text: "report to main"}},
		{0, store.Record{Lane: "ship-lane", Kind: kinds.Ask, To: []string{"main"}, Text: "ask to main"}},
		{0, store.Record{Lane: "fix-lane", Kind: kinds.Incident, To: []string{"root"}, Text: "incident to root"}},
		{0, store.Record{Lane: "ship-lane", Kind: kinds.Head, Text: "head broadcast"}},
		{0, store.Record{Lane: "ship-lane", Kind: kinds.State, Text: "state broadcast"}},
		{0, store.Record{Lane: "peer-lane", Kind: kinds.Note, To: []string{"peer:s1"}, Text: "note to peer"}},
		{0, store.Record{Lane: "peer-lane", Kind: kinds.Answer, To: []string{"peer:s1"}, Text: "answer to peer"}},
	}
	rootKinds := store.DefaultToKinds("main")
	for _, tc := range []struct {
		name   string
		filter store.Filter
		want   []string
	}{
		{
			name:   "subscription",
			filter: store.Filter{Drive: "d", Peer: "s1", For: "root", Kinds: []kinds.Kind{kinds.State}, ToKinds: rootKinds},
			want:   []string{"ask to main", "incident to root", "state broadcast", "answer to peer"},
		},
		{
			name:   "hook",
			filter: store.Filter{Drive: "d", Peer: "s1", To: "root", ToKinds: rootKinds},
			want:   []string{"ask to main", "incident to root", "answer to peer"},
		},
		{
			name:   "tail",
			filter: store.Filter{Drive: "d", Peer: "s1", For: "root", Kinds: []kinds.Kind{kinds.State}},
			want:   []string{"state to root", "report to main", "ask to main", "incident to root", "state broadcast", "note to peer", "answer to peer"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := delivered(t, tc.filter, corpus); !slices.Equal(got, tc.want) {
				t.Fatalf("delivered %q, want %q", got, tc.want)
			}
		})
	}
	if got := store.DefaultToKinds("ship-lane"); got != nil {
		t.Fatalf("DefaultToKinds(ship-lane) = %v, want every kind", got)
	}
}
