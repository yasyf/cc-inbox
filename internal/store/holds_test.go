package store_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

type registry struct {
	t     *testing.T
	st    *store.Store
	clock *testutil.Clock
	posts *int
}

func newRegistry(t *testing.T) registry {
	st, clock := testutil.Store(t)
	return registry{t, st, clock, new(int)}
}

func (g registry) post(r store.Record) int64 {
	g.t.Helper()
	g.clock.Advance(time.Second)
	*g.posts++
	r.Text = fmt.Sprintf("%s, post %d", r.Text, *g.posts)
	return testutil.Post(g.t, g.st, r).Seq
}

func (g registry) up(lane string) {
	g.t.Helper()
	g.post(store.Record{Lane: lane, Kind: kinds.State, Text: lane + " up"})
}

func (g registry) launch(lane string) {
	g.t.Helper()
	g.post(store.Record{Lane: "root", Kind: kinds.State, Topic: "spawn:" + lane, Refs: store.Refs{Lanes: []string{lane}}, Text: "SPAWNED " + lane + " /brief.md"})
}

func (g registry) standDown(lane, successor string) {
	g.t.Helper()
	r := store.Record{Lane: "root", Kind: kinds.StandDown, To: []string{lane}, Text: lane + " stands down"}
	if successor != "" {
		r.Fields.Attrs = map[string]string{"successor": successor}
	}
	g.post(r)
}

func (g registry) note(to string) int64 {
	g.t.Helper()
	return g.post(store.Record{Lane: "root", Kind: kinds.Note, To: []string{to}, Text: "for " + to})
}

func (g registry) reads(lane string) []int64 {
	g.t.Helper()
	got, err := g.st.Query(context.Background(), store.Filter{Drive: "d", To: lane, Kinds: []kinds.Kind{kinds.Note}})
	if err != nil {
		g.t.Fatal(err)
	}
	seqs := make([]int64, 0, len(got))
	for _, r := range got {
		seqs = append(seqs, r.Seq)
	}
	return seqs
}

func (g registry) reach(name string) store.Reach {
	g.t.Helper()
	r, err := g.st.Reach(context.Background(), "d", name)
	if err != nil {
		g.t.Fatal(err)
	}
	return r
}

func TestStem(t *testing.T) {
	for _, tt := range []struct{ lane, want string }{
		{"signal-valkey-3", "signal-valkey"},
		{"fidelity-desk-21", "fidelity-desk"},
		{"fidelity-desk", "fidelity-desk"},
		{"p8a-step7", "p8a-step7"},
		{"lane-1a", "lane-1a"},
		{"-3", "-3"},
		{"release-007", "release"},
	} {
		if got := store.Stem(tt.lane); got != tt.want {
			t.Errorf("Stem(%q) = %q, want %q", tt.lane, got, tt.want)
		}
	}
}

func TestAPostToAStemReachesItsNewestLiveHolder(t *testing.T) {
	g := newRegistry(t)
	g.up("sv-2")
	g.launch("sv-3")
	stem := g.note("sv")
	running := g.note("sv-2")
	if got := g.reads("sv-3"); !slices.Equal(got, []int64{stem}) {
		t.Fatalf("sv-3 reads %v after it registered; want the stem's #%d", got, stem)
	}
	if got := g.reads("sv-2"); !slices.Equal(got, []int64{running}) {
		t.Fatalf("running sv-2 reads %v; want only its own #%d, the stem now sv-3's", got, running)
	}
	if r := g.reach("sv"); r.Lane != "sv-3" || !slices.Equal(r.Holders, []string{"sv-3", "sv-2"}) || r.Started {
		t.Fatalf("Reach(sv) = %+v; want sv-3 holding over the running sv-2", r)
	}
	if r := g.reach("sv-2"); r.Lane != "sv-2" || r.Down || !r.Started {
		t.Fatalf("Reach(sv-2) = %+v; want a running lane reading its own mail", r)
	}

	g.standDown("sv-2", "")
	retired := g.note("sv-2")
	if got := g.reads("sv-3"); !slices.Equal(got, []int64{stem, running, retired}) {
		t.Fatalf("sv-3 reads %v after sv-2 stood down; want the stem's and all of sv-2's mail", got)
	}
	if r := g.reach("sv-2"); r.Lane != "sv-3" || !r.Down {
		t.Fatalf("Reach(sv-2) = %+v; want its mail on sv-3", r)
	}

	g.standDown("sv-3", "sv-4")
	moved := g.note("sv")
	if got := g.reads("sv-4"); !slices.Equal(got, []int64{stem, running, retired, moved}) {
		t.Fatalf("sv-4 reads %v after sv-3 handed it the stem; want every record of the family", got)
	}
	if got := g.reads("sv-3"); len(got) != 0 {
		t.Fatalf("stood-down sv-3 reads %v; want nothing", got)
	}
	for _, name := range []string{"sv", "sv-2", "sv-3"} {
		if r := g.reach(name); r.Lane != "sv-4" {
			t.Fatalf("Reach(%s) = %+v; want sv-4", name, r)
		}
	}

	g.standDown("sv-4", "")
	if r := g.reach("sv"); r.Lane != "" || len(r.Holders) != 0 {
		t.Fatalf("Reach(sv) = %+v after the last holder stood down; want no lane", r)
	}
	waiting := g.note("sv")
	g.clock.Advance(time.Minute)
	k := store.PeerKey{Session: "sv-5-thread"}
	if err := g.st.BindLane(context.Background(), k, "d", "sv-5", g.clock.Now()); err != nil {
		t.Fatal(err)
	}
	if r := g.reach("sv"); r.Lane != "sv-5" {
		t.Fatalf("Reach(sv) = %+v after sv-5 bound a thread; want sv-5", r)
	}
	cursor, ok, err := g.st.Cursor(context.Background(), store.LaneCursor(k), "d")
	if err != nil || !ok || cursor != waiting-1 {
		t.Fatalf("sv-5's lane cursor = %d, %t, %v; want it before #%d, the stem's mail no lane held, and past what sv-3 and sv-4 read", cursor, ok, err, waiting)
	}
}

func TestAnOlderLiveLaneNamedTheStemLeavesItsMailWithTheNewestHolder(t *testing.T) {
	g := newRegistry(t)
	g.up("desk")
	g.up("desk-7")
	g.up("other-1")
	order := g.note("desk")
	if got := g.reads("desk-7"); !slices.Equal(got, []int64{order}) {
		t.Fatalf("desk-7 reads %v; want the order to its alias, #%d", got, order)
	}
	if got := g.reads("other-1"); len(got) != 0 {
		t.Fatalf("other-1 reads %v; want nothing of another stem", got)
	}
	if r := g.reach("desk"); r.Lane != "desk-7" {
		t.Fatalf("Reach(desk) = %+v; want desk-7, the lane delivery gives the alias's mail", r)
	}
	if holding, err := g.st.Holding(context.Background(), "d", []string{"desk-7"}); err != nil || !slices.Equal(holding, []string{"desk-7"}) {
		t.Fatalf("Holding(desk-7) = %v, %v; want desk-7, which reads its stem's mail", holding, err)
	}

	g.launch("desk")
	if r := g.reach("desk"); r.Lane != "desk" {
		t.Fatalf("Reach(desk) = %+v after a launch under the stem itself; want desk", r)
	}
}

func TestARefusedPostStopsHoldingTheStemOnceTheIndexesAreMaintained(t *testing.T) {
	g := newRegistry(t)
	g.up("sv-1")
	g.up("other-1")
	refused := func(lane string) {
		g.t.Helper()
		g.post(store.Record{Lane: lane, Kind: kinds.Blocker, Topic: store.RefusedPostTopic, To: []string{lane}, Text: "post refused"})
	}
	refused("sv")
	refused("reposted")
	g.up("reposted")
	refused("bound")
	if err := g.st.BindLane(context.Background(), store.PeerKey{Session: "bound-thread"}, "d", "bound", g.clock.Now()); err != nil {
		t.Fatal(err)
	}
	order := g.note("sv")
	if got := g.reads("sv-1"); len(got) != 0 {
		t.Fatalf("sv-1 reads %v before maintenance; want this test to reproduce the stolen stem", got)
	}

	if _, err := store.MaintainIndexes(context.Background(), g.st.HomeDir()); err != nil {
		t.Fatal(err)
	}
	if got := g.reads("sv-1"); !slices.Equal(got, []int64{order}) {
		t.Fatalf("sv-1 reads %v; want the order to its alias, #%d", got, order)
	}
	if got := g.reads("other-1"); len(got) != 0 {
		t.Fatalf("other-1 reads %v; want nothing of another stem", got)
	}
	if r := g.reach("sv"); r.Lane != "sv-1" || r.Started {
		t.Fatalf("Reach(sv) = %+v; want sv-1 and no lane named sv", r)
	}
	for _, kept := range []string{"reposted", "bound"} {
		if r := g.reach(kept); !r.Started || r.Lane != kept {
			t.Fatalf("Reach(%s) = %+v; want the lane kept, it did more than be refused", kept, r)
		}
	}
	if _, err := store.MaintainIndexes(context.Background(), g.st.HomeDir()); err != nil {
		t.Fatalf("a second maintenance with nothing to drop: %v", err)
	}
}

func TestRunningNumberedLanesKeepTheirOwnMail(t *testing.T) {
	g := newRegistry(t)
	for _, lane := range []string{"fd-14", "fd-15", "fd-16"} {
		g.up(lane)
	}
	direct := map[string]int64{}
	for _, lane := range []string{"fd-14", "fd-15", "fd-16"} {
		direct[lane] = g.note(lane)
	}
	stem := g.note("fd")
	for lane, want := range map[string][]int64{"fd-14": {direct["fd-14"]}, "fd-15": {direct["fd-15"]}, "fd-16": {direct["fd-16"], stem}} {
		if got := g.reads(lane); !slices.Equal(got, want) {
			t.Errorf("%s reads %v; want %v", lane, got, want)
		}
	}
	if r := g.reach("fd"); r.Lane != "fd-16" || !slices.Equal(r.Holders, []string{"fd-16", "fd-15", "fd-14"}) {
		t.Fatalf("Reach(fd) = %+v; want the pool named, newest first", r)
	}
}

func TestALaneSeenAfterItsStandDownReadsItsMailAgain(t *testing.T) {
	g := newRegistry(t)
	g.up("desk-1")
	g.up("desk-2")
	g.standDown("desk-1", "")
	g.up("desk-1")
	if r := g.reach("desk-1"); r.Lane != "desk-1" || r.Down {
		t.Fatalf("Reach(desk-1) = %+v after it posted again; want it live", r)
	}
}

func TestASuccessorOwesItsPredecessorsOpenAsk(t *testing.T) {
	g := newRegistry(t)
	g.up("sv-2")
	ask := g.post(store.Record{Lane: "root", Kind: kinds.Ask, To: []string{"sv-2"}, Text: "ready to land?"})
	g.standDown("sv-2", "sv-3")
	open, err := g.st.OpenTo(context.Background(), "d", "sv-3", []kinds.Kind{kinds.Ask})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(open, func(r store.Record) bool { return r.Seq == ask }) {
		t.Fatalf("sv-3's open asks = %+v; want sv-2's #%d", open, ask)
	}
	chain, standDown, err := g.st.Successors(context.Background(), "d", "sv-2")
	if err != nil || !slices.Equal(chain, []string{"sv-3"}) || standDown == 0 {
		t.Fatalf("Successors(sv-2) = %v, %d, %v; want [sv-3] and its stand-down", chain, standDown, err)
	}
}
