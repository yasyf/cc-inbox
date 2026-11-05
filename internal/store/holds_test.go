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

func TestALaneThatStoodDownBeforeItWasNamedASuccessorLeavesTheStemToTheRunningLane(t *testing.T) {
	g := newRegistry(t)
	g.up("bugs-1")
	g.up("bugs-2")
	g.up("bugs-2-opus")
	g.standDown("bugs-2-opus", "")
	g.standDown("bugs-1", "bugs-2-opus")

	stem := g.note("bugs")
	first := g.note("bugs-1")
	fallback := g.note("bugs-2-opus")
	if got := g.reads("bugs-2"); !slices.Equal(got, []int64{stem, first, fallback}) {
		t.Fatalf("bugs-2 reads %v; want the stem's and both stood-down lanes' mail", got)
	}
	for _, name := range []string{"bugs", "bugs-1", "bugs-2-opus"} {
		if r := g.reach(name); r.Lane != "bugs-2" {
			t.Fatalf("Reach(%s) = %+v; want bugs-2", name, r)
		}
	}

	g.up("bugs-2-opus")
	if r := g.reach("bugs"); r.Lane != "bugs-2-opus" {
		t.Fatalf("Reach(bugs) = %+v after bugs-2-opus posted again; want it holding the stem it was handed", r)
	}
}

func TestASuccessorThatNeverRegisteredReadsItsOwnMail(t *testing.T) {
	g := newRegistry(t)
	g.up("bugs-1")
	g.standDown("bugs-1", "relief")
	if r := g.reach("relief"); r.Lane != "relief" {
		t.Fatalf("Reach(relief) = %+v; want the named successor", r)
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

func TestAReplicaKnowsTheHolderWhateverOrderTheRecordsArrived(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	start := clock.Now()
	arrive := func(r store.Record, written time.Duration) {
		t.Helper()
		if _, _, err := st.PostAt(ctx, r, 0, start.Add(written)); err != nil {
			t.Fatal(err)
		}
	}
	clock.Advance(time.Hour)
	arrive(store.Record{Drive: "d", Lane: "root", Kind: kinds.StandDown, To: []string{"desk-7"}, Fields: store.Fields{Attrs: map[string]string{"successor": "desk-8"}}, Text: "desk-7 stands down"}, 20*time.Minute)
	arrive(store.Record{Drive: "d", Lane: "desk-8", Kind: kinds.State, Text: "desk-8 up"}, 30*time.Minute)
	arrive(store.Record{Drive: "d", Lane: "desk-7", Kind: kinds.State, Text: "desk-7 from the backlog"}, 10*time.Minute)
	arrive(store.Record{Drive: "d", Lane: "desk-7", Kind: kinds.Report, Text: "desk-7 later in the backlog"}, 15*time.Minute)

	reach, err := st.Reach(ctx, "d", "desk")
	if err != nil {
		t.Fatal(err)
	}
	if reach.Lane != "desk-8" {
		t.Fatalf("Reach(desk) = %+v; want desk-8, the lane desk-7 handed over to", reach)
	}
	aliases, err := st.Aliases(ctx, "d", "desk-8")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(aliases, "desk") || !slices.Contains(aliases, "desk-7") {
		t.Fatalf("Aliases(desk-8) = %q; want desk and desk-7", aliases)
	}
}

func TestAnAskReadsOpenOnlyWhileALaneCanHearItsAnswer(t *testing.T) {
	g := newRegistry(t)
	ctx := context.Background()
	ask := func(lane string) int64 {
		return g.post(store.Record{Lane: lane, Kind: kinds.Ask, To: []string{"root"}, Text: "which head?"})
	}
	g.up("probe-1")
	orphaned := ask("probe-1")
	g.standDown("probe-1", "")
	g.up("relay-3")
	handed := ask("relay-3")
	g.standDown("relay-3", "relay-4")
	g.up("pool-1")
	g.up("pool-2")
	pooled := ask("pool-1")
	g.standDown("pool-1", "")
	g.up("parked-1")
	parked := ask("parked-1")
	g.standDown("parked-1", "")
	g.up("parked-1")
	g.up("answered-1")
	answered := ask("answered-1")
	g.post(store.Record{Lane: "root", Kind: kinds.Answer, Re: answered, To: []string{"answered-1"}, Text: "dev head"})
	g.up("shipped-1")
	finished := ask("shipped-1")
	g.post(store.Record{Lane: "shipped-1", Kind: kinds.Done, To: []string{"root"}, Text: "PR merged"})
	g.up("resting-1")
	resumed := ask("resting-1")
	g.post(store.Record{Lane: "resting-1", Kind: kinds.Stopped, To: []string{"root"}, Text: "parked"})
	g.up("resting-1")
	want := []int64{handed, pooled, parked, resumed}

	among, err := g.st.OpenAmong(ctx, "d", []int64{orphaned, handed, pooled, parked, answered, finished, resumed})
	if err != nil {
		t.Fatal(err)
	}
	digest, _, _, err := g.st.OpenSince(ctx, "d", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	toRoot, err := g.st.OpenTo(ctx, "d", "root", []kinds.Kind{kinds.Ask})
	if err != nil {
		t.Fatal(err)
	}
	for name, got := range map[string][]store.Record{"OpenAmong": among, "OpenSince": digest, "OpenTo": toRoot} {
		seqs := []int64{}
		for _, r := range got {
			if r.Kind == kinds.Ask {
				seqs = append(seqs, r.Seq)
			}
		}
		if !slices.Equal(seqs, want) {
			t.Errorf("%s open asks = %v; want %v: the successor's, the stem holder's and the lanes seen again keep theirs, the orphan's, the done lane's and the answered one close", name, seqs, want)
		}
	}
}

func TestAliasesFollowEveryHoldChange(t *testing.T) {
	for _, served := range []bool{false, true} {
		t.Run(fmt.Sprintf("read model %t", served), func(t *testing.T) {
			st, _ := testutil.Store(t)
			testutil.Post(t, st, store.Record{Lane: "desk-7", Kind: kinds.State, Text: "up"})
			if served {
				model, err := st.BuildReadModel(t.Context(), noPace)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { st.ServeReadModel(nil); _ = model.Close() })
				st.ServeReadModel(model)
			}
			steps := []store.Record{
				{Lane: "other", Kind: kinds.Note, Text: "changes no hold"},
				{Lane: "desk-8", Kind: kinds.State, Text: "registers"},
				{Lane: "root", Kind: kinds.StandDown, Text: "hands over", To: []string{"desk-7"}, Fields: store.Fields{Attrs: map[string]string{"successor": "desk-8"}}},
				{Lane: "desk-7", Kind: kinds.Report, Text: "came back"},
				{Lane: "root", Kind: kinds.StandDown, Text: "ends", To: []string{"desk-8"}},
			}
			for _, step := range steps {
				for _, lane := range []string{"desk", "desk-7", "desk-8"} {
					if _, err := st.Aliases(t.Context(), "d", lane); err != nil {
						t.Fatal(err)
					}
				}
				testutil.Post(t, st, step)
				fresh, err := store.Open(t.Context(), st.HomeDir())
				if err != nil {
					t.Fatal(err)
				}
				for _, lane := range []string{"desk", "desk-7", "desk-8"} {
					got, err := st.Aliases(t.Context(), "d", lane)
					if err != nil {
						t.Fatal(err)
					}
					want, err := fresh.Aliases(t.Context(), "d", lane)
					if err != nil {
						t.Fatal(err)
					}
					if !slices.Equal(got, want) {
						t.Errorf("after %s %s: Aliases(%s) = %q, a fresh store reads %q", step.Lane, step.Kind, lane, got, want)
					}
				}
				if err := fresh.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
